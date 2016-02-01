-module(saturn_internal_serv).
-behaviour(gen_server).
-define(SERVER, ?MODULE).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/2]).

-export([handle/1,
         delayed_delivery_completed/2,
         deliver_delayed/4]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {queues :: dict(),
                busy :: dict(),
                delay,
                myid}).

-record(label, {operation :: remote_read | update | remote_reply,
                key,
                timestamp :: non_neg_integer(),
                node,
                sender :: non_neg_integer(),
                payload}).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link(Nodes, MyId) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [Nodes, MyId], []).

handle(Message) ->
    gen_server:call(?MODULE, Message, infinity).

delayed_delivery_completed(Node, Number) ->
    gen_server:call(?MODULE, {delayed_delivery_completed, Node, Number}).
%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init([Nodes, MyId]) ->
    {Queues, Busy} = lists:foldl(fun(Node,{Queues0, Busy0}) ->
                                    {dict:store(Node, queue:new(), Queues0), dict:store(Node, false, Busy0)}
                                 end, {dict:new(), dict:new()}, Nodes),
    {ok, #state{queues=Queues, myid=MyId, busy=Busy, delay=100}}.

handle_call({delayed_delivery_completed, Node, Number}, _From, S0=#state{queues=Queues0, busy=Busy0}) ->
    Queue0 = dict:fetch(Node, Queues0),
    List0 = queue:to_list(Queue0),
    Length = length(List0),
    case Length == Number of
        true ->
            List1 = [];
        false ->
            List1 = lists:sublist(List0, Number + 1, Length - Number)
    end,
    Queue1 = queue:from_list(List1),
    Queues1 = dict:store(Node, Queue1, Queues0),
    Busy1 = dict:store(Node, false, Busy0),
    {noreply, S0#state{queues=Queues1, busy=Busy1}};
    
handle_call({new_stream, Stream, IdSender}, _From, S0=#state{queues=Queues0}) ->
    Now = now_milisec(),
    Queues1 = lists:foldl(fun(Label, Acc0) ->
                            Key = Label#label.key,
                            lists:foldl(fun(Node, Acc1) ->
                                            case Node of
                                                IdSender ->
                                                    Acc1;
                                                _ ->
                                                    case groups_manager_serv:interested(Node, Key) of
                                                        {ok, true} ->
                                                            Queue0 = dict:fetch(Node, Acc1),
                                                            Queue1 = queue:in({Label, Now}, Queue0),
                                                            dict:store(Node, Queue1, Acc1);
                                                        {ok, false} -> Acc1
                                                    end
                                            end
                                        end, Acc0, dict:fetch_keys(Queues0))
                          end, Queues0, Stream),
    S1 = lists:foldl(fun(Node, State) ->
                        deliver_labels(Node, State)
                     end, S0#state{queues=Queues1}, dict:fetch_keys(Queues1)),
    {reply, ok, S1};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------
deliver_labels(Node, S0=#state{queues=Queues0, busy=Busy0}) ->
    Queue0 = dict:fetch(Node, Queues0),
    case dict:fetch(Node, Busy0) of
        true ->
            S0;
        false ->
            Stream0 = queue:to_list(Queue0),
            delivery_round(Stream0, Node, S0)
    end.

delivery_round(Stream, Node, S0=#state{delay=Delay, queues=Queues0, myid=MyId, busy=Busy0}) ->
    case filter_labels(Stream, [], Delay) of
        {[], []} ->
            Queues1 = dict:store(Node, queue:new(), Queues0),
            S0#state{queues=Queues1};
        {[], Rest} ->
            [First|Tail] = Rest,
            {Label, Time} = First,
            Delayable = get_delayable(Tail, Time, [Label]),
            spawn(saturn_internal_serv, deliver_delayed, [Node, MyId, Delayable, Time + Delay]),
            Queues1 = dict:store(Node, queue:from_list(Rest), Queues0),
            Busy1 = dict:store(Node, true, Busy0),
            S0#state{queues=Queues1, busy=Busy1};
        {FinalStream, Rest} ->
            case groups_manager_serv:get_hostport(Node) of
                {ok, {Host, Port}} ->
                    saturn_internal_propagation_fsm_sup:start_fsm([Port, Host, {new_stream, FinalStream, MyId}]);
                {error, no_host} ->
                    lager:error("no_host when trying to propagate")
            end,
            delivery_round(Rest, Node, S0)
    end.
    
deliver_delayed(Node, MyId, Stream, When) ->
    Now = now_milisec(),
    Delay = (When - Now),
    case Delay > 0 of
        true ->
            timer:sleep(trunc(Delay));
        false ->
            noop
    end,
    case groups_manager_serv:get_hostport(Node) of
        {ok, {Host, Port}} ->
            saturn_internal_propagation_fsm_sup:start_fsm([Port, Host, {new_stream, Stream, MyId}]);
        {error, no_host} ->
            lager:error("no_host when trying to propagate")
    end,
    saturn_internal_serv:delayed_delivery_completed(Node, length(Stream)).

get_delayable([], _Time, Delayable) ->
    Delayable;
    
get_delayable([H|T], Time, Delayable) ->
    {LabelNext, TimeNext} = H,
    case TimeNext=<Time of
        true ->
            get_delayable(T, Time, Delayable ++ [LabelNext]);
        false ->
            Delayable
    end.

filter_labels([], Sendable, _Delay) ->
    {Sendable, []};

filter_labels([Next|Rest], Sendable, Delay) ->
    Now = now_milisec(),
    {Label, Time} = Next,
    case (Time+Delay)>Now of
        true ->
            {Sendable, [Next|Rest]};
        false ->
            filter_labels(Rest, Sendable ++ [Label], Delay)
    end.

now_microsec()->
    %% Not very efficient. os:timestamp() faster but non monotonic. Test!
    {MegaSecs, Secs, MicroSecs} = erlang:now(),
    (MegaSecs * 1000000 + Secs) * 1000000 + MicroSecs.

now_milisec() ->
    now_microsec()/1000.

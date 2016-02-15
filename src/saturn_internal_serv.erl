-module(saturn_internal_serv).
-behaviour(gen_server).

-include("saturn_internal.hrl").

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/2]).

-export([handle/1,
         handle/2,
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

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

reg_name(MyId) ->  list_to_atom(integer_to_list(MyId) ++ atom_to_list(?MODULE)).

start_link(Nodes, MyId) ->
    case ?PROPAGATION_MODE of
        naive_erlang ->
            gen_server:start_link({global, reg_name(MyId)}, ?MODULE, [Nodes, MyId], []);
        _ ->
            gen_server:start_link({local, ?MODULE}, ?MODULE, [Nodes, MyId], [])
    end.

handle(Message) ->
    gen_server:call(?MODULE, Message, infinity).

handle(MyId, Message) ->
    gen_server:call({global, reg_name(MyId)}, Message, infinity).

delayed_delivery_completed(Node, Number) ->
    gen_server:call(?MODULE, {delayed_delivery_completed, Node, Number}).
%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init([Nodes, MyId]) ->
    {Queues, Busy} = lists:foldl(fun(Node,{Queues0, Busy0}) ->
                                    {dict:store(Node, queue:new(), Queues0), dict:store(Node, false, Busy0)}
                                 end, {dict:new(), dict:new()}, Nodes),
    {ok, #state{queues=Queues, myid=MyId, busy=Busy, delay=0}}.

handle_call({delayed_delivery_completed, Node, Number}, _From, S0=#state{queues=Queues0, busy=Busy0}) ->
    Busy1 = dict:store(Node, false, Busy0),
    S1 = S0#state{busy=Busy1},
    Queue0 = dict:fetch(Node, Queues0),
    List0 = queue:to_list(Queue0),
    Length = length(List0),
    case Length == Number of
        true ->
            List1 = [],
            Queue1 = queue:from_list(List1),
            Queues1 = dict:store(Node, Queue1, Queues0),
            S2 = S1#state{queues=Queues1};
        false ->
            List1 = lists:sublist(List0, Number + 1, Length - Number),
            S2 = delivery_round(List1, Node, S0)
    end,
    {noreply, S2};
    
handle_call({new_stream, Stream, IdSender}, _From, S0=#state{queues=Queues0}) ->
    Now = now_milisec(),
    Queues1 = lists:foldl(fun(Label, Acc0) ->
                            BKey = Label#label.bkey,
                            lists:foldl(fun(Node, Acc1) ->
                                            case Node of
                                                IdSender ->
                                                    Acc1;
                                                _ ->
                                                    case groups_manager_serv:interested(Node, BKey) of
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
            propagate_stream(Node, FinalStream, MyId),
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
    propagate_stream(Node, Stream, MyId),
    saturn_internal_serv:delayed_delivery_completed(Node, length(Stream)).

propagate_stream(Node, Stream, MyId) ->
    case ?PROPAGATION_MODE of
        naive_erlang ->
            case groups_manager_serv:is_leaf(Node) of
                {ok, true} ->
                    ServerName = list_to_atom(integer_to_list(Node) ++ atom_to_list(saturn_leaf_converger)),
                    gen_server:call({global, ServerName}, {new_stream, Stream, MyId}, infinity);
                {ok, false} ->
                    saturn_internal_serv:handle(Node, {new_stream, Stream, MyId})
            end;
        short_tcp ->
            case groups_manager_serv:get_hostport(Node) of
                {ok, {Host, Port}} ->
                    saturn_internal_propagation_fsm_sup:start_fsm([Port, Host, {new_stream, Stream, MyId}]);
                {error, no_host} ->
                    lager:error("no_host when trying to propagate")
            end
    end.

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

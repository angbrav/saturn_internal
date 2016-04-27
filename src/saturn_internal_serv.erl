-module(saturn_internal_serv).
-behaviour(gen_server).

-include("saturn_internal.hrl").

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/2]).

-export([handle/1,
         restart/1,
         handle/2]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {queues :: dict(),
                busy :: dict(),
                delays, %has to be in microsecs
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
    gen_server:cast(?MODULE, Message).

handle(MyId, Message) ->
    gen_server:cast({global, reg_name(MyId)}, Message).

restart(MyId) ->
    gen_server:call({global, reg_name(MyId)}, restart).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init([Nodes, MyId]) ->
    lager:info("Internal started: ~p", [MyId]),
    {Queues, Busy} = lists:foldl(fun(Node,{Queues0, Busy0}) ->
                                    {dict:store(Node, queue:new(), Queues0), dict:store(Node, false, Busy0)}
                                 end, {dict:new(), dict:new()}, Nodes),
    {ok, Delays0} = groups_manager_serv:get_delays_internal(),
    Delays1 = lists:foldl(fun({Node, Delay}, Dict) ->
                            dict:store(Node, Delay*1000, Dict)
                          end, dict:new(), dict:to_list(Delays0)),
    {ok, #state{queues=Queues, myid=MyId, busy=Busy, delays=Delays1}}.

handle_cast({new_stream, Stream, IdSender}, S0=#state{queues=Queues0, busy=Busy0, delays=Delays, myid=MyId}) ->
    Queues1 = lists:foldl(fun(Label, Acc0) ->
                            BKey = Label#label.bkey,
                            lists:foldl(fun(Node, Acc1) ->
                                            case Node of
                                                IdSender ->
                                                    Acc1;
                                                _ ->
                                                    case groups_manager_serv:interested(Node, BKey) of
                                                        {ok, true} ->
                                                            case Label#label.operation of
                                                                update ->
                                                                    Delay = dict:fetch(Node, Delays),
                                                                    Now = now_microsec(),
                                                                    Time = Now + Delay;
                                                                _ ->
                                                                    Time = 0
                                                            end,
                                                            Queue0 = dict:fetch(Node, Acc1),
                                                            Queue1 = queue:in({Time, Label}, Queue0),
                                                            dict:store(Node, Queue1, Acc1);
                                                        {ok, false} -> Acc1
                                                    end
                                            end
                                        end, Acc0, dict:fetch_keys(Queues0))
                          end, Queues0, Stream),
    {Queues2, Busy1} = lists:foldl(fun(Node, {Acc1, Acc2}) ->
                                    case dict:fetch(Node, Busy0) of
                                        false ->
                                            {NewQueue, NewPending} = deliver_labels(dict:fetch(Node, Queues1), Node, MyId, []),
                                            {dict:store(Node, NewQueue, Acc1), dict:store(Node, NewPending, Acc2)};
                                        true ->
                                            OldQueue = dict:fetch(Node, Queues1),
                                            {dict:store(Node, OldQueue, Acc1), dict:store(Node, true, Acc2)}
                                    end
                                   end, {dict:new(), dict:new()}, dict:fetch_keys(Queues1)),
    {noreply, S0#state{queues=Queues2, busy=Busy1}};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_call(restart, _From, S0=#state{busy=Busy0, queues=Queues0}) ->
    Queues1 = lists:foldl(fun({Node, _}, Acc) ->
                            dict:store(Node, queue:new(), Acc)
                          end, dict:new(), dict:to_list(Queues0)),
    Busy1 = lists:foldl(fun({Node, _}, Acc) ->
                            dict:store(Node, false, Acc)
                        end, dict:new(), dict:to_list(Busy0)),
    {reply, ok, S0#state{queues=Queues1, busy=Busy1}};

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_info({deliver_labels, Node}, S0=#state{queues=Queues0, busy=Busy0, myid=MyId}) ->
    {NewQueue, NewPending} = deliver_labels(dict:fetch(Node, Queues0), Node, MyId, []),
    Queues1 = dict:store(Node, NewQueue, Queues0),
    Busy1 = dict:store(Node, NewPending, Busy0),
    {noreply, S0#state{queues=Queues1, busy=Busy1}};

handle_info(Info, State) ->
    lager:info("Weird message: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------
deliver_labels(Queue0, Node, MyId, Deliverables0) ->
    Now = saturn_utilities:now_microsec(),
    case queue:out(Queue0) of
        {empty, Queue0} ->
            propagate_stream(Node, lists:reverse(Deliverables0), MyId),
            {Queue0, false};
        {{value, {Time, Label}}, Queue1} when Time =< Now ->
            deliver_labels(Queue1, Node, MyId, [Label|Deliverables0]);
        {{value, {Time, _Label}}, _Queue1} ->
            propagate_stream(Node, lists:reverse(Deliverables0), MyId),
            NextDelivery = trunc((Time - Now)/1000),
            erlang:send_after(NextDelivery, self(), {deliver_labels, Node}),
            {Queue0, true}
    end.
               
propagate_stream(_Node, [], _MyId) ->
    done;

propagate_stream(Node, Stream, MyId) ->
    %lager:info("Stream to propagate to ~p: ~p", [Node, Stream]),
    case ?PROPAGATION_MODE of
        naive_erlang ->
            case groups_manager_serv:is_leaf(Node) of
                {ok, true} ->
                    ServerName = list_to_atom(integer_to_list(Node) ++ atom_to_list(saturn_leaf_converger)),
                    gen_server:cast({global, ServerName}, {new_stream, Stream, MyId});
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

now_microsec()->
    %% Not very efficient. os:timestamp() faster but non monotonic. Test!
    {MegaSecs, Secs, MicroSecs} = erlang:now(),
    (MegaSecs * 1000000 + Secs) * 1000000 + MicroSecs.

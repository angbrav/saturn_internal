-module(saturn_internal_serv).
-behaviour(gen_server).
-define(SERVER, ?MODULE).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/2]).

-export([handle/1]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {queues :: dict(),
                myid}).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link(Nodes, MyId) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [Nodes, MyId], []).

handle(Message) ->
    gen_server:call(?MODULE, Message, infinity).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init([Nodes, MyId]) ->
    Queues = lists:foldl(fun(Node, Acc) ->
                            dict:store(Node, queue:new(), Acc)
                         end, dict:new(), Nodes),
    {ok, #state{queues=Queues, myid=MyId}}.

handle_call({new_stream, Stream, IdSender}, _From, S0=#state{queues=Queues0, myid=MyId}) ->
    Queues1 = lists:foldl(fun(Label, Acc0) ->
                            {Key, _Clock, _Node} = Label,
                            lists:foldl(fun(Node, Acc1) ->
                                            case Node of
                                                IdSender ->
                                                    Acc1;
                                                _ ->
                                                    case groups_manager_serv:interested(Node, Key) of
                                                        {ok, true} ->
                                                            Queue0 = dict:fetch(Node, Acc1),
                                                            Queue1 = queue:in(Label, Queue0),
                                                            dict:store(Node, Queue1, Acc1);
                                                        {ok, false} -> Acc1
                                                    end
                                            end
                                        end, Acc0, dict:fetch_keys(Queues0))
                          end, Queues0, Stream),
    Queues2 = lists:foldl(fun(Node, Acc) ->
                            Queue0 = dict:fetch(Node, Acc),
                            case queue:is_empty(Queue0) of
                                false ->
                                    Stream = queue:to_list(Queue0),
                                    case groups_manager_serv:get_hostport(Node) of
                                        {ok, {Host, Port}} ->
                                            saturn_internal_propagation_fsm_sup:start_fsm([Port, Host, {new_stream, Stream, MyId}]),
                                            dict:store(Node, queue:new(), Acc);
                                        {error, no_host} ->
                                            Acc
                                    end;
                                true ->
                                    Acc
                            end
                          end, Queues1, dict:fetch_keys(Queues1)),
    {reply, ok, S0#state{queues=Queues2}};

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


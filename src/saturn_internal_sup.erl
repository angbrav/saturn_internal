-module(saturn_internal_sup).

-behaviour(supervisor).

%% API
-export([start_link/0,
         start_internal/2]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_internal(Port, MyId) ->
    
    groups_manager_serv:set_myid(MyId),
    {ok, Nodes} = groups_manager_serv:get_mypath(),
    supervisor:start_child(?MODULE, {saturn_internal_serv,
                    {saturn_internal_serv, start_link, [Nodes, MyId]},
                    permanent, 5000, worker, [saturn_internal_serv]}),

    supervisor:start_child(?MODULE, {saturn_tcp_recv_fsm,
                    {saturn_tcp_recv_fsm, start_link, [Port, saturn_internal_serv]},
                    permanent, 5000, worker, [saturn_tcp_recv_fsm]}),

    supervisor:start_child(?MODULE, {tcp_connection_handler_fsm_sup,
                    {tcp_connection_handler_fsm_sup, start_link, []},
                    permanent, 5000, supervisor, [tcp_connection_handler_fsm_sup]}),
    

    {ok, List} = inet:getif(),
    {Ip, _, _} = hd(List),
    Host = inet_parse:ntoa(Ip),

    lager:info("saturn_internal started"),

    {ok, {Host, Port}}.

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->

    {ok, { {one_for_one, 5, 10}, []}}.

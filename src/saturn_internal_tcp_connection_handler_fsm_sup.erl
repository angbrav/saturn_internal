%% @doc Supervise the fsm.
-module(saturn_internal_tcp_connection_handler_fsm_sup).
-behavior(supervisor).

-export([start_fsm/1,
         start_link/0]).
-export([init/1]).


start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).


start_fsm(Args) ->
    supervisor:start_child(?MODULE, Args).


init([]) ->
    Worker = {saturn_internal_tcp_connection_handler_fsm,
              {saturn_internal_tcp_connection_handler_fsm, start_link, []},
              transient, 5000, worker, [saturn_internal_tcp_connection_handler_fsm]},
    {ok, {{simple_one_for_one, 5, 10}, [Worker]}}.

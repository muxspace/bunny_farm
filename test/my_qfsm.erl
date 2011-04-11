-module(my_qfsm).
-behaviour(gen_qfsm).
-export([get_value/1, next/0, get_connection/0,
         state_a/2, state_a/3,
         state_b/2, state_b/3,
         state_c/2, state_c/3]).
-export([start_link/1, init/2,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         stop/1, terminate/3, code_change/4]).

-record(state, {cache_pid, data}).

%% Get the value of the data field for testing
get_value(Event) -> gen_qfsm:sync_send_event(?MODULE, Event).

next() -> gen_qfsm:send_event(?MODULE, []).

%% For testing one of the cached connections
get_connection() -> gen_qfsm:sync_send_all_state_event(?MODULE, connection).

start_link(Data) ->
  ConnSpecs = [<<"qfsm.one">>, {<<"qfsm.two">>,<<"key">>}],
  gen_qfsm:start_link({local,?MODULE}, ?MODULE, [Data], [], ConnSpecs).

stop(Pid) ->
  gen_qfsm:send_all_state_event(Pid,stop).

init([Data], CachePid) ->
  State = #state{cache_pid=CachePid, data=Data},
  {ok, state_a, State}.
  

state_a(_Event, State) ->
  {next_state, state_b, State#state{data=1}}.

state_a(Event, _From, State) ->
  {reply, {Event,State#state.data}, state_b, State#state{data=1}}.

state_b(_Event, State) ->
  {next_state, state_c, State#state{data=2}}.

state_b(Event, _From, State) ->
  {reply, {Event,State#state.data}, state_c, State#state{data=2}}.

state_c(_Event, State) ->
  {next_state, state_a, State#state{data=3}}.

state_c(Event, _From, State) ->
  {reply, {Event,State#state.data}, state_a, State#state{data=3}}.


handle_event(stop, _StateName, StateData) ->
  {stop, normal, StateData};

handle_event(_Event, StateName, StateData) ->
  Data = StateData#state.data + 3,
  {next_state, StateName, StateData#state{data=Data}}.

handle_sync_event(connection, _From, StateName, StateData) ->
  Conn = qcache:get_conn(StateData#state.cache_pid, <<"qfsm.two">>),
  error_logger:info_msg("[my_qfsm] Conn = ~p~n", [Conn]),
  {reply, Conn, StateName, StateData};

handle_sync_event(Event, _From, StateName, StateData) ->
  Data = StateData#state.data + 3,
  {reply, {Event,Data}, StateName, StateData#state{data=Data}}.

handle_info(_, StateName, StateData) ->
  {noreply, StateName, StateData}.

terminate(_,_,_) -> ok.
code_change(_,StateName,StateData,_) -> {ok, StateName, StateData}.

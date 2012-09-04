-module(mz_qserver).
-behaviour(gen_qserver).
-export([get_value/2, set_value/3,
         get_connection/1, get_connection/2, get_connections/1]).
-export([start_link/2, init/2,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         stop/1, terminate/2, code_change/3]).

-record(state, {cache_pid, tuples}).

get_value(Name,K) -> gen_qserver:call(Name, {get_value,K}).
set_value(Name,K,V) -> gen_qserver:cast(Name, {set_value,K,V}).

%% For testing one of the cached connections
get_connection(Name) -> gen_qserver:call(Name, connection).
get_connection(Name,X) -> gen_qserver:call(Name, {connection,X}).
get_connections(Name) -> gen_qserver:call(Name, connections).

start_link(Name,TupleList) ->
  ConnSpecs = [
    <<"qserver.one">>,
    {<<"qserver.two">>,<<"key">>},
    {<<"qserver.three">>,[{encoding,<<"application/bson">>}]}
  ],
  gen_qserver:start_link({local,Name}, ?MODULE, TupleList, [], ConnSpecs).

stop(Pid) ->
  gen_qserver:cast(Pid,stop).

init(TupleList, CachePid) ->
  State = #state{cache_pid=CachePid, tuples=TupleList ++ [{key1,1},{key2,2}]},
  {ok, State}.
  
%% This passes through RPC calls
handle_call({<<B/binary>>, Args}, From, State) ->
  error_logger:info_msg("[mz_qserver] Got publish ~p => ~p~n", [B,Args]),
  handle_call(Args, From, State);

handle_call(connection, From, State) ->
  handle_call({connection, {<<"qserver.two">>,<<"key">>}}, From, State);

handle_call({connection,X}, _From, State) ->
  Conn = qcache:get_conn(State#state.cache_pid, X),
  {reply, Conn, State};

handle_call(connections, _From, State) ->
  {reply, qcache:get_conns(State#state.cache_pid), State};

handle_call({get_value,K}, _From, State) ->
  {reply, proplists:get_value(K,State#state.tuples), State}.


handle_cast({<<B/binary>>, Args}, State) ->
  error_logger:info_msg("[mz_qserver] Got RPC ~p => ~p~n", [B,Args]),
  handle_cast(Args, State);

handle_cast({set_value,K,V}, State) ->
  error_logger:info_msg("[mz_qserver] Setting ~p = ~p~n", [K,V]),
  TupleList = lists:keystore(K,1,State#state.tuples, {K,V}),
  {noreply, State#state{tuples=TupleList}};

handle_cast(stop, State) -> {stop,normal,State};

handle_cast(A, State) ->
  error_logger:info_msg("[mz_qserver] Got unexpected cast: ~p~n", [A]),
  {noreply, State}.

handle_info(_, State) ->
  {noreply, State}.

terminate(_,_) -> ok.
code_change(_,State,_) -> {ok, State}.

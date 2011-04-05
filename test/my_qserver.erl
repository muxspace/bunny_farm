-module(my_qserver).
-behaviour(gen_qserver).
-export([get_value/1, set_value/2]).
-export([start_link/1, init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         stop/1, terminate/2, code_change/3]).

get_value(K) -> gen_qserver:call(?MODULE, {get_value,K}).
set_value(K,V) -> gen_qserver:cast(?MODULE, {set_value,K,V}).

start_link(TupleList) ->
  ConnSpecs = [<<"exchange.one">>, {<<"exchange.two">>,<<"key">>}],
  gen_qserver:start_link({local,?MODULE}, ?MODULE, TupleList, [], ConnSpecs).

stop(Pid) ->
  gen_qserver:cast(Pid,stop).

init(TupleList) ->
  {ok, TupleList ++ [{key1,1},{key2,2}]}.
  
%% This passes through RPC calls
handle_call({<<_B/binary>>, Args}, From, State) ->
  handle_call(Args, From, State);

handle_call({get_value,K}, _From, State) ->
  {reply, proplists:get_value(K,State), State}.

handle_cast({set_value,K,V}, State) ->
  NewState = lists:keystore(K,1,State, {K,V}),
  {noreply, NewState};

handle_cast(_, State) ->
  {noreply, State}.

handle_info(_, State) ->
  {noreply, State}.

terminate(_,_) -> ok.
code_change(_,State,_) -> {ok, State}.

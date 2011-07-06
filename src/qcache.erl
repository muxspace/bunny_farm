%% Copyright 2011 Brian Lee Yung Rowe
%% 
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%% 
%%   http://www.apache.org/licenses/LICENSE-2.0
%% 
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.

-module(qcache).
-behaviour(gen_server).
-include("bunny_farm.hrl").
-include("private_macros.hrl").
-export([start_link/0, init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2, code_change/3]).
-export([get_bus/2, get_conn/2,
         put_conn/2, put_conns/2,
         activate/2,
         connections/1 ]).

-record(state, {handles=[]}).
-record(qconn, {id, tag, active=false, handle}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% PUBLIC %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_link() ->
  gen_server:start_link(?MODULE, [], []).

get_bus(ServerRef, Tuple) when is_tuple(Tuple) ->
  gen_server:call(ServerRef, {get_bus, Tuple});

get_bus(ServerRef, Exchange) ->
  gen_server:call(ServerRef, {get_bus, {id,Exchange}}).

get_conn(ServerRef, Tuple) when is_tuple(Tuple) ->
  gen_server:call(ServerRef, {get_conn, Tuple});

get_conn(ServerRef, Exchange) ->
  gen_server:call(ServerRef, {get_conn, {id,Exchange}}).

put_conn(ServerRef, PropList) ->
  gen_server:cast(ServerRef, {put_conn, PropList}).

put_conns(ServerRef, Conns) when is_list(Conns) ->
  gen_server:cast(ServerRef, {put_conns, Conns}).

activate(ServerRef, Tuple) when is_tuple(Tuple) ->
  gen_server:cast(ServerRef, {activate, Tuple}).

connections(ServerRef) ->
  gen_server:call(ServerRef, connections).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% PRIVATE %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
to_proplist(#qconn{}=QConn) ->
  [_Name|Vs] = tuple_to_list(QConn),
  Ks = record_info(fields,qconn),
  lists:zip(Ks,Vs).

to_qconn(PropList) when is_list(PropList) ->
  list_to_tuple([qconn|[?PV(X,PropList) || X <- record_info(fields,qconn)]]).


bus({tag,Tag}, State) ->
  case lists:keyfind(Tag,3,State#state.handles) of
    false -> not_found;
    #qconn{}=QConn -> QConn#qconn.handle
  end;

bus({id,Exchange}, State) ->
  case lists:keyfind(Exchange,2,State#state.handles) of
    false -> not_found;
    #qconn{}=QConn -> QConn#qconn.handle
  end.


conn({tag,Tag}, State) ->
  case lists:keyfind(Tag,3,State#state.handles) of
    false -> not_found;
    #qconn{}=QConn -> to_proplist(QConn)
  end;

conn({id,Exchange}, State) ->
  case lists:keyfind(Exchange,2,State#state.handles) of
    false -> not_found;
    #qconn{}=QConn -> to_proplist(QConn)
  end.



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% GEN_SERVER %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init(_) ->
  %error_logger:info_msg("[qcache] Starting up~n"),
  {ok, #state{}}.

handle_call(connections, _From, State) ->
  Handles = lists:map(fun(X) -> to_proplist(X) end, State#state.handles),
  {reply, Handles, State};

handle_call({get_bus,Tuple}, _From, State) when is_tuple(Tuple) ->
  BusHandle = bus(Tuple, State),
  {reply, BusHandle, State};

handle_call({get_conn,Tuple}, _From, State) when is_tuple(Tuple) ->
  Conn = conn(Tuple, State),
  {reply, Conn, State}.


%% This replaces existing handles with the same exchange name.
handle_cast({put_conn,PropList}, State) when is_list(PropList) ->
  QConn = to_qconn(PropList),
  Handles = lists:keystore(QConn#qconn.id, 2, State#state.handles, QConn),
  {noreply, State#state{handles=Handles}};

handle_cast({put_conns,Conns}, State) when is_list(Conns) ->
  Fn = fun(X, Acc) ->
    QConn = to_qconn(X),
    lists:keystore(QConn#qconn.id, 2, Acc, QConn)
  end,
  Handles = lists:foldl(Fn, State#state.handles, Conns),
  ?info("Storing handles:~n  ~p", [Handles]),
  {noreply, State#state{handles=Handles}};

handle_cast({activate, {tag,Tag}}, State) ->
  H = State#state.handles,
  Handles = case lists:keyfind(Tag,3, H) of
    %% TODO Some sort of error needs to be thrown if the Tag doesn't exist
    false -> H;
    #qconn{}=QConn ->
      lists:keystore(Tag,3,H,QConn#qconn{active=true})
  end,
  {noreply, State#state{handles=Handles}};

handle_cast({activate, {id,Exchange}}, State) ->
  H = State#state.handles,
  Handles = case lists:keyfind(Exchange,2, H) of
    %% TODO Some sort of error needs to be thrown if the Tag doesn't exist
    false -> H;
    #qconn{}=QConn ->
      lists:keystore(Exchange,2,H,QConn#qconn{active=true})
  end,
  {noreply, State#state{handles=Handles}}.


handle_info(_Info, State) -> {noreply, State}.

terminate(_Reason, _State) -> ok.

code_change(_OldVersion, State, _Extra) ->
  {ok, State}.


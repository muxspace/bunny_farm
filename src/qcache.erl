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
-include("bunny_farm.hrl").
-include("private_macros.hrl").
-compile([{parse_transform,lager_transform}]).
-export([get_bus/2, get_conn/2, get_conns/1,
         put_conn/2, put_conns/2,
         activate/2,
         connections/1,
         new/0, delete/1]).

-record(qconn, {id, tag, active=false, handle}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% PUBLIC %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

new() ->
  Tid = ets:new(qcache_ets, [set, {keypos,2}]),
  {ok,Tid}.

get_bus(Tid, {id,Id}) ->
  get_bus(Tid, Id);

get_bus(Tid, #qconn{id=Id}) ->
  get_bus(Tid, Id);

get_bus(Tid, PropList) when is_list(PropList) ->
  get_bus(Tid, to_qconn(PropList));

get_bus(Tid, Id) ->
  case ets:lookup(Tid, Id) of
    [] -> not_found;
    [#qconn{handle=Handle}] -> Handle
  end.

get_conn(Tid, {id,Id}) -> get_conn(Tid,Id);

get_conn(Tid, {tag,Tag}) ->
  case ets:match(Tid, {'_','_',Tag,'_','_'}) of
    [] -> not_found;
    [[#qconn{}=Conn]] -> to_proplist(Conn)
  end;

get_conn(Tid, #qconn{id=Id}) ->
  get_conn(Tid, Id);

get_conn(Tid, PropList) when is_list(PropList) ->
  get_conn(Tid, to_qconn(PropList));

get_conn(Tid, Id) ->
  case ets:lookup(Tid, Id) of
    [] -> not_found;
    [#qconn{}=Conn] -> to_proplist(Conn)
  end.

get_conns(Tid) ->
  case ets:match(Tid, '$1') of
    [] -> [];
    Items when is_list(Items) -> [ to_proplist(V) || [V] <- Items ]
  end.

put_conn(Tid, #qconn{}=Conn) ->
  ets:insert(Tid, Conn);

put_conn(Tid, PropList) ->
  put_conn(Tid, to_qconn(PropList)).

put_conns(Tid, Conns) when is_list(Conns) ->
  [ put_conn(Tid, Conn) || Conn <- Conns ].

activate(Tid, {id,Id}) ->
  case ets:lookup(Tid,Id) of
    [[]] -> ok;
    [#qconn{}=Conn] -> put_conn(Tid, Conn#qconn{active=true});
    Other -> error_logger:info_msg("[qcache] Unexpected result: '~p'~n",[Other])
  end;

activate(Tid, {tag,Tag}) ->
  case ets:match(Tid, {'_','_',Tag,'_','_'}) of
    [[]] -> ok;
    [[#qconn{}=Conn]] -> put_conn(Tid, Conn#qconn{active=true});
    Other -> error_logger:info_msg("[qcache] Unexpected result: '~p'~n",[Other])
  end.

connections(Tid) ->
  Flat = lists:flatten(ets:match(Tid, '$1')),
  [ to_proplist(X) || X <- Flat ].

delete(Tid) -> 
  ets:delete(Tid).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% PRIVATE %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
to_proplist(#qconn{}=QConn) ->
  [_Name|Vs] = tuple_to_list(QConn),
  Ks = record_info(fields,qconn),
  lists:zip(Ks,Vs).

to_qconn(PropList) when is_list(PropList) ->
  list_to_tuple([qconn|[?PV(X,PropList) || X <- record_info(fields,qconn)]]).


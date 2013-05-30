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

-module(gen_qserver).
-behaviour(gen_server).
-include("bunny_farm.hrl").
-include("private_macros.hrl").
-compile([{parse_transform,lager_transform}]).
-export([behaviour_info/1]).
-export([start_link/4, start_link/5, init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2, code_change/3]).
-export([call/2, call/3, cast/2]).

-record(gen_qstate, {module, module_state, cache_pid, encoding}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% PUBLIC %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

behaviour_info(callbacks) ->
  [{init,2},
   {handle_call,3},
   {handle_cast,2},
   {terminate,2}].

call(ServerRef, Request) ->
  gen_server:call(ServerRef, Request).

call(ServerRef, Request, Timeout) ->
  gen_server:call(ServerRef, Request, Timeout).

cast(ServerRef, Request) ->
  gen_server:cast(ServerRef, Request).

start_link(Module, Args, [{encoding,Encoding}|Options], Connections) ->
  gen_server:start_link(?MODULE, [Module,Args,Connections,Encoding], Options);

start_link(Module, Args, Options, Connections) ->
  gen_server:start_link(?MODULE, [Module,Args,Connections], Options).

start_link(ServerName, Module, Args, [{encoding,Encoding}|Options], ConnSpecs) ->
  gen_server:start_link(ServerName, ?MODULE, [Module,Args,ConnSpecs,Encoding], Options);

start_link(ServerName, Module, Args, Options, ConnSpecs) ->
  gen_server:start_link(ServerName, ?MODULE, [Module,Args,ConnSpecs], Options).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% PRIVATE %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
tag() ->
  In = float_to_list(element(3,now()) + random:uniform()),
  base64:encode(In).

bus(CachePid, {id,X}) ->
  case qcache:get_bus(CachePid, {id,X}) of
    not_found ->
      Conn = connect(X),
      qcache:put_conn(CachePid, Conn),
      proplists:get_value(handle,Conn);
    BH -> BH
  end.

%% Publish with options
%-spec connect({maybe_binary(), maybe_binary()}) -> [tuple()].
connect({<<X/binary>>, Options}) when is_list(Options) ->
  lager:debug("Opening ~p for publishing with options ~p", [X,Options]),
  Handle = bunny_farm:open({X,Options}),
  %error_logger:info_msg("[gen_qserver] Returning handle spec"),
  [{id,X}, {tag,<<"">>}, {active,true}, {handle,Handle}];

%% Consume with maybe options
connect({MaybeX, MaybeK}) ->
  lager:debug("Opening ~p => ~p for consuming", [MaybeX,MaybeK]),
  Handle = bunny_farm:open(MaybeX,MaybeK),
  Tag = tag(),
  bunny_farm:consume(Handle, [{consumer_tag,Tag}]),
  Exchange = case MaybeX of
    {Exch,_} -> Exch;
    Exch -> Exch
  end,
  Key = case MaybeK of
    {K,_} -> K;
    K -> K
  end,
  %error_logger:info_msg("[gen_qserver] Returning handle spec"),
  [{id,{Exchange,Key}}, {tag,Tag}, {handle,Handle}];

%% Publish with no options
connect(<<X/binary>>) -> connect({X,[]}).


response({noreply, ModState}, #gen_qstate{}=State) ->
  {noreply, State#gen_qstate{module_state=ModState}};

response({noreply, ModState, hibernate}, #gen_qstate{}=State) ->
  {noreply, State#gen_qstate{module_state=ModState}, hibernate};

response({noreply, ModState, Timeout}, #gen_qstate{}=State) ->
  {noreply, State#gen_qstate{module_state=ModState}, Timeout};

response({reply, Reply, ModState}, #gen_qstate{}=State) ->
  {reply, Reply, State#gen_qstate{module_state=ModState}};

response({reply, Reply, ModState, hibernate}, #gen_qstate{}=State) ->
  {reply, Reply, State#gen_qstate{module_state=ModState}, hibernate};

response({reply, Reply, ModState, Timeout}, #gen_qstate{}=State) ->
  {reply, Reply, State#gen_qstate{module_state=ModState}, Timeout};

response({stop, Reason, ModState}, #gen_qstate{}=State) ->
  {stop, Reason, State#gen_qstate{module_state=ModState}};

response({stop, Reason, Reply, ModState}, #gen_qstate{}=State) ->
  {stop, Reason, Reply, State#gen_qstate{module_state=ModState}}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% GEN_SERVER %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([Module, Args, ConnSpecs]) ->
  init([Module, Args, ConnSpecs, undefined]);

init([Module, Args, ConnSpecs, Encoding]) ->
  {ok,Pid} = qcache:new(),
  Handles = lists:map(fun(Conn) -> connect(Conn) end, ConnSpecs),
  qcache:put_conns(Pid, Handles),
  random:seed(now()),
  case Module:init(Args, Pid) of
    {ok, ModuleState} ->
      State = #gen_qstate{module=Module, module_state=ModuleState, cache_pid=Pid, encoding=Encoding},
      Response = {ok, State};
    {ok, ModuleState, Timeout} ->
      State = #gen_qstate{module=Module, module_state=ModuleState, cache_pid=Pid, encoding=Encoding},
      Response = {ok, State, Timeout};
    {stop, Reason} ->
      Response = {stop, Reason}
  end,
  Response.

handle_call(Request, From, State) ->
  Module = State#gen_qstate.module,
  ModuleState = State#gen_qstate.module_state,
  Response = Module:handle_call(Request,From,ModuleState),
  response(Response, State).


handle_cast(Request, State) ->
  Module = State#gen_qstate.module,
  ModuleState = State#gen_qstate.module_state,
  Response = Module:handle_cast(Request,ModuleState),
  response(Response, State).



%% Tags are auto-generated during subscription
handle_info(#'basic.consume_ok'{consumer_tag=Tag}, State) ->
  lager:debug("Connection ACK on consumer_tag ~p",[Tag]),
  qcache:activate(State#gen_qstate.cache_pid, {tag,Tag}),
  {noreply, State};

% Handle messages coming off the bus
handle_info({#'basic.deliver'{routing_key=Key}, Content}, State) ->
  CachePid = State#gen_qstate.cache_pid,
  %lager:debug("Message:~n  ~p", [Content]),
  Payload = case State#gen_qstate.encoding of
    undefined -> farm_tools:decode_payload(Content);
    Encoding -> farm_tools:decode_payload(Encoding,Content)
  end,
  case farm_tools:is_rpc(Content) of
    true -> 
      ResponseTuple = handle_call({Key, Payload}, self(), State),
      case ResponseTuple of
        {noreply,NewState} -> ok;
        % TODO: Clean up the embedded cases
        {reply,Response,NewState} ->
          {X,ReplyTo} = farm_tools:reply_to(Content),
          BusHandle = bus(CachePid, {id,X}),
          %lager:debug("Responding to ~p => ~p", [X,ReplyTo]),
          %error_logger:info_msg("Responding to ~p => ~p~n", [X,ReplyTo]),
          lager:debug("Response = ~p", [Response]),
          Props = [ {content_type, farm_tools:content_type(Content)},
                    {correlation_id, farm_tools:correlation_id(Content)} ],
          Msg = #message{payload=Response, props=Props},
          bunny_farm:respond(Msg, ReplyTo, BusHandle)
      end,
      {noreply, NewState};
    _ ->
      handle_cast({Key,Payload}, State)
  end;

%% Fallback
handle_info(Info, #gen_qstate{module=Module,
                              module_state=ModuleState}=State) ->
  case Module:handle_info(Info, ModuleState) of
    {noreply, NewModuleState} ->
       {noreply, State#gen_qstate{module_state=NewModuleState}};
    {noreply, NewModuleState, AfterRequest} -> % timeout/hibernate
       {noreply, State#gen_qstate{module_state=NewModuleState}, AfterRequest};
    {stop, Reason, NewModuleState} ->
       {stop, Reason, State#gen_qstate{module_state=NewModuleState}}
  end.


terminate(Reason, #gen_qstate{cache_pid=CachePid}=State) ->
  Handles = qcache:connections(CachePid),
  Module = State#gen_qstate.module,
  ModuleState = State#gen_qstate.module_state,
  Module:terminate(Reason, ModuleState),
  Fn = fun(PList) ->
    bunny_farm:close(?PV(handle,PList), ?PV(tag,PList))
  end,
  lists:map(Fn, Handles),
  qcache:delete(CachePid),
  ok.

code_change(_OldVersion, State, _Extra) ->
  {ok, State}.


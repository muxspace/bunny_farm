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

-module(gen_qfsm).
-behaviour(gen_fsm).
-include("bunny_farm.hrl").
-include("private_macros.hrl").
-compile([{parse_transform,lager_transform}]).
-export([behaviour_info/1]).
-export([start_link/4, start_link/5, init/1,
         handle_info/3,
         handle_event/3,
         handle_sync_event/4,
         terminate/3, code_change/4]).
-export([send_event/2, send_all_state_event/2,
         sync_send_event/2, sync_send_event/3,
         sync_send_all_state_event/2, sync_send_all_state_event/3,
         reply/2,
         send_event_after/2,
         start_timer/2, cancel_timer/1]).
-export([static_state/2, static_state/3]).

-record(gen_qstate, {module, module_state, fsm_state, cache_pid}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% PUBLIC %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

behaviour_info(callbacks) ->
  [{init,2},
   {handle_event,3},
   {handle_sync_event,4},
   {terminate,3}].

send_event(ServerRef, Event) ->
  gen_fsm:send_event(ServerRef, Event).

send_all_state_event(ServerRef, Event) ->
  gen_fsm:send_all_state_event(ServerRef, Event).


sync_send_event(ServerRef, Event) ->
  gen_fsm:sync_send_event(ServerRef, Event).

sync_send_event(ServerRef, Event, Timeout) ->
  gen_fsm:sync_send_event(ServerRef, Event, Timeout).

sync_send_all_state_event(ServerRef, Event) ->
  gen_fsm:sync_send_all_state_event(ServerRef, Event).

sync_send_all_state_event(ServerRef, Event, Timeout) ->
  gen_fsm:sync_send_all_state_event(ServerRef, Event, Timeout).


start_link(Module, Args, Options, Connections) ->
  gen_fsm:start_link(?MODULE, [Module,Args,Connections], Options).

start_link(ServerName, Module, Args, Options, ConnSpecs) ->
  gen_fsm:start_link(ServerName, ?MODULE, [Module,Args,ConnSpecs], Options).


reply(Caller, Reply) ->
  gen_fsm:reply(Caller, Reply).

send_event_after(Time, Event) ->
  gen_fsm:send_event_after(Time, Event).

start_timer(Time, Msg) ->
  gen_fsm:start_timer(Time, Msg).

cancel_timer(TimerRef) ->
  gen_fsm:cancel_timer(TimerRef).

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

%% Consume
-spec connect({exchange(), routing_key()}) -> [ {exchange(), boolean(), bus_handle()} ].
connect({<<Exchange/binary>>, <<Key/binary>>}) ->
  lager:debug("Opening ~p => ~p for consuming", [Exchange,Key]),
  Handle = bunny_farm:open(Exchange,Key),
  Tag = tag(),
  bunny_farm:consume(Handle, [{consumer_tag,Tag}]),
  %error_logger:info_msg("[gen_qfsm] Returning handle spec"),
  [{id,Exchange}, {tag,Tag}, {handle,Handle}];

%% Consume
connect({Exchange, Key}) ->
  connect({farm_tools:binarize(Exchange), farm_tools:binarize(Key)});

%% Publish
connect(<<Exchange/binary>>) ->
  lager:debug("Opening ~p for publishing", [Exchange]),
  Handle = bunny_farm:open(Exchange),
  %error_logger:info_msg("[gen_qfsm] Returning handle spec"),
  [{id,Exchange}, {tag,<<"">>}, {active,true}, {handle,Handle}];

%% Publish
connect(Exchange) ->
  connect(farm_tools:binarize(Exchange)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% GEN_SERVER %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([Module, Args, ConnSpecs]) ->
  {ok,Pid} = qcache:new(),
  Handles = lists:map(fun(Conn) -> connect(Conn) end, ConnSpecs),
  qcache:put_conns(Pid, Handles),
  random:seed(now()),
  case Module:init(Args, Pid) of
    {ok, StateName, StateData} ->
      State = #gen_qstate{module=Module, module_state=StateData,
                          fsm_state=StateName, cache_pid=Pid},
      Response = {ok, static_state, State};
    {ok, StateName, StateData, Timeout} ->
      State = #gen_qstate{module=Module, module_state=StateData,
                          fsm_state=StateName, cache_pid=Pid},
      Response = {ok, static_state, State, Timeout};
    {stop, Reason} ->
      Response = {stop, Reason}
  end,
  Response.

static_state(Event, State) ->
  Module = State#gen_qstate.module,
  ModuleState = State#gen_qstate.module_state,
  FsmState = State#gen_qstate.fsm_state,
  case Module:FsmState(Event, ModuleState) of
    {next_state, FState, MState} ->
      NextState = State#gen_qstate{module_state=MState, fsm_state=FState},
    {next_state, static_state, NextState};

    {next_state, FState, MState, Timeout} ->
      NextState = State#gen_qstate{module_state=MState, fsm_state=FState},
    {next_state, static_state, NextState, Timeout};

    {stop, Reason, NextModState} ->
      {stop, Reason, State#gen_qstate{module_state=NextModState}}
  end.


static_state(Event, From, State) ->
  Module = State#gen_qstate.module,
  ModuleState = State#gen_qstate.module_state,
  FsmState = State#gen_qstate.fsm_state,
  case Module:FsmState(Event, From, ModuleState) of
    {reply,Reply,FState,MState} ->
      NextState = State#gen_qstate{module_state=MState, fsm_state=FState},
    {reply, Reply, static_state, NextState};

    {reply,Reply,FState,MState,Timeout} ->
      NextState = State#gen_qstate{module_state=MState, fsm_state=FState},
    {reply, Reply, static_state, NextState, Timeout};

    {next_state, FState, MState} ->
      NextState = State#gen_qstate{module_state=MState, fsm_state=FState},
    {next_state, static_state, NextState};

    {next_state, FState, MState, Timeout} ->
      NextState = State#gen_qstate{module_state=MState, fsm_state=FState},
    {next_state, static_state, NextState, Timeout};

    {stop, Reason, NextModState} ->
      {stop, Reason, State#gen_qstate{module_state=NextModState}}
  end.



handle_event(Event, static_state, State) ->
  Module = State#gen_qstate.module,
  ModuleState = State#gen_qstate.module_state,
  FsmState = State#gen_qstate.fsm_state,
  case Module:handle_event(Event, FsmState, ModuleState) of
    {next_state, FState, MState} ->
      NextState = State#gen_qstate{module_state=MState, fsm_state=FState},
    {next_state, static_state, NextState};

    {next_state, FState, MState, Timeout} ->
      NextState = State#gen_qstate{module_state=MState, fsm_state=FState},
    {next_state, static_state, NextState, Timeout};

    {stop, Reason, NextModState} ->
      {stop, Reason, State#gen_qstate{module_state=NextModState}}
  end.


handle_sync_event(Event, From, static_state, State) ->
  Module = State#gen_qstate.module,
  ModuleState = State#gen_qstate.module_state,
  FsmState = State#gen_qstate.fsm_state,
  case Module:handle_sync_event(Event, From, FsmState, ModuleState) of
    {reply,Reply,FState,MState} ->
      NextState = State#gen_qstate{module_state=MState, fsm_state=FState},
    {reply, Reply, static_state, NextState};

    {reply,Reply,FState,MState,Timeout} ->
      NextState = State#gen_qstate{module_state=MState, fsm_state=FState},
    {reply, Reply, static_state, NextState, Timeout};

    {next_state, FState, MState} ->
      NextState = State#gen_qstate{module_state=MState, fsm_state=FState},
    {next_state, static_state, NextState};

    {next_state, FState, MState, Timeout} ->
      NextState = State#gen_qstate{module_state=MState, fsm_state=FState},
    {next_state, static_state, NextState, Timeout};

    {stop, Reason, NextModState} ->
      {stop, Reason, State#gen_qstate{module_state=NextModState}}
  end.


%% Tags are auto-generated during subscription
handle_info(#'basic.consume_ok'{consumer_tag=Tag}, static_state, State) ->
  lager:debug("Connection ACK on consumer_tag ~p",[Tag]),
  qcache:activate(State#gen_qstate.cache_pid, {tag,Tag}),
  {next_state, static_state, State};

% Handle messages coming off the bus
handle_info({#'basic.deliver'{routing_key=Key}, Content},
              static_state, State) ->
  CachePid = State#gen_qstate.cache_pid,
  Payload = farm_tools:decode_payload(Content),
  case farm_tools:is_rpc(Content) of
    true -> 
      {reply,Response, _, MState} =
        static_state({Key, Payload}, self(), State),
      {X,ReplyTo} = farm_tools:reply_to(Content),
      BusHandle = bus(CachePid, {id,X}),
      lager:debug("Responding to ~p => ~p", [X,ReplyTo]),
      Props = [ {content_type, farm_tools:content_type(Content)},
                {correlation_id, farm_tools:correlation_id(Content)} ],
      Msg = #message{payload=Response, props=Props},
      bunny_farm:respond(Msg, ReplyTo, BusHandle),
      %error_logger:info_msg("[gen_qfsm] Sent"),
      {next_state, static_state, MState};
    _ ->
      static_state({Key,Payload}, State)
  end.


terminate(Reason, static_state, State) ->
  Handles = qcache:connections(State#gen_qstate.cache_pid),
  Module = State#gen_qstate.module,
  ModuleState = State#gen_qstate.module_state,
  FsmState = State#gen_qstate.fsm_state,
  Module:terminate(Reason, FsmState, ModuleState),
  Fn = fun(PList) ->
    bunny_farm:close(?PV(handle,PList), ?PV(tag,PList))
  end,
  lists:map(Fn, Handles),
  ok.

code_change(_OldVersion, static_state, State, _Extra) ->
  {ok, static_state, State}.


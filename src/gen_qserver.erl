-module(gen_qserver).
-behaviour(gen_server).
-include("bunny_farm.hrl").
-include("private_macros.hrl").
-export([behaviour_info/1]).
-export([start_link/4, start_link/5, init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2, code_change/3]).
-export([call/2, call/3, cast/2]).

-record(gen_qstate, {module, module_state, cache_pid}).

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

start_link(Module, Args, Options, Connections) ->
  gen_server:start_link(?MODULE, [Module,Args,Connections], Options).

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

%% Consume
-spec connect({exchange(), routing_key()}) -> [ {exchange(), boolean(), bus_handle()} ].
connect({<<Exchange/binary>>, <<Key/binary>>}) ->
  ?info("Opening ~p => ~p for consuming", [Exchange,Key]),
  Handle = bunny_farm:open(Exchange,Key),
  Tag = tag(),
  bunny_farm:consume(Handle, [{consumer_tag,Tag}]),
  %error_logger:info_msg("[gen_qserver] Returning handle spec"),
  [{id,Exchange}, {tag,Tag}, {handle,Handle}];

%% Consume
connect({Exchange, Key}) ->
  connect({farm_tools:binarize(Exchange), farm_tools:binarize(Key)});

%% Publish
connect(<<Exchange/binary>>) ->
  ?info("Opening ~p for publishing", [Exchange]),
  Handle = bunny_farm:open(Exchange),
  %error_logger:info_msg("[gen_qserver] Returning handle spec"),
  [{id,Exchange}, {tag,<<"">>}, {active,true}, {handle,Handle}];

%% Publish
connect(Exchange) ->
  connect(farm_tools:binarize(Exchange)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% GEN_SERVER %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([Module, Args, ConnSpecs]) ->
  {ok,Pid} = qcache:start_link(),
  random:seed(now()),
  case Module:init(Args, Pid) of
    {ok, ModuleState} ->
      Handles = lists:map(fun(Conn) -> connect(Conn) end, ConnSpecs),
      qcache:put_conns(Pid, Handles),
      State = #gen_qstate{module=Module, module_state=ModuleState, cache_pid=Pid},
      Response = {ok, State};
    {ok, ModuleState, Timeout} ->
      Handles = lists:map(fun(Conn) -> connect(Conn) end, ConnSpecs),
      qcache:put_conns(Pid, Handles),
      State = #gen_qstate{module=Module, module_state=ModuleState, cache_pid=Pid},
      Response = {ok, State, Timeout};
    {stop, Reason} ->
      Response = {stop, Reason}
  end,
  Response.

handle_call(Request, From, State) ->
  Module = State#gen_qstate.module,
  ModuleState = State#gen_qstate.module_state,
  {reply, Response, NextState} = Module:handle_call(Request,From,ModuleState),
  {reply, Response, State#gen_qstate{module_state=NextState}}.


handle_cast(Request, State) ->
  Module = State#gen_qstate.module,
  ModuleState = State#gen_qstate.module_state,
  {noreply, NextState} = Module:handle_cast(Request,ModuleState),
  {noreply, State#gen_qstate{module_state=NextState}}.



%% Tags are auto-generated during subscription
handle_info(#'basic.consume_ok'{consumer_tag=Tag}, State) ->
  ?info("Connection ACK on consumer_tag ~p",[Tag]),
  qcache:activate(State#gen_qstate.cache_pid, {tag,Tag}),
  {noreply, State};

% Handle messages coming off the bus
handle_info({#'basic.deliver'{routing_key=Key,exchange=OX}, Content}, State) ->
  CachePid = State#gen_qstate.cache_pid,
  Payload = farm_tools:decode_payload(Content),
  case farm_tools:is_rpc(Content) of
    true -> 
      {reply,Response,NewState} = handle_call({Key, Payload}, self(), State),
      {X,ReplyTo} = farm_tools:reply_to(Content, OX),
      BusHandle = bus(CachePid, {id,X}),
      ?info("Responding to ~p => ~p", [X,ReplyTo]),
      ?info("Response = ~p", [Response]),
      bunny_farm:respond(Response, ReplyTo, BusHandle),
      {noreply, NewState};
    _ ->
      handle_cast({Key,Payload}, State)
  end.


terminate(Reason, State) ->
  Handles = qcache:connections(State#gen_qstate.cache_pid),
  Module = State#gen_qstate.module,
  ModuleState = State#gen_qstate.module_state,
  Module:terminate(Reason, ModuleState),
  Fn = fun(PList) ->
    bunny_farm:close(?PV(handle,PList), ?PV(tag,PList))
  end,
  lists:map(Fn, Handles),
  ok.

code_change(_OldVersion, State, _Extra) ->
  {ok, State}.


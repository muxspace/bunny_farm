-module(gen_qserver).
-behaviour(gen_server).
-include("bunny_farm.hrl").
-export([behaviour_info/1]).
-export([start_link/4, start_link/5, init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2, code_change/3]).
-export([call/2, call/3, cast/2]).
-export([get_bus/2 ]).

-record(state, {module, module_state, handles}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% PUBLIC %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

behaviour_info(callbacks) ->
  [{init,1},
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

get_bus(ServerRef, Exchange) ->
  gen_server:call(ServerRef, {get_bus, Exchange}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% PRIVATE %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
tag() ->
  In = float_to_list(element(3,now()) + random:uniform()),
  base64:encode(In).

bus({tag,Tag}, State) ->
  lists:keyfind(Tag,2,State#state.handles);

bus({exchange,Exchange}, State) ->
  lists:keyfind(Exchange,1,State#state.handles).


%% Consume
-spec connect({exchange(), routing_key()}) -> [ {exchange(), boolean(), bus_handle()} ].
connect({<<Exchange/binary>>, <<Key/binary>>}) ->
  %error_logger:info_msg("[gen_qserver] Opening ~p => ~p~n", [Exchange,Key]),
  Handle = bunny_farm:open(Exchange,Key),
  Tag = tag(),
  bunny_farm:consume(Handle, [{consumer_tag,Tag}]),
  %error_logger:info_msg("[gen_qserver] Returning handle spec"),
  {Exchange, Tag, false, Handle};

%% Consume
connect({Exchange, Key}) ->
  connect({farm_tools:binarize(Exchange), farm_tools:binarize(Key)});

%% Publish
connect(<<Exchange/binary>>) ->
  %error_logger:info_msg("[gen_qserver] Opening ~p~n", [Exchange]),
  Handle = bunny_farm:open(Exchange),
  %error_logger:info_msg("[gen_qserver] Returning handle spec"),
  {Exchange, <<"">>, true, Handle};

%% Publish
connect(Exchange) ->
  connect(farm_tools:binarize(Exchange)).

activate_bus({tag,Tag}, Handles) ->
  case lists:keyfind(Tag,2, Handles) of
    %% TODO Some sort of error needs to be thrown if the Tag doesn't exist
    false -> Handles;
    {Exchange,Tag,_,Bus} ->
      lists:keystore(Tag,2,Handles,{Exchange,Tag,true,Bus})
  end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% GEN_SERVER %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

init([Module, Args, ConnSpecs]) ->
  random:seed(now()),
  case Module:init(Args) of
    {ok, ModuleState} ->
      Handles = lists:map(fun(Conn) -> connect(Conn) end, ConnSpecs),
      State = #state{module=Module, module_state=ModuleState, handles=Handles},
      Response = {ok, State};
    {ok, ModuleState, Timeout} ->
      Handles = lists:map(fun(Conn) -> connect(Conn) end, ConnSpecs),
      State = #state{module=Module, module_state=ModuleState, handles=Handles},
      Response = {ok, State, Timeout};
    {stop, Reason} ->
      Response = {stop, Reason}
  end,
  Response.

handle_call({get_bus,Exchange}, _From, State) ->
  BusHandle = bus({exchange,Exchange}, State),
  {reply, BusHandle, State};

handle_call(Request, From, State) ->
  Module = State#state.module,
  ModuleState = State#state.module_state,
  {reply, Response, NextState} = Module:handle_call(Request,From,ModuleState),
  {reply, Response, State#state{module_state=NextState}}.


handle_cast(Request, State) ->
  Module = State#state.module,
  ModuleState = State#state.module_state,
  {noreply, NextState} = Module:handle_cast(Request,ModuleState),
  {noreply, State#state{module_state=NextState}}.



%% Tags are auto-generated during subscription
handle_info(#'basic.consume_ok'{consumer_tag=Tag}, State) ->
  %error_logger:info_msg("Connection ACK~n",[]),
  Handles = activate_bus({tag,Tag}, State#state.handles),
  {noreply, State#state{handles=Handles}};

% Handle messages coming off the bus
handle_info({#'basic.deliver'{routing_key=Key,exchange=OX}, Content}, State) ->
  Payload = farm_tools:decode_payload(Content),
  case farm_tools:is_rpc(Content) of
    true -> 
      {reply,Response,State} = handle_call({Key, Payload}, self(), State),
      {X,ReplyTo} = farm_tools:reply_to(Content, OX),
      case bus({exchange,X}, State) of
        false ->
          Conn = connect(X),
          BusHandle = element(4,Conn),
          Handles = [Conn | State#state.handles];
        Conn ->
          BusHandle = element(4,Conn),
          Handles = State#state.handles
      end,
          
      %error_logger:info_msg("[gen_qserver] Responding to ~p => ~p~n", [X,ReplyTo]),
      bunny_farm:respond(Response, ReplyTo, BusHandle),
      %error_logger:info_msg("[gen_qserver] Sent"),
      {noreply, State#state{handles=Handles}};
    _ ->
      handle_cast(Payload, State)
  end.


terminate(Reason, State) ->
  Module = State#state.module,
  ModuleState = State#state.module_state,
  Module:terminate(Reason, ModuleState),
  Fn = fun({_,Tag,_,#bus_handle{}=BusHandle}) ->
    bunny_farm:close(BusHandle, Tag)
  end,
  lists:map(Fn, State#state.handles),
  ok.

code_change(_OldVersion, State, _Extra) ->
  {ok, State}.


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

-module(bunny_farm).
-include("bunny_farm.hrl").
-include("private_macros.hrl").
-compile([{parse_transform,lager_transform}]).
-export([open/1, open/2, close/1, close/2]).
-export([declare_exchange/1, declare_exchange/2,
  declare_queue/1, declare_queue/2, declare_queue/3,
  bind/3]).
-export([consume/1, consume/2,
  publish/3,
  rpc/3, rpc/4, respond/3]).
-ifdef(TEST).
-compile(export_all).
-endif.

%% Convenience function for opening a connection for publishing
%% messages. The routing key can be included but if it is not,
%% then the connection can be re-used for multiple routing keys
%% on the same exchange.
%% Example
%%   BusHandle = bunny_farm:open(<<"my.exchange">>),
%%   bunny_farm:publish(Message, K,BusHandle),
open(MaybeTuple) ->
  {X,XO} = resolve_options(exchange, MaybeTuple),
  BusHandle = open_it(#bus_handle{exchange=X, options=XO}),
  bunny_farm:declare_exchange(BusHandle),
  BusHandle.

%% Convenience function to open and declare all intermediate objects. This
%% is the typical pattern for consuming messages from a topic exchange.
%% @returns bus_handle
%% Example
%%   BusHandle = bunny_farm:open(X, K),
%%   bunny_farm:consume(BusHandle),
open(MaybeX, MaybeK) ->
  {X,XO} = resolve_options(exchange, MaybeX),
  {K,KO} = resolve_options(queue, MaybeK),

  BusHandle = open_it(#bus_handle{exchange=X, options=XO}),
  bunny_farm:declare_exchange(BusHandle),
  case X of
    <<"">> -> Q = bunny_farm:declare_queue(K, BusHandle, KO);
    _ -> Q = bunny_farm:declare_queue(BusHandle, KO)
  end,
  bunny_farm:bind(Q, K, BusHandle).


close(#bus_handle{channel=Channel, conn=Connection}) ->
  amqp_channel:close(Channel),
  amqp_connection:close(Connection).

close(#bus_handle{}=BusHandle, <<"">>) ->
  close(BusHandle);

close(#bus_handle{channel=Channel}=BusHandle, Tag) ->
  #'basic.cancel_ok'{} =
    amqp_channel:call(Channel, #'basic.cancel'{consumer_tag=Tag}),
  close(BusHandle).



consume(#bus_handle{}=BusHandle) ->
  consume(BusHandle, []).

consume(#bus_handle{queue=Q,channel=Channel}, Options) when is_list(Options) ->
  AllOptions = [{queue,Q}, {no_ack,true}] ++ Options,
  BasicConsume = farm_tools:to_basic_consume(AllOptions),
  lager:debug("Sending subscription request:~n  ~p", [BasicConsume]),
  %error_logger:info_msg("Sending subscription request:~n  ~p~n",[BasicConsume]),
  amqp_channel:subscribe(Channel, BasicConsume, self()).


%publish(#message{}=Message, #bus_handle{}=BusHandle) ->
%  publish(Message, BusHandle#bus_handle.routing_key, BusHandle);

%publish(Payload, #bus_handle{}=BusHandle) ->
%  publish(#message{payload=Payload}, BusHandle).

%% This is the recommended call to use as the same exchange can be reused
publish(#message{payload=Payload, props=Props}, K,
        #bus_handle{exchange=X, channel=Channel, options=Options}) ->
  MimeType = case farm_tools:content_type(Props) of
    undefined -> farm_tools:encoding(Options);
    M -> M
  end,
  EncPayload = farm_tools:encode_payload(MimeType, Payload),
  %lager:debug("Publish:~n  ~p", [EncPayload]),
  ContentType = {content_type,MimeType},
  AProps = farm_tools:to_amqp_props(lists:merge([ContentType], Props)),
  AMsg = #amqp_msg{payload=EncPayload, props=AProps},
  BasicPublish = #'basic.publish'{exchange=X, routing_key=K},
  amqp_channel:cast(Channel, BasicPublish, AMsg);

publish(Payload, RoutingKey, #bus_handle{}=BusHandle) ->
  publish(#message{payload=Payload}, RoutingKey, BusHandle).



rpc(#message{payload=Payload, props=Props}, K,
    #bus_handle{exchange=X, channel=Channel, options=Options}) ->
  MimeType = case farm_tools:content_type(Props) of
    undefined -> proplists:get_value(encoding, Options);
    M -> M
  end,
  ContentType = {content_type,MimeType},
  AProps = farm_tools:to_amqp_props(lists:merge([ContentType], Props)),
  AMsg = #amqp_msg{payload=farm_tools:encode_payload(MimeType,Payload),
                   props=AProps},
  BasicPublish = #'basic.publish'{exchange=X, routing_key=K},
  amqp_channel:cast(Channel, BasicPublish, AMsg).

rpc(Payload, ReplyTo, K, BusHandle) ->
  Props = [{reply_to,ReplyTo}, {correlation_id,ReplyTo}],
  rpc(#message{payload=Payload, props=Props}, K, BusHandle).


%% This is used to send the response of an RPC. The primary difference
%% between this and publish is that the data is retained as an erlang
%% binary.
respond(#message{payload=Payload, props=Props}, RoutingKey,
        #bus_handle{exchange=X,channel=Channel}) ->
  MimeType = farm_tools:content_type(Props),
  AMsg = #amqp_msg{payload=farm_tools:encode_payload(MimeType,Payload),
                   props=farm_tools:to_amqp_props(Props)},
  BasicPublish = #'basic.publish'{exchange=X, routing_key=RoutingKey},
  error_logger:info_msg("Responding to ~p~n", [BasicPublish]),
  amqp_channel:cast(Channel, BasicPublish, AMsg);

respond(Payload, RoutingKey, #bus_handle{}=BusHandle) ->
  respond(#message{payload=Payload}, RoutingKey, BusHandle).



%% This is special handling for the default exchange. This exchange cannot be
%% explicitly declared so it just returns.
declare_exchange(#bus_handle{exchange= <<"">>}) -> ok;

%% Type - The exchange type (e.g. <<"topic">>)
declare_exchange(#bus_handle{exchange=Key, channel=Channel, options=Options}) ->
  AllOptions = lists:merge([{exchange,Key}], Options),
  ExchDeclare = farm_tools:to_exchange_declare(AllOptions),
  lager:debug("Declaring exchange: ~p", [ExchDeclare]),
  #'exchange.declare_ok'{} = amqp_channel:call(Channel, ExchDeclare),
  ok.

declare_exchange(<<"">>, #bus_handle{}) -> ok;

declare_exchange(Key, #bus_handle{}=BusHandle) ->
  declare_exchange(BusHandle#bus_handle{exchange=Key}).

%% http://www.rabbitmq.com/amqp-0-9-1-quickref.html
%% Use configured options for the queue. Since no routing key is specified,
%% attempt to read options for the routing key <<"">>.
declare_queue(#bus_handle{}=BusHandle) ->
  declare_queue(BusHandle, queue_options(<<"">>)).

%% Options - Tuple list of k,v options
declare_queue(#bus_handle{}=BusHandle, Options) when is_list(Options) ->
  declare_queue(<<"">>, BusHandle, Options).

declare_queue(Key, #bus_handle{channel=Channel}, Options) ->
  AllOptions = lists:keymerge(1, lists:sort(Options), [{queue,Key}]),
  QueueDeclare = farm_tools:to_queue_declare(AllOptions),
  #'queue.declare_ok'{queue=Q} = amqp_channel:call(Channel, QueueDeclare),
  Q.



bind(Q, _BindKey, #bus_handle{exchange= <<"">>}=BusHandle) ->
  BusHandle#bus_handle{queue=Q};

bind(Q, BindKey, #bus_handle{exchange=X, channel=Channel}=BusHandle) ->
  QueueBind = #'queue.bind'{exchange=X, queue=Q, routing_key=BindKey},
  #'queue.bind_ok'{} = amqp_channel:call(Channel, QueueBind),
  BusHandle#bus_handle{queue=Q}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% PRIVATE %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
open_it(#bus_handle{}=BusHandle) ->
  Keys = [amqp_username, amqp_password,amqp_virtual_host],
  {H,R} = get_server(),
  lager:debug("Opening connection to ~p:~p", [H,R]),
  lager:debug("Calling pid is ~p", [self()]),
  lager:debug("Calling application is ~p", [application:get_application()]),
  [U,P,V] = lists:map(fun get_env/1, Keys),
  Params = #amqp_params{username=U, password=P, virtual_host=V,
             host=H, port=R},
  open_it(network, Params, BusHandle).

open_it(Method, #amqp_params{}=Params, #bus_handle{}=BusHandle) ->
  {ok,Connection} = amqp_connection:start(Method, Params),
  {ok,Channel} = amqp_connection:open_channel(Connection),
  BusHandle#bus_handle{channel=Channel, conn=Connection}.


default(Key) ->
    Defaults = [{amqp_username, <<"guest">>},
                {amqp_password, <<"guest">>},
                {amqp_virtual_host, <<"/">>},
                {amqp_servers, []}, % Format is {host,port}
                {amqp_host, "localhost"},
                {amqp_port, 5672},
                {amqp_encoding, <<"application/x-erlang">>},
                {amqp_exchanges, []},
                {amqp_queues, []}],

    %% Note(superbobry): try fetching the value from 'bunny_farm'
    %% environment first, this might be useful for example when all
    %% applications use a single RabbitMQ instance.
    case application:get_env(bunny_farm, Key) of
        {ok, Value} -> Value;
        undefined   ->
            proplists:get_value(Key, Defaults)
    end.

get_env(Key) ->
  Default = default(Key),
  case application:get_env(Key) of
    undefined -> Default;
    {ok, H} -> H
  end.

%% If amqp_servers is defined, use that. Otherwise fall back to amqp_host and
%% amqp_port
get_server() ->
  case get_env(amqp_servers) of
    [] ->
      {get_env(amqp_host), get_env(amqp_port)};
    Servers ->
      Idx = random:uniform(length(Servers)),
      lists:nth(Idx, Servers)
  end.

%% Define defaults that override rabbitmq defaults
exchange_defaults() ->
  Encoding = get_env(amqp_encoding),
  lists:sort([ {encoding, Encoding}, {type,<<"topic">>} ]).

%% Get the proplist for a given channel
exchange_options(X) ->
  Channels = get_env(amqp_exchanges),
  ChannelList = [ List || {K, List} <- Channels, K == X ],
  case ChannelList of
    [] -> [];
    [Channel] -> lists:sort(Channel)
  end.

%% Define defaults that override rabbitmq defaults
queue_defaults() ->
  lists:sort([ {exclusive,true} ]).

queue_options(X) ->
  Channels = get_env(amqp_queues),
  ChannelList = [ List || {K, List} <- Channels, K == X ],
  case ChannelList of
    [] -> [];
    [Channel] -> lists:sort(Channel)
  end.

%% Decouple the exchange and options. If no options exist, then use defaults.
resolve_options(exchange, MaybeTuple) ->
  Defaults = exchange_defaults(),
  case MaybeTuple of
    {X,O} -> Os = lists:merge([lists:sort(O),exchange_options(X),Defaults]);
    X -> Os = lists:merge([exchange_options(X),Defaults])
  end,
  {X,Os};

resolve_options(queue, MaybeTuple) ->
  Defaults = queue_defaults(),
  case MaybeTuple of
    {K,O} -> Os = lists:merge([lists:sort(O),queue_options(K),Defaults]);
    K -> Os = lists:merge(queue_options(K),Defaults)
  end,
  {K,Os}.

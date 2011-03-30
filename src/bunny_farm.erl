-module(bunny_farm).
-include("bunny_farm.hrl").
-export([open/0, open/1, open/2, open/3, close/1]).
-export([declare_exchange/2, declare_exchange/3,
  declare_queue/1, declare_queue/2, declare_queue/3,
  bind/4]).
-export([consume/1, publish/2, publish/3, rpc/3, respond/3]).

open() -> open(#bus_handle{}).

open(BusHandle) when is_record(BusHandle,bus_handle) ->
  open(network, #amqp_params{}, BusHandle);

%% Convenience function for opening a connection for publishing 
%% messages. The routing key can be included but if it is not,
%% then the connection can be re-used for multiple routing keys
%% on the same exchange.
%% Example
%%   BusHandle = bunny_farm:open(<<"my.exchange">>),
%%   bunny_farm:publish(Message, K,BusHandle),
open(X) ->
  BusHandle = bunny_farm:open(#bus_handle{exchange=X}),
  bunny_farm:declare_exchange(<<"topic">>, BusHandle),
  BusHandle.

open(Method, Params, BusHandle) when
    is_record(Params,amqp_params), is_record(BusHandle,bus_handle) ->
  {ok,Connection} = amqp_connection:start(Method, #amqp_params{}),
  {ok,Channel} = amqp_connection:open_channel(Connection),
  BusHandle#bus_handle{channel=Channel, conn=Connection};

%% Convenience function to open and declare all intermediate objects. This
%% is the typical pattern for consuming messages from a topic exchange.
%% @returns bus_handle
%% Example
%%   BusHandle = bunny_farm:open(X, K),
%%   bunny_farm:consume(BusHandle),
open(X, K, Options) when is_list(Options) ->
  BusHandle0 = bunny_farm:open(#bus_handle{exchange=X}),
  bunny_farm:declare_exchange(<<"topic">>, X, BusHandle0),
  Q = bunny_farm:declare_queue(BusHandle0, Options),
  bunny_farm:bind(X,Q, K, BusHandle0).

open(X, K) -> open(X, K, [{exclusive,true}]).

close(#bus_handle{channel=Channel, conn=Connection}) ->
  amqp_channel:close(Channel),
  amqp_connection:close(Connection).


%% Type - The exchange type (e.g. <<"topic">>)
declare_exchange(Type, #bus_handle{exchange=Key, channel=Channel}) ->
  ExchDeclare = #'exchange.declare'{exchange=Key, type=Type},
  #'exchange.declare_ok'{} = amqp_channel:call(Channel, ExchDeclare),
  ok.

declare_exchange(Type, Key, #bus_handle{channel=Channel}) ->
  ExchDeclare = #'exchange.declare'{exchange=Key, type=Type},
  #'exchange.declare_ok'{} = amqp_channel:call(Channel, ExchDeclare),
  ok.

%% Options - Tuple list of k,v options
%% http://www.rabbitmq.com/amqp-0-9-1-quickref.html
declare_queue(BusHandle) when is_record(BusHandle,bus_handle) ->
  declare_queue(BusHandle, []).

declare_queue(BusHandle, Options) when is_record(BusHandle,bus_handle) ->
  declare_queue(<<"">>, BusHandle, Options).

declare_queue(Key, #bus_handle{channel=Channel}, Options) ->
  AllOptions = [{queue,Key}, {ticket,0}, {arguments,[]}] ++ Options,
  QueueDeclare = farm_tools:to_queue_declare(AllOptions),
  #'queue.declare_ok'{queue=Q, message_count=_OrderCount,
      consumer_count=_ConsumerCount} = amqp_channel:call(Channel, QueueDeclare),
  Q.


  
bind(X, Q, BindKey, BusHandle) when is_record(BusHandle,bus_handle) ->
  Channel = BusHandle#bus_handle.channel,
  QueueBind = #'queue.bind'{exchange=X, queue=Q, routing_key=BindKey},
  #'queue.bind_ok'{} = amqp_channel:call(Channel, QueueBind),
  BusHandle#bus_handle{queue=Q}.


consume(#bus_handle{queue=Q, channel=Channel}) ->
  BasicConsume = #'basic.consume'{queue=Q, no_ack=true},
  Msg = "[bunny_farm] Sending subscription request: ~p~n",
  error_logger:info_msg(Msg, [BasicConsume]),
  amqp_channel:subscribe(Channel, BasicConsume, self()).


publish(#message{}=Message,
        #bus_handle{exchange=X, routing_key=K, channel=Channel}) ->
  Payload = Message#message.payload,
  Props = Message#message.props,
  AMsg = #amqp_msg{payload=farm_tools:encode_payload(Payload),
                   props=farm_tools:to_amqp_props(Props)},
  BasicPublish = #'basic.publish'{exchange=X, routing_key=K}, 
  amqp_channel:cast(Channel, BasicPublish, AMsg);

publish(Payload, #bus_handle{}=BusHandle) ->
  publish(#message{payload=Payload}, BusHandle).

%% This is the recommended call to use as the same exchange can be reused
publish(#message{}=Message, RoutingKey,
        #bus_handle{exchange=X, channel=Channel}) ->
  Payload = Message#message.payload,
  Props = Message#message.props,
  AMsg = #amqp_msg{payload=farm_tools:encode_payload(Payload),
                   props=farm_tools:to_amqp_props(Props)},
  BasicPublish = #'basic.publish'{exchange=X, routing_key=RoutingKey}, 
  amqp_channel:cast(Channel, BasicPublish, AMsg);

publish(Payload, RoutingKey, #bus_handle{}=BusHandle) ->
  publish(#message{payload=Payload}, RoutingKey, BusHandle).


%% Make an RPC call. The reply_to and correlation_ids are both set to ReplyTo
rpc({Procedure,Arguments}, ReplyTo, BusHandle) ->
  rpc(#rpc{procedure=Procedure, args=Arguments}, ReplyTo, BusHandle);

rpc(#rpc{}=RPC, ReplyTo, #bus_handle{exchange=X, routing_key=K, channel=Channel}) ->
  Props = [{reply_to,ReplyTo}, {correlation_id,ReplyTo}],
  AMsg = #amqp_msg{payload=farm_tools:encode_payload(erlang,RPC),
                   props=farm_tools:to_amqp_props(Props)},
  BasicPublish = #'basic.publish'{exchange=X, routing_key=K}, 
  amqp_channel:cast(Channel, BasicPublish, AMsg);

%% This ts the recommended way to call this function
rpc(#message{payload=#rpc{}}=Message, K,
    #bus_handle{exchange=X,channel=Channel}) ->
  Payload = Message#message.payload,
  Props = Message#message.props,
  AMsg = #amqp_msg{payload=farm_tools:encode_payload(erlang,Payload),
                   props=farm_tools:to_amqp_props(Props)},
  BasicPublish = #'basic.publish'{exchange=X, routing_key=K}, 
  amqp_channel:cast(Channel, BasicPublish, AMsg).

%% This is used to send the response of an RPC. The primary difference 
%% between this and publish is that the data is retained as an erlang
%% binary.
respond(#message{payload=Payload, props=Props}, RoutingKey,
        #bus_handle{exchange=X,channel=Channel}) ->
  AMsg = #amqp_msg{payload=farm_tools:encode_payload(erlang,Payload),
                   props=farm_tools:to_amqp_props(Props)},
  BasicPublish = #'basic.publish'{exchange=X, routing_key=RoutingKey}, 
  amqp_channel:cast(Channel, BasicPublish, AMsg);

respond(Payload, RoutingKey, #bus_handle{}=BusHandle) ->
  respond(#message{payload=Payload}, RoutingKey, BusHandle).

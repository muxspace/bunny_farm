-module(bunny_farm).
-include("bunny_farm.hrl").
-export([open/0, open/1, open/3, close/1]).
-export([declare_exchange/2, declare_exchange/3,
  declare_queue/1, declare_queue/2,
  bind/4]).
-export([consume/1, publish/3]).

open() -> open(#bus_handle{}).

open(BusHandle) when is_record(BusHandle,bus_handle) ->
  open(network, #amqp_params{}, BusHandle).

open(Method, Params, BusHandle) when
    is_record(Params,amqp_params), is_record(BusHandle,bus_handle) ->
  {ok,Connection} = amqp_connection:start(Method, #amqp_params{}),
  {ok,Channel} = amqp_connection:open_channel(Connection),
  BusHandle#bus_handle{channel=Channel, conn=Connection}.

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

declare_queue(#bus_handle{channel=Channel}) ->
  QueueDeclare = #'queue.declare'{},
  #'queue.declare_ok'{queue=Q,
    message_count=_OrderCount,
    consumer_count=_ConsumerCount} = amqp_channel:call(Channel, QueueDeclare),
  Q.

declare_queue(Key, #bus_handle{channel=Channel}) ->
  QueueDeclare = #'queue.declare'{queue=Key},
  #'queue.declare_ok'{queue=Q,
    message_count=_OrderCount,
    consumer_count=_ConsumerCount} = amqp_channel:call(Channel, QueueDeclare),
  Q.

bind(X, Q, BindKey, #bus_handle{channel=Channel}) ->
  QueueBind = #'queue.bind'{exchange=X, queue=Q, routing_key=BindKey},
  #'queue.bind_ok'{} = amqp_channel:call(Channel, QueueBind).

consume(#bus_handle{queue=Q, channel=Channel}) ->
  BasicConsume = #'basic.consume'{queue=Q, no_ack=true},
  io:format("[bunny_farm] Sending subscription request: ~p~n", [BasicConsume]),
  Tag = amqp_channel:subscribe(Channel, BasicConsume, self()),
  Tag.

publish(Payload, RoutingKey, #bus_handle{exchange=X, channel=Channel}) ->
  BasicPublish = #'basic.publish'{exchange=X, routing_key=RoutingKey}, 
  amqp_channel:cast(Channel, BasicPublish, #amqp_msg{payload=Payload}).


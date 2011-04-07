-module(bunny_farm).
-include("bunny_farm.hrl").
-include("private_macros.hrl").
-export([open/1, open/2, open/3, close/1, close/2]).
-export([declare_exchange/2, declare_exchange/3,
  declare_queue/1, declare_queue/2, declare_queue/3,
  bind/4]).
-export([consume/1, consume/2, publish/2, publish/3, 
  rpc/3, rpc/4, respond/3]).

%% Convenience function for opening a connection for publishing 
%% messages. The routing key can be included but if it is not,
%% then the connection can be re-used for multiple routing keys
%% on the same exchange.
%% Example
%%   BusHandle = bunny_farm:open(<<"my.exchange">>),
%%   bunny_farm:publish(Message, K,BusHandle),
open(X) ->
  BusHandle = open_it(#bus_handle{exchange=X}),
  bunny_farm:declare_exchange(<<"topic">>, BusHandle),
  BusHandle.

%% Convenience function to open and declare all intermediate objects. This
%% is the typical pattern for consuming messages from a topic exchange.
%% @returns bus_handle
%% Example
%%   BusHandle = bunny_farm:open(X, K),
%%   bunny_farm:consume(BusHandle),
open(X, K) -> open(X, K, []).

open(X, Key, Options) when is_list(Options) ->
  Defaults = [{exclusive,true}],
  Fn = fun({K,V},Acc) -> lists:keystore(K,1,Acc,{K,V}) end,
  AllOptions = lists:foldl(Fn, Defaults, Options),

  BusHandle = open_it(#bus_handle{exchange=X}),
  bunny_farm:declare_exchange(<<"topic">>, BusHandle),
  Q = bunny_farm:declare_queue(BusHandle, AllOptions),
  bunny_farm:bind(X,Q, Key, BusHandle).


close(#bus_handle{channel=Channel, conn=Connection}) ->
  amqp_channel:close(Channel),
  amqp_connection:close(Connection).

close(#bus_handle{}=BusHandle, <<"">>) ->
  close(BusHandle);

close(#bus_handle{channel=Channel, conn=Connection}, Tag) ->
  #'basic.cancel_ok'{} =
    amqp_channel:call(Channel, #'basic.cancel'{consumer_tag=Tag}),
  ok = amqp_channel:close(Channel),
  ok = amqp_connection:close(Connection),
  ok.



consume(#bus_handle{}=BusHandle) ->
  consume(BusHandle, []).

consume(#bus_handle{queue=Q,channel=Channel}, Options) when is_list(Options) ->
  AllOptions = [{queue,Q}, {no_ack,true}] ++ Options,
  BasicConsume = farm_tools:to_basic_consume(AllOptions),
  ?info("Sending subscription request:~n  ~p", [BasicConsume]),
  amqp_channel:subscribe(Channel, BasicConsume, self()).


publish(#message{}=Message, #bus_handle{}=BusHandle) ->
  publish(Message, BusHandle#bus_handle.routing_key, BusHandle);

publish(Payload, #bus_handle{}=BusHandle) ->
  publish(#message{payload=Payload}, BusHandle).

%% This is the recommended call to use as the same exchange can be reused
publish(#message{payload=Payload, props=Props, encoding=Encoding},
        RoutingKey, #bus_handle{exchange=X, channel=Channel}) ->
  AMsg = #amqp_msg{payload=farm_tools:encode_payload(Encoding, Payload),
                   props=farm_tools:to_amqp_props(Props)},
  BasicPublish = #'basic.publish'{exchange=X, routing_key=RoutingKey}, 
  amqp_channel:cast(Channel, BasicPublish, AMsg);

publish(Payload, RoutingKey, #bus_handle{}=BusHandle) ->
  publish(#message{payload=Payload}, RoutingKey, BusHandle).



rpc(#message{payload=Payload, props=Props}, K,
    #bus_handle{exchange=X, channel=Channel}) ->
  AMsg = #amqp_msg{payload=farm_tools:encode_payload(erlang,Payload),
                   props=farm_tools:to_amqp_props(Props)},
  BasicPublish = #'basic.publish'{exchange=X, routing_key=K}, 
  amqp_channel:cast(Channel, BasicPublish, AMsg).

rpc(Payload, ReplyTo, K, #bus_handle{exchange=X, channel=Channel}) ->
  Props = [{reply_to,ReplyTo}, {correlation_id,ReplyTo}],
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
declare_queue(#bus_handle{}=BusHandle) ->
  declare_queue(BusHandle, []).

declare_queue(#bus_handle{}=BusHandle, Options) ->
  declare_queue(<<"">>, BusHandle, Options).

declare_queue(Key, #bus_handle{channel=Channel}, Options) ->
  Defaults = [{queue,Key}, {ticket,0}, {arguments,[]}],
  Fn = fun({K,V},Acc) -> lists:keystore(K,1,Acc,{K,V}) end,
  AllOptions = lists:foldl(Fn, Defaults, Options),
  QueueDeclare = farm_tools:to_queue_declare(AllOptions),
  #'queue.declare_ok'{queue=Q, message_count=_OrderCount,
      consumer_count=_ConsumerCount} = amqp_channel:call(Channel, QueueDeclare),
  Q.


  
bind(X, Q, BindKey, BusHandle) when is_record(BusHandle,bus_handle) ->
  Channel = BusHandle#bus_handle.channel,
  QueueBind = #'queue.bind'{exchange=X, queue=Q, routing_key=BindKey},
  #'queue.bind_ok'{} = amqp_channel:call(Channel, QueueBind),
  BusHandle#bus_handle{queue=Q}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% PRIVATE %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
open_it(#bus_handle{}=BusHandle) ->
  open_it(network, #amqp_params{}, BusHandle).

open_it(Method, #amqp_params{}=Params, #bus_handle{}=BusHandle) ->
  {ok,Connection} = amqp_connection:start(Method, Params),
  {ok,Channel} = amqp_connection:open_channel(Connection),
  BusHandle#bus_handle{channel=Channel, conn=Connection}.


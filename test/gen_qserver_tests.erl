-module(gen_qserver_tests).
-include("bunny_farm.hrl").
-include_lib("eunit/include/eunit.hrl").

main_test_() ->
  %application:start(sasl),
  {setup,
    fun setup/0,
    fun cleanup/1,
    [ fun init_value/0,
      fun set_value_normal/0,
      fun get_value_queue/0,
      fun set_value_queue/0,
      fun explicit_encoding/0,
      fun default_reply_exchange/0,
      fun explicit_reply_exchange/0,
      fun conn_spec_override/0,
      fun get_connection/0
    ]
  }.

setup() ->
  {ok,Pid} = my_qserver:start_link([{key3,3}]),
  Pid.

cleanup(Pid) ->
  my_qserver:stop(Pid),
  ok.

init_value() ->
  Act = my_qserver:get_value(key3),
  ?assertEqual(3, Act).

set_value_normal() ->
  my_qserver:set_value(key1, foo),
  Act = my_qserver:get_value(key1),
  ?assertEqual(foo, Act).

get_value_queue() ->
  K = <<"get_value_queue">>,
  PubBus = bunny_farm:open(<<"qserver.two">>),
  SubBus = bunny_farm:open(<<"qserver.sub">>, K),
  bunny_farm:consume(SubBus),
  receive
    #'basic.consume_ok'{consumer_tag=ConsumerTag} -> ok
  end,

  ReplyTo = <<"qserver.sub:get_value_queue">>,
  bunny_farm:rpc({get_value, key3}, ReplyTo, <<"key">>, PubBus),

  error_logger:info_msg("[gen_qserver_tests] Waiting for response~n"),
  receive
    {#'basic.deliver'{}, Content} ->
      Act = farm_tools:decode_payload(Content)
  end,

  bunny_farm:close(SubBus, ConsumerTag),
  bunny_farm:close(PubBus),
  ?assertEqual(3, Act).


set_value_queue() ->
  error_logger:info_msg("[set_value_queue] Opening connections~n"),
  K = <<"set_value_queue">>,
  PubBus = bunny_farm:open(<<"qserver.two">>),
  SubBus = bunny_farm:open(<<"qserver.sub">>, K),
  error_logger:info_msg("[set_value_queue] Consuming <<qserver.sub>>~n"),
  bunny_farm:consume(SubBus),
  receive
    #'basic.consume_ok'{consumer_tag=ConsumerTag} -> ok
  end,

  error_logger:info_msg("[set_value_queue] Sending set_value"),
  Message = #message{payload={set_value, key5, 5}, encoding=erlang},
  bunny_farm:publish(Message, <<"key">>, PubBus),

  error_logger:info_msg("[set_value_queue] Calling RPC to get_value"),
  ReplyTo = <<"qserver.sub:set_value_queue">>,
  bunny_farm:rpc({get_value, key5}, ReplyTo, <<"key">>, PubBus),

  error_logger:info_msg("[set_value_queue] Waiting for response~n"),
  receive
    {#'basic.deliver'{}, Content} ->
      Act = farm_tools:decode_payload(Content)
  end,

  bunny_farm:close(SubBus, ConsumerTag),
  bunny_farm:close(PubBus),
  ?assertEqual(5, Act).


explicit_encoding() ->
  K = <<"explicit_encoding">>,
  PubBus = bunny_farm:open(<<"qserver.two">>),
  SubBus = bunny_farm:open(<<"">>, K),
  bunny_farm:consume(SubBus),
  receive
    #'basic.consume_ok'{consumer_tag=ConsumerTag} -> ok
  end,

  ReplyTo = <<"explicit_encoding">>,
  BsonBus = farm_tools:content_type(<<"application/bson">>, PubBus),
  bunny_farm:rpc({get_value, key3}, ReplyTo, <<"key">>, BsonBus),

  error_logger:info_msg("[explicit_encoding] Waiting for response~n"),
  receive
    {#'basic.deliver'{}, Content} ->
      Act = farm_tools:decode_payload(Content)
  end,

  bunny_farm:close(SubBus, ConsumerTag),
  bunny_farm:close(PubBus),
  ?assertEqual(3, Act).


default_reply_exchange() ->
  K = <<"default_reply_exchange">>,
  PubBus = bunny_farm:open(<<"qserver.two">>),
  SubBus = bunny_farm:open(<<"">>, K),
  bunny_farm:consume(SubBus),
  receive
    #'basic.consume_ok'{consumer_tag=ConsumerTag} -> ok
  end,

  ReplyTo = <<"default_reply_exchange">>,
  bunny_farm:rpc({get_value, key3}, ReplyTo, <<"key">>, PubBus),

  error_logger:info_msg("[default_reply_exchange] Waiting for response~n"),
  receive
    {#'basic.deliver'{}, Content} ->
      Act = farm_tools:decode_payload(Content)
  end,

  bunny_farm:close(SubBus, ConsumerTag),
  bunny_farm:close(PubBus),
  ?assertEqual(3, Act).


explicit_reply_exchange() ->
  K = <<"explicit_reply_exchange">>,
  PubBus = bunny_farm:open(<<"qserver.two">>),
  SubBus = bunny_farm:open(<<"qserver.sub">>, K),
  bunny_farm:consume(SubBus),
  receive
    #'basic.consume_ok'{consumer_tag=ConsumerTag} -> ok
  end,

  ReplyTo = <<"qserver.sub:explicit_reply_exchange">>,
  bunny_farm:rpc({get_value, key3}, ReplyTo, <<"key">>, PubBus),

  error_logger:info_msg("[explicit_reply_exchange] Waiting for response~n"),
  receive
    {#'basic.deliver'{}, Content} ->
      Act = farm_tools:decode_payload(Content)
  end,

  bunny_farm:close(SubBus, ConsumerTag),
  bunny_farm:close(PubBus),
  ?assertEqual(3, Act).


conn_spec_override() ->
  Conn = my_qserver:get_connection(<<"qserver.three">>),
  Handle = proplists:get_value(handle,Conn),
  ?assertEqual(<<"qserver.three">>, proplists:get_value(id,Conn)),
  ?assertEqual(bus_handle, element(1,Handle)),
  Encoding = proplists:get_value(encoding,Handle#bus_handle.options),
  ?assertEqual(<<"application/bson">>, Encoding).


get_connection() ->
  Conn = my_qserver:get_connection(),
  Handle = proplists:get_value(handle,Conn),
  ?assertEqual({<<"qserver.two">>,<<"key">>}, proplists:get_value(id,Conn)),
  ?assertEqual(bus_handle, element(1,Handle)).


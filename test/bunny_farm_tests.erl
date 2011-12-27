-module(bunny_farm_tests).
-include("bunny_farm.hrl").
-include_lib("eunit/include/eunit.hrl").

get_env_u_test() ->
  <<"guest">> = bunny_farm:get_env(amqp_username).

get_env_p_test() ->
  <<"guest">> = bunny_farm:get_env(amqp_password).

get_env_v_test() ->
  <<"/">> = bunny_farm:get_env(amqp_virtual_host).

get_env_h_test() ->
  "localhost" = bunny_farm:get_env(amqp_host).

get_env_r_test() ->
  5672 = bunny_farm:get_env(amqp_port).

encoding_octet_test() ->
  error_logger:info_msg("[bunny_farm_tests] Testing none encoding~n"),
  PubBus = bunny_farm:open({<<"dummy.exchange">>, [{encoding, <<"application/octet-stream">>}]}),

  SubBus = bunny_farm:open(<<"dummy.exchange">>, <<"dummy.route">>),
  bunny_farm:consume(SubBus),
  receive
    #'basic.consume_ok'{consumer_tag=ConsumerTag} -> ok
  end,

  bunny_farm:publish(<<"dummy payload">>, <<"dummy.route">>, PubBus),
  receive
    {#'basic.deliver'{}, Content} ->
      Act = farm_tools:decode_payload(Content)
  end,

  bunny_farm:close(SubBus, ConsumerTag),
  bunny_farm:close(PubBus),
  ?assertEqual(<<"dummy payload">>, Act).

encoding_none_test() ->
  error_logger:info_msg("[bunny_farm_tests] Testing none encoding~n"),
  PubBus = bunny_farm:open({<<"dummy.exchange">>, [{encoding, none}]}),

  SubBus = bunny_farm:open(<<"dummy.exchange">>, <<"dummy.route">>),
  bunny_farm:consume(SubBus),
  receive
    #'basic.consume_ok'{consumer_tag=ConsumerTag} -> ok
  end,

  bunny_farm:publish(<<"dummy payload">>, <<"dummy.route">>, PubBus),
  receive
    {#'basic.deliver'{}, Content} ->
      Act = farm_tools:decode_payload(Content)
  end,

  bunny_farm:close(SubBus, ConsumerTag),
  bunny_farm:close(PubBus),
  ?assertEqual(<<"dummy payload">>, Act).

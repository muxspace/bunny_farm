-module(qcache_tests).
-include("bunny_farm.hrl").
-include_lib("eunit/include/eunit.hrl").

main_test_() ->
  %application:start(sasl),
  {setup,
    fun setup/0,
    fun cleanup/1,
    [ fun count_connections_call/0,
      fun count_connections_rpc_1/0,
      fun count_connections_rpc_2/0
    ]
  }.

setup() ->
  ok.

cleanup(_) ->
  ok.

count_connections_call() ->
  {ok,Pid} = mz_qserver:start_link(mz1, [{key3,3}]),
  mz_qserver:set_value(mz1, key1, foo),
  _ = mz_qserver:get_value(mz1, key1),
  Conns = mz_qserver:get_connections(mz1),
  Exp = 3, % Based on ConnSpecs in my_qserver.erl
  mz_qserver:stop(mz1),
  ?assertEqual(Exp, length(Conns)).

count_connections_rpc_1() ->
  {ok,Pid} = mz_qserver:start_link(mz2, [{key3,3}]),
  K = <<"get_value_queue">>,
  PubBus = bunny_farm:open(<<"qserver.two">>),
  SubBus = bunny_farm:open(<<"qserver.sub">>, K),
  bunny_farm:consume(SubBus),
  receive
    #'basic.consume_ok'{consumer_tag=ConsumerTag} -> ok
  end,

  ReplyTo = <<"qserver.sub:get_value_queue">>,
  bunny_farm:rpc({get_value, key3}, ReplyTo, <<"key">>, PubBus),
  receive
    {#'basic.deliver'{}, Content} ->
      Act = farm_tools:decode_payload(Content)
  end,

  bunny_farm:close(SubBus, ConsumerTag),
  bunny_farm:close(PubBus),
  Conns = mz_qserver:get_connections(mz2),
  %error_logger:info_msg("[qcache_tests] Got conns ~p~n",[Conns]),
  Exp = 4, % Based on ConnSpecs in my_qserver.erl + new queue
  mz_qserver:stop(mz2),
  ?assertEqual(Exp, length(Conns)).

count_connections_rpc_2() ->
  {ok,Pid} = mz_qserver:start_link(mz3, [{key3,3}]),
  R = <<"get_value_queue_1">>,
  PubBus = bunny_farm:open(<<"qserver.two">>),
  SubBus = bunny_farm:open(<<"">>, R),

  bunny_farm:consume(SubBus),
  receive
    #'basic.consume_ok'{consumer_tag=ConsumerTag} -> ok
  end,

  bunny_farm:rpc({get_value, key3}, R, <<"key">>, PubBus),
  receive
    {#'basic.deliver'{}, Content} ->
      Act = farm_tools:decode_payload(Content)
  end,

  bunny_farm:rpc({get_value, key3}, R, <<"key">>, PubBus),
  receive
    {#'basic.deliver'{}, Content} ->
      Act = farm_tools:decode_payload(Content)
  end,

  bunny_farm:rpc({get_value, key3}, R, <<"key">>, PubBus),
  receive
    {#'basic.deliver'{}, Content} ->
      Act = farm_tools:decode_payload(Content)
  end,

  bunny_farm:close(SubBus, ConsumerTag),
  bunny_farm:close(PubBus),
  Conns = mz_qserver:get_connections(mz3),
  error_logger:info_msg("[qcache_tests] Got conns ~p~n",[Conns]),
  Exp = 4, % Based on ConnSpecs in my_qserver.erl + 1 for default exchange
  mz_qserver:stop(mz3),
  ?assertEqual(Exp, length(Conns)).

%% Disabled -- Seems that the first channel that receives a message blocks 
%% the other channel from receiving a message. Not sure how/why this happens
count_connections_rpc_3() ->
  {ok,Pid} = mz_qserver:start_link(mz4, [{key3,3}]),
  R1 = <<"get_value_queue_1">>,
  R2 = <<"get_value_queue_2">>,
  PubBus = bunny_farm:open(<<"qserver.two">>),
  SubBus1 = bunny_farm:open(<<"">>, R1),
  SubBus2 = bunny_farm:open(<<"exchange2">>, R2),
  error_logger:info_msg("Subscribing to SubBus1~n"),
  bunny_farm:consume(SubBus1),
  receive
    #'basic.consume_ok'{consumer_tag=ConsumerTag1} -> ok
  end,

  error_logger:info_msg("Subscribing to SubBus2~n"),
  bunny_farm:consume(SubBus2),
  receive
    #'basic.consume_ok'{consumer_tag=ConsumerTag2} -> ok
  end,

  bunny_farm:rpc({get_value, key3}, R1, <<"key">>, PubBus),
  error_logger:info_msg("Waiting for R1~n"),
  receive
    {#'basic.deliver'{}, Content} ->
      Act = farm_tools:decode_payload(Content)
  end,

  bunny_farm:rpc({get_value, key3}, <<"exchange2:",R2/binary>>, <<"key">>, PubBus),
  error_logger:info_msg("Waiting for R2~n"),
  receive
    {#'basic.deliver'{}, Content} ->
      Act = farm_tools:decode_payload(Content)
  end,

  bunny_farm:close(SubBus1, ConsumerTag1),
  bunny_farm:close(SubBus2, ConsumerTag2),
  bunny_farm:close(PubBus),
  Conns = mz_qserver:get_connections(mz4),
  error_logger:info_msg("[qcache_tests] Got conns ~p~n",[Conns]),
  Exp = 4, % Based on ConnSpecs in my_qserver.erl + 1 for default exchange
  mz_qserver:stop(mz4),
  ?assertEqual(Exp, length(Conns)).


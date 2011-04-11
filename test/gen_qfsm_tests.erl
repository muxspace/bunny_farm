-module(gen_qfsm_tests).
-include("bunny_farm.hrl").
-include_lib("eunit/include/eunit.hrl").

main_test_() ->
  %application:start(sasl),
  {setup,
    fun setup/0,
    fun cleanup/1,
    [ fun init_value/0,
      fun next_normal/0,
      fun next_queue/0,
      fun get_connection/0
    ]
  }.

setup() ->
  {ok,Pid} = my_qfsm:start_link(0),
  Pid.

cleanup(Pid) ->
  my_qfsm:stop(Pid),
  ok.

init_value() ->
  Act = my_qfsm:get_value(init_event),
  ?assertEqual({init_event,0}, Act).
  % state_a

next_normal() ->
  my_qfsm:next(),
  % state_b
  Act = my_qfsm:get_value(normal_event),
  % state_c
  ?assertEqual({normal_event,2}, Act).


next_queue() ->
  error_logger:info_msg("[gen_qfsm_tests] Opening connections~n"),
  K = <<"gen_qfsm_tests">>,
  PubBus = bunny_farm:open(<<"qfsm.two">>),
  SubBus = bunny_farm:open(<<"qfsm.sub">>, K),
  error_logger:info_msg("[gen_qfsm_tests] Consuming <<qfsm.sub>>~n"),
  bunny_farm:consume(SubBus),
  receive
    #'basic.consume_ok'{consumer_tag=ConsumerTag} -> ok
  end,

  error_logger:info_msg("[gen_qfsm_tests] Sending set_value"),
  Message = #message{payload=event_1, encoding=erlang},
  % state_a
  bunny_farm:publish(Message, <<"key">>, PubBus),

  error_logger:info_msg("[gen_qfsm_tests] Calling RPC to get_value"),
  ReplyTo = <<"qfsm.sub:gen_qfsm_tests">>,
  bunny_farm:rpc(event_2, ReplyTo, <<"key">>, PubBus),

  error_logger:info_msg("[gen_qfsm_tests] Waiting for response~n"),
  receive
    {#'basic.deliver'{}, Content} ->
      {{<<_K/binary>>, Event}, Act} = farm_tools:decode_payload(Content)
  end,
  % state_b

  bunny_farm:close(SubBus, ConsumerTag),
  bunny_farm:close(PubBus),
  %% This format is here since we pass back the un-transformed event from
  %% the RPC.
  error_logger:info_msg("[gen_qfsm_tests] Act = ~p~n", [Act]),
  ?assertEqual(1, Act),
  ?assertEqual(event_2, Event).


get_connection() ->
  Conn = my_qfsm:get_connection(),
  error_logger:info_msg("[gen_qfsm_tests] Conn = ~p~n", [Conn]),
  Handle = proplists:get_value(handle,Conn),
  ?assertEqual(<<"qfsm.two">>, proplists:get_value(id,Conn)),
  ?assertEqual(bus_handle, element(1,Handle)).


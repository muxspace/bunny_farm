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


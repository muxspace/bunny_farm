-module(bunny_farm_tests).
-include("bunny_farm.hrl").
-include_lib("eunit/include/eunit.hrl").

get_env_u_test() ->
  <<"guest">> = bunny_farm:get_env(username).

get_env_p_test() ->
  <<"guest">> = bunny_farm:get_env(password).

get_env_v_test() ->
  <<"/">> = bunny_farm:get_env(virtual_host).

get_env_h_test() ->
  "localhost" = bunny_farm:get_env(host).

get_env_r_test() ->
  5672 = bunny_farm:get_env(port).


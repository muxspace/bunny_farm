-module(farm_tools_tests).
-include("bunny_farm.hrl").
-include_lib("eunit/include/eunit.hrl").

listify_test_() ->
  Exp = "one_1",
  Act = farm_tools:listify([one,1], "_"),
  ?_assertMatch(Exp,Act).

atomize_test_() ->
  Exp = one_1,
  Act = farm_tools:atomize([one, "_", 1]),
  ?_assertMatch(Exp,Act).
  
binarize_test_() ->
  Exp = <<"one_1">>,
  Act = farm_tools:binarize([one, "_", 1]),
  ?_assertMatch(Exp,Act).

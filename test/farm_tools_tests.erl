-module(farm_tools_tests).
-include("bunny_farm.hrl").
-include_lib("eunit/include/eunit.hrl").

listify_test() ->
  Exp = "one_1",
  Act = farm_tools:listify([one,1], "_"),
  Exp = Act.

atomize_test() ->
  Exp = one_1,
  Act = farm_tools:atomize([one, "_", 1]),
  Exp = Act.
  
binarize_list_test() ->
  Exp = <<"one_1">>,
  Act = farm_tools:binarize([one, "_", 1]),
  Exp = Act.

binarize_other_test() ->
  Exp = <<"one">>,
  Act = farm_tools:binarize(one),
  Exp = Act.

bson_simple_test() ->
  TupleList = [ {fruit, pear}, {grain, barley} ],
  Encoded = farm_tools:encode_payload(bson, TupleList),
  Decoded = farm_tools:decode_payload(bson, Encoded),
  TupleList = Decoded.

bson_plain_tuple() ->
  Tuple = {fruit, pear},
  Encoded = farm_tools:encode_payload(Tuple),
  Decoded = farm_tools:decode_payload(bson, Encoded),
  [Tuple] = Decoded.

bson_decode_test() ->
  TupleList = [ {fruit, pear}, {grain, barley} ],
  Encoded = farm_tools:encode_payload(erlang, TupleList),
  Decoded = farm_tools:decode_payload(Encoded),
  TupleList = Decoded.
  

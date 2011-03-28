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
  
binarize_test() ->
  Exp = <<"one_1">>,
  Act = farm_tools:binarize([one, "_", 1]),
  Exp = Act.

bson_simple_test() ->
  %TupleList = [ {fruits, [{fruit1, peach}, {fruit2, pear}]}, {grain, barley} ],
  TupleList = [ {fruit, pear}, {grain, barley} ],
  %Doc = { fruits, {fruit1,peach, fruit2,pear}, grain, barley },
  %Exp = bson:put_document(Doc),
  Encoded = farm_tools:encode_payload(bson, TupleList),
  Decoded = farm_tools:decode_payload(bson, Encoded),
  TupleList = Decoded.


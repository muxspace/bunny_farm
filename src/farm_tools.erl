-module(farm_tools).
-include("bunny_farm.hrl").
-export([decode_properties/1, decode_payload/1, encode_payload/1]).
-export([to_list/1, atomize/1, atomize/2, listify/1, listify/2]).
-export([binarize/1]).
-export([to_queue_declare/1, to_amqp_props/1]).

%% Properties is a 'P_basic' record. We convert it back to a tuple
%% list
decode_properties(#amqp_msg{props=Properties}) ->
  [_Name|Vs] = tuple_to_list(Properties),
  Ks = record_info(fields,'P_basic'),
  lists:zip(Ks,Vs).

decode_payload(#amqp_msg{payload=Payload}) ->
  decode_payload(Payload);
  
decode_payload(Payload) -> 
  binary_to_term(Payload).

encode_payload(Payload) ->
  binarize(Payload).

to_list(Float) when is_float(Float) -> float_to_list(Float);
to_list(Integer) when is_integer(Integer) -> integer_to_list(Integer);
to_list(Atom) when is_atom(Atom) -> atom_to_list(Atom);
to_list(List) when is_list(List) -> List.

atomize(List) when is_list(List) ->
  list_to_atom(lists:foldl(fun(X,Y) -> Y ++ to_list(X) end, [], List)).

atomize(List, Sep) when is_list(List) ->
  list_to_atom(listify(List, Sep)).

listify(List) when is_list(List) ->
  listify(List, " ").

listify(List, Sep) when is_list(List) ->
  [H|T] = List,
  lists:foldl(fun(X,Y) -> Y ++ Sep ++ to_list(X) end, to_list(H), T).
  
binarize(List) when is_list(List) ->
  [H|T] = List,
  O = lists:foldl(fun(X,Y) -> Y ++ to_list(X) end, to_list(H), T),
  list_to_binary(O);

binarize(Other) -> term_to_binary(Other).

%% Converts a tuple list of values to a queue.declare record
to_queue_declare(Props) ->
  list_to_tuple(['queue.declare'|[proplists:get_value(X,Props,false) || 
    X <- record_info(fields,'queue.declare')]]).

%% Converts a tuple list of values to amqp_msg properties (P_basic)
to_amqp_props(Props) ->
  list_to_tuple(['P_basic'|[proplists:get_value(X,Props) || 
    X <- record_info(fields,'P_basic')]]).


%% Copyright 2011 Brian Lee Yung Rowe
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.

-module(farm_tools).
-include("bunny_farm.hrl").
-export([decode_properties/1,
  decode_payload/1, decode_payload/2,
  encode_payload/1, encode_payload/2]).
-export([to_list/1, atomize/1, atomize/2, listify/1, listify/2]).
-export([binarize/1]).
-export([to_exchange_declare/1,
         to_queue_declare/1,
         to_amqp_props/1,
         to_basic_consume/1,
         is_rpc/1,
         reply_to/1, reply_to/2,
         content_type/1, content_type/2,
         encoding/1,
         correlation_id/1 ]).

%% Properties is a 'P_basic' record. We convert it back to a tuple
%% list
decode_properties(#amqp_msg{props=Properties}) ->
  [_Name|Vs] = tuple_to_list(Properties),
  Ks = record_info(fields,'P_basic'),
  lists:zip(Ks,Vs).

decode_payload(#amqp_msg{payload=Payload}=Content) ->
  decode_payload(content_type(Content), Payload);
decode_payload(Payload) -> decode_payload(bson, Payload).

decode_payload(Encoding, #amqp_msg{payload=Payload}=_Content) ->
  decode_payload(Encoding, Payload);
decode_payload(none, Payload) -> Payload;
decode_payload(<<"application/octet-stream">>, Payload) -> Payload;
decode_payload({_E,M,F}, Payload) -> M:F(Payload);

decode_payload(<<"application/x-erlang">>, Payload) ->
  decode_payload(erlang, Payload);
decode_payload(erlang, Payload) -> binary_to_term(Payload);
decode_payload(<<"application/bson">>, Payload) ->
  decode_payload(bson, Payload);
decode_payload(bson, Payload) ->
  try
    {Doc,_Bin} = bson_binary:get_document(Payload),
    bson:fields(Doc)
  catch
    error:{badmatch,_} -> decode_payload(erlang, Payload);
    error:function_clause -> decode_payload(erlang, Payload)
  end.

encode_payload(Payload) -> encode_payload(bson, Payload).

encode_payload(none, Payload) -> Payload;
encode_payload(<<"application/octet-stream">>, Payload) -> Payload;
encode_payload({_E,M,F}, Payload) -> M:F(Payload);

encode_payload(<<"application/x-erlang">>, Payload) ->
  encode_payload(erlang, Payload);
encode_payload(erlang, Payload) -> term_to_binary(Payload);
encode_payload(<<"application/bson">>, Payload) ->
  encode_payload(bson, Payload);
encode_payload(bson, Payload) when is_list(Payload) ->
  bson_binary:put_document(bson:document(Payload));
encode_payload(bson, Payload) ->
  bson_binary:put_document(bson:document([Payload])).

%% Convert types to strings
to_list(Binary) when is_binary(Binary) -> binary_to_list(Binary);
to_list(Float) when is_float(Float) -> float_to_list(Float);
to_list(Integer) when is_integer(Integer) -> integer_to_list(Integer);
to_list(Atom) when is_atom(Atom) -> atom_to_list(Atom);
to_list(List) when is_list(List) -> List.

%% Convert strings to atoms
atomize(List) when is_list(List) ->
  list_to_atom(lists:foldl(fun(X,Y) -> Y ++ to_list(X) end, [], List)).

atomize(List, Sep) when is_list(List) ->
  list_to_atom(listify(List, Sep)).

%% Convert a list of elements into a single string
%% DEPRECATED Use lists:concat instead
listify(List) when is_list(List) ->
  listify(List, " ").

listify(List, Sep) when is_list(List) ->
  [H|T] = List,
  lists:foldl(fun(X,Y) -> Y ++ Sep ++ to_list(X) end, to_list(H), T).

%% Convenience function to convert values to binary strings. Useful for
%% creating binary names for exchanges or routing keys. Not recommended
%% for payloads.
binarize(Binary) when is_binary(Binary) -> Binary;

%% Example
%%   farm_tools:binarize([my, "-", 2]) => <<"my-2">>
binarize(List) when is_list(List) ->
  [H|T] = List,
  O = lists:foldl(fun(X,Y) -> Y ++ to_list(X) end, to_list(H), T),
  list_to_binary(O);

binarize(Other) -> list_to_binary(to_list(Other)).

%% Converts a tuple list of values to a queue.declare record
-spec to_exchange_declare([{atom(), term()}]) -> #'exchange.declare'{}.
to_exchange_declare(Props) ->
  %% This is a safety in case certain arguments aren't set elsewhere
  Defaults = [ {ticket,0}, {arguments,[]} ],
  Enriched = lists:merge(Props, Defaults),
  list_to_tuple(['exchange.declare'|[proplists:get_value(X,Enriched,false) ||
    X <- record_info(fields,'exchange.declare')]]).

%% Converts a tuple list of values to a queue.declare record
-spec to_queue_declare([{atom(), term()}]) -> #'queue.declare'{}.
to_queue_declare(Props) ->
  %% This is a safety in case certain arguments aren't set elsewhere
  Defaults = [ {ticket,0}, {arguments,[]} ],
  Enriched = lists:merge(Props, Defaults),
  list_to_tuple(['queue.declare'|[proplists:get_value(X,Enriched,false) ||
    X <- record_info(fields,'queue.declare')]]).

%% Converts a tuple list to a basic.consume record
-spec to_basic_consume([{atom(), term()}]) -> #'basic.consume'{}.
to_basic_consume(Props) ->
  Defaults = [ {ticket,0}, {arguments,[]}, {consumer_tag,<<"">>} ],
  Enriched = lists:merge(Props, Defaults),
  list_to_tuple(['basic.consume'|[proplists:get_value(X,Enriched,false) ||
    X <- record_info(fields,'basic.consume')]]).

%% Converts a tuple list of values to amqp_msg properties (P_basic)
-spec to_amqp_props([{atom(), term()}]) -> #'P_basic'{}.
to_amqp_props(Props) ->
  list_to_tuple(['P_basic'|[proplists:get_value(X,Props) ||
    X <- record_info(fields,'P_basic')]]).

is_rpc(#amqp_msg{props=Props}) ->
  case Props#'P_basic'.reply_to of
    undefined -> false;
    _ -> true
  end.

%% Convenience function to get the reply_to property
%% This will split the reply_to value into an Exchange and Route if a
%% colon (:) is found as a separator. Otherwise, the existing exchange
%% will be used.
-spec reply_to(#amqp_msg{}, binary()) -> binary().
reply_to(#amqp_msg{}=Content, _SourceX) -> reply_to(Content).

-spec reply_to(#amqp_msg{}) -> binary().
reply_to(#amqp_msg{}=Content) ->
  Props = farm_tools:decode_properties(Content),
  Parts = binary:split(proplists:get_value(reply_to, Props), <<":">>),
  case Parts of
    [X,K] -> {X,K};
    [K] -> {<<"">>,K}
  end.


-spec correlation_id(#amqp_msg{}) -> binary().
correlation_id(#amqp_msg{}=Content) ->
  correlation_id(farm_tools:decode_properties(Content));

correlation_id(Props) when is_list(Props) ->
  p(correlation_id, Props).

-spec content_type(#amqp_msg{}) -> binary().
content_type(#amqp_msg{}=Content) ->
  content_type(farm_tools:decode_properties(Content));

content_type(Props) when is_list(Props) ->
  p(content_type, Props).

%% Override the content type in a bus handle
content_type(ContentType, #bus_handle{options=Os}=BusHandle) ->
  BusHandle#bus_handle{options=lists:merge([{content_type,ContentType}],Os)}.

encoding(Options) ->
  case proplists:get_value(encoding, Options) of
    {E,_M,_F} -> E;
    none -> <<"application/octet-stream">>;
    E -> E
  end.

p(P, #amqp_msg{}=Content) ->
  p(P, farm_tools:decode_properties(Content), undefined);

p(P, Props) when is_list(Props) ->
  p(P, Props, undefined).

p(P, #amqp_msg{}=Content, Default) ->
  p(P, farm_tools:decode_properties(Content), Default);

%% This construction is used because sometimes the value is set to undefined,
%% and we want to change these to the default in addition to undefined values.
p(P, Props, Default) when is_list(Props) ->
  case proplists:get_value(P, Props) of
    undefined -> Default;
    V -> V
  end.

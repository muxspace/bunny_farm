-ifndef(BUNNY_FARM_HRL).
-define(BUNNY_FARM_HRL, true).

-include_lib("amqp_client/include/amqp_client.hrl").
-record(bus_handle, {exchange, queue, routing_key, channel, conn}).
-record(message, {payload, props=[] }).
%% Represents a generic remote procedure call
-record(rpc, {procedure, args}).


-endif.

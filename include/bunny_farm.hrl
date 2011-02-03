-ifndef(BUNNY_FARM_HRL).
-define(BUNNY_FARM_HRL, true).

-include_lib("amqp_client/include/amqp_client.hrl").
-record(bus_handle, {exchange, queue, routing_key, channel, conn}).
-record(message, {payload, props=#'P_basic'{} }).

-endif.

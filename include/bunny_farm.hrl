-ifndef(BUNNY_FARM_HRL).
-define(BUNNY_FARM_HRL, true).

-include_lib("amqp_client/include/amqp_client.hrl").
-record(bus_handle, {exchange, queue, channel, conn}).

-endif.

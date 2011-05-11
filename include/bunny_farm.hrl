-ifndef(BUNNY_FARM_HRL).
-define(BUNNY_FARM_HRL, true).

-include_lib("amqp_client/include/amqp_client.hrl").
-record(bus_handle, {exchange, queue, routing_key, channel, conn, options=[]}).

%% This is only used for publishing messages. It gets converted to
%% an AMQP message when sending over the wire.
-record(message, {payload, props=[], encoding=bson }).


-type bus_handle() :: #bus_handle{}.
-type exchange() :: binary().
-type routing_key() :: binary().
-type myabe_binary() :: binary() | {binary(),list()}.

-endif.

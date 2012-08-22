# bunny_farm

This is a wrapper library for managing AMQP connections. This simplifies the
declaration and handshake process to make writing publishers and consumers a
little easier.

Included are queue-based implementations of a `gen_server` and `gen_fsm` that
abstract away some of the complexities of using the message queue. These
behaviors unify the standard gen_server APIs with the message queue semantics,
simplifying the behaviour implementation while maintaining flexibility in the
communication methods. These servers manage all bus connections, opening,
caching, and closing these connections as needed.


## Using the gen_qserver

The `gen_qserver` manages all the connection details related to the queue. When
starting a server, the last argument is passed into `gen_qserver:start_link/5`
must contain a list of connection specs. These can be of the following forms:

```erlang
<<"exchange">> -- Used for publishing
{<<"exchange">>,Options} -- Used for publishing
{<<"exchange">>, <<"route">>} -- Used for consuming
{{<<"exchange">>,Options}, {<<"route">>,Options}} -- Used for consuming
```

Any valid connection parameters for an exchange.declare or queue.declare is
allowed, plus a few additional conveniences. To set the encoding for a given
connection, do this:

```erlang
{<<"exchange">>, [{encoding,<<"application/bson">>}]}
```

Based on the spec, a connection will be made and the bus handles will be
cached in memory. These can be accessed in code by a call to `qcache`:

```erlang
qcache:get_bus(Tid, <<"exchange">>)
```

The id must be the same as when configuring the connection. Don't mix and
match as this will yield unexpected results.

The `Tid` is how your implementation can access its cache. This id is
provided to your module via the new() function. If no explicit publish operations
are implemented, then this argument can be safely ignored.

The `qcache` module supports other forms for more granular used.

### RPC

Support for RPCs is built into the `gen_qserver`. They are handled automatically
by the underlying implementation, dispatching to `handle_call/3` and then
replying to the specified exchange and queue. RPCs are sent as erlang binaries
as opposed to BSON for greater flexibility.

Responses utilize the standard AMQP method for replying to an RPC. This can be
over-ridden using a specific syntax for the routing key.

### Callbacks

The `init/2` replaces `init/1` in a `gen_server` implementation

```erlang
Module:init(Args, CachePid)
```

RPC calls are routed to `handle_call`, with the routing key as the first element
of a tuple. This granular control based on pattern matching the
[bitstring](http://www.erlang.org/doc/programming_examples/bit_syntax.html).

```erlang
handle_call({<<"route">>, Payload}, _From, State)
```

All other messages are dispatched to handle_cast, again with the routing key
as the first element of a tuple.

```erlang
handle_cast({<<"route">>, Payload}, State)
```

If no special handling is required, then these calls can be routed to
standard `gen_server` forms with the following:

```erlang
handle_cast({<<_B/binary>>, Payload}, State) ->
  handle_cast(Payload, State).
```

## Using the gen_qfsm

The queue-enabled FSM works like the regular
[gen_fsm](http://www.erlang.org/doc/design_principles/fsm.html) except that it
can listen to messages on the bus. Configuration is the same as the
`gen_qserver`.  Normal publish messages are treated as asynchronous
calls--invoking `StateName/2`-- whereas RPC messages are dispatched to the
synchronous calls--invoking `StateName/3`.  Any events that come from the bus
are structured as `{<<RoutingKey/binary>>, Event}` The corresponding
Module:StateName will be called with this event, as in the standard `gen_fsm`.


### Limitations

Currently the
[`send_all_state_event`](http://www.erlang.org/doc/man/gen_fsm.html#send_all_state_event-2)
forms are not supported.

### Example

A simple implementation, `my_qfsm`, is in the test directory. This
implementation illustrates the callback structure, while the test shows how to
make calls (albeit in a not-recommended fashion).

## Standalone Use

It is possible to use the library without the servers. This usage requires
a bit more wiring on the consumer side.

### Publishing to a Topic Exchange

When publishing to a topic exchange, a queue isn't necessary since messages
aren't being read. The exchange will manage routing messages to queues once
they are defined. This makes sending messages fairly simple.

Simple publishing encodes messages as [BSON](http://bsonspec.org/), so messages
should conform to a structure that can be converted to BSON, such as a proplist.

```erlang
BusHandle = bunny_farm:open(<<"exchange">>)
bunny_farm:publish([{key1,message}], <<"routing_key.1">>, BusHandle)
```

Note that the `BusHandle` can be reused for multiple messages against arbitrary
routes.

### Consuming Messages from a Topic Exchange

Subscribing to messages requires a bit more set up but not much. Here we need
to declare a queue. While naming the queue is not required, it can be useful
for managing the queue later on. Both methods are illustrated below.

#### Auto-Named Queue

This is the recommended approach as queues typically don't need to be accessed
directly.

```erlang
BusHandle = bunny_farm:open(<<"exchange">>, <<"routing_key.#">>)
bunny_farm:consume(BusHandle)
```

#### Named Queue

In the event that an explicit queue name is required, then the following can
be done.

```erlang
BusHandle = bunny_farm:open(<<"exchange">>, {<<"routing_key.#">>,[{queue,Q}]})
bunny_farm:consume(BusHandle)
```

If no routing key is necessary, then the following is simpler

```erlang
BusHandle = bunny_farm:open(<<"exchange">>),
bunny_farm:publish(Message, QueueName, BusHandle),
```

#### Work Queues

A work queue can be implemented by using the default exchange with a named
queue.

```erlang
BusHandle = bunny_farm:open(<<"">>),
bunny_farm:publish(Message, QueueName, BusHandle),
```

When using a `gen_qserver`, the connection spec is described by

```erlang
{<<"">>, {QueueName, [{exclusive,false}]}}
```

Note that this structure is the same as what gets passed in to the `open/1`
function in the first named queue example.

Once the subscriptions have been set up, then raw AMQP messages need to be
detected to actually process data. In a `gen_server` setting, this is caught by
the handle_info callback. With the `gen_qserver` it is automatically wired.

### Making an RPC Over A Topic Exchange

Asynchronous RPCs can be executed by calling the `bunny_farm:rpc/4` function.
The message `reply_to` property is used to control the return routing key after
the operation is made. A special form of this field can be used to send the
response over a separate exchange.

#### Respond On Same Exchange

The standard behaviour is to respond on the default exchange as specified in the
AMQP spec. The queue will be named based on the reply-to field. Any bitstring is
valid except one containing a colon, as this will be interpreted as a two part
route (see below).

```erlang
ReplyTo = <<"reply_route">>
```

#### Respond On Different Exchange

This form is useful for sending the request and receiving the response on
distinct exchanges. The ReplyTo is constructed in two parts separated by a
colon, where the first section is the exchange and the second is the
routing key.

```erlang
BusHandle = qcache:get_bus(CachePid, <<"reply_exchange">>)
ReplyTo = <<"reply_exchange:reply_route">>
bunny_farm:rpc({get_value, key5}, ReplyTo, <<"request_route">>, BusHandle)
```

## Message Encoding

The default message encoding is erlang binaries. To send messages using another
encoding, a tuple can be passed to publish with the encoding explicitly
defined. The encoding is represented by the mime type.

```erlang
bunny_farm:publish({Message, <<"application/bson">>}, <<"routing_key">>, PubBus)
```

The same behavior exists for RPC commands.

If the same encoding will be used on all messages for a given exchange, then the
encoding can be set in the connection configuration.

## Connection Configuration

The connection information for the RabbitMQ server can be set in the application
configuration file. The following variables are currently supported and can be
added to the parameter list of your application.

```erlang
{amqp_username, <<"guest">>},
{amqp_password, <<"guest">>},
{amqp_virtual_host, <<"/">>},
{amqp_servers, [{"localhost",5672}] },
{amqp_encoding, <<"application/erlang">>},

{amqp_exchanges, [
  {<<"exchange">>, [{K,V}] }
]},
{amqp_queues, [
  {<<"routing_key">>, [{K,V}] }
]}
```

Note that the `amqp_host` and `amqp_port` are now deprecated in favor of the
singular `amqp_servers`. The older configuration will continue to work, but new
applications should use `amqp_servers`. Keep in mind that if `amqp_servers` is
populated, then it will override any settings in the legacy fields. This change
towards a singular `amqp_servers` variable was made to support clusters.
Multiple `{host,port}` tuples can be entered as a list and will be selected
randomly. In the future this may switch to a round-robin or other method.

Channel definitions provide a way to set connection configuration each time a
specific channel is opened. If no such information is provided, then default
values will be used. Channels are referenced by the type of channel (whether it
is used for publishing or subscribing), and a binary string which is either the
name of the exchange or a colon-separated pair representing the exchange and
routing key. Options are specified as a proplist. Current options include

```erlang
{encoding, binary()},
{type, binary()},
{durable, boolean()},
{exclusive, boolean()},
{queue, binary()}
```

The encoding property is only available for pub channels.

### Processes
A gen_qserver and gen_qfsm both utilize a qcache, which is now backed by ETS,
so each server without any connections creates a single process. Rabbit
itself will create many more processes.

## Future

* Message-specific overrides for encoding (currently this is done at the
  exchange level via the config)

## Author

Brian Lee Yung Rowe

# Broker and control routing

Phase 4 provides two real, independently supervised processes:

```text
purdue-cv-broker
purdue-cv-control-router
```

Both load the authoritative mission configuration. The broker binds
`messaging.broker.publisher_endpoint` (XSUB) and
`messaging.broker.subscriber_endpoint` (XPUB). The control router binds
`messaging.control.client_endpoint` and `messaging.control.module_endpoint` as
ROUTER sockets. Publishers, subscribers, surface clients, and modules connect.
No production endpoint is duplicated in service code.

## Socket ownership and shutdown

Each process creates its own `zmq.Context` inside its service `run()` method.
The context, both sockets, and the poller are created, used, closed, and
terminated by that same thread. The broker and router do not pass sockets to
cleanup threads. Phase 3's shutdown coordinator owns signal intent and state
transitions; the socket-owning loop performs `LINGER=0` closure before context
termination and then completes the `STOPPED` transition. Polling is bounded at
100 ms, so SIGTERM does not depend on an incoming message.

The reusable control client records both its creating process and thread. It
rejects use of an inherited context/socket after `fork()` and requires a new
client instance in the child process.

## Data broker

The XSUB/XPUB broker forwards multipart frames without parsing or rebuilding
the Phase 1 envelope. It also forwards XPUB subscription notifications back to
XSUB, preserving ordinary ZeroMQ subscription routing. The transport limit is
4 MiB and remains distinct from Phase 1's 1 MiB normal-envelope semantic limit.

## Control framing

A DEALER sends exactly two application frames:

```text
[kind, payload]
```

A ROUTER receives or sends the corresponding three frames:

```text
[routing_identity, kind, payload]
```

Kinds are `REGISTER_MODULE`, `REGISTER_MODULE_RESPONSE`, `MODULE_HEARTBEAT`,
`COMMAND_REQUEST`, and `COMMAND_RESPONSE`. Registration and commands use the
existing protobuf messages. A heartbeat payload is exactly the current 16-byte
module session UUID; the routing identity supplies the stable module ID and the
same UUID. This small framing discriminator is transport metadata, not a second
wire command model.

The router validates framing, protobuf structure, UUID lengths, oneof command
selection, configured target identity, current registered session, target
availability, and routability. It never applies module lifecycle transitions.
The target module owns state validation and returns
`INVALID_STATE_TRANSITION` when appropriate.

## Registration and heartbeat policy

The registry is keyed by stable module ID. A record retains the current session
UUID, routing identity, supported oneof names, reported state, process and host
identity, registration time, heartbeat time, and availability. A never-before-
seen valid session replaces the current session by arrival order. The replaced
session is retired and cannot later re-register, heartbeat, or respond. This
prevents late traffic from an obsolete execution taking authority back.

Modules register immediately, wait at most 500 ms for acknowledgement, retry at
one-second intervals while remaining `STARTING`, and request exit 75 after ten
failed attempts. The policy accepts injectable clocks for deterministic tests.
After registration, heartbeats are sent once per second. A record becomes
unavailable when elapsed monotonic time is greater than or equal to 3.5 seconds.

## Commands, duplicate protection, and client timeouts

Command type is always `request.WhichOneof("command")`. The router preserves the
serialized request and original command UUID. Known-unavailable targets return
`REJECTED / TARGET_UNAVAILABLE`; `zmq.Again` from the configured 200 ms module
send returns `REJECTED / TARGET_SEND_TIMEOUT`. Other ZeroMQ errors are not
blanket-mapped to timeout.

The simulated module and production runner command caches use a ten-minute
monotonic TTL and 1024-entry capacity for finalized outcomes. Pending
`RECEIVED`/`ACCEPTED` reservations neither expire nor evict; if every slot is
pending, a new state-changing command returns `MODULE_BUSY`. Final results enter
the cache before the response send and are evicted oldest-first when capacity
is needed. A duplicate state-changing UUID is not executed and returns
`DUPLICATE_COMMAND_ID`; `get_command_status` returns the stored result without
re-execution. Unknown or expired finalized entries return `INVALID_COMMAND`,
using the already documented canonical code rather than adding a protobuf or
error code.

While a command UUID is still in the router's correlation table, the router
rejects another request with that UUID instead of overwriting the originating
client route. Once no route is pending, the target's retained status cache is
authoritative for duplicate execution protection. Client or registration reply
backpressure is logged as a peer-local delivery failure and does not stop the
router process.

The reusable client waits 500 ms for initial acknowledgement and never resends
the original command. A timeout produces `OUTCOME_UNKNOWN`, logs the original
UUID, reconnects its own socket, and permits one status query for that unknown
command. After an `ACCEPTED` result, the client applies the centralized command
deadline, then polls status at one-second intervals for no more than ten
seconds. A requested timeout may shorten but cannot extend a canonical limit;
the specification does not explicitly settle that ambiguity, so this is the
conservative interpretation.

## Metrics

`messages_received` counts messages accepted at the receiving layer's structural
boundary. Because the broker is deliberately payload-agnostic, each complete
transport receive is accepted there; the router and clients count only traffic
that passes their framing, protobuf, identity, and correlation checks.
`messages_sent` counts successful sends, not attempts. `invalid_messages` counts
malformed framing, protobufs, identities, and stale/inconsistent sessions.
`reconnect_count` increments on explicit client socket recreation.
`unknown_payload_types` remains receiver-owned.

The broker stays payload-agnostic. The shared `ReceivedMultipartValidator` is
the data receiver integration point for `observed_sequence_gaps`. It keys
continuity by `(publisher_session_id, source_id)` and increments by the number
of missing sequence values. New sessions reset continuity, while duplicate or
reordered messages are invalid and do not create negative gaps.

## Deferred runtime ownership

Phase 4 closes real ZeroMQ send-loop, ZeroMQ cleanup, broker/router signal
shutdown, control registration, and supervisor exit-translation risks.
Phase 6 now owns and tests simulated GStreamer teardown, camera-created
shared-memory cleanup/stale recovery, and the camera-to-module-runner portion of
the process trajectory. Physical V4L2/DepthAI teardown remains owned by the
physical-camera subsystem because Phase 6 does not open those resources. The
remaining full trajectory through a production subscriber and persistent
recorder is owned by the recorder/data-plane receiver subsystem; that receiver
also owns `observed_sequence_gaps` because the broker remains payload-agnostic.

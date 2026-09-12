---
title: Agent messaging
description: Send durable messages between independent agent threads through fixed, authorized routes.
---

# Agent messaging

Use peer messaging when independent agents need to exchange input. For a parent that launches and
steers a child worker, use [background subagents](./subagents/background) instead.

## Send messages through fixed peer routes

Peers are independent Agent Threads. Their input Schema belongs to the receiving Definition:

```ts
const Advisor = Messaging.peer("advisor", { target: advisor });
const send = Messaging.sendTool(Advisor);
// Programmatic: Messaging.send(Advisor, input, { idempotencyKey })
```

Provide the caller-bound `MessagingHost` returned by
`durableRuntime.messagingHost({ sourceThreadId, principal })` for programmatic operations.
The interpreter provides native tools with the actual caller facet. `sendTool`, `replyTool`,
`inboxTool`, `inspectTool`, and `retryTool` each derive a native Tool, Toolkit, and handler Layer;
install only the operations the host wants to expose.
The runtime provides `MessagingHost.forTool` through Effect context with the same per-Run
identity check and unavailable default as worker tools.

`PeerRoutes` maps a source, fixed peer name, and registered target to a destination Thread.
`PeerAuthorizer` separately authorizes context, read, send, and control and returns a stable
delivery principal. Both deny by default. Reply authorization receives the recorded sender
address and the original `reply` operation. Incoming messages confer no reverse send grant or
worker management grant. Use `Subagent.followUp` for worker input; a peer route cannot bypass
worker admission and budget ownership.

The runtime retains authenticated sender and return-address metadata separately from application
input, backed by a canonical source proof. `Messaging.inbox` returns bounded provenance from
authorized sender Threads. `Messaging.reply` requires one of those actual inbound references
and checks its sender against the declared peer. A send's optional `inReplyTo` is correlation
only. Models cannot choose arbitrary destination Threads, principals, or return addresses.

`Messaging.send` and `reply` return a retained message status. `pending` means outbound work is
stored; `accepted` includes the destination Receipt; `processed` includes its Settlement. Use
`Messaging.inspect` to read status. Automatic delivery retries preserve the exact destination,
input, principal, code version, and admission identity, including after a lost admission reply.
`Messaging.retry` renews a parked delivery's finite retry budget after control authorization;
conclusively refused and processed deliveries cannot be rewound.

Default delivery limits are eight automatic attempts, a 30-second attempt timeout, exponential
backoff from 1 to 60 seconds, and a 24-hour peer deadline. Exhaustion parks work; rejection remains
inspectable. `PeerMessageCapacity` bounds canonical send intents across all peers and principals
in a source Thread (default 256, maximum 1,000), including preparations whose insertion failed.
The delivery store separately bounds pending and retained rows. Neither history nor deduplication
evidence is automatically deleted. Effect spans identify preparation, admission, and driver
operations; persisted failures contain bounded codes rather than raw application errors.

Node's scoped delivery pump and Cloudflare's persisted alarms rediscover stored obligations even
after both Runs settle and wake hints are lost. During an active Cloudflare maintenance pass,
message delivery continues alongside source execution, so a message can reach its destination
before the source Run finishes. Progress still needs a functioning host and
available capacity. The [canonical Cloudflare application](https://github.com/danieljvdm/effect-agent/tree/main/examples/travel-planner)
provides the runnable application entrypoint.

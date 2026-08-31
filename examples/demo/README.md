# Chat-first demo with a Phase 2 simulator

A simple chat is the default experience. The detailed Phase 2 operations bench remains available
under the **Simulator** tab. Both surfaces use TanStack Start, Effect Atom, Effect RPC over
HTTP/NDJSON, and the public framework packages.

```sh
vp run -F @effect-agent/example-demo dev
```

## Chat

The default **OpenAI agent** profile is a real model-driven Phase 2 travel agent. The model decides
which framework Tools to call and coordinates repeatable flight, lodging, and activity suppliers.
Those supplier results are fixed demo inventory, so the interaction is safe and reproducible while
the orchestration is genuinely model-driven. Put `OPENAI_API_KEY` in `examples/demo/.env` (copy
`.env.example` to start); the Vite server loads that credential through Effect Config without
exposing it to the client bundle.

Use the chat directly to exercise the runtime:

- Ask it to plan the fixed London trip to see the live model call fixture supplier Tools through
  the bounded framework scheduler. The trace records the Tools the model actually chose; it is
  not a canned transcript.
- While a travel Run is active, send another message. The first admitted update is steering and is
  delivered after the current Tool batch; a later update is a queued follow-up delivered at the
  next safe stop boundary. The composer also exposes one-click date and room-preference updates so
  the timing behavior is easy to reproduce.
- Ask it to place a temporary hold. The runtime pauses before the handler, displays an approval
  card, and starts the demo hold handler only after explicit approval. No real reservation is made.
- Launch a token, Tool-call, spend, or time recipe to see the same live coordinator stopped by a
  typed run-level budget.

The **Scripted replay** profile is the deliberate offline option. It uses the same travel schemas,
runtime hooks, fixture handlers, and event stream with a deterministic model, making exact ordering
and failure cases repeatable without credentials. Free-form messages are never silently classified
by regular expressions: OpenAI mode always reaches the live travel agent, while Scripted replay
always reaches its explicit offline path. Tool activity stays inline beneath the active assistant
message.

The Simulator remains an optional deterministic evidence inspector, not a prerequisite for using
the Phase 2 features in chat.

## Simulator

A server-scoped runtime owns the simulator's ephemeral Thread, bounded input queue, approval
resolver, and event queue. The generated RPC client streams Schema-encoded framework and
operational events; separate unary RPCs admit steering, follow-up, and approval decisions without
replacing the stream.

Simulator scenarios:

- **Guided control run** starts flight, lodging, and activity Tools concurrently, forces them to
  finish in reverse order, and shows results committed in declaration order. While the batch is
  active, it queues a departure-date steering command and a room-preference follow-up. Steering is
  claimed after the complete Tool batch; the follow-up waits until the Run would otherwise stop.
- **Risky itinerary hold** exposes an approval checkpoint. The hold handler start count remains
  zero until approval. Denial or a 20-second non-response fails closed.
- **Token, Tool, spend, and time fuses** run with deliberately tiny limits and expose the exact
  rejected usage dimension and observed value.
- **Tool handler defect** makes every supplier handler die mid-batch. The producer boundary
  converts the defect into the stream's typed terminal failure and releases the single-run
  registry, so the browser never waits on a stranded Run.

The guided scenario also validates one bounded deterministic MCP discovery result, executes a
fixed `/bin/echo` request through `@effect-agent/sandbox-local`, and shows the implementation's
explicit `unisolated` posture, disabled network, output bound, deadline, and exit status. Its
context hook presents a compacted model view while the ephemeral Thread retains the full
official Effect AI `Prompt`.

The scripted replay path is credential-free and makes no external network call. The demo does not
claim persistence, accepted-work durability, OS isolation, exactly-once side effects, or recovery
after process loss. The Simulator's raw evidence panel is the same authoritative stream that drives
its projections.

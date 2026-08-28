---
title: The runtime model
description: Runs, Turns, Tool batches, safe seams, and one interpreter across maturity levels.
---

# The runtime model

The runtime is an explicit transition machine. Effects interpret decisions, but they do not hide
the transition rules.

## Domain units

| Unit             | Meaning                                                            |
| ---------------- | ------------------------------------------------------------------ |
| Agent Definition | immutable program, Schemas, Toolkit, instructions, policy          |
| Agent Binding    | one Definition paired with one explicit Effect AI Model            |
| Run              | one logical request against a Conversation                         |
| Turn             | one model response, optionally followed by one complete Tool batch |
| Attempt          | one durable ownership period advancing a Submission                |
| Conversation     | ordered canonical or ephemeral history shared across Runs          |

An ephemeral Run has one process Attempt implicitly. A durable Run may span several Attempts
without changing the semantic Turn loop.

## One interpreter

```text
decode input
  ↓
evaluate instructions and prepare context
  ↓
stream one Effect AI response
  ↓
reduce and validate the complete response
  ├─ final → decode structured output → complete
  └─ tools
       ↓
     preflight the complete Tool batch
       ↓
     execute under finite permits
       ↓
     commit results in declaration order
       ↓
     drain steering → evaluate policy → next Turn
```

`run`, `stream`, and `start` expose this one implementation. The durable assemblies insert
canonical commit seams around the same transitions; they do not create another agent loop.

## Complete before consequential

The engine will not execute a Tool until the assistant response and all Tool arguments are
complete. A length-truncated response cannot start a handler. The next Model request sees either a
complete Tool batch or none.

This makes the runtime deterministic at the exact boundary where external behavior begins.

## Safe-seam input

New input can arrive while a Model or Tool batch is active, but it cannot mutate that work.

- steering waits until the completed response and complete Tool batch;
- follow-up waits until the Run would otherwise stop;
- the initial drain policy consumes one queued item;
- durable joining uses the same seam with recoverable `joining` and `joined` states.

The semantic rule stays identical even when the storage mechanism changes.

## Events observe; commands act

`RunEvent` is a stable, typed observation API. Subscribers may render, trace, meter, or project
events. They do not participate in state transitions through arbitrary callbacks.

Steering, approval, follow-up, durable abort, and operator recovery are explicit command/service
interfaces. Observation cannot secretly become control flow.

The opt-in `RunToolFailureObserver` covers failures that become Tool results or broker outcomes,
so they may never reach the application's ordinary Run failure boundary. It is a trusted local
observation interface, not callback middleware: its closed `Effect<void>` returns no decision,
has no typed failure channel, and cannot change a Tool result. Observer and reporter defects are
isolated. Delivery runs inline under the existing Tool permit, so it consumes time and remains
externally interruptible. The observer must not reenter the engine. Its live Causes never enter
events, canonical records, or automatic telemetry; see [Tool failure observation](../guide/run-agents#observe-recovered-tool-failures).

## Bounded concurrency

Each complete Tool batch runs through finite `Semaphore` permits. Live progress may follow actual
completion order; committed results always retain model declaration order. Outer Agent, tenant, or
platform bounds may make concurrency stricter.

Attached Subagents reuse that scheduler instead of inventing a second fan-out system.

---
title: The runtime model
description: Runs, Turns, Tool batches, safe seams, and one interpreter across maturity levels.
---

# The runtime model

<StatusCallout status="available" phase="P1–P3" title="The semantic interpreter and safe Turn seams exist; future durability adds commit boundaries around them." />

The runtime is an explicit transition machine. Effects interpret decisions, but they do not hide
the transition rules.

## Domain units

| Unit             | Meaning                                                            |
| ---------------- | ------------------------------------------------------------------ |
| Agent Definition | immutable program, Schemas, Toolkit, instructions, policy          |
| Agent Binding    | one Definition paired with one explicit Effect AI Model            |
| Run              | one logical request against a Conversation                         |
| Turn             | one model response, optionally followed by one complete Tool batch |
| Attempt          | one future durable ownership period advancing a Submission         |
| Conversation     | ordered canonical or ephemeral history shared across Runs          |

An ephemeral Run has one process Attempt implicitly. A future durable Run may span several
Attempts without changing the semantic Turn loop.

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

`run`, `stream`, and `start` expose this one implementation. Planned durability inserts canonical
commit seams around the same transitions; it does not create another agent loop.

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
- future durable joining uses the same seam with recoverable `joining` and `joined` states.

The semantic rule stays identical even when the storage mechanism changes.

## Events observe; commands act

`RunEvent` is a stable, typed observation surface. Subscribers may render, trace, meter, or project
events. They do not participate in state transitions through arbitrary callbacks.

Steering, approval, follow-up, durable abort, and operator recovery are explicit command/service
interfaces. Observation cannot secretly become control flow.

## Bounded concurrency

Each complete Tool batch runs through finite `Semaphore` permits. Live progress may reflect actual
completion order; committed results always retain model declaration order. Outer Agent, tenant, or
platform bounds may make concurrency stricter.

Future Subagents reuse that scheduler instead of inventing a second fan-out system.

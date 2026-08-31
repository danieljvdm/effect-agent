---
title: The runtime model
description: How runs alternate between model responses, tool batches, queued input, and policy checks.
---

# The runtime model

Each run repeats one model and tool loop until it produces valid output or stops under policy.

## Terms {#domain-units}

| Unit             | Meaning                                                            |
| ---------------- | ------------------------------------------------------------------ |
| Agent Definition | immutable program, Schemas, Toolkit, instructions, policy          |
| Agent Binding    | one Definition paired with one Effect AI Model                     |
| Run              | one logical request against a Conversation                         |
| Turn             | one model response, optionally followed by one complete Tool batch |
| Attempt          | one durable ownership period advancing a Submission                |
| Conversation     | ordered history shared across Runs                                 |

An ephemeral run has one process attempt. A durable run may span several attempts while keeping
the same turn semantics.

<a id="one-interpreter"></a>

## One agent loop

```text
decode input
  ↓
evaluate instructions and prepare context
  ↓
stream and validate one Effect AI response
  ├─ final → decode output → complete
  └─ tools
       ↓
     preflight the complete tool batch
       ↓
     execute under finite permits
       ↓
     commit results in declaration order
       ↓
     drain steering → evaluate policy → next turn
```

`run`, `stream`, and `start` expose this loop. Durable hosts add canonical commits and
recovery at the same transitions.

<a id="complete-before-consequential"></a>

## Validate before running tools

The engine waits for the full assistant response and all tool arguments before starting a
handler. A truncated response starts no tool. The next model request sees a complete tool batch or
no batch.

<a id="safe-seam-input"></a>

## When queued input is applied

Input may arrive while a model or tool batch is active. Delivery waits for the current work to
finish:

- steering follows the complete response and tool batch;
- follow-up arrives only when the run would stop;
- the initial drain consumes one queued item.

<a id="events-observe-commands-act"></a>

## Events and commands

`RunEvent` subscribers may render, trace, meter, or build projections. Steering, approval,
follow-up, abort, and recovery use command or service interfaces.

To report failures that the model recovers from, install a
[tool failure observer](../guide/run-agents#observe-recovered-tool-failures).

## Limit concurrency {#bounded-concurrency}

Each tool batch runs through a finite `Semaphore`. Progress may follow completion order. Canonical
results keep declaration order. Outer agent, tenant, or platform limits may lower concurrency.
Attached Subagents use the same scheduler.

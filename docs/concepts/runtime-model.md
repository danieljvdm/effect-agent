---
title: The runtime model
description: Follow an agent run through context preparation, model calls, tools, subagents, budgets, and durable recovery.
---

<script setup>
import RuntimeLoop from '../.vitepress/theme/components/RuntimeLoop.vue'
</script>

# The runtime model

A run turns input into Schema-validated output through repeated model requests and tool batches.
The engine prepares context, enforces budgets, and applies new input between turns. A durable host
records progress so another worker can continue the same run after an interruption.

<a id="one-interpreter"></a>

## One agent loop

<RuntimeLoop />

`run`, `stream`, and `start` expose the same interpreter. Each turn contains one model request and
its response, optionally followed by a complete tool batch. A final response can finish the run;
tool results feed the next turn.

## Prepare context and compact {#context-and-compaction}

The runtime decodes input and evaluates the agent's instructions at run start. Before each model
request, it transforms the thread into a prompt, compacts it if needed, then adds the output
contract and the current run status when enabled.

Two token limits serve different purposes:

| Limit               | What it controls                                  |
| ------------------- | ------------------------------------------------- |
| `contextTokenLimit` | The live context for one model request            |
| `tokenBudget`       | Cumulative input and output tokens across the run |

When context is too large, the default compactor first prunes old tool results outside the recent
protected tail. If that is insufficient, it makes one metered summary call. Instructions, protected
input, and tool call/result pairing survive compaction. The summary call consumes run budget too.

Compaction changes what the model sees. The canonical thread log retains its evidence.
Durable hosts record each applied compaction as `CompactionCreated` before using the new view, so
recovery restores the same replacement and coverage. Pruning and summarization cover complete
prior-run records. The optional rollover strategy also covers settled batches inside the current
run, retaining its original instructions and input. A fresh context window preserves the run identity
and cumulative budgets. Native context tools let the model request a rollover and retrieve retained
evidence; an application-owned notes document can carry working state between windows.

See [Context management](../guide/context-management) for prompt transforms, compaction strategies,
context overflow recovery, and model-visible budget status.

<a id="complete-before-consequential"></a>

## Validate before running tools

The model response must finish and all tool arguments must validate before any application handler
starts. A truncated response starts no tool. The engine preflights the complete batch for approval,
authorization, and applicable limits before execution.

Durable hosts commit the validated response before tool preparation. If a worker disappears in
that gap, its replacement can resume the declared batch without asking the model to choose again.

Each application tool result passes through `toolResultBounds` before entering history or storage.
Oversized output becomes a bounded envelope with its head, tail, and original byte count. The
model and durable log receive the same bounded result.

### Limit concurrency {#bounded-concurrency}

Tool handlers run under a finite `Semaphore`. Progress can arrive in completion order, but canonical
results keep the model's declaration order. The next model request sees the complete batch.
Agent, tenant, and platform limits may further reduce concurrency.

## Subagents {#subagents}

Delegation starts a child run in a fresh thread through a tool in the parent's toolkit. The
child repeats the same model/tool loop under its own policy and a reserved allowance. Its projected
result returns to the parent as the delegation tool result; its raw transcript stays private.

The parent joins the child outcome before settling that tool call. Durable recovery preserves the
child's identity, allowance, and committed usage across attempts. See the
[Subagents guide](../guide/subagents) for setup, input and result projections, budgets, authority,
and child lifecycle.

<a id="safe-seam-input"></a>
<a id="when-queued-input-is-applied"></a>

## Apply input between turns

New input can arrive while inference or tools are running. It waits for a safe boundary:

- **Steering** applies after the complete response and tool batch, before the next model request.
- **Follow-up** applies when the run would otherwise stop. A final response may therefore lead to
  another turn when queued input remains.
- **Joined durable input** attaches an accepted submission to the active run at the same boundary
  and settles with that run.

The initial drain consumes one queued item. Queued input never edits an in-flight model request
or tool call. See [Run & stream](../guide/run-agents) for the command interfaces.

## Enforce budgets and finish {#budgets-and-stopping}

Every agent has finite turn, tool call, and duration bounds. Optional token and cost budgets limit
run usage. `completionReserveTokens` holds back token capacity for the final answer. A caller can
tighten a run's turn and tool call allowances without raising the definition's ceiling.

Policy checks govern admission to the next turn and tool batch. Usage checks account for completed
model calls, including compaction. A duration deadline can interrupt work already in flight.
Observed token and cost usage may cross a threshold; these checks do not reserve a provider's
maximum charge before the request.

| Stop condition                                 | Runtime behavior                                                                               |
| ---------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| Valid final output                             | Decode through the output Schema and complete when no queued input continues the run           |
| Successful designated completion tool          | Project its singleton result through the output Schema and complete immediately                |
| Turn, tool call, or token exhaustion           | Follow `onExhaustion`: fail, or produce a constrained final answer with at most one grace turn |
| Duration, cost, or repeated tool failure limit | Fail with a typed policy error                                                                 |
| Abort or interruption                          | End the current execution; durable ownership loss may leave work for a replacement attempt     |

In final-answer mode, an over-budget tool batch starts no handlers. Finalization forbids further
research tools, while allowing a designated completion tool. Results report
`finishReason: "budget-exhausted"` and the exhausted dimension.

See [Budgets & bounded autonomy](./budgets) for exact limits, shared usage budgets, and partial
child results.

## Store history and recover work {#storage-and-recovery}

Persistent history lets a later run reload a thread. Durable execution also tracks accepted
work and owes a terminal settlement even if the process that started it disappears.

| Runtime data                | Responsibility                                                                                          |
| --------------------------- | ------------------------------------------------------------------------------------------------------- |
| Thread log                  | Append-only canonical facts: applied input, model responses, tool outcomes, compaction, and settlements |
| Submission ledger           | Accepted work, queue order, attempt ownership, abort intent, and outstanding settlement obligations     |
| Projections and checkpoints | Rebuildable views and replay optimizations derived from the log                                         |

The Node host uses SQLite; the Cloudflare host uses Durable Objects. The memory adapter supports
tests and in-process storage. The host and adapter together determine the recovery guarantee.

A durable host acknowledges input with a Receipt only after admission, thread materialization,
and readiness are committed. Each attempt writes under a fenced ownership token. A replacement
restores the committed boundary, accounting, and original deadline; it receives no fresh run budget.

::: warning External effects can be uncertain
If an ordinary tool may have acted before its worker disappeared, recovery records an
`UnknownToolOutcome` unless reconciliation establishes the result. It does not automatically replay
the call. Durable Steps can reuse recorded results, but their external execution is at least once
and may repeat. Applications still need idempotency or reconciliation.
:::

See [Persistence & durability](./durability) for execution modes and recovery at each boundary,
or the [Node.js](../platforms/node) and [Cloudflare](../platforms/cloudflare) guides for storage setup.

<a id="events-observe-commands-act"></a>
<a id="events-and-commands"></a>

## Observe and control a run

`RunEvent` subscribers render output, trace execution, meter usage, or build projections. Events
describe progress; steering, follow-up, approval, abort, and recovery use commands or services.
An event stream is not the canonical storage log.

Install a [tool failure observer](../guide/run-agents#observe-recovered-tool-failures) to report
failures that the model recovers from.

## Terms {#domain-units}

| Unit             | Meaning                                                                      |
| ---------------- | ---------------------------------------------------------------------------- |
| Agent Definition | Immutable program: Schemas, Toolkit, instructions, and policy                |
| Agent Binding    | One Definition paired with one Effect AI Model                               |
| Thread           | Ordered history shared across runs                                           |
| Run              | One logical execution request against a thread                               |
| Turn             | One model request and response, optionally followed by a complete tool batch |
| Submission       | Input accepted for durable processing                                        |
| Attempt          | One durable ownership period advancing a submission                          |
| Settlement       | The single durable terminal outcome owed to an accepted submission           |

An ephemeral run lives for one Scope. A durable run may span several attempts while keeping the
same turn semantics.

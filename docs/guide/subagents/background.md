---
title: Durable background subagents
description: Give the parent background tools and send worker findings back as new input.
---

# Durable background subagents

Give the parent tools to start and steer a researcher while it keeps chatting:

```ts twoslash
import * as Subagent from "@effect-agent/capabilities/Subagent";
import { Researcher } from "./researcher.ts";

const background = Subagent.background(Researcher, {
  start: true,
  followUp: true,
  reportToParent: true,
});
```

A start returns the worker reference and input receipt. The parent keeps responding, and a
`WorkerCompletion` message arrives when the child run ends. It contains the projected result
or a bounded failure, the worker and run identities, and a budget-exhaustion flag. Finishing
or aborting the parent run leaves the worker and pending report running.

Reports join an active parent run at an input boundary or start a later run in the same thread.
The framework delivers them separately from the parent's application input: no report tags,
mapper, input union, or extra host registration is required. Existing callers must opt in.

## Send intermediate findings

Declare the update Schema on the Agent once, then enable parent reporting:

<<< @/snippets/travel-planner/background-updates.ts{ts twoslash}

Save as `background-updates.ts`. This example reviews source notes supplied in its input; add your
research tools to its toolkit for live retrieval. The native `emit_update` tool accepts
`{ value: AreaConcern }`. Its acknowledgement retains the finding and lets the child continue.
An update is provisional information, independent of the final hotel result.

Give a coordinator `hotels.toolkit` and provide `hotels.layer`. Register the exact
`HotelResearcher` definition alongside that coordinator, using the host setup below.
With `reportToParent: true`, the parent receives both `WorkerUpdate` and `WorkerCompletion`
without an application input union, mapper, or `reporting` entry. Agents without `updates`
continue to send only completion.

The parent consumes the finding at a safe input boundary or in a later run. It can explain the
concern, ask the user how to proceed, and use follow-up tools to redirect the hotel worker and
other workers to Rosebank. Emission does not wait for a user decision or stop the child.
See [update delivery guarantees](../../reference/subagents#update-delivery-guarantees) for ordering,
backpressure, and recovery.

## Give the parent its tools

<<< @/snippets/travel-planner/background-coordinator.ts{ts twoslash}

Save as `background-coordinator.ts`. This uses the
[activity researcher](./ephemeral-attached#define-the-child) directly.
The default result is `{ output, budgetExhausted }`. Use an explicit `Subagent.make` declaration
when the parent should receive a [custom result projection](../../reference/subagents#input-and-result-mappings).

### Define the parent's input

<<< @/snippets/travel-planner/background-input.ts{ts twoslash}

Save as `background-input.ts`. Instructions and host policy keep the original admitted
application input as their context. For a completion, the framework renders the typed message
instead of calling the application's `inputPrompt` again.

## Connect the host {#connect-both-agents-to-a-durable-host}

<<< @/snippets/travel-planner/background-host.ts{ts twoslash}

Save as `background-host.ts`. Each entry registers an agent and its code versions with the host.
The host discovers reporting from the coordinator's background tools.
The Layers supply tool handlers, provider credentials, and worker access.

The host recovers accepted work and pending reports after restarts. Keep report preparation
free of external side effects: recovery may repeat it before its decision is recorded.

### Authorize the conversation

<<< @/snippets/travel-planner/background-access.ts{ts twoslash}

Save as `background-access.ts`. Worker access denies by default; this local example permits
one user and conversation. In an application, check authenticated identity and thread ownership.

## Run it

<<< @/snippets/travel-planner/background-main.ts{ts twoslash}

Save as `background-main.ts` and run with `node --experimental-transform-types background-main.ts`.
Use the [Node.js setup](../../platforms/node#start-the-host) to submit
`BackgroundCoordinator` with the exported `principal`, `threadId`, and this input:

```json
{ "text": "Find food and walking activities in Lisbon." }
```

Keep the host running so research and report delivery can progress. The
[Cloudflare runtime](../../platforms/cloudflare) supports the same contracts.

## Follow up and cancel {#steer-inspect-and-cancel}

A follow-up joins an active worker run at a safe input boundary or starts a later run.
Opt in to `inspect`, `list`, or `cancel` tools when needed; inspection reads a saved result. Cancellation targets one input's receipt;
it does not close the worker. Several inputs joining one run produce one logical report.
An input cancelled before it starts a run produces no completion message.

Workers share a bounded allocation from their source by default. Host lifetime and concurrency
limits still apply. See [independent budgets](../../reference/subagents#independently-fund-background-runs)
for separately funded work.

For application-driven starts, see the [programmatic API](../../reference/subagents#start-workers-from-application-code).
For delivery failures and recovery, see [report guarantees](../../reference/subagents#completion-report-guarantees).

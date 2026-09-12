---
title: Durable background subagents
description: Give the parent background tools and send worker findings back as new input.
---

# Durable background subagents

Give the parent tools to start and steer a researcher while it keeps chatting:

<<< @/snippets/travel-planner/background-coordinator.ts{ts twoslash}

`ResearchBackground.toolkit` exposes start, follow-up, inspection, listing, and cancellation.
A start returns a worker reference immediately; follow-ups go to that worker's existing thread.
Finishing or aborting the parent run leaves the worker running.

This uses the [activity researcher](./ephemeral-attached#define-the-child) and its `Research`
declaration. Save the code as `background-coordinator.ts`.

## Send findings back {#turn-the-findings-into-parent-input}

<<< @/snippets/travel-planner/background-report.ts{ts twoslash}

Save this as `background-report.ts`. `Subagent.reporting` turns the child's result into **new input
for the parent**. A finished search sends `ResearchFinished`; failure or cancellation sends
`ResearchFailed`. The parent can then explain the findings to the user.

Reports can join an active parent run at an input boundary or start a later run in the same
thread. Report delivery is configured on the host below.

### Define the parent's inputs

<<< @/snippets/travel-planner/background-input.ts{ts twoslash}

Save as `background-input.ts`. User messages and worker reports share this input Schema;
the `_tag` distinguishes them. The child returns `{ activities, partial }` through its result
projection, so the report does not copy private research notes.

## Connect the host {#connect-both-agents-to-a-durable-host}

<<< @/snippets/travel-planner/background-host.ts{ts twoslash}

Save as `background-host.ts`. Each entry registers an agent and its code versions with the host.
`reporting: [researchReport]` connects completed research to the coordinator that started it.
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
{ "_tag": "Message", "text": "Find food and walking activities in Lisbon." }
```

Keep the host running so research and report delivery can progress. The
[Cloudflare runtime](../../platforms/cloudflare) supports the same contracts.

## Follow up and cancel {#steer-inspect-and-cancel}

A follow-up joins an active worker run at a safe input boundary or starts a later run.
Inspection reads progress or a saved result. Cancellation targets one input's receipt;
it does not close the worker. Several inputs joining one run produce one logical report.

Workers share a bounded allocation from their source by default. Host lifetime and concurrency
limits still apply. See [independent budgets](../../reference/subagents#independently-fund-background-runs)
for separately funded work.

For application-driven starts, see the [programmatic API](../../reference/subagents#start-workers-from-application-code).
For delivery failures and recovery, see [report guarantees](../../reference/subagents#completion-report-guarantees).

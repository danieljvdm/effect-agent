---
title: Subagents
description: Declare a child agent, give its tool to a parent, and choose attached or background execution.
---

# Subagents

Give a parent agent a specialist it can call:

<<< @/snippets/travel-planner/subagent-basics.ts{ts twoslash}

The parent calls `summarize` like any other tool. The child gets that call's input, runs with its
own instructions and conversation, and returns `{ output, budgetExhausted }`. Its intermediate
history stays out of the parent's context. Each agent can use its own model and toolkit.

This defines the agents. Choose how to run them below.

## Choose an execution kind

| Kind                                                 | Parent behavior                                                        | Use it when                                                    |
| ---------------------------------------------------- | ---------------------------------------------------------------------- | -------------------------------------------------------------- |
| [Ephemeral attached](./subagents/ephemeral-attached) | Waits for a tool result; child shares its Scope                        | Restarting the task after a process crash is acceptable        |
| [Durable attached](./subagents/durable-attached)     | Suspends, then resumes with the child's result                         | The parent needs the answer and progress must survive restarts |
| [Durable background](./subagents/background)         | Continues; receives findings through inspection or a completion report | The user should keep chatting while work runs                  |

Both attached forms use `Summarize.tool`. Durable execution comes from the host you run them on.
For background work, expose start and follow-up tools instead:

```ts twoslash
import * as Subagent from "@effect-agent/capabilities/Subagent";
import { Summarize } from "./subagent-basics.ts";
// ---cut---
const background = Subagent.background(Summarize, {
  start: true,
  followUp: true,
  inspect: true,
  cancel: true,
});
```

Give the parent `background.toolkit` and provide `background.layer` for its handlers. The
[background guide](./subagents/background) shows how findings become new input to the parent.

## Lifecycle and limits

Attached children can run concurrently, but the parent's next model call waits for the batch to
settle. A durable parent releases its execution slot while waiting and recovers the same child
after a restart. Aborting the parent propagates cancellation to attached children.

Background workers keep running after the parent finishes or aborts. Follow-ups continue the
same child thread. Durable hosts recover their accepted work and pending report delivery.

All three forms enforce permissions and budgets. Parent tools are not inherited. See the
[subagent reference](../reference/subagents) for projections, nested delegation, and limits, or
[durability](../concepts/durability) for recovery of uncertain external actions.

<!-- Keep previously published section links useful after the guide split. -->
<details>
<summary>Looking for a section from the previous guide?</summary>

<a id="start-with-the-child-contract"></a>
<a id="define-the-child"></a>
<a id="define-a-delegation"></a>
<a id="give-the-parent-the-delegation-tool"></a>
<a id="bind-models-and-run"></a>
Child definitions, delegation tools, and model bindings now live in the
[ephemeral attached walkthrough](./subagents/ephemeral-attached).

<a id="keep-children-attached"></a>
Durable registration and recovery now live in the [durable attached guide](./subagents/durable-attached).

<a id="start-and-manage-a-background-worker"></a>
<a id="continue-work-in-the-background"></a>
<a id="deliver-completion-reports"></a>
Starting workers, follow-ups, and replies now live in the [background guide](./subagents/background).

<a id="bound-child-work"></a>
<a id="handle-failures"></a>
<a id="limit-authority"></a>
<a id="bound-nested-delegation"></a>
<a id="independently-fund-background-runs"></a>
<a id="resolve-policies-from-captured-input"></a>
Advanced policies now live in the [subagent reference](../reference/subagents).

<a id="send-messages-through-fixed-peer-routes"></a>
Peer routes now live in [Agent messaging](./messaging).

</details>

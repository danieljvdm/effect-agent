---
title: In-memory attached subagents
description: Run a parent and child in one process, and return the child's findings as a tool result.
---

# In-memory attached subagents

Bind a model to the child, then run the parent:

<<< @/snippets/travel-planner/delegation-live.ts{ts twoslash}

`Coordinator` calls the `Research` tool, waits for its findings, and builds an itinerary.
`Subagent.layer` supplies the child's model and tool handlers. The parent and child can
use different models.

`InMemory.layer` keeps parent and child conversations in memory and shares one reservation
ledger across the parent’s subagents. Provide it once around all child handler Layers, as above.
Reuse the parent's Thread ID for follow-up Runs within that application Scope. Each child has its
own Thread. IDs are generated automatically; context preparation is optional. Process loss loses
this history and active execution.

The files below define `Research`, `Coordinator`, and the sample activity tools. Save them beside
`delegation-live.ts`.

## Define the child

::: code-group

<<< @/snippets/travel-planner/researcher.ts{ts twoslash}

<<< @/snippets/travel-planner/tools.ts{ts twoslash}

:::

`Researcher` receives a city and focus. Its `search_activities` tool uses sample data, so only
the model needs an API key. The child's tool calls stay in its own conversation.

## Expose the child as a tool {#define-a-delegation}

<<< @/snippets/travel-planner/delegation.ts{ts twoslash}

`Subagent.make` uses the child's input Schema as its tool parameters and returns
`{ output, budgetExhausted }`. The parent policy supplies a shared delegation budget, and the
child's own policy can tighten its limits. Expected child failures become bounded
`SubagentExecutionFailure` values without a custom error Schema or mapper.

## Give the parent the delegation tool

<<< @/snippets/travel-planner/coordinator.ts{ts twoslash}

The parent sees the child's output as the tool's answer:

```json
{
  "output": {
    "activities": ["Riverside walk", "Food market"],
    "researchNotes": "Both fit a day of food and walking."
  },
  "budgetExhausted": false
}
```

## Run it {#bind-models-and-run}

<<< @/snippets/travel-planner/delegation-main.ts{ts twoslash}

```sh
export OPENAI_API_KEY="your-api-key"
node --experimental-transform-types delegation-main.ts
```

The child shares the parent's Scope. Interruption stops both; a process restart loses active
execution. Use [durable attached](./durable-attached) when that work needs recovery. Stored history
alone does not make execution durable.

## Customize the result

To expose only selected findings, add `success` and `projectResult` to the declaration.
You can also supply explicit child limits and map failures to an application error.
The [mapping example](../../reference/subagents#input-and-result-mappings) shows all three
customizations after the minimal setup above.

## Failure and limits

One delegation counts as one parent tool call. The child consumes its own reserved allowance.
Set `failureMode: "return"` to give expected child failures to the parent model as data; defects
and interruption retain their Effect meaning.

See [budgets and permissions](../../reference/subagents), or switch to
[background workers](./background) so the parent can continue while children work.

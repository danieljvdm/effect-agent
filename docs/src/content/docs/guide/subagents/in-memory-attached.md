---
title: In-memory attached subagents
description: Run a parent and child in one process, and return the child's findings as a tool result.
---

<a id="in-memory-attached-subagents"></a>

Provide model services to the parent and child, then run the parent:

```ts twoslash title="delegation-live.ts" src="snippets/travel-planner/delegation-live.ts"

```

`Coordinator` calls the `Research` tool, waits for its findings, and builds an itinerary.
`Subagent.layer(Research)` builds the child's tool handlers and requires model services.
`Layer.provideMerge(ModelLive)` supplies that model to the handlers and exposes it to the parent.
To give a child its own model, provide it directly to that child's handler Layer with
`Layer.provide(ChildModel)`.

`InMemory.layer` keeps parent and child conversations in memory and shares one reservation
ledger across the parent’s subagents. Provide it once around all child handler Layers, as above.
Reuse the parent's Thread ID for follow-up Runs within that application Scope. Each child has its
own Thread. IDs are generated automatically; context preparation is optional. Process loss loses
this history and active execution.

The files below define `Research`, `Coordinator`, and the sample activity tools. Save them beside
`delegation-live.ts`.

## Define the child

```ts twoslash title="researcher.ts" src="snippets/travel-planner/researcher.ts"

```

```ts twoslash title="tools.ts" src="snippets/travel-planner/tools.ts"

```

`Researcher` receives a city and focus. Its `search_activities` tool uses sample data, so only
the model needs an API key. The child's tool calls stay in its own conversation.

<a id="define-a-delegation"></a>

## Expose the child as a tool

```ts twoslash title="delegation.ts" src="snippets/travel-planner/delegation.ts"

```

`Subagent.make` uses the child's input Schema as its tool parameters and returns
`{ output, budgetExhausted }`. The parent policy supplies a shared delegation budget, and the
child's own policy can tighten its limits. Expected child failures become bounded
`SubagentExecutionFailure` values without a custom error Schema or mapper.

## Give the parent the delegation tool

```ts twoslash title="coordinator.ts" src="snippets/travel-planner/coordinator.ts"

```

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

<a id="bind-models-and-run"></a>

## Run it

```ts twoslash title="delegation-main.ts" src="snippets/travel-planner/delegation-main.ts"

```

```sh
export OPENAI_API_KEY="your-api-key"
node --experimental-transform-types delegation-main.ts
```

The child shares the parent's Scope. Interruption stops both; a process restart loses active
execution. Use [durable attached](/guide/subagents/durable-attached/) when that work needs recovery. Stored history
alone does not make execution durable.

## Customize the result

To expose only selected findings, add `success` and `projectResult` to the declaration.
You can also supply explicit child limits and map failures to an application error.
The [mapping example](/reference/subagents/#input-and-result-mappings) shows all three
customizations after the minimal setup above.

## Failure and limits

One delegation counts as one parent tool call. The child consumes its own reserved allowance.
Set `failureMode: "return"` to give expected child failures to the parent model as data; defects
and interruption retain their Effect meaning.

See [budgets and permissions](/reference/subagents/), or switch to
[background workers](/guide/subagents/background/) so the parent can continue while children work.

---
title: Ephemeral attached subagents
description: Run a parent and child in one process, and return the child's findings as a tool result.
---

# Ephemeral attached subagents

Bind a model to the child, then run the parent:

<<< @/snippets/travel-planner/delegation-live.ts{ts twoslash}

`Coordinator` calls the `Research` tool, waits for its findings, and builds an itinerary.
`SubagentRuntime.layer` supplies the child's model and tool handlers. The parent and child can
use different models.

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

`Subagent.make` uses the child's input Schema as its tool parameters. This example customizes
the result: `projectResult` returns the activities and a `partial` flag, leaving research notes
in the child's thread. The policy bounds each research task.

For default input and output mapping, only `name` and `{ target: Researcher }` are needed.
See the [minimal declaration](../subagents) or [mapping reference](../../reference/subagents#input-and-result-mappings).

## Give the parent the delegation tool

<<< @/snippets/travel-planner/coordinator.ts{ts twoslash}

The parent sees the projected result as the tool's answer:

```json
{ "activities": ["Riverside walk", "Food market"], "partial": false }
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

## Failure and limits

One delegation counts as one parent tool call. The child consumes its own reserved allowance.
Set `failureMode: "return"` to give expected child failures to the parent model as data; defects
and interruption retain their Effect meaning.

See [budgets and permissions](../../reference/subagents), or switch to
[background workers](./background) so the parent can continue while children work.

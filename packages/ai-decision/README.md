# @effect-agent/ai-decision

Ask typed questions about application state. A `DecisionSet` defines the input and questions;
a `DecisionModel` evaluates them through a provider. Your application owns thresholds,
routing, and side effects.

```text
input + DecisionSet → DecisionModel → typed answers → application action
                           ↑
                     provider Layer
```

```ts
import { DecisionModel, DecisionQuery, DecisionSet } from "@effect-agent/ai-decision";
import { Effect, Schema } from "effect";

const TicketAssessment = DecisionSet.make({
  input: Schema.Struct({ message: Schema.String }),
  questions: {
    department: DecisionQuery.choice({
      instructions: "Which team should handle this ticket?",
      options: { billing: "Payments and refunds", technical: "Bugs and outages" },
    }),
  },
});

const assess = Effect.gen(function* () {
  const model = yield* DecisionModel.DecisionModel;
  const { answers } = yield* model.evaluate(TicketAssessment, {
    message: "Please refund my duplicate charge.",
  });
  return answers.department.choice; // "billing" | "technical"
});
```

Supply a provider Layer such as `TypeSafeDecisionModel.model("jev-latest")` from
[`@effect-agent/ai-typesafe`](../ai-typesafe). The [complete example](../ai-typesafe/examples/decision.ts)
includes provider setup and an application state transition.

Use `choice` for named alternatives, `score` for ordered levels, and `probability` for a yes/no
estimate. Questions in a set evaluate independently against the same schema-encoded input.
Only include input the provider should receive. Returned probabilities are evidence for your
application's policy, not authorization to act.

Read the [guide](https://effect-agent.com/guide/tools#decision-transitions) for the mental model
and the [reference](https://effect-agent.com/reference/decision-models) for query options,
results, errors, and provider behavior.

## Choose a thread's model

`AutoModel.make({ version, models })` builds a catalog of `{ model, description }` profiles.
`select({ threadId, state })` asks the supplied `DecisionModel` to choose one and returns the
original native model plus a schema-backed record. Save that record with the thread, then use
`restore(threadId, record)` for every later run. Each subagent thread can have its own selection.

The host owns atomic thread creation and persistence. Restoring a missing profile, a different
catalog version, or another thread's record fails without making a new selection. Provider
clients, model settings, and dependencies remain on the native model Layers.
See [AutoModel](https://effect-agent.com/reference/decision-models#automodel) for configuration
and a complete Jev example.

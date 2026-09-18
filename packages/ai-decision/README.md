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
Pass it to `Agent.withModel` or `Subagent.layer` to select automatically on each thread's first
turn, including every new subagent. Supply `DecisionModel`, native provider clients, and a shared
`AutoModel.layerMemory()` to retain choices across follow-ups.

Durable hosts provide `AutoModel.SelectionStore` to atomically retain selection records across
restarts. Missing profiles, catalog version mismatches, and wrong-thread records fail without
reselecting. Explicit `select` and `restore` remain available for host-owned admission.
See [AutoModel](https://effect-agent.com/reference/decision-models#automodel) for configuration
and a complete Jev example.

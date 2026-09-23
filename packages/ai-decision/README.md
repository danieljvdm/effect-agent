# @effect-agent/ai-decision

Choose a native Effect language model once per thread with `AutoModel`. Follow-up runs retain
that choice, while each child thread selects independently. The upstream `DecisionModel`
service evaluates the application-approved catalog.

```ts
import { AutoModel } from "@effect-agent/ai-decision";
import { OpenAiLanguageModel } from "@effect/ai-openai";

const ThreadModels = AutoModel.make({
  version: "profiles-v1",
  models: {
    routine: {
      description: "Low cost; routine tasks",
      model: OpenAiLanguageModel.model("gpt-6-luna"),
    },
    complex: {
      description: "Difficult reasoning and ambiguous requirements",
      model: OpenAiLanguageModel.model("gpt-6-astra"),
    },
  },
});
```

Provide `ThreadModels`, a native `DecisionModel` such as
`TypeSafeDecisionModel.model("jev-latest")` from `@effect/ai-typesafe`, provider clients, and one
shared `AutoModel.layerMemory()` around your agent and subagent handlers. At least two profiles
are required. Selection does not acquire candidate models until generation starts.

Durable hosts provide `AutoModel.SelectionStore` to atomically retain version 2 selection records.
Wrong-thread, missing-profile, catalog-version, and record-version mismatches fail without
reselection or mutation. Explicit `select`, `restore`, and `resolve` support host-owned admission.

For ordinary assessments, import `Decision` and `DecisionModel` from `effect/unstable/ai`.
Use `LanguageModelDecisionModel.layer` to answer those decisions with any native language model
that supports structured output. Provide your chosen model Layer to the adapter; provider clients
and configuration remain application-owned. Its probabilities are LLM estimates, not calibrated
confidence scores. Native validation rejects invalid distributions; retries and failover are explicit.
See the [language-model example](examples/language-model.ts) and
[provider setup](https://effect-agent.com/reference/decision-models#language-model-adapter).

See the [reference](https://effect-agent.com/reference/decision-models#automodel) for ownership,
configuration, and migration details, or the runnable [decision](examples/decision.ts),
[direct client](examples/evaluate.ts), and [tool](examples/tool.ts) examples.

# @effect-agent/ai-decision

Evaluate choice, score, and probability questions against explicit state with any decision provider.

```ts
import { DecisionModel, DecisionQuery, DecisionSet } from "@effect-agent/ai-decision";
import { Effect, Schema } from "effect";

const RequestAssessment = DecisionSet.make({
  input: Schema.Struct({ message: Schema.String }),
  questions: {
    task: DecisionQuery.choice({
      instructions: "What kind of task does the message request?",
      options: { math: "Mathematical reasoning", coding: "Writing or debugging code", other: null },
    }),
    difficulty: DecisionQuery.score({
      instructions: "How difficult is this task?",
      levels: ["Routine", "Several reasoning steps", "Substantial specialist reasoning"],
    }),
    wantsSpeed: DecisionQuery.probability({
      instructions: "Does the user explicitly prioritize a fast answer?",
    }),
  },
});

const assess = Effect.gen(function* () {
  const model = yield* DecisionModel.DecisionModel;
  const { answers } = yield* model.evaluate(RequestAssessment, {
    message: "Explain why this integral converges.",
  });
  return {
    task: answers.task.choice, // "math" | "coding" | "other"
    difficulty: answers.difficulty.score, // fractional position from 0 to 2
    wantsSpeed: answers.wantsSpeed.probability, // probability from 0 to 1
  };
});
```

`assess` requires `DecisionModel`; supply an adapter such as
`TypeSafeDecisionModel.model("jev-latest")` from `@effect-agent/ai-typesafe`, with its client Layer.
The shared package depends only on Effect. It supplies neither a generative LanguageModel nor a
state-machine runtime. Compose typed answers with ordinary Effect branching, state, schedules,
and application-owned side effects. See the [compiling state-transition example](../ai-typesafe/examples/decision.ts).

## Questions and evidence

| Type          | Definition                                           | Answer                                                |
| ------------- | ---------------------------------------------------- | ----------------------------------------------------- |
| `choice`      | Named `options` with descriptions                    | Choice and complete probabilities                     |
| `score`       | Ordered `levels` descriptions                        | Probability-weighted score, legend, and probabilities |
| `probability` | A yes/no question, with optional true/false criteria | Probability in [0, 1]                                 |

Questions in one request evaluate independently against the same state. Dependent questions need
another evaluation after updating state. Query constructors preserve choice keys without a type
annotation. Choice needs at least one option, and Score needs at least two levels; evaluation
validates these constraints. Dynamic maps require checking entries for absence.

Probabilities express model evidence; they do not prove correctness. The library supplies no
universal threshold, implicit boolean conversion, or automatic handler dispatch. Application code
owns acceptance, transitions, and authorization. `probability` is the neutral name for TypeSafe's
`noul`; it carries no separate confidence field.

## Reusable sets and explicit state

`DecisionSet.make({ input, questions })` creates a provider-independent definition. Construction
performs no encoding or model I/O. Treat definitions and nested instruction content as readonly.
`model.evaluate(set, input)` accepts the Schema's decoded type and sends its encoded representation
as state. Encoding must produce a string, JSON object, or JSON array. Extra object fields follow
the input Schema's encoding behavior; ordinary Struct schemas omit undeclared fields. Never put
data in a model-visible schema unless the provider is authorized to receive it.

Input encoding requirements, including any caller-owned Scope, remain in the evaluation's Effect
environment. Invalid input, an
unsupported encoded state, or invalid questions fail with `AiError.InvalidRequestError` before
provider I/O. Encoder defects and interruption propagate normally. The set owns no provider
binding, final-result projection, routing policy, or persistence.

The lower-level `model.evaluate({ state, questions })` remains available, including for runtime
catalogues. Query constructors work there too; `choice({ options })` and `score({ levels })`
produce the shared `criteria` representation. Raw definitions can use
`satisfies DecisionSchema.Questions` to retain literal keys. Both entry points use the same
provider callback, response validation, resource scope, and usage accounting.

## Provider-specific evidence

Shared Choice and Score answers retain full distributions without requiring a provider's
confidence formula. Optional `providerMetadata` stores provider-namespaced JSON evidence.
The TypeSafe adapter retains confidence at `result.providerMetadata.typesafe.confidence`,
keyed by question ID; decode the `typesafe` namespace with
`TypeSafeDecisionModel.ProviderMetadata` before using it. The direct TypeSafe HTTP client
continues returning its native `confidence` fields. Provider metadata is untrusted data and
has no cross-provider interpretation.

## Provider contract

`DecisionModel.make({ evaluate })` captures provider services in its construction Effect.
The callback receives a JSON snapshot of `{ state, questions }` and returns untrusted evidence.
The service validates exact answer IDs and types, allowed choices, complete distributions summing
to one within `1e-6` by default, maximal-probability choices, and rubric-consistent weighted scores.
Unexpected fields fail validation. Valid numbers are preserved, never normalized.

A provider adapter can supply `choiceProbabilitySum`, an Effect Schema check, to `DecisionModel.make`
for its bounded Choice rounding rule. This trusted construction option replaces only the Choice
sum check; exact keys, finite [0, 1] values, winning choices, and all Score checks remain enforced.
Response metadata cannot select a different validator. The TypeSafe adapter shares its HTTP client's
check for observed two-decimal Choice totals of `0.99` or `1.01`, bounded by `0.005` per option
and at most one percentage point overall. Higher-precision drift retains the strict tolerance.

Callbacks return `Effect<unknown, AiError, R>`. Request and response failures become native
`AiError` reasons `InvalidRequestError` and `InvalidOutputError`, respectively, with bounded generic
diagnostics. Provider failures keep their native `AiError`; defects and interruption propagate.
Resources acquired by the provider close per call; construction dependencies remain visible.
Use the constructor for provider adapters so validation cannot be skipped accidentally.

There are no default retries or deadlines. Compose Effect policies explicitly. Each successful
result reports `provider`, resolved `model`, and `usage` with nullable `inputTokens`/`outputTokens`.
Usage is separate from an agent Run's language-model budgets. Hosts own billing controls.
The `DecisionModel.evaluate` span adds no state, question, answer, or credential logging.

Public imports are `DecisionModel`, `DecisionQuery`, `DecisionSchema`, and `DecisionSet` from
the package root, or the matching `decision-model`, `decision-query`, `decision-schema`, and
`decision-set` subpaths.

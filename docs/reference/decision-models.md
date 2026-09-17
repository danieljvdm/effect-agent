---
title: Decision models
description: Query options, typed results, Jev configuration, probability validation, and errors.
---

# Decision models

Start with the [decision guide](../guide/tools#decision-transitions) for the ownership model
and a first evaluation. The [complete example](https://github.com/danieljvdm/effect-agent/blob/main/packages/ai-typesafe/examples/decision.ts)
includes all three query types and provider setup.

## Queries

Import `DecisionQuery` from `@effect-agent/ai-decision`. Every constructor requires
`instructions`: a string, JSON object, or JSON array.

| Constructor   | Other options                                                                               | Answer fields                      |
| ------------- | ------------------------------------------------------------------------------------------- | ---------------------------------- |
| `choice`      | `options`: at least one named option, described by a string or `null` to use its name alone | `choice`, `probabilities`          |
| `score`       | `levels`: at least two ordered string descriptions                                          | `score`, `legend`, `probabilities` |
| `probability` | Optional `criteria` with optional `true` and `false` descriptions                           | `probability`                      |

Every answer has a matching `type`. Choice keys infer the selected option's literal union.
Score is a probability-weighted, zero-indexed position: three levels allow scores from 0 to 2,
including fractions. Its legend and probabilities use stringified level indexes. A probability
answer estimates a yes/no proposition in [0, 1], with no automatic threshold.

For example, add a rating and a yes/no question to a set's `questions`:

```ts twoslash
import { DecisionQuery } from "@effect-agent/ai-decision";

const questions = {
  severity: DecisionQuery.score({
    instructions: "How much work is blocked?",
    levels: ["None", "Some work", "All work"],
  }),
  urgent: DecisionQuery.probability({
    instructions: "Does this need immediate attention?",
  }),
};
```

## Evaluation

`DecisionSet.make({ input, questions })` pairs an input Schema with reusable questions.
Construction performs no model I/O; input encoding and query validation happen at evaluation.
Treat definitions, including nested instruction content, as readonly.

| Call on the `DecisionModel` service | Input                                                                       |
| ----------------------------------- | --------------------------------------------------------------------------- |
| `evaluate(set, input)`              | The Schema's decoded type; its encoded value becomes provider-visible state |
| `evaluate({ state, questions })`    | State supplied directly as a string, JSON object, or JSON array             |

Input encoding services remain requirements of the returned Effect. Encoded state must have
one of the supported shapes. Extra fields follow the Schema's encoding behavior; ordinary
Struct schemas omit undeclared fields. Include only data the provider should receive.

Raw questions use `criteria` for choice options and score levels. Use
`satisfies DecisionSchema.Questions` to retain literals when defining raw questions separately.
With dynamic question or option maps, check for missing entries and narrow mixed answers by `type`.

## Results and evidence

| Field                                     | Meaning                                                        |
| ----------------------------------------- | -------------------------------------------------------------- |
| `answers`                                 | Typed answers keyed by question ID                             |
| `provider`                                | Provider identifier; `"typesafe"` for the Jev adapter          |
| `model`                                   | Returned model identifier, which may resolve a requested alias |
| `usage.inputTokens`, `usage.outputTokens` | Nonnegative token counts, or `null` when unavailable           |
| `providerMetadata`                        | Optional provider-namespaced JSON evidence                     |

Usage is returned to the caller and is separate from an agent Run's language-model budgets.
The caller owns accounting, acceptance thresholds, and authorization.

Jev's choice and score confidence values are retained at
`result.providerMetadata.typesafe.confidence[questionId]`. Decode the `typesafe` namespace with
`TypeSafeDecisionModel.ProviderMetadata` before using it. Confidence has provider-specific
meaning; it is not a correctness guarantee. Probability questions have no separate confidence.

### Probability validation

Responses must match the submitted question IDs, kinds, and criteria. A choice must have
maximal probability; ties are allowed. Distributions must be complete, and each probability
must be finite and in [0, 1]. Unexpected response fields are rejected.

| Check                    | Behavior                                                                                                              |
| ------------------------ | --------------------------------------------------------------------------------------------------------------------- |
| Default distribution sum | Must equal 1 within `1e-6`                                                                                            |
| Jev choice rounding      | Two-decimal values may total `0.99` or `1.01`, within `0.005` per option and at most `0.01` total error               |
| Score                    | Sum retains the `1e-6` tolerance; the score must match its weighted level within `1e-6` times the highest level index |

The Jev rounding allowance applies only when every choice probability is representable to
two decimal places. Higher-precision values keep the default tolerance. A single option at
`0.99` is invalid. Both the direct client and adapter preserve reported values without normalization.

## TypeSafe client

`TypeSafeDecisionModel.model(model)` supplies a `DecisionModel` Layer requiring `TypeSafeClient`.
Configure it with an Effect HttpClient:

```ts twoslash
import { TypeSafeClient, TypeSafeDecisionModel } from "@effect-agent/ai-typesafe";
import { Layer } from "effect";
import { FetchHttpClient } from "effect/unstable/http";

const DecisionLive = TypeSafeDecisionModel.model("jev-latest").pipe(
  Layer.provide(TypeSafeClient.layerConfig().pipe(Layer.provide(FetchHttpClient.layer))),
);
```

| Client option     | `make` / `layer`                | `layerConfig`                       | Default                                                              |
| ----------------- | ------------------------------- | ----------------------------------- | -------------------------------------------------------------------- |
| `apiKey`          | Required `Redacted<string>`     | Optional `Config<Redacted<string>>` | `layerConfig` reads `TYPESAFE_API_KEY`                               |
| `apiUrl`          | Optional string                 | Optional `Config<string>`           | `https://api.typesafe.ai/v1`                                         |
| `transformClient` | Optional HttpClient transformer | Same                                | No transformation; applied after authentication and status filtering |

Client acquisition sends no requests. Evaluations send `POST /systemone` with bearer
authentication. The versioned base URL can be overridden for a proxy.

### Direct evaluation

`TypeSafeClient.evaluate({ model, state, questions })` exposes Jev's native API.
Use `choice`, `score`, and `noul` question types, with `criteria` for choice options or score
levels. Choice and score answers include `confidence`; noul answers contain `noul` in [0, 1].
Usage uses `input_tokens` and `output_tokens`. `TypeSafeSchema` exposes the native schemas.
The shared adapter translates `probability` to `noul` and moves confidence into provider metadata.

See the [direct evaluation example](https://github.com/danieljvdm/effect-agent/blob/main/packages/ai-typesafe/examples/evaluate.ts)
for mixed questions, bounded retries, and an overall timeout.

## Errors and policies

Evaluations fail with `AiError`. Invalid local input is rejected before provider I/O.

| Failure                                                      | `AiError` reason                     |
| ------------------------------------------------------------ | ------------------------------------ |
| Invalid input or questions; TypeSafe HTTP 422                | `InvalidRequestError`                |
| Malformed response or answers that disagree with the request | `InvalidOutputError`                 |
| TypeSafe HTTP 401                                            | `AuthenticationError` (`InvalidKey`) |
| TypeSafe HTTP 429                                            | `RateLimitError`                     |
| TypeSafe HTTP 529                                            | `InternalProviderError`              |
| HTTP transport failure                                       | `NetworkError`                       |

Other HTTP statuses use Effect AI's status mapping. `layerConfig` can also fail with
`ConfigError`. Defects and interruption propagate normally; cancellation reaches the supplied
HttpClient, including response-body reads.

There are no default retries or deadlines. Compose `Effect.retry` and `Effect.timeout`, or
configure HTTP policies through `transformClient`.

TypeSafe HTTP errors retain status, headers, and provider error text. Credentials are redacted,
but provider error text may contain submitted content. The integration adds no body logging;
the supplied HttpClient and application control HTTP tracing.

## Provider adapters

Build adapters with `DecisionModel.make({ evaluate })`. The constructor captures provider
dependencies and validates the returned evidence; resources acquired during evaluation close
per call. An optional `choiceProbabilitySum` Schema check supplies a provider's choice rounding
rule while retaining the other response checks. See the
[constructor API comments](https://github.com/danieljvdm/effect-agent/blob/main/packages/ai-decision/src/DecisionModel.ts)
for the adapter contract.

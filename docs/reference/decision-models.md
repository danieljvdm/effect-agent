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

## AutoModel

Pass an AutoModel catalog to `Agent.withModel` or `Subagent.layer`. The runtime selects before
each thread's first model call and keeps that choice for later turns and follow-up runs. Each
new subagent selects independently from its delegated task.

<<< @/snippets/travel-planner/auto-model.ts#catalog{ts twoslash}

Each profile pairs a native Effect model Layer with a description. Configure reasoning effort
and provider options on that Layer; describe capability, cost, and appropriate tasks in the
catalog. Supply Jev through [`TypeSafeDecisionModel`](#typesafe-client), or use another
`DecisionModel` implementation.

```ts twoslash
import { Assistant, Research, ThreadModels } from "./auto-model.ts";
import { Agent, Subagent } from "effect-agent";
// ---cut---
const BoundAssistant = Agent.withModel(Assistant, ThreadModels);
const ResearchLive = Subagent.layer(Research, ThreadModels);
```

Provide one `AutoModel.layerMemory()` alongside `InMemory.layer`, outside the parent program
and child handler Layers. The shared store keys choices by thread ID, so siblings use the same
catalog without sharing a selection. The selected native model retains its provider identity,
client requirements, streaming, tools, and structured output.

<<< @/snippets/travel-planner/auto-model.ts#runs{ts}

The [complete example](https://github.com/danieljvdm/effect-agent/blob/main/docs/snippets/travel-planner/auto-model.ts)
includes Jev, provider clients, and shared Layer assembly.

### Thread ownership

The runtime supplies the rendered prompt and eligible tool descriptions to the resolver after
input validation and `inputPrompt` projection, before context preparation. Raw input fields
excluded by that projection are excluded from selection too. Selection is inside the run's
deadline and interruption scope. Context hooks may prepare prompts but cannot replace an
AutoModel binding through `modelCall`; doing so fails with `AiError.InvalidRequestError`.

`AutoModel.SelectionStore.getOrCreate(threadId, select)` owns atomic creation and retention.
It returns the committed winning `SelectionRecord` before generation starts. Concurrent
resolutions of the same thread share that choice; independent threads can select concurrently.
Failed or interrupted selections may retry. A crash before commitment can repeat a selector
request. Generative failure after commitment does not change the chosen model.

`layerMemory({ capacity? })` retains choices for one Layer lifetime. Capacity defaults to 10,000
distinct attempted thread IDs; entries never expire or evict. New threads fail at capacity while
existing choices remain available. Rebuilding the Layer loses selections. Durable hosts must
provide a `SelectionStore` backed by application-owned thread storage, preserving records across
restarts. Keep model capacity and pricing configuration aligned with the selected profiles.

Stable model settings avoid implicit changes that can reduce prompt-cache reuse; cache hits
still depend on provider behavior and prompt prefixes.

### Configuration and records

| API or field                               | Behavior                                                                                     |
| ------------------------------------------ | -------------------------------------------------------------------------------------------- |
| `make({ models, version, instructions? })` | Snapshots approved model profiles; construction performs no I/O                              |
| `version`                                  | Required nonempty version; change when model, effort, or settings change                     |
| `instructions`                             | Optional Choice policy; defaults to the least expensive capable profile for the whole task   |
| `resolve({ threadId, state })`             | Runtime integration; uses `SelectionStore`, `DecisionModel`, and native client services      |
| `layerMemory({ capacity? })`               | Bounded shared selection storage for ephemeral hosts                                         |
| `SelectionStore`                           | Host storage port; durable implementations must atomically retain the winning record         |
| `select({ threadId, state })`              | Explicit one-shot Choice evaluation for hosts that own admission; re-execution selects again |
| `restore(threadId, record)`                | Validates stored data and returns the native model without decision-provider I/O             |
| `SelectionRecord`                          | Schema for format version, thread ID, catalog version, profile ID, and decision evidence     |

Use `Schema.encodeEffect(AutoModel.SelectionRecord)` at storage boundaries; never serialize
model Layers. Retain previous catalogs for active threads. Wrong-thread, malformed, missing-profile,
or catalog-version mismatches fail with `AiError.InvalidRequestError` rather than reselecting.
Jev's reported probability precision is retained in the record.

The host must restrict candidates to compatible, authorized profiles and send only task context
the decision provider may receive. Selection has no implicit retries, fallbacks, or confidence
thresholds. Selector usage and metadata live in `record.decision`, separately from generative run
accounting. `AutoModel.select` tracing records the profile ID without adding task-body logging.

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
`TypeSafeClient.layer` requires `TypeSafeClient.Config` and an Effect `HttpClient`:

```ts twoslash
import { TypeSafeClient, TypeSafeDecisionModel } from "@effect-agent/ai-typesafe";
import { Layer } from "effect";
import { FetchHttpClient } from "effect/unstable/http";

const DecisionLive = TypeSafeDecisionModel.model("jev-latest").pipe(
  Layer.provide(TypeSafeClient.layer),
  Layer.provide(TypeSafeClient.Config.layer),
  Layer.provide(FetchHttpClient.layer),
);
```

| Config service field | Type                        | `TypeSafeClient.Config.layer` source                           |
| -------------------- | --------------------------- | -------------------------------------------------------------- |
| `apiKey`             | Required `Redacted<string>` | `TYPESAFE_API_KEY`                                             |
| `apiUrl`             | Optional string             | `TYPESAFE_API_URL`, defaulting to `https://api.typesafe.ai/v1` |

For application-owned configuration, supply `TypeSafeClient.Config` with `Layer.effect` or
`Layer.succeed`. `make` is an Effect and `layer` is a Layer; neither takes configuration arguments.
Both capture their requirements at construction, so evaluation only requires the client service.

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

Other HTTP statuses use Effect AI's status mapping. `TypeSafeClient.Config.layer` can fail with
`ConfigError`. Defects and interruption propagate normally; cancellation reaches the supplied
HttpClient, including response-body reads.

There are no default retries or deadlines. Compose `Effect.retry` and `Effect.timeout`, or
customize the HttpClient requirement with `Layer.updateService` before providing the transport:

```ts twoslash
import { TypeSafeClient } from "@effect-agent/ai-typesafe";
import { flow, Layer, Schedule } from "effect";
import { FetchHttpClient, HttpClient } from "effect/unstable/http";

const ClientLive = TypeSafeClient.layer.pipe(
  Layer.updateService(
    HttpClient.HttpClient,
    flow(
      HttpClient.filterStatusOk,
      HttpClient.retryTransient({ times: 1, schedule: Schedule.spaced("20 millis") }),
    ),
  ),
  Layer.provide(TypeSafeClient.Config.layer),
  Layer.provide(FetchHttpClient.layer),
);
```

Filter statuses before retrying to share one retry budget across HTTP and transport failures.
Request middleware on the supplied client receives the authenticated, absolute request URL.

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

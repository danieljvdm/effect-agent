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

Choose a native model once when a thread starts, then reuse it for every turn and follow-up run:

<<< @/snippets/travel-planner/auto-model.ts#catalog{ts twoslash}

Import `AutoModel` from `@effect-agent/ai-decision`. Supply Jev with the
[`TypeSafeDecisionModel` Layer](#typesafe-client); another `DecisionModel` implementation works
with the same catalog. Each profile pairs an existing native Effect model Layer with a description.
Configure reasoning effort and other provider settings on that Layer. Descriptions should explain
capability, cost, and when a profile is appropriate; there is no built-in model catalog.

```ts twoslash
import { ThreadModels } from "./auto-model.ts";
import { Effect } from "effect";
// ---cut---
const createSelection = ThreadModels.select({
  threadId: "research-thread",
  state: {
    task: "Investigate why invoice totals differ from the ledger",
    tools: ["read_document", "calculate"],
    constraints: ["Explain each discrepancy"],
  },
});

const reuseSelection = Effect.gen(function* () {
  const selected = yield* createSelection;
  // Save selected.record in the host's thread metadata before starting work.
  return yield* ThreadModels.restore("research-thread", selected.record);
});
```

Bind `selected.model` using `Agent.withModel(agent, selected.model)`. It is the original model,
so its identity, client requirements, streaming, tools, and structured output remain native.
The [complete example](https://github.com/danieljvdm/effect-agent/blob/main/docs/snippets/travel-planner/auto-model.ts)
includes provider Layers and two runs on the same thread.

### Thread ownership

The host atomically saves `selected.record` with its thread metadata before generation. On a
follow-up or restart, load that record and call `restore`; it requires no decision provider.
Concurrent thread creators must use the stored winning record before either starts work. A crash
before commitment can require another selector call; this API makes no exactly-once execution claim.

Select separately for each subagent thread using its delegated task and available tools. Follow-ups
to that child restore its original selection. Keep selection at the host's thread-creation boundary;
selecting while constructing a shared `Subagent.layer` would give all its children the same choice.
For durable hosts, retain the record in application-owned admission/thread data and restore the
model through the existing [resolved model context](../guide/context-management#resolve-routing-and-capacity-together).
Keep context limits and pricing aligned with that same profile.

Stable model settings avoid implicit model changes that can reduce prompt-cache reuse; cache hits
still depend on provider behavior and prompt prefixes. AutoModel neither changes prompts nor
adds generation-time routing. Re-executing `select` makes a new decision, so retain the record
instead of calling it on each run.

### Configuration and records

| API or field                               | Behavior                                                                                   |
| ------------------------------------------ | ------------------------------------------------------------------------------------------ |
| `make({ models, version, instructions? })` | Pure catalog construction; profiles and model bindings are snapshotted                     |
| `version`                                  | Required nonempty application version for the catalog's model configuration                |
| `instructions`                             | Optional Choice policy; defaults to the least expensive capable profile for the whole task |
| `select({ threadId, state })`              | One Choice evaluation; requires `DecisionModel`, fails with `AiError`                      |
| `restore(threadId, record)`                | Validates unknown stored data and returns the configured native model without I/O          |
| `SelectionRecord`                          | Schema for format version, thread ID, catalog version, profile ID, and decision evidence   |

Use `Schema.encodeEffect(AutoModel.SelectionRecord)` at storage boundaries. Model Layers are live
definitions and are not serialized. Change the catalog version whenever a profile's model, effort,
or settings change, and retain previous catalogs for active threads. A missing profile, version
mismatch, wrong thread, or malformed record fails with `AiError.InvalidRequestError`; restoration
never substitutes another profile. Jev's reported probability precision is retained in the record.

The host must authorize the thread and restrict candidates to compatible, approved profiles.
Selection is not an authorization decision. Only send task context the decision provider may receive.
State should include the whole task, relevant documents, constraints, and available tools.

Selection has no default retries, deadlines, fallbacks, or confidence threshold. Apply Effect
policies before committing the record. Selector usage and provider metadata live in
`record.decision`, separately from generative run accounting. `AutoModel.select` tracing records
the chosen profile ID; it adds no task-body logging.

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

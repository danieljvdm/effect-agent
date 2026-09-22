---
title: Decision models
description: Native Effect decisions, TypeSafe configuration, and thread-owned model selection.
---

# Decision models

Use `Decision` and `DecisionModel` from `effect/unstable/ai` to evaluate schema-defined input.
The model returns evidence; application code owns thresholds, routing, and side effects.
Start with the [decision guide](../guide/tools#decision-transitions) or the
[complete example](https://github.com/danieljvdm/effect-agent/blob/main/packages/ai-decision/examples/decision.ts).

## Definitions and answers

`Decision.make({ input, decisions })` pairs an input Schema with named decisions.
`DecisionModel.decide(definition, { input })` encodes that input using `Schema.toCodecJson`
and answers all decisions in one provider call. Encoding services remain visible in the Effect's
requirements. Include only data the provider should receive.

| Constructor            | Required options besides string `instructions`                | Answer                                                    |
| ---------------------- | ------------------------------------------------------------- | --------------------------------------------------------- |
| `Decision.classify`    | `criteria`: at least two labels mapped to string descriptions | `label`, `probabilities`, optional `confidence`           |
| `Decision.rate`        | `criteria`: at least two distinct ordered string levels       | `rating`, `label`, `probabilities`, optional `confidence` |
| `Decision.probability` | None; optional `criteria` describes both `false` and `true`   | `probability`                                             |

Classification labels and rating levels infer literal unions. Ratings can be fractional,
from zero to the last level's index; their probability keys are the level strings. The rating
label is the most probable level, choosing the first on ties. Probability answers are in [0, 1].
Probability criteria are optional; when supplied, describe both outcomes. Code inspecting a
`Decision.Probability` must check whether `criteria` is present before reading it.
Constructors throw for empty decision sets or invalid criteria counts; define valid static
assessments before executing them.

```ts twoslash
import { Schema } from "effect";
import { Decision, DecisionModel } from "effect/unstable/ai";

const Urgency = Decision.make({
  input: Schema.Struct({ message: Schema.String }),
  decisions: {
    urgent: Decision.probability({
      instructions: "Does this need immediate attention?",
    }),
  },
});

const assessment = DecisionModel.decide(Urgency, {
  input: { message: "Our production deployment is blocked." },
});
```

Results contain `answers` keyed by decision name and `usage.inputTokens` / `usage.outputTokens`.
Unreported token counts are `undefined`. Usage is separate from an agent Run's language-model
budgets. Classification and rating confidence is optional, provider-defined evidence, not a
correctness guarantee. Provider and resolved model identifiers are not part of the response.

## Probability validation

Native `DecisionModel` validates required answers, kinds, labels, finite probabilities in [0, 1],
and distribution sums within `1e-6` of 1. Classification may return a label that does not have the
highest probability. Ratings must lie within the scale; the core derives their labels from the
distribution. Application acceptance policies remain explicit.

Effect rc.117 does not accept the former adapter's rounded totals of `0.99` or `1.01`.
Reported probabilities are preserved without normalization; those totals fail with
`AiError.InvalidOutputError`.

## AutoModel

AutoModel is a native model Layer: provide it to satisfy an agent's model requirement.
The runtime selects before each thread's first model call and keeps that choice for later turns
and follow-up runs. Each new subagent selects independently from its delegated task.

<<< @/snippets/travel-planner/auto-model.ts#catalog{ts twoslash}

Import `AutoModel` from `@effect-agent/ai-decision`. At least two profiles are required.
Each profile pairs a native Effect model Layer with a description. Configure reasoning effort
and provider options on that Layer; describe capability, cost, and appropriate tasks in the
catalog. Supply Jev through [`TypeSafeDecisionModel`](#typesafe-client), or use another
`DecisionModel` implementation.

```ts twoslash
import { Assistant, Research, ThreadModels } from "./auto-model.ts";
import { Effect, Layer } from "effect";
import { AgentRuntime, Subagent } from "effect-agent";
// ---cut---
const program = AgentRuntime.run(Assistant, "Compare train and bus travel.").pipe(
  Effect.provide(ThreadModels),
);
const ResearchLive = Subagent.layer(Research).pipe(Layer.provide(ThreadModels));
```

Provide one `AutoModel.layerMemory()` alongside `InMemory.layer`, outside the parent program
and child handler Layers. The shared store keys choices by thread ID, so siblings use the same
catalog without sharing a selection. The selected native model retains its provider identity,
client requirements, streaming, tools, and structured output. Building the AutoModel Layer
captures dependencies without selecting or acquiring any candidate. It requires an agent thread;
for direct `LanguageModel` calls, explicitly `resolve` a thread and provide the returned model.

<<< @/snippets/travel-planner/auto-model.ts#runs{ts}

The [complete example](https://github.com/danieljvdm/effect-agent/blob/main/docs/snippets/travel-planner/auto-model.ts)
includes Jev, provider clients, and shared Layer assembly.

### Thread ownership

The runtime supplies the rendered prompt and eligible tool descriptions to the resolver after
input validation and `inputPrompt` projection, before context preparation. Raw input fields
excluded by that projection are excluded from selection too. Selection is inside the run's
deadline and interruption scope. Context hooks may prepare prompts but cannot replace the
selected model through `modelCall`; doing so fails with `AiError.InvalidRequestError`.

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

| API or field                               | Behavior                                                                                                  |
| ------------------------------------------ | --------------------------------------------------------------------------------------------------------- |
| `make({ models, version, instructions? })` | Native Model Layer over approved profiles; construction performs no selection                             |
| `version`                                  | Required nonempty version; change when model, effort, or settings change                                  |
| `instructions`                             | Optional string classification policy; defaults to the least expensive capable profile for the whole task |
| `resolve({ threadId, state })`             | Explicit resolution using `SelectionStore`, `DecisionModel`, and native client services                   |
| `layerMemory({ capacity? })`               | Bounded shared selection storage for ephemeral hosts                                                      |
| `SelectionStore`                           | Host storage port; durable implementations must atomically retain the winning record                      |
| `select({ threadId, state })`              | Explicit one-shot classification for hosts that own admission; re-execution selects again                 |
| `restore(threadId, record)`                | Validates stored data and returns the native model without decision-provider I/O                          |
| `SelectionRecord`                          | Schema for format version, thread ID, catalog version, profile ID, and decision evidence                  |

Use `Schema.encodeEffect(AutoModel.SelectionRecord)` at storage boundaries; never serialize
model Layers. Retain previous catalogs for active threads. Wrong-thread, malformed, missing-profile,
or catalog-version mismatches fail with `AiError.InvalidRequestError` rather than reselecting.
New selections use record version 2: `decision.answers.model` contains `label`,
`probabilities`, and optional `confidence`; `decision.usage` contains optional token counts.
Version 1 records are rejected without mutation or reselection. Hosts with active version 1
threads must retain their previous runtime and catalog until those threads finish, or implement
an explicit, data-preserving upgrade in their storage adapter.

The host must restrict candidates to compatible, authorized profiles and send only task context
the decision provider may receive. Selection has no implicit retries, fallbacks, or confidence
thresholds. Selector usage and confidence live in `record.decision`, separately from generative run
accounting. `AutoModel.select` tracing records the profile ID without adding task-body logging.

## TypeSafe client

Install `@effect/ai-typesafe` at the same rc as Effect. Its `TypeSafeDecisionModel.model(model)`
provides the native `DecisionModel` and provider/model identity. It does not provide a
`LanguageModel`.

```ts twoslash
import { TypeSafeClient, TypeSafeDecisionModel } from "@effect/ai-typesafe";
import { Layer } from "effect";
import { FetchHttpClient } from "effect/unstable/http";

const DecisionLive = TypeSafeDecisionModel.model("jev-latest").pipe(
  Layer.provide(TypeSafeClient.layerConfig()),
  Layer.provide(FetchHttpClient.layer),
);
```

`TypeSafeClient.layerConfig()` reads `TYPESAFE_API_KEY`. To read a custom base URL, pass
`{ apiUrl: Config.string("TYPESAFE_API_URL") }`; the default is `https://api.typesafe.ai/v1`.
Use `TypeSafeClient.layer({ apiKey, apiUrl, transformClient })` for explicit configuration.
The API key is a `Redacted<string>`. Construction performs no requests.

For low-level access, obtain `TypeSafeClient.TypeSafeClient` and call
`client.systemOne({ model, state, questions })` or `client.listModels()`.
`TypeSafeSchema` exposes their wire schemas. System One uses `choice`, `score`, and `noul`
questions. Use `DecisionModel.decide` for request-derived answer types and validated
probability distributions; `systemOne` returns the provider's wire answer union.
The [direct example](https://github.com/danieljvdm/effect-agent/blob/main/packages/ai-decision/examples/evaluate.ts)
shows bounded retries and a timeout.

## Errors and policies

Native decision input encoding failures use `AiError.InvalidUserInputError`; invalid answers
use `AiError.InvalidOutputError`. TypeSafe maps HTTP failures to typed `AiError` reasons,
including authentication, rate limiting, and provider failures. Configuration can fail with
`ConfigError`. Defects and interruption propagate.

There are no automatic retries or deadlines. Compose `Effect.retry` and `Effect.timeout`,
or configure HTTP policies through the client's `transformClient` option or
`TypeSafeConfig.withClientTransform`. Rate-limit errors retain retry delays when available.
HTTP error text may include submitted content; the host controls tracing and logging.

## Migrating from the local packages

| Former API                                             | Native API                                                 |
| ------------------------------------------------------ | ---------------------------------------------------------- |
| `DecisionSet.make({ input, questions })`               | `Decision.make({ input, decisions })`                      |
| `DecisionQuery.choice({ options })`                    | `Decision.classify({ criteria })`                          |
| `DecisionQuery.score({ levels })`                      | `Decision.rate({ criteria })`                              |
| `DecisionQuery.probability`                            | `Decision.probability`, with optional outcome descriptions |
| `model.evaluate(set, input)`                           | `DecisionModel.decide(definition, { input })`              |
| Choice `.choice` / score `.score`                      | Classification `.label` / rating `.rating`                 |
| `@effect-agent/ai-typesafe`                            | `@effect/ai-typesafe`                                      |
| `TypeSafeClient.Config.layer` + `TypeSafeClient.layer` | `TypeSafeClient.layerConfig()`                             |
| `client.evaluate(request)`                             | `client.systemOne(request)`                                |

`@effect-agent/ai-decision` now exports only `AutoModel`. Import the shared decision APIs directly
from Effect; provider integrations come directly from upstream. Custom providers implement
`DecisionModel.make({ decide })`, returning tagged provider answers and usage.

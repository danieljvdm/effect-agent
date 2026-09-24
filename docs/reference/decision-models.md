---
title: Decision models
description: Native Effect decisions, durable decision turns, model adapters, and thread-owned model selection.
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
Unreported token counts are `undefined`. Standalone calls do not enter an agent Run's usage
accounting; [durable decision turns](#durable-decision-turns) do. Classification and rating
confidence is optional, provider-defined evidence, not a correctness guarantee. Provider and
resolved model identifiers are not part of the response.

## Durable decision turns

Import `DecisionTurn` from `effect-agent` to let a durable Run classify one eligible Turn and
project the answer through an existing Tool. Construct it with
`DecisionTurn.make(agent, { version, decision, model, tool, prepare, project })` and pass it as
`decisionTurn` on the agent's durable registration. The definition contains one `route`
classification; `tool` must be the exact Tool in that agent's Toolkit. The registered
LanguageModel remains responsible for ordinary generation.
Turns reserved for required completion or budget finalization stay with the LanguageModel.

`prepare` receives the original Run input and the current assembled Prompt, including steering
and settled Tool results. It returns only the state the decision provider may receive. Keep it
read-only, and keep `project` pure: actions belong in the Tool handler. The decision model is
acquired only when preparation returns a state and is scoped to the current Attempt.

- `prepare` returning `None` skips inference and allows preparation on a later ordinary Turn.
- `project` returning `None` commits a metered abstention and continues with the LanguageModel.
  An abstention consumes the decision slot, just as a projected Tool call does.

A projection supplies host-authored text and parameters for the fixed Tool. Current Tool
visibility, parameter validation, authorization, approvals, budgets, and receipts still apply.
Decision usage counts toward the Run with the actual decision provider and model identity.
Budget-rejected projections retain the decision and failed Tool result atomically before continuing.

The canonical decision consumes the slot for the rest of the Run, including after checkpoint
retirement and recovery. A failure before that append may repeat inference. An unresolved
ordinary Tool operation still requires reconciliation after ownership loss; selecting it through
a decision does not make its external effects exactly once.

Change `version` when preparation, projection, or model configuration changes their meaning.
The contract hashes declared schemas and decisions, not callback code or the model Layer.
Compiled registrations include that contract automatically. With low-level
`DurableWorkerBinding.make`, include `yield* decisionTurn.contract` in the agent declaration
before hashing the supplied digests; the constructor does not rewrite them. Construct the
descriptor against the exact registered Agent Definition, including any policy overrides.

Recovery checkpoints are disposable: an incompatible cache is rebuilt from canonical history.
This applies to Runs without a decision descriptor too; canonical history is preserved.

## Probability validation

Native `DecisionModel` validates required answers, kinds, labels, finite probabilities in [0, 1],
and distribution sums within `1e-6` of 1. Classification may return a label that does not have the
highest probability. Ratings must lie within the scale; the core derives their labels from the
distribution. Application acceptance policies remain explicit.

Effect rc.117 does not accept the former adapter's rounded totals of `0.99` or `1.01`.
Reported probabilities are preserved without normalization; those totals fail with
`AiError.InvalidOutputError`.

## Language model adapter

Provide `LanguageModelDecisionModel.layer` with any native language model that supports structured output.

```ts twoslash
import { LanguageModelDecisionModel } from "@effect-agent/ai-decision";
import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai";
import { Config, Effect, Layer, Schema } from "effect";
import { Decision, DecisionModel } from "effect/unstable/ai";
import { FetchHttpClient } from "effect/unstable/http";

const Sentiment = Decision.make({
  input: Schema.String,
  decisions: {
    tone: Decision.classify({
      instructions: "Classify the sentiment.",
      criteria: {
        positive: "Expresses satisfaction",
        negative: "Expresses dissatisfaction",
      },
    }),
  },
});

const DecisionLive = LanguageModelDecisionModel.layer.pipe(
  Layer.provide(OpenAiLanguageModel.model("gpt-6-luna", { service_tier: "priority" })),
  Layer.provide(OpenAiClient.layerConfig({ apiKey: Config.Redacted("OPENAI_API_KEY") })),
  Layer.provide(FetchHttpClient.layer),
);

const program = DecisionModel.decide(Sentiment, { input: "This is excellent!" }).pipe(
  Effect.map(({ answers }) => answers.tone.label), // "positive" | "negative"
  Effect.provide(DecisionLive),
);
```

The adapter answers all classification, rating, and probability decisions in one `generateObject`
call. It derives the response schema from the decisions, puts decision instructions in the system
message, and sends schema-encoded input as untrusted user data. Prompt separation does not make
model decisions an authorization boundary. Input is still sent to the selected provider.

Probabilities are LLM estimates, not calibrated confidence scores. Native `DecisionModel`
validation applies unchanged: invalid distributions fail with `InvalidOutputError` without
normalization. JSON or schema decoding failures retain `StructuredOutputError`. Token usage is
forwarded; the adapter does not invent confidence values. Provider errors, defects, and interruption
propagate. Configure model options on the supplied Layer and compose retry, timeout, or failover
policies explicitly; this adapter does not automatically retry another provider.

The Layer requires `LanguageModel` and provides `DecisionModel`. Import it from the package root
or `@effect-agent/ai-decision/language-model-decision-model`. The
[provider-neutral example](https://github.com/danieljvdm/effect-agent/blob/main/packages/ai-decision/examples/language-model.ts)
uses the same decision API without selecting a provider.

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

Standalone decision calls have no automatic retries or deadlines. Compose `Effect.retry` and `Effect.timeout`,
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

`@effect-agent/ai-decision` exports `AutoModel` and `LanguageModelDecisionModel`. Import the shared
decision APIs directly from Effect; provider integrations come directly from upstream. Custom providers implement
`DecisionModel.make({ decide })`, returning tagged provider answers and usage.

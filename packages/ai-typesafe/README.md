# @effect-agent/ai-typesafe

Evaluate TypeSafe AI choice, score, and noul questions with Effect. The package uses
Effect's platform-neutral `HttpClient`, `Schema`, `Config`, and `AiError` and has
an Effect runtime peer dependency (`^4.0.0-rc.115`) and the shared `@effect-agent/ai-decision` contract.

```ts
import { TypeSafeClient } from "@effect-agent/ai-typesafe";
import { Config, Effect, Layer } from "effect";
import { FetchHttpClient } from "effect/unstable/http";

const evaluation = Effect.gen(function* () {
  const client = yield* TypeSafeClient.TypeSafeClient;
  const result = yield* client.evaluate({
    model: "jev-latest",
    state: { message: "I was charged twice. Please refund the duplicate." },
    questions: {
      department: {
        type: "choice",
        instructions: "Which team should handle this ticket?",
        criteria: { billing: "Payments and refunds", technical: "Bugs and outages" },
      },
    },
  });

  return result.answers.department.choice; // "billing" | "technical"
});

const ClientLive = TypeSafeClient.layerConfig({
  apiKey: Config.Redacted("TYPESAFE_API_KEY"),
}).pipe(Layer.provide(FetchHttpClient.layer));

const program = evaluation.pipe(Effect.provide(ClientLive));
```

Run `program` with your application's Effect runtime. The compiling
[mixed-question example](examples/evaluate.ts) also shows bounded retries and an
overall timeout. Acquiring the client does not send requests.

## Questions and answers

For reusable, provider-independent definitions, use `DecisionQuery` and `DecisionSet` from
`@effect-agent/ai-decision` with `TypeSafeDecisionModel.model("jev-latest")`. The
[decision example](examples/decision.ts) evaluates all three question kinds against schema-encoded
input. Shared answers retain their distributions; Jev confidence is stored under
`result.providerMetadata.typesafe.confidence`, keyed by question ID. Decode that namespace with
`TypeSafeDecisionModel.ProviderMetadata`. The direct client below retains the native wire answers.

`evaluate` sends `POST https://api.typesafe.ai/v1/systemone` with bearer authentication
and the required `{ model, state, questions }` body. State and instructions accept
strings, JSON objects, and JSON arrays. Nested values must be JSON-compatible.

| Question | Criteria                                                                 | Answer                                                      |
| -------- | ------------------------------------------------------------------------ | ----------------------------------------------------------- |
| `choice` | Nonempty map of option names to a string or `null`                       | `choice`, `probabilities`, `confidence`                     |
| `score`  | At least two ordered string descriptions                                 | Fractional `score`, `legend`, `probabilities`, `confidence` |
| `noul`   | Optional object with optional string descriptions for `true` and `false` | `noul` in [0, 1]                                            |

Every answer includes its `type`. `model` and `usage.input_tokens` /
`usage.output_tokens` are retained. A returned model may resolve the requested alias
to a concrete version. Required fields are never filled in from examples or defaults.
The supported wire shapes follow the [API reference](https://docs.typesafe.ai/api),
including string-valued score legends.

Use `satisfies TypeSafeSchema.Questions` to retain question keys and choice literals
when defining questions separately. Runtime-built maps retain broader types:
unknown answer IDs and probability keys may be absent, and mixed answers must be
narrowed by `type`. Optional criteria remain optional in the probability map.
Open string, numeric, and template-pattern indexes allow absent entries;
explicitly required keys keep their required types.

The client builds a response schema from each request. It checks the exact answer
IDs and kinds, permitted choices, complete probability keys, and matching score
legends. Probabilities and confidence must be finite and in [0, 1]. A choice must
have maximal probability; ties are valid. Scores range from zero to the last level
and must match their probability-weighted value. Distribution sums normally allow `1e-6`
serialization error. For Choice only, observed two-decimal probabilities can total `0.99` or
`1.01`: the allowance is `0.005` per option, capped at one percentage point overall.
Every probability must be representable to two decimal places to use this allowance;
higher-precision drift is rejected. A single `0.99` option and an all-zero large catalogue
remain invalid. Values are preserved, never normalized. Score sums retain the `1e-6` tolerance,
and score comparisons allow `1e-6` times the highest level index.
Unexpected response fields are rejected.

Confidence summarizes a distribution; it is not a correctness guarantee. Noul has
no separate confidence. Choose application thresholds using evaluated examples for
your domain. See TypeSafe's [confidence guide](https://docs.typesafe.ai/confidence).

## Errors, cancellation, and HTTP policies

Evaluation failures use native `AiError` reasons:

| Failure                                                      | Reason                               |
| ------------------------------------------------------------ | ------------------------------------ |
| Invalid local request or HTTP 422                            | `InvalidRequestError`                |
| HTTP 401                                                     | `AuthenticationError` (`InvalidKey`) |
| HTTP 429                                                     | `RateLimitError`                     |
| HTTP 529                                                     | `InternalProviderError`              |
| Transport failure                                            | `NetworkError`                       |
| Malformed JSON or a response that disagrees with the request | `InvalidOutputError`                 |

`layerConfig` can also fail with `ConfigError`. Defects and interruption remain
defects and interruption. Cancellation reaches the supplied HttpClient, including
while reading the response body.

HTTP status failures retain the status, request details, response headers, and raw
provider error text without assuming an error-body protocol. Sensitive headers and
the configured API key are redacted from diagnostics. Provider error text may contain
submitted content. The integration adds no body logging; HTTP tracing is controlled
by Effect HttpClient and the application.

There are no default retries or deadlines. Compose `Effect.retry` with a bounded
schedule and `Effect.timeout`, or supply `transformClient` to `make`, `layer`, or
`layerConfig`. The transformer receives the client after authentication and status
filtering. `apiUrl` overrides the versioned base URL for a proxy or substitute.

## Provider-neutral decisions

`TypeSafeDecisionModel.model("jev-latest")` supplies `DecisionModel` from
`@effect-agent/ai-decision`. Provide `TypeSafeClient` when constructing the Layer.
The adapter maps shared `probability` questions and answers to TypeSafe's `noul` wire format;
choice and score evidence is preserved. HTTP policies remain on the client.
The adapter supplies the same bounded Choice rounding check to the shared model validator.
The [compiling example](examples/decision.ts) uses all three evaluations to advance a typed
application state. Model decisions do not execute tools or authorize side effects.

## Native Effect AI tools

The compiling [tool example](examples/tool.ts) declares a native `Tool` with
`failure: AiError.AiError` and `dependencies: [TypeSafeClient.TypeSafeClient]`, then
implements it through `Toolkit.toLayer`. The declared default failure mode keeps
failures in the Effect error channel. Supply TypeSafeClient when executing the tool
or providing it to an agent's language model.

The package exposes evaluations directly. Language generation, chat, streaming,
and tool planning are outside its API.

## Modules and extraction

Root exports are the `TypeSafeClient`, `TypeSafeSchema`, and `TypeSafeDecisionModel` namespaces. Direct imports
use `@effect-agent/ai-typesafe/type-safe-client` and
`@effect-agent/ai-typesafe/type-safe-schema`, and
`@effect-agent/ai-typesafe/type-safe-decision-model`.

This package follows Effect provider module conventions while incubating in the
Effect Agent release group. Its source imports Effect and the inward decision contract; it has no engine,
platform, persistence, or vendor SDK dependency. Upstream extraction would change
package metadata, service identity, import paths, and release documentation while
retaining the HTTP contracts and tests.

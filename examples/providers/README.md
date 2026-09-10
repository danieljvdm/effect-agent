# Provider Model bindings

This leaf workspace is a compile-time and binding proof for the Phase 1 `E`
(ephemeral) Travel Planner. It imports the shared `TravelPlanner` Definition from
`@effect-agent/testing` and creates two explicit native Effect AI Model bindings:

- `OpenAiTravelPlanner` uses `OpenAiLanguageModel.model("gpt-4.1-mini")`.
- `AnthropicTravelPlanner` uses `AnthropicLanguageModel.model("claude-haiku-4-5")`.

It intentionally has no default live invocation or smoke command. The ordinary
test suite makes no network request and requires no credentials. An application
that elects to execute either binding must provide the corresponding upstream
Effect AI client Layer, configured with its own redacted `OPENAI_API_KEY` or
`ANTHROPIC_API_KEY`, plus the normal Travel Planner handler Layers. The result
remains class `E`: process loss has no recovery promise.

No provider wrapper, registry, or ambient model selection is introduced here.

## Cloudflare AI Gateway

[`src/cloudflare-ai-gateway.ts`](src/cloudflare-ai-gateway.ts) supplies two interchangeable
handler Layers for `WebSearch.tool`: `openAiGatewaySearch` uses Cloudflare's account REST API,
and `anthropicGatewaySearch` uses the Anthropic provider-native gateway. Supply the account ID,
gateway ID, redacted Cloudflare token, and an Effect `HttpClient`; no provider SDK wrapper or
Worker-only import is needed. Include `WebSearch.tool` in any agent's toolkit, then provide
one of these Layers independently of the agent's own model.

The REST example needs a Workers AI Read token and uses `openai/gpt-4.1-mini`. The provider
proxy example uses the native `claude-haiku-4-5` name and Gateway authentication with stored
keys or Unified Billing. Search-model usage is returned with the tool result and is billed
separately from the parent Run's model accounting. Both examples bound provider output tokens.

The [Gateway guide](../../docs/platforms/cloudflare.md#ai-gateway) covers direct provider keys,
other providers, embeddings, streaming, and native search in the primary model's toolkit.
Deterministic tests exercise the real Effect client encoders/decoders with local HTTP fixtures;
they do not call Cloudflare or incur inference charges.

## Persistent history

The offline history example runs two inputs against one SQLite Thread, then reconstructs
the Prompt in a new process. It requires Node, which supplies `node:sqlite`, and no credentials.

```sh
vp run -F @effect-agent/example-providers history --database /tmp/effect-agent-history.sqlite seed
vp run -F @effect-agent/example-providers history --database /tmp/effect-agent-history.sqlite show
```

`seed` appends two complete Runs each time it executes. `show` loads their canonical history
without constructing a model. The functions in [`src/history.ts`](src/history.ts) demonstrate
`AgentRuntime.run` with `PersistentHistory.layer` and `ThreadHistory.load`; an application can replace the scripted Model with
an upstream provider Model. An interrupted Run has no completion or recovery promise.

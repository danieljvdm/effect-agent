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

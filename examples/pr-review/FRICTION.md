# Authoring friction observed while building `example-pr-review`

Real observations from building this example against the public framework
path, recorded for the API-simplification backlog (same convention as
`examples/repo-ops/FRICTION.md`).

1. **Typing a reusable run helper requires engine internals.** A function
   that accepts "any binding of this definition" cannot be written against
   `Agent.Any` — `AgentRuntime.run` wants the full nine-parameter
   `RuntimeBinding<...>`, so `executeReview` re-states Input/Output/Toolkit
   plus four inference-only type parameters. A public
   `Agent.BindingOf<typeof Definition>` alias would remove the boilerplate.

2. **`AgentResult.output` is typed as decoded but carries the encoded JSON.**
   `run` validates the terminal text against the output schema but returns
   the parsed JSON value, so a class-schema consumer must re-decode
   (`Schema.decodeUnknownEffect(CodeReview)(result.output)`) to get a real
   instance. Either returning the decoded value or typing the field as the
   Encoded side would make the contract self-evident.

3. **Scripted-model boilerplate is copied between examples again.** The
   prompt-keyed scripted `LanguageModel` (call counter outside the Layer,
   `toolTurn`/`finalParts` stream-part helpers) is now hand-rolled in
   repo-ops, the demo, and here. `@effect-agent/testing` exports Travel
   Planner fixtures but no generic "script these tool calls, then this final
   text" constructor usable by leaf examples.

4. **Live-gate helper is fixture-coupled.** `phase7LiveProfileEnabled` pins
   `OPENAI_API_KEY`, so this example still re-implements the same two-line
   gate. A parameterized `liveProfileEnabled(credentialEnv)` in
   `@effect-agent/testing` would keep one convention.

5. **No shared repo-relative path validator.** The fail-closed path
   normalization demanded by SEC-007 is hand-copied from repo-ops (third
   copy in the repository, as its FRICTION.md predicted).

6. **`Layer.succeed(Service)(value)` vs `Layer.succeed(Service, value)`**
   both typecheck, and examples mix the two; a lint-level convention would
   help newcomers copying from mixed sources.

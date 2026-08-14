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

## S1 attached delegation (first real consumer, the fan-out variant)

7. **`Subagent.define` hardcodes `failureMode: "error"`, so one failed child
   aborts the whole parent Run.** A fan-out coordinator wants partial
   results with honest reporting — "unit-002 unreviewed: AgentPolicyError"
   in the summary — but every expected child failure (`mapChildFailure`
   output, `SubagentBudgetExhausted`, …) travels the handler error channel
   and fails the Run typed. The workaround that ships here: the parent
   Toolkit carries a same-name Tool value with identical Schemas but
   `failureMode: "return"`; Effect AI resolves handlers by Tool name, so the
   real S1 handler executes and the typed failure becomes a model-visible
   failed tool result. It works and it is compile-proofed, but it forces the
   consumer to re-state the framework's failure union
   (`SubagentPrestartDenied | SubagentBudgetExhausted | … | ToolCallWaiting |
SubagentDurabilityError`) verbatim — including two members that are
   engine-internal protocol signals no example should have to know about —
   and it would silently break durable mode (a "returned" `ToolCallWaiting`
   is not a suspension). `Subagent.define` should take a containment option
   (`failureMode`, or a coordinator-level "failed children are results"
   policy) and keep the waiting signal out of the author-visible union.

8. **`mapChildFailure` cannot see which invocation failed.** The declared
   failure type is shared by every invocation, and the mapping receives only
   the child failure — not the Tool parameters, the Tool Call id, or the
   `SubagentPrepareContext`. A unit-failed marker therefore cannot carry its
   own `unitId`; the coordinator has to correlate the failed result with the
   call it declared. Passing the same bounded context `prepareInput` gets
   would make failure markers self-describing.

9. **Two bounds must be kept aligned by hand.** The child's `AgentPolicy`
   (the bound that actually trips typed) and the delegation's
   `SubagentPolicy` (the reservation the parent accounts) repeat the same
   numbers — `maxTurns`, `maxToolCalls`, `maxDuration` — with no compile-time
   or runtime check that they agree. A drift means reservations that never
   match observed usage. `SubagentPolicy.fromAgentPolicy(child.policy,
{maxChildren, maxConcurrency})` would remove the duplication.

10. **The scripted-model boilerplate tax doubles with delegation.** Item 3
    already records the per-example scripted `LanguageModel`; a delegation
    consumer now needs TWO of them (coordinator and child), and the child one
    must be prompt-keyed by briefed identity so concurrent children sharing
    one Model value stay independent. That pattern (key the script on a brief
    marker in the child's own prompt) is subtle enough that it was copied
    from the travel-planner fixtures — `@effect-agent/testing` should export
    it.

11. **Wiring the handler Layer takes framework-internals knowledge.** Getting
    `fanOutHandlersLayer(childBinding)` to build requires knowing that its
    construction wants the child toolkit handlers AND `SubagentReservations`
    AND `IdGenerator`, that `SubagentReservationsMemoryLive` exists for
    ephemeral runs, and that `Layer.provideMerge(sourceLayer)` must feed both
    toolkit Layers separately. None of that is discoverable from the
    `Subagent.define` call site; a "minimal ephemeral delegation" doc recipe
    (or a bundled `SubagentRuntime.ephemeralSupportLayer`) would have saved
    the longest debugging loop of this example.

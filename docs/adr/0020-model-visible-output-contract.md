# ADR-0020: Model-visible output contract and the final-answer-tool direction

- Status: **Proposed** (not owner-approved; output conformance is a core contract — the Phase A
  default is prototyped on its proposal branch and reversible, the Phase B protocol change is
  design-only)
- Related decisions: [D-002](../DECISIONS.md#d-002--relationship-to-effect-ai),
  [D-022](../DECISIONS.md#d-022--model-integration),
  [D-027](../DECISIONS.md#d-027--agent-definition-and-model-binding),
  [D-029](../DECISIONS.md#d-029--durable-runtime-placement-and-leases),
  [D-032](../DECISIONS.md#d-032--cloudflare-conversation-objects),
  [D-038](../DECISIONS.md#d-038--output-schema-conformance)
- Working plan: [OUTPUT-CONFORMANCE-PLAN.md](../OUTPUT-CONFORMANCE-PLAN.md)
- Issue: [#41](https://github.com/danieljvdm/effect-agent/issues/41)

## Context

An Agent Definition's `output` Schema is enforced in exactly one place: after a `stop` finish,
`decodeFinalOutput` JSON-parses the Turn's final text and validates it against the Schema
(AUTH-008). The model is never told the Schema exists. `LanguageModel.streamText` — the engine's
only model entry point — pins the provider `responseFormat` to `{ type: "text" }`; the upstream
structured-output path (`generateObject`, OpenAI `response_format: json_schema`, Anthropic
structured outputs) is non-streaming and cannot host the Turn loop, and a Turn cannot know before
streaming whether the model will call Tools or finish.

Consequently the only conformance force is hand-written prose in `instructions`. Every agent in
the repository hand-maintains such prose; forgetting it fails the live path at the very end with
`"Agent output is not valid JSON"`, while deterministic suites stay green because scripted models
fabricate the JSON regardless of the request (the exact break of issue #41 in
`examples/code-mode-cloudflare`, stopgapped in PR #42).

Constraints in force: no framework-owned Effect AI primitive copies (ADR-0002); the Turn stays a
streamed `Response.StreamPart` reduction; evaluated instructions and Turn responses are canonical
(`ModelResponseRecorded`, D-029) and the DN ≡ DC claim is byte-equal against one committed golden
(D-032), so official history must not absorb framework-generated wording; model output remains
untrusted and decode-before-success remains authoritative; structured output must not be silently
downgraded (providers §5). The in-flight context-economics ADR (ADR-0018) already establishes the
request-time seam this record uses — its run-status message is appended only to the outgoing
`streamText` prompt, never to history or the journal — so the two compose as independent,
non-canonical request projections.

## Decision

1. **Phase A — model-visible output contract (proposed default).** At model-request
   materialization — after `RunContextHook.prepare`, never into official history — the engine
   appends one framework-owned system message stating the final-output contract: a fixed
   directive plus the JSON Schema derived from the encoded side of `agent.definition.output` via
   Effect AI's `Tool.getJsonSchemaFromSchema` (the same derivation Code Mode uses for
   model-facing declarations). The fragment is a per-request projection of the immutable
   definition, exactly like Tool schemas: canonical records, run events, the committed DN/DC
   golden, and every public type are unchanged.
2. **Placement is normative.** The contract message is inserted immediately after the request
   prompt's **last** system message (position 0 when none exists), extending the last contiguous
   system block. The Anthropic provider replaces its top-level `system` parameter per contiguous
   system group, so only the last block survives there: an isolated trailing contract message
   would discard the author's instructions, and a contract attached to an _earlier_ block (a
   resumed Conversation's original instructions ahead of this Run's evaluated instructions) would
   itself be discarded. Extending the last block keeps author content and contract together on
   every provider and preserves per-message cache-control annotations. Because injection follows
   context preparation, compaction and context transforms can never drop the contract, and it is
   present on every Turn's request.
3. **Honest fallback, not a new failure mode.** A Definition whose output Schema cannot render to
   JSON Schema runs exactly as before — no injection — with a Turn-1 diagnostic log. Phase A is
   guidance; enforcement stays with `decodeFinalOutput` (AUTH-008 unchanged), so a Schema that
   decodes but does not render must not start failing Runs. No conformance claim is made beyond
   "communicated".
4. **Phase B — the final-answer tool is the enforced direction, gated on its own accepted ADR.**
   Provider-grade enforcement will use a synthetic engine-owned finish Tool whose `parameters`
   Schema is the output Schema; the model finishes by calling it, the engine validates the
   arguments and treats the call as the terminal seam without running any handler. Rationale:
   Tool parameters are the one channel providers already JSON-Schema-validate on every
   Tool-calling model — Effect AI's own Anthropic provider implements `generateObject` on
   non-structured-output models with exactly this forced-tool pattern — and the mechanism stays
   inside the streamed tool loop, dissolving the text-vs-tools ambiguity. It is a Turn-protocol
   change (terminal tool batches, mixed-batch rules, canonical response shape, event surface,
   steering/follow-up/resume seams, reserved-name collision, opt-in→default migration) and is
   therefore not implemented under this ADR, in the pattern of ADR-0017's durable deferral.
5. **`generateObject` repair is rejected as a default.** A post-failure repair call hides the
   contract miss as invisible cost and latency and creates output/history divergence. It may
   return later as an explicit bounded acceptance policy (`onInvalidOutput`), owner-gated.
6. **Upstream-first obligation.** File the Effect AI proposal for `responseFormat` on
   `streamText` (structured output for streaming) per D-002; if adopted, the engine passes the
   output Schema through natively and Phase B's scope narrows to providers without enforcement.
7. **Specification impact lands only on acceptance.** Two requirements are added to
   `docs/spec/runtime.md` and `docs/spec/testing.md` with coverage rows when this ADR is
   accepted: RUN-027 — every model request carries the derived output-Schema representation, with
   the documented fallback — and TEST-016 — a live-shaped LanguageModel substitute that derives
   responses only from the model-visible request, so offline suites catch missing contract
   communication. The numbers are provisional (next free at acceptance): the budget arc holds
   RUN-018–020 and the in-flight context-economics ADR reserves further RUN slots. On acceptance
   the authoring §4 derivation sentence also becomes true for output Schemas. While Proposed, no
   normative requirement changes, keeping the TEST-011 gate honest.

## Consequences

- Forgetting to restate the output Schema in `instructions` no longer silently breaks only the
  live path; the hand-written shape prose across travel-planner, docs-researcher, pr-review, and
  the code-mode-cloudflare example becomes deletable duplication (follow-up cleanup).
- Every model request grows by the rendered Schema — the cost shape of one extra Tool
  declaration.
- Tests that assert the model-request shape see one additional system message; tests that assert
  official history or canonical prompts see nothing, which is the verifiable boundary of the
  change.
- The deterministic test kit gains a live-shaped substitute whose answers derive from the request
  it received, closing the scripted-model blind spot for this class (TEST-016 on acceptance).
- Until the Phase B ADR is accepted, conformance remains post-hoc decode: a hint plus typed
  failure, honestly reported — no provider-enforcement claim is made.

## Rejected alternatives

- **Swap the final Turn to `generateObject`.** The engine cannot know a Turn is final before
  streaming it, and `generateObject` cannot host the tool loop; structured output as a separate
  non-streamed mode contradicts the one-interpreter rule (RUN-001).
- **Inject into official history (`makeInitialPrompt`).** Simplest wiring, but the fragment would
  be committed inside `ModelResponseRecorded`, changing canonical bytes for every durable
  Conversation, invalidating the committed DN/DC golden, and freezing framework wording into
  history; compaction could also drop it. Injection at request materialization has none of these
  properties.
- **Fail Runs (or `Agent.define`) on non-renderable output Schemas in Phase A.** Turns a hint
  into a regression for agents that decode fine today. Fail-closed construction is correct where
  the schema is the contract — the Phase B finish Tool, like Code Mode's declaration deriver.
- **`generateObject` repair as the default conformance mechanism.** See Decision 5.
- **Documenting the hand-written convention instead of mechanizing it.** The convention already
  exists and is what failed; unchecked prose duplication of Schemas is the problem itself.
- **Waiting for upstream `responseFormat` on `streamText`.** The upstream timeline is not ours,
  and `responseFormat`-with-tools semantics are uneven across providers (Anthropic's non-native
  fallback claims the forced-tool slot). Filed upstream in parallel instead (Decision 6).

## Validation

- Request evidence: the contract message is present on every Turn's provider request (first,
  tool-loop continuation, and post-compaction Turns), positioned adjacent to the leading system
  block, and contains the JSON Schema derived from the definition's output Schema.
- Boundary evidence: official history (`onHistory`), run events, canonical records, and the
  committed DN/DC golden are byte-identical with and without the feature enabled for scripted
  runs; the non-renderable-Schema fallback reproduces today's behavior exactly plus one
  diagnostic.
- Live-shaped evidence: a deterministic LanguageModel that answers only from the request it
  received produces schema-valid final output solely because the engine communicated the
  contract — with contract communication removed, the same substitute fails the Run the way the
  live path failed in issue #41.
- Type evidence: `E`/`R` inference for `AgentRuntime.run`/`stream` is unchanged (no new failure,
  no new requirement).
- Phase B (under its own ADR): direct-vs-finish-tool equivalence of validated output, terminal
  batch semantics, canonical/golden migration, and reserved-name collision all fail closed.

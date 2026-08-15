# Output Schema Conformance Plan

Status: **Proposed — awaiting owner decision**
Date: 2026-08-15
Issue: [#41 — Engine never tells the model the output schema](https://github.com/danieljvdm/effect-agent/issues/41)

Governance registration: this plan is registered as **D-038 (Proposed)** in
[docs/DECISIONS.md](DECISIONS.md) and its normative architecture content is drafted in
[ADR-0020](adr/0020-model-visible-output-contract.md) (Proposed). Output conformance is a core
contract (Agent `E`/`R` types, the final-output decode boundary, and — for the enforced variant —
the Turn protocol and canonical records), so nothing in this plan is owner-approved by default.
The branch carries a locally reversible prototype of the Phase A recommendation
(section 11) so the owner reviews a concrete, tested change rather than prose alone.

## 1. Objective

An Agent Definition declares an `output` Schema (D-027, [authoring §2](spec/authoring.md)). Make
the runtime communicate that Schema to the model, and give the framework a credible path from
"communicated" to "enforced", without breaking the streaming Turn architecture, the
canonical record contract, or ADR-0002's rule against framework-owned Effect AI primitives.

## 2. The gap today

The `output` Schema has exactly two consumers today, both post-hoc validation boundaries and
neither model-visible. After the model finishes with `stop` and no queued input remains, the
interpreter's otherwise-stop seam calls `decodeFinalOutput` (`packages/engine/src/index.ts`),
which:

1. decodes the Turn's final text through `Schema.fromJsonString(Schema.Json)` — failure produces
   the typed `AgentOutputError` `"Agent output is not valid JSON: …"`;
2. validates the parsed JSON against `agent.definition.output` and completes the Run with the
   encoded JSON as `RunCompleted.output`.

The second consumer is `reduceRunEvents`: `run` and `start` decode `RunCompleted.output` through
the Schema again to produce the typed `AgentResult`. Both are fail-closed decode seams (AUTH-008)
and both stay authoritative under every option in this plan.

The model request never carries the schema in any form. Every Turn calls

```ts
LanguageModel.streamText({ prompt, toolkit, disableToolCallResolution: true });
```

([runtime §3 step 5](spec/runtime.md), [providers §2](spec/providers.md)) and `streamText`
hard-codes `responseFormat: { type: "text" }` in the normalized `ProviderOptions` it hands the
provider. The provider-facing response format, OpenAI's native
`response_format: { type: "json_schema" }`, and Anthropic's structured-output path exist upstream
— but only behind `LanguageModel.generateObject`, which is non-streaming and cannot host the Turn
loop (section 5.5).

So the only force producing schema-valid final text today is prose the agent author hand-writes
in `instructions`. The [authoring specification §4](spec/authoring.md) already asserts "Effect AI
derives provider-facing schema representations from Tool and structured output Schemas" — true
for Tool parameters, currently false for the Agent output Schema. This plan closes that gap.

## 3. Evidence

**The live break (issue #41).** `examples/code-mode-cloudflare`'s live OpenAI profile instructed
the model only to "return a concise answer string". The scripted profile hard-codes
`{"answer":"…"}` and stayed green; the live model returned a prose sentence and the Run failed at
the very end with `Agent output is not valid JSON: Expected a valid JSON string`. PR #42 fixed
the demo by hand-adding the JSON-shape instruction — the per-agent workaround this plan removes
the need for.

**The convention is hand-maintained everywhere.** Every agent in the repository restates its
output Schema as prose, by hand, today:

- `packages/testing/src/fixtures/travel-planner/deterministic-layers.ts` — "Then return only a
  JSON object of exactly this shape, no prose:" followed by a hand-written shape;
- `packages/testing/src/fixtures/docs-researcher/definition.ts` — twice (parent and child);
- `packages/pr-review/src/internal/review-agent.ts` and `internal/fan-out.ts` — three
  multi-hundred-character hand-written shape restatements;
- `examples/code-mode-cloudflare/src/agent.ts` — the PR #42 stopgap.

Each is unchecked duplication: nothing verifies the prose matches the Schema, and the compiler
cannot help when the Schema changes.

**Offline suites structurally cannot catch the omission.** The deterministic
`ScriptedModel` ([testing §2](spec/testing.md)) plays back scripted parts regardless of what the
request said, so a scripted final turn always "happens to" emit valid JSON. An agent can pass its
entire deterministic suite (TEST-002, TEST-014) and still fail on every live call. TEST-007
live smokes are release-lane and opt-in, so the failure class ships silently. Section 9 proposes
TEST-016 to close the class offline.

## 4. Constraints

1. **ADR-0002 / D-002 / D-022.** No framework-owned copies of `Tool`, `Toolkit`, `LanguageModel`,
   `Prompt`, `Response`, `Model`. Generally useful missing capabilities go upstream first.
2. **Streaming Turn architecture.** The Turn is a `Response.StreamPart` reduction over
   `streamText`; a Turn does not know a priori whether the model will call Tools or finish
   ([runtime §3–4](spec/runtime.md)). Any mechanism must survive that.
3. **Canonical record stability.** Evaluated instructions and Turn responses are canonical
   (`ModelResponseRecorded`, D-029). The DN ≡ DC claim is byte-equal normalized canonical
   evidence against one committed golden (D-032). A mechanism that mutates official history
   changes canonical records for every existing DN and DC Conversation and invalidates the
   golden.
4. **Fail-closed, but honest.** Model output is untrusted input; `AUTH-008` (structured output is
   Schema-decoded before Run success) stays the enforcement authority regardless of any hint or
   provider claim.
5. **No silent downgrade.** [Providers §5](spec/providers.md): the runtime must not silently
   downgrade structured output. Whatever conformance level a mechanism claims must be reported
   honestly.
6. **D-025 phase gate.** No new packages; this is engine (and later possibly core) surface.

## 5. Options

### 5.1 Option A — model-visible output contract (auto-injection)

The engine renders `agent.definition.output` to JSON Schema with the existing Effect AI
derivation (`Tool.getJsonSchemaFromSchema`, the same machinery Code Mode uses for model-facing
declarations, CAP-014) and appends one framework-owned system message — the _final output
contract_ — to the model request:

> Final output contract: when the task is complete, the final assistant message must be only JSON
> that is valid against this JSON Schema — no prose, no Markdown code fences, nothing before or
> after the JSON. `{…derived JSON Schema…}`

Two injection sites were analyzed; they are not equivalent:

|                  | A-official: append to official history in `makeInitialPrompt`                                                                                                                                                                                                                              | A-request: append at model-request materialization in `makeTurn` (recommended)                                                                       |
| ---------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| Canonical impact | The fragment enters official history, so it is committed inside `ModelResponseRecorded` (D-029). Every DN and DC Conversation's canonical bytes change; the committed DN/DC golden must be regenerated; future framework wording changes alter canonical history for unchanged definitions | None. Official history, canonical records, run events, and the committed golden are byte-identical; the fragment exists only in the provider request |
| Compaction       | A compacting `RunContextHook` can drop or summarize the fragment                                                                                                                                                                                                                           | Applied after `context.prepare`, so the contract survives any context transform on every Turn                                                        |
| Turn coverage    | Present once at history head                                                                                                                                                                                                                                                               | Re-applied to every model request, including post-compaction Turns                                                                                   |
| Replay           | Recorded wording is replayed verbatim (frozen per Conversation)                                                                                                                                                                                                                            | Re-derived per Attempt from the definition — same source the instructions replay from                                                                |

**A-request is the recommended variant.** The contract is a projection of the immutable
definition, exactly like the Tool schemas the provider request already carries on every call —
and Tool schemas are not canonical history either. The in-flight context-economics ADR
(ADR-0018, PR #54) establishes the same seam from the other direction: its run-status message is
appended only to the outgoing `streamText` prompt, never to history or the journal. The two
compose — compaction produces the request view, the contract joins that view's last system
block, run-status appends trailing user content — all request-time projections, none canonical.

Placement is normative, not cosmetic: the injected message goes immediately after the request
prompt's **last** system message (position 0 when none exists), extending the last contiguous
system block. The Anthropic provider maps each contiguous system group to its top-level `system`
parameter and a **later system group replaces the earlier one**, so only the last block survives
there. An isolated trailing contract message would therefore discard the author's instructions,
and a contract attached to an earlier block — a resumed Conversation's original instructions
sitting ahead of this Run's evaluated instructions — would itself be discarded. Extending the
last block keeps author content and contract together on every provider and preserves
per-message cache-control annotations. (The prototype's test run caught exactly the
earlier-block variant, via the official-history-prefix suite.)

Non-renderable output Schemas (Effect AI's JSON-Schema derivation throws) fall back to today's
exact behavior — no injection, a Turn-1 warning log. Option A is guidance, not enforcement;
failing Runs that decode fine today would be a regression. Contrast B1, where the schema _is_ the
tool contract and construction fails closed, consistent with Code Mode's declaration deriver.

Properties:

- **Enforcement: none.** A strong, uniform hint; `decodeFinalOutput` remains the authority
  (AUTH-008 unchanged). A model can still emit invalid output — the failure mode becomes rare
  instead of structural.
- **Non-breaking.** No API change, no `E`/`R` change, no event change, no canonical change. The
  model-visible prompt changes for every agent, which is the point; the prototype quantifies the
  test blast radius (section 10).
- **Effort: small.** One engine module plus one call site; the prototype on this branch is ~60
  lines of implementation.
- **Cost.** The schema rides every model request, like Tool schemas do. For the repository's
  agents this is tens to a few hundred tokens per request.

### 5.2 Option B1 — final-answer tool (enforced, protocol change)

Expose a synthetic engine-owned finish Tool (working name `submit_final_output`) whose
`parameters` Schema **is** the output Schema. The model finishes by calling it; the engine treats
that call as the terminal seam — it never runs an application handler, validates the arguments
against `agent.definition.output`, and completes the Run.

Why this is the right long-term shape:

- **The strongest existing conformance channel.** Tool parameters are strictly
  provider-validated where a provider guarantees strict tool schemas (OpenAI `strict` function
  schemas, Anthropic structured-output-capable models) and generation guidance elsewhere — and in
  every case the engine Schema-decodes the arguments fail-closed, so B1's floor is engine
  enforcement, not hope. Effect AI's own Anthropic provider implements `generateObject` on
  non-structured-output models by injecting exactly this — a synthetic tool named after the
  object with `toolChoice` forced to it (`prepareTools`,
  `repos/effect/packages/ai/anthropic/src/AnthropicLanguageModel.ts`). B1 is the same pattern
  lifted to the framework's Turn loop, so it works uniformly wherever Tool calling works, with
  strict provider validation on top wherever the provider offers it.
- **Streaming survives.** The finish Tool is just another Tool in the existing streamed tool-call
  loop; no `generateObject` detour, no non-streamed Turn.
- **The a-priori ambiguity dissolves.** The Turn no longer needs to guess text-vs-tools: work
  Turns call work Tools, the finish Turn calls the finish Tool.
- **ADR-0002 is respected.** The finish Tool is an ordinary Effect AI `Tool.make` value
  constructed by the framework — the same move the Subagent delegation builder (ADR-0010) and
  Code Mode builder (ADR-0017) already make. No primitive is duplicated.

Why it is a protocol change requiring its own accepted ADR before implementation:

1. **Finish semantics.** A Tool-call batch becomes a legal Run terminal. Today
   `finishReason: "tool-calls"` always forces another Turn and a `stop` finish with pending
   application calls is a protocol error; both rules change.
2. **Mixed batches.** Normative answers are required for: finish call + work calls in one batch;
   finish call + assistant text; multiple finish calls; a finish call with invalid arguments
   (reprompt? typed failure? bounded retries interact with `AgentPolicy`).
3. **Canonical records.** The terminal response's canonical shape changes (a tool-call message,
   not a text message). `ModelResponseRecorded` consumers, transcript projections, the recovery
   classifier's response-boundary reasoning, and the DN/DC golden all see it.
4. **Event surface.** Either the finish call surfaces as a new semantic event or it is folded
   into `RunCompleted`; observers must not infer an application handler where none ran
   (the provider-executed precedent in [runtime §5](spec/runtime.md)).
5. **Seams.** `settleOrFollowUp` triggers on text-stop today; follow-up drain, steering, and the
   DN/DC batch-resume path (`RunTurnResume`) must recognize the finish call.
6. **Naming and collision.** The reserved name must fail closed against application Tools and
   MCP-discovered Tools that collide.
7. **Adoption.** Default-on is a hard behavior change for every existing agent (their models
   suddenly see and are expected to call a new Tool); definition-level opt-in first, then a
   default flip, is the plausible migration — sequencing belongs to the B1 ADR.

Effort: medium — engine Turn-machine surgery plus spec, canonical, and golden updates, behind an
accepted ADR. Non-streaming cost: none. Enforcement: engine Schema-decode of the finish-Tool
arguments everywhere (the AUTH-008 authority, equivalent to `decodeFinalOutput`), with strict
provider validation on top wherever the provider guarantees strict tool schemas.

### 5.3 Option B2 — `generateObject` repair call

Keep the loop unchanged; when `decodeFinalOutput` fails, issue one non-streamed
`LanguageModel.generateObject({ prompt: history + response + repair instruction, schema })` and
complete with its validated object.

Honest assessment: as a _default_ this is the wrong mechanism.

- It converts a defect signal into an invisible cost: every structurally non-conforming agent
  silently pays one extra model call per Run instead of surfacing the gap.
- The repaired value is model output the author's model never actually said in-loop; canonical
  history then contains a final text message that disagrees with `RunCompleted.output`, or a new
  canonical record family for the repair — both need design work disproportionate to the value.
- The repair step is non-streamed and adds tail latency exactly at the user-visible end.
- Enforcement honesty is provider-dependent (`generateObject` is natively enforced on OpenAI and
  structured-output Anthropic models; elsewhere it is the provider's own shim).

B2 remains attractive later as an explicit, bounded **acceptance policy** (the authoring spec's
reserved "response acceptance policy" optional field): `onInvalidOutput: "fail" | "repair"`,
default `"fail"`, repair metered and surfaced in events. Rejected as the default answer to #41.

### 5.4 Option C — upstream `responseFormat` on `streamText`

`ProviderOptions.responseFormat` already models `{ type: "json", objectName, schema }` and every
provider adapter already consumes it; only the `streamText` entry point pins `{ type: "text" }`.
Upstreaming an optional `responseFormat` on `GenerateTextOptions` (or a `streamObject`) would let
the engine pass the output Schema through natively — provider-enforced on the final text, fully
streaming, no framework protocol change.

This is the D-002 upstream-first move and [providers §5](spec/providers.md) explicitly directs
it ("propose the general capability upstream"). It is not a plan of record because the timeline
is not ours and the provider semantics of `responseFormat` _combined with tools_ are uneven
(Anthropic's non-native fallback claims the forced-tool slot, which conflicts with application
Tools). Action regardless of A/B1: file the upstream proposal; if it lands, Phase A's injected
contract can be superseded by (or paired with) the native format, and B1's scope shrinks to
providers without enforcement.

### 5.5 Rejected outright

- **Swap the final Turn to `generateObject`.** The engine cannot know a Turn is final before
  streaming it, and `generateObject` cannot host the tool loop. Structural mismatch.
- **Fail Run start (or `Agent.define`) on a non-renderable output Schema.** Breaks currently
  working agents whose Schemas decode fine but do not render; the schema-communication feature
  must not be a new failure mode for Option A. (B1 construction is where fail-closed is correct.)
- **Do nothing / document the convention.** The convention already exists and is exactly what
  failed; unchecked prose duplication of Schemas is the disease, not the cure.

## 6. Comparison

|                             | Enforced?                                                     | Streaming preserved      | Breaking surface                                | Canonical impact                        | Extra model calls         | Effort               |
| --------------------------- | ------------------------------------------------------------- | ------------------------ | ----------------------------------------------- | --------------------------------------- | ------------------------- | -------------------- |
| A (request-seam injection)  | No — uniform hint                                             | Yes                      | None (prompt-visible only)                      | None                                    | None                      | Small (prototyped)   |
| B1 (final-answer tool)      | Yes — engine decode; strict provider validation where offered | Yes                      | Turn protocol, events, canonical response shape | Terminal response becomes a tool call   | None                      | Medium, ADR-gated    |
| B2 (repair call)            | Yes — on the repair step only                                 | Loop yes; repair step no | Output provenance semantics                     | Repair record or output/text divergence | +1 per non-conforming Run | Small–medium         |
| C (upstream responseFormat) | Yes — where providers support it                              | Yes                      | None framework-side                             | None                                    | None                      | Not ours to schedule |

## 7. Recommendation

**Adopt A now (as the proposed default), adopt B1 as the enforced target behind its own accepted
ADR, file C upstream, fold B2 into a future acceptance policy.**

A and B1 are not competitors. A is the floor: it removes the silent per-agent failure mode
immediately, costs nothing architecturally, and remains useful under B1 (a model that sees the
contract early plans toward it; the finish Tool enforces it). B1 is the ceiling: enforcement
uniform with the tool loop the engine already runs. This mirrors how the repository already
sequences risk: ship the reversible default, gate the protocol change on an accepted ADR
(ADR-0017's ephemeral/durable split is the precedent).

## 8. Phase plan

### Phase A — model-visible output contract (this branch prototypes it)

- **A0 — Governance.** This plan; [ADR-0020](adr/0020-model-visible-output-contract.md)
  (Proposed); D-038 (Proposed). Owner decision converts A0 into spec edits (section 9).
- **A1 — Engine.** `packages/engine/src/output-contract-internal.ts`: pure per-request
  JSON-Schema rendering (no process-global cache — derivation is cheap relative to a model call,
  exactly as providers re-derive Tool schemas per request), the contract message,
  last-system-block insertion; one call site in `makeTurn` deriving the contract before
  `context.prepare` (exposed as `RunContextRequest.outputContract` so adapters can reserve its
  overhead) and wrapping the request prompt after it; Turn-1 warning on non-renderable Schemas.
  No core, session, storage, or platform change.
- **A2 — Tests.** Engine suite: contract present on every Turn's request and placed adjacent to
  the last system block; official history never contains it (`onHistory` evidence);
  non-renderable Schema falls back byte-identically; the **live-shaped model** case (TEST-016
  shape): a deterministic `LanguageModel` that derives its final message _only from the request
  it received_ — emitting schema-conforming JSON only when the request advertises the schema,
  prose otherwise — proves the engine, not the test, communicates the contract. Existing
  request-shape assertions updated (section 10).
- **A3 — Follow-up cleanup (separate PR, after acceptance).** Delete the now-duplicated
  hand-written shape prose from travel-planner, docs-researcher, pr-review, and the
  code-mode-cloudflare example; re-run the live profiles once as evidence.

### Phase B — final-answer tool (design only until its ADR is accepted)

- **B0 — ADR.** A dedicated ADR answering section 5.2's seven protocol questions, with the
  canonical-record and golden migration plan and the opt-in→default sequencing.
- **B1a — Engine.** Finish-Tool construction (fail-closed on non-renderable Schemas and name
  collisions), terminal-batch semantics, seam integration, events.
- **B1b — DN/DC surface.** Canonical response shape, recovery classifier cases, golden
  regeneration, DN/DC equivalence re-evidence.
- **B1c — Migration.** Definition-level opt-in, repository agents migrated, default-flip
  decision.

Phase C (upstream) proceeds independently: file the Effect AI proposal referencing this plan.

## 9. Proposed requirements (spec edits land only on acceptance)

To keep the TEST-011 coverage gate honest, no `docs/spec/*.md` requirement is added while this
plan is Proposed. The IDs below are **provisional**: the budget arc (D-037/ADR-0019) holds
RUN-018–020 on `main` and the in-flight context-economics ADR (ADR-0018, PR #54) reserves six
further RUN slots, so the final numbers are whatever is next free when this plan is accepted. On
acceptance:

- **RUN-027** (runtime §13): _Every model request of a Run whose Agent Definition declares an
  output Schema carries a model-visible representation of that Schema derived by Effect AI's
  JSON-Schema derivation; a Definition whose output Schema cannot be derived runs with the
  documented fallback and a diagnostic, never a silent difference._ Runtime §3 step 5 and §9
  gain the request-materialization wording; [authoring §4](spec/authoring.md)'s derivation
  sentence becomes true as written.
- **TEST-016** (testing §14): _The deterministic test kit provides a live-shaped LanguageModel
  substitute that derives its responses only from the model-visible request (prompt text,
  advertised schemas, tools) — never from test-known expected output — and the engine's
  final-output path runs against it, so offline suites detect contract information missing from
  the request that a scripted model would fabricate away._ This closes the offline/live
  divergence class of issue #41 for good, independent of which option enforces conformance.
- **AUTH-008 unchanged** — decode-before-success remains the enforcement authority under every
  option.

## 10. Compatibility and blast radius (measured by the prototype)

- **Canonical records, run events, `E`/`R` types: unchanged. Public API: one additive optional
  field.** `RunContextRequest.outputContract` (engine `run-options.ts`, publicly re-exported) is
  new observable hook-request surface — additive and optional, absent entirely when the Schema
  is unrenderable, so existing hook implementations and request literals keep compiling and
  behaving. The DN/DC committed
  golden (`phase6TravelPlannerGoldenEvidence`) is untouched because `ModelResponseRecorded`
  carries official history, and A-request never writes there. Type tests needed no change.
- **Model-visible prompts change for every agent** — the intended effect. Deterministic suites
  are insensitive by construction (`ScriptedModel` ignores request content) except where a test
  asserts the request's message-role shape. The prototype's full-suite run surfaced exactly four
  such assertions, all updated mechanically with a one-line comment:
  `packages/engine/test/agent-runtime.test.ts` (compacted request), `packages/testing/test/travel-planner.test.ts`
  (first-Turn request), `travel-planner-phase2.test.ts` (compacted request), and
  `durable-runtime.test.ts` (resumed-Attempt request). Every canonical-log, journal, and
  official-history assertion — including the committed DN/DC golden — passed untouched, which is
  the measured proof that official history is clean.
- **Agents with hand-written shape prose** now send both the prose and the contract — redundant,
  consistent, and removable (A3).
- **Token cost** — the rendered schema per request, comparable to one additional Tool
  declaration.
- **Context-limit interaction (visible to preparation today; engine enforcement composes with
  the context-economics arc).** The contract is appended after `context.prepare`, so a hook
  compacting to an exact model-input limit could otherwise be pushed over it by the contract's
  serialized size. The prototype therefore passes the exact contract text into
  `RunContextRequest.outputContract` (additive optional field): a limit-targeting adapter
  reserves precisely that overhead in its own window calculation before the engine composes the
  request, and the suite asserts byte-equality between what preparation saw and what was
  appended. The engine itself has no window to enforce today (adapters own their margins); when
  the in-flight context-economics arc's engine compaction lands, its window calculation must
  reserve the same value the way it reserves Tool-schema overhead.
- **Reversal** — delete `packages/engine/src/output-contract-internal.ts`, its `makeTurn` call
  site, the `RunContextRequest.outputContract` field, and its test file; revert the assertion
  updates. No data, schema, or API migration.

## 11. Prototype status (this branch)

Implemented per A1/A2 and marked as the ADR-0020 proposed default in code comments:

- `packages/engine/src/output-contract-internal.ts` — pure rendering and insertion;
- `makeTurn` call site in `packages/engine/src/index.ts`;
- `packages/engine/test/output-contract.test.ts` — contract-on-every-Turn, placement,
  official-history cleanliness, context-preparation visibility (byte-equal reserve value),
  non-renderable fallback with the Turn-1 diagnostic asserted once for the Attempt's Turn 1
  across a two-Turn Run (a recovering DN/DC Attempt that re-executes Turn 1 may repeat it —
  at-least-once recovery), and the live-shaped model case;
- request-shape assertion updates listed in section 10;
- `bun run ready` green.

## 12. Open questions for the owner

1. **Adopt A as the default (this plan), or opt-in first?** The prototype is default-on; an
   opt-in would need a definition-level field now (core surface) that B1's migration will want
   anyway.
2. **Definition-level control surface.** When B1 arrives, does conformance mode live on the
   Definition (`output` unchanged, a sibling option), or on `AgentPolicy`? The Definition is
   recommended: it is model-visible behavior, not a bound.
3. **B2 as a future acceptance policy** — worth reserving in the authoring spec now, or leave to
   the B1 ADR?
4. **Upstream timing** — file the Effect AI `responseFormat`-on-`streamText` proposal
   immediately, or after B1's ADR settles the framework's own direction?

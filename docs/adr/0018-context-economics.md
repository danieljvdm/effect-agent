# ADR-0018: Context economics — bounds, tracking, graceful exhaustion, and engine-native compaction

- Status: Accepted (owner-directed, 2026-08-15)
- Related decisions: [D-013](../DECISIONS.md#d-013--child-agent-budgets),
  [D-035](../DECISIONS.md#d-035--code-mode-capability),
  [D-036](../DECISIONS.md#d-036--context-economics)
- Related records: [ADR-0003](0003-canonical-log-and-ledger.md),
  [ADR-0004](0004-uncertain-external-effects.md),
  [ADR-0012](0012-durable-tool-uncertainty-and-steps.md),
  [ADR-0019](0019-budget-soft-landing-and-extension.md) (this record amends its decision 1 by
  admitting the token dimension into the `onExhaustion` soft landing)

## Context

On 2026-08-15 a production orchestrator run (kommunikasie task `dea53970`) exhausted its 200,000
`tokenBudget` doing repository research — 183 tool calls across a wake storm — and settled failed
with no user-visible output. The framework's own behavior made that outcome structural rather
than accidental:

- Every model call re-sends the full accumulated history. The ephemeral loop appends to one
  mutable Prompt; the DN/DC journal projection folds every canonical record of the Conversation,
  including every prior Run's raw tool output, into every new Run's opening prompt.
- Nothing bounds a tool result before it enters history. A single oversized result is re-paid on
  every later call.
- The only live control is `tokenBudget`: a cumulative input+output counter that charges the full
  prompt of every call (cache reads at par), checked after each response, fatal on breach. Because
  cumulative input is ΣC_k over calls with context C_k, the counter grows quadratically in history
  length: a Run can "exceed 200k" while its live context never passes a fraction of the model
  window. It bounds neither spend (prompt caching makes re-sent prefixes roughly 10× cheaper than
  the counter claims) nor context size, and it offers no degradation path.
- The compaction primitives shipped in Phase 2 (`capabilities/context.ts`: digest-bound
  `CompactionArtifact`, `ContextTransform`, `ContextCompactor`) have no non-test callers, no
  trigger, and no projection integration; `CompactionCreated` was a declared canonical record that
  nothing emitted. Runtime spec §9 steps 4–5 ("Calculate window/budget", "Compact if policy
  requires") and the context-overflow retry sentence were specified but unimplemented.

All three surveyed reference harnesses converge on the same layered stack: OpenAI codex bounds
exec output per model (default 10k tokens, middle-out elision, applied at record time), tracks
cached tokens distinctly, auto-compacts at 90% of a per-model window, and exposes remaining
context to the model (`get_context_remaining`, injected reminders); pi caps every tool at
2,000 lines / 50 KB with continuation markers, requires `contextWindow` on every model, triggers
compaction at window − 16,384 with a 20k keep-tail, and persists compaction as an append-only
`CompactionEntry`; opencode wraps every tool (MCP included) in one 2,000-line / 50 KB truncator
with disk spill, prunes old tool outputs to "[Old tool result content cleared]" while protecting
the most recent 40k tokens, and compacts when usage exceeds window − output-reserve. None of them
lets exhaustion end a run silently.

## Decision

1. **Policy vocabulary.** `AgentPolicy` separates the three quantities the old counter conflated:
   `tokenBudget` (unchanged meaning: cumulative runaway stop), `costBudgetMicrousd` (spend; its
   estimator already receives cache-split usage), and a new optional `contextTokenLimit` (a bound
   on one model call's live context, supplied by the host from its model choice). New required
   fields with `make`-time defaults: `toolResultBounds` (default `{ maxBytes: 51_200 }`),
   `runStatus` (`"appended" | "off"`, default `"appended"`), and `compaction`
   (`CompactionPolicy { keepRecentTokens: 20_000, mode: "prune-then-summarize" }`). Exhaustion
   resolution stays ADR-0019's single `onExhaustion` knob — this record adds no second knob and
   instead extends that one to the token dimension (decision 5).
2. **Tool results are bounded at the settle seam.** The engine applies
   `applyToolResultBounds` to every application tool result's encoded wire form — MCP included —
   once, before the value enters records or prompts, so both carry the same bounded value. An
   oversized result becomes the canonical `TruncatedToolResult` envelope
   `{ truncatedToolResult: true, originalBytes, head, tail }`, a valid JSON value that decodes
   under a versioned core Schema. Provider-executed results are exempt (the provider already
   materialized them into the response). Spill-to-blob-storage with a re-fetch handle is deferred:
   it needs a blob-store port decision and is recorded here so it is proposed as an extension,
   not a rediscovery.
3. **Usage tracking is cache-aware and context-aware.** `UsageTotals`/`UsageDelta` carry
   `cacheReadInputTokens` and `cacheWriteInputTokens` distinctly (`inputTokens` stays the total),
   and totals expose `lastInputTokens`/`lastOutputTokens` — the most recent call's numbers — as
   the live-context estimate. Hosts can now meter spend and context separately from one snapshot.
4. **The agent can see itself.** With `runStatus: "appended"`, each outgoing model request ends
   with a derived run-status message (turns, tool calls, tokens against budget, last-call
   context, elapsed time, and a wrap-up warning at ≥ 80% of any dimension). The message is
   derived from usage at prompt-assembly time and is never persisted: canonical history stays
   append-only and rule 7 intact.
5. **Token exhaustion joins the ADR-0019 soft landing (dated amendment).** Crossing 80% of a
   configured budget emits a `BudgetWarning` run event once per dimension. ADR-0019 shipped the
   `onExhaustion: "final-answer"` resolution for `maxTurns`/`maxToolCalls` and deliberately kept
   tokens a hard rail; this record amends that boundary with the same one-shot discipline that
   satisfies its "never loop or spend unboundedly" rationale. Under `"final-answer"`, a
   `tokenBudget` breach (a post-response check) resolves through ADR-0019's own machinery: a
   breaching response that already carries a decodable final answer at a stop completes the Run
   directly with no extra call; otherwise any declared batch settles synthetically (never
   executes) and the Run takes at most one constrained grace Turn (`toolChoice: "none"`), whose
   usage is consumed once and exempt from re-triggering breach. Either way the Run settles as
   `RunCompleted` with `finishReason: "budget-exhausted"` and the `exhausted` dimension marker —
   the marker ADR-0019 S3's re-delegation grant flow anticipates — which this record also stamps
   on the turn and tool-call soft landings. `onExhaustion: "fail"` keeps token breach fatal
   (`AgentPolicyError`); `maxDuration` and cost stay hard rails in both modes. A token-exhausted
   Subagent child under `"final-answer"` therefore settles as success whose projected output
   reaches the parent normally, with `SubagentCompleted.exhausted` making the degradation
   observable; the parent still never receives a partial child transcript.
6. **Compaction is engine-native and projection-shaped.** When the estimated next context (the
   last provider-reported input plus a chars/4 estimate of newly appended parts) exceeds
   `contextTokenLimit`, the engine compacts at the pre-Turn seam, synchronously, per
   `CompactionPolicy`: first prune — replace application tool results older than the protected
   `keepRecentTokens` tail with `"[tool result cleared by compaction]"`, preserving message
   structure and call/result pairing — then, if still over, summarize through one metered model
   call on the Run's bound model (its usage is consumed like any other call) into a structured
   summary (goal, constraints, progress, decisions, next steps, critical context). The rebuilt
   prompt is instruction prefix + summary message + kept tail. Cut points never split an
   assistant tool call from its result, and prepared-unsettled tool records are always in the
   kept tail (ADR-0004 compatibility).
7. **Canonical compaction (DN and DC assemblies) is a record, not a rewrite.** Each compaction appends
   `CompactionCreated { runId, turn, kind: "clear-tool-results" | "summarize", coversThrough,
summary? }` inside the same epoch-fenced canonical log it covers. The session selects
   `coversThrough` itself (walking its own records with the shared token estimator, cutting only
   at whole-Turn boundaries) and clamps coverage to records of PRIOR Runs only — the owner Run's
   records are never covered, since its first response record carries the evaluated instructions
   and input, and the cross-Run `historyBefore` replay is the growth that compaction exists to
   bound. The engine's in-memory rebuild is a view that may cover more than the record; the
   record is canonical, and a threshold compaction with no prior-Run records to cover commits no
   record. The run-journal projection
   folds it: records at sequence ≤ `coversThrough` render as the summary (kind `summarize`) or
   with cleared tool results (kind `clear-tool-results`); an invalid range is fail-safe ignored
   and the full history stays authoritative. `sourceDigest` is removed from this record:
   re-verifying a digest would re-read the full covered range on every wake, defeating the
   purpose, and the record is already fenced by the log it lives in. Digest-bound artifacts
   remain the contract for host-supplied compaction through `capabilities/context.ts`, which
   stays the host-facing toolkit. Compaction never erases official history; only projections
   change.
8. **Context overflow is typed and recoverable.** A provider context-length rejection is
   classified into a typed error. With compaction configured, the engine issues at most one
   framework-level retry after compaction — transport or provider ambiguity may still duplicate
   the external model execution, per the at-least-once recovery invariant. Without compaction,
   or if the retried request overflows again, the Run fails with
   `ContextOverflowError { message, retried }` instead of an opaque provider error.

## Consequences

- The runtime specification's §9 steps 4–5 become implemented semantics; requirement IDs
  RUN-022…RUN-027 and CAP-017 are defined; RUN-011 (already re-specified by ADR-0019 as the
  honest `budget-exhausted` settlement) additionally names the token dimension and the
  `exhausted` marker; ADR-0019 carries a dated amendment note for the token-dimension extension.
- `AgentPolicy`'s shape changes (new required fields with defaults), so agent-definition digests
  change; incompatible private development data resets rather than migrating, per repository
  policy.
- Default behavior changes for existing consumers: tool results are bounded at 50 KiB unless a
  policy raises `toolResultBounds`, token exhaustion soft-lands instead of failing unless
  `onExhaustion: "fail"` is set (turn/tool-call exhaustion already soft-lands per ADR-0019), and
  every prompt carries a run-status line unless `runStatus: "off"`.
- `CompactionCreated`'s canonical shape is reshaped (kind, coversThrough, optional summary; no
  digest); stored development conversations that carry the old shape fail decode clearly.
- The DN and DC assemblies gain one new durable mutation (the compaction record append,
  owned by their shared session coordinator), which carries
  failpoints before and after and recovery-classifier coverage like every other commit point.
- Long-lived Conversations in the DN and DC assemblies stop growing their per-wake projection
  cost without bound:
  the fold starts from the latest valid compaction instead of the beginning of time.

## Rejected alternatives

- **A second exhaustion knob beside ADR-0019's.** The first draft of this record shipped its own
  `onBudgetExhausted` field and finalize Turn in parallel with ADR-0019's `onExhaustion` soft
  landing. Rejected during rebase reconciliation: one knob must govern every dimension's
  resolution, so the token dimension rides ADR-0019's machinery (synthetic settlement,
  `toolChoice: "none"`, single grace Turn, `budget-exhausted` finish reason) instead of a
  parallel mechanism.

- **Raise `tokenBudget`.** The counter is cumulative ΣC_k and quadratic in history length;
  raising it delays the same crash and doubles the silent burn. The incident budget was not too
  small — it measured the wrong quantity.
- **Cache-discount `tokenBudget` in place.** Silently changing what an existing knob measures
  under consumers' feet is worse than adding the right knobs beside it. Spend belongs to
  `costBudgetMicrousd`, whose estimator already receives cache-split usage.
- **Rewrite or delete journal records during compaction.** Violates the append-only canonical
  log (rule 7, ADR-0003). pi and codex both ship append-only compaction records
  (`CompactionEntry`, `RolloutItem::Compacted`) in production; erasure is unnecessary.
- **Background/daemon-fiber compaction.** The engine must not create daemon fibers, and a
  concurrent compactor races the turn loop. codex compacts synchronously at turn seams; so do
  we.
- **Wire `capabilities/context.ts` types directly into the engine.** The dependency direction
  forbids an engine → capabilities import, and `ModelContextMessage` is a text projection that
  cannot carry real tool parts, so it cannot be the literal prompt. Those types remain the
  host-facing artifact toolkit; the engine owns its own compaction on real Prompt values.
- **Keep `sourceDigest` on engine-emitted `CompactionCreated`.** Digest re-verification forces
  every wake to re-read the covered range — the exact O(history) work compaction exists to
  remove. The record is appended by the fenced owner into the log it covers; the log is the
  authority. Digests remain for host-supplied artifacts, which cross a trust boundary.
- **Reject oversized tool results instead of truncating.** Rejection (the broker and Subagent
  `maxResultBytes` posture) is right for programmatic inner calls, but at the model seam it
  turns one oversized grep into a dead run. Every surveyed harness truncates with an explicit
  marker; the envelope keeps the outcome useful and honest.

## Validation

- Bounding: an oversized application tool result becomes a `TruncatedToolResult` envelope of at
  most `maxBytes` encoded bytes in both the prompt and the settled record; within-bounds results
  pass through byte-identical; provider-executed results are untouched.
- Tracking: cache-read/write splits and last-call tokens round-trip through `UsageDelta`,
  `UsageTotals`, and the budget snapshot.
- Status: the run-status message appears exactly once per outgoing request, reflects the
  deterministic counters, and never appears in canonical history or the journal.
- Exhaustion: the warning fires once per dimension at 80%; a token breach under
  `"final-answer"` settles directly when the breaching response carries decodable output and
  otherwise takes at most one constrained grace Turn, completing with
  `finishReason: "budget-exhausted"` and `exhausted: "tokens"`; `"fail"` preserves the typed
  `AgentPolicyError`; duration breach stays fatal; a grace-Turn response that declares tool
  calls fails typed per ADR-0019's fail-closed rule.
- Compaction: the pre-Turn trigger fires from the estimate; prune preserves pairing and the
  protected tail; summarize is one metered call; the canonical record round-trips (DN and DC
  assemblies); the projection
  folds prune and summary records correctly, ignores invalid ranges fail-safe, and applies
  across Runs (`historyBefore`); failpoints cover the record append; crash between record and
  model call resumes onto the compacted projection without duplicating the record.
- Overflow: classified provider rejections compact-and-retry once when possible and fail
  `ContextOverflowError` otherwise; the error is typed in `E` and visible in type tests.

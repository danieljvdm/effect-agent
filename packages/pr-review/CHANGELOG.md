# @effect-agent/pr-review

## 0.1.0-beta.36

### Patch Changes

- Updated dependencies []:
  - effect-agent@0.1.0-beta.36

## 0.1.0-beta.35

### Patch Changes

- Updated dependencies []:
  - effect-agent@0.1.0-beta.35

## 0.1.0-beta.34

### Patch Changes

- [#202](https://github.com/danieljvdm/effect-agent/pull/202) [`cf4a8d9`](https://github.com/danieljvdm/effect-agent/commit/cf4a8d9c645d5d8a2e552f4bb4902af4253d91ee) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Align the Effect family with rc.111 to decode nested OpenAI error events, and preserve transformed Tool parameters under its encoded response contract.

- Updated dependencies [[`cf4a8d9`](https://github.com/danieljvdm/effect-agent/commit/cf4a8d9c645d5d8a2e552f4bb4902af4253d91ee)]:
  - effect-agent@0.1.0-beta.34

## 0.1.0-beta.33

### Patch Changes

- Updated dependencies []:
  - effect-agent@0.1.0-beta.33

## 0.1.0-beta.32

### Patch Changes

- [#192](https://github.com/danieljvdm/effect-agent/pull/192) [`047ac9a`](https://github.com/danieljvdm/effect-agent/commit/047ac9a74faa63dbbb05dafbd39a45a801d09d9c) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Preserve distinct findings with matching labels and remove premature wrap-up prompts from single-pass reviews. Publish maximum-length findings without repeating their text in inline agent prompts.

- Updated dependencies []:
  - effect-agent@0.1.0-beta.32

## 0.1.0-beta.31

### Patch Changes

- Updated dependencies []:
  - effect-agent@0.1.0-beta.31

## 0.1.0-beta.30

### Patch Changes

- [#167](https://github.com/danieljvdm/effect-agent/pull/167) [`cb2256d`](https://github.com/danieljvdm/effect-agent/commit/cb2256d48838b83aa15cf9c252194c8ac96678c3) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Accept reliability findings emitted from the reviewer's documented defect vocabulary.

- Updated dependencies []:
  - effect-agent@0.1.0-beta.30

## 0.1.0-beta.29

### Minor Changes

- [#161](https://github.com/danieljvdm/effect-agent/pull/161) [`b3be989`](https://github.com/danieljvdm/effect-agent/commit/b3be989556006cde3b0fd49c320b2f2eb492e76b) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Replace the reviewer with a provider-neutral, single-pass agent and move GitHub and provider policy
  to the private channel. Review large diffs in one bounded four-shard parallel wave, limit automatic
  GitHub waves to two, require a collaborator command for later reviews, and present findings with
  severity and category labels plus agent-ready prompts.

### Patch Changes

- Updated dependencies []:
  - effect-agent@0.1.0-beta.29

## 0.1.0-beta.28

### Patch Changes

- Updated dependencies []:
  - effect-agent@0.1.0-beta.28

## 0.1.0-beta.27

### Patch Changes

- [#158](https://github.com/danieljvdm/effect-agent/pull/158) [`3bb8632`](https://github.com/danieljvdm/effect-agent/commit/3bb8632e04b7517b4a896df0e393436d84a55ff7) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Keep incremental reviews scoped after amended or force-pushed heads by comparing complete Git tree
  snapshots across bounded PR paths, then hydrating changed paths from the current full PR records.
  Fall back to a full review with an observable reason when either snapshot is unavailable, malformed,
  or truncated.

- [#156](https://github.com/danieljvdm/effect-agent/pull/156) [`c210275`](https://github.com/danieljvdm/effect-agent/commit/c2102759a41ea392fc6493d11696debc22f9cf80) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Let Action and CLI users opt each OpenAI review into Fast processing with `service-tier: fast`, and reject the provider-specific setting when Anthropic is selected.

- [#157](https://github.com/danieljvdm/effect-agent/pull/157) [`03dccae`](https://github.com/danieljvdm/effect-agent/commit/03dccae48585e9571af8f38095927f41b05cbba5) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Keep each failed incremental review stage attached to its own unchanged paths so unrelated leftovers no longer widen model scope. Reopen discovery only for paths whose candidate verification must be regenerated.

- Updated dependencies []:
  - effect-agent@0.1.0-beta.27

## 0.1.0-beta.26

### Patch Changes

- Updated dependencies []:
  - effect-agent@0.1.0-beta.26

## 0.1.0-beta.25

### Minor Changes

- [#144](https://github.com/danieljvdm/effect-agent/pull/144) [`2de44f5`](https://github.com/danieljvdm/effect-agent/commit/2de44f5f61d1eb932fce2ef00aef08b2c4b4be18) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Let maintainers adjudicate findings from the pull request itself — reply `/adjudicate accepted-risk|refuted|obsolete[: reason]` on a finding's inline thread, or comment `/adjudicate <disposition> "<exact title>"[: reason]` in the conversation for unanchored concerns — and the exact identity leaves active findings, verdict counts, and the check conclusion, renders in a collapsed "Adjudicated" section, and persists in the signed review state; only OWNER/MEMBER/COLLABORATOR comments count, everything else is ignored fail-closed. Inject prior-round findings on re-reviewed paths into incremental reviewer prompts; **BEHAVIOR CHANGE:** direct `PrReview.run` callers now provide `ReviewExecutionContext`, using `fullReviewExecutionContextLayer` for an explicit full review.

- [#140](https://github.com/danieljvdm/effect-agent/pull/140) [`eb9c5fd`](https://github.com/danieljvdm/effect-agent/commit/eb9c5fd4683a63807b131f8c8d94e9c1205bd36d) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Remove deprecated pull-request review outputs and aliases, legacy review-state decoding, and unused
  Travel Planner fixtures. Require Cloudflare worker bindings to use the per-incarnation callback.

### Patch Changes

- [#145](https://github.com/danieljvdm/effect-agent/pull/145) [`84aef35`](https://github.com/danieljvdm/effect-agent/commit/84aef359e7551330204557c940d8c2c5db773bec) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Report undiffable files (binaries, oversized files) in gate reasons, the posted
  review callout, and the step summary with the honest remedy — remove them from
  the pull request or exclude them with ignore globs — instead of promising an
  automatic retry that can never settle them.

- [#143](https://github.com/danieljvdm/effect-agent/pull/143) [`a0ce59c`](https://github.com/danieljvdm/effect-agent/commit/a0ce59c8b172cb1a5cbfdd57086401fb1714157d) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Bind review-body concerns to their changed evidence paths so incremental reviews recheck them when related files change or disappear. Distinguish current findings from carried concerns in posted review counts.

- Updated dependencies []:
  - effect-agent@0.1.0-beta.25

## 0.1.0-beta.24

### Patch Changes

- [#138](https://github.com/danieljvdm/effect-agent/pull/138) [`00597ba`](https://github.com/danieljvdm/effect-agent/commit/00597ba82fdaeb698aa29e1b3385ea689a163d84) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Keep incremental reviews incremental after a rebase, and retry only the failed pass on unchanged leftover paths.

  A rewritten head no longer fail-closes to a full-diff rediscovery when a two-dot tree comparison can name the current PR paths whose contents changed. Outdated GitHub comments that omit `line` no longer block stale-review retirement.

- [#136](https://github.com/danieljvdm/effect-agent/pull/136) [`966fe3a`](https://github.com/danieljvdm/effect-agent/commit/966fe3ab5f01d4f812f97b6cda7a6ac7f3a46f68) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Hash review fingerprints through Effect `Crypto.Crypto` instead of `globalThis.crypto`.

  BEHAVIOR CHANGE: `computeChangesetFingerprint`, `computeProfileFingerprint`, and `PrReview` fingerprint/`run` Effects now require `Crypto.Crypto`. Node CLI/Action hosts already satisfy this via `NodeServices.layer`.

- Updated dependencies []:
  - effect-agent@0.1.0-beta.24

## 0.1.0-beta.23

### Minor Changes

- [#132](https://github.com/danieljvdm/effect-agent/pull/132) [`15f041f`](https://github.com/danieljvdm/effect-agent/commit/15f041f6dcd092f5933ce528db391d6185dd85d6) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Make incremental reviews converge: the authenticated baseline now advances on every completed
  run, carrying unsettled or unreviewable scope forward for automatic retry, and fan-out passes
  are host-scheduled with one retry each so a flaky pass can no longer reopen the whole
  post-baseline scope. BEHAVIOR CHANGE: stored review state moved to `state-v2` (the first run
  after upgrading performs one full review), blocking findings now outrank machinery gaps in the
  check conclusion, and the coordinator-model exports (`FanOutReviewer`, `makeFanOutReviewSuite`,
  `fanOutHandlersLayer`, `DelegateFileReview`, `FileReviewRequest`, `FileReviewUnitResult`,
  `FileReviewWorkRejected`, `FileReviewUnitFailed`, `ListReviewUnits`) and the
  `usageScope`/`reviewShape` options are removed — fan-out runs report whole-run usage via
  `executeFanOutReview`.

### Patch Changes

- [#125](https://github.com/danieljvdm/effect-agent/pull/125) [`1581182`](https://github.com/danieljvdm/effect-agent/commit/1581182cc7b06dcc340d15f32c8af93ecc4f0902) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Remove the ignored `failOn` option from `runReviewAction` and the `FailOnPolicy` export; host-derived check conclusions were already unconditional, so the option had no effect. The packaged Action still accepts the deprecated `fail-on` input and continues to ignore it.

- Updated dependencies []:
  - effect-agent@0.1.0-beta.23

## 0.1.0-beta.22

### Patch Changes

- [#121](https://github.com/danieljvdm/effect-agent/pull/121) [`0717444`](https://github.com/danieljvdm/effect-agent/commit/0717444097bb4fc8be4ab665ccac8a09de4f1c3d) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Publish a fan-out finding's GitHub suggestion block only when independent verification settles its text as committable replacement source. A confirmed finding whose suggestion is not settled as committable is published without the suggestion block, and a verification pass that leaves a carried suggestion unsettled fails the unit's settlement.

- Updated dependencies []:
  - effect-agent@0.1.0-beta.22

## 0.1.0-beta.21

### Patch Changes

- [#117](https://github.com/danieljvdm/effect-agent/pull/117) [`27618dc`](https://github.com/danieljvdm/effect-agent/commit/27618dc03b0703fc784dc7abc4280fc74bb95045) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Review pull requests with deterministic complete evidence sharding, independent general and
  specialist discovery, and request-bound verification that publishes only confirmed candidates.
  Default the Action to this fan-out pipeline and require complete input assignment plus settled
  configured work before emitting authenticated incremental state or a successful conclusion.
- Updated dependencies []:
  - effect-agent@0.1.0-beta.21

## 0.1.0-beta.20

### Patch Changes

- [#111](https://github.com/danieljvdm/effect-agent/pull/111) [`c715f9f`](https://github.com/danieljvdm/effect-agent/commit/c715f9f8e436fa85e8c1ef2b27f640e637ea52e4) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Align every public package with the Effect 4.0.0-rc.110 family.

- [#111](https://github.com/danieljvdm/effect-agent/pull/111) [`c715f9f`](https://github.com/danieljvdm/effect-agent/commit/c715f9f8e436fa85e8c1ef2b27f640e637ea52e4) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Fix `validateMcpDiscovery` reporting a permanent schema drift for MCP tools whose parameters or success type is a named, refined Schema (a branded ID, a bounded string, a `Schema.Class`) — both schema derivations now resolve a top-level `$ref` before comparison.

- Updated dependencies [[`c715f9f`](https://github.com/danieljvdm/effect-agent/commit/c715f9f8e436fa85e8c1ef2b27f640e637ea52e4), [`c715f9f`](https://github.com/danieljvdm/effect-agent/commit/c715f9f8e436fa85e8c1ef2b27f640e637ea52e4)]:
  - effect-agent@0.1.0-beta.20

## 0.1.0-beta.19

### Patch Changes

- [#95](https://github.com/danieljvdm/effect-agent/pull/95) [`1e0e2a5`](https://github.com/danieljvdm/effect-agent/commit/1e0e2a5b9024fd1afe1375afec00ceec5302111e) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Skip model execution for authenticated, patch-equivalent pull-request rebases while preserving the prior review conclusion. Changeset fingerprints now ignore unified-diff hunk coordinate shifts but remain sensitive to changed diff content and review configuration.

- Updated dependencies []:
  - effect-agent@0.1.0-beta.19

## 0.1.0-beta.18

### Patch Changes

- Updated dependencies []:
  - effect-agent@0.1.0-beta.18

## 0.1.0-beta.17

### Patch Changes

- Updated dependencies []:
  - effect-agent@0.1.0-beta.17

## 0.1.0-beta.16

### Patch Changes

- Updated dependencies []:
  - effect-agent@0.1.0-beta.16

## 0.1.0-beta.15

### Patch Changes

- Updated dependencies []:
  - effect-agent@0.1.0-beta.15

## 0.1.0-beta.14

### Minor Changes

- [#89](https://github.com/danieljvdm/effect-agent/pull/89) [`1469580`](https://github.com/danieljvdm/effect-agent/commit/146958084443303c5b9a1202c085e551af0ee182) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Richer review presentation, derived host-side from validated data. Every inline
  comment now ends with a collapsed "🤖 Prompt for AI agents" copy-paste block
  (opening with a fixed untrusted-review-data preamble), and the review body adds
  a consolidated all-findings prompt so demoted and carried findings hand an
  agent their instruction too. The body opens with a host-derived stats line —
  changeset size, severity tally, and a deterministic 1–5 review-effort estimate
  — and renders the model's new optional per-file `walkthrough` as a collapsed
  table whose paths are validated against the changeset like finding anchors
  (fan-out children report `fileSummaries`, projected and merged by the
  coordinator, and host-verified against the delegation Tool events so only
  in-unit child-reported summaries survive; carried findings' prompts cite their
  baseline commit, never the current head). Findings may carry an optional
  `category` chip rendered beside
  the severity; demoted and carried-finding sections collapse into counted
  `<details>` blocks. Oversized bodies shed the consolidated prompt first, then
  the walkthrough, before any review item, and every omission stays announced.
  Stale-review retirement matches both the categorized and the pre-category
  inline first-line formats.

- [#87](https://github.com/danieljvdm/effect-agent/pull/87) [`68addaa`](https://github.com/danieljvdm/effect-agent/commit/68addaa026927a75d193b64cdb86542e5c37345b) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Make review runs visible while they execute. The packaged Action now posts one sticky "review in
  progress" issue comment the moment a run starts — naming the scope, head commit, model, and
  workflow run — and rewrites that same comment in place with the settled outcome (posted verdict,
  blocking/incomplete callout, or run failure). Posting is at-least-once with generation-fenced
  writes: a stale run cannot replace a newer run's status, and duplicate comments left by unfenced
  overlapping runs are best-effort deleted by the next run. Progress reporting is cosmetic and
  fail-open: GitHub faults are logged and never change the review, the check conclusion, or the run
  result. Disable with the new `progress-comment` input; dry runs post no progress.

  Action logs now render one compact line per event (tool executions, warnings with their cause)
  instead of raw OTel-style telemetry dumps. The new `log-level` input (default `Info`) shows the
  engine's per-turn telemetry at `Debug` or quiets routine runs at `Warn`.

### Patch Changes

- [#87](https://github.com/danieljvdm/effect-agent/pull/87) [`68addaa`](https://github.com/danieljvdm/effect-agent/commit/68addaa026927a75d193b64cdb86542e5c37345b) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Keep exploratory out-of-scope reads from killing a review run. The read tools already return
  typed refusals as model-visible results, but the engine's default 3-consecutive-failure stop
  policy aborted the run when one parallel batch probed several paths outside the review scope —
  the first incremental delta whose pull-request description named other files died this way before
  the model had seen a single refusal. The flat reviewer and per-unit child policies now tolerate an
  exploratory batch (`repeatedFailureLimit: 12`, still bounded by their tool-call and duration
  budgets), and the reviewer instructions state explicitly that the listed changeset is the complete
  readable scope — in incremental reviews a deliberate subset of the pull request's full diff.
- Updated dependencies []:
  - effect-agent@0.1.0-beta.14

## 0.1.0-beta.13

### Minor Changes

- [#88](https://github.com/danieljvdm/effect-agent/pull/88) [`75f9aca`](https://github.com/danieljvdm/effect-agent/commit/75f9aca2558511b0b129c27669b7e920c3ef0b4f) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Recover GitHub-omitted textual patches through bounded UTF-8 base/head content so generated and oversized text files can complete review coverage without repository-specific ignores. Binary, unreadable, incomplete, and over-bound content remains fail-closed.

### Patch Changes

- Updated dependencies []:
  - effect-agent@0.1.0-beta.13

## 0.1.0-beta.12

### Patch Changes

- Updated dependencies []:
  - effect-agent@0.1.0-beta.12

## 0.1.0-beta.11

### Patch Changes

- [#79](https://github.com/danieljvdm/effect-agent/pull/79) [`1539616`](https://github.com/danieljvdm/effect-agent/commit/153961639051ec6dae8dcf33b0e44c138f52a790) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Give OpenAI reasoning models enough output-token and wall-clock headroom to
  finish high-effort delegated reviews instead of leaving fully read units
  unreviewed with protocol or duration failures.
- Updated dependencies []:
  - effect-agent@0.1.0-beta.11

## 0.1.0-beta.10

### Patch Changes

- [#77](https://github.com/danieljvdm/effect-agent/pull/77) [`41fc909`](https://github.com/danieljvdm/effect-agent/commit/41fc9095238a30654280396350ac0339ca603726) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Allow GitHub App-authored reviews to supply the expected posting login for authenticated
  incremental continuity and unchanged-review fingerprint matching.
- Updated dependencies []:
  - effect-agent@0.1.0-beta.10

## 0.1.0-beta.9

### Minor Changes

- [#75](https://github.com/danieljvdm/effect-agent/pull/75) [`dcea6cb`](https://github.com/danieljvdm/effect-agent/commit/dcea6cb50ff2835bd72446202742029c35c321bb) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Retire prior marker-bearing bot reviews after a newer review posts: supersede and collapse their bodies, strike findings resolved by the newest authenticated state, minimize matching inline comments as outdated, and keep cosmetic retirement failures fail-open.

### Patch Changes

- Updated dependencies []:
  - effect-agent@0.1.0-beta.9

## 0.1.0-beta.8

### Patch Changes

- [#68](https://github.com/danieljvdm/effect-agent/pull/68) [`fd16e63`](https://github.com/danieljvdm/effect-agent/commit/fd16e63f34df0653afdf7ef167bc1ddd324676b6) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Activate native context compaction for the packaged flat, file-unit, and fan-out coordinator
  reviewers with a 150k-token live-context ceiling. This keeps output and summary headroom while
  preserving the existing cumulative token budgets; tool-heavy review histories prune old results
  before paying for a summarization call.
- Updated dependencies []:
  - effect-agent@0.1.0-beta.8

## 0.1.0-beta.7

### Minor Changes

- [#54](https://github.com/danieljvdm/effect-agent/pull/54) [`afe755a`](https://github.com/danieljvdm/effect-agent/commit/afe755a331172ffca9ceee7dd82bb452c6ccbb8a) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Context economics ([#54](https://github.com/danieljvdm/effect-agent/issues/54), RUN-022–027/CAP-017): application tool results are bounded by default (50 KiB
  `TruncatedToolResult` envelopes), budget accounting becomes cache-aware with last-call
  live-context tracking, every request can carry a derived run-status message, the token
  dimension joins the `onExhaustion` soft landing (RUN-018) with the `exhausted` dimension marker,
  and the engine compacts natively at the pre-Turn seam (prune, then one metered summarize)
  with a canonical `CompactionCreated` record that projections fold across Runs; provider
  context-length rejections compact-and-retry once, then fail typed (`ContextOverflowError`).

- [#50](https://github.com/danieljvdm/effect-agent/pull/50) [`b44ed77`](https://github.com/danieljvdm/effect-agent/commit/b44ed7771c3e1ace2516507b0b54d11e662f036c) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Delegation containment (D-037, ADR-0019 S2, SUB-033): `Subagent.define` gains
  `failureMode: "error" | "return"` (default `"error"`, today's semantics). Under `"return"` every
  expected delegation failure — the declared child failure plus `SubagentPrestartDenied`,
  `SubagentBudgetExhausted`, `SubagentProjectionFailure`, and `SubagentExecutionFailure` — becomes
  model-visible result data in the Tool success union instead of failing the parent Run, so one
  dead child cannot detonate a fan-out. The engine signals (`ToolCallWaiting`,
  `SubagentDurabilityError`) always stay in the error channel, preserving durable suspension by
  construction, and the durable settlement join records the contained failure with the same
  non-failure polarity the live batch continues with. pr-review retires its same-name shadow-Tool
  workaround for the first-party option, adopts the S1 `final-answer` soft landing in all three
  default reviewer policies (an exhausted child or coordinator now returns a partial review instead
  of "unit unreviewed: AgentPolicyError"), and reverts the fan-out `repeatedFailureLimit` sizing
  hack. Contained unit failures reach coverage classification with richer tags
  (`FileReviewUnitFailed:<childErrorTag>`).

### Patch Changes

- Updated dependencies []:
  - effect-agent@0.1.0-beta.7

## 0.1.0-beta.6

### Patch Changes

- [#39](https://github.com/danieljvdm/effect-agent/pull/39) [`e13ee6e`](https://github.com/danieljvdm/effect-agent/commit/e13ee6e7817549e99837d06e86caf2dea8656aa8) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Budget soft landing (D-037, ADR-0019, RUN-018/019/020): `AgentPolicy` gains
  `onExhaustion: "final-answer" | "fail"`, defaulting to `"final-answer"` — Turn and Tool Call
  exhaustion now settle the Run through one constrained final-answer opportunity instead of failing
  it. An over-budget Tool batch settles synthetically as model-visible failed results (no handler
  starts, no durable batch declaration, exempt from repeated-failure folding), subsequent model
  requests carry `toolChoice: "none"`, Turn exhaustion admits exactly one grace Turn, and the Run
  completes with the honest `finishReason: "budget-exhausted"` on the live event, the reduced
  `AgentResult`, and (additively) the durable `SubmissionSettled` record. Duration, token, cost, and
  repeated-failure bounds stay hard rails; `onExhaustion: "fail"` preserves the prior run-fatal
  behavior exactly. BEHAVIOR CHANGE ON UPGRADE: Turn/Tool-Call budget deaths become honest
  completions unless a policy pins `"fail"` — `@effect-agent/pr-review` pins `"fail"` pending its
  containment rework.

- [#26](https://github.com/danieljvdm/effect-agent/pull/26) [`9d9bc91`](https://github.com/danieljvdm/effect-agent/commit/9d9bc910de6b0acf751d6729e955e1554688dd89) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add a `guidance-file` action input (`PR_REVIEW_GUIDANCE_FILE`): the review
  guidance can now live as a committed review-profile document instead of
  workflow YAML, read at run time and injected before any inline `guidance`.
  A configured-but-unreadable file fails typed rather than reviewing without
  its profile.

- [#43](https://github.com/danieljvdm/effect-agent/pull/43) [`0778186`](https://github.com/danieljvdm/effect-agent/commit/077818687de70f209c1e1269fae45b9c205b7b05) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Make GitHub Action PR reviews incremental across corrective pushes using authenticated,
  lineage-validated review state, preserve unresolved findings and accepted scope, provide an
  explicit final full-diff audit, and fail the review check for blocking findings or incomplete
  coverage. Align delegated file-review tool-call bounds with the maximum review-unit size so
  normal diff and context reads can complete without deterministic policy exhaustion.

- [#21](https://github.com/danieljvdm/effect-agent/pull/21) [`93281be`](https://github.com/danieljvdm/effect-agent/commit/93281be964d11caa63b5efed2976835780ca1eb8) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Introduce `@effect-agent/pr-review`: the pull-request reviewer promoted from
  `examples/pr-review` into a publishable package (owner decision D-034,
  ADR-0016). Schema-first review contracts, `PullRequestSource`/`ReviewPublisher`
  ports with GitHub REST adapters, fail-closed anchor validation and publication
  planning, flat and S1 fan-out reviewer shapes, the `PrReview` configuration
  factory (guidance, policy override, findings bound, ignore globs, extra
  read-only tools), a deterministic `./testing` entry, and `./action`/`./cli`
  host entrypoints backing the prebuilt node-runtime GitHub Action at `action/`.
  Deployment class E; review posting is never claimed exactly-once.

- [#23](https://github.com/danieljvdm/effect-agent/pull/23) [`b5b31b8`](https://github.com/danieljvdm/effect-agent/commit/b5b31b8b9c9870e0e6efd30ff305adde4021ba4f) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Skip re-reviews of unchanged changesets. Every posted review now embeds an
  invisible changeset fingerprint (SHA-256 over the ignore-filtered changeset
  plus the prompt signature); the action harness and the CLI's
  `--skip-unchanged` compare it against the last posted review through the new
  `PriorReviews` port and skip typed when nothing effective changed — so
  base-branch auto-merges and equivalent rebases stop re-triggering reviews,
  while real changes, conflict resolutions, and configuration changes still
  review. Fails open: a fingerprint lookup fault reviews instead of skipping.
  `PrReview.make`/`makeFanOut` expose the fingerprint; `runReviewAction` now
  takes the reviewer object (`{ run, fingerprint }`) and a `skip-unchanged`
  action input (default `true`).
- Updated dependencies [[`94c169a`](https://github.com/danieljvdm/effect-agent/commit/94c169a44a248972158ca955e33fb02dd5e55463)]:
  - effect-agent@0.1.0-beta.6

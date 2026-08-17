# @effect-agent/pr-review

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

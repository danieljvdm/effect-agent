# @effect-agent/pr-review

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

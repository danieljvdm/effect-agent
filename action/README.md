# Effect Agent PR Review action

This is a prebuilt Node runtime GitHub Action for
[`@effect-agent/pr-review`](../packages/pr-review). Its bounded, read-only reviewer validates input
assignment, verifier-confirmed findings, and anchors in host code before the check concludes. The committed
`dist/index.mjs` bundle runs directly (`vp run action:build` regenerates it;
`vp run check` fails when it is stale).
No install step is required.

```yaml
on:
  pull_request:
    types: [opened, reopened, ready_for_review, synchronize, labeled]
permissions:
  contents: read
  pull-requests: write
jobs:
  review:
    runs-on: ubuntu-latest
    steps:
      # Only needed for guidance-file (reads the committed profile); the
      # reviewer itself never reads the checkout.
      - uses: actions/checkout@v4
      - id: review
        uses: danieljvdm/effect-agent/action@main
        with:
          openai-api-key: ${{ secrets.OPENAI_API_KEY }}
          state-secret: ${{ secrets.PR_REVIEW_STATE_SECRET }}
          # Set when github-token is a custom GitHub App token:
          # review-author: kommunikasie[bot]
          # or: provider: anthropic + anthropic-api-key
          effort: high # low..max or a number in [0,1], per-provider ladder
          guidance-file: .github/review-guidance.md # committed review profile
          guidance: |
            This is an Effect codebase. Flag naked Promises in public APIs.
          ignore: "**/*.lock,dist/**"
          fan-out: true # settled assurance requires discovery + verification
          review-mode: incremental
          retire-stale-reviews: true
```

No `actions/checkout` is required: the reviewer reads the pull request
through the GitHub API and never checks out or executes untrusted PR code.
`guidance-file` is the exception because it reads from the workspace. On `pull_request` events,
the profile comes from the PR's merge ref and has the same trust level as the workflow file, which
PRs can also edit. Repositories that need a base-ref profile should check out the file from the
base branch. Do not combine `guidance-file` with `pull_request_target`.

## Incremental reviews and authenticated continuity state

Each completed review may embed a bounded, schema-validated state marker. `state-secret` signs the
PR identity, base/head lineage, reviewer profile, settled-scope fingerprint, unresolved findings,
retryable paths, and settlement status. On `synchronize`, the action reviews changed paths still
present in the PR plus carried unreviewed paths. Unchanged findings remain active; changed or
reverted paths receive fresh discovery. A failed pass carries only its own scope forward, and the
baseline still advances. A compatible base advance also includes overlapping PR paths.

Before comparing ancestry, the action checks the authenticated settled-scope fingerprint. It
hashes the effective diff, bounded patchless
evidence, PR framing, and reviewer profile, but excludes commit IDs, base ancestry, and hunk line
coordinates. A fully settled, patch-equivalent rebase skips model work and keeps the prior
conclusion. Changed evidence or configuration reviews again.

Only a terminal marker from `review-author`, pinned to the reviewed commit and signed with the same
secret, may narrow scope. The default author is `github-actions[bot]`; custom GitHub App tokens
require `<app-slug>[bot]`. The marker is capped at 24,000 characters. Missing, invalid, or oversized
state and incompatible PR or profile identity force a full review. After state passes those checks,
an unavailable, truncated, or non-ancestor three-dot comparison may use a two-dot content
comparison to identify changed PR paths. `skip-unchanged: "true"` is the default.

The workflow needs `contents: read` and `pull-requests: write`, but not `checks: write`.
Fingerprints compare text, not runtime meaning, so use `review-mode: final` when a base change needs
a fresh semantic audit.

With `retire-stale-reviews: "true"`, the action collapses older reviews from the same bot, strikes
resolved findings, and minimizes outdated inline comments. It never edits newer reviews or the
signed state comment. Retirement is cosmetic and fail-open.

## Maintainer adjudication

A maintainer settles a finding without changing code by replying
`/adjudicate accepted-risk|refuted|obsolete[: reason]` on its inline thread, or — for an unanchored
concern — commenting `/adjudicate <disposition> "<exact title>"[: reason]` in the PR conversation.
The exact identity leaves active findings, verdict counts, and the check conclusion, renders in a
collapsed "Adjudicated" section, and persists in the signed state; re-running the action (the next
push, or a manual re-run) applies it, and the skip-unchanged path re-reads adjudications so a
blocking check lifts without a new commit. Only OWNER, MEMBER, or COLLABORATOR comments count —
everything else is ignored fail-closed — the later adjudication of an identity wins, and an
adjudication never strikes a finding as "resolved" during retirement (a verdict is not a fix).
Free-text rebuttals are deliberately not parsed: only the explicit verb is auditable.

## Run visibility

The action posts one sticky progress comment for each active review and rewrites it with the final
outcome. Invisible markers and `review-author` protect unrelated comments. Writes are at least once
and generation-fenced; the next run removes duplicates when possible. Use a workflow-level per-PR
concurrency group for strict single-comment behavior. Progress is cosmetic and fail-open. Dry runs
post nothing; `progress-comment: "false"` disables it.

Logs render one line per event. Use `log-level: Debug` for per-turn and per-handler telemetry or
`Warn` to hide routine events.

## Final merge-readiness audit

`review-mode: final` ignores prior state, reviews the bounded full PR diff, and verifies fresh
candidates. This repository runs it when an operator applies `pr-review:final-audit`. The required
`review` check blocks merges on a blocking or incomplete result.

Posted reviews contain a host-derived severity callout and statistics, a path-validated walkthrough,
concerns, inline comments, a copyable agent prompt, and a run footer. Full-review fallbacks name
their reason. The action also writes a step summary and `conclusion`, `input-coverage`,
`review-assurance`, `review-mode`, and `review-reason` outputs.

The check ignores the model verdict. Blocking findings or concerns produce `blocking`; incomplete
input or an unsettled pass produces `incomplete` and carries the affected paths forward. Invalid
anchors are discarded and counted. Fan-out is the default because the flat reviewer has no
independent verifier. Every assigned unit receives general and specialist discovery, and large
diffs split into bounded evidence shards. Capacity overflow names every affected path and the exact
shard count. Settled assurance means the configured work completed, not that every defect was
found.

Custom `runReviewAction` hosts need a `profileFingerprint` and authenticated state to skip an
unchanged review.

Keep job `timeout-minutes` above `max-duration-minutes`, whose defaults are 8 for flat reviews and
20 for fan-out. A runner kill posts nothing; budget exhaustion fails typed.

[`action.yml`](action.yml) documents inputs, outputs, and defaults. Execution remains deployment
class E, and posting is never exactly once. Runtime failure posts nothing. Incomplete work posts an
`incomplete` result and carries the gap forward. Draft and non-PR events return `skipped=true`.

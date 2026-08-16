# Effect Agent PR Review action

Prebuilt node-runtime GitHub Action over
[`@effect-agent/pr-review`](../packages/pr-review): a bounded, read-only
reviewer whose findings and required-file coverage are validated against the
real diff before the check concludes. No install step — the committed
`dist/index.mjs` bundle runs directly (`vp run action:build` regenerates it;
`vp run check` fails when it is stale).

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
          review-mode: incremental
          retire-stale-reviews: true
```

No `actions/checkout` is required: the reviewer reads the pull request
through the GitHub API and never checks out or executes untrusted PR code.
(`guidance-file` is the exception: it reads from the workspace, so on
`pull_request` events the profile comes from the PR's merge ref — the same
trust level as the workflow file itself, which PRs can also edit. Repos that
want a base-ref profile should check the file out from the base branch, and
must not combine `guidance-file` with `pull_request_target`.)

## Incremental reviews and authenticated continuity state

Every review with complete host-verified coverage embeds a bounded,
schema-validated, terminal state marker in its review body. `state-secret`
HMAC-authenticates the marker so model text or another workflow cannot forge
scope-narrowing authority. The marker records the PR
identity, base/head lineage, reviewer-profile fingerprint, reviewed-scope
fingerprint, and unresolved findings/concerns. On the next `synchronize`, the
action validates that state and asks GitHub for the previous-head...current-head
comparison. Only changed paths still present in the current PR diff are sent
to the model; prior unresolved findings in unchanged paths remain active, and
paths changed or reverted since the baseline are reconsidered. A compatible
ancestor advance of the base includes overlapping PR paths as affected
context.

The action visibly falls back to the full current PR diff when state is
missing or unreadable, PR/base/head/profile identity is incompatible, a prior
head is no longer an ancestor, a comparison is unavailable or truncated, or
the base lineage changed materially. `skip-unchanged: "true"` (the default)
avoids model execution when the same head was already covered, but preserves
the stored blocking or successful conclusion.
Only terminal markers authored by the configured `review-author` identity,
authenticated with the same stable `state-secret`, and pinned to the review's
commit may narrow scope. The author defaults to `github-actions[bot]`; when
`github-token` belongs to a custom GitHub App, set `review-author` to
`<app-slug>[bot]`. Missing or rotated secrets, user-authored marker text, and
author/token mismatches safely force a full review. Prefer a dedicated secret;
do not expose it to the model or derive it from pull-request content.
The authenticated marker itself is capped at 24,000 characters. Signing or
size failures omit state, render a bounded warning, and force the next run to
review fully instead of posting continuity data that cannot be recovered.

After a new state-bearing review posts, `retire-stale-reviews: "true"` (the
default) turns earlier marker-bearing bot reviews into collapsed, superseded
history. Findings absent from the newest unresolved set are struck through in
the old body, and their inline comments are minimized as outdated. The
mutation boundary requires the same GitHub actor as the newly posted review
and a strictly older submission time/id, so copied markers and newer concurrent
reviews are not touched. The
authenticated state comments remain byte-identical and terminal, so edited
reviews are still valid continuity inputs. Retirement is cosmetic and
fail-open: GitHub edit or minimization failures are logged without changing
the review check conclusion. Set the input to `"false"` to leave prior review
bodies and inline comments untouched.

## Final merge-readiness audit

`review-mode: final` deliberately ignores the incremental baseline and audits
the bounded full current PR diff. It is not run on ordinary corrective pushes.
This repository triggers one such audit when an operator applies the
`pr-review:final-audit` label. The audit reruns the same required `review`
check on the current head, so a blocking or incomplete final audit prevents
merge. Removing and reapplying the label requests another explicit audit.

Posted reviews open with a severity callout derived host-side from the
validated findings, followed by a host-derived stats line (changeset size,
severity tally, deterministic review-effort estimate) and a collapsed
per-file walkthrough table whose paths are validated against the changeset.
Non-anchored concerns render as body sections; inline comments carry an
optional category chip and a collapsed copy-paste "Prompt for AI agents"
(with a consolidated all-findings variant in the body). Reviews end with a
footer naming the selected scope, model, token usage, and workflow run.
Deliberate full-review fallbacks explain their reason in the body. The run
also writes a step summary and `conclusion`, `coverage`, `review-mode`, and
`review-reason` outputs.

The check conclusion does not trust the model verdict. Any active blocking
finding or concern fails the job. Missing required coverage — including an
`AgentPolicyError`, failed/unassigned unit, undiffable required path,
truncated source surface, or coordinator failure — is also non-success. The
legacy `fail-on` input is accepted for compatibility but no longer weakens or
changes this conservative gate.

If you set `max-duration-minutes` (or rely on the defaults: 8 flat / 15
fan-out), keep the job's `timeout-minutes` above it — a runner-killed job
posts nothing, while a budget-ended run fails typed with its forensics.

Inputs, outputs, and defaults are documented in [`action.yml`](action.yml).
Honest limits: execution remains deployment class E and posting is never
exactly-once. Review progress survives across runs only through the bounded
state in successfully covered GitHub reviews. A runtime failure posts
nothing; a settled but structurally incomplete review is posted honestly and
fails the check. A draft or non-PR event is a typed skip (`skipped=true`).

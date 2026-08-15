# Effect Agent PR Review action

Prebuilt node-runtime GitHub Action over
[`@effect-agent/pr-review`](../packages/pr-review): a bounded, read-only
reviewer whose findings are validated against the real diff before anything
is posted. No install step — the committed `dist/index.mjs` bundle runs
directly (`bun run action:build` regenerates it; `bun run check` fails when
it is stale).

```yaml
on:
  pull_request:
    types: [opened, reopened, ready_for_review, synchronize]
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
          # or: provider: anthropic + anthropic-api-key
          effort: high # low..max or a number in [0,1], per-provider ladder
          guidance-file: .github/review-guidance.md # committed review profile
          guidance: |
            This is an Effect codebase. Flag naked Promises in public APIs.
          ignore: "**/*.lock,dist/**"
          fail-on: never # or request-changes
```

No `actions/checkout` is required: the reviewer reads the pull request
through the GitHub API and never checks out or executes untrusted PR code.
(`guidance-file` is the exception: it reads from the workspace, so on
`pull_request` events the profile comes from the PR's merge ref — the same
trust level as the workflow file itself, which PRs can also edit. Repos that
want a base-ref profile should check the file out from the base branch, and
must not combine `guidance-file` with `pull_request_target`.)

Re-reviews of an unchanged changeset are skipped by default: each posted
review embeds a changeset fingerprint, so base-branch auto-merges and
equivalent rebases don't re-trigger the model (`skip-unchanged: "false"`
disables this). Model, effort, and guidance changes invalidate the
fingerprint and review again.

Posted reviews open with a severity callout derived host-side from the
validated findings, carry non-anchored concerns as body sections, and end
with a footer naming the model, token usage, and workflow run. The run also
writes a step summary (verdict, counts, tokens) to the Actions job page.

If you set `max-duration-minutes` (or rely on the defaults: 8 flat / 15
fan-out), keep the job's `timeout-minutes` above it — a runner-killed job
posts nothing, while a budget-ended run fails typed with its forensics.

Inputs, outputs, and defaults are documented in [`action.yml`](action.yml).
Honest limits: deployment class E — one ephemeral run per event, no
durability claim, and posting is never exactly-once; a failed or truncated
run posts nothing. A draft or non-PR event is a typed skip (`skipped=true`).

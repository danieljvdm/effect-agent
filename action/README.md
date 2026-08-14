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
      - id: review
        uses: danieljvdm/effect-agent/action@main
        with:
          openai-api-key: ${{ secrets.OPENAI_API_KEY }}
          # or: provider: anthropic + anthropic-api-key
          guidance: |
            This is an Effect codebase. Flag naked Promises in public APIs.
          ignore: "**/*.lock,dist/**"
          fail-on: never # or request-changes
```

No `actions/checkout` is required: the reviewer reads the pull request
through the GitHub API and never checks out or executes untrusted PR code.

Inputs, outputs, and defaults are documented in [`action.yml`](action.yml).
Honest limits: deployment class E — one ephemeral run per event, no
durability claim, and posting is never exactly-once; a failed or truncated
run posts nothing. A draft or non-PR event is a typed skip (`skipped=true`).

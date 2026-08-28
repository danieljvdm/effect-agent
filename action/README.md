# Effect Agent PR Review action

This directory is the published GitHub Action path. It contains the consumer
contract in `action.yml` and the committed JavaScript bundle in `dist/`.

The private
[`@effect-agent/pr-review-action`](../packages/pr-review-action) workspace
owns the source and tests. The public
[`@effect-agent/pr-review`](../packages/pr-review) package remains provider-
and transport-neutral.

Rebuild the committed bundle with `vp run action:build`.

## Review behavior

One wave reviews up to four bounded diff shards, each with one tool-free model turn. The host
validates paths and RIGHT-side anchors, merges the results, and publishes against the inspected
head only after every shard settles. A failed wave publishes an honest failure marker and no
findings. Blocking findings request changes and fail the Action after publication; other outcomes
remain comments and cannot clear an older change request.

Automatic waves use the configured limit, defaulting to two; zero disables automatic reviews.
Only trusted bot-authored terminal markers count. Failed attempts count but cannot become diff
baselines. An owner, member, or collaborator can request `@effect-agent review` for incremental
review or `@effect-agent review full` for the whole admitted diff. Manual waves do not consume the
automatic allowance. Missing baselines or incomplete comparisons stop incremental review instead
of silently expanding scope. The workflow runs trusted default-branch code, serializes attempts,
and refuses stale heads.

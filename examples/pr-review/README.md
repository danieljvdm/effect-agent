# GitHub review channel

This private leaf workspace adapts GitHub to `@effect-agent/pr-review`. It owns
event selection, REST calls, OpenAI binding, diff admission, and publication.
The package itself remains provider- and transport-neutral.

Large admitted diffs are partitioned into at most four size-balanced shards.
Each shard gets one tool-free model turn, all shards run in one bounded parallel
wave, and the channel merges at most twelve findings into one GitHub review.

Automatic events admit at most two review waves, including failed waves, for a
hard ceiling of eight automatic model turns per pull request. Further pushes
require `@effect-agent review` or `@effect-agent full review` from a repository
collaborator. Review bodies carry only a tiny terminal marker; no model
conversation or signed continuity state is persisted.

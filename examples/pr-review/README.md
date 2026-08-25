# GitHub review channel

This private leaf workspace adapts GitHub to `@effect-agent/pr-review`. It owns
event selection, REST calls, OpenAI binding, diff admission, and publication.
The package itself remains provider- and transport-neutral.

Large admitted diffs are partitioned into at most four size-balanced shards.
Each shard gets one tool-free model turn, all shards run in one bounded parallel
wave, and the channel merges at most twelve findings into one GitHub review.
It renders each finding with a severity and category label plus a collapsed,
copyable prompt for coding agents. The compact receipt splits input usage into
uncached, cached, and cache-write tokens. For known GPT-5.6 models, it also
estimates cost from OpenAI's published standard input, cached-input,
cache-write, and output rates. Presentation is deterministic and adds no model
calls.

The bundled channel resolves a defaulted `ReviewPresentation` Effect reference.
An embedding host may provide another implementation to replace the visible
GitHub Markdown without changing the reviewer. The channel always appends its
trusted attempt marker after rendering, so a presentation override cannot
weaken automatic-wave accounting.

`automatic-review-limit` sets the number of automatic attempts per pull request
and defaults to two. It accepts any non-negative integer without a configured
upper cap; zero disables automatic reviews and notices. The final admitted
attempt shows that automatic reviews are paused and points collaborators to
`@effect-agent review` or `@effect-agent review full`. The previous slash forms
remain accepted as compatibility aliases. The next automatic event
publishes one closing no-model review with the last completed review and current
head. Later pushes remain quiet. Manual reviews do not resume automatic reviews.
Trailing command whitespace is ignored; additional command text is rejected before
model execution. An accepted manual command receives an `eyes` reaction before the channel reads
pull-request state or starts model work.
Review bodies carry only a tiny terminal marker; no model conversation or
signed continuity state is persisted.

The first automatic attempt and every full command review GitHub's current PR
diff. Later incremental attempts compare the last completed reviewed head with
the current head by exact commit-tree contents across the current pull-request
paths. This keeps amended, rebased, and force-pushed reviews incremental without
persisting file state. An unavailable or truncated tree comparison makes no
model call; only an explicit full command may widen review scope. Changed paths
are hydrated from the current pull-request diff, and published inline findings
are rechecked against that diff.

Blocking findings publish a GitHub change request against the inspected head.
Other reviews remain comments because a partial later pass cannot safely clear
an older blocking review. A maintainer dismisses a resolved change request.

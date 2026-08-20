---
"@effect-agent/pr-review": patch
---

Keep incremental reviews incremental after a rebase, and retry only the failed pass on unchanged leftover paths.

A rewritten head no longer fail-closes to a full-diff rediscovery when a two-dot tree comparison can name the current PR paths whose contents changed. Outdated GitHub comments that omit `line` no longer block stale-review retirement.

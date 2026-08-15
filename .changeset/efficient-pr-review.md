---
"@effect-agent/pr-review": patch
---

Activate native context compaction for the packaged flat, file-unit, and fan-out coordinator
reviewers with a 150k-token live-context ceiling. This keeps output and summary headroom while
preserving the existing cumulative token budgets; tool-heavy review histories prune old results
before paying for a summarization call.

---
"@effect-agent/pr-review": patch
---

Keep each failed incremental review stage attached to its own unchanged paths so unrelated leftovers no longer widen model scope. Reopen discovery only for paths whose candidate verification must be regenerated.

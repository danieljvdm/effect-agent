---
"@effect-agent/engine": patch
---

Preserve Run policy allowances across replacement Attempts and reserve programmatic calls and grace finalization before execution. Reject unusable compaction summaries while retaining reported usage and the previous summary.

BEHAVIOR CHANGE: Supply complete, consistent `resumeUsage` whenever a custom coordinator passes `resume` to the runtime.

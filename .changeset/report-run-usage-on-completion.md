---
"@effect-agent/capabilities": patch
"@effect-agent/engine": patch
"@effect-agent/core": patch
---

Report cumulative Run spend on `RunCompleted`, `SubagentCompleted` and the agent result. A parent could not account for delegated work before this: a child is a separate Run, so the parent's `RunBudgetHook` never observed its model calls, and no other seam carried the totals back. The new `usage` field is optional so records written before it still decode.

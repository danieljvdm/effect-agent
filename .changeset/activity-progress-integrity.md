---
"@yielded/agent-storage-sqlite": patch
"@yielded/agent": patch
---

Reject oversized activity progress before persisting it and reject pending work beyond the captured Thread tail. Keep prior progress intact on rejected writes and release the pass's claim on inconsistent tails.

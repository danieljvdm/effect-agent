---
"@effect-agent/storage-sqlite": patch
"@effect-agent/thread": patch
---

Reject oversized activity progress before persisting it and reject pending work beyond the captured Thread tail. Keep prior progress intact on rejected writes and release the pass's claim on inconsistent tails.

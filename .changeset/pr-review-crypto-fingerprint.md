---
"@effect-agent/pr-review": patch
---

Hash review fingerprints through Effect `Crypto.Crypto` instead of `globalThis.crypto`.

BEHAVIOR CHANGE: `computeChangesetFingerprint`, `computeProfileFingerprint`, and `PrReview` fingerprint/`run` Effects now require `Crypto.Crypto`. Node CLI/Action hosts already satisfy this via `NodeServices.layer`.

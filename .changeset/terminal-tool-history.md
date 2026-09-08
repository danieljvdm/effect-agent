---
"@effect-agent/engine": patch
"@effect-agent/thread": patch
---

Preserve completed Tool results before repeated-failure termination and allow later context rollover past invisible incomplete batches from canonically terminated prior Runs without rewriting or replaying their evidence.

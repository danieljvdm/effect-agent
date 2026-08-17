---
"@effect-agent/engine": patch
"@effect-agent/session": patch
"@effect-agent/testing": patch
---

Preserve one wall-clock `maxDuration` deadline across durable Attempts. The coordinator now
derives the logical Run deadline from its first canonical input record, so recovery and
`waitingForChild` suspension cannot reset the parent allowance; queue time remains excluded and
the engine's deadline option is tightening-only. Already-settled child joins remain mandatory
recovery cleanup before an expired parent records its typed duration failure; cleanup authority
names the exact open delegation Calls and duration interruption resumes before continuation.
Adapter certification fixtures now keep their Run duration above the deliberate multi-round
virtual lease-expiry horizon instead of relying on replacement Attempts to reset it.

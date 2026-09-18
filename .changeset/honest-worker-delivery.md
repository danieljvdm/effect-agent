---
"effect-agent": patch
---

Return retained worker delivery states with stable message references and inspect the same operation through destination acceptance and settlement.

BEHAVIOR CHANGE: Read `Subagent.start(...).delivery` and the `MessageStatus` returned by `followUp`; inspect their `message` instead of resending pending input, and use an accepted `receipt` for execution results, waiting, or cancellation.

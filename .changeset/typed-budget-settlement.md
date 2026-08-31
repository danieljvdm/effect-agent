---
"@effect-agent/core": minor
"@effect-agent/thread": minor
---

Typed budget dimension on durable settlements (RUN-011, #83): the canonical `SubmissionSettled`
record additively persists `exhausted` (`"tokens" | "tool-calls" | "turns"`) beside
`finishReason: "budget-exhausted"` for a completed soft landing, and `policyLimit` (the typed
`AgentPolicyError.limit`) beside the bounded `{errorTag, message}` failure projection for a
`failed` hard-rail settlement — consumers read the dimension typed instead of parsing message
text. Decode is family-bound fail-closed (`exhausted` only with the budget-exhausted
finishReason, `policyLimit` only on a failed outcome) and histories persisted before the
metadata existed keep decoding with it absent (schemaVersion 1 unchanged).
`@effect-agent/core` now exports the `ExhaustedLimit` and `PolicyLimit` literal schemas backing
the fields.

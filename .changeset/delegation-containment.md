---
"@effect-agent/capabilities": minor
---

Delegation containment (D-037, ADR-0019 S2, SUB-033): `Subagent.define` gains
`failureMode: "error" | "return"` (default `"error"`, today's semantics). Under `"return"` every
expected delegation failure — the declared child failure plus `SubagentPrestartDenied`,
`SubagentBudgetExhausted`, `SubagentProjectionFailure`, and `SubagentExecutionFailure` — becomes
model-visible result data in the Tool success union instead of failing the parent Run, so one
dead child cannot detonate a fan-out. The engine signals (`ToolCallWaiting`,
`SubagentDurabilityError`) always stay in the error channel, preserving durable suspension by
construction, and the durable settlement join records the contained failure with the same
non-failure polarity the live batch continues with.

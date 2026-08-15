---
"@effect-agent/capabilities": minor
"@effect-agent/pr-review": minor
---

Delegation containment (D-037, ADR-0019 S2, SUB-033): `Subagent.define` gains
`failureMode: "error" | "return"` (default `"error"`, today's semantics). Under `"return"` every
expected delegation failure — the declared child failure plus `SubagentPrestartDenied`,
`SubagentBudgetExhausted`, `SubagentProjectionFailure`, and `SubagentExecutionFailure` — becomes
model-visible result data in the Tool success union instead of failing the parent Run, so one
dead child cannot detonate a fan-out. The engine signals (`ToolCallWaiting`,
`SubagentDurabilityError`) always stay in the error channel, preserving durable suspension by
construction, and the durable settlement join records the contained failure with the same
non-failure polarity the live batch continues with. pr-review retires its same-name shadow-Tool
workaround for the first-party option, adopts the S1 `final-answer` soft landing in all three
default reviewer policies (an exhausted child or coordinator now returns a partial review instead
of "unit unreviewed: AgentPolicyError"), and reverts the fan-out `repeatedFailureLimit` sizing
hack. Contained unit failures reach coverage classification with richer tags
(`FileReviewUnitFailed:<childErrorTag>`).

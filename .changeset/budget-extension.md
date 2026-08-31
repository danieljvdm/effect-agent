---
"@effect-agent/engine": minor
"@effect-agent/capabilities": minor
"@effect-agent/thread": patch
---

Budget extension (D-037, ADR-0019 S3, RUN-021/SUB-034): `RunOptions` gains tightening-only
`toolCallAllowance` and `turnAllowance` — the effective limit is
`min(policy bound, max(1, floor(allowance)))`, never wider, and the `onExhaustion` soft landing
keys off the effective limits. `Subagent.define` gains
`toolCallAllowance: { default, fromParameters }`, clamped fail-closed to the delegation's
per-invocation `SubagentPolicy.maxToolCalls` slice and threaded into ephemeral child runs, so an
orchestrator model grants a scout more budget by re-delegating with a raised allowance (fresh
child; never a mid-flight top-up). `projectResult` now receives a bounded
`SubagentResultContext` whose `budgetExhausted` marker is honest on both paths — from the
ephemeral child result's `finishReason`, or from the child Settlement's durable marker carried
through the new optional `ChildEstablishSettled.finishReason` (threaded by the thread
coordinator shared by the DN and DC assemblies; exercised in the DN-profile durable-subagent
suites) — so a budget-truncated partial can be surfaced in the declared success Schema. Existing one-argument `projectResult` functions keep
compiling unchanged. Also hardens S2 containment per its autoreviewer findings: `Subagent.define`
is overloaded so the Tool channels follow the `failureMode` value; genuine engine signals are
classified by unspoofable provenance instead of `instanceof` on exported classes; each delegation
exposes its canonical `containedFailure` schema (pr-review's coverage decoder now derives from
it); and the pr-review child reviewer deliberately returns to typed exhaustion — a review is a
coverage claim, so a budget-exhausted unit stays honestly unreviewed (contained as result data,
never run-fatal).

---
"@effect-agent/core": minor
"@effect-agent/engine": minor
"@effect-agent/session": minor
---

Budget soft landing (D-037, ADR-0019, RUN-018/019/020): `AgentPolicy` gains
`onExhaustion: "final-answer" | "fail"`, defaulting to `"final-answer"` — Turn and Tool Call
exhaustion now settle the Run through one constrained final-answer opportunity instead of failing
it. An over-budget Tool batch settles synthetically as model-visible failed results (no handler
starts, no durable batch declaration, exempt from repeated-failure folding), subsequent model
requests carry `toolChoice: "none"`, Turn exhaustion admits exactly one grace Turn, and the Run
completes with the honest `finishReason: "budget-exhausted"` on the live event, the reduced
`AgentResult`, and (additively) the durable `SubmissionSettled` record. Duration, token, cost, and
repeated-failure bounds stay hard rails; `onExhaustion: "fail"` preserves the prior run-fatal
behavior exactly. BEHAVIOR CHANGE ON UPGRADE: Turn/Tool-Call budget deaths become honest
completions unless a policy pins `"fail"`.

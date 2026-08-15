---
"@effect-agent/engine": minor
---

Model-visible output contract (#41, #55, RUN-028/TEST-016): every model request of a Run whose
Agent Definition declares an output Schema now carries a framework-owned system message stating
the final-output contract — a fixed directive plus the JSON Schema derived from the encoded side
of `agent.definition.output` via Effect AI's derivation, inserted immediately after the request's
last system message. The contract is a request-time projection applied after
`RunContextHook.prepare`: official history, canonical records, run events, and the DN/DC golden
are unchanged, and compaction cannot drop it. Context adapters receive the exact text through the
new additive optional `RunContextRequest.outputContract` field so a limit-targeting hook can
reserve its overhead; the field is absent entirely when the output Schema cannot render to JSON
Schema, in which case the Run behaves exactly as before with one Turn-1 diagnostic per Attempt.
`decodeFinalOutput` remains the conformance authority (AUTH-008). BEHAVIOR CHANGE ON UPGRADE:
model-visible prompts grow by the rendered Schema on every request, and tests asserting
request-message shapes will see one additional system message; hand-written JSON-shape prose in
`instructions` becomes redundant but stays harmless.

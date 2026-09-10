---
"@effect-agent/core": patch
"@effect-agent/engine": patch
---

Expose native tool failure modes through `Agent.inspectTools` and distinguish propagated failures from returned results in tool events, spans, and logs. Clarify when declared tool failures end a run and when the model or programmatic caller receives them.

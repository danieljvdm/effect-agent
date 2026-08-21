---
"@effect-agent/engine": minor
---

Reject malformed DN/DC resume Tool results and usage seeds before external execution, and preserve
engine infrastructure causes on live errors without exposing them in public diagnostics. BEHAVIOR
CHANGE: pass a positive finite `maxResultBytes` to `ToolBroker.openPass`; `AgentRuntime.layer` is
removed.

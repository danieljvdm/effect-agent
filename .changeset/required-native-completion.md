---
"@effect-agent/core": patch
"@effect-agent/engine": patch
---

Allow agents to require a native completion Tool, rejecting ordinary final text instead of parsing it as structured output. Permit a sole completion Tool on the final allowed turn when no further model call is needed.

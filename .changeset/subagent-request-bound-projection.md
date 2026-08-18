---
"@effect-agent/capabilities": patch
---

Pass the original Schema-decoded delegation parameters to `projectResult` on both ephemeral and
durable attached-subagent paths so result declassification can bind child output to the exact
request without trusting echoed identity fields.

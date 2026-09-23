---
"effect-agent": patch
---

Support ephemeral attached helpers inside durable runs with `Subagent.make(..., { execution: "ephemeral" })`. Retain committed results and execution allowances across recovery while applying ordinary tool replay rules.

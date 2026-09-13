---
"effect-agent": patch
---

Run agents and attached subagents with `Ephemeral.layer` for in-memory conversation history, default IDs, and module-level `Subagent.layer` and `ThreadHistory.layer` APIs.

BEHAVIOR CHANGE: Remove `IdGenerator` from service requirement unions and omit routine ID Layer provisions; custom generator overrides still work, and explicitly selecting the default uses the module-level `layer` export from `effect-agent/id-generator`.

BEHAVIOR CHANGE: Replace `ThreadHistory.layerTransient` with `ThreadHistory.layer`; share one application Layer and reuse Thread IDs to retain conversations between Runs. Complete history updates remain after a failed or interrupted Run and are released when the application Scope closes. Custom history adapters must declare `retention` as `"incremental"` or `"on-success"` and return a history owner from `open`.

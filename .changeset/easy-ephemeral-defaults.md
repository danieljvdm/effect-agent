---
"effect-agent": patch
---

Run agents and attached subagents with `Ephemeral.layer`, default IDs, and module-level `Subagent.layer` and `ThreadHistory.layerTransient` APIs.

BEHAVIOR CHANGE: Remove `IdGenerator` from service requirement unions and omit routine ID Layer provisions; custom generator overrides still work, and explicitly selecting the default uses the module-level `layer` export from `effect-agent/id-generator`.

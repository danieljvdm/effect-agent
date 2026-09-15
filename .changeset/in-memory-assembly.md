---
"effect-agent": patch
---

Rename the in-memory runtime setup to `InMemory.layer` and clarify that conversations can span Runs for the lifetime of the application Scope.

BEHAVIOR CHANGE: Replace the `Ephemeral` root import with `InMemory` and the `effect-agent/ephemeral` module path with `effect-agent/in-memory`.

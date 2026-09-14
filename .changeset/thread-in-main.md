---
"effect-agent": minor
---

Fold `@effect-agent/thread` into `effect-agent` and expose the in-memory conversation model through the `Thread` namespace. Keep database drivers and platform hosts in their adapter packages.

BEHAVIOR CHANGE: Replace `@effect-agent/thread/*` imports with `effect-agent/*` and remove the old dependency. Use `Thread.Store`, `Thread.Thread`, `Thread.layerMemory`, and `Thread.toPrompt` in place of the `EphemeralThreads` module; import `PersistentHistory` from the package root for `PersistentHistory.layer`. Stored formats and service identities are unchanged.

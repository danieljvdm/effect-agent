---
"@effect-agent/platform-cloudflare": patch
---

Run Dynamic Worker Code Mode host calls on a Scope-owned pass fiber so they inherit the `execute` Context and die with the pass.

BEHAVIOR CHANGE: `CodeExecutionHost.call` now sees services provided to `execute` instead of Effect defaults from a `runFork` root.

---
"@effect-agent/workflow": minor
"@effect-agent/platform-node": minor
"@effect-agent/thread": minor
---

Run existing registered agents through an application-supplied Effect Workflow engine with durable dispatch repair. Add SQLite dispatch storage, a scoped Node repair trigger, and bounded durable processing with attempt-scoped ownership release.

BEHAVIOR CHANGE: Replace `NodeDurableRuntime` and its `Options`, `Config`, `ConfigValue`, `Services`, and `InitializationError` exports with the corresponding `NodeDurableAgentRuntime` names.

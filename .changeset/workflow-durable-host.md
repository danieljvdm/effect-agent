---
"@yielded/agent-workflow": minor
"@yielded/agent-platform-node": minor
"@yielded/agent-platform-cloudflare": minor
"@yielded/agent-testing": minor
"@yielded/agent": minor
---

Run existing registered agents through an application-supplied Effect Workflow engine with durable dispatch repair. Add SQLite dispatch storage, a scoped Node repair trigger, and bounded durable processing with attempt-scoped ownership release.

BEHAVIOR CHANGE: Replace `NodeDurableRuntime` and its `Options`, `Config`, `ConfigValue`, `Services`, and `InitializationError` exports with the corresponding `NodeDurableAgentRuntime` names.

BEHAVIOR CHANGE: Register agents with `DurableAgentRuntime.layerRegistered` or `NodeDurableAgentRuntime.layerRegistered`, and use `layerWithBindings` for precompiled bindings. Call `processThreadResolved(threadId)` and run the `runResolvedWorker` Effect without binding arguments; use `NodeDurableHost.layer` and `ThreadMaintenance.layer` as Layer values over an already-assembled runtime.

BEHAVIOR CHANGE: Supply `WakeScheduler`, `ToolReconciler`, and `DurableRuntimeFailpoint` when calling `runChaosPlan`, which constructs registered runtimes for its delegation fixtures.

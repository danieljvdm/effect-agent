# @effect-agent/workflow

## 0.1.0-beta.45

### Minor Changes

- [#309](https://github.com/danieljvdm/effect-agent/pull/309) [`c8812c2`](https://github.com/danieljvdm/effect-agent/commit/c8812c221004bfbeded7a56a03f13102e282f4e0) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Run existing registered agents through an application-supplied Effect Workflow engine with durable dispatch repair. Add SQLite dispatch storage, a scoped Node repair trigger, and bounded durable processing with attempt-scoped ownership release.

  BEHAVIOR CHANGE: Replace `NodeDurableRuntime` and its `Options`, `Config`, `ConfigValue`, `Services`, and `InitializationError` exports with the corresponding `NodeDurableAgentRuntime` names.

  BEHAVIOR CHANGE: Register agents with `DurableAgentRuntime.layerRegistered` or `NodeDurableAgentRuntime.layerRegistered`, and use `layerWithBindings` for precompiled bindings. Call `processThreadResolved(threadId)` and run the `runResolvedWorker` Effect without binding arguments; use `NodeDurableHost.layer` and `ThreadMaintenance.layer` as Layer values over an already-assembled runtime.

  BEHAVIOR CHANGE: Supply `WakeScheduler`, `ToolReconciler`, and `DurableRuntimeFailpoint` when calling `runChaosPlan`, which constructs registered runtimes for its delegation fixtures.

### Patch Changes

- Updated dependencies [[`c8812c2`](https://github.com/danieljvdm/effect-agent/commit/c8812c221004bfbeded7a56a03f13102e282f4e0)]:
  - @effect-agent/thread@0.1.0-beta.45
  - @effect-agent/core@0.1.0-beta.45

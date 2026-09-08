# @effect-agent/workflow

## 0.1.0-beta.58

### Patch Changes

- Updated dependencies [[`daca525`](https://github.com/danieljvdm/effect-agent/commit/daca52585983bb90b6c43a29e4a44a28c8de1743)]:
  - @effect-agent/thread@0.1.0-beta.58
  - @effect-agent/core@0.1.0-beta.58

## 0.1.0-beta.57

### Patch Changes

- Updated dependencies [[`4ff21e2`](https://github.com/danieljvdm/effect-agent/commit/4ff21e2a4c3735be34955e7d5caf64f623d33f81), [`4ff21e2`](https://github.com/danieljvdm/effect-agent/commit/4ff21e2a4c3735be34955e7d5caf64f623d33f81)]:
  - @effect-agent/core@0.1.0-beta.57
  - @effect-agent/thread@0.1.0-beta.57

## 0.1.0-beta.56

### Patch Changes

- Updated dependencies []:
  - @effect-agent/thread@0.1.0-beta.56
  - @effect-agent/core@0.1.0-beta.56

## 0.1.0-beta.55

### Patch Changes

- Updated dependencies [[`2259fc0`](https://github.com/danieljvdm/effect-agent/commit/2259fc05eec3bfac2a92a8d055953f3482e54735)]:
  - @effect-agent/thread@0.1.0-beta.55
  - @effect-agent/core@0.1.0-beta.55

## 0.1.0-beta.54

### Patch Changes

- Updated dependencies [[`e9c7591`](https://github.com/danieljvdm/effect-agent/commit/e9c75913024f09b74cdbb1bd4f25b15c87acf06e), [`e9c7591`](https://github.com/danieljvdm/effect-agent/commit/e9c75913024f09b74cdbb1bd4f25b15c87acf06e)]:
  - @effect-agent/core@0.1.0-beta.54
  - @effect-agent/thread@0.1.0-beta.54

## 0.1.0-beta.53

### Patch Changes

- Updated dependencies [[`d93903e`](https://github.com/danieljvdm/effect-agent/commit/d93903ec923da7a9841b5ab1a72bba5c0a0fb34b), [`6b4839f`](https://github.com/danieljvdm/effect-agent/commit/6b4839f6ab14adcf82c72159152ab5fe2a946f97)]:
  - @effect-agent/thread@0.1.0-beta.53
  - @effect-agent/core@0.1.0-beta.53

## 0.1.0-beta.52

### Patch Changes

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.52
  - @effect-agent/thread@0.1.0-beta.52

## 0.1.0-beta.51

### Patch Changes

- [#341](https://github.com/danieljvdm/effect-agent/pull/341) [`75898ae`](https://github.com/danieljvdm/effect-agent/commit/75898aef60b09945d90bfe5674b5153edb0717eb) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add revisioned subscription management, bounded event retention, and explicit recovery of parked admissions. Fence fresh destination admission by host policy and retain one unsettled submission per optional admission group until canonical settlement.

  BEHAVIOR CHANGE: Reset incompatible development storage and update custom stores for required configuration revisions and retry generations.

- Updated dependencies [[`75898ae`](https://github.com/danieljvdm/effect-agent/commit/75898aef60b09945d90bfe5674b5153edb0717eb)]:
  - @effect-agent/thread@0.1.0-beta.51
  - @effect-agent/core@0.1.0-beta.51

## 0.1.0-beta.50

### Patch Changes

- Updated dependencies [[`207e910`](https://github.com/danieljvdm/effect-agent/commit/207e9108d25f25a204324b0f6217fea89de569cf), [`0438a7b`](https://github.com/danieljvdm/effect-agent/commit/0438a7b9c58869a91870d3df44dc163ec790a929), [`207e910`](https://github.com/danieljvdm/effect-agent/commit/207e9108d25f25a204324b0f6217fea89de569cf)]:
  - @effect-agent/thread@0.1.0-beta.50
  - @effect-agent/core@0.1.0-beta.50

## 0.1.0-beta.49

### Patch Changes

- Updated dependencies [[`b285e5b`](https://github.com/danieljvdm/effect-agent/commit/b285e5b06a52ac7fc4e3c7fc0ff232650e33857f), [`b285e5b`](https://github.com/danieljvdm/effect-agent/commit/b285e5b06a52ac7fc4e3c7fc0ff232650e33857f), [`b285e5b`](https://github.com/danieljvdm/effect-agent/commit/b285e5b06a52ac7fc4e3c7fc0ff232650e33857f)]:
  - @effect-agent/thread@0.1.0-beta.49
  - @effect-agent/core@0.1.0-beta.49

## 0.1.0-beta.48

### Patch Changes

- Updated dependencies [[`e640747`](https://github.com/danieljvdm/effect-agent/commit/e6407479ae233527685928bead040dbfe5153a22), [`8899bdb`](https://github.com/danieljvdm/effect-agent/commit/8899bdbcbbd16c5b7f9981564939f64729b73015)]:
  - @effect-agent/thread@0.1.0-beta.48
  - @effect-agent/core@0.1.0-beta.48

## 0.1.0-beta.47

### Patch Changes

- Updated dependencies [[`e6ff3bc`](https://github.com/danieljvdm/effect-agent/commit/e6ff3bcd1b5ce0f2348de668853482ba9d5e126b)]:
  - @effect-agent/core@0.1.0-beta.47
  - @effect-agent/thread@0.1.0-beta.47

## 0.1.0-beta.46

### Minor Changes

- [#313](https://github.com/danieljvdm/effect-agent/pull/313) [`c1a6e6a`](https://github.com/danieljvdm/effect-agent/commit/c1a6e6a915be73a49b2c266e2df74256f44c25e2) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Import module namespaces from package roots, or import declarations from their explicit PascalCase module paths, following the package map's migration examples. Discard unused modules from audited packages when bundling consumers.
  BEHAVIOR CHANGE: Replace flat declaration imports, lowercase aggregate paths, cross-package aliases, and internal helper imports with their documented owning modules; use `MemoryThreadStoreLive` instead of `MemoryStorageLive`.

- [#315](https://github.com/danieljvdm/effect-agent/pull/315) [`cebe728`](https://github.com/danieljvdm/effect-agent/commit/cebe728685cf9f45c1d9579273222a865bb8109d) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Run registered agents inside native Effect Workflow handlers with `AgentWorkflow.execute` and suspend until their typed results are available. **BEHAVIOR CHANGE:** supply `principal` to `WorkflowAgentHost.layer`; custom dispatch stores must return the retained intent from `put`, preserve and atomically attach its completion token, and compare the full intent before removal.

### Patch Changes

- Updated dependencies [[`c1a6e6a`](https://github.com/danieljvdm/effect-agent/commit/c1a6e6a915be73a49b2c266e2df74256f44c25e2), [`cebe728`](https://github.com/danieljvdm/effect-agent/commit/cebe728685cf9f45c1d9579273222a865bb8109d)]:
  - @effect-agent/core@0.1.0-beta.46
  - @effect-agent/thread@0.1.0-beta.46

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

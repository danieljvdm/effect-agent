# @effect-agent/engine

## 0.1.0-beta.47

### Patch Changes

- Updated dependencies [[`e6ff3bc`](https://github.com/danieljvdm/effect-agent/commit/e6ff3bcd1b5ce0f2348de668853482ba9d5e126b)]:
  - @effect-agent/core@0.1.0-beta.47

## 0.1.0-beta.46

### Minor Changes

- [#313](https://github.com/danieljvdm/effect-agent/pull/313) [`c1a6e6a`](https://github.com/danieljvdm/effect-agent/commit/c1a6e6a915be73a49b2c266e2df74256f44c25e2) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Import module namespaces from package roots, or import declarations from their explicit PascalCase module paths, following the package map's migration examples. Discard unused modules from audited packages when bundling consumers.
  BEHAVIOR CHANGE: Replace flat declaration imports, lowercase aggregate paths, cross-package aliases, and internal helper imports with their documented owning modules; use `MemoryThreadStoreLive` instead of `MemoryStorageLive`.

### Patch Changes

- Updated dependencies [[`c1a6e6a`](https://github.com/danieljvdm/effect-agent/commit/c1a6e6a915be73a49b2c266e2df74256f44c25e2)]:
  - @effect-agent/core@0.1.0-beta.46

## 0.1.0-beta.45

### Patch Changes

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.45

## 0.1.0-beta.44

### Patch Changes

- [#307](https://github.com/danieljvdm/effect-agent/pull/307) [`f8365ee`](https://github.com/danieljvdm/effect-agent/commit/f8365eee4048076ced0a79b9149efc29297b7c41) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Upgrade to Effect rc.112 and `effect-cf` 0.40.0 while preserving MCP transports and Cloudflare host behavior.

  BEHAVIOR CHANGE: Upgrade Effect and its provider/platform/SQL packages to rc.112 or a compatible version. In Cloudflare hosts, provide `effect-cf@^0.40.0` and enable `nodejs_compat` for its async context support.

- Updated dependencies [[`f8365ee`](https://github.com/danieljvdm/effect-agent/commit/f8365eee4048076ced0a79b9149efc29297b7c41)]:
  - @effect-agent/core@0.1.0-beta.44

## 0.1.0-beta.43

### Patch Changes

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.43

## 0.1.0-beta.42

### Patch Changes

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.42

## 0.1.0-beta.41

### Minor Changes

- [#281](https://github.com/danieljvdm/effect-agent/pull/281) [`edfa7dc`](https://github.com/danieljvdm/effect-agent/commit/edfa7dc6693dea2a84366f5053826ffa87f7c587) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Derive delegation schemas and mappings from child definitions, inherit omitted policy defaults within shared reservation limits, and accept model Layers directly for subagent execution and durable registration.

  BEHAVIOR CHANGE: Durable delegations enforce shared reservation caps; configure identical `parentCaps` when multiple delegation policies share a parent Run.

### Patch Changes

- Updated dependencies [[`e21d6da`](https://github.com/danieljvdm/effect-agent/commit/e21d6da596b97c98ace533c3fa42fe9767d127e1), [`edfa7dc`](https://github.com/danieljvdm/effect-agent/commit/edfa7dc6693dea2a84366f5053826ffa87f7c587)]:
  - @effect-agent/core@0.1.0-beta.41

## 0.1.0-beta.40

### Minor Changes

- [#269](https://github.com/danieljvdm/effect-agent/pull/269) [`018f1ad`](https://github.com/danieljvdm/effect-agent/commit/018f1ad8455a0075b9cf764f85fe9b6972f07eb7) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Provide optional context loading through `RunContextPreparation` for ephemeral and durable runs, and catch its concrete tagged errors directly.

  BEHAVIOR CHANGE: replace `RunContextPreparationError.make` with a declared `AgentInputError`, `MemoryRecallError`, or `CompactionError`.

- [#269](https://github.com/danieljvdm/effect-agent/pull/269) [`018f1ad`](https://github.com/danieljvdm/effect-agent/commit/018f1ad8455a0075b9cf764f85fe9b6972f07eb7) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Compose bounded, attributed Markdown or external corpus passages as optional transient model context. Keep recalled text outside Thread history and compaction coverage, and reload it when durable execution resumes.

### Patch Changes

- [#269](https://github.com/danieljvdm/effect-agent/pull/269) [`018f1ad`](https://github.com/danieljvdm/effect-agent/commit/018f1ad8455a0075b9cf764f85fe9b6972f07eb7) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Deduplicate equivalent recalled JSON metadata regardless of object member order. Load transient references after initial canonical compaction, then compact further when needed to fit the complete prompt.

- Updated dependencies [[`018f1ad`](https://github.com/danieljvdm/effect-agent/commit/018f1ad8455a0075b9cf764f85fe9b6972f07eb7), [`1432833`](https://github.com/danieljvdm/effect-agent/commit/14328336cd3480c5ddda8447f522591eb99eaaeb), [`c36fe73`](https://github.com/danieljvdm/effect-agent/commit/c36fe73d2d226f9271c6dd60071159b0d82862ae), [`018f1ad`](https://github.com/danieljvdm/effect-agent/commit/018f1ad8455a0075b9cf764f85fe9b6972f07eb7), [`0fbcbbf`](https://github.com/danieljvdm/effect-agent/commit/0fbcbbf3c8c2ca7595543e545baddb0c6f965436)]:
  - @effect-agent/core@0.1.0-beta.40

## 0.1.0-beta.39

### Minor Changes

- [#251](https://github.com/danieljvdm/effect-agent/pull/251) [`7bab6c0`](https://github.com/danieljvdm/effect-agent/commit/7bab6c053b01398a0f1898374103997da6550268) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add Effectful `inputPrompt` projections to Agent Definitions so hosts can omit input fields from model requests while retaining canonical input for authorization and recovery.

- [#252](https://github.com/danieljvdm/effect-agent/pull/252) [`0d88d90`](https://github.com/danieljvdm/effect-agent/commit/0d88d90443e7d35e34799f4458d274fde99e0859) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Provide canonical thread history to `AgentRuntime.run`, `start`, and `stream` through `PersistentHistory.layer` without admitting durable work. Make checkpoint storage an optional `ThreadStore.checkpoints` capability.

  BEHAVIOR CHANGE: Provide `ThreadHistory.layerTransient` for transient execution or `PersistentHistory.layer` with a ThreadStore for retained history. Use `store.checkpoints.save` and `store.checkpoints.load` after checking capability availability. `UserInputRecorded.submissionId` is present only for durably accepted input.

- [#263](https://github.com/danieljvdm/effect-agent/pull/263) [`95865d7`](https://github.com/danieljvdm/effect-agent/commit/95865d78f55546d42f562f2f13509bbfc198c091) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Rename `@effect-agent/session` to `@effect-agent/thread` and rename the Conversation framework API to Thread.

  BEHAVIOR CHANGE: Rename Conversation identifiers, fields, record families and tags, and the durable-admin `--conversation` selector to their Thread equivalents. Reset incompatible alpha storage before upgrading.

- [#242](https://github.com/danieljvdm/effect-agent/pull/242) [`655bf5f`](https://github.com/danieljvdm/effect-agent/commit/655bf5f217dce1865c97ce613246c27846bfaf8a) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Replace compaction through `ContextCompactor` Layers, with a bounded default and configurable summary Model. Preserve metered usage, protected Tool pairs, and canonical coverage of the selected history.

  BEHAVIOR CHANGE: migrate capabilities `ContextCompactor.compact(snapshot)` implementations to the engine request/decision contract; `contextCompactorRunContextLayer` now installs native compaction instead of a prompt hook.

  BEHAVIOR CHANGE: durable compaction fails if its coverage cannot map to complete prior-Run records or its summary exceeds 65,536 characters. Empty or incomplete model summaries and whitespace-only custom summaries are rejected.

  BEHAVIOR CHANGE: pressure compaction and overflow recovery share one prune, one summary, and one summary-model call per Turn.

- [#257](https://github.com/danieljvdm/effect-agent/pull/257) [`511c852`](https://github.com/danieljvdm/effect-agent/commit/511c85212a564ff2729de401620fcbdeddcb4748) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Construct agents with `Agent.make` and execute Definitions with native model Layers through `Effect.provide`. Accept Schema-encoded inputs in `run`, `stream`, and `start`, and decode external data through `runUnknown`, `streamUnknown`, and `startUnknown`.

  BEHAVIOR CHANGE: Replace `Agent.define` with `Agent.make`; move inputs typed as `unknown` to the explicit unknown-input operations.

### Patch Changes

- [#262](https://github.com/danieljvdm/effect-agent/pull/262) [`34ca82e`](https://github.com/danieljvdm/effect-agent/commit/34ca82e86191bc85229bd32886b8cfaf9a2edce9) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Enforce durable child tool-call allowances across recovery and distinguish passing checks from complete adapter certification. Rename the custom durable assembly to `layerWithServices` and preserve Node extension-layer construction errors and dependencies.

  BEHAVIOR CHANGE: Replace `DurableAgentRuntime.layerWithContext` with `layerWithServices`, still supplying both separate services. Regenerate certification reports with the `effect-agent/certification@2` schema and use `fullyCertified` for gates requiring executed real-loss checks; `ok` retains its executed-check meaning. Existing child records without an allowance keep their original definition policy; start a new delegation to apply a limit.

- [#243](https://github.com/danieljvdm/effect-agent/pull/243) [`e0aa7d9`](https://github.com/danieljvdm/effect-agent/commit/e0aa7d9442ca2ec62df8195a2f9cce7b52af5257) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Preserve Run policy allowances across replacement Attempts and reserve programmatic calls and grace finalization before execution. Reject unusable compaction summaries while retaining reported usage and the previous summary.

  BEHAVIOR CHANGE: Supply complete, consistent `resumeUsage` whenever a custom coordinator passes `resume` to the runtime.

- [#248](https://github.com/danieljvdm/effect-agent/pull/248) [`f4f37c3`](https://github.com/danieljvdm/effect-agent/commit/f4f37c37fa1b650341c6e18ee3a22cd6f518bfd2) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Compose prompt preparation and Tool authorization independently in durable hosts, preserving both across recovery.

  BEHAVIOR CHANGE: move `RunContextPreparation.toolAuthorization` to a separate `RunToolAuthorization` Layer and provide both services to `DurableAgentRuntime.layerWithServices`.

- [#243](https://github.com/danieljvdm/effect-agent/pull/243) [`e0aa7d9`](https://github.com/danieljvdm/effect-agent/commit/e0aa7d9442ca2ec62df8195a2f9cce7b52af5257) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Preserve Run limits across durable recovery, require explicit delegation replay authority, and reject unusable compaction summaries. Authorize settlement waits and aborts through the runtime authorizer and reject settlement Receipts whose Submission belongs to another Thread.

  BEHAVIOR CHANGE: Reset private-development histories whose RunStarted records predate policy accounting version 1 before resuming them.

- [#259](https://github.com/danieljvdm/effect-agent/pull/259) [`79fbd8b`](https://github.com/danieljvdm/effect-agent/commit/79fbd8b755434a162629a534478e188636d186fe) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Let host spending admission replace the reviewer's cumulative token quota and retain usage diagnostics when an accounted attempt fails before recording findings. Preserve tool definitions when selecting a required completion tool.

- [#258](https://github.com/danieljvdm/effect-agent/pull/258) [`4c458e4`](https://github.com/danieljvdm/effect-agent/commit/4c458e43738bb243d1e343c97ecfd49e3b41ca9f) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Run self-contained Agents without an extra caller Scope. Preserve caller-supplied scoped requirements and application Layer lifetimes.

- [#250](https://github.com/danieljvdm/effect-agent/pull/250) [`d004a36`](https://github.com/danieljvdm/effect-agent/commit/d004a361518c23cdc81f1768e5ab31560e014935) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Retain recorded review findings when research stops, mark partial results with `incomplete` or `exhausted`, and admit review Action requests only below $1. Permit completion on the single grace turn and reuse stable OpenAI prompt prefixes.

- [#256](https://github.com/danieljvdm/effect-agent/pull/256) [`ac70e21`](https://github.com/danieljvdm/effect-agent/commit/ac70e212c7d9741ce48bd9b2a4dbd355f9dac72e) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Declare `effect` as a required `^4.0.0-rc.111` peer across all public packages so they share the application's runtime and accept compatible upgrades. Keep `effect` in application dependencies at a version satisfying the framework's and providers' peer ranges.

- Updated dependencies [[`e0aa7d9`](https://github.com/danieljvdm/effect-agent/commit/e0aa7d9442ca2ec62df8195a2f9cce7b52af5257), [`7bab6c0`](https://github.com/danieljvdm/effect-agent/commit/7bab6c053b01398a0f1898374103997da6550268), [`95865d7`](https://github.com/danieljvdm/effect-agent/commit/95865d78f55546d42f562f2f13509bbfc198c091), [`ac70e21`](https://github.com/danieljvdm/effect-agent/commit/ac70e212c7d9741ce48bd9b2a4dbd355f9dac72e), [`511c852`](https://github.com/danieljvdm/effect-agent/commit/511c85212a564ff2729de401620fcbdeddcb4748)]:
  - @effect-agent/core@0.1.0-beta.39

## 0.1.0-beta.38

### Patch Changes

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.38

## 0.1.0-beta.37

### Patch Changes

- [#199](https://github.com/danieljvdm/effect-agent/pull/199) [`bd48a7b`](https://github.com/danieljvdm/effect-agent/commit/bd48a7b200fb71335b19edd7941be331b6ede9ea) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Preserve single-turn provider-hosted lookups for agents without required completion. Require the completion handler after provider-only responses exhaust the token budget.

- [#199](https://github.com/danieljvdm/effect-agent/pull/199) [`bd48a7b`](https://github.com/danieljvdm/effect-agent/commit/bd48a7b200fb71335b19edd7941be331b6ede9ea) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Allow agents to require a native completion Tool, rejecting ordinary final text instead of parsing it as structured output. Permit a sole completion Tool on the final allowed turn when no further model call is needed.

- Updated dependencies [[`bd48a7b`](https://github.com/danieljvdm/effect-agent/commit/bd48a7b200fb71335b19edd7941be331b6ede9ea)]:
  - @effect-agent/core@0.1.0-beta.37

## 0.1.0-beta.36

### Patch Changes

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.36

## 0.1.0-beta.35

### Patch Changes

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.35

## 0.1.0-beta.34

### Minor Changes

- [#206](https://github.com/danieljvdm/effect-agent/pull/206) [`aa3ebfb`](https://github.com/danieljvdm/effect-agent/commit/aa3ebfb4fd1e69be77c433a881ddecb3567c36c2) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Expose non-propagating Tool failures to an opt-in trusted local observer, preserving live Causes without automatic export. Install the same observer through durable Node and Cloudflare runtime options while excluding settled-call replay.

### Patch Changes

- [#202](https://github.com/danieljvdm/effect-agent/pull/202) [`cf4a8d9`](https://github.com/danieljvdm/effect-agent/commit/cf4a8d9c645d5d8a2e552f4bb4902af4253d91ee) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Preserve each durable Run's original start and duration across recovery, report truthful elapsed time, and complete verified child cleanup without allowing execution after expiry. Reject incompatible execution history without canonical start evidence; reset affected private-development data before resuming it.

- [#205](https://github.com/danieljvdm/effect-agent/pull/205) [`baecd08`](https://github.com/danieljvdm/effect-agent/commit/baecd08f1d6f2c0698e16487cdcccf2f6ffcebca) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Keep completion Tool arguments governed by their own Schema instead of the private Agent output contract. Retain JSON final-text completion when the designated completion Tool is not called.

- [#202](https://github.com/danieljvdm/effect-agent/pull/202) [`cf4a8d9`](https://github.com/danieljvdm/effect-agent/commit/cf4a8d9c645d5d8a2e552f4bb4902af4253d91ee) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Align the Effect family with rc.111 to decode nested OpenAI error events, and preserve transformed Tool parameters under its encoded response contract.

- [#205](https://github.com/danieljvdm/effect-agent/pull/205) [`baecd08`](https://github.com/danieljvdm/effect-agent/commit/baecd08f1d6f2c0698e16487cdcccf2f6ffcebca) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add opt-in native Conversation RPC tracing with binding/method client spans, transient current-span propagation, and typed receiver invocation hooks. Remove routine storage codec, failpoint-wrapper, and engine identifier-helper spans while preserving validation, failures, and I/O tracing.

  BEHAVIOR CHANGE: Upgrade the host's `effect-cf` dependency to `^0.34.0` for the native tracing contract.

- Updated dependencies [[`cf4a8d9`](https://github.com/danieljvdm/effect-agent/commit/cf4a8d9c645d5d8a2e552f4bb4902af4253d91ee)]:
  - @effect-agent/core@0.1.0-beta.34

## 0.1.0-beta.33

### Patch Changes

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.33

## 0.1.0-beta.32

### Patch Changes

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.32

## 0.1.0-beta.31

### Patch Changes

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.31

## 0.1.0-beta.30

### Patch Changes

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.30

## 0.1.0-beta.29

### Patch Changes

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.29

## 0.1.0-beta.28

### Patch Changes

- [#159](https://github.com/danieljvdm/effect-agent/pull/159) [`374771d`](https://github.com/danieljvdm/effect-agent/commit/374771d90afa26ce7e1832f76715aa7b9eea3741) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Normalize provider cache-write usage when the provider also includes those tokens in uncached
  input. Preserve additive canonical usage totals without rejecting valid model responses.
- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.28

## 0.1.0-beta.27

### Minor Changes

- [#155](https://github.com/danieljvdm/effect-agent/pull/155) [`773264b`](https://github.com/danieljvdm/effect-agent/commit/773264b75759c4456e1e549d2172bbe39610a8c1) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add crash-safe terminal delivery Tools and final model responses, completion-capacity reservation, Run-scoped prompt provenance, and target-aware compaction.
  Persist priced per-call model usage in the DN and DC assemblies and expose aggregate usage on Run settlements.

### Patch Changes

- Updated dependencies [[`773264b`](https://github.com/danieljvdm/effect-agent/commit/773264b75759c4456e1e549d2172bbe39610a8c1)]:
  - @effect-agent/core@0.1.0-beta.27

## 0.1.0-beta.26

### Patch Changes

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.26

## 0.1.0-beta.25

### Minor Changes

- [#142](https://github.com/danieljvdm/effect-agent/pull/142) [`b6804dd`](https://github.com/danieljvdm/effect-agent/commit/b6804dd60cc83b569d0e87b88521952c20ba9b7d) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Bound model responses, Run event replay, Subagent event bursts, diagnostics, and programmatic Tool
  results with engine-owned limits. BEHAVIOR CHANGE: `ToolBroker.openPass` now requires an explicit
  positive `maxResultBytes`.

- [#142](https://github.com/danieljvdm/effect-agent/pull/142) [`b6804dd`](https://github.com/danieljvdm/effect-agent/commit/b6804dd60cc83b569d0e87b88521952c20ba9b7d) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Reject malformed DN/DC resume Tool results and usage seeds before external execution, and preserve
  engine infrastructure causes on live errors without exposing them in public diagnostics. BEHAVIOR
  CHANGE: pass a positive finite `maxResultBytes` to `ToolBroker.openPass`; `AgentRuntime.layer` is
  removed.

### Patch Changes

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.25

## 0.1.0-beta.24

### Patch Changes

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.24

## 0.1.0-beta.23

### Patch Changes

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.23

## 0.1.0-beta.22

### Minor Changes

- [#124](https://github.com/danieljvdm/effect-agent/pull/124) [`ce8b39c`](https://github.com/danieljvdm/effect-agent/commit/ce8b39ce8f716c0a11c6394d136b67cb9be84588) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Authorize every still-executable model-declared call in a fresh or resumed application Tool batch
  through a host-supplied Run option before durable preparation or Handler execution. Settle denied
  accepted work with a typed failure while preserving canonical Run, Turn, input, and Tool Call
  identity across recovery.

### Patch Changes

- Updated dependencies [[`ce8b39c`](https://github.com/danieljvdm/effect-agent/commit/ce8b39ce8f716c0a11c6394d136b67cb9be84588)]:
  - @effect-agent/core@0.1.0-beta.22

## 0.1.0-beta.21

### Patch Changes

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.21

## 0.1.0-beta.20

### Patch Changes

- [#111](https://github.com/danieljvdm/effect-agent/pull/111) [`c715f9f`](https://github.com/danieljvdm/effect-agent/commit/c715f9f8e436fa85e8c1ef2b27f640e637ea52e4) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Align every public package with the Effect 4.0.0-rc.110 family.

- [#111](https://github.com/danieljvdm/effect-agent/pull/111) [`c715f9f`](https://github.com/danieljvdm/effect-agent/commit/c715f9f8e436fa85e8c1ef2b27f640e637ea52e4) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Fix `validateMcpDiscovery` reporting a permanent schema drift for MCP tools whose parameters or success type is a named, refined Schema (a branded ID, a bounded string, a `Schema.Class`) — both schema derivations now resolve a top-level `$ref` before comparison.

- Updated dependencies [[`c715f9f`](https://github.com/danieljvdm/effect-agent/commit/c715f9f8e436fa85e8c1ef2b27f640e637ea52e4), [`c715f9f`](https://github.com/danieljvdm/effect-agent/commit/c715f9f8e436fa85e8c1ef2b27f640e637ea52e4)]:
  - @effect-agent/core@0.1.0-beta.20

## 0.1.0-beta.19

### Minor Changes

- [#106](https://github.com/danieljvdm/effect-agent/pull/106) [`9e31de4`](https://github.com/danieljvdm/effect-agent/commit/9e31de4c5f63ebc7eefbce33d3e0ed2052538f26) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Expose host-supplied model-context preparation through Cloudflare Conversation Object options
  ([#49](https://github.com/danieljvdm/effect-agent/issues/49)). A generic scoped `RunContextPreparation` service now composes after canonical durable
  resume reconstruction, `contextCompactorRunContextLayer` adapts the digest-bound
  `ContextCompactor` capability with typed failures, and `CloudflareDurableRuntimeOptions.runContext`
  accepts a closed Layer or per-incarnation Layer factory. Compaction changes only model-visible
  context; canonical history remains recoverable across Durable Object eviction and retries.

### Patch Changes

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.19

## 0.1.0-beta.18

### Patch Changes

- [#104](https://github.com/danieljvdm/effect-agent/pull/104) [`f36fd40`](https://github.com/danieljvdm/effect-agent/commit/f36fd409f8a34e13c87646fd857a4060ac89e89d) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Preserve one wall-clock `maxDuration` deadline across durable Attempts. The coordinator now
  derives the logical Run deadline from its first canonical input record, so recovery and
  `waitingForChild` suspension cannot reset the parent allowance; queue time remains excluded and
  the engine's deadline option is tightening-only. Already-settled child joins remain mandatory
  recovery cleanup before an expired parent records its typed duration failure; cleanup authority
  names the exact open delegation Calls and duration interruption resumes before continuation.
  Adapter certification fixtures now keep their Run duration above the deliberate multi-round
  virtual lease-expiry horizon instead of relying on replacement Attempts to reset it.
- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.18

## 0.1.0-beta.17

### Minor Changes

- [#101](https://github.com/danieljvdm/effect-agent/pull/101) [`016df57`](https://github.com/danieljvdm/effect-agent/commit/016df574fa8c0f362468d848ae830d72532cbcaf) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add a Definition-owned Schema boundary for typed application run dispositions and persist valid
  ordinary-completion values on canonical `SubmissionSettled` records across crash recovery and
  replay, including materialization through the public durable Settlement API.

### Patch Changes

- Updated dependencies [[`016df57`](https://github.com/danieljvdm/effect-agent/commit/016df574fa8c0f362468d848ae830d72532cbcaf)]:
  - @effect-agent/core@0.1.0-beta.17

## 0.1.0-beta.16

### Patch Changes

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.16

## 0.1.0-beta.15

### Patch Changes

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.15

## 0.1.0-beta.14

### Patch Changes

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.14

## 0.1.0-beta.13

### Patch Changes

- Updated dependencies [[`68b48c9`](https://github.com/danieljvdm/effect-agent/commit/68b48c932b6a76d2c8ed0f04cc87c123a9fd11e4)]:
  - @effect-agent/core@0.1.0-beta.13

## 0.1.0-beta.12

### Patch Changes

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.12

## 0.1.0-beta.11

### Patch Changes

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.11

## 0.1.0-beta.10

### Patch Changes

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.10

## 0.1.0-beta.9

### Patch Changes

- [#66](https://github.com/danieljvdm/effect-agent/pull/66) [`91ff50d`](https://github.com/danieljvdm/effect-agent/commit/91ff50df5480a0ccdfb8e0a00db39a1576e6c34b) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Harden context economics per the reviewer's second pass: the cost budget is
  enforced even when a token breach soft-lands the same response; tool results
  that cannot serialize become a bounded `UnserializableToolResult` sentinel
  instead of passing through unbounded; recovery re-seeds spend and derives
  token-exhaustion state (fail mode rejects an already-over-budget resume before
  any model call); compaction summarizer usage is staged into the canonical
  turn record; staged usage is validated as non-negative finite integers; and a
  provider-only breaching stop settles honestly as budget-exhausted.
- Updated dependencies [[`91ff50d`](https://github.com/danieljvdm/effect-agent/commit/91ff50df5480a0ccdfb8e0a00db39a1576e6c34b)]:
  - @effect-agent/core@0.1.0-beta.9

## 0.1.0-beta.8

### Patch Changes

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.8

## 0.1.0-beta.7

### Minor Changes

- [#56](https://github.com/danieljvdm/effect-agent/pull/56) [`5c49b78`](https://github.com/danieljvdm/effect-agent/commit/5c49b786604b3e8389cdc2c54d4f5cb284eac2b7) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Budget extension (D-037, ADR-0019 S3, RUN-021/SUB-034): `RunOptions` gains tightening-only
  `toolCallAllowance` and `turnAllowance` — the effective limit is
  `min(policy bound, max(1, floor(allowance)))`, never wider, and the `onExhaustion` soft landing
  keys off the effective limits. `Subagent.define` gains
  `toolCallAllowance: { default, fromParameters }`, clamped fail-closed to the delegation's
  per-invocation `SubagentPolicy.maxToolCalls` slice and threaded into ephemeral child runs, so an
  orchestrator model grants a scout more budget by re-delegating with a raised allowance (fresh
  child; never a mid-flight top-up). `projectResult` now receives a bounded
  `SubagentResultContext` whose `budgetExhausted` marker is honest on both paths — from the
  ephemeral child result's `finishReason`, or from the child Settlement's durable marker carried
  through the new optional `ChildEstablishSettled.finishReason` (threaded by the session
  coordinator shared by the DN and DC assemblies; exercised in the DN-profile durable-subagent
  suites) — so a budget-truncated partial can be surfaced in the declared success Schema. Existing one-argument `projectResult` functions keep
  compiling unchanged. Also hardens S2 containment per its autoreviewer findings: `Subagent.define`
  is overloaded so the Tool channels follow the `failureMode` value; genuine engine signals are
  classified by unspoofable provenance instead of `instanceof` on exported classes; each delegation
  exposes its canonical `containedFailure` schema (pr-review's coverage decoder now derives from
  it); and the pr-review child reviewer deliberately returns to typed exhaustion — a review is a
  coverage claim, so a budget-exhausted unit stays honestly unreviewed (contained as result data,
  never run-fatal).

- [#54](https://github.com/danieljvdm/effect-agent/pull/54) [`afe755a`](https://github.com/danieljvdm/effect-agent/commit/afe755a331172ffca9ceee7dd82bb452c6ccbb8a) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Context economics ([#54](https://github.com/danieljvdm/effect-agent/issues/54), RUN-022–027/CAP-017): application tool results are bounded by default (50 KiB
  `TruncatedToolResult` envelopes), budget accounting becomes cache-aware with last-call
  live-context tracking, every request can carry a derived run-status message, the token
  dimension joins the `onExhaustion` soft landing (RUN-018) with the `exhausted` dimension marker,
  and the engine compacts natively at the pre-Turn seam (prune, then one metered summarize)
  with a canonical `CompactionCreated` record that projections fold across Runs; provider
  context-length rejections compact-and-retry once, then fail typed (`ContextOverflowError`).

- [#63](https://github.com/danieljvdm/effect-agent/pull/63) [`3a44b5f`](https://github.com/danieljvdm/effect-agent/commit/3a44b5f6595f4070abb61c79d5b756a9f7ed20af) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Model-visible output contract ([#41](https://github.com/danieljvdm/effect-agent/issues/41), [#55](https://github.com/danieljvdm/effect-agent/issues/55), RUN-028/TEST-016): every model request of a Run whose
  Agent Definition declares an output Schema now carries a framework-owned system message stating
  the final-output contract — a fixed directive plus the JSON Schema derived from the encoded side
  of `agent.definition.output` via Effect AI's derivation, inserted immediately after the request's
  last system message. The contract is a request-time projection applied after
  `RunContextHook.prepare`: official history, canonical records, run events, and the DN/DC golden
  are unchanged, and compaction cannot drop it. Context adapters receive the exact text through the
  new additive optional `RunContextRequest.outputContract` field so a limit-targeting hook can
  reserve its overhead; the field is absent entirely when the output Schema cannot render to JSON
  Schema, in which case the Run behaves exactly as before with one Turn-1 diagnostic per Attempt.
  `decodeFinalOutput` remains the conformance authority (AUTH-008). BEHAVIOR CHANGE ON UPGRADE:
  model-visible prompts grow by the rendered Schema on every request, and tests asserting
  request-message shapes will see one additional system message; hand-written JSON-shape prose in
  `instructions` becomes redundant but stays harmless.

### Patch Changes

- Updated dependencies [[`afe755a`](https://github.com/danieljvdm/effect-agent/commit/afe755a331172ffca9ceee7dd82bb452c6ccbb8a)]:
  - @effect-agent/core@0.1.0-beta.7

## 0.1.0-beta.6

### Minor Changes

- [#39](https://github.com/danieljvdm/effect-agent/pull/39) [`e13ee6e`](https://github.com/danieljvdm/effect-agent/commit/e13ee6e7817549e99837d06e86caf2dea8656aa8) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Budget soft landing (D-037, ADR-0019, RUN-018/019/020): `AgentPolicy` gains
  `onExhaustion: "final-answer" | "fail"`, defaulting to `"final-answer"` — Turn and Tool Call
  exhaustion now settle the Run through one constrained final-answer opportunity instead of failing
  it. An over-budget Tool batch settles synthetically as model-visible failed results (no handler
  starts, no durable batch declaration, exempt from repeated-failure folding), subsequent model
  requests carry `toolChoice: "none"`, Turn exhaustion admits exactly one grace Turn, and the Run
  completes with the honest `finishReason: "budget-exhausted"` on the live event, the reduced
  `AgentResult`, and (additively) the durable `SubmissionSettled` record. Duration, token, cost, and
  repeated-failure bounds stay hard rails; `onExhaustion: "fail"` preserves the prior run-fatal
  behavior exactly. BEHAVIOR CHANGE ON UPGRADE: Turn/Tool-Call budget deaths become honest
  completions unless a policy pins `"fail"` — `@effect-agent/pr-review` pins `"fail"` pending its
  containment rework.

### Patch Changes

- [#30](https://github.com/danieljvdm/effect-agent/pull/30) [`94c169a`](https://github.com/danieljvdm/effect-agent/commit/94c169a44a248972158ca955e33fb02dd5e55463) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Export privacy-safe canonical Tool spans and bounded terminal logs from the engine, including
  model-declared and programmatic broker calls, value-level failures, and delayed terminal event/
  trace commit, while isolating complete span-lifecycle defects through Effect's error reporter.
  Build Cloudflare Conversation Objects on `effect-cf`'s native `DurableObject.make` boundary so it
  owns the cached runtime, event-scoped Layers, native RPC methods, `waitUntil`, and post-RPC OTLP
  flush isolation. Upgrade to `effect-cf` 0.25.3 so the same upstream boundary flushes alarm
  telemetry. Remove Effect Agent's duplicate telemetry service, flush coordinator, timeout
  configuration, and lifecycle fixture matrix.
- Updated dependencies [[`e13ee6e`](https://github.com/danieljvdm/effect-agent/commit/e13ee6e7817549e99837d06e86caf2dea8656aa8)]:
  - @effect-agent/core@0.1.0-beta.6

## 0.0.1-beta.5

### Patch Changes

- [#19](https://github.com/danieljvdm/effect-agent/pull/19) [`a063031`](https://github.com/danieljvdm/effect-agent/commit/a063031c6b1f1637d947ae193a410b6bb9e8a9fc) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Align every public package with Effect 4.0.0-beta.107. Also expose per-incarnation Cloudflare
  Binding capture with live Durable Object context and derived identities, and prevent incomplete
  application Tool batches from a failed or aborted Run from poisoning prompts for later Runs.
- Updated dependencies [[`a063031`](https://github.com/danieljvdm/effect-agent/commit/a063031c6b1f1637d947ae193a410b6bb9e8a9fc)]:
  - @effect-agent/core@0.0.1-beta.5

## 0.0.1-beta.4

### Patch Changes

- [#13](https://github.com/danieljvdm/effect-agent/pull/13) [`f4e3786`](https://github.com/danieljvdm/effect-agent/commit/f4e378635a794d4c17192ee3de011697ccec3a3b) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Introduce the `effect-agent` umbrella package: the framework's complete pure
  surface — schema-first authoring (core), the bounded interpreter (engine),
  and operational capabilities — as one dependency-clean root package,
  mirroring how `effect` fronts the `@effect/*` satellites. Platform adapters
  remain scoped. The umbrella is version-fixed to its three constituents.
- Updated dependencies [[`f4e3786`](https://github.com/danieljvdm/effect-agent/commit/f4e378635a794d4c17192ee3de011697ccec3a3b)]:
  - @effect-agent/core@0.0.1-beta.4

## 0.0.1-beta.3

### Patch Changes

- Adopt the MIT license across every published package, and ship the Cloudflare
  packages with type declarations for the first time: their Durable Object
  class factory now carries an explicit `ConversationObjectClass` return type,
  which unblocks TypeScript declaration emit (TS4094). Supersedes the
  0.0.1-beta.2 round (and the Cloudflare pair's 0.0.1-beta.0), which was
  published out of band from an uncommitted tree, still UNLICENSED, and without
  `.d.mts` for the Cloudflare packages.
- Updated dependencies []:
  - @effect-agent/core@0.0.1-beta.3

## 0.0.1-beta.1

### Patch Changes

- Republish with correctly pinned internal dependencies. The 0.0.1-beta.0
  artifacts depended on internal `@effect-agent/*` versions that were never
  published (`workspace:*` ranges were resolved from a stale lockfile at
  publish time); the release script now pins internal ranges to the exact
  workspace versions itself.
- Updated dependencies []:
  - @effect-agent/core@0.0.1-beta.1

## 0.0.1-beta.0

### Patch Changes

- Initial beta-channel release of the Effect Agent framework packages for live
  integration testing: the schema-first authoring core, the ephemeral
  interpreter, operational capabilities, sandbox contracts and the local
  adapter, canonical session records with the durable coordinator, the memory
  and SQLite storage adapters, the Node platform assembly, and the
  deterministic testing kit. The Cloudflare packages stay private until their
  declaration-emit blocker (TS4094) is resolved.
- Updated dependencies []:
  - @effect-agent/core@0.0.1-beta.0

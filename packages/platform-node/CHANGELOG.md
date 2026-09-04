# @effect-agent/platform-node

## 0.1.0-beta.49

### Patch Changes

- [#327](https://github.com/danieljvdm/effect-agent/pull/327) [`b285e5b`](https://github.com/danieljvdm/effect-agent/commit/b285e5b06a52ac7fc4e3c7fc0ff232650e33857f) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Poll durable abort intent without loading unrelated recovery state. Implement `SubmissionLedger.readAbortIntent` in custom ledger adapters.

- Updated dependencies [[`91ac3bf`](https://github.com/danieljvdm/effect-agent/commit/91ac3bf8cabe1cd7d7851995a3fd714b02db58a0), [`b285e5b`](https://github.com/danieljvdm/effect-agent/commit/b285e5b06a52ac7fc4e3c7fc0ff232650e33857f), [`b285e5b`](https://github.com/danieljvdm/effect-agent/commit/b285e5b06a52ac7fc4e3c7fc0ff232650e33857f), [`b54eea8`](https://github.com/danieljvdm/effect-agent/commit/b54eea8ce9973a1ef2a58ddd6eb87bcc912bec75), [`e3024c0`](https://github.com/danieljvdm/effect-agent/commit/e3024c00673a12b0df79127bcf68176742c51294), [`b54eea8`](https://github.com/danieljvdm/effect-agent/commit/b54eea8ce9973a1ef2a58ddd6eb87bcc912bec75), [`b285e5b`](https://github.com/danieljvdm/effect-agent/commit/b285e5b06a52ac7fc4e3c7fc0ff232650e33857f)]:
  - @effect-agent/engine@0.1.0-beta.49
  - @effect-agent/thread@0.1.0-beta.49
  - @effect-agent/storage-sqlite@0.1.0-beta.49
  - @effect-agent/workflow@0.1.0-beta.49
  - @effect-agent/core@0.1.0-beta.49

## 0.1.0-beta.48

### Minor Changes

- [#317](https://github.com/danieljvdm/effect-agent/pull/317) [`671516a`](https://github.com/danieljvdm/effect-agent/commit/671516af0ecc66169af1fc81d5d0bf9242143c1e) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Start a scoped worker pool with `NodeDurableHost.layer(registrations, options)` and supervise it with `NodeDurableHost.run`. Close admission when the pool exits and stop workers before releasing runtime resources.

### Patch Changes

- Updated dependencies [[`e640747`](https://github.com/danieljvdm/effect-agent/commit/e6407479ae233527685928bead040dbfe5153a22), [`8899bdb`](https://github.com/danieljvdm/effect-agent/commit/8899bdbcbbd16c5b7f9981564939f64729b73015)]:
  - @effect-agent/engine@0.1.0-beta.48
  - @effect-agent/thread@0.1.0-beta.48
  - @effect-agent/core@0.1.0-beta.48
  - @effect-agent/workflow@0.1.0-beta.48
  - @effect-agent/storage-sqlite@0.1.0-beta.48

## 0.1.0-beta.47

### Patch Changes

- Updated dependencies [[`e6ff3bc`](https://github.com/danieljvdm/effect-agent/commit/e6ff3bcd1b5ce0f2348de668853482ba9d5e126b)]:
  - @effect-agent/core@0.1.0-beta.47
  - @effect-agent/engine@0.1.0-beta.47
  - @effect-agent/storage-sqlite@0.1.0-beta.47
  - @effect-agent/thread@0.1.0-beta.47
  - @effect-agent/workflow@0.1.0-beta.47

## 0.1.0-beta.46

### Minor Changes

- [#313](https://github.com/danieljvdm/effect-agent/pull/313) [`c1a6e6a`](https://github.com/danieljvdm/effect-agent/commit/c1a6e6a915be73a49b2c266e2df74256f44c25e2) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Import module namespaces from package roots, or import declarations from their explicit PascalCase module paths, following the package map's migration examples. Discard unused modules from audited packages when bundling consumers.
  BEHAVIOR CHANGE: Replace flat declaration imports, lowercase aggregate paths, cross-package aliases, and internal helper imports with their documented owning modules; use `MemoryThreadStoreLive` instead of `MemoryStorageLive`.

- [#315](https://github.com/danieljvdm/effect-agent/pull/315) [`cebe728`](https://github.com/danieljvdm/effect-agent/commit/cebe728685cf9f45c1d9579273222a865bb8109d) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Run registered agents inside native Effect Workflow handlers with `AgentWorkflow.execute` and suspend until their typed results are available. **BEHAVIOR CHANGE:** supply `principal` to `WorkflowAgentHost.layer`; custom dispatch stores must return the retained intent from `put`, preserve and atomically attach its completion token, and compare the full intent before removal.

### Patch Changes

- Updated dependencies [[`c1a6e6a`](https://github.com/danieljvdm/effect-agent/commit/c1a6e6a915be73a49b2c266e2df74256f44c25e2), [`cebe728`](https://github.com/danieljvdm/effect-agent/commit/cebe728685cf9f45c1d9579273222a865bb8109d)]:
  - @effect-agent/core@0.1.0-beta.46
  - @effect-agent/engine@0.1.0-beta.46
  - @effect-agent/storage-sqlite@0.1.0-beta.46
  - @effect-agent/thread@0.1.0-beta.46
  - @effect-agent/workflow@0.1.0-beta.46

## 0.1.0-beta.45

### Minor Changes

- [#309](https://github.com/danieljvdm/effect-agent/pull/309) [`c8812c2`](https://github.com/danieljvdm/effect-agent/commit/c8812c221004bfbeded7a56a03f13102e282f4e0) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Run existing registered agents through an application-supplied Effect Workflow engine with durable dispatch repair. Add SQLite dispatch storage, a scoped Node repair trigger, and bounded durable processing with attempt-scoped ownership release.

  BEHAVIOR CHANGE: Replace `NodeDurableRuntime` and its `Options`, `Config`, `ConfigValue`, `Services`, and `InitializationError` exports with the corresponding `NodeDurableAgentRuntime` names.

  BEHAVIOR CHANGE: Register agents with `DurableAgentRuntime.layerRegistered` or `NodeDurableAgentRuntime.layerRegistered`, and use `layerWithBindings` for precompiled bindings. Call `processThreadResolved(threadId)` and run the `runResolvedWorker` Effect without binding arguments; use `NodeDurableHost.layer` and `ThreadMaintenance.layer` as Layer values over an already-assembled runtime.

  BEHAVIOR CHANGE: Supply `WakeScheduler`, `ToolReconciler`, and `DurableRuntimeFailpoint` when calling `runChaosPlan`, which constructs registered runtimes for its delegation fixtures.

### Patch Changes

- Updated dependencies [[`c8812c2`](https://github.com/danieljvdm/effect-agent/commit/c8812c221004bfbeded7a56a03f13102e282f4e0)]:
  - @effect-agent/workflow@0.1.0-beta.45
  - @effect-agent/thread@0.1.0-beta.45
  - @effect-agent/storage-sqlite@0.1.0-beta.45
  - @effect-agent/core@0.1.0-beta.45
  - @effect-agent/engine@0.1.0-beta.45

## 0.1.0-beta.44

### Patch Changes

- [#307](https://github.com/danieljvdm/effect-agent/pull/307) [`f8365ee`](https://github.com/danieljvdm/effect-agent/commit/f8365eee4048076ced0a79b9149efc29297b7c41) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Upgrade to Effect rc.112 and `effect-cf` 0.40.0 while preserving MCP transports and Cloudflare host behavior.

  BEHAVIOR CHANGE: Upgrade Effect and its provider/platform/SQL packages to rc.112 or a compatible version. In Cloudflare hosts, provide `effect-cf@^0.40.0` and enable `nodejs_compat` for its async context support.

- Updated dependencies [[`f8365ee`](https://github.com/danieljvdm/effect-agent/commit/f8365eee4048076ced0a79b9149efc29297b7c41)]:
  - @effect-agent/core@0.1.0-beta.44
  - @effect-agent/engine@0.1.0-beta.44
  - @effect-agent/thread@0.1.0-beta.44
  - @effect-agent/storage-sqlite@0.1.0-beta.44

## 0.1.0-beta.43

### Patch Changes

- Updated dependencies [[`361c643`](https://github.com/danieljvdm/effect-agent/commit/361c643bfd1ac40095bc1d63d4d84c5a0afbf3d0)]:
  - @effect-agent/thread@0.1.0-beta.43
  - @effect-agent/storage-sqlite@0.1.0-beta.43
  - @effect-agent/core@0.1.0-beta.43
  - @effect-agent/engine@0.1.0-beta.43

## 0.1.0-beta.42

### Patch Changes

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.42
  - @effect-agent/engine@0.1.0-beta.42
  - @effect-agent/thread@0.1.0-beta.42
  - @effect-agent/storage-sqlite@0.1.0-beta.42

## 0.1.0-beta.41

### Patch Changes

- Updated dependencies [[`e21d6da`](https://github.com/danieljvdm/effect-agent/commit/e21d6da596b97c98ace533c3fa42fe9767d127e1), [`edfa7dc`](https://github.com/danieljvdm/effect-agent/commit/edfa7dc6693dea2a84366f5053826ffa87f7c587)]:
  - @effect-agent/core@0.1.0-beta.41
  - @effect-agent/thread@0.1.0-beta.41
  - @effect-agent/storage-sqlite@0.1.0-beta.41
  - @effect-agent/engine@0.1.0-beta.41

## 0.1.0-beta.40

### Patch Changes

- [#280](https://github.com/danieljvdm/effect-agent/pull/280) [`614a81d`](https://github.com/danieljvdm/effect-agent/commit/614a81db4f9d121ac209cc56fc6d420f43f4ab1b) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Assemble Cloudflare Code Mode, REST capture tools, interactive browsers, and thread clients with platform-owned Layer constructors. Register Node agents through `NodeDurableHost.layerRegistered(registrations, options)` while preserving application service requirements and host-scoped cleanup.

- Updated dependencies [[`720e6d9`](https://github.com/danieljvdm/effect-agent/commit/720e6d952cf14cf61a6550c01473938fd46a1e74), [`018f1ad`](https://github.com/danieljvdm/effect-agent/commit/018f1ad8455a0075b9cf764f85fe9b6972f07eb7), [`720e6d9`](https://github.com/danieljvdm/effect-agent/commit/720e6d952cf14cf61a6550c01473938fd46a1e74), [`018f1ad`](https://github.com/danieljvdm/effect-agent/commit/018f1ad8455a0075b9cf764f85fe9b6972f07eb7), [`b885b9f`](https://github.com/danieljvdm/effect-agent/commit/b885b9fa92a7ea72612a513c66adff13f35bd527), [`1432833`](https://github.com/danieljvdm/effect-agent/commit/14328336cd3480c5ddda8447f522591eb99eaaeb), [`1432833`](https://github.com/danieljvdm/effect-agent/commit/14328336cd3480c5ddda8447f522591eb99eaaeb), [`c36fe73`](https://github.com/danieljvdm/effect-agent/commit/c36fe73d2d226f9271c6dd60071159b0d82862ae), [`018f1ad`](https://github.com/danieljvdm/effect-agent/commit/018f1ad8455a0075b9cf764f85fe9b6972f07eb7), [`018f1ad`](https://github.com/danieljvdm/effect-agent/commit/018f1ad8455a0075b9cf764f85fe9b6972f07eb7), [`0fbcbbf`](https://github.com/danieljvdm/effect-agent/commit/0fbcbbf3c8c2ca7595543e545baddb0c6f965436)]:
  - @effect-agent/storage-sqlite@0.1.0-beta.40
  - @effect-agent/thread@0.1.0-beta.40
  - @effect-agent/core@0.1.0-beta.40
  - @effect-agent/engine@0.1.0-beta.40

## 0.1.0-beta.39

### Minor Changes

- [#262](https://github.com/danieljvdm/effect-agent/pull/262) [`34ca82e`](https://github.com/danieljvdm/effect-agent/commit/34ca82e86191bc85229bd32886b8cfaf9a2edce9) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Enforce durable child tool-call allowances across recovery and distinguish passing checks from complete adapter certification. Rename the custom durable assembly to `layerWithServices` and preserve Node extension-layer construction errors and dependencies.

  BEHAVIOR CHANGE: Replace `DurableAgentRuntime.layerWithContext` with `layerWithServices`, still supplying both separate services. Regenerate certification reports with the `effect-agent/certification@2` schema and use `fullyCertified` for gates requiring executed real-loss checks; `ok` retains its executed-check meaning. Existing child records without an allowance keep their original definition policy; start a new delegation to apply a limit.

- [#241](https://github.com/danieljvdm/effect-agent/pull/241) [`dd85dc0`](https://github.com/danieljvdm/effect-agent/commit/dd85dc07e2513e2ec56316fd7609e137d6c3f6fa) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add durable once and continuous event subscriptions that deliver Schema-defined input through ordinary Thread admission. Provide owner-scoped management Tools and a GitHub workflow run completion source with missed-webhook reconciliation.

  BEHAVIOR CHANGE: Reset incompatible private-development SQLite databases before opening them with storage version 6.

- [#263](https://github.com/danieljvdm/effect-agent/pull/263) [`95865d7`](https://github.com/danieljvdm/effect-agent/commit/95865d78f55546d42f562f2f13509bbfc198c091) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Rename `@effect-agent/session` to `@effect-agent/thread` and rename the Conversation framework API to Thread.

  BEHAVIOR CHANGE: Rename Conversation identifiers, fields, record families and tags, and the durable-admin `--conversation` selector to their Thread equivalents. Reset incompatible alpha storage before upgrading.

- [#241](https://github.com/danieljvdm/effect-agent/pull/241) [`dd85dc0`](https://github.com/danieljvdm/effect-agent/commit/dd85dc07e2513e2ec56316fd7609e137d6c3f6fa) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Bind subscription input preparation to each destination Agent's retained definition version, authorize reconciliation through explicit host policy, and preserve newer delivery retry state.

  BEHAVIOR CHANGE: Provide `SubscriptionInputBindings` and `SubscriptionAuthorizer.reconcile` in subscription hosts, and import GitHub integration from `@effect-agent/thread/github`.

### Patch Changes

- [#260](https://github.com/danieljvdm/effect-agent/pull/260) [`e6d05f5`](https://github.com/danieljvdm/effect-agent/commit/e6d05f51783035cec4f99247de2f064e730770ca) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Compose Cloudflare Thread Objects from application Layers and typed Agent version declarations, preserving initialization failures and scoped dependencies. Resolve durable work from explicit exact-version bindings and reject digest-transparent registrations.

  BEHAVIOR CHANGE: Replace `makeConversationObjectClass` with `ThreadObject.make`. Pass a composed `ThreadObject.layer(registrations)` to `ThreadObject.make`, move preparation and Tool authorization into Layers, and use `options.eventLayer` for observability. Pass bindings directly to resolved worker methods and `NodeDurableHost.layer(bindings)` instead of providing `AgentBindingResolver`.

- [#248](https://github.com/danieljvdm/effect-agent/pull/248) [`f4f37c3`](https://github.com/danieljvdm/effect-agent/commit/f4f37c37fa1b650341c6e18ee3a22cd6f518bfd2) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Compose prompt preparation and Tool authorization independently in durable hosts, preserving both across recovery.

  BEHAVIOR CHANGE: move `RunContextPreparation.toolAuthorization` to a separate `RunToolAuthorization` Layer and provide both services to `DurableAgentRuntime.layerWithServices`.

- [#256](https://github.com/danieljvdm/effect-agent/pull/256) [`ac70e21`](https://github.com/danieljvdm/effect-agent/commit/ac70e212c7d9741ce48bd9b2a4dbd355f9dac72e) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Declare `effect` as a required `^4.0.0-rc.111` peer across all public packages so they share the application's runtime and accept compatible upgrades. Keep `effect` in application dependencies at a version satisfying the framework's and providers' peer ranges.

- [#241](https://github.com/danieljvdm/effect-agent/pull/241) [`dd85dc0`](https://github.com/danieljvdm/effect-agent/commit/dd85dc07e2513e2ec56316fd7609e137d6c3f6fa) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Keep subscription recovery and Node polling active after failures combined with sibling interruption, while still stopping when the host Scope closes.

- Updated dependencies [[`e6d05f5`](https://github.com/danieljvdm/effect-agent/commit/e6d05f51783035cec4f99247de2f064e730770ca), [`34ca82e`](https://github.com/danieljvdm/effect-agent/commit/34ca82e86191bc85229bd32886b8cfaf9a2edce9), [`dd85dc0`](https://github.com/danieljvdm/effect-agent/commit/dd85dc07e2513e2ec56316fd7609e137d6c3f6fa), [`e0aa7d9`](https://github.com/danieljvdm/effect-agent/commit/e0aa7d9442ca2ec62df8195a2f9cce7b52af5257), [`f4f37c3`](https://github.com/danieljvdm/effect-agent/commit/f4f37c37fa1b650341c6e18ee3a22cd6f518bfd2), [`e0aa7d9`](https://github.com/danieljvdm/effect-agent/commit/e0aa7d9442ca2ec62df8195a2f9cce7b52af5257), [`7bab6c0`](https://github.com/danieljvdm/effect-agent/commit/7bab6c053b01398a0f1898374103997da6550268), [`f8de2d8`](https://github.com/danieljvdm/effect-agent/commit/f8de2d8a022e81eac9c357b361dd567fb65ac239), [`0d88d90`](https://github.com/danieljvdm/effect-agent/commit/0d88d90443e7d35e34799f4458d274fde99e0859), [`79fbd8b`](https://github.com/danieljvdm/effect-agent/commit/79fbd8b755434a162629a534478e188636d186fe), [`4c458e4`](https://github.com/danieljvdm/effect-agent/commit/4c458e43738bb243d1e343c97ecfd49e3b41ca9f), [`95865d7`](https://github.com/danieljvdm/effect-agent/commit/95865d78f55546d42f562f2f13509bbfc198c091), [`655bf5f`](https://github.com/danieljvdm/effect-agent/commit/655bf5f217dce1865c97ce613246c27846bfaf8a), [`d004a36`](https://github.com/danieljvdm/effect-agent/commit/d004a361518c23cdc81f1768e5ab31560e014935), [`ac70e21`](https://github.com/danieljvdm/effect-agent/commit/ac70e212c7d9741ce48bd9b2a4dbd355f9dac72e), [`dd85dc0`](https://github.com/danieljvdm/effect-agent/commit/dd85dc07e2513e2ec56316fd7609e137d6c3f6fa), [`dd85dc0`](https://github.com/danieljvdm/effect-agent/commit/dd85dc07e2513e2ec56316fd7609e137d6c3f6fa), [`511c852`](https://github.com/danieljvdm/effect-agent/commit/511c85212a564ff2729de401620fcbdeddcb4748)]:
  - @effect-agent/thread@0.1.0-beta.39
  - @effect-agent/engine@0.1.0-beta.39
  - @effect-agent/storage-sqlite@0.1.0-beta.39
  - @effect-agent/core@0.1.0-beta.39

## 0.1.0-beta.38

### Patch Changes

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.38
  - @effect-agent/engine@0.1.0-beta.38
  - @effect-agent/session@0.1.0-beta.38
  - @effect-agent/storage-sqlite@0.1.0-beta.38

## 0.1.0-beta.37

### Minor Changes

- [#212](https://github.com/danieljvdm/effect-agent/pull/212) [`242b601`](https://github.com/danieljvdm/effect-agent/commit/242b601c6d14c3448c2a3acdc28b97b48e27cf92) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add durable schedules for typed Agent input with owner authorization, one-shot, interval and cron timing, and recovery through ordinary Submission admission on Node and Cloudflare.

  BEHAVIOR CHANGE: Reset older private-development SQLite databases for storage version 5, and provide `effect-cf ^0.37.0` to Cloudflare hosts.

- [#218](https://github.com/danieljvdm/effect-agent/pull/218) [`b43cf38`](https://github.com/danieljvdm/effect-agent/commit/b43cf38093f716cefc998241183ca2059ee83fe0) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Separate scheduling management from driver authority, expose explicit public status, and fix DST delivery, failed-record starvation, and repeated resume. Allow positive host interval minimums and release operational capacity when schedules finish while retaining creation replay guarantees.

  BEHAVIOR CHANGE: Cloudflare consumers yield `Scheduling` from `CloudflareSchedulingClient.layer`; local drivers use `ScheduleDriver.layer`. Status omits persisted input and admission internals, and `dueBatchSize` bounds a query page within a sweep.

### Patch Changes

- Updated dependencies [[`242b601`](https://github.com/danieljvdm/effect-agent/commit/242b601c6d14c3448c2a3acdc28b97b48e27cf92), [`bd48a7b`](https://github.com/danieljvdm/effect-agent/commit/bd48a7b200fb71335b19edd7941be331b6ede9ea), [`bd48a7b`](https://github.com/danieljvdm/effect-agent/commit/bd48a7b200fb71335b19edd7941be331b6ede9ea), [`b43cf38`](https://github.com/danieljvdm/effect-agent/commit/b43cf38093f716cefc998241183ca2059ee83fe0)]:
  - @effect-agent/session@0.1.0-beta.37
  - @effect-agent/storage-sqlite@0.1.0-beta.37
  - @effect-agent/engine@0.1.0-beta.37
  - @effect-agent/core@0.1.0-beta.37

## 0.1.0-beta.36

### Patch Changes

- Updated dependencies [[`082c258`](https://github.com/danieljvdm/effect-agent/commit/082c2584573c1ffbfa7d5b7166f4243e996816eb)]:
  - @effect-agent/session@0.1.0-beta.36
  - @effect-agent/storage-sqlite@0.1.0-beta.36
  - @effect-agent/core@0.1.0-beta.36
  - @effect-agent/engine@0.1.0-beta.36

## 0.1.0-beta.35

### Patch Changes

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.35
  - @effect-agent/engine@0.1.0-beta.35
  - @effect-agent/session@0.1.0-beta.35
  - @effect-agent/storage-sqlite@0.1.0-beta.35

## 0.1.0-beta.34

### Minor Changes

- [#206](https://github.com/danieljvdm/effect-agent/pull/206) [`aa3ebfb`](https://github.com/danieljvdm/effect-agent/commit/aa3ebfb4fd1e69be77c433a881ddecb3567c36c2) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Expose non-propagating Tool failures to an opt-in trusted local observer, preserving live Causes without automatic export. Install the same observer through durable Node and Cloudflare runtime options while excluding settled-call replay.

### Patch Changes

- [#202](https://github.com/danieljvdm/effect-agent/pull/202) [`cf4a8d9`](https://github.com/danieljvdm/effect-agent/commit/cf4a8d9c645d5d8a2e552f4bb4902af4253d91ee) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Align the Effect family with rc.111 to decode nested OpenAI error events, and preserve transformed Tool parameters under its encoded response contract.

- Updated dependencies [[`cf4a8d9`](https://github.com/danieljvdm/effect-agent/commit/cf4a8d9c645d5d8a2e552f4bb4902af4253d91ee), [`baecd08`](https://github.com/danieljvdm/effect-agent/commit/baecd08f1d6f2c0698e16487cdcccf2f6ffcebca), [`cf4a8d9`](https://github.com/danieljvdm/effect-agent/commit/cf4a8d9c645d5d8a2e552f4bb4902af4253d91ee), [`baecd08`](https://github.com/danieljvdm/effect-agent/commit/baecd08f1d6f2c0698e16487cdcccf2f6ffcebca), [`aa3ebfb`](https://github.com/danieljvdm/effect-agent/commit/aa3ebfb4fd1e69be77c433a881ddecb3567c36c2)]:
  - @effect-agent/engine@0.1.0-beta.34
  - @effect-agent/session@0.1.0-beta.34
  - @effect-agent/core@0.1.0-beta.34
  - @effect-agent/storage-sqlite@0.1.0-beta.34

## 0.1.0-beta.33

### Patch Changes

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.33
  - @effect-agent/engine@0.1.0-beta.33
  - @effect-agent/session@0.1.0-beta.33
  - @effect-agent/storage-sqlite@0.1.0-beta.33

## 0.1.0-beta.32

### Patch Changes

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.32
  - @effect-agent/engine@0.1.0-beta.32
  - @effect-agent/session@0.1.0-beta.32
  - @effect-agent/storage-sqlite@0.1.0-beta.32

## 0.1.0-beta.31

### Patch Changes

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.31
  - @effect-agent/engine@0.1.0-beta.31
  - @effect-agent/session@0.1.0-beta.31
  - @effect-agent/storage-sqlite@0.1.0-beta.31

## 0.1.0-beta.30

### Patch Changes

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.30
  - @effect-agent/engine@0.1.0-beta.30
  - @effect-agent/session@0.1.0-beta.30
  - @effect-agent/storage-sqlite@0.1.0-beta.30

## 0.1.0-beta.29

### Patch Changes

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.29
  - @effect-agent/engine@0.1.0-beta.29
  - @effect-agent/session@0.1.0-beta.29
  - @effect-agent/storage-sqlite@0.1.0-beta.29

## 0.1.0-beta.28

### Patch Changes

- Updated dependencies [[`374771d`](https://github.com/danieljvdm/effect-agent/commit/374771d90afa26ce7e1832f76715aa7b9eea3741)]:
  - @effect-agent/engine@0.1.0-beta.28
  - @effect-agent/session@0.1.0-beta.28
  - @effect-agent/storage-sqlite@0.1.0-beta.28
  - @effect-agent/core@0.1.0-beta.28

## 0.1.0-beta.27

### Minor Changes

- [#155](https://github.com/danieljvdm/effect-agent/pull/155) [`773264b`](https://github.com/danieljvdm/effect-agent/commit/773264b75759c4456e1e549d2172bbe39610a8c1) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add crash-safe terminal delivery Tools and final model responses, completion-capacity reservation, Run-scoped prompt provenance, and target-aware compaction.
  Persist priced per-call model usage in the DN and DC assemblies and expose aggregate usage on Run settlements.

### Patch Changes

- Updated dependencies [[`773264b`](https://github.com/danieljvdm/effect-agent/commit/773264b75759c4456e1e549d2172bbe39610a8c1)]:
  - @effect-agent/core@0.1.0-beta.27
  - @effect-agent/engine@0.1.0-beta.27
  - @effect-agent/session@0.1.0-beta.27
  - @effect-agent/storage-sqlite@0.1.0-beta.27

## 0.1.0-beta.26

### Patch Changes

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.26
  - @effect-agent/session@0.1.0-beta.26
  - @effect-agent/storage-sqlite@0.1.0-beta.26

## 0.1.0-beta.25

### Patch Changes

- [#142](https://github.com/danieljvdm/effect-agent/pull/142) [`b6804dd`](https://github.com/danieljvdm/effect-agent/commit/b6804dd60cc83b569d0e87b88521952c20ba9b7d) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Validate Node runtime configuration before opening SQLite. Keep Cloudflare RPC failures typed when foreign diagnostics are hostile, and close both Dynamic Worker RPC handles when a pass ends.

- Updated dependencies [[`b6804dd`](https://github.com/danieljvdm/effect-agent/commit/b6804dd60cc83b569d0e87b88521952c20ba9b7d), [`b6804dd`](https://github.com/danieljvdm/effect-agent/commit/b6804dd60cc83b569d0e87b88521952c20ba9b7d)]:
  - @effect-agent/session@0.1.0-beta.25
  - @effect-agent/storage-sqlite@0.1.0-beta.25
  - @effect-agent/core@0.1.0-beta.25

## 0.1.0-beta.24

### Patch Changes

- Updated dependencies [[`6e3f56f`](https://github.com/danieljvdm/effect-agent/commit/6e3f56fbadd831372124578b027ea2bd5ff8f008)]:
  - @effect-agent/session@0.1.0-beta.24
  - @effect-agent/storage-sqlite@0.1.0-beta.24
  - @effect-agent/core@0.1.0-beta.24

## 0.1.0-beta.23

### Patch Changes

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.23
  - @effect-agent/session@0.1.0-beta.23
  - @effect-agent/storage-sqlite@0.1.0-beta.23

## 0.1.0-beta.22

### Patch Changes

- Updated dependencies [[`ce8b39c`](https://github.com/danieljvdm/effect-agent/commit/ce8b39ce8f716c0a11c6394d136b67cb9be84588)]:
  - @effect-agent/core@0.1.0-beta.22
  - @effect-agent/session@0.1.0-beta.22
  - @effect-agent/storage-sqlite@0.1.0-beta.22

## 0.1.0-beta.21

### Patch Changes

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.21
  - @effect-agent/session@0.1.0-beta.21
  - @effect-agent/storage-sqlite@0.1.0-beta.21

## 0.1.0-beta.20

### Patch Changes

- [#111](https://github.com/danieljvdm/effect-agent/pull/111) [`c715f9f`](https://github.com/danieljvdm/effect-agent/commit/c715f9f8e436fa85e8c1ef2b27f640e637ea52e4) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Align every public package with the Effect 4.0.0-rc.110 family.

- [#111](https://github.com/danieljvdm/effect-agent/pull/111) [`c715f9f`](https://github.com/danieljvdm/effect-agent/commit/c715f9f8e436fa85e8c1ef2b27f640e637ea52e4) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Fix `validateMcpDiscovery` reporting a permanent schema drift for MCP tools whose parameters or success type is a named, refined Schema (a branded ID, a bounded string, a `Schema.Class`) — both schema derivations now resolve a top-level `$ref` before comparison.

- Updated dependencies [[`7c093ec`](https://github.com/danieljvdm/effect-agent/commit/7c093ecfd900a0c55163fce76b0609d04434fa73), [`c715f9f`](https://github.com/danieljvdm/effect-agent/commit/c715f9f8e436fa85e8c1ef2b27f640e637ea52e4), [`c715f9f`](https://github.com/danieljvdm/effect-agent/commit/c715f9f8e436fa85e8c1ef2b27f640e637ea52e4)]:
  - @effect-agent/session@0.1.0-beta.20
  - @effect-agent/core@0.1.0-beta.20
  - @effect-agent/storage-sqlite@0.1.0-beta.20

## 0.1.0-beta.19

### Patch Changes

- Updated dependencies [[`9e31de4`](https://github.com/danieljvdm/effect-agent/commit/9e31de4c5f63ebc7eefbce33d3e0ed2052538f26), [`b8beef5`](https://github.com/danieljvdm/effect-agent/commit/b8beef5624f6704b0e52b5023babd1272d6b0603)]:
  - @effect-agent/session@0.1.0-beta.19
  - @effect-agent/storage-sqlite@0.1.0-beta.19
  - @effect-agent/core@0.1.0-beta.19

## 0.1.0-beta.18

### Patch Changes

- Updated dependencies [[`f36fd40`](https://github.com/danieljvdm/effect-agent/commit/f36fd409f8a34e13c87646fd857a4060ac89e89d)]:
  - @effect-agent/session@0.1.0-beta.18
  - @effect-agent/storage-sqlite@0.1.0-beta.18
  - @effect-agent/core@0.1.0-beta.18

## 0.1.0-beta.17

### Patch Changes

- Updated dependencies [[`016df57`](https://github.com/danieljvdm/effect-agent/commit/016df574fa8c0f362468d848ae830d72532cbcaf)]:
  - @effect-agent/core@0.1.0-beta.17
  - @effect-agent/session@0.1.0-beta.17
  - @effect-agent/storage-sqlite@0.1.0-beta.17

## 0.1.0-beta.16

### Patch Changes

- Updated dependencies [[`e4b32b5`](https://github.com/danieljvdm/effect-agent/commit/e4b32b54061e58de57d5c27f06f8ef2a821ccb38)]:
  - @effect-agent/session@0.1.0-beta.16
  - @effect-agent/storage-sqlite@0.1.0-beta.16
  - @effect-agent/core@0.1.0-beta.16

## 0.1.0-beta.15

### Patch Changes

- Updated dependencies [[`38ac06e`](https://github.com/danieljvdm/effect-agent/commit/38ac06eea0956d7bef4576c5e527c6053f5a86f0)]:
  - @effect-agent/session@0.1.0-beta.15
  - @effect-agent/storage-sqlite@0.1.0-beta.15
  - @effect-agent/core@0.1.0-beta.15

## 0.1.0-beta.14

### Patch Changes

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.14
  - @effect-agent/session@0.1.0-beta.14
  - @effect-agent/storage-sqlite@0.1.0-beta.14

## 0.1.0-beta.13

### Patch Changes

- Updated dependencies [[`68b48c9`](https://github.com/danieljvdm/effect-agent/commit/68b48c932b6a76d2c8ed0f04cc87c123a9fd11e4)]:
  - @effect-agent/core@0.1.0-beta.13
  - @effect-agent/session@0.1.0-beta.13
  - @effect-agent/storage-sqlite@0.1.0-beta.13

## 0.1.0-beta.12

### Patch Changes

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.12
  - @effect-agent/session@0.1.0-beta.12
  - @effect-agent/storage-sqlite@0.1.0-beta.12

## 0.1.0-beta.11

### Patch Changes

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.11
  - @effect-agent/session@0.1.0-beta.11
  - @effect-agent/storage-sqlite@0.1.0-beta.11

## 0.1.0-beta.10

### Patch Changes

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.10
  - @effect-agent/session@0.1.0-beta.10
  - @effect-agent/storage-sqlite@0.1.0-beta.10

## 0.1.0-beta.9

### Patch Changes

- Updated dependencies [[`91ff50d`](https://github.com/danieljvdm/effect-agent/commit/91ff50df5480a0ccdfb8e0a00db39a1576e6c34b)]:
  - @effect-agent/core@0.1.0-beta.9
  - @effect-agent/session@0.1.0-beta.9
  - @effect-agent/storage-sqlite@0.1.0-beta.9

## 0.1.0-beta.8

### Patch Changes

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.8
  - @effect-agent/session@0.1.0-beta.8
  - @effect-agent/storage-sqlite@0.1.0-beta.8

## 0.1.0-beta.7

### Patch Changes

- Updated dependencies [[`5c49b78`](https://github.com/danieljvdm/effect-agent/commit/5c49b786604b3e8389cdc2c54d4f5cb284eac2b7), [`afe755a`](https://github.com/danieljvdm/effect-agent/commit/afe755a331172ffca9ceee7dd82bb452c6ccbb8a)]:
  - @effect-agent/session@0.1.0-beta.7
  - @effect-agent/core@0.1.0-beta.7
  - @effect-agent/storage-sqlite@0.1.0-beta.7

## 0.1.0-beta.6

### Patch Changes

- Updated dependencies [[`e13ee6e`](https://github.com/danieljvdm/effect-agent/commit/e13ee6e7817549e99837d06e86caf2dea8656aa8)]:
  - @effect-agent/core@0.1.0-beta.6
  - @effect-agent/session@0.1.0-beta.6
  - @effect-agent/storage-sqlite@0.1.0-beta.6

## 0.0.1-beta.5

### Patch Changes

- [#19](https://github.com/danieljvdm/effect-agent/pull/19) [`a063031`](https://github.com/danieljvdm/effect-agent/commit/a063031c6b1f1637d947ae193a410b6bb9e8a9fc) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Align every public package with Effect 4.0.0-beta.107. Also expose per-incarnation Cloudflare
  Binding capture with live Durable Object context and derived identities, and prevent incomplete
  application Tool batches from a failed or aborted Run from poisoning prompts for later Runs.
- Updated dependencies [[`a063031`](https://github.com/danieljvdm/effect-agent/commit/a063031c6b1f1637d947ae193a410b6bb9e8a9fc)]:
  - @effect-agent/core@0.0.1-beta.5
  - @effect-agent/session@0.0.1-beta.5
  - @effect-agent/storage-sqlite@0.0.1-beta.5

## 0.0.1-beta.4

### Patch Changes

- [#13](https://github.com/danieljvdm/effect-agent/pull/13) [`f4e3786`](https://github.com/danieljvdm/effect-agent/commit/f4e378635a794d4c17192ee3de011697ccec3a3b) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Introduce the `effect-agent` umbrella package: the framework's complete pure
  surface — schema-first authoring (core), the bounded interpreter (engine),
  and operational capabilities — as one dependency-clean root package,
  mirroring how `effect` fronts the `@effect/*` satellites. Platform adapters
  remain scoped. The umbrella is version-fixed to its three constituents.
- Updated dependencies [[`f4e3786`](https://github.com/danieljvdm/effect-agent/commit/f4e378635a794d4c17192ee3de011697ccec3a3b)]:
  - @effect-agent/core@0.0.1-beta.4
  - @effect-agent/session@0.0.1-beta.4
  - @effect-agent/storage-sqlite@0.0.1-beta.4

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
  - @effect-agent/session@0.0.1-beta.3
  - @effect-agent/storage-sqlite@0.0.1-beta.3

## 0.0.1-beta.1

### Patch Changes

- Republish with correctly pinned internal dependencies. The 0.0.1-beta.0
  artifacts depended on internal `@effect-agent/*` versions that were never
  published (`workspace:*` ranges were resolved from a stale lockfile at
  publish time); the release script now pins internal ranges to the exact
  workspace versions itself.
- Updated dependencies []:
  - @effect-agent/core@0.0.1-beta.1
  - @effect-agent/session@0.0.1-beta.1
  - @effect-agent/storage-sqlite@0.0.1-beta.1

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
  - @effect-agent/session@0.0.1-beta.0
  - @effect-agent/storage-sqlite@0.0.1-beta.0

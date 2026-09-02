# @effect-agent/testing

## 0.1.0-beta.41

### Patch Changes

- Updated dependencies [[`e21d6da`](https://github.com/danieljvdm/effect-agent/commit/e21d6da596b97c98ace533c3fa42fe9767d127e1), [`edfa7dc`](https://github.com/danieljvdm/effect-agent/commit/edfa7dc6693dea2a84366f5053826ffa87f7c587)]:
  - @effect-agent/core@0.1.0-beta.41
  - @effect-agent/thread@0.1.0-beta.41
  - @effect-agent/capabilities@0.1.0-beta.41
  - @effect-agent/engine@0.1.0-beta.41
  - @effect-agent/sandbox@0.1.0-beta.41

## 0.1.0-beta.40

### Patch Changes

- [#269](https://github.com/danieljvdm/effect-agent/pull/269) [`018f1ad`](https://github.com/danieljvdm/effect-agent/commit/018f1ad8455a0075b9cf764f85fe9b6972f07eb7) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Provide optional context loading through `RunContextPreparation` for ephemeral and durable runs, and catch its concrete tagged errors directly.

  BEHAVIOR CHANGE: replace `RunContextPreparationError.make` with a declared `AgentInputError`, `MemoryRecallError`, or `CompactionError`.

- Updated dependencies [[`720e6d9`](https://github.com/danieljvdm/effect-agent/commit/720e6d952cf14cf61a6550c01473938fd46a1e74), [`1432833`](https://github.com/danieljvdm/effect-agent/commit/14328336cd3480c5ddda8447f522591eb99eaaeb), [`018f1ad`](https://github.com/danieljvdm/effect-agent/commit/018f1ad8455a0075b9cf764f85fe9b6972f07eb7), [`1432833`](https://github.com/danieljvdm/effect-agent/commit/14328336cd3480c5ddda8447f522591eb99eaaeb), [`720e6d9`](https://github.com/danieljvdm/effect-agent/commit/720e6d952cf14cf61a6550c01473938fd46a1e74), [`018f1ad`](https://github.com/danieljvdm/effect-agent/commit/018f1ad8455a0075b9cf764f85fe9b6972f07eb7), [`1432833`](https://github.com/danieljvdm/effect-agent/commit/14328336cd3480c5ddda8447f522591eb99eaaeb), [`c36fe73`](https://github.com/danieljvdm/effect-agent/commit/c36fe73d2d226f9271c6dd60071159b0d82862ae), [`018f1ad`](https://github.com/danieljvdm/effect-agent/commit/018f1ad8455a0075b9cf764f85fe9b6972f07eb7), [`018f1ad`](https://github.com/danieljvdm/effect-agent/commit/018f1ad8455a0075b9cf764f85fe9b6972f07eb7), [`c36fe73`](https://github.com/danieljvdm/effect-agent/commit/c36fe73d2d226f9271c6dd60071159b0d82862ae), [`0fbcbbf`](https://github.com/danieljvdm/effect-agent/commit/0fbcbbf3c8c2ca7595543e545baddb0c6f965436)]:
  - @effect-agent/thread@0.1.0-beta.40
  - @effect-agent/capabilities@0.1.0-beta.40
  - @effect-agent/core@0.1.0-beta.40
  - @effect-agent/engine@0.1.0-beta.40
  - @effect-agent/sandbox@0.1.0-beta.40

## 0.1.0-beta.39

### Minor Changes

- [#262](https://github.com/danieljvdm/effect-agent/pull/262) [`34ca82e`](https://github.com/danieljvdm/effect-agent/commit/34ca82e86191bc85229bd32886b8cfaf9a2edce9) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Enforce durable child tool-call allowances across recovery and distinguish passing checks from complete adapter certification. Rename the custom durable assembly to `layerWithServices` and preserve Node extension-layer construction errors and dependencies.

  BEHAVIOR CHANGE: Replace `DurableAgentRuntime.layerWithContext` with `layerWithServices`, still supplying both separate services. Regenerate certification reports with the `effect-agent/certification@2` schema and use `fullyCertified` for gates requiring executed real-loss checks; `ok` retains its executed-check meaning. Existing child records without an allowance keep their original definition policy; start a new delegation to apply a limit.

- [#249](https://github.com/danieljvdm/effect-agent/pull/249) [`f8de2d8`](https://github.com/danieljvdm/effect-agent/commit/f8de2d8a022e81eac9c357b361dd567fb65ac239) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Import specialized testing utilities and fixtures from their documented subpaths, and use failpoint controls from `/testing` with `TestControl.layer` in place of `Failpoint.layerTest`; keep migration loaders internal.
  Import Browser Run adapters from their dedicated Cloudflare subpaths and install `@cloudflare/puppeteer` explicitly when using `/interactive-browser`.

- [#252](https://github.com/danieljvdm/effect-agent/pull/252) [`0d88d90`](https://github.com/danieljvdm/effect-agent/commit/0d88d90443e7d35e34799f4458d274fde99e0859) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Provide canonical thread history to `AgentRuntime.run`, `start`, and `stream` through `PersistentHistory.layer` without admitting durable work. Make checkpoint storage an optional `ThreadStore.checkpoints` capability.

  BEHAVIOR CHANGE: Provide `ThreadHistory.layerTransient` for transient execution or `PersistentHistory.layer` with a ThreadStore for retained history. Use `store.checkpoints.save` and `store.checkpoints.load` after checking capability availability. `UserInputRecorded.submissionId` is present only for durably accepted input.

- [#263](https://github.com/danieljvdm/effect-agent/pull/263) [`95865d7`](https://github.com/danieljvdm/effect-agent/commit/95865d78f55546d42f562f2f13509bbfc198c091) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Rename `@effect-agent/session` to `@effect-agent/thread` and rename the Conversation framework API to Thread.

  BEHAVIOR CHANGE: Rename Conversation identifiers, fields, record families and tags, and the durable-admin `--conversation` selector to their Thread equivalents. Reset incompatible alpha storage before upgrading.

### Patch Changes

- [#260](https://github.com/danieljvdm/effect-agent/pull/260) [`e6d05f5`](https://github.com/danieljvdm/effect-agent/commit/e6d05f51783035cec4f99247de2f064e730770ca) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Compose Cloudflare Thread Objects from application Layers and typed Agent version declarations, preserving initialization failures and scoped dependencies. Resolve durable work from explicit exact-version bindings and reject digest-transparent registrations.

  BEHAVIOR CHANGE: Replace `makeConversationObjectClass` with `ThreadObject.make`. Pass a composed `ThreadObject.layer(registrations)` to `ThreadObject.make`, move preparation and Tool authorization into Layers, and use `options.eventLayer` for observability. Pass bindings directly to resolved worker methods and `NodeDurableHost.layer(bindings)` instead of providing `AgentBindingResolver`.

- [#256](https://github.com/danieljvdm/effect-agent/pull/256) [`ac70e21`](https://github.com/danieljvdm/effect-agent/commit/ac70e212c7d9741ce48bd9b2a4dbd355f9dac72e) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Declare `effect` as a required `^4.0.0-rc.111` peer across all public packages so they share the application's runtime and accept compatible upgrades. Keep `effect` in application dependencies at a version satisfying the framework's and providers' peer ranges.

- Updated dependencies [[`e6d05f5`](https://github.com/danieljvdm/effect-agent/commit/e6d05f51783035cec4f99247de2f064e730770ca), [`34ca82e`](https://github.com/danieljvdm/effect-agent/commit/34ca82e86191bc85229bd32886b8cfaf9a2edce9), [`dd85dc0`](https://github.com/danieljvdm/effect-agent/commit/dd85dc07e2513e2ec56316fd7609e137d6c3f6fa), [`e0aa7d9`](https://github.com/danieljvdm/effect-agent/commit/e0aa7d9442ca2ec62df8195a2f9cce7b52af5257), [`f4f37c3`](https://github.com/danieljvdm/effect-agent/commit/f4f37c37fa1b650341c6e18ee3a22cd6f518bfd2), [`e0aa7d9`](https://github.com/danieljvdm/effect-agent/commit/e0aa7d9442ca2ec62df8195a2f9cce7b52af5257), [`7bab6c0`](https://github.com/danieljvdm/effect-agent/commit/7bab6c053b01398a0f1898374103997da6550268), [`f8de2d8`](https://github.com/danieljvdm/effect-agent/commit/f8de2d8a022e81eac9c357b361dd567fb65ac239), [`0d88d90`](https://github.com/danieljvdm/effect-agent/commit/0d88d90443e7d35e34799f4458d274fde99e0859), [`79fbd8b`](https://github.com/danieljvdm/effect-agent/commit/79fbd8b755434a162629a534478e188636d186fe), [`4c458e4`](https://github.com/danieljvdm/effect-agent/commit/4c458e43738bb243d1e343c97ecfd49e3b41ca9f), [`95865d7`](https://github.com/danieljvdm/effect-agent/commit/95865d78f55546d42f562f2f13509bbfc198c091), [`655bf5f`](https://github.com/danieljvdm/effect-agent/commit/655bf5f217dce1865c97ce613246c27846bfaf8a), [`d004a36`](https://github.com/danieljvdm/effect-agent/commit/d004a361518c23cdc81f1768e5ab31560e014935), [`ac70e21`](https://github.com/danieljvdm/effect-agent/commit/ac70e212c7d9741ce48bd9b2a4dbd355f9dac72e), [`dd85dc0`](https://github.com/danieljvdm/effect-agent/commit/dd85dc07e2513e2ec56316fd7609e137d6c3f6fa), [`dd85dc0`](https://github.com/danieljvdm/effect-agent/commit/dd85dc07e2513e2ec56316fd7609e137d6c3f6fa), [`511c852`](https://github.com/danieljvdm/effect-agent/commit/511c85212a564ff2729de401620fcbdeddcb4748)]:
  - @effect-agent/thread@0.1.0-beta.39
  - @effect-agent/capabilities@0.1.0-beta.39
  - @effect-agent/engine@0.1.0-beta.39
  - @effect-agent/core@0.1.0-beta.39
  - @effect-agent/sandbox@0.1.0-beta.39

## 0.1.0-beta.38

### Patch Changes

- Updated dependencies []:
  - @effect-agent/capabilities@0.1.0-beta.38
  - @effect-agent/core@0.1.0-beta.38
  - @effect-agent/engine@0.1.0-beta.38
  - @effect-agent/platform-node@0.1.0-beta.38
  - @effect-agent/sandbox@0.1.0-beta.38
  - @effect-agent/session@0.1.0-beta.38
  - @effect-agent/storage-memory@0.1.0-beta.38
  - @effect-agent/storage-sqlite@0.1.0-beta.38

## 0.1.0-beta.37

### Patch Changes

- Updated dependencies [[`242b601`](https://github.com/danieljvdm/effect-agent/commit/242b601c6d14c3448c2a3acdc28b97b48e27cf92), [`bd48a7b`](https://github.com/danieljvdm/effect-agent/commit/bd48a7b200fb71335b19edd7941be331b6ede9ea), [`bd48a7b`](https://github.com/danieljvdm/effect-agent/commit/bd48a7b200fb71335b19edd7941be331b6ede9ea), [`b43cf38`](https://github.com/danieljvdm/effect-agent/commit/b43cf38093f716cefc998241183ca2059ee83fe0)]:
  - @effect-agent/session@0.1.0-beta.37
  - @effect-agent/storage-memory@0.1.0-beta.37
  - @effect-agent/storage-sqlite@0.1.0-beta.37
  - @effect-agent/platform-node@0.1.0-beta.37
  - @effect-agent/engine@0.1.0-beta.37
  - @effect-agent/core@0.1.0-beta.37
  - @effect-agent/capabilities@0.1.0-beta.37
  - @effect-agent/sandbox@0.1.0-beta.37

## 0.1.0-beta.36

### Patch Changes

- Updated dependencies [[`082c258`](https://github.com/danieljvdm/effect-agent/commit/082c2584573c1ffbfa7d5b7166f4243e996816eb)]:
  - @effect-agent/session@0.1.0-beta.36
  - @effect-agent/storage-memory@0.1.0-beta.36
  - @effect-agent/storage-sqlite@0.1.0-beta.36
  - @effect-agent/platform-node@0.1.0-beta.36
  - @effect-agent/capabilities@0.1.0-beta.36
  - @effect-agent/core@0.1.0-beta.36
  - @effect-agent/engine@0.1.0-beta.36
  - @effect-agent/sandbox@0.1.0-beta.36

## 0.1.0-beta.35

### Patch Changes

- Updated dependencies [[`065c455`](https://github.com/danieljvdm/effect-agent/commit/065c455d1277f73157f610429de283f41ec83d9c), [`06d4f88`](https://github.com/danieljvdm/effect-agent/commit/06d4f88c78ad175bb7e4106d53e01a2c6076ebdc)]:
  - @effect-agent/sandbox@0.1.0-beta.35
  - @effect-agent/capabilities@0.1.0-beta.35
  - @effect-agent/platform-node@0.1.0-beta.35
  - @effect-agent/core@0.1.0-beta.35
  - @effect-agent/engine@0.1.0-beta.35
  - @effect-agent/session@0.1.0-beta.35
  - @effect-agent/storage-memory@0.1.0-beta.35
  - @effect-agent/storage-sqlite@0.1.0-beta.35

## 0.1.0-beta.34

### Patch Changes

- [#202](https://github.com/danieljvdm/effect-agent/pull/202) [`cf4a8d9`](https://github.com/danieljvdm/effect-agent/commit/cf4a8d9c645d5d8a2e552f4bb4902af4253d91ee) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Align the Effect family with rc.111 to decode nested OpenAI error events, and preserve transformed Tool parameters under its encoded response contract.

- Updated dependencies [[`cf4a8d9`](https://github.com/danieljvdm/effect-agent/commit/cf4a8d9c645d5d8a2e552f4bb4902af4253d91ee), [`baecd08`](https://github.com/danieljvdm/effect-agent/commit/baecd08f1d6f2c0698e16487cdcccf2f6ffcebca), [`cf4a8d9`](https://github.com/danieljvdm/effect-agent/commit/cf4a8d9c645d5d8a2e552f4bb4902af4253d91ee), [`baecd08`](https://github.com/danieljvdm/effect-agent/commit/baecd08f1d6f2c0698e16487cdcccf2f6ffcebca), [`aa3ebfb`](https://github.com/danieljvdm/effect-agent/commit/aa3ebfb4fd1e69be77c433a881ddecb3567c36c2)]:
  - @effect-agent/engine@0.1.0-beta.34
  - @effect-agent/session@0.1.0-beta.34
  - @effect-agent/core@0.1.0-beta.34
  - @effect-agent/capabilities@0.1.0-beta.34
  - @effect-agent/sandbox@0.1.0-beta.34
  - @effect-agent/storage-memory@0.1.0-beta.34
  - @effect-agent/storage-sqlite@0.1.0-beta.34
  - @effect-agent/platform-node@0.1.0-beta.34

## 0.1.0-beta.33

### Patch Changes

- Updated dependencies []:
  - @effect-agent/capabilities@0.1.0-beta.33
  - @effect-agent/core@0.1.0-beta.33
  - @effect-agent/engine@0.1.0-beta.33
  - @effect-agent/platform-node@0.1.0-beta.33
  - @effect-agent/sandbox@0.1.0-beta.33
  - @effect-agent/session@0.1.0-beta.33
  - @effect-agent/storage-memory@0.1.0-beta.33
  - @effect-agent/storage-sqlite@0.1.0-beta.33

## 0.1.0-beta.32

### Patch Changes

- Updated dependencies [[`7592ded`](https://github.com/danieljvdm/effect-agent/commit/7592deda757e0eeb0243f86bae9c2b15623e3c76)]:
  - @effect-agent/sandbox@0.1.0-beta.32
  - @effect-agent/capabilities@0.1.0-beta.32
  - @effect-agent/platform-node@0.1.0-beta.32
  - @effect-agent/core@0.1.0-beta.32
  - @effect-agent/engine@0.1.0-beta.32
  - @effect-agent/session@0.1.0-beta.32
  - @effect-agent/storage-memory@0.1.0-beta.32
  - @effect-agent/storage-sqlite@0.1.0-beta.32

## 0.1.0-beta.31

### Patch Changes

- Updated dependencies [[`d3c42d4`](https://github.com/danieljvdm/effect-agent/commit/d3c42d4e34f27610845863ec29908cd3fce95188)]:
  - @effect-agent/sandbox@0.1.0-beta.31
  - @effect-agent/capabilities@0.1.0-beta.31
  - @effect-agent/platform-node@0.1.0-beta.31
  - @effect-agent/core@0.1.0-beta.31
  - @effect-agent/engine@0.1.0-beta.31
  - @effect-agent/session@0.1.0-beta.31
  - @effect-agent/storage-memory@0.1.0-beta.31
  - @effect-agent/storage-sqlite@0.1.0-beta.31

## 0.1.0-beta.30

### Patch Changes

- Updated dependencies [[`d123424`](https://github.com/danieljvdm/effect-agent/commit/d123424be7679cfe1b8d133d0d2aa1497e087590), [`d123424`](https://github.com/danieljvdm/effect-agent/commit/d123424be7679cfe1b8d133d0d2aa1497e087590), [`d123424`](https://github.com/danieljvdm/effect-agent/commit/d123424be7679cfe1b8d133d0d2aa1497e087590), [`34d05cd`](https://github.com/danieljvdm/effect-agent/commit/34d05cd1ce06f57f890b18b5ba1bce8af85db3e3)]:
  - @effect-agent/sandbox@0.1.0-beta.30
  - @effect-agent/capabilities@0.1.0-beta.30
  - @effect-agent/platform-node@0.1.0-beta.30
  - @effect-agent/core@0.1.0-beta.30
  - @effect-agent/engine@0.1.0-beta.30
  - @effect-agent/session@0.1.0-beta.30
  - @effect-agent/storage-memory@0.1.0-beta.30
  - @effect-agent/storage-sqlite@0.1.0-beta.30

## 0.1.0-beta.29

### Patch Changes

- Updated dependencies []:
  - @effect-agent/capabilities@0.1.0-beta.29
  - @effect-agent/core@0.1.0-beta.29
  - @effect-agent/engine@0.1.0-beta.29
  - @effect-agent/platform-node@0.1.0-beta.29
  - @effect-agent/sandbox@0.1.0-beta.29
  - @effect-agent/session@0.1.0-beta.29
  - @effect-agent/storage-memory@0.1.0-beta.29
  - @effect-agent/storage-sqlite@0.1.0-beta.29

## 0.1.0-beta.28

### Patch Changes

- Updated dependencies [[`374771d`](https://github.com/danieljvdm/effect-agent/commit/374771d90afa26ce7e1832f76715aa7b9eea3741)]:
  - @effect-agent/engine@0.1.0-beta.28
  - @effect-agent/capabilities@0.1.0-beta.28
  - @effect-agent/platform-node@0.1.0-beta.28
  - @effect-agent/session@0.1.0-beta.28
  - @effect-agent/storage-memory@0.1.0-beta.28
  - @effect-agent/storage-sqlite@0.1.0-beta.28
  - @effect-agent/core@0.1.0-beta.28
  - @effect-agent/sandbox@0.1.0-beta.28

## 0.1.0-beta.27

### Patch Changes

- [#155](https://github.com/danieljvdm/effect-agent/pull/155) [`773264b`](https://github.com/danieljvdm/effect-agent/commit/773264b75759c4456e1e549d2172bbe39610a8c1) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add crash-safe terminal delivery Tools and final model responses, completion-capacity reservation, Run-scoped prompt provenance, and target-aware compaction.
  Persist priced per-call model usage in the DN and DC assemblies and expose aggregate usage on Run settlements.
- Updated dependencies [[`47e9a53`](https://github.com/danieljvdm/effect-agent/commit/47e9a53d99555af3b0ac993b5c9c55ad266e327b), [`773264b`](https://github.com/danieljvdm/effect-agent/commit/773264b75759c4456e1e549d2172bbe39610a8c1)]:
  - @effect-agent/sandbox@0.1.0-beta.27
  - @effect-agent/capabilities@0.1.0-beta.27
  - @effect-agent/core@0.1.0-beta.27
  - @effect-agent/engine@0.1.0-beta.27
  - @effect-agent/session@0.1.0-beta.27
  - @effect-agent/platform-node@0.1.0-beta.27
  - @effect-agent/storage-memory@0.1.0-beta.27
  - @effect-agent/storage-sqlite@0.1.0-beta.27

## 0.1.0-beta.26

### Patch Changes

- Updated dependencies []:
  - @effect-agent/capabilities@0.1.0-beta.26
  - @effect-agent/core@0.1.0-beta.26
  - @effect-agent/engine@0.1.0-beta.26
  - @effect-agent/platform-node@0.1.0-beta.26
  - @effect-agent/sandbox@0.1.0-beta.26
  - @effect-agent/session@0.1.0-beta.26
  - @effect-agent/storage-memory@0.1.0-beta.26
  - @effect-agent/storage-sqlite@0.1.0-beta.26

## 0.1.0-beta.25

### Minor Changes

- [#140](https://github.com/danieljvdm/effect-agent/pull/140) [`eb9c5fd`](https://github.com/danieljvdm/effect-agent/commit/eb9c5fd4683a63807b131f8c8d94e9c1205bd36d) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Remove deprecated pull-request review outputs and aliases, legacy review-state decoding, and unused
  Travel Planner fixtures. Require Cloudflare worker bindings to use the per-incarnation callback.

### Patch Changes

- [#142](https://github.com/danieljvdm/effect-agent/pull/142) [`b6804dd`](https://github.com/danieljvdm/effect-agent/commit/b6804dd60cc83b569d0e87b88521952c20ba9b7d) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Harden deterministic chaos replay, MCP Schema decoding, and SQL fixture boundaries. Preserve concrete Effect failures inside chaos lanes and reject malformed seed configuration instead of partially parsing it.

- Updated dependencies [[`b6804dd`](https://github.com/danieljvdm/effect-agent/commit/b6804dd60cc83b569d0e87b88521952c20ba9b7d), [`b6804dd`](https://github.com/danieljvdm/effect-agent/commit/b6804dd60cc83b569d0e87b88521952c20ba9b7d), [`b6804dd`](https://github.com/danieljvdm/effect-agent/commit/b6804dd60cc83b569d0e87b88521952c20ba9b7d), [`b6804dd`](https://github.com/danieljvdm/effect-agent/commit/b6804dd60cc83b569d0e87b88521952c20ba9b7d), [`b6804dd`](https://github.com/danieljvdm/effect-agent/commit/b6804dd60cc83b569d0e87b88521952c20ba9b7d), [`b6804dd`](https://github.com/danieljvdm/effect-agent/commit/b6804dd60cc83b569d0e87b88521952c20ba9b7d)]:
  - @effect-agent/platform-node@0.1.0-beta.25
  - @effect-agent/engine@0.1.0-beta.25
  - @effect-agent/sandbox@0.1.0-beta.25
  - @effect-agent/session@0.1.0-beta.25
  - @effect-agent/storage-memory@0.1.0-beta.25
  - @effect-agent/storage-sqlite@0.1.0-beta.25
  - @effect-agent/capabilities@0.1.0-beta.25
  - @effect-agent/core@0.1.0-beta.25

## 0.1.0-beta.24

### Patch Changes

- Updated dependencies [[`6e3f56f`](https://github.com/danieljvdm/effect-agent/commit/6e3f56fbadd831372124578b027ea2bd5ff8f008)]:
  - @effect-agent/session@0.1.0-beta.24
  - @effect-agent/platform-node@0.1.0-beta.24
  - @effect-agent/storage-memory@0.1.0-beta.24
  - @effect-agent/storage-sqlite@0.1.0-beta.24
  - @effect-agent/capabilities@0.1.0-beta.24
  - @effect-agent/core@0.1.0-beta.24
  - @effect-agent/engine@0.1.0-beta.24
  - @effect-agent/sandbox@0.1.0-beta.24

## 0.1.0-beta.23

### Patch Changes

- Updated dependencies []:
  - @effect-agent/capabilities@0.1.0-beta.23
  - @effect-agent/core@0.1.0-beta.23
  - @effect-agent/engine@0.1.0-beta.23
  - @effect-agent/platform-node@0.1.0-beta.23
  - @effect-agent/sandbox@0.1.0-beta.23
  - @effect-agent/session@0.1.0-beta.23
  - @effect-agent/storage-memory@0.1.0-beta.23
  - @effect-agent/storage-sqlite@0.1.0-beta.23

## 0.1.0-beta.22

### Patch Changes

- [#122](https://github.com/danieljvdm/effect-agent/pull/122) [`1d88eed`](https://github.com/danieljvdm/effect-agent/commit/1d88eed54e1def3746d9010537d90f32db517a80) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Stabilize the encoded Travel Planner persistence fixture's declaration so repeated package builds
  produce the same release artifact.
- Updated dependencies [[`ce8b39c`](https://github.com/danieljvdm/effect-agent/commit/ce8b39ce8f716c0a11c6394d136b67cb9be84588)]:
  - @effect-agent/capabilities@0.1.0-beta.22
  - @effect-agent/core@0.1.0-beta.22
  - @effect-agent/engine@0.1.0-beta.22
  - @effect-agent/session@0.1.0-beta.22
  - @effect-agent/platform-node@0.1.0-beta.22
  - @effect-agent/storage-memory@0.1.0-beta.22
  - @effect-agent/storage-sqlite@0.1.0-beta.22
  - @effect-agent/sandbox@0.1.0-beta.22

## 0.1.0-beta.21

### Patch Changes

- Updated dependencies [[`27618dc`](https://github.com/danieljvdm/effect-agent/commit/27618dc03b0703fc784dc7abc4280fc74bb95045)]:
  - @effect-agent/capabilities@0.1.0-beta.21
  - @effect-agent/platform-node@0.1.0-beta.21
  - @effect-agent/core@0.1.0-beta.21
  - @effect-agent/engine@0.1.0-beta.21
  - @effect-agent/sandbox@0.1.0-beta.21
  - @effect-agent/session@0.1.0-beta.21
  - @effect-agent/storage-memory@0.1.0-beta.21
  - @effect-agent/storage-sqlite@0.1.0-beta.21

## 0.1.0-beta.20

### Patch Changes

- [#111](https://github.com/danieljvdm/effect-agent/pull/111) [`c715f9f`](https://github.com/danieljvdm/effect-agent/commit/c715f9f8e436fa85e8c1ef2b27f640e637ea52e4) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Align every public package with the Effect 4.0.0-rc.110 family.

- [#111](https://github.com/danieljvdm/effect-agent/pull/111) [`c715f9f`](https://github.com/danieljvdm/effect-agent/commit/c715f9f8e436fa85e8c1ef2b27f640e637ea52e4) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Fix `validateMcpDiscovery` reporting a permanent schema drift for MCP tools whose parameters or success type is a named, refined Schema (a branded ID, a bounded string, a `Schema.Class`) — both schema derivations now resolve a top-level `$ref` before comparison.

- Updated dependencies [[`7c093ec`](https://github.com/danieljvdm/effect-agent/commit/7c093ecfd900a0c55163fce76b0609d04434fa73), [`c715f9f`](https://github.com/danieljvdm/effect-agent/commit/c715f9f8e436fa85e8c1ef2b27f640e637ea52e4), [`c715f9f`](https://github.com/danieljvdm/effect-agent/commit/c715f9f8e436fa85e8c1ef2b27f640e637ea52e4)]:
  - @effect-agent/session@0.1.0-beta.20
  - @effect-agent/core@0.1.0-beta.20
  - @effect-agent/engine@0.1.0-beta.20
  - @effect-agent/capabilities@0.1.0-beta.20
  - @effect-agent/sandbox@0.1.0-beta.20
  - @effect-agent/storage-memory@0.1.0-beta.20
  - @effect-agent/storage-sqlite@0.1.0-beta.20
  - @effect-agent/platform-node@0.1.0-beta.20

## 0.1.0-beta.19

### Patch Changes

- Updated dependencies [[`9e31de4`](https://github.com/danieljvdm/effect-agent/commit/9e31de4c5f63ebc7eefbce33d3e0ed2052538f26), [`b8beef5`](https://github.com/danieljvdm/effect-agent/commit/b8beef5624f6704b0e52b5023babd1272d6b0603)]:
  - @effect-agent/engine@0.1.0-beta.19
  - @effect-agent/capabilities@0.1.0-beta.19
  - @effect-agent/session@0.1.0-beta.19
  - @effect-agent/storage-memory@0.1.0-beta.19
  - @effect-agent/storage-sqlite@0.1.0-beta.19
  - @effect-agent/platform-node@0.1.0-beta.19
  - @effect-agent/core@0.1.0-beta.19
  - @effect-agent/sandbox@0.1.0-beta.19

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
- Updated dependencies [[`f36fd40`](https://github.com/danieljvdm/effect-agent/commit/f36fd409f8a34e13c87646fd857a4060ac89e89d)]:
  - @effect-agent/engine@0.1.0-beta.18
  - @effect-agent/session@0.1.0-beta.18
  - @effect-agent/capabilities@0.1.0-beta.18
  - @effect-agent/platform-node@0.1.0-beta.18
  - @effect-agent/storage-memory@0.1.0-beta.18
  - @effect-agent/storage-sqlite@0.1.0-beta.18
  - @effect-agent/core@0.1.0-beta.18
  - @effect-agent/sandbox@0.1.0-beta.18

## 0.1.0-beta.17

### Patch Changes

- Updated dependencies [[`016df57`](https://github.com/danieljvdm/effect-agent/commit/016df574fa8c0f362468d848ae830d72532cbcaf)]:
  - @effect-agent/core@0.1.0-beta.17
  - @effect-agent/engine@0.1.0-beta.17
  - @effect-agent/session@0.1.0-beta.17
  - @effect-agent/capabilities@0.1.0-beta.17
  - @effect-agent/platform-node@0.1.0-beta.17
  - @effect-agent/storage-memory@0.1.0-beta.17
  - @effect-agent/storage-sqlite@0.1.0-beta.17
  - @effect-agent/sandbox@0.1.0-beta.17

## 0.1.0-beta.16

### Patch Changes

- Updated dependencies [[`e4b32b5`](https://github.com/danieljvdm/effect-agent/commit/e4b32b54061e58de57d5c27f06f8ef2a821ccb38)]:
  - @effect-agent/session@0.1.0-beta.16
  - @effect-agent/platform-node@0.1.0-beta.16
  - @effect-agent/storage-memory@0.1.0-beta.16
  - @effect-agent/storage-sqlite@0.1.0-beta.16
  - @effect-agent/capabilities@0.1.0-beta.16
  - @effect-agent/core@0.1.0-beta.16
  - @effect-agent/engine@0.1.0-beta.16
  - @effect-agent/sandbox@0.1.0-beta.16

## 0.1.0-beta.15

### Patch Changes

- Updated dependencies [[`38ac06e`](https://github.com/danieljvdm/effect-agent/commit/38ac06eea0956d7bef4576c5e527c6053f5a86f0)]:
  - @effect-agent/session@0.1.0-beta.15
  - @effect-agent/storage-memory@0.1.0-beta.15
  - @effect-agent/storage-sqlite@0.1.0-beta.15
  - @effect-agent/platform-node@0.1.0-beta.15
  - @effect-agent/capabilities@0.1.0-beta.15
  - @effect-agent/core@0.1.0-beta.15
  - @effect-agent/engine@0.1.0-beta.15
  - @effect-agent/sandbox@0.1.0-beta.15

## 0.1.0-beta.14

### Patch Changes

- Updated dependencies []:
  - @effect-agent/capabilities@0.1.0-beta.14
  - @effect-agent/core@0.1.0-beta.14
  - @effect-agent/engine@0.1.0-beta.14
  - @effect-agent/platform-node@0.1.0-beta.14
  - @effect-agent/sandbox@0.1.0-beta.14
  - @effect-agent/session@0.1.0-beta.14
  - @effect-agent/storage-memory@0.1.0-beta.14
  - @effect-agent/storage-sqlite@0.1.0-beta.14

## 0.1.0-beta.13

### Patch Changes

- Updated dependencies [[`68b48c9`](https://github.com/danieljvdm/effect-agent/commit/68b48c932b6a76d2c8ed0f04cc87c123a9fd11e4)]:
  - @effect-agent/core@0.1.0-beta.13
  - @effect-agent/session@0.1.0-beta.13
  - @effect-agent/capabilities@0.1.0-beta.13
  - @effect-agent/engine@0.1.0-beta.13
  - @effect-agent/platform-node@0.1.0-beta.13
  - @effect-agent/storage-memory@0.1.0-beta.13
  - @effect-agent/storage-sqlite@0.1.0-beta.13
  - @effect-agent/sandbox@0.1.0-beta.13

## 0.1.0-beta.12

### Patch Changes

- Updated dependencies []:
  - @effect-agent/capabilities@0.1.0-beta.12
  - @effect-agent/core@0.1.0-beta.12
  - @effect-agent/engine@0.1.0-beta.12
  - @effect-agent/platform-node@0.1.0-beta.12
  - @effect-agent/sandbox@0.1.0-beta.12
  - @effect-agent/session@0.1.0-beta.12
  - @effect-agent/storage-memory@0.1.0-beta.12
  - @effect-agent/storage-sqlite@0.1.0-beta.12

## 0.1.0-beta.11

### Patch Changes

- Updated dependencies []:
  - @effect-agent/capabilities@0.1.0-beta.11
  - @effect-agent/core@0.1.0-beta.11
  - @effect-agent/engine@0.1.0-beta.11
  - @effect-agent/platform-node@0.1.0-beta.11
  - @effect-agent/sandbox@0.1.0-beta.11
  - @effect-agent/session@0.1.0-beta.11
  - @effect-agent/storage-memory@0.1.0-beta.11
  - @effect-agent/storage-sqlite@0.1.0-beta.11

## 0.1.0-beta.10

### Patch Changes

- Updated dependencies []:
  - @effect-agent/capabilities@0.1.0-beta.10
  - @effect-agent/core@0.1.0-beta.10
  - @effect-agent/engine@0.1.0-beta.10
  - @effect-agent/platform-node@0.1.0-beta.10
  - @effect-agent/sandbox@0.1.0-beta.10
  - @effect-agent/session@0.1.0-beta.10
  - @effect-agent/storage-memory@0.1.0-beta.10
  - @effect-agent/storage-sqlite@0.1.0-beta.10

## 0.1.0-beta.9

### Patch Changes

- Updated dependencies [[`91ff50d`](https://github.com/danieljvdm/effect-agent/commit/91ff50df5480a0ccdfb8e0a00db39a1576e6c34b)]:
  - @effect-agent/core@0.1.0-beta.9
  - @effect-agent/engine@0.1.0-beta.9
  - @effect-agent/session@0.1.0-beta.9
  - @effect-agent/capabilities@0.1.0-beta.9
  - @effect-agent/platform-node@0.1.0-beta.9
  - @effect-agent/storage-memory@0.1.0-beta.9
  - @effect-agent/storage-sqlite@0.1.0-beta.9
  - @effect-agent/sandbox@0.1.0-beta.9

## 0.1.0-beta.8

### Patch Changes

- Updated dependencies []:
  - @effect-agent/capabilities@0.1.0-beta.8
  - @effect-agent/core@0.1.0-beta.8
  - @effect-agent/engine@0.1.0-beta.8
  - @effect-agent/platform-node@0.1.0-beta.8
  - @effect-agent/sandbox@0.1.0-beta.8
  - @effect-agent/session@0.1.0-beta.8
  - @effect-agent/storage-memory@0.1.0-beta.8
  - @effect-agent/storage-sqlite@0.1.0-beta.8

## 0.1.0-beta.7

### Minor Changes

- [#54](https://github.com/danieljvdm/effect-agent/pull/54) [`afe755a`](https://github.com/danieljvdm/effect-agent/commit/afe755a331172ffca9ceee7dd82bb452c6ccbb8a) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Context economics ([#54](https://github.com/danieljvdm/effect-agent/issues/54), RUN-022–027/CAP-017): application tool results are bounded by default (50 KiB
  `TruncatedToolResult` envelopes), budget accounting becomes cache-aware with last-call
  live-context tracking, every request can carry a derived run-status message, the token
  dimension joins the `onExhaustion` soft landing (RUN-018) with the `exhausted` dimension marker,
  and the engine compacts natively at the pre-Turn seam (prune, then one metered summarize)
  with a canonical `CompactionCreated` record that projections fold across Runs; provider
  context-length rejections compact-and-retry once, then fail typed (`ContextOverflowError`).

### Patch Changes

- Updated dependencies [[`5c49b78`](https://github.com/danieljvdm/effect-agent/commit/5c49b786604b3e8389cdc2c54d4f5cb284eac2b7), [`afe755a`](https://github.com/danieljvdm/effect-agent/commit/afe755a331172ffca9ceee7dd82bb452c6ccbb8a), [`b44ed77`](https://github.com/danieljvdm/effect-agent/commit/b44ed7771c3e1ace2516507b0b54d11e662f036c), [`3a44b5f`](https://github.com/danieljvdm/effect-agent/commit/3a44b5f6595f4070abb61c79d5b756a9f7ed20af)]:
  - @effect-agent/engine@0.1.0-beta.7
  - @effect-agent/capabilities@0.1.0-beta.7
  - @effect-agent/session@0.1.0-beta.7
  - @effect-agent/core@0.1.0-beta.7
  - @effect-agent/platform-node@0.1.0-beta.7
  - @effect-agent/storage-memory@0.1.0-beta.7
  - @effect-agent/storage-sqlite@0.1.0-beta.7
  - @effect-agent/sandbox@0.1.0-beta.7

## 0.1.0-beta.6

### Patch Changes

- Updated dependencies [[`e13ee6e`](https://github.com/danieljvdm/effect-agent/commit/e13ee6e7817549e99837d06e86caf2dea8656aa8), [`94c169a`](https://github.com/danieljvdm/effect-agent/commit/94c169a44a248972158ca955e33fb02dd5e55463)]:
  - @effect-agent/core@0.1.0-beta.6
  - @effect-agent/engine@0.1.0-beta.6
  - @effect-agent/session@0.1.0-beta.6
  - @effect-agent/capabilities@0.1.0-beta.6
  - @effect-agent/platform-node@0.1.0-beta.6
  - @effect-agent/storage-memory@0.1.0-beta.6
  - @effect-agent/storage-sqlite@0.1.0-beta.6
  - @effect-agent/sandbox@0.1.0-beta.6

## 0.0.1-beta.5

### Patch Changes

- [#19](https://github.com/danieljvdm/effect-agent/pull/19) [`a063031`](https://github.com/danieljvdm/effect-agent/commit/a063031c6b1f1637d947ae193a410b6bb9e8a9fc) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Align every public package with Effect 4.0.0-beta.107. Also expose per-incarnation Cloudflare
  Binding capture with live Durable Object context and derived identities, and prevent incomplete
  application Tool batches from a failed or aborted Run from poisoning prompts for later Runs.
- Updated dependencies [[`a063031`](https://github.com/danieljvdm/effect-agent/commit/a063031c6b1f1637d947ae193a410b6bb9e8a9fc)]:
  - @effect-agent/core@0.0.1-beta.5
  - @effect-agent/engine@0.0.1-beta.5
  - @effect-agent/capabilities@0.0.1-beta.5
  - @effect-agent/session@0.0.1-beta.5
  - @effect-agent/storage-memory@0.0.1-beta.5
  - @effect-agent/storage-sqlite@0.0.1-beta.5
  - @effect-agent/platform-node@0.0.1-beta.5

## 0.0.1-beta.4

### Patch Changes

- [#13](https://github.com/danieljvdm/effect-agent/pull/13) [`f4e3786`](https://github.com/danieljvdm/effect-agent/commit/f4e378635a794d4c17192ee3de011697ccec3a3b) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Introduce the `effect-agent` umbrella package: the framework's complete pure
  surface — schema-first authoring (core), the bounded interpreter (engine),
  and operational capabilities — as one dependency-clean root package,
  mirroring how `effect` fronts the `@effect/*` satellites. Platform adapters
  remain scoped. The umbrella is version-fixed to its three constituents.
- Updated dependencies [[`f4e3786`](https://github.com/danieljvdm/effect-agent/commit/f4e378635a794d4c17192ee3de011697ccec3a3b)]:
  - @effect-agent/core@0.0.1-beta.4
  - @effect-agent/engine@0.0.1-beta.4
  - @effect-agent/capabilities@0.0.1-beta.4
  - @effect-agent/session@0.0.1-beta.4
  - @effect-agent/storage-memory@0.0.1-beta.4
  - @effect-agent/storage-sqlite@0.0.1-beta.4
  - @effect-agent/platform-node@0.0.1-beta.4

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
  - @effect-agent/engine@0.0.1-beta.3
  - @effect-agent/capabilities@0.0.1-beta.3
  - @effect-agent/session@0.0.1-beta.3
  - @effect-agent/storage-memory@0.0.1-beta.3
  - @effect-agent/storage-sqlite@0.0.1-beta.3
  - @effect-agent/platform-node@0.0.1-beta.3

## 0.0.1-beta.1

### Patch Changes

- Republish with correctly pinned internal dependencies. The 0.0.1-beta.0
  artifacts depended on internal `@effect-agent/*` versions that were never
  published (`workspace:*` ranges were resolved from a stale lockfile at
  publish time); the release script now pins internal ranges to the exact
  workspace versions itself.
- Updated dependencies []:
  - @effect-agent/core@0.0.1-beta.1
  - @effect-agent/engine@0.0.1-beta.1
  - @effect-agent/capabilities@0.0.1-beta.1
  - @effect-agent/session@0.0.1-beta.1
  - @effect-agent/storage-memory@0.0.1-beta.1
  - @effect-agent/storage-sqlite@0.0.1-beta.1
  - @effect-agent/platform-node@0.0.1-beta.1

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
  - @effect-agent/engine@0.0.1-beta.0
  - @effect-agent/capabilities@0.0.1-beta.0
  - @effect-agent/session@0.0.1-beta.0
  - @effect-agent/storage-memory@0.0.1-beta.0
  - @effect-agent/storage-sqlite@0.0.1-beta.0
  - @effect-agent/platform-node@0.0.1-beta.0

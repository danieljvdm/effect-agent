# @effect-agent/platform-cloudflare

## 0.1.0-beta.41

### Minor Changes

- [#284](https://github.com/danieljvdm/effect-agent/pull/284) [`e21d6da`](https://github.com/danieljvdm/effect-agent/commit/e21d6da596b97c98ace533c3fa42fe9767d127e1) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add optional namespace-owned Cloudflare memory with bounded batch recall, authoritative semantic-candidate validation, and durable conditional writes shared across Threads. Limit semantic recall output with `maxOutputBytes`, counting repeated attribution and metadata.

  BEHAVIOR CHANGE: Construct access and document scopes with `MemoryScope.make` or decode them with its Schema; Cloudflare memory clients require the existing branded `Principal`, capped at 256 characters.

  BEHAVIOR CHANGE: Replace `recallMemory` with `Memory.recall` for multi-source composition, or use `client.recall(candidates, limits)` for a bound Cloudflare memory client. The old function is removed without an alias.

### Patch Changes

- Updated dependencies [[`e21d6da`](https://github.com/danieljvdm/effect-agent/commit/e21d6da596b97c98ace533c3fa42fe9767d127e1), [`edfa7dc`](https://github.com/danieljvdm/effect-agent/commit/edfa7dc6693dea2a84366f5053826ffa87f7c587)]:
  - @effect-agent/core@0.1.0-beta.41
  - @effect-agent/thread@0.1.0-beta.41
  - @effect-agent/storage-cloudflare@0.1.0-beta.41
  - @effect-agent/engine@0.1.0-beta.41
  - @effect-agent/sandbox@0.1.0-beta.41

## 0.1.0-beta.40

### Patch Changes

- [#280](https://github.com/danieljvdm/effect-agent/pull/280) [`614a81d`](https://github.com/danieljvdm/effect-agent/commit/614a81db4f9d121ac209cc56fc6d420f43f4ab1b) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Compose WebCapture tool handlers and a Worker browser binding with `CloudflareBrowser.layer(ReadPage, { browser: env.BROWSER })`. Pass an explicit `workersAi` authorization and accounting policy to enable structured extraction.

- [#280](https://github.com/danieljvdm/effect-agent/pull/280) [`614a81d`](https://github.com/danieljvdm/effect-agent/commit/614a81db4f9d121ac209cc56fc6d420f43f4ab1b) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Assemble Cloudflare Code Mode, REST capture tools, interactive browsers, and thread clients with platform-owned Layer constructors. Register Node agents through `NodeDurableHost.layerRegistered(registrations, options)` while preserving application service requirements and host-scoped cleanup.

- Updated dependencies [[`720e6d9`](https://github.com/danieljvdm/effect-agent/commit/720e6d952cf14cf61a6550c01473938fd46a1e74), [`018f1ad`](https://github.com/danieljvdm/effect-agent/commit/018f1ad8455a0075b9cf764f85fe9b6972f07eb7), [`720e6d9`](https://github.com/danieljvdm/effect-agent/commit/720e6d952cf14cf61a6550c01473938fd46a1e74), [`018f1ad`](https://github.com/danieljvdm/effect-agent/commit/018f1ad8455a0075b9cf764f85fe9b6972f07eb7), [`1432833`](https://github.com/danieljvdm/effect-agent/commit/14328336cd3480c5ddda8447f522591eb99eaaeb), [`c36fe73`](https://github.com/danieljvdm/effect-agent/commit/c36fe73d2d226f9271c6dd60071159b0d82862ae), [`018f1ad`](https://github.com/danieljvdm/effect-agent/commit/018f1ad8455a0075b9cf764f85fe9b6972f07eb7), [`018f1ad`](https://github.com/danieljvdm/effect-agent/commit/018f1ad8455a0075b9cf764f85fe9b6972f07eb7), [`0fbcbbf`](https://github.com/danieljvdm/effect-agent/commit/0fbcbbf3c8c2ca7595543e545baddb0c6f965436)]:
  - @effect-agent/thread@0.1.0-beta.40
  - @effect-agent/core@0.1.0-beta.40
  - @effect-agent/engine@0.1.0-beta.40
  - @effect-agent/storage-cloudflare@0.1.0-beta.40
  - @effect-agent/sandbox@0.1.0-beta.40

## 0.1.0-beta.39

### Minor Changes

- [#241](https://github.com/danieljvdm/effect-agent/pull/241) [`dd85dc0`](https://github.com/danieljvdm/effect-agent/commit/dd85dc07e2513e2ec56316fd7609e137d6c3f6fa) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add durable once and continuous event subscriptions that deliver Schema-defined input through ordinary Thread admission. Provide owner-scoped management Tools and a GitHub workflow run completion source with missed-webhook reconciliation.

  BEHAVIOR CHANGE: Reset incompatible private-development SQLite databases before opening them with storage version 6.

- [#249](https://github.com/danieljvdm/effect-agent/pull/249) [`f8de2d8`](https://github.com/danieljvdm/effect-agent/commit/f8de2d8a022e81eac9c357b361dd567fb65ac239) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Import specialized testing utilities and fixtures from their documented subpaths, and use failpoint controls from `/testing` with `TestControl.layer` in place of `Failpoint.layerTest`; keep migration loaders internal.
  Import Browser Run adapters from their dedicated Cloudflare subpaths and install `@cloudflare/puppeteer` explicitly when using `/interactive-browser`.

- [#263](https://github.com/danieljvdm/effect-agent/pull/263) [`95865d7`](https://github.com/danieljvdm/effect-agent/commit/95865d78f55546d42f562f2f13509bbfc198c091) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Rename `@effect-agent/session` to `@effect-agent/thread` and rename the Conversation framework API to Thread.

  BEHAVIOR CHANGE: Rename Conversation identifiers, fields, record families and tags, and the durable-admin `--conversation` selector to their Thread equivalents. Reset incompatible alpha storage before upgrading.

- [#241](https://github.com/danieljvdm/effect-agent/pull/241) [`dd85dc0`](https://github.com/danieljvdm/effect-agent/commit/dd85dc07e2513e2ec56316fd7609e137d6c3f6fa) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Bind subscription input preparation to each destination Agent's retained definition version, authorize reconciliation through explicit host policy, and preserve newer delivery retry state.

  BEHAVIOR CHANGE: Provide `SubscriptionInputBindings` and `SubscriptionAuthorizer.reconcile` in subscription hosts, and import GitHub integration from `@effect-agent/thread/github`.

### Patch Changes

- [#260](https://github.com/danieljvdm/effect-agent/pull/260) [`e6d05f5`](https://github.com/danieljvdm/effect-agent/commit/e6d05f51783035cec4f99247de2f064e730770ca) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Compose Cloudflare Thread Objects from application Layers and typed Agent version declarations, preserving initialization failures and scoped dependencies. Resolve durable work from explicit exact-version bindings and reject digest-transparent registrations.

  BEHAVIOR CHANGE: Replace `makeConversationObjectClass` with `ThreadObject.make`. Pass a composed `ThreadObject.layer(registrations)` to `ThreadObject.make`, move preparation and Tool authorization into Layers, and use `options.eventLayer` for observability. Pass bindings directly to resolved worker methods and `NodeDurableHost.layer(bindings)` instead of providing `AgentBindingResolver`.

- [#262](https://github.com/danieljvdm/effect-agent/pull/262) [`34ca82e`](https://github.com/danieljvdm/effect-agent/commit/34ca82e86191bc85229bd32886b8cfaf9a2edce9) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Enforce durable child tool-call allowances across recovery and distinguish passing checks from complete adapter certification. Rename the custom durable assembly to `layerWithServices` and preserve Node extension-layer construction errors and dependencies.

  BEHAVIOR CHANGE: Replace `DurableAgentRuntime.layerWithContext` with `layerWithServices`, still supplying both separate services. Regenerate certification reports with the `effect-agent/certification@2` schema and use `fullyCertified` for gates requiring executed real-loss checks; `ok` retains its executed-check meaning. Existing child records without an allowance keep their original definition policy; start a new delegation to apply a limit.

- [#248](https://github.com/danieljvdm/effect-agent/pull/248) [`f4f37c3`](https://github.com/danieljvdm/effect-agent/commit/f4f37c37fa1b650341c6e18ee3a22cd6f518bfd2) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Compose prompt preparation and Tool authorization independently in durable hosts, preserving both across recovery.

  BEHAVIOR CHANGE: move `RunContextPreparation.toolAuthorization` to a separate `RunToolAuthorization` Layer and provide both services to `DurableAgentRuntime.layerWithServices`.

- [#243](https://github.com/danieljvdm/effect-agent/pull/243) [`e0aa7d9`](https://github.com/danieljvdm/effect-agent/commit/e0aa7d9442ca2ec62df8195a2f9cce7b52af5257) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Preserve Run limits across durable recovery, require explicit delegation replay authority, and reject unusable compaction summaries. Authorize settlement waits and aborts through the runtime authorizer and reject settlement Receipts whose Submission belongs to another Thread.

  BEHAVIOR CHANGE: Reset private-development histories whose RunStarted records predate policy accounting version 1 before resuming them.

- [#256](https://github.com/danieljvdm/effect-agent/pull/256) [`ac70e21`](https://github.com/danieljvdm/effect-agent/commit/ac70e212c7d9741ce48bd9b2a4dbd355f9dac72e) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Declare `effect` as a required `^4.0.0-rc.111` peer across all public packages so they share the application's runtime and accept compatible upgrades. Keep `effect` in application dependencies at a version satisfying the framework's and providers' peer ranges.

- Updated dependencies [[`e6d05f5`](https://github.com/danieljvdm/effect-agent/commit/e6d05f51783035cec4f99247de2f064e730770ca), [`34ca82e`](https://github.com/danieljvdm/effect-agent/commit/34ca82e86191bc85229bd32886b8cfaf9a2edce9), [`dd85dc0`](https://github.com/danieljvdm/effect-agent/commit/dd85dc07e2513e2ec56316fd7609e137d6c3f6fa), [`e0aa7d9`](https://github.com/danieljvdm/effect-agent/commit/e0aa7d9442ca2ec62df8195a2f9cce7b52af5257), [`f4f37c3`](https://github.com/danieljvdm/effect-agent/commit/f4f37c37fa1b650341c6e18ee3a22cd6f518bfd2), [`e0aa7d9`](https://github.com/danieljvdm/effect-agent/commit/e0aa7d9442ca2ec62df8195a2f9cce7b52af5257), [`7bab6c0`](https://github.com/danieljvdm/effect-agent/commit/7bab6c053b01398a0f1898374103997da6550268), [`f8de2d8`](https://github.com/danieljvdm/effect-agent/commit/f8de2d8a022e81eac9c357b361dd567fb65ac239), [`0d88d90`](https://github.com/danieljvdm/effect-agent/commit/0d88d90443e7d35e34799f4458d274fde99e0859), [`79fbd8b`](https://github.com/danieljvdm/effect-agent/commit/79fbd8b755434a162629a534478e188636d186fe), [`4c458e4`](https://github.com/danieljvdm/effect-agent/commit/4c458e43738bb243d1e343c97ecfd49e3b41ca9f), [`95865d7`](https://github.com/danieljvdm/effect-agent/commit/95865d78f55546d42f562f2f13509bbfc198c091), [`655bf5f`](https://github.com/danieljvdm/effect-agent/commit/655bf5f217dce1865c97ce613246c27846bfaf8a), [`d004a36`](https://github.com/danieljvdm/effect-agent/commit/d004a361518c23cdc81f1768e5ab31560e014935), [`ac70e21`](https://github.com/danieljvdm/effect-agent/commit/ac70e212c7d9741ce48bd9b2a4dbd355f9dac72e), [`dd85dc0`](https://github.com/danieljvdm/effect-agent/commit/dd85dc07e2513e2ec56316fd7609e137d6c3f6fa), [`dd85dc0`](https://github.com/danieljvdm/effect-agent/commit/dd85dc07e2513e2ec56316fd7609e137d6c3f6fa), [`511c852`](https://github.com/danieljvdm/effect-agent/commit/511c85212a564ff2729de401620fcbdeddcb4748)]:
  - @effect-agent/thread@0.1.0-beta.39
  - @effect-agent/engine@0.1.0-beta.39
  - @effect-agent/storage-cloudflare@0.1.0-beta.39
  - @effect-agent/core@0.1.0-beta.39
  - @effect-agent/sandbox@0.1.0-beta.39

## 0.1.0-beta.38

### Patch Changes

- [#222](https://github.com/danieljvdm/effect-agent/pull/222) [`22ee09d`](https://github.com/danieljvdm/effect-agent/commit/22ee09d279bf0561ef7eb96e3f80f4b29481a71c) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Confirm exact Browser Run session termination before reporting cleanup success, including already-absent sessions. Provide `BrowserRunSessionLifecycle.layer({ accountId, apiToken })` with an account-scoped Browser Rendering Write token when constructing the interactive binding.

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.38
  - @effect-agent/engine@0.1.0-beta.38
  - @effect-agent/sandbox@0.1.0-beta.38
  - @effect-agent/session@0.1.0-beta.38
  - @effect-agent/storage-cloudflare@0.1.0-beta.38

## 0.1.0-beta.37

### Minor Changes

- [#212](https://github.com/danieljvdm/effect-agent/pull/212) [`242b601`](https://github.com/danieljvdm/effect-agent/commit/242b601c6d14c3448c2a3acdc28b97b48e27cf92) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add durable schedules for typed Agent input with owner authorization, one-shot, interval and cron timing, and recovery through ordinary Submission admission on Node and Cloudflare.

  BEHAVIOR CHANGE: Reset older private-development SQLite databases for storage version 5, and provide `effect-cf ^0.37.0` to Cloudflare hosts.

- [#219](https://github.com/danieljvdm/effect-agent/pull/219) [`d713c28`](https://github.com/danieljvdm/effect-agent/commit/d713c28f5b7537e133e063e5df0207ecd4046856) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Expose a validated launch viewport and host-only session resizing without spending the agent action budget.

  BEHAVIOR CHANGE: Handle `InteractiveBrowserPolicyDeniedError` when building `BrowserRunInteractiveBinding.layer` with invalid viewport configuration.

- [#218](https://github.com/danieljvdm/effect-agent/pull/218) [`b43cf38`](https://github.com/danieljvdm/effect-agent/commit/b43cf38093f716cefc998241183ca2059ee83fe0) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Separate scheduling management from driver authority, expose explicit public status, and fix DST delivery, failed-record starvation, and repeated resume. Allow positive host interval minimums and release operational capacity when schedules finish while retaining creation replay guarantees.

  BEHAVIOR CHANGE: Cloudflare consumers yield `Scheduling` from `CloudflareSchedulingClient.layer`; local drivers use `ScheduleDriver.layer`. Status omits persisted input and admission internals, and `dueBatchSize` bounds a query page within a sweep.

### Patch Changes

- [#220](https://github.com/danieljvdm/effect-agent/pull/220) [`5f83df4`](https://github.com/danieljvdm/effect-agent/commit/5f83df46d392b1d61e39cb2c74d9eebf36c52415) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Expose bounded JSON page text and form-control observations, including native option selection, and wait for DOM-ready navigation with a 30-second timeout. Require unique click/fill targets, export `isBrowserRunUndispatchedActionError`, and observe bounded fetch/XHR settlement while preserving uncertainty and cleanup on interruption.

- Updated dependencies [[`242b601`](https://github.com/danieljvdm/effect-agent/commit/242b601c6d14c3448c2a3acdc28b97b48e27cf92), [`bd48a7b`](https://github.com/danieljvdm/effect-agent/commit/bd48a7b200fb71335b19edd7941be331b6ede9ea), [`bd48a7b`](https://github.com/danieljvdm/effect-agent/commit/bd48a7b200fb71335b19edd7941be331b6ede9ea), [`b43cf38`](https://github.com/danieljvdm/effect-agent/commit/b43cf38093f716cefc998241183ca2059ee83fe0)]:
  - @effect-agent/session@0.1.0-beta.37
  - @effect-agent/storage-cloudflare@0.1.0-beta.37
  - @effect-agent/engine@0.1.0-beta.37
  - @effect-agent/core@0.1.0-beta.37
  - @effect-agent/sandbox@0.1.0-beta.37

## 0.1.0-beta.36

### Patch Changes

- [#214](https://github.com/danieljvdm/effect-agent/pull/214) [`082c258`](https://github.com/danieljvdm/effect-agent/commit/082c2584573c1ffbfa7d5b7166f4243e996816eb) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Allow durably authorized aborts to settle unknown submissions and release queued followers without replaying uncertain tools. Quiesce Cloudflare maintenance for ready followers behind an unresolved external wait.

- Updated dependencies [[`082c258`](https://github.com/danieljvdm/effect-agent/commit/082c2584573c1ffbfa7d5b7166f4243e996816eb)]:
  - @effect-agent/session@0.1.0-beta.36
  - @effect-agent/storage-cloudflare@0.1.0-beta.36
  - @effect-agent/core@0.1.0-beta.36
  - @effect-agent/engine@0.1.0-beta.36
  - @effect-agent/sandbox@0.1.0-beta.36

## 0.1.0-beta.35

### Patch Changes

- [#208](https://github.com/danieljvdm/effect-agent/pull/208) [`065c455`](https://github.com/danieljvdm/effect-agent/commit/065c455d1277f73157f610429de283f41ec83d9c) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add an explicit interactive browser network policy and reject `PublicWeb` with a typed unsupported error before Cloudflare launches a browser.

  BEHAVIOR CHANGE: Move `allowedHosts` into `network: { _tag: "ExactHosts", allowedHosts }` for existing page-request allowlist workflows; `PublicWeb` remains unsupported on Cloudflare.

- [#210](https://github.com/danieljvdm/effect-agent/pull/210) [`06d4f88`](https://github.com/danieljvdm/effect-agent/commit/06d4f88c78ad175bb7e4106d53e01a2c6076ebdc) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add an explicit `Unrestricted` interactive browser policy for arbitrary-site browsing without URL/host or private-network containment guarantees, retaining session limits and host controls. Admit credential-free HTTP and HTTPS interactive navigation and URL observations without changing PageCapture contracts.

- Updated dependencies [[`065c455`](https://github.com/danieljvdm/effect-agent/commit/065c455d1277f73157f610429de283f41ec83d9c), [`06d4f88`](https://github.com/danieljvdm/effect-agent/commit/06d4f88c78ad175bb7e4106d53e01a2c6076ebdc)]:
  - @effect-agent/sandbox@0.1.0-beta.35
  - @effect-agent/storage-cloudflare@0.1.0-beta.35
  - @effect-agent/core@0.1.0-beta.35
  - @effect-agent/engine@0.1.0-beta.35
  - @effect-agent/session@0.1.0-beta.35

## 0.1.0-beta.34

### Minor Changes

- [#205](https://github.com/danieljvdm/effect-agent/pull/205) [`baecd08`](https://github.com/danieljvdm/effect-agent/commit/baecd08f1d6f2c0698e16487cdcccf2f6ffcebca) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add opt-in native Conversation RPC tracing with binding/method client spans, transient current-span propagation, and typed receiver invocation hooks. Remove routine storage codec, failpoint-wrapper, and engine identifier-helper spans while preserving validation, failures, and I/O tracing.

  BEHAVIOR CHANGE: Upgrade the host's `effect-cf` dependency to `^0.34.0` for the native tracing contract.

- [#206](https://github.com/danieljvdm/effect-agent/pull/206) [`aa3ebfb`](https://github.com/danieljvdm/effect-agent/commit/aa3ebfb4fd1e69be77c433a881ddecb3567c36c2) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Expose non-propagating Tool failures to an opt-in trusted local observer, preserving live Causes without automatic export. Install the same observer through durable Node and Cloudflare runtime options while excluding settled-call replay.

### Patch Changes

- [#202](https://github.com/danieljvdm/effect-agent/pull/202) [`cf4a8d9`](https://github.com/danieljvdm/effect-agent/commit/cf4a8d9c645d5d8a2e552f4bb4902af4253d91ee) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Align the Effect family with rc.111 to decode nested OpenAI error events, and preserve transformed Tool parameters under its encoded response contract.

- Updated dependencies [[`cf4a8d9`](https://github.com/danieljvdm/effect-agent/commit/cf4a8d9c645d5d8a2e552f4bb4902af4253d91ee), [`baecd08`](https://github.com/danieljvdm/effect-agent/commit/baecd08f1d6f2c0698e16487cdcccf2f6ffcebca), [`cf4a8d9`](https://github.com/danieljvdm/effect-agent/commit/cf4a8d9c645d5d8a2e552f4bb4902af4253d91ee), [`baecd08`](https://github.com/danieljvdm/effect-agent/commit/baecd08f1d6f2c0698e16487cdcccf2f6ffcebca), [`aa3ebfb`](https://github.com/danieljvdm/effect-agent/commit/aa3ebfb4fd1e69be77c433a881ddecb3567c36c2)]:
  - @effect-agent/engine@0.1.0-beta.34
  - @effect-agent/session@0.1.0-beta.34
  - @effect-agent/core@0.1.0-beta.34
  - @effect-agent/sandbox@0.1.0-beta.34
  - @effect-agent/storage-cloudflare@0.1.0-beta.34

## 0.1.0-beta.33

### Patch Changes

- [#196](https://github.com/danieljvdm/effect-agent/pull/196) [`2aa8713`](https://github.com/danieljvdm/effect-agent/commit/2aa8713d943e20faedfae029551b6faa2f8b08d4) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Update controlled React form state when filling interactive browser fields.

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.33
  - @effect-agent/engine@0.1.0-beta.33
  - @effect-agent/sandbox@0.1.0-beta.33
  - @effect-agent/session@0.1.0-beta.33
  - @effect-agent/storage-cloudflare@0.1.0-beta.33

## 0.1.0-beta.32

### Minor Changes

- [#194](https://github.com/danieljvdm/effect-agent/pull/194) [`7592ded`](https://github.com/danieljvdm/effect-agent/commit/7592deda757e0eeb0243f86bae9c2b15623e3c76) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add same-session PNG screenshots, viewport scrolling, and explicit closure to interactive browser handles. Expose host-only Cloudflare Live View, handoff, and cleanup through redacted session identities.

  BEHAVIOR CHANGE: Custom browser adapters must implement `screenshot`, `scroll`, and the `close` Effect.

### Patch Changes

- Updated dependencies [[`7592ded`](https://github.com/danieljvdm/effect-agent/commit/7592deda757e0eeb0243f86bae9c2b15623e3c76)]:
  - @effect-agent/sandbox@0.1.0-beta.32
  - @effect-agent/storage-cloudflare@0.1.0-beta.32
  - @effect-agent/core@0.1.0-beta.32
  - @effect-agent/engine@0.1.0-beta.32
  - @effect-agent/session@0.1.0-beta.32

## 0.1.0-beta.31

### Patch Changes

- [#183](https://github.com/danieljvdm/effect-agent/pull/183) [`d3c42d4`](https://github.com/danieljvdm/effect-agent/commit/d3c42d4e34f27610845863ec29908cd3fce95188) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add bounded selector scrape to `PageCapture`, `WebCapture.makeScrape`, and the Cloudflare binding and REST adapters.

- Updated dependencies [[`d3c42d4`](https://github.com/danieljvdm/effect-agent/commit/d3c42d4e34f27610845863ec29908cd3fce95188)]:
  - @effect-agent/sandbox@0.1.0-beta.31
  - @effect-agent/storage-cloudflare@0.1.0-beta.31
  - @effect-agent/core@0.1.0-beta.31
  - @effect-agent/engine@0.1.0-beta.31
  - @effect-agent/session@0.1.0-beta.31

## 0.1.0-beta.30

### Patch Changes

- [#172](https://github.com/danieljvdm/effect-agent/pull/172) [`d123424`](https://github.com/danieljvdm/effect-agent/commit/d123424be7679cfe1b8d133d0d2aa1497e087590) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add a bounded `PageScreenshot` PNG port and the native Cloudflare Browser Run Quick Action Layer.
  Screenshot bytes remain caller-owned and are never persisted or projected by the framework.

- [#172](https://github.com/danieljvdm/effect-agent/pull/172) [`d123424`](https://github.com/danieljvdm/effect-agent/commit/d123424be7679cfe1b8d133d0d2aa1497e087590) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add a Node-safe Cloudflare Browser Run REST PageCapture Layer with explicit Chromium and Kitesurf selection.

- [#172](https://github.com/danieljvdm/effect-agent/pull/172) [`d123424`](https://github.com/danieljvdm/effect-agent/commit/d123424be7679cfe1b8d133d0d2aa1497e087590) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add a bounded same-host `PageCrawl` stream and a Cloudflare Browser Run REST adapter with scoped
  remote-job cleanup.

- [#172](https://github.com/danieljvdm/effect-agent/pull/172) [`d123424`](https://github.com/danieljvdm/effect-agent/commit/d123424be7679cfe1b8d133d0d2aa1497e087590) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add a scoped, provider-neutral `InteractiveBrowser` contract for bounded navigation and interaction, with typed busy, limit, capacity, expiry, and uncertain-execution semantics.

  Document the Cloudflare Browser Run Puppeteer adapter boundary and opt-in Worker proof requirements.

- Updated dependencies [[`d123424`](https://github.com/danieljvdm/effect-agent/commit/d123424be7679cfe1b8d133d0d2aa1497e087590), [`d123424`](https://github.com/danieljvdm/effect-agent/commit/d123424be7679cfe1b8d133d0d2aa1497e087590), [`d123424`](https://github.com/danieljvdm/effect-agent/commit/d123424be7679cfe1b8d133d0d2aa1497e087590)]:
  - @effect-agent/sandbox@0.1.0-beta.30
  - @effect-agent/storage-cloudflare@0.1.0-beta.30
  - @effect-agent/core@0.1.0-beta.30
  - @effect-agent/engine@0.1.0-beta.30
  - @effect-agent/session@0.1.0-beta.30

## 0.1.0-beta.29

### Patch Changes

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.29
  - @effect-agent/engine@0.1.0-beta.29
  - @effect-agent/sandbox@0.1.0-beta.29
  - @effect-agent/session@0.1.0-beta.29
  - @effect-agent/storage-cloudflare@0.1.0-beta.29

## 0.1.0-beta.28

### Patch Changes

- Updated dependencies [[`374771d`](https://github.com/danieljvdm/effect-agent/commit/374771d90afa26ce7e1832f76715aa7b9eea3741)]:
  - @effect-agent/engine@0.1.0-beta.28
  - @effect-agent/session@0.1.0-beta.28
  - @effect-agent/storage-cloudflare@0.1.0-beta.28
  - @effect-agent/core@0.1.0-beta.28
  - @effect-agent/sandbox@0.1.0-beta.28

## 0.1.0-beta.27

### Minor Changes

- [#155](https://github.com/danieljvdm/effect-agent/pull/155) [`773264b`](https://github.com/danieljvdm/effect-agent/commit/773264b75759c4456e1e549d2172bbe39610a8c1) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add crash-safe terminal delivery Tools and final model responses, completion-capacity reservation, Run-scoped prompt provenance, and target-aware compaction.
  Persist priced per-call model usage in the DN and DC assemblies and expose aggregate usage on Run settlements.

### Patch Changes

- [#148](https://github.com/danieljvdm/effect-agent/pull/148) [`47e9a53`](https://github.com/danieljvdm/effect-agent/commit/47e9a53d99555af3b0ac993b5c9c55ad266e327b) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add the schema-first `PageCapture` port and conservative `WebCapture.make`/`WebCapture.makeExtract` Tools over an immutable, deny-by-default browser-request allowlist. Add native `BrowserRun` Quick Action Layers with bounded response streaming and a typed Workers AI authorization and accounting failure for structured extraction.

  ```ts
  const readDocs = WebCapture.make("read_webpage", {
    description: "Read documentation pages.",
    urls: ["docs.example.com", "*.effect.website"],
  });
  // worker: browserQuickActionCaptureLayer().pipe(
  //   Layer.provide(BrowserQuickActionBrowserBinding.layer({ browser: env.BROWSER })),
  // )
  ```

- Updated dependencies [[`47e9a53`](https://github.com/danieljvdm/effect-agent/commit/47e9a53d99555af3b0ac993b5c9c55ad266e327b), [`773264b`](https://github.com/danieljvdm/effect-agent/commit/773264b75759c4456e1e549d2172bbe39610a8c1)]:
  - @effect-agent/sandbox@0.1.0-beta.27
  - @effect-agent/core@0.1.0-beta.27
  - @effect-agent/engine@0.1.0-beta.27
  - @effect-agent/session@0.1.0-beta.27
  - @effect-agent/storage-cloudflare@0.1.0-beta.27

## 0.1.0-beta.26

### Patch Changes

- [#146](https://github.com/danieljvdm/effect-agent/pull/146) [`02311ad`](https://github.com/danieljvdm/effect-agent/commit/02311ad49b6982a15525b8be3f9252536a77be8a) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Route Dynamic Worker host calls through a pass-scoped RPC target owned by the caller's event context.
  Remove the application-provided Code Mode host entrypoint binding.
- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.26
  - @effect-agent/engine@0.1.0-beta.26
  - @effect-agent/sandbox@0.1.0-beta.26
  - @effect-agent/session@0.1.0-beta.26
  - @effect-agent/storage-cloudflare@0.1.0-beta.26

## 0.1.0-beta.25

### Minor Changes

- [#140](https://github.com/danieljvdm/effect-agent/pull/140) [`eb9c5fd`](https://github.com/danieljvdm/effect-agent/commit/eb9c5fd4683a63807b131f8c8d94e9c1205bd36d) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Remove deprecated pull-request review outputs and aliases, legacy review-state decoding, and unused
  Travel Planner fixtures. Require Cloudflare worker bindings to use the per-incarnation callback.

### Patch Changes

- [#142](https://github.com/danieljvdm/effect-agent/pull/142) [`b6804dd`](https://github.com/danieljvdm/effect-agent/commit/b6804dd60cc83b569d0e87b88521952c20ba9b7d) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Validate Node runtime configuration before opening SQLite. Keep Cloudflare RPC failures typed when foreign diagnostics are hostile, and close both Dynamic Worker RPC handles when a pass ends.

- Updated dependencies [[`b6804dd`](https://github.com/danieljvdm/effect-agent/commit/b6804dd60cc83b569d0e87b88521952c20ba9b7d), [`b6804dd`](https://github.com/danieljvdm/effect-agent/commit/b6804dd60cc83b569d0e87b88521952c20ba9b7d), [`b6804dd`](https://github.com/danieljvdm/effect-agent/commit/b6804dd60cc83b569d0e87b88521952c20ba9b7d), [`b6804dd`](https://github.com/danieljvdm/effect-agent/commit/b6804dd60cc83b569d0e87b88521952c20ba9b7d), [`b6804dd`](https://github.com/danieljvdm/effect-agent/commit/b6804dd60cc83b569d0e87b88521952c20ba9b7d)]:
  - @effect-agent/engine@0.1.0-beta.25
  - @effect-agent/sandbox@0.1.0-beta.25
  - @effect-agent/session@0.1.0-beta.25
  - @effect-agent/storage-cloudflare@0.1.0-beta.25
  - @effect-agent/core@0.1.0-beta.25

## 0.1.0-beta.24

### Patch Changes

- Updated dependencies [[`6e3f56f`](https://github.com/danieljvdm/effect-agent/commit/6e3f56fbadd831372124578b027ea2bd5ff8f008)]:
  - @effect-agent/session@0.1.0-beta.24
  - @effect-agent/storage-cloudflare@0.1.0-beta.24
  - @effect-agent/core@0.1.0-beta.24
  - @effect-agent/engine@0.1.0-beta.24
  - @effect-agent/sandbox@0.1.0-beta.24

## 0.1.0-beta.23

### Patch Changes

- [#133](https://github.com/danieljvdm/effect-agent/pull/133) [`b130876`](https://github.com/danieljvdm/effect-agent/commit/b130876477ff39349e0d8249298cc0589d284540) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Run Dynamic Worker Code Mode host calls on a Scope-owned pass fiber so they inherit the `execute` Context and die with the pass.

  BEHAVIOR CHANGE: `CodeExecutionHost.call` now sees services provided to `execute` instead of Effect defaults from a `runFork` root.

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.23
  - @effect-agent/engine@0.1.0-beta.23
  - @effect-agent/sandbox@0.1.0-beta.23
  - @effect-agent/session@0.1.0-beta.23
  - @effect-agent/storage-cloudflare@0.1.0-beta.23

## 0.1.0-beta.22

### Minor Changes

- [#124](https://github.com/danieljvdm/effect-agent/pull/124) [`ce8b39c`](https://github.com/danieljvdm/effect-agent/commit/ce8b39ce8f716c0a11c6394d136b67cb9be84588) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Authorize every still-executable model-declared call in a fresh or resumed application Tool batch
  through a host-supplied Run option before durable preparation or Handler execution. Settle denied
  accepted work with a typed failure while preserving canonical Run, Turn, input, and Tool Call
  identity across recovery.

### Patch Changes

- Updated dependencies [[`ce8b39c`](https://github.com/danieljvdm/effect-agent/commit/ce8b39ce8f716c0a11c6394d136b67cb9be84588)]:
  - @effect-agent/core@0.1.0-beta.22
  - @effect-agent/engine@0.1.0-beta.22
  - @effect-agent/session@0.1.0-beta.22
  - @effect-agent/storage-cloudflare@0.1.0-beta.22
  - @effect-agent/sandbox@0.1.0-beta.22

## 0.1.0-beta.21

### Patch Changes

- Updated dependencies []:
  - @effect-agent/storage-cloudflare@0.1.0-beta.21
  - @effect-agent/core@0.1.0-beta.21
  - @effect-agent/engine@0.1.0-beta.21
  - @effect-agent/sandbox@0.1.0-beta.21
  - @effect-agent/session@0.1.0-beta.21

## 0.1.0-beta.20

### Patch Changes

- [#116](https://github.com/danieljvdm/effect-agent/pull/116) [`11a6562`](https://github.com/danieljvdm/effect-agent/commit/11a65620f330736e92931f100618c797437b0ca4) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Stop requesting the `allowExperimental` Worker Loader option for Code Mode dynamic workers — it made every pass fail to load unless the calling worker had the `experimental` compatibility flag, which deployed Workers cannot set.

- [#111](https://github.com/danieljvdm/effect-agent/pull/111) [`c715f9f`](https://github.com/danieljvdm/effect-agent/commit/c715f9f8e436fa85e8c1ef2b27f640e637ea52e4) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Align every public package with the Effect 4.0.0-rc.110 family.

- [#111](https://github.com/danieljvdm/effect-agent/pull/111) [`c715f9f`](https://github.com/danieljvdm/effect-agent/commit/c715f9f8e436fa85e8c1ef2b27f640e637ea52e4) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Fix `validateMcpDiscovery` reporting a permanent schema drift for MCP tools whose parameters or success type is a named, refined Schema (a branded ID, a bounded string, a `Schema.Class`) — both schema derivations now resolve a top-level `$ref` before comparison.

- Updated dependencies [[`7c093ec`](https://github.com/danieljvdm/effect-agent/commit/7c093ecfd900a0c55163fce76b0609d04434fa73), [`c715f9f`](https://github.com/danieljvdm/effect-agent/commit/c715f9f8e436fa85e8c1ef2b27f640e637ea52e4), [`c715f9f`](https://github.com/danieljvdm/effect-agent/commit/c715f9f8e436fa85e8c1ef2b27f640e637ea52e4)]:
  - @effect-agent/session@0.1.0-beta.20
  - @effect-agent/core@0.1.0-beta.20
  - @effect-agent/engine@0.1.0-beta.20
  - @effect-agent/sandbox@0.1.0-beta.20
  - @effect-agent/storage-cloudflare@0.1.0-beta.20

## 0.1.0-beta.19

### Minor Changes

- [#106](https://github.com/danieljvdm/effect-agent/pull/106) [`9e31de4`](https://github.com/danieljvdm/effect-agent/commit/9e31de4c5f63ebc7eefbce33d3e0ed2052538f26) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Expose host-supplied model-context preparation through Cloudflare Conversation Object options
  ([#49](https://github.com/danieljvdm/effect-agent/issues/49)). A generic scoped `RunContextPreparation` service now composes after canonical durable
  resume reconstruction, `contextCompactorRunContextLayer` adapts the digest-bound
  `ContextCompactor` capability with typed failures, and `CloudflareDurableRuntimeOptions.runContext`
  accepts a closed Layer or per-incarnation Layer factory. Compaction changes only model-visible
  context; canonical history remains recoverable across Durable Object eviction and retries.

### Patch Changes

- Updated dependencies [[`9e31de4`](https://github.com/danieljvdm/effect-agent/commit/9e31de4c5f63ebc7eefbce33d3e0ed2052538f26), [`b8beef5`](https://github.com/danieljvdm/effect-agent/commit/b8beef5624f6704b0e52b5023babd1272d6b0603)]:
  - @effect-agent/engine@0.1.0-beta.19
  - @effect-agent/session@0.1.0-beta.19
  - @effect-agent/storage-cloudflare@0.1.0-beta.19
  - @effect-agent/core@0.1.0-beta.19
  - @effect-agent/sandbox@0.1.0-beta.19

## 0.1.0-beta.18

### Patch Changes

- Updated dependencies [[`f36fd40`](https://github.com/danieljvdm/effect-agent/commit/f36fd409f8a34e13c87646fd857a4060ac89e89d)]:
  - @effect-agent/session@0.1.0-beta.18
  - @effect-agent/storage-cloudflare@0.1.0-beta.18
  - @effect-agent/core@0.1.0-beta.18
  - @effect-agent/sandbox@0.1.0-beta.18

## 0.1.0-beta.17

### Patch Changes

- Updated dependencies [[`016df57`](https://github.com/danieljvdm/effect-agent/commit/016df574fa8c0f362468d848ae830d72532cbcaf)]:
  - @effect-agent/core@0.1.0-beta.17
  - @effect-agent/session@0.1.0-beta.17
  - @effect-agent/storage-cloudflare@0.1.0-beta.17
  - @effect-agent/sandbox@0.1.0-beta.17

## 0.1.0-beta.16

### Minor Changes

- [#99](https://github.com/danieljvdm/effect-agent/pull/99) [`e4b32b5`](https://github.com/danieljvdm/effect-agent/commit/e4b32b54061e58de57d5c27f06f8ef2a821ccb38) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add the Effect-native durable progress wait from [#94](https://github.com/danieljvdm/effect-agent/issues/94). Runtime and Cloudflare callers now subscribe
  before an authoritative canonical read, wake from post-commit hints without polling, broadcast to
  concurrent same-conversation waiters, clean up on interruption, and reconnect safely after Durable
  Object eviction. Cloudflare observation and resolution calls also preserve typed authorization
  denials, and the client Layer now requires an explicit `Crypto.Crypto` provider for cancellation
  identities.

### Patch Changes

- Updated dependencies [[`e4b32b5`](https://github.com/danieljvdm/effect-agent/commit/e4b32b54061e58de57d5c27f06f8ef2a821ccb38)]:
  - @effect-agent/session@0.1.0-beta.16
  - @effect-agent/storage-cloudflare@0.1.0-beta.16
  - @effect-agent/core@0.1.0-beta.16
  - @effect-agent/sandbox@0.1.0-beta.16

## 0.1.0-beta.15

### Patch Changes

- [#97](https://github.com/danieljvdm/effect-agent/pull/97) [`38ac06e`](https://github.com/danieljvdm/effect-agent/commit/38ac06eea0956d7bef4576c5e527c6053f5a86f0) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Make Cloudflare Conversation maintenance durably incremental and quiescent ([#93](https://github.com/danieljvdm/effect-agent/issues/93)). Stable
  externally-driven waits now clear their alarm after acknowledging the observed maintenance
  generation, while pre-armed public and routed mutations, restart recovery, and bounded autonomous
  rearming preserve liveness. A caught-up forced alarm takes an O(1) maintenance-record path without
  recovery, ledger scans, or canonical-history reads. Child settlements also commit the parent's
  durable wake before child ledger finalization, preventing eviction from losing a quiescent join.
- Updated dependencies [[`38ac06e`](https://github.com/danieljvdm/effect-agent/commit/38ac06eea0956d7bef4576c5e527c6053f5a86f0)]:
  - @effect-agent/session@0.1.0-beta.15
  - @effect-agent/storage-cloudflare@0.1.0-beta.15
  - @effect-agent/core@0.1.0-beta.15
  - @effect-agent/sandbox@0.1.0-beta.15

## 0.1.0-beta.14

### Patch Changes

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.14
  - @effect-agent/sandbox@0.1.0-beta.14
  - @effect-agent/session@0.1.0-beta.14
  - @effect-agent/storage-cloudflare@0.1.0-beta.14

## 0.1.0-beta.13

### Patch Changes

- [#84](https://github.com/danieljvdm/effect-agent/pull/84) [`dd0e5c3`](https://github.com/danieljvdm/effect-agent/commit/dd0e5c38a462abe341063842521530c0d484e54a) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Run Dynamic Worker Code Mode host callbacks on retained independent Effect fibers so guest RPC
  callbacks can complete without deadlocking the in-flight worker RPC. Bound callback execution by
  the pass deadline and host-call limits, and close, interrupt, and settle callback work on teardown.
- Updated dependencies [[`68b48c9`](https://github.com/danieljvdm/effect-agent/commit/68b48c932b6a76d2c8ed0f04cc87c123a9fd11e4)]:
  - @effect-agent/core@0.1.0-beta.13
  - @effect-agent/session@0.1.0-beta.13
  - @effect-agent/storage-cloudflare@0.1.0-beta.13
  - @effect-agent/sandbox@0.1.0-beta.13

## 0.1.0-beta.12

### Patch Changes

- [#81](https://github.com/danieljvdm/effect-agent/pull/81) [`51bc32b`](https://github.com/danieljvdm/effect-agent/commit/51bc32b982abc21412c55d61064be5ec6fa1664f) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Publish `effect-cf` as a compatible host-owned peer and update the workspace integration to 0.27.0, avoiding consumer overrides and duplicate Effect service identities.

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.12
  - @effect-agent/sandbox@0.1.0-beta.12
  - @effect-agent/session@0.1.0-beta.12
  - @effect-agent/storage-cloudflare@0.1.0-beta.12

## 0.1.0-beta.11

### Patch Changes

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.11
  - @effect-agent/sandbox@0.1.0-beta.11
  - @effect-agent/session@0.1.0-beta.11
  - @effect-agent/storage-cloudflare@0.1.0-beta.11

## 0.1.0-beta.10

### Patch Changes

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.10
  - @effect-agent/sandbox@0.1.0-beta.10
  - @effect-agent/session@0.1.0-beta.10
  - @effect-agent/storage-cloudflare@0.1.0-beta.10

## 0.1.0-beta.9

### Patch Changes

- Updated dependencies [[`91ff50d`](https://github.com/danieljvdm/effect-agent/commit/91ff50df5480a0ccdfb8e0a00db39a1576e6c34b)]:
  - @effect-agent/core@0.1.0-beta.9
  - @effect-agent/session@0.1.0-beta.9
  - @effect-agent/storage-cloudflare@0.1.0-beta.9
  - @effect-agent/sandbox@0.1.0-beta.9

## 0.1.0-beta.8

### Patch Changes

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.8
  - @effect-agent/sandbox@0.1.0-beta.8
  - @effect-agent/session@0.1.0-beta.8
  - @effect-agent/storage-cloudflare@0.1.0-beta.8

## 0.1.0-beta.7

### Patch Changes

- Updated dependencies [[`5c49b78`](https://github.com/danieljvdm/effect-agent/commit/5c49b786604b3e8389cdc2c54d4f5cb284eac2b7), [`afe755a`](https://github.com/danieljvdm/effect-agent/commit/afe755a331172ffca9ceee7dd82bb452c6ccbb8a)]:
  - @effect-agent/session@0.1.0-beta.7
  - @effect-agent/core@0.1.0-beta.7
  - @effect-agent/storage-cloudflare@0.1.0-beta.7
  - @effect-agent/sandbox@0.1.0-beta.7

## 0.1.0-beta.6

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
  - @effect-agent/session@0.1.0-beta.6
  - @effect-agent/storage-cloudflare@0.1.0-beta.6
  - @effect-agent/sandbox@0.1.0-beta.6

## 0.0.1-beta.5

### Patch Changes

- [#19](https://github.com/danieljvdm/effect-agent/pull/19) [`a063031`](https://github.com/danieljvdm/effect-agent/commit/a063031c6b1f1637d947ae193a410b6bb9e8a9fc) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Align every public package with Effect 4.0.0-beta.107. Also expose per-incarnation Cloudflare
  Binding capture with live Durable Object context and derived identities, and prevent incomplete
  application Tool batches from a failed or aborted Run from poisoning prompts for later Runs.
- Updated dependencies [[`a063031`](https://github.com/danieljvdm/effect-agent/commit/a063031c6b1f1637d947ae193a410b6bb9e8a9fc)]:
  - @effect-agent/core@0.0.1-beta.5
  - @effect-agent/session@0.0.1-beta.5
  - @effect-agent/storage-cloudflare@0.0.1-beta.5

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
  - @effect-agent/storage-cloudflare@0.0.1-beta.4

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
  - @effect-agent/storage-cloudflare@0.0.1-beta.3

# @effect-agent/platform-cloudflare

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

# @effect-agent/sandbox

## 0.1.0-beta.45

## 0.1.0-beta.44

### Patch Changes

- [#307](https://github.com/danieljvdm/effect-agent/pull/307) [`f8365ee`](https://github.com/danieljvdm/effect-agent/commit/f8365eee4048076ced0a79b9149efc29297b7c41) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Upgrade to Effect rc.112 and `effect-cf` 0.40.0 while preserving MCP transports and Cloudflare host behavior.

  BEHAVIOR CHANGE: Upgrade Effect and its provider/platform/SQL packages to rc.112 or a compatible version. In Cloudflare hosts, provide `effect-cf@^0.40.0` and enable `nodejs_compat` for its async context support.

## 0.1.0-beta.43

### Minor Changes

- [#287](https://github.com/danieljvdm/effect-agent/pull/287) [`361c643`](https://github.com/danieljvdm/effect-agent/commit/361c643bfd1ac40095bc1d63d4d84c5a0afbf3d0) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add private login and card autofill with host-owned credential grants, opaque browser references, and explicit post-use recipient trust. Allow Agent Registrations to supply fresh scoped services for each durable Attempt.

## 0.1.0-beta.42

## 0.1.0-beta.41

## 0.1.0-beta.40

## 0.1.0-beta.39

### Minor Changes

- [#263](https://github.com/danieljvdm/effect-agent/pull/263) [`95865d7`](https://github.com/danieljvdm/effect-agent/commit/95865d78f55546d42f562f2f13509bbfc198c091) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Rename `@effect-agent/session` to `@effect-agent/thread` and rename the Conversation framework API to Thread.

  BEHAVIOR CHANGE: Rename Conversation identifiers, fields, record families and tags, and the durable-admin `--conversation` selector to their Thread equivalents. Reset incompatible alpha storage before upgrading.

### Patch Changes

- [#256](https://github.com/danieljvdm/effect-agent/pull/256) [`ac70e21`](https://github.com/danieljvdm/effect-agent/commit/ac70e212c7d9741ce48bd9b2a4dbd355f9dac72e) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Declare `effect` as a required `^4.0.0-rc.111` peer across all public packages so they share the application's runtime and accept compatible upgrades. Keep `effect` in application dependencies at a version satisfying the framework's and providers' peer ranges.

## 0.1.0-beta.38

## 0.1.0-beta.37

## 0.1.0-beta.36

## 0.1.0-beta.35

### Minor Changes

- [#208](https://github.com/danieljvdm/effect-agent/pull/208) [`065c455`](https://github.com/danieljvdm/effect-agent/commit/065c455d1277f73157f610429de283f41ec83d9c) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add an explicit interactive browser network policy and reject `PublicWeb` with a typed unsupported error before Cloudflare launches a browser.

  BEHAVIOR CHANGE: Move `allowedHosts` into `network: { _tag: "ExactHosts", allowedHosts }` for existing page-request allowlist workflows; `PublicWeb` remains unsupported on Cloudflare.

### Patch Changes

- [#210](https://github.com/danieljvdm/effect-agent/pull/210) [`06d4f88`](https://github.com/danieljvdm/effect-agent/commit/06d4f88c78ad175bb7e4106d53e01a2c6076ebdc) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add an explicit `Unrestricted` interactive browser policy for arbitrary-site browsing without URL/host or private-network containment guarantees, retaining session limits and host controls. Admit credential-free HTTP and HTTPS interactive navigation and URL observations without changing PageCapture contracts.

## 0.1.0-beta.34

### Patch Changes

- [#202](https://github.com/danieljvdm/effect-agent/pull/202) [`cf4a8d9`](https://github.com/danieljvdm/effect-agent/commit/cf4a8d9c645d5d8a2e552f4bb4902af4253d91ee) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Align the Effect family with rc.111 to decode nested OpenAI error events, and preserve transformed Tool parameters under its encoded response contract.

## 0.1.0-beta.33

## 0.1.0-beta.32

### Minor Changes

- [#194](https://github.com/danieljvdm/effect-agent/pull/194) [`7592ded`](https://github.com/danieljvdm/effect-agent/commit/7592deda757e0eeb0243f86bae9c2b15623e3c76) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add same-session PNG screenshots, viewport scrolling, and explicit closure to interactive browser handles. Expose host-only Cloudflare Live View, handoff, and cleanup through redacted session identities.

  BEHAVIOR CHANGE: Custom browser adapters must implement `screenshot`, `scroll`, and the `close` Effect.

## 0.1.0-beta.31

### Patch Changes

- [#183](https://github.com/danieljvdm/effect-agent/pull/183) [`d3c42d4`](https://github.com/danieljvdm/effect-agent/commit/d3c42d4e34f27610845863ec29908cd3fce95188) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add bounded selector scrape to `PageCapture`, `WebCapture.makeScrape`, and the Cloudflare binding and REST adapters.

## 0.1.0-beta.30

### Patch Changes

- [#172](https://github.com/danieljvdm/effect-agent/pull/172) [`d123424`](https://github.com/danieljvdm/effect-agent/commit/d123424be7679cfe1b8d133d0d2aa1497e087590) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add a bounded `PageScreenshot` PNG port and the native Cloudflare Browser Run Quick Action Layer.
  Screenshot bytes remain caller-owned and are never persisted or projected by the framework.

- [#172](https://github.com/danieljvdm/effect-agent/pull/172) [`d123424`](https://github.com/danieljvdm/effect-agent/commit/d123424be7679cfe1b8d133d0d2aa1497e087590) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add a bounded same-host `PageCrawl` stream and a Cloudflare Browser Run REST adapter with scoped
  remote-job cleanup.

- [#172](https://github.com/danieljvdm/effect-agent/pull/172) [`d123424`](https://github.com/danieljvdm/effect-agent/commit/d123424be7679cfe1b8d133d0d2aa1497e087590) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add a scoped, provider-neutral `InteractiveBrowser` contract for bounded navigation and interaction, with typed busy, limit, capacity, expiry, and uncertain-execution semantics.

  Document the Cloudflare Browser Run Puppeteer adapter boundary and opt-in Worker proof requirements.

## 0.1.0-beta.29

## 0.1.0-beta.28

## 0.1.0-beta.27

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

## 0.1.0-beta.26

## 0.1.0-beta.25

### Patch Changes

- [#142](https://github.com/danieljvdm/effect-agent/pull/142) [`b6804dd`](https://github.com/danieljvdm/effect-agent/commit/b6804dd60cc83b569d0e87b88521952c20ba9b7d) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Bound sandbox diagnostics and terminal artifact metadata. Reject mismatched local runtime identities and report post-start transport failures as exit failures.

## 0.1.0-beta.24

## 0.1.0-beta.23

## 0.1.0-beta.22

## 0.1.0-beta.21

## 0.1.0-beta.20

### Patch Changes

- [#111](https://github.com/danieljvdm/effect-agent/pull/111) [`c715f9f`](https://github.com/danieljvdm/effect-agent/commit/c715f9f8e436fa85e8c1ef2b27f640e637ea52e4) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Align every public package with the Effect 4.0.0-rc.110 family.

- [#111](https://github.com/danieljvdm/effect-agent/pull/111) [`c715f9f`](https://github.com/danieljvdm/effect-agent/commit/c715f9f8e436fa85e8c1ef2b27f640e637ea52e4) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Fix `validateMcpDiscovery` reporting a permanent schema drift for MCP tools whose parameters or success type is a named, refined Schema (a branded ID, a bounded string, a `Schema.Class`) — both schema derivations now resolve a top-level `$ref` before comparison.

## 0.1.0-beta.19

## 0.1.0-beta.18

## 0.1.0-beta.17

## 0.1.0-beta.16

## 0.1.0-beta.15

## 0.1.0-beta.14

## 0.1.0-beta.13

## 0.1.0-beta.12

## 0.1.0-beta.11

## 0.1.0-beta.10

## 0.1.0-beta.9

## 0.1.0-beta.8

## 0.1.0-beta.7

## 0.1.0-beta.6

## 0.0.1-beta.5

### Patch Changes

- [#19](https://github.com/danieljvdm/effect-agent/pull/19) [`a063031`](https://github.com/danieljvdm/effect-agent/commit/a063031c6b1f1637d947ae193a410b6bb9e8a9fc) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Align every public package with Effect 4.0.0-beta.107. Also expose per-incarnation Cloudflare
  Binding capture with live Durable Object context and derived identities, and prevent incomplete
  application Tool batches from a failed or aborted Run from poisoning prompts for later Runs.

## 0.0.1-beta.4

### Patch Changes

- [#13](https://github.com/danieljvdm/effect-agent/pull/13) [`f4e3786`](https://github.com/danieljvdm/effect-agent/commit/f4e378635a794d4c17192ee3de011697ccec3a3b) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Introduce the `effect-agent` umbrella package: the framework's complete pure
  surface — schema-first authoring (core), the bounded interpreter (engine),
  and operational capabilities — as one dependency-clean root package,
  mirroring how `effect` fronts the `@effect/*` satellites. Platform adapters
  remain scoped. The umbrella is version-fixed to its three constituents.

## 0.0.1-beta.3

### Patch Changes

- Adopt the MIT license across every published package, and ship the Cloudflare
  packages with type declarations for the first time: their Durable Object
  class factory now carries an explicit `ConversationObjectClass` return type,
  which unblocks TypeScript declaration emit (TS4094). Supersedes the
  0.0.1-beta.2 round (and the Cloudflare pair's 0.0.1-beta.0), which was
  published out of band from an uncommitted tree, still UNLICENSED, and without
  `.d.mts` for the Cloudflare packages.

## 0.0.1-beta.1

### Patch Changes

- Republish with correctly pinned internal dependencies. The 0.0.1-beta.0
  artifacts depended on internal `@effect-agent/*` versions that were never
  published (`workspace:*` ranges were resolved from a stale lockfile at
  publish time); the release script now pins internal ranges to the exact
  workspace versions itself.

## 0.0.1-beta.0

### Patch Changes

- Initial beta-channel release of the Effect Agent framework packages for live
  integration testing: the schema-first authoring core, the ephemeral
  interpreter, operational capabilities, sandbox contracts and the local
  adapter, canonical session records with the durable coordinator, the memory
  and SQLite storage adapters, the Node platform assembly, and the
  deterministic testing kit. The Cloudflare packages stay private until their
  declaration-emit blocker (TS4094) is resolved.

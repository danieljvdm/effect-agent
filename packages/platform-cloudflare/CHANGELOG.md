# @effect-agent/platform-cloudflare

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

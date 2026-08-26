# @effect-agent/storage-memory

## 0.1.0-beta.30

### Patch Changes

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.30
  - @effect-agent/session@0.1.0-beta.30

## 0.1.0-beta.29

### Patch Changes

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.29
  - @effect-agent/session@0.1.0-beta.29

## 0.1.0-beta.28

### Patch Changes

- Updated dependencies []:
  - @effect-agent/session@0.1.0-beta.28
  - @effect-agent/core@0.1.0-beta.28

## 0.1.0-beta.27

### Patch Changes

- Updated dependencies [[`773264b`](https://github.com/danieljvdm/effect-agent/commit/773264b75759c4456e1e549d2172bbe39610a8c1)]:
  - @effect-agent/core@0.1.0-beta.27
  - @effect-agent/session@0.1.0-beta.27

## 0.1.0-beta.26

### Patch Changes

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.26
  - @effect-agent/session@0.1.0-beta.26

## 0.1.0-beta.25

### Patch Changes

- [#142](https://github.com/danieljvdm/effect-agent/pull/142) [`b6804dd`](https://github.com/danieljvdm/effect-agent/commit/b6804dd60cc83b569d0e87b88521952c20ba9b7d) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Validate storage configuration before acquiring SQLite resources, and compare replayed persisted JSON by Schema semantics instead of serialized key order. Keep Cloudflare transport failures typed under hostile foreign values and narrow routed responses with operation schemas.

- Updated dependencies [[`b6804dd`](https://github.com/danieljvdm/effect-agent/commit/b6804dd60cc83b569d0e87b88521952c20ba9b7d)]:
  - @effect-agent/session@0.1.0-beta.25
  - @effect-agent/core@0.1.0-beta.25

## 0.1.0-beta.24

### Patch Changes

- Updated dependencies [[`6e3f56f`](https://github.com/danieljvdm/effect-agent/commit/6e3f56fbadd831372124578b027ea2bd5ff8f008)]:
  - @effect-agent/session@0.1.0-beta.24
  - @effect-agent/core@0.1.0-beta.24

## 0.1.0-beta.23

### Patch Changes

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.23
  - @effect-agent/session@0.1.0-beta.23

## 0.1.0-beta.22

### Patch Changes

- Updated dependencies [[`ce8b39c`](https://github.com/danieljvdm/effect-agent/commit/ce8b39ce8f716c0a11c6394d136b67cb9be84588)]:
  - @effect-agent/core@0.1.0-beta.22
  - @effect-agent/session@0.1.0-beta.22

## 0.1.0-beta.21

### Patch Changes

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.21
  - @effect-agent/session@0.1.0-beta.21

## 0.1.0-beta.20

### Patch Changes

- [#111](https://github.com/danieljvdm/effect-agent/pull/111) [`c715f9f`](https://github.com/danieljvdm/effect-agent/commit/c715f9f8e436fa85e8c1ef2b27f640e637ea52e4) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Align every public package with the Effect 4.0.0-rc.110 family.

- [#111](https://github.com/danieljvdm/effect-agent/pull/111) [`c715f9f`](https://github.com/danieljvdm/effect-agent/commit/c715f9f8e436fa85e8c1ef2b27f640e637ea52e4) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Fix `validateMcpDiscovery` reporting a permanent schema drift for MCP tools whose parameters or success type is a named, refined Schema (a branded ID, a bounded string, a `Schema.Class`) — both schema derivations now resolve a top-level `$ref` before comparison.

- Updated dependencies [[`7c093ec`](https://github.com/danieljvdm/effect-agent/commit/7c093ecfd900a0c55163fce76b0609d04434fa73), [`c715f9f`](https://github.com/danieljvdm/effect-agent/commit/c715f9f8e436fa85e8c1ef2b27f640e637ea52e4), [`c715f9f`](https://github.com/danieljvdm/effect-agent/commit/c715f9f8e436fa85e8c1ef2b27f640e637ea52e4)]:
  - @effect-agent/session@0.1.0-beta.20
  - @effect-agent/core@0.1.0-beta.20

## 0.1.0-beta.19

### Patch Changes

- [#105](https://github.com/danieljvdm/effect-agent/pull/105) [`b8beef5`](https://github.com/danieljvdm/effect-agent/commit/b8beef5624f6704b0e52b5023babd1272d6b0603) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Require every failed canonical `SubmissionSettled` record to carry the exact bounded generic
  `{ errorTag, message }` diagnostic and expose it as `Settlement.failure`. Joined failure fanout,
  recovery, durable adapter finalization, and idempotent replay preserve the host's canonical
  diagnostic byte-for-byte. Result-less completed joins and aborted settlements remain explicitly
  valid; malformed private-development failed records now fail closed at Schema decode.
- Updated dependencies [[`9e31de4`](https://github.com/danieljvdm/effect-agent/commit/9e31de4c5f63ebc7eefbce33d3e0ed2052538f26), [`b8beef5`](https://github.com/danieljvdm/effect-agent/commit/b8beef5624f6704b0e52b5023babd1272d6b0603)]:
  - @effect-agent/session@0.1.0-beta.19
  - @effect-agent/core@0.1.0-beta.19

## 0.1.0-beta.18

### Patch Changes

- Updated dependencies [[`f36fd40`](https://github.com/danieljvdm/effect-agent/commit/f36fd409f8a34e13c87646fd857a4060ac89e89d)]:
  - @effect-agent/session@0.1.0-beta.18
  - @effect-agent/core@0.1.0-beta.18

## 0.1.0-beta.17

### Patch Changes

- Updated dependencies [[`016df57`](https://github.com/danieljvdm/effect-agent/commit/016df574fa8c0f362468d848ae830d72532cbcaf)]:
  - @effect-agent/core@0.1.0-beta.17
  - @effect-agent/session@0.1.0-beta.17

## 0.1.0-beta.16

### Patch Changes

- Updated dependencies [[`e4b32b5`](https://github.com/danieljvdm/effect-agent/commit/e4b32b54061e58de57d5c27f06f8ef2a821ccb38)]:
  - @effect-agent/session@0.1.0-beta.16
  - @effect-agent/core@0.1.0-beta.16

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
  - @effect-agent/core@0.1.0-beta.15

## 0.1.0-beta.14

### Patch Changes

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.14
  - @effect-agent/session@0.1.0-beta.14

## 0.1.0-beta.13

### Patch Changes

- Updated dependencies [[`68b48c9`](https://github.com/danieljvdm/effect-agent/commit/68b48c932b6a76d2c8ed0f04cc87c123a9fd11e4)]:
  - @effect-agent/core@0.1.0-beta.13
  - @effect-agent/session@0.1.0-beta.13

## 0.1.0-beta.12

### Patch Changes

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.12
  - @effect-agent/session@0.1.0-beta.12

## 0.1.0-beta.11

### Patch Changes

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.11
  - @effect-agent/session@0.1.0-beta.11

## 0.1.0-beta.10

### Patch Changes

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.10
  - @effect-agent/session@0.1.0-beta.10

## 0.1.0-beta.9

### Patch Changes

- Updated dependencies [[`91ff50d`](https://github.com/danieljvdm/effect-agent/commit/91ff50df5480a0ccdfb8e0a00db39a1576e6c34b)]:
  - @effect-agent/core@0.1.0-beta.9
  - @effect-agent/session@0.1.0-beta.9

## 0.1.0-beta.8

### Patch Changes

- Updated dependencies []:
  - @effect-agent/core@0.1.0-beta.8
  - @effect-agent/session@0.1.0-beta.8

## 0.1.0-beta.7

### Patch Changes

- Updated dependencies [[`5c49b78`](https://github.com/danieljvdm/effect-agent/commit/5c49b786604b3e8389cdc2c54d4f5cb284eac2b7), [`afe755a`](https://github.com/danieljvdm/effect-agent/commit/afe755a331172ffca9ceee7dd82bb452c6ccbb8a)]:
  - @effect-agent/session@0.1.0-beta.7
  - @effect-agent/core@0.1.0-beta.7

## 0.1.0-beta.6

### Patch Changes

- Updated dependencies [[`e13ee6e`](https://github.com/danieljvdm/effect-agent/commit/e13ee6e7817549e99837d06e86caf2dea8656aa8)]:
  - @effect-agent/core@0.1.0-beta.6
  - @effect-agent/session@0.1.0-beta.6

## 0.0.1-beta.5

### Patch Changes

- [#19](https://github.com/danieljvdm/effect-agent/pull/19) [`a063031`](https://github.com/danieljvdm/effect-agent/commit/a063031c6b1f1637d947ae193a410b6bb9e8a9fc) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Align every public package with Effect 4.0.0-beta.107. Also expose per-incarnation Cloudflare
  Binding capture with live Durable Object context and derived identities, and prevent incomplete
  application Tool batches from a failed or aborted Run from poisoning prompts for later Runs.
- Updated dependencies [[`a063031`](https://github.com/danieljvdm/effect-agent/commit/a063031c6b1f1637d947ae193a410b6bb9e8a9fc)]:
  - @effect-agent/core@0.0.1-beta.5
  - @effect-agent/session@0.0.1-beta.5

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

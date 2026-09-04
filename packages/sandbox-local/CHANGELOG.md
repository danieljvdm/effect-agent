# @effect-agent/sandbox-local

## 0.1.0-beta.48

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.48

## 0.1.0-beta.47

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.47

## 0.1.0-beta.46

### Minor Changes

- [#313](https://github.com/danieljvdm/effect-agent/pull/313) [`c1a6e6a`](https://github.com/danieljvdm/effect-agent/commit/c1a6e6a915be73a49b2c266e2df74256f44c25e2) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Import module namespaces from package roots, or import declarations from their explicit PascalCase module paths, following the package map's migration examples. Discard unused modules from audited packages when bundling consumers.
  BEHAVIOR CHANGE: Replace flat declaration imports, lowercase aggregate paths, cross-package aliases, and internal helper imports with their documented owning modules; use `MemoryThreadStoreLive` instead of `MemoryStorageLive`.

### Patch Changes

- Updated dependencies [[`c1a6e6a`](https://github.com/danieljvdm/effect-agent/commit/c1a6e6a915be73a49b2c266e2df74256f44c25e2)]:
  - @effect-agent/sandbox@0.1.0-beta.46

## 0.1.0-beta.45

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.45

## 0.1.0-beta.44

### Patch Changes

- [#307](https://github.com/danieljvdm/effect-agent/pull/307) [`f8365ee`](https://github.com/danieljvdm/effect-agent/commit/f8365eee4048076ced0a79b9149efc29297b7c41) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Upgrade to Effect rc.112 and `effect-cf` 0.40.0 while preserving MCP transports and Cloudflare host behavior.

  BEHAVIOR CHANGE: Upgrade Effect and its provider/platform/SQL packages to rc.112 or a compatible version. In Cloudflare hosts, provide `effect-cf@^0.40.0` and enable `nodejs_compat` for its async context support.

- Updated dependencies [[`f8365ee`](https://github.com/danieljvdm/effect-agent/commit/f8365eee4048076ced0a79b9149efc29297b7c41)]:
  - @effect-agent/sandbox@0.1.0-beta.44

## 0.1.0-beta.43

### Patch Changes

- Updated dependencies [[`361c643`](https://github.com/danieljvdm/effect-agent/commit/361c643bfd1ac40095bc1d63d4d84c5a0afbf3d0)]:
  - @effect-agent/sandbox@0.1.0-beta.43

## 0.1.0-beta.42

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.42

## 0.1.0-beta.41

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.41

## 0.1.0-beta.40

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.40

## 0.1.0-beta.39

### Minor Changes

- [#263](https://github.com/danieljvdm/effect-agent/pull/263) [`95865d7`](https://github.com/danieljvdm/effect-agent/commit/95865d78f55546d42f562f2f13509bbfc198c091) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Rename `@effect-agent/session` to `@effect-agent/thread` and rename the Conversation framework API to Thread.

  BEHAVIOR CHANGE: Rename Conversation identifiers, fields, record families and tags, and the durable-admin `--conversation` selector to their Thread equivalents. Reset incompatible alpha storage before upgrading.

### Patch Changes

- [#256](https://github.com/danieljvdm/effect-agent/pull/256) [`ac70e21`](https://github.com/danieljvdm/effect-agent/commit/ac70e212c7d9741ce48bd9b2a4dbd355f9dac72e) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Declare `effect` as a required `^4.0.0-rc.111` peer across all public packages so they share the application's runtime and accept compatible upgrades. Keep `effect` in application dependencies at a version satisfying the framework's and providers' peer ranges.

- Updated dependencies [[`95865d7`](https://github.com/danieljvdm/effect-agent/commit/95865d78f55546d42f562f2f13509bbfc198c091), [`ac70e21`](https://github.com/danieljvdm/effect-agent/commit/ac70e212c7d9741ce48bd9b2a4dbd355f9dac72e)]:
  - @effect-agent/sandbox@0.1.0-beta.39

## 0.1.0-beta.38

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.38

## 0.1.0-beta.37

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.37

## 0.1.0-beta.36

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.36

## 0.1.0-beta.35

### Patch Changes

- Updated dependencies [[`065c455`](https://github.com/danieljvdm/effect-agent/commit/065c455d1277f73157f610429de283f41ec83d9c), [`06d4f88`](https://github.com/danieljvdm/effect-agent/commit/06d4f88c78ad175bb7e4106d53e01a2c6076ebdc)]:
  - @effect-agent/sandbox@0.1.0-beta.35

## 0.1.0-beta.34

### Patch Changes

- [#202](https://github.com/danieljvdm/effect-agent/pull/202) [`cf4a8d9`](https://github.com/danieljvdm/effect-agent/commit/cf4a8d9c645d5d8a2e552f4bb4902af4253d91ee) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Align the Effect family with rc.111 to decode nested OpenAI error events, and preserve transformed Tool parameters under its encoded response contract.

- Updated dependencies [[`cf4a8d9`](https://github.com/danieljvdm/effect-agent/commit/cf4a8d9c645d5d8a2e552f4bb4902af4253d91ee)]:
  - @effect-agent/sandbox@0.1.0-beta.34

## 0.1.0-beta.33

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.33

## 0.1.0-beta.32

### Patch Changes

- Updated dependencies [[`7592ded`](https://github.com/danieljvdm/effect-agent/commit/7592deda757e0eeb0243f86bae9c2b15623e3c76)]:
  - @effect-agent/sandbox@0.1.0-beta.32

## 0.1.0-beta.31

### Patch Changes

- Updated dependencies [[`d3c42d4`](https://github.com/danieljvdm/effect-agent/commit/d3c42d4e34f27610845863ec29908cd3fce95188)]:
  - @effect-agent/sandbox@0.1.0-beta.31

## 0.1.0-beta.30

### Patch Changes

- Updated dependencies [[`d123424`](https://github.com/danieljvdm/effect-agent/commit/d123424be7679cfe1b8d133d0d2aa1497e087590), [`d123424`](https://github.com/danieljvdm/effect-agent/commit/d123424be7679cfe1b8d133d0d2aa1497e087590), [`d123424`](https://github.com/danieljvdm/effect-agent/commit/d123424be7679cfe1b8d133d0d2aa1497e087590)]:
  - @effect-agent/sandbox@0.1.0-beta.30

## 0.1.0-beta.29

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.29

## 0.1.0-beta.28

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.28

## 0.1.0-beta.27

### Patch Changes

- Updated dependencies [[`47e9a53`](https://github.com/danieljvdm/effect-agent/commit/47e9a53d99555af3b0ac993b5c9c55ad266e327b)]:
  - @effect-agent/sandbox@0.1.0-beta.27

## 0.1.0-beta.26

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.26

## 0.1.0-beta.25

### Patch Changes

- [#142](https://github.com/danieljvdm/effect-agent/pull/142) [`b6804dd`](https://github.com/danieljvdm/effect-agent/commit/b6804dd60cc83b569d0e87b88521952c20ba9b7d) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Bound sandbox diagnostics and terminal artifact metadata. Reject mismatched local runtime identities and report post-start transport failures as exit failures.

- Updated dependencies [[`b6804dd`](https://github.com/danieljvdm/effect-agent/commit/b6804dd60cc83b569d0e87b88521952c20ba9b7d)]:
  - @effect-agent/sandbox@0.1.0-beta.25

## 0.1.0-beta.24

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.24

## 0.1.0-beta.23

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.23

## 0.1.0-beta.22

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.22

## 0.1.0-beta.21

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.21

## 0.1.0-beta.20

### Patch Changes

- [#111](https://github.com/danieljvdm/effect-agent/pull/111) [`c715f9f`](https://github.com/danieljvdm/effect-agent/commit/c715f9f8e436fa85e8c1ef2b27f640e637ea52e4) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Align every public package with the Effect 4.0.0-rc.110 family.

- [#111](https://github.com/danieljvdm/effect-agent/pull/111) [`c715f9f`](https://github.com/danieljvdm/effect-agent/commit/c715f9f8e436fa85e8c1ef2b27f640e637ea52e4) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Fix `validateMcpDiscovery` reporting a permanent schema drift for MCP tools whose parameters or success type is a named, refined Schema (a branded ID, a bounded string, a `Schema.Class`) — both schema derivations now resolve a top-level `$ref` before comparison.

- Updated dependencies [[`c715f9f`](https://github.com/danieljvdm/effect-agent/commit/c715f9f8e436fa85e8c1ef2b27f640e637ea52e4), [`c715f9f`](https://github.com/danieljvdm/effect-agent/commit/c715f9f8e436fa85e8c1ef2b27f640e637ea52e4)]:
  - @effect-agent/sandbox@0.1.0-beta.20

## 0.1.0-beta.19

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.19

## 0.1.0-beta.18

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.18

## 0.1.0-beta.17

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.17

## 0.1.0-beta.16

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.16

## 0.1.0-beta.15

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.15

## 0.1.0-beta.14

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.14

## 0.1.0-beta.13

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.13

## 0.1.0-beta.12

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.12

## 0.1.0-beta.11

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.11

## 0.1.0-beta.10

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.10

## 0.1.0-beta.9

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.9

## 0.1.0-beta.8

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.8

## 0.1.0-beta.7

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.7

## 0.1.0-beta.6

### Patch Changes

- Updated dependencies []:
  - @effect-agent/sandbox@0.1.0-beta.6

## 0.0.1-beta.5

### Patch Changes

- [#19](https://github.com/danieljvdm/effect-agent/pull/19) [`a063031`](https://github.com/danieljvdm/effect-agent/commit/a063031c6b1f1637d947ae193a410b6bb9e8a9fc) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Align every public package with Effect 4.0.0-beta.107. Also expose per-incarnation Cloudflare
  Binding capture with live Durable Object context and derived identities, and prevent incomplete
  application Tool batches from a failed or aborted Run from poisoning prompts for later Runs.
- Updated dependencies [[`a063031`](https://github.com/danieljvdm/effect-agent/commit/a063031c6b1f1637d947ae193a410b6bb9e8a9fc)]:
  - @effect-agent/sandbox@0.0.1-beta.5

## 0.0.1-beta.4

### Patch Changes

- [#13](https://github.com/danieljvdm/effect-agent/pull/13) [`f4e3786`](https://github.com/danieljvdm/effect-agent/commit/f4e378635a794d4c17192ee3de011697ccec3a3b) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Introduce the `effect-agent` umbrella package: the framework's complete pure
  surface — schema-first authoring (core), the bounded interpreter (engine),
  and operational capabilities — as one dependency-clean root package,
  mirroring how `effect` fronts the `@effect/*` satellites. Platform adapters
  remain scoped. The umbrella is version-fixed to its three constituents.
- Updated dependencies [[`f4e3786`](https://github.com/danieljvdm/effect-agent/commit/f4e378635a794d4c17192ee3de011697ccec3a3b)]:
  - @effect-agent/sandbox@0.0.1-beta.4

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
  - @effect-agent/sandbox@0.0.1-beta.3

## 0.0.1-beta.1

### Patch Changes

- Republish with correctly pinned internal dependencies. The 0.0.1-beta.0
  artifacts depended on internal `@effect-agent/*` versions that were never
  published (`workspace:*` ranges were resolved from a stale lockfile at
  publish time); the release script now pins internal ranges to the exact
  workspace versions itself.
- Updated dependencies []:
  - @effect-agent/sandbox@0.0.1-beta.1

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
  - @effect-agent/sandbox@0.0.1-beta.0

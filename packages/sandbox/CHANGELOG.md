# @effect-agent/sandbox

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

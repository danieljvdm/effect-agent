# @effect-agent/core

## 0.0.1-beta.2

### Patch Changes

- [`63c9646`](https://github.com/danieljvdm/effect-agent/commit/63c9646574e621269e0ccf105104eeb98c6ab530) Thanks [@danieljvdm](https://github.com/danieljvdm)! - Add the Cloudflare Durable Object host packages to the beta channel, align the
  runtime packages with Effect 4.0.0-beta.105 through singleton peer contracts,
  and support per-incarnation binding resolution for host application tools.

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

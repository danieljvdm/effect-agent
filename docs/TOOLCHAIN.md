# Repository toolchain

Status: **Accepted for private development**

This repository is a Vite+ monorepo derived from
[`danieljvdm/vp-effect-cf-template`](https://github.com/danieljvdm/vp-effect-cf-template).
Its hosted and Cloudflare applications are intentionally absent. A local browser test bench lives
under `examples/demo`; `examples/providers` is a compile-only native provider-binding leaf.
Cloudflare support exists as library packages (`storage-cloudflare`, `platform-cloudflare` since
Phase 6), not as an application scaffold: there is no `wrangler` dependency or configuration,
and the Worker entries in the repository are test fixtures.

## Source-of-truth versions

The root `package.json` is the only version source for shared dependencies.

| Tool                              |        Current pin | Purpose                                                                           |
| --------------------------------- | -----------------: | --------------------------------------------------------------------------------- |
| Bun                               |           `1.3.14` | Package manager, workspace resolver, and lockfile                                 |
| Vite+                             |            `0.2.6` | Formatting, linting, tests, library builds, staged checks, and task orchestration |
| Vitest                            |           `4.1.10` | Vite+ test runtime, pinned through an override so integrations share one instance |
| Effect                            |   `4.0.0-beta.102` | Runtime, Schema, services, and Effect AI                                          |
| `@effect/platform-node`           |   `4.0.0-beta.102` | Node services used by repository scripts                                          |
| `@effect/platform-browser`        |   `4.0.0-beta.102` | `BrowserCrypto` for the workerd runtime (Cloudflare packages)                     |
| `@effect/sql-sqlite-do`           |   `4.0.0-beta.102` | Durable Object SQLite `SqlClient` and Migrator (Cloudflare packages)              |
| `@effect/vitest`                  |   `4.0.0-beta.102` | Effect-aware test execution and scoped Layer composition                          |
| `@cloudflare/vitest-pool-workers` |           `0.21.3` | In-workerd Vitest pool for the Cloudflare package suites (vendors wrangler)       |
| `@cloudflare/workers-types`       |     `5.20260813.1` | Cloudflare runtime types (types-only devDependency)                               |
| Miniflare                         |     `4.20260730.0` | Programmatic workerd runtimes for the restart-persistence test lane               |
| esbuild                           |           `0.28.1` | Bundles the Miniflare-lane worker entry (Miniflare no longer bundles)             |
| Node.js                           | `22.18+ or 24.11+` | Runtime range compatible with Vite+                                               |
| TypeScript                        |            `7.0.2` | Type checker used with the Effect compiler patch                                  |
| `@effect/tsgo`                    |           `0.24.3` | Effect-aware TypeScript diagnostics                                               |
| `@types/node`                     |           `26.1.2` | Node types for repository scripts                                                 |
| `tsx`                             |           `4.23.1` | TypeScript script runner                                                          |
| VitePress                         |   `2.0.0-alpha.18` | Markdown-driven documentation site                                                |
| Vue                               |           `3.5.40` | VitePress theme components                                                        |

Workspace packages refer to shared versions with `catalog:`. They must not introduce a second
Effect version. The Bun lockfile is committed and CI installs it with `--frozen-lockfile`.

Root overrides align the contributor skills CLI with the repository's Effect and
`@effect/platform-node` versions. Framework packages, repository scripts, and contributor tooling
therefore install one Effect runtime. The Vitest override matches the version bundled by Vite+ so
`@effect/vitest` and `vite-plus/test` resolve the same runner, assertion, and test context state.
Vitest remains supplied by Vite+ rather than becoming a separate workspace dependency, with one
probed exception: `vp test` cannot drive the Cloudflare Workers pool runner, so
`storage-cloudflare` and `platform-cloudflare` declare the catalog-pinned `vitest` directly and
run `vitest run` as their `test` scripts (P6 WP0 probe, decision D-P6-7).

## Current workspace

Only packages required by the active roadmap phase exist:

```text
packages/
  core/                 Domain and Agent authoring package
  engine/               Ephemeral Agent interpreter
  capabilities/         Operational policy and capability adapters
  sandbox/              Platform-neutral sandbox contracts
  sandbox-local/        Node-local sandbox adapter
  session/              Canonical Conversation records, reducers, and store ports
  storage-memory/       Scoped deterministic reference storage adapter
  storage-sqlite/       Node SQLite persistence adapter
  storage-cloudflare/   Durable Object SQLite persistence adapter and routed port protocol
  platform-node/        Class DN Node/SQLite Layer assembly and durable host
  platform-cloudflare/  Class DC Cloudflare Layer assembly (Conversation Objects, alarms)
  testing/              Scripted model, fixtures, and conformance test kit
examples/
  demo/             Leaf TanStack Start browser bench
  pr-review/        Leaf GitHub pull-request reviewer (CLI + composite Action)
  providers/        Leaf OpenAI/Anthropic Model-binding compile proof
  repo-ops/         Leaf repo-ops evidence auditor (P7 internal agent)
```

Shared compiler options live in root `tsconfig.base.json`; they do not need a workspace package.

The package dependency direction is:

```text
core <- engine <- capabilities
core <- sandbox <- sandbox-local
core <- engine <- session <- storage adapters
engine + session + selected adapters <- platform packages
core + engine <- testing
```

`testing` is an outward test kit used by tests and examples. Production packages must never depend
on it. Additional framework package directories are created only when their roadmap phase starts.
There is no `apps/` workspace. `examples/*` are runnable, private leaf consumers: framework
packages never import them, and they add no deployment or durability claim.

## Commands

Run commands from the repository root:

| Command                                       | Meaning                                                                        |
| --------------------------------------------- | ------------------------------------------------------------------------------ |
| `bun install`                                 | Install dependencies and run repository setup                                  |
| `bun run check`                               | Run Vite+ format/lint/type checks, package type checks, and script type checks |
| `bun run test`                                | Run package and example test tasks in dependency order                         |
| `bun run build`                               | Build packages, runnable examples, and the VitePress documentation site        |
| `bun run docs:dev`                            | Run the local VitePress documentation server                                   |
| `bun run docs:build`                          | Build and validate the Markdown documentation site                             |
| `bun run docs:preview`                        | Preview the built documentation site                                           |
| `bun --filter @effect-agent/example-demo dev` | Run the chat-first demo and Phase 2 simulator on port 4173                     |
| `bun run ready`                               | Run check, test, and build; this is the local and CI handoff gate              |
| `bun run format:write`                        | Apply repository formatting                                                    |
| `bun run sync:effect`                         | Align the local Effect source checkout with the catalog pin                    |
| `bun run sync:agent-skills`                   | Refresh checked-in contributor skills                                          |
| `bun run sync:agent-skills:dry`               | Preview contributor skill changes                                              |

Vite+ owns shared configuration in `vite.config.ts`. Package-specific scripts remain in each
package manifest so Vite+ can order and cache them from the actual workspace dependency graph.
The root declares the catalog's Vite+ core alias as `vite` as well as `vite-plus`; Bun needs that
direct peer provider for the Vite+ toolchain. VitePress intentionally resolves the official Vite
implementation as its nested dependency; the repository therefore does not use a global `vite`
override. The documentation build is part of the root `build` and `ready` gates.

## Releasing to npm

Versioning uses changesets (`bun run changeset`, `bun run changeset:version`);
publishing uses `bun run release:publish`, NOT `changeset publish`. In a
non-pnpm repository changesets shells out to `npm publish`, which ships
`workspace:*` and `catalog:` protocol ranges verbatim; `bun publish` resolves
both at publish time. The release script also swaps each manifest's
source-first export map (a private-development convention, see below) for the
built `dist` entries during the publish, restores it afterwards, and skips
versions already on the registry, so a partial publish can be re-run.

Releases currently ship on the **beta channel**: the repository is in
changesets pre mode (`.changeset/pre.json`, tag `beta`), so `changeset
version` produces `X.Y.Z-beta.N` versions, and the release script publishes
any prerelease under the matching npm dist-tag (never `latest`). Consumers
install with `bun add @effect-agent/core@beta` (or an exact version).
Leaving the channel for a stable release is `bun x changeset pre exit`
followed by the normal sequence.

The release sequence from an authenticated npm session (`bunx npm login`, an
owner of the `@effect-agent` scope):

1. `bun run changeset` — describe the change (one exists for `0.0.1-beta.0`);
2. `bun run changeset:version && bun install` — cut versions and changelogs;
3. `bun run ready`;
4. `bun run release:publish -- --dry-run`, then without `--dry-run`
   (append `--otp <code>` when npm 2FA asks);
5. `bun x changeset tag && git push --follow-tags`.

All twelve packages publish under the MIT license (owner decision
2026-08-14, resolving the licensing half of D-023). The Cloudflare pair
joined the channel after their declaration-emit fix: the Durable Object
class factory carries an explicit `ConversationObjectClass` return type
because TS4094 rejects inferring an exported anonymous class type around a
private field, and both packages pin `pack: { dts, sourcemap }` in their
Vite configs since a package-level config suppresses `vp pack`'s
zero-config defaults.

## Post-install setup

`scripts/postinstall.ts` performs three deterministic steps:

1. `sync-effect-submodule.ts` reads the exact `effect` catalog version and checks out the matching
   `effect@<version>` tag in `repos/effect`.
2. `vp config` installs the repository's `.vite-hooks/pre-commit` hook.
3. `effect-tsgo patch` patches the managed TypeScript compiler.

CI skips the source checkout because production checks use installed packages, not reference
source. Local development keeps the checkout because implementation agents often need to verify
current Effect v4 and Effect AI behavior.

The attributed Flue and Pi source snapshots are separate shallow Git submodules at `repos/flue`
and `repos/pi`. Their gitlinks are pinned to the commits recorded in
[REFERENCE-ANALYSIS.md](REFERENCE-ANALYSIS.md). A recursive clone initializes them; an existing
clone can initialize them with:

```sh
git submodule update --init -- repos/flue repos/pi
```

`bun install` and `bun run sync:effect` synchronize only `repos/effect`; they do not move the
research snapshots. Updating a research snapshot requires updating its submodule pointer and the
matching reference-analysis record together.

To upgrade Effect:

1. update every Effect-family catalog entry in root `package.json` as one change;
2. run `bun install`;
3. run `bun run sync:effect`;
4. run `bun run ready`;
5. run the Effect AI semantic/provider suite once it exists;
6. commit the catalog, lockfile, and submodule pointer together.

## Contributor agent skills

[`@danieljvdm/agent-skills`](https://github.com/danieljvdm/agent-skills) is a root development
dependency. `agent-skills.jsonc` opts into its `effect` family:

- `effect-ts`

The sync command copies these into `.agents/skills` and creates `.claude/skills` symlinks to the
copies. Both the copies and symlinks are committed so a fresh checkout gives agents the same
instructions before dependency installation. Re-run the sync command after changing the manifest
or updating the dependency.

These are contributor instructions. They are not the runtime Skill abstraction described in the
product specification and must not be imported by a framework package.

The [project execution guide](guides/project-execution.md#3-skill-routing) maps implementation
work to the focused skill references. Work items and prompt packets name the references they
require, and phase evidence records any justified exception.

## Adding a package

Create a package only when a roadmap phase requires it:

1. add `packages/<name>/package.json`, `src/index.ts`, and `tsconfig.json`;
2. use the working `@effect-agent/<name>` scope and keep it private;
3. use `catalog:` for shared external dependencies and `workspace:*` for internal packages;
4. extend the root `tsconfig.base.json`;
5. add it to root `tsconfig.json` references;
6. declare only inward workspace dependencies;
7. provide `check`, `test`, and `build` scripts when the package has those behaviors;
8. update `docs/ARCHITECTURE.md` and `docs/ROADMAP.md`;
9. run `bun run ready`.

Package export maps point to source during private development. Distribution builds are produced
with `vp pack`; final `dist` export maps, publication files, versioning, provenance, and release
automation are deferred until open-source preparation.

## CI and hooks

The GitHub Actions workflow installs the exact Bun version, performs a frozen-lockfile install,
and runs `bun run ready`. It does not initialize any source submodule.

The Vite+ pre-commit hook runs the staged formatter. CI remains authoritative: hooks improve local
feedback but are not a correctness boundary.

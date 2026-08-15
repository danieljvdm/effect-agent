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
| Effect                            |   `4.0.0-beta.107` | Runtime, Schema, services, and Effect AI                                          |
| `@effect/platform-node`           |   `4.0.0-beta.107` | Node services used by repository scripts                                          |
| `@effect/platform-browser`        |   `4.0.0-beta.107` | `BrowserCrypto` for the workerd runtime (Cloudflare packages)                     |
| `@effect/sql-sqlite-do`           |   `4.0.0-beta.107` | Durable Object SQLite `SqlClient` and Migrator (Cloudflare packages)              |
| `@effect/vitest`                  |   `4.0.0-beta.107` | Effect-aware test execution and scoped Layer composition                          |
| `@cloudflare/vitest-pool-workers` |           `0.21.3` | In-workerd Vitest pool for the Cloudflare package suites (vendors wrangler)       |
| `@cloudflare/workers-types`       |     `5.20260813.1` | Cloudflare runtime types (types-only devDependency)                               |
| Miniflare                         |     `4.20260730.0` | Programmatic workerd runtimes for the restart-persistence test lane               |
| esbuild                           |           `0.28.1` | Bundles the Miniflare-lane worker entry (Miniflare no longer bundles)             |
| Node.js                           | `22.18+ or 24.11+` | Runtime range compatible with Vite+                                               |
| TypeScript                        |            `7.0.2` | Type checker used with the Effect compiler patch                                  |
| `@effect/tsgo`                    |           `0.33.0` | Effect-aware TypeScript diagnostics                                               |
| `@types/node`                     |           `26.1.2` | Node types for repository scripts                                                 |
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
run `vitest run` as their `test` scripts.

## Current workspace

Only the packages the framework needs today exist:

```text
packages/
  effect-agent/         Umbrella: the pure authoring/interpreter/capabilities surface as one package
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
  pr-review/            Packaged GitHub pull-request reviewer (factory, adapters, CLI/Action entrypoints)
  testing/              Scripted model, fixtures, and conformance test kit
examples/
  demo/             Leaf TanStack Start browser bench
  pr-review/        Leaf consumer of @effect-agent/pr-review (guidance, extra tool, ignore globs)
  providers/        Leaf OpenAI/Anthropic Model-binding compile proof
  repo-ops/         Leaf repo-ops evidence auditor (P7 internal agent)
action/             Prebuilt node24 GitHub Action over @effect-agent/pr-review (committed bundle)
```

Shared compiler options live in root `tsconfig.base.json`; they do not need a workspace package.

The package dependency direction is:

```text
core <- engine <- capabilities
core <- sandbox <- sandbox-local
core <- sandbox <- capabilities
core <- engine <- session <- storage adapters
engine + session + sandbox + selected adapters <- platform packages
core + engine <- testing
core + engine + capabilities <- effect-agent (umbrella) <- pr-review
```

`testing` is an outward test kit used by tests and examples. Production packages must never depend
on it. Additional framework package directories are created only for a genuinely new framework concern
agreed with the repository owner in the pull request that introduces them (`pr-review` is the one
packaged application). There is no `apps/`
workspace. `examples/*` are runnable, private leaf consumers: framework packages never import
them, and they add no deployment or durability claim.

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

All public framework workspaces form one Changesets `fixed` group. Any package changeset advances
all fourteen packages to one shared version, including packages with no behavioral change in that
release. This deliberately spends additional package versions in exchange for a single compatible
release coordinate across the framework. When adding a public package, add its manifest name to
the fixed group in `.changeset/config.json`; the toolchain test fails if the workspace and group
diverge.

Releases are automated: on every push to `main`, `.github/workflows/release.yml`
maintains a "Version Packages (beta)" PR from the pending changesets, and
merging that PR publishes via npm **trusted publishing** — the workflow's OIDC
identity is exchanged for short-lived credentials (no npm token, no OTP), with
provenance attached. The generated PR is release metadata over code that
already passed the ordinary PR gates: its Static checks, Tests, Build, and
agentic Review workflows use `paths-ignore` for the narrow generated surface.
The trusted main-branch Release workflow independently resolves the PR returned
by Changesets, validates its repository, branch, base, and commit identities,
runs `changeset version` and lockfile generation in a temporary worktree, and
requires the resulting complete Git tree to match the proposed head
byte-for-byte. It posts the required `ready` check directly to that immutable
head only after successful verification. An unexpected path invokes the
ordinary PR workflows; a later human mutation has a new head without the
verified check and therefore remains unmergeable. After merge, the release
workflow still performs a frozen install and rebuilds the exact versioned tree
before any registry mutation.

Each package on npmjs.com lists `release.yml` in `danieljvdm/effect-agent` as
its trusted publisher (package Settings, a one-time registration; "Allow npm
publish" only). In CI the publish script runs in `--ci` mode: `bun pm pack`
resolves the `workspace:`/`catalog:` protocols into the tarball and the npm CLI
uploads it, because only npm implements the OIDC exchange.

The manual fallback from an authenticated npm session (`bunx npm login`, an
owner of the `@effect-agent` scope):

1. `bun run changeset` — describe the change (one exists for `0.0.1-beta.0`);
2. `bun run changeset:version && bun install` — cut versions and changelogs;
3. `bun run ready`;
4. `bun run release:publish -- --dry-run`, then without `--dry-run`
   (append `--otp <code>` when npm 2FA asks);
5. `bun x changeset tag && git push --follow-tags`.

All fourteen packages publish under the MIT license (owner decision
2026-08-14). The Cloudflare pair
joined the channel after their declaration-emit fix: the Durable Object
class factory carries an explicit `ConversationObjectClass` return type
because TS4094 rejects inferring an exported anonymous class type around a
private field, and both packages pin `pack: { dts, sourcemap }` in their
Vite configs since a package-level config suppresses `vp pack`'s
zero-config defaults.

## Script runners

Repository scripts run under **Bun** (`bun scripts/<name>.ts`) — there is no
`tsx` in this repository. The one exception is anything that imports
`@effect-agent/storage-sqlite`: Bun does not implement `node:sqlite`, so
`admin:durable` runs under `node --experimental-transform-types` (plain
strip-only mode rejects the framework's runtime `namespace` declarations),
and the platform-node crash/soak harnesses spawn their TypeScript worker
entries the same way (verified: the full process-kill matrix passes under
transform-mode workers).

## Post-install setup

`scripts/postinstall.ts` performs three deterministic steps:

1. `sync-effect-submodule.ts` reads the exact `effect` catalog version and checks out the matching
   `effect@<version>` tag in `repos/effect`.
2. `vp config` installs the repository's `.vite-hooks/pre-commit` hook.
3. `effect-tsgo patch` patches the managed TypeScript compiler.

CI installs with lifecycle scripts suppressed, verifies the locked Dev Kit outputs, and then runs
`bun run patch:tsgo` explicitly in every job that checks, tests, or builds TypeScript. This preserves
the postinstall compiler invariant without cloning the Effect source checkout. Local development
keeps the checkout because implementation agents often need to verify current Effect v4 and Effect
AI behavior.

Known workaround: the `preferTypedSchemaDecoder` language-service rule is set to `"off"` in
`tsconfig.base.json` because `@effect/tsgo` `0.33.0` nil-panics in that rule
(`internal/rules/prefer_typed_schema_decoder.go:37`) while checking `packages/session`. Re-enable
it once a fixed tsgo lands.

The attributed Flue and Pi source snapshots are separate shallow Git submodules at `repos/flue`
and `repos/pi`. Their gitlinks are pinned to the commits recorded in
attributed research inputs. A recursive clone initializes them; an existing
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

Contributor skills are managed by [`@danieljvdm/dev-kit`](https://github.com/danieljvdm/dev-kit)
from `dev-kit.jsonc` (currently the `dev-kit`, `effect-ts`, `testing`, and `cloudflare`
selections). `dev-kit apply` runs on postinstall and converges the committed copies in
`.agents/skills` plus the `.claude/skills` symlinks; `dev-kit.lock.json` records the exact
resolution, and CI verifies it with `dev-kit apply --locked` after a script-suppressed frozen
install. Use the `dev-kit` skill before changing managed outputs.

These are contributor instructions. They are not the runtime Skill abstraction described in the
product specification and must not be imported by a framework package.

## Adding a package

Create a package only for a genuinely new framework concern, agreed with the repository owner in
the pull request that introduces it:

1. add `packages/<name>/package.json`, `src/index.ts`, and `tsconfig.json`;
2. use the working `@effect-agent/<name>` scope with the sibling manifest shape (MIT,
   `publishConfig.access: public`, source-first exports) — packages publish on the `beta`
   dist-tag, and each new package needs its one-time npm trusted-publisher
   registration before CI can publish it; add the package to the single fixed release group in
   `.changeset/config.json`;
3. use `catalog:` for shared external dependencies and `workspace:*` for internal packages;
4. extend the root `tsconfig.base.json`;
5. add it to root `tsconfig.json` references;
6. declare only inward workspace dependencies;
7. provide `check`, `test`, and `build` scripts when the package has those behaviors;
8. update `docs/ARCHITECTURE.md`;
9. run `bun run ready`.

Package export maps point to source during private development. Distribution builds are produced
with `vp pack`; final `dist` export maps, publication files, versioning, provenance, and release
automation are deferred until open-source preparation.

## CI and hooks

The CI workflow runs on pull requests, not again after their merge to `main`. Ordinary PRs install
the exact Bun version with a frozen-lockfile install, then run the `ready` gate as three parallel
jobs — Static checks (`bun run check`), Tests (`bun run test`), and Build (`bun run build`) — with
a fan-in job that keeps the required branch-protection check named `ready`. The exact internal
Changesets release PR is the only exception: CI and PR Review path-filter the exact set of files
Changesets may generate. GitHub deliberately leaves `pull_request` workflows created through
`GITHUB_TOKEN` approval-required, so the trusted Release run validates the PR lineage, regenerates
the complete tree from its checked-out `main` commit, and creates the required `ready` check on the
verified head through the Checks API. The check is success only after an exact-tree comparison; a
verification failure posts failure, and a resolution failure posts nothing. Unexpected files route
through the ordinary workflows, while a manual generated-path-only push receives no new `ready`
check. Both cases are fail-closed.

Each ordinary-PR job restores and saves the Vite Task cache
(`node_modules/.vite/task-cache`), so later synchronize events on the same PR can replay
per-package gates whose fingerprinted inputs did not change. GitHub scopes those caches to the
PR's merge ref; removing duplicate `main` CI intentionally trades the previous cross-PR
default-branch baseline for lower post-merge compute, so a PR's first run may be cold. The CI
workflow does not initialize any source submodule. On `main`, the Release workflow is the sole
push-triggered package automation and owns version-PR maintenance and publishing.

The Vite+ pre-commit hook runs the staged formatter. CI remains authoritative: hooks improve local
feedback but are not a correctness boundary.

# Repository toolchain

Status: **Accepted for private development**

This repository is a Vite+ monorepo derived from
[`danieljvdm/vp-effect-cf-template`](https://github.com/danieljvdm/vp-effect-cf-template).
Its hosted and Cloudflare applications are intentionally absent. A local browser test bench lives
under `examples/demo`; Cloudflare support will be introduced later as library packages, not as an
application scaffold.

## Source-of-truth versions

The root `package.json` is the only version source for shared dependencies.

| Tool                    |        Current pin | Purpose                                                                           |
| ----------------------- | -----------------: | --------------------------------------------------------------------------------- |
| Bun                     |           `1.3.14` | Package manager, workspace resolver, and lockfile                                 |
| Vite+                   |            `0.2.6` | Formatting, linting, tests, library builds, staged checks, and task orchestration |
| Vitest                  |           `4.1.10` | Vite+ test runtime, pinned through an override so integrations share one instance |
| Effect                  |   `4.0.0-beta.102` | Runtime, Schema, services, and Effect AI                                          |
| `@effect/platform-node` |   `4.0.0-beta.102` | Node services used by repository scripts                                          |
| `@effect/vitest`        |   `4.0.0-beta.102` | Effect-aware test execution and scoped Layer composition                          |
| Node.js                 | `22.18+ or 24.11+` | Runtime range compatible with Vite+                                               |
| TypeScript              |            `7.0.2` | Type checker used with the Effect compiler patch                                  |
| `@effect/tsgo`          |           `0.24.3` | Effect-aware TypeScript diagnostics                                               |
| `@types/node`           |           `26.1.2` | Node types for repository scripts                                                 |
| `tsx`                   |           `4.23.1` | TypeScript script runner                                                          |

Workspace packages refer to shared versions with `catalog:`. They must not introduce a second
Effect version. The Bun lockfile is committed and CI installs it with `--frozen-lockfile`.

Root overrides align the contributor skills CLI with the repository's Effect and
`@effect/platform-node` versions. Framework packages, repository scripts, and contributor tooling
therefore install one Effect runtime. The Vitest override matches the version bundled by Vite+ so
`@effect/vitest` and `vite-plus/test` resolve the same runner, assertion, and test context state.
Vitest remains supplied by Vite+ rather than becoming a separate workspace dependency.

## Current workspace

Only packages required by the active roadmap phase exist:

```text
packages/
  core/     Phase 0 domain and authoring package
  engine/   Phase 0 ephemeral interpreter package
  testing/  Phase 0 scripted model and conformance test kit
examples/
  demo/     Leaf TanStack Start browser bench for the cumulative Travel Planner
```

Shared compiler options live in root `tsconfig.base.json`; they do not need a workspace package.

The package dependency direction is:

```text
core <- engine <- testing
  ^__________________|
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
| `bun run build`                               | Build library packages and runnable examples                                   |
| `bun --filter @effect-agent/example-demo dev` | Run the Phase 0 browser bench on port 4173                                     |
| `bun run ready`                               | Run check, test, and build; this is the local and CI handoff gate              |
| `bun run format:write`                        | Apply repository formatting                                                    |
| `bun run sync:effect`                         | Align the local Effect source checkout with the catalog pin                    |
| `bun run sync:agent-skills`                   | Refresh checked-in contributor skills                                          |
| `bun run sync:agent-skills:dry`               | Preview contributor skill changes                                              |

Vite+ owns shared configuration in `vite.config.ts`. Package-specific scripts remain in each
package manifest so Vite+ can order and cache them from the actual workspace dependency graph.
The root declares the catalog's Vite+ core alias as `vite` as well as `vite-plus`; Bun needs that
direct peer provider to avoid resolving a second Vite implementation.

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

- `effect-cli`
- `effect-patterns`

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

# Repository toolchain

Status: **Public alpha, distributed on npm's beta channel**

This repository is a Vite+ monorepo derived from
[`danieljvdm/vp-effect-cf-template`](https://github.com/danieljvdm/vp-effect-cf-template).
Its hosted and Cloudflare applications are intentionally absent. A local browser test bench lives
under `examples/demo`; `examples/providers` is a compile-only native provider-binding leaf.
`examples/pr-work-orders` is a private trusted-local proof of the head-bound work-order host.
`examples/pr-work-order-ingress` is the private GitHub dispatch and isolated-publication proof.
Cloudflare support exists as library packages (`storage-cloudflare`, `platform-cloudflare` since
Phase 6), not as an application scaffold. The narrowly scoped
`examples/browser-run-worker-proof` leaf owns the repository's direct Wrangler dependency and one
configuration used only for its opt-in temporary deployment. Other Worker entries remain test
fixtures or private examples.
`examples/pr-review-eval` is the opt-in live-model quality bench for the provider-neutral PR
reviewer. It stores only safe fixtures in Git and keeps private corpora and raw run results ignored.

## Source-of-truth versions

The root `package.json` is the only version source for shared dependencies.

| Tool                              |          Current pin | Purpose                                                                           |
| --------------------------------- | -------------------: | --------------------------------------------------------------------------------- |
| Bun                               |             `1.3.14` | Package manager, workspace resolver, and lockfile                                 |
| Vite+                             |              `0.2.6` | Formatting, linting, tests, library builds, staged checks, and task orchestration |
| Vitest                            |             `4.1.10` | Vite+ test runtime, pinned through an override so integrations share one instance |
| Effect                            |       `4.0.0-rc.111` | Runtime, Schema, services, and Effect AI                                          |
| `@effect/ai-openai`               |       `4.0.0-rc.111` | Upstream OpenAI provider used by examples and the review Action                   |
| `@effect/ai-anthropic`            |       `4.0.0-rc.111` | Upstream Anthropic provider used by examples                                      |
| `@effect/platform-node`           |       `4.0.0-rc.111` | Node services used by repository scripts                                          |
| `@effect/platform-browser`        |       `4.0.0-rc.111` | `BrowserCrypto` for the workerd runtime (Cloudflare packages)                     |
| `@effect/sql-d1`                  |       `4.0.0-rc.111` | Required D1 peer of the Cloudflare runtime boundary                               |
| `@effect/sql-sqlite-do`           |       `4.0.0-rc.111` | Durable Object SQLite `SqlClient` and Migrator (Cloudflare packages)              |
| `@effect/sql-sqlite-node`         |       `4.0.0-rc.111` | Node SQLite storage adapter                                                       |
| `@effect/vitest`                  |       `4.0.0-rc.111` | Effect-aware test execution and scoped Layer composition                          |
| `effect-cf`                       |             `0.37.0` | Effect-native Cloudflare runtime boundary used by `platform-cloudflare`           |
| `@cloudflare/vitest-pool-workers` |             `0.21.3` | In-workerd Vitest pool for the Cloudflare package suites (vendors wrangler)       |
| `@cloudflare/workers-types`       |       `5.20260825.1` | Cloudflare runtime types (types-only devDependency)                               |
| Miniflare                         | `5.20260811.1-alpha` | Programmatic workerd runtimes for Code Mode and restart-persistence test lanes    |
| esbuild                           |             `0.28.1` | Bundles the Miniflare-lane worker entry (Miniflare no longer bundles)             |
| Node.js                           |   `22.18+ or 24.11+` | Runtime range compatible with Vite+                                               |
| TypeScript                        |              `7.0.2` | Type checker used with the Effect compiler patch                                  |
| `@effect/tsgo`                    |             `0.33.0` | Effect-aware TypeScript diagnostics                                               |
| `@types/node`                     |             `26.1.2` | Node types for repository scripts                                                 |
| VitePress                         |     `2.0.0-alpha.18` | Markdown-driven documentation site                                                |
| Vue                               |             `3.5.40` | VitePress theme components                                                        |
| Wrangler                          |            `4.123.0` | Opt-in temporary Browser Run Worker deployment                                    |

Workspace packages refer to shared versions with `catalog:`. They must not introduce a second
Effect version. The Bun lockfile is committed and CI installs it with `--frozen-lockfile`.

Every public package declares `effect` as a required `^4.0.0-rc.111` peer, following upstream
Effect packages. Its `devDependencies` entry uses `catalog:` to supply the exact development
version for local builds and tests. Raise the peer minimum when code needs newer Effect APIs;
updating the development pin alone does not require raising it. Private applications and examples declare
`effect` as a regular dependency. Platform and SQL implementations remain regular dependencies
of the adapters that acquire them; their public service contracts come from `effect`. Internal
framework dependencies remain `workspace:*`, which publishes as the exact framework release.

The catalog contains exact development pins; published Effect peers accept compatible versions
within their caret ranges. Public consumers keep all framework packages at one exact release and
select Effect/provider versions that satisfy their peer requirements. The public alpha permits
breaking framework API and stored-schema changes before 1.0. There is no
compatibility window or migration tooling promise; incompatible data fails clearly and disposable
development data may be reset. See [installation and compatibility](guide/getting-started.md#installation-and-compatibility).

The root catalog selects one exact `effect-cf` version for reproducible workspace installs.
`@effect-agent/platform-cloudflare` publishes `effect-cf` as the compatible `^0.37.0` host peer
and uses the catalog entry only as a development dependency. Consumers therefore provide one
shared runtime instance without needing a root override to replace a nested exact dependency.
Version `0.37.0` supplies native RPC trace validation and propagation, shared span helpers, and
typed invocation hooks. The platform's development dependencies align its required D1 peer with
the Effect catalog; the D1 driver does not become an Effect Agent runtime dependency.

Root overrides align the contributor skills CLI with the repository's Effect and
`@effect/platform-node` versions. Framework packages, repository scripts, and contributor tooling
therefore install one Effect runtime. The Vitest override matches the version bundled by Vite+ so
`@effect/vitest` and `vite-plus/test` resolve the same runner, assertion, and test context state.
Vitest remains supplied by Vite+ rather than becoming a separate workspace dependency, with one
probed exception: `vp test` cannot drive the Cloudflare Workers pool runner, so
`storage-cloudflare` and `platform-cloudflare` declare the catalog-pinned `vitest` directly and
define `vitest run` test tasks in their Vite configs. The Code Mode Cloudflare example uses the
same task configuration for its programmatic Miniflare tests.

## Current workspace

Only the packages the framework needs today exist:

```text
packages/
  effect-agent/         Umbrella: the platform-neutral authoring, interpreter, and capabilities APIs
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
  pr-review/            Provider-neutral pull-request investigation and verification
  pr-review-action/     Private GitHub adapter for pr-review
  testing/              Scripted model, fixtures, and conformance test kit
examples/
  demo/             Leaf TanStack Start browser bench
  pr-work-orders/           Trusted-local work-order implementer proof (private, class E)
  pr-work-order-ingress/    GitHub dispatch, persistent journal, isolated checks/publication
  providers/        Leaf OpenAI/Anthropic Model-binding compile proof
  repo-ops/         Leaf repo-ops evidence auditor (P7 internal agent)
  browser-run-worker-proof/  Opt-in temporary deployment proving the native Browser Run binding
  pr-review-eval/   Opt-in replay bench for first-pass PR-review model quality
action/             Published PR-review Action metadata and committed node24 bundle
work-order-action/  Prebuilt node24 PR work-order Action (committed bundle, separate authority)
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

Vite+ is the command authority and is distinct from Vite. Use `vp help` and
`vp <command> --help` to inspect commands. Bun remains the package manager and script runtime
behind these tasks. Do not invoke `bun run`, `npm run`, `pnpm run`, `yarn run`, or the underlying
compiler, test, lint, or formatting binaries directly.

| Command                                    | Meaning                                                                        |
| ------------------------------------------ | ------------------------------------------------------------------------------ |
| `vp install`                               | Install dependencies and run repository setup                                  |
| `vp run check`                             | Run Vite+ format/lint/type checks, package type checks, and script type checks |
| `vp run test`                              | Run all package and example test tasks, with at most four tasks at once        |
| `vp run build`                             | Build packages, runnable examples, and the VitePress documentation site        |
| `vp run docs:dev`                          | Run the local VitePress documentation server                                   |
| `vp run docs:build`                        | Build and validate the Markdown documentation site                             |
| `vp run docs:preview`                      | Preview the built documentation site                                           |
| `vp run -F @effect-agent/example-demo dev` | Run the chat-first demo and Phase 2 simulator on port 4173                     |
| `vp run ready`                             | Run check, test, and build; this is the local and CI handoff gate              |
| `vp fmt`                                   | Apply repository formatting                                                    |

Use `vp check` for static checks, `vp fmt --check` for formatting, `vp lint` for linting, and
`vp test` for the root test runner. `vp run test` dispatches all workspace suites, including the
Cloudflare pool tasks. `vp run ready` is the required handoff gate. If command or runtime setup
fails, run `vp env doctor` and include its output when asking for help.

Vite+ owns shared configuration in `vite.config.ts`. Package commands remain in their manifests
or Vite task configs so Vite+ can discover and cache them from the workspace dependency graph.
The root declares the catalog's Vite+ core alias as `vite` as well as `vite-plus`; Bun needs that
direct peer provider for the Vite+ toolchain. VitePress intentionally resolves the official Vite
implementation as its nested dependency; the repository therefore does not use a global `vite`
override. The documentation build is part of the root `build` and `ready` gates.

Tests import workspace source directly and do not consume other test tasks' outputs. The test
command therefore removes package dependency ordering while retaining a four-task concurrency
limit. Build tasks still run in dependency order. Every existing suite, including process-kill,
soak, and adapter conformance tests, remains in the ordinary test command.

Vite Task caches successful suites and fingerprints the files they read, including imported
workspace source and installed dependencies. Vitest's on-disk result cache is disabled in each test
config: its mutable `results.json` otherwise prevents tasks from being cached or reused on a fresh
runner. CI transfers only `node_modules/.vite/task-cache`, not Vitest result directories.
The direct Vitest tasks exclude generated Vite files and `node_modules` directory listings from
their inputs. Installed dependency files remain tracked, and the lockfile additionally invalidates
results when dependencies are added or removed. These tasks restore no generated files.
Use `vp run -v test` to see each cache decision, `vp run --last-details` to inspect the last run,
and `vp run --no-cache test` to execute every suite again.

## Releasing to npm

Versioning uses changesets (`vp run changeset`, `vp run changeset:version`);
publishing uses `vp run release:publish`, not `changeset publish`. In a
non-pnpm repository changesets shells out to `npm publish`, which ships
`workspace:*` and `catalog:` protocol ranges verbatim; `bun publish` resolves
both at publish time. The release script also swaps each manifest's
source-first export map used in the workspace for the
built `dist` entries during the publish, restores it afterwards, and skips
versions already on the registry during manual publication. CI pack mode still
builds those tarballs so the isolated publisher can compare their integrity and
safely resume a partial release.

The public alpha ships on the **beta channel**. "Alpha" describes product maturity; it does not
select a different npm tag. The repository is in
changesets pre mode (`.changeset/pre.json`, tag `beta`), so `changeset
version` produces `X.Y.Z-beta.N` versions, and the release script publishes
any prerelease under the matching npm dist-tag, never `latest`. Consumers install the
platform-neutral `effect-agent@beta` umbrella or individual scoped packages, with optional
adapters installed separately, as described in the [getting-started guide](guide/getting-started.md).
Leaving prerelease mode for a future stable release requires an explicit release decision and
`vp run changeset pre exit` before the normal sequence. Alpha documentation changes do not change
tags or publish a release.

All public framework workspaces form one Changesets `fixed` group. Any package changeset advances
all fourteen packages to one shared version, including packages with no behavioral change in that
release. This deliberately spends additional package versions in exchange for a single compatible
release coordinate across the framework. When adding a public package, add its manifest name to
the fixed group in `.changeset/config.json`; the toolchain test fails if the workspace and group
diverge.

Releases are automated: on every push to `main`, `.github/workflows/release.yml`
maintains a "Version Packages (beta)" PR from the pending changesets. When no pending changeset
remains, the workflow classifies the pushed tree against a clean Changesets regeneration from its
first parent; only that exact generated tree may enter the publish jobs. An ordinary no-changeset
push is not release authority. Merging the version PR publishes through npm trusted publishing. The workflow's OIDC
identity is exchanged for short-lived credentials (no npm token, no OTP), with
provenance attached. The generated PR is release metadata over code that
already passed the ordinary PR gates: its Static checks, Tests, Build, and
agentic Review workflows use `paths-ignore` for those generated files.
The trusted main-branch Release workflow independently resolves the PR returned
by Changesets, validates its repository, branch, base, and commit identities,
runs `changeset version` and lockfile generation in a temporary worktree, and
requires the resulting complete Git tree to match the proposed head
byte-for-byte. It posts the required `ready` check directly to that immutable
head only after successful verification. GitHub requires approval for matching
`pull_request` workflows triggered by `GITHUB_TOKEN` on opened, synchronize, or
reopened events. The path filters prevent those runs and their approval prompt
for generated-only changes. Explicit `@effect-agent review` comments still work.
An unexpected path in an automated Changesets update is rejected by exact-tree
verification and receives a failing `ready`; path routing is not its security boundary. A later human
mutation with an unexpected path invokes the ordinary PR workflows, while a
generated-only mutation has a new head without a verified check and remains
unmergeable. The exact-tree verifier runs in a fresh read-only job checked out from the triggering
`main` SHA; the Changesets action's workspace never reaches it. Inside the detached trusted-base
worktree, the verifier performs a script-suppressed frozen install before invoking that worktree's
Changesets binary, then performs a second script-suppressed install only to regenerate the release
lockfile. Cleanup attempts every Git and filesystem operation and reports any cleanup failure in
the typed verification result instead of silently accepting a leaked worktree.

The version job and every external action are pinned to full commit SHAs and have neither Checks
API nor npm OIDC permission. The action-free reporting job alone has `checks: write`; immediately
before publishing the check it re-reads the PR head, source, and base and confirms that the active
`main` rules require `ready` with strict up-to-date enforcement. If lineage moved, verification did
not complete, or that rule is absent, it posts a failing `ready` conclusion. Operators must preserve
the ruleset's `strict_required_status_checks_policy` setting: it invalidates the head-bound success
for merge purposes whenever `main` later advances, until Changesets regenerates from the new base.

After the version PR merge passes that exact-tree classification, an unprivileged preparation job
frozen-installs and rebuilds the exact versioned tree, packs every public workspace with Bun into a scope-owned staging directory, and
atomically renames that directory into place only after every tarball and the manifest are
complete. Failure or interruption removes the partial staging tree, so a retry never inherits an
incomplete release artifact. The atomic rename itself is uninterruptible, and a failed preparation,
commit, cleanup, or temporary manifest installation/restoration remains a typed release failure
with preceding causes preserved. Even a partial installation failure attempts to restore the
original manifest bytes before returning. The job then uploads one checksummed immutable artifact. A separate
action-free job is the sole holder of `id-token: write`. It checks the artifact
digests, requires the release manifest to contain the exact fourteen-package fixed set at one
`X.Y.Z-beta.N` version and the policy-owned `beta` dist-tag, verifies a pinned npm CLI tarball, and
publishes with provenance; it does not check out repository code, install dependencies, run a
build, or invoke a repository script. Per-package verify-and-publish pipelines fan out as
concurrent background jobs (each npm process performs its own OIDC exchange); every job is
awaited, per-package logs replay in manifest order, and any failure fails the step only after the
full sweep. If a same-version registry entry appears during a retry, the
job skips it only when its SRI integrity matches the prepared tarball and the registry's `beta` tag
already selects that version. A final action-free job has tag-write but no OIDC authority and
creates only validated framework-package tags at the triggering `main` SHA; an existing lightweight
or annotated tag must dereference to that exact commit.

Each package on npmjs.com lists `release.yml` in `danieljvdm/effect-agent` as
its trusted publisher (package Settings, a one-time registration; "Allow npm
publish" only). In CI the release script runs in `--pack-directory` mode in
the unprivileged preparation job: `bun pm pack` resolves the
`workspace:`/`catalog:` protocols into the tarball without consulting the npm registry. The
transferred manifest accepts only safe `packages/<basename>.tgz` relative paths. The isolated publisher
then uploads that tarball with the pinned npm CLI, because only npm implements
the OIDC exchange.

The manual fallback from an authenticated npm session (`bunx npm login`, an
owner of the `@effect-agent` scope):

1. `vp run changeset` describes the consumer-visible change;
2. `vp run changeset:version` cuts versions and changelogs, then `vp install` updates the lockfile;
3. `vp run ready`;
4. `vp run release:publish --dry-run`, then without `--dry-run`
   (append `--otp <code>` when npm 2FA asks);
5. `vp run changeset tag`, then `git push --follow-tags`.

All fourteen packages publish under the MIT license (owner decision
2026-08-14). The Cloudflare pair
joined the channel after their declaration-emit fix: the Durable Object
class factory carries an explicit `ConversationObject.Class` return type
because TS4094 rejects inferring an exported anonymous class type around a
private field, and both packages pin `pack: { dts, sourcemap }` in their
Vite configs since a package-level config suppresses `vp pack`'s
zero-config defaults.

## Script runners

Run repository scripts through `vp run <task>`. Their package-script entrypoints use **Bun**
with `bun scripts/<name>.ts`. There is no
`tsx` in this repository. The one exception is anything that imports
`@effect-agent/storage-sqlite`: Bun does not implement `node:sqlite`, so
`admin:durable` runs under `node --experimental-transform-types` (plain
strip-only mode rejects the framework's runtime `namespace` declarations),
and the platform-node crash/soak harnesses spawn their TypeScript worker
entries the same way (verified: the full process-kill matrix passes under
transform-mode workers).

## Post-install setup

`vp install` runs `vp config --no-agent --hooks-dir .vite-hooks` through the root `prepare` script,
which materializes the repository's `.vite-hooks` Git hook dispatcher. It does not touch the
compiler patch; that remains a separate, explicit step.

`vp run patch:tsgo` (`scripts/patch-effect-tsgo.ts`) applies the Effect TypeScript-Go compiler
patch standalone, verifying the installed `@effect/tsgo` and `typescript` versions are the exact
pinned pair before patching.

CI installs with lifecycle scripts suppressed and then runs the `patch:tsgo` task explicitly in
every job that checks, tests, or builds TypeScript. This preserves the compiler-patch invariant
without any source checkout. Contributor tooling reads the installed Effect source from
`node_modules/effect` when it needs to verify current Effect v4 or Effect AI behavior.

Known workaround: the `preferTypedSchemaDecoder` language-service rule is set to `"off"` in
`tsconfig.base.json` because `@effect/tsgo` `0.33.0` nil-panics in that rule
(`internal/rules/prefer_typed_schema_decoder.go:37`) while checking `packages/session`. Re-enable
it once a fixed tsgo lands.

To upgrade Effect:

1. update every Effect-family catalog entry in root `package.json` as one change;
2. run `vp install`;
3. run `vp run ready`, including the provider example's compilation;
4. run any relevant opt-in live-provider checks with host credentials, as described by their examples;
5. commit the catalog and lockfile together.

## Contributor agent skills

Contributor skills under `.agents/skills` (symlinked from `.claude/skills`) are repo-owned copies
from the [`@danieljvdm/dev-kit`](https://github.com/danieljvdm/dev-kit) catalog, each tracked by
its own `.dev-kit-origin.json` receipt instead of a shared manifest or lockfile. Check for upstream
updates with `bunx @danieljvdm/dev-kit@latest skills status`; fast-forward an unmodified skill with
`skills update <name>`, which leaves a locally edited copy for an agent merge instead of
overwriting it. Add a new skill from the approved catalog with `skills add <name>`. Use the
`dev-kit` skill before changing these outputs.

These are contributor instructions and must not be imported by a framework package. No runtime
Skill API is currently implemented.

## Adding a package

Create a package only for a genuinely new framework concern, agreed with the repository owner in
the pull request that introduces it:

1. add `packages/<name>/package.json`, `src/index.ts`, and `tsconfig.json`;
2. use the working `@effect-agent/<name>` scope with the sibling manifest shape (MIT,
   `publishConfig.access: public`, source-first exports). Packages publish on the `beta`
   dist-tag, and each new package needs its one-time npm trusted-publisher
   registration before CI can publish it; add the package to the single fixed release group in
   `.changeset/config.json`;
3. use `catalog:` for shared external dependencies and `workspace:*` for internal packages;
4. extend the root `tsconfig.base.json`;
5. add it to root `tsconfig.json` references;
6. declare only inward workspace dependencies;
7. provide `check`, `test`, and `build` scripts when the package has those behaviors;
8. update existing guides and API comments affected by the change;
9. run `vp run ready`.

Workspace export maps point to source. `vp run build` dispatches package builds through `vp pack`.
The release script installs `dist` export maps while packing or publishing and restores the
workspace manifests afterwards. Public files, Changesets versioning, provenance, and release
automation are already configured as described above.

## CI and hooks

The CI workflow runs the full gate on pull requests. Ordinary PRs install
the exact Bun version with a frozen-lockfile install, then run static checks, tests, and builds
in parallel, with a fan-in job that keeps the required branch-protection check named `ready`.
The test matrix gives `platform-node`, `testing`, `platform-cloudflare`, and `storage-cloudflare`
separate runners. Its remaining-workspace job includes every other package, including new
packages. Each runner executes one package test task at a time, because every task already
starts its own Vitest worker pool. File isolation, test counts, and timeouts remain unchanged.
The exact internal Changesets release PR is the only exception: CI and PR Review path-filter the exact set of files
Changesets may generate, avoiding approval-required workflow runs on `GITHUB_TOKEN` updates.
The trusted Release run validates the PR lineage, regenerates the complete tree from its checked-out
`main` commit, and creates the required `ready` check on the verified head through the Checks API.
The check is success only after an exact-tree comparison in a fresh read-only job; unexpected files,
setup failures, or any other incomplete verification post failure to the current PR head. An invalid
or unresolvable PR identity produces no success. For later human updates, unexpected paths invoke
the ordinary workflows, while a generated-path-only push receives no new `ready` check. Checks
authority is isolated to an action-free job that revalidates the live source/head/base and the
strict up-to-date branch rule before reporting. Both cases are fail-closed.

Each ordinary-PR job restores and saves the Vite Task cache
(`node_modules/.vite/task-cache`), so later synchronize events on the same PR can replay
per-package gates whose fingerprinted inputs did not change. Test matrix jobs have separate
cache keys so their snapshots cannot shadow each other. They also save successful task results
when another task fails; Vite Task never caches the failed task. GitHub scopes those caches to the
PR's merge ref. Main pushes run only the test matrix to publish shared baselines for new PRs;
static checks, builds, and the `ready` fan-in remain PR-only. Main test runs finish independently
so a newer push cannot repeatedly cancel cache publication. No test is skipped based on a changed-path
list: Vite Task validates each restored task's inputs before replay. The CI workflow does not
initialize any source submodule. The separate Release workflow continues to own version-PR
maintenance and publishing.

The Vite+ pre-commit hook runs `vp check --fix` on staged JavaScript and
TypeScript. CI remains authoritative for the full `check` gate, including
per-package `tsc` and action-bundle freshness.

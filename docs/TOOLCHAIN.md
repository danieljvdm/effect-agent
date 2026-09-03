# Repository toolchain

Use Vite+ for repository commands. Bun is the package manager and script runtime.
Framework packages live in `packages/*`; runnable examples live in `examples/*`.

## Versions {#source-of-truth-versions}

The root [package.json](../package.json) owns shared dependency versions.
Workspace manifests use `catalog:` for those dependencies and `workspace:*` for internal packages.
Commit the Bun lockfile; CI installs with `--frozen-lockfile`.

| Tool                                                    | Repository version   |
| ------------------------------------------------------- | -------------------- |
| Bun                                                     | `1.4.0`              |
| Vite+                                                   | `0.3.0`              |
| Effect and its provider/platform/SQL/Atom/test packages | `4.0.0-rc.111`       |
| `effect-cf`                                             | `0.37.0`             |
| TypeScript                                              | `7.0.2`              |
| `@effect/tsgo`                                          | `0.33.0`             |
| Node.js                                                 | `22.18+` or `24.11+` |

Public packages require `effect@^4.0.0-rc.111` as a peer. The exact catalog pin supplies the
development version. Raise the peer minimum when code needs a newer API.
Private examples declare Effect as a regular dependency. Adapters depend on the platform and
SQL implementations they use.

`platform-cloudflare` requires `effect-cf@^0.37.0` as a host peer and uses the exact catalog
version for development. Its D1 peer is aligned with Effect. Consumers provide the shared runtime.

Root overrides keep Effect, its Node platform, and Vitest on one version.
The docs deployment runs Alchemy under Bun, so the root also installs Alchemy's
optional `@effect/platform-bun` peer at the shared Effect version.
Vite+ supplies Vitest except in the two Cloudflare packages, whose Workers pool requires a
direct catalog-pinned Vitest dependency and a Vite task. Run those tasks through `vp run`.
The Code Mode example also uses a Vite task for its Miniflare tests.

VitePress uses its own Vite dependency. Keep the root Vite+ core alias required by Vite+;
do not add a global Vite override.

## Current workspace

See the [package map](reference/packages.md) for public packages and capabilities.

| Directory                           | Purpose                                               |
| ----------------------------------- | ----------------------------------------------------- |
| `packages/*`                        | Framework and private PR-review integration packages  |
| `examples/demo`                     | Local browser app                                     |
| `examples/cloudflare-memory`        | Opt-in deployed Thread-to-Memory latency benchmark    |
| `examples/providers`                | Provider bindings and persistent-history example      |
| `examples/repo-ops`                 | Repository evidence auditor                           |
| `examples/browser-run-worker-proof` | Opt-in hosted Browser Run verification; owns Wrangler |
| `examples/pr-review-eval`           | Opt-in live review evaluation                         |
| `examples/code-mode-cloudflare`     | Generated JavaScript over a SQLite DO warehouse       |
| `action/`                           | PR-review Action contract and ignored build output    |

Framework code stays in `packages/*`. Examples are leaf workspaces.
Provider integrations come from upstream Effect AI Layers.

```text
core <- engine <- capabilities
core <- sandbox <- sandbox-local
core <- sandbox <- capabilities
core <- engine <- thread <- storage adapters
engine + thread + sandbox + adapters <- platform packages
core + engine <- testing
core + engine + capabilities <- effect-agent <- pr-review
```

Arrows point toward dependencies. An inward package must not import an outward one.
Shared compiler settings live in `tsconfig.base.json`.

## Commands

Run `vp help` or `vp <command> --help` for options.

| Command                                    | Use                                                   |
| ------------------------------------------ | ----------------------------------------------------- |
| `vp install`                               | Install dependencies and hooks                        |
| `vp check`                                 | Format, lint, and type checks                         |
| `vp fmt` / `vp fmt --check`                | Format files / check formatting                       |
| `vp lint` / `vp lint --fix`                | Lint / apply fixes                                    |
| `vp test`                                  | Root test runner                                      |
| `vp run check`                             | All static checks, package types, scripts, and purity |
| `vp run test`                              | All workspace suites, including Cloudflare            |
| `vp run build`                             | Package, docs, and Action builds                      |
| `vp run ready`                             | Full handoff gate: check, test, build                 |
| `vp run docs:dev`                          | Docs development server                               |
| `vp run docs:build`                        | Build docs and check links                            |
| `vp run docs:preview`                      | Preview built docs                                    |
| `vp run -F @effect-agent/example-demo dev` | Browser demo on port 4173                             |
| `vp env doctor`                            | Diagnose toolchain setup                              |

Use `vp run <task>` for other scripts. Do not use `bun run`, `npm run`, `pnpm run`,
`yarn run`, or invoke the wrapped compiler, formatter, linter, or test runner directly.
Include `vp env doctor` output when asking for toolchain help.

Tests run with at most four workspace tasks at once and without dependency ordering.
Builds follow dependency order. Process-kill, soak, and adapter contract suites are part of
the ordinary test command.

Vite Task caches successful results against their inputs. Vitest's mutable result cache is
disabled so it does not invalidate task caching. CI transfers `node_modules/.vite/task-cache`.
Direct Vitest tasks exclude generated Vite files and dependency directory listings, but track
imported dependency files and the lockfile. Failed tasks are never cached.

Use `vp run -v test` for cache decisions, `vp run --last-details` for the previous run,
or `vp run --no-cache test` to rerun every suite.

## Storage certification reports

The repository runs the same certification against memory, SQLite, and Cloudflare adapters. Set
`EFFECT_AGENT_CERTIFICATION_OUT` in a Node certification test to write a local schema-encoded
report. Set `PRINT_REPORT` in the Cloudflare test file to print its workerd report.

## Documentation examples

The homepage imports `docs/snippets/travel-planner/*.ts`.
Edit those files to change its examples. A `twoslash` fence enables type hovers and compiler
validation during `vp run docs:build`. Relative imports resolve from that snippet directory.

Twoslash uses the pinned `typescript-twoslash` JavaScript compiler API; repository checks use
TypeScript 7. Production builds reuse Twoslash compiler and filesystem caches within the process;
the dev server disables both so imported snippet edits remain visible.
Keep compiler validation enabled. Do not suppress errors with `noErrors` or
`noErrorValidation`.

Keep `yield*` inside a generator, such as `Effect.gen(function* () { ... })`.
Outside a generator, the formatter parses `*` as multiplication and inserts spaces.

## Link previews

The docs config adds Open Graph and Twitter metadata to the built HTML. Each page uses its
resolved title and description, a canonical URL on `https://effect-agent.com`, and the shared
`docs/public/social-card.png`. Preview crawlers do not need JavaScript.

Edit `docs/.vitepress/assets/social-card.html` to change the artwork. Serve the repository root
locally, open that file in a browser, wait for `document.fonts.ready`, and capture its `main`
element at 1200 × 630 CSS pixels with device scale 1. Save the PNG to
`docs/public/social-card.png`. The source uses the installed IBM Plex fonts and `docs/public/mark.svg`;
it is not published. The 32 × 32 favicon and 180 × 180 Apple touch icon are PNG renders of that mark.

Run `vp run docs:build` and inspect the generated HTML for the homepage, a guide, and a directory
index such as `platforms/index.html`. Image URLs must be absolute, and canonical URLs must match
the site's clean routes. Existing messages may retain a cached preview after a deployment.

## Releasing to npm

All fourteen public packages share one Changesets fixed group and publish to `beta`
as `X.Y.Z-beta.N`. Keep the group in `.changeset/config.json` aligned with public workspaces.
The project is in prerelease mode. Leaving it requires an explicit release decision and
`vp run changeset pre exit`.

Use `vp run changeset` to describe a consumer-visible change.
On pushes to `main`, `.github/workflows/release.yml` maintains the version PR.
After that PR merges, the workflow publishes through npm trusted publishing with provenance.

The release PR runs the same static checks, tests, and builds as every other PR. The workflow
uses the existing Effect Agent GitHub App to create and update it, so those pushes trigger
ordinary CI. Keep the App's contents and pull-request write permissions enabled and configure
the `EFFECT_AGENT_APP_ID` and `EFFECT_AGENT_APP_PRIVATE_KEY` repository secrets.
The checkout disables persisted credentials so Changesets uses the App token.

After merge, Changesets handles registry version checks, publishing, package tags, and GitHub
releases. The publish command builds the workspace and temporarily prepares npm-ready manifests:
source exports point at built files, `workspace:*` dependencies use the current workspace
versions, and `catalog:` dependencies use the root catalog. All source manifests are restored
on success, failure, or interruption. npm publishes through OIDC with provenance; each package
must list `release.yml` in `danieljvdm/effect-agent` as its trusted publisher.

Changesets defaults packages with no stable release to `latest`. The adapter temporarily marks
the prerelease state as exiting while running `changeset publish --tag beta`, then restores it.
It never runs versioning in that state, so versions and the ongoing beta release train stay intact.

The release workflow does not require strict up-to-date branch rules or post a separate
`ready` check. Changesets refreshes the version PR as changes land on `main`.

For an authenticated manual release:

1. Run `vp run changeset`.
2. Run `vp run changeset:version`, then `vp install`.
3. Run `vp run ready`.
4. Run `vp run release:publish --dry-run`, then `vp run release:publish`.
   Add `--otp <code>` if npm requests it.
5. Run `git push --follow-tags` to push the tags created by Changesets.

Use `release:publish` so npm receives built exports and resolved dependencies.
Its dry run builds and inspects packages without publishing or creating tags.
All public packages use the MIT license.

## Script runners

Package scripts use Bun through `vp run`.
Scripts that import `@effect-agent/storage-sqlite` continue to use
`node --experimental-transform-types` to exercise the Node host runtime.
Strip-only execution cannot handle the framework's runtime namespaces.
This includes `admin:durable` and the Node crash/soak workers.

## Post-install setup

`vp install` runs `vp config --no-agent --hooks-dir .vite-hooks` through `prepare`.
Apply the compiler patch separately with `vp run patch:tsgo`.
It checks the installed TypeScript and `@effect/tsgo` versions before patching.

CI suppresses lifecycle scripts, then explicitly patches the compiler in jobs that check, test,
or build TypeScript. Read installed Effect sources in `node_modules/effect`.

`preferTypedSchemaDecoder` is disabled in `tsconfig.base.json` because `@effect/tsgo@0.33.0`
panics in that rule while checking thread. Re-enable it after the upstream fix.

To upgrade Effect:

1. Update all Effect-family catalog entries together.
2. Run `vp install`.
3. Run `vp run ready`, including provider example compilation.
4. Run applicable opt-in provider checks with host credentials.
5. Commit the catalog and lockfile together.

## Contributor agent skills

`.agents/skills` contains repo-owned Dev Kit skills, tracked by individual
`.dev-kit-origin.json` receipts and linked from `.claude/skills`.

- Check updates with `bunx @danieljvdm/dev-kit@latest skills status`.
- Update an unmodified skill with `bunx @danieljvdm/dev-kit@latest skills update <name>`.
- Add an approved skill with `bunx @danieljvdm/dev-kit@latest skills add <name>`.

The CLI leaves locally edited skills for an agent merge. Use the `dev-kit` skill before
changing these files. Contributor skills are tooling; framework packages must not import them.

## Adding a package

Get owner agreement before adding a new framework concern.

1. Add `packages/<name>/package.json`, `src/index.ts`, and `tsconfig.json`.
2. Match sibling manifests: MIT license, public publishing, and source-first exports.
3. Add the package to the Changesets fixed group and configure its npm trusted publisher.
4. Use `catalog:` and `workspace:*` dependencies with the required inward direction.
5. Extend `tsconfig.base.json` and add a root `tsconfig.json` project reference.
6. Add applicable `check`, `test`, and `build` tasks and update the guides.
7. Run `vp run ready`.

`vp run build` dispatches `vp pack`. A package-level Vite config overrides zero-config pack
defaults, so declare `dts` and `sourcemap` there when needed.

## CI and hooks

PR CI runs static checks, tests, and builds, then reports the required `ready` result.
Cloudflare storage, Cloudflare platform, Node platform, and testing have dedicated test runners.
The remaining-workspace job includes every other package and runs one package task at a time.

The generated Changesets PR runs ordinary CI like other PRs.
Explicit `@effect-agent review` comments still request review.

PR Review uses `pull_request_target` and runs only trusted default-branch code.
Fork reviews wait for approval before checkout, token creation, or model execution.
Open the PR Review run from the PR's checks, select **Review deployments**, select
`pr-review-forks`, then **Approve and deploy**. GitHub uses deployment wording for
this approval gate, but the job does not deploy anything or create deployment records.
Approving an ordinary fork workflow does not grant it repository secrets.

Before enabling this workflow, configure **Settings → Environments → pr-review-forks**
with repository maintainers as required reviewers. The current reviewer is `danieljvdm`;
update this list when maintainers change. Allow self-review so a maintainer can approve
their own fork PR. Keep this environment and its required-reviewer rule in place;
a missing environment is automatically created without protection by GitHub.
The separate `pr-review` environment has no approval requirement and is used for
same-repository PRs and authorized review comments. Both environments use
`deployment: false` to avoid adding review runs to deployment history.

Each fork PR update requires approval. The Action's expected-head check skips an
approved run if its PR head has since changed. Comment-triggered reviews retain their
existing maintainer authorization and do not require a second approval. Never check
out, install dependencies from, or execute the PR head in this secret-bearing workflow;
the reviewer reads untrusted source through GitHub's API instead.

Each test-matrix job has its own task-cache key. Successful task results are saved even when
another task fails. Main pushes run static checks, tests, and builds to populate shared caches
and validate Action releases. The `ready` fan-in runs only on PRs. Main runs are not cancelled
by newer pushes.
Tests are reused only when task inputs match, never solely because paths did not change.

The pre-commit hook runs `vp check --fix` on staged JavaScript and TypeScript.
CI runs the full gate, including package type checks and the Action build.

Action bundles use the catalog-pinned esbuild. `vp run action:build` writes ignored
output to `action/dist/index.mjs` and checks its Node.js syntax. The root build task
also builds the Action, so every PR validates bundling without committing generated
JavaScript. There is no bundle freshness check or input-hash manifest.

On successful `main` runs, CI publishes the exact build artifact in a child commit
of the validated source. It creates `action-<source-commit-sha>` and advances
`action-v1` in one atomic push, without changing `main`. Only the publication job
has repository write permission; it installs no dependencies and runs no project
code. Superseded source commits are skipped, and a Git lease prevents competing
publishers from overwriting a newer channel. Failed publication preserves the last
release and can be retried by rerunning the failed CI job.

Consumers use `danieljvdm/effect-agent/action@action-v1` or pin the distribution
commit SHA printed in the CI summary. New source commits and `@main` no longer
contain a runnable bundle; older SHA pins still work. Before the initial cutover, seed `action-v1` with the
last validated source commit that still contains the bundle, then migrate existing
workflows. The first successful main CI run publishes the new distribution commits.
These tags are independent of npm package releases.

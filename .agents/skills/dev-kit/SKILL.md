---
name: dev-kit
description: Dev-kit operations for projects that configure dev-kit.jsonc, sync portable skills, run plan/apply or automatic postinstalls, perform locked CI checks, maintain dev-kit.lock.json, resolve ownership conflicts, or enable Effect TypeScript-Go.
---

# Dev Kit

Treat `dev-kit.jsonc` as desired state, `dev-kit.lock.json` as the committed
resolution, and `.dev-kit/state.json` as local ownership receipts. Deep
reference — catalog maintenance, package-skill discovery rules, preset
internals — lives in the package README
(`node_modules/@danieljvdm/dev-kit/README.md`).

Use the high-level commands for routine changes: `dev-kit init`, `dev-kit add
<skill...>`, `dev-kit remove <skill...>`, `dev-kit list --all`, `dev-kit search
<words...>`, and `dev-kit info <skill>`. Add and remove apply immediately unless
passed `--no-apply`; `dev-kit sync` applies an already-edited manifest.

## Apply loop

1. Establish the Git root. Read project agent instructions, the current
   manifest and lock, package and workspace manifests, framework and tool
   configuration, representative source boundaries, and CI workflows. Build a
   concrete inventory of the platforms, frameworks, tools, and workflows the
   repository actually uses; do not infer capabilities from a product or
   company name alone.
2. Run `dev-kit list --all`, then use `dev-kit search <terms>` and `dev-kit info
<skill>` for each capability in the inventory. Compare every candidate's
   trigger description with concrete repository evidence. Keep explicitly
   requested creative or advisory skills even when they have no mechanical
   dependency signal.
3. Choose the narrowest useful set. Prefer focused external skills over a
   generic umbrella; select an umbrella or source family only when its full
   breadth is intentionally useful, never because one member matches. Explain
   any uncertain inclusion before applying it. Unused `references/` folders
   inside a selected skill cost repository space, not agent context, so a
   multi-product repository can justify an umbrella while still excluding
   unrelated top-level skills.
4. Update `dev-kit.jsonc`. Preserve JSONC comments and validate against the
   package schema. Finish with each desired resource represented once and every
   external selection supported by repository evidence or an explicit request.
5. Run `dev-kit plan`. Use `--manifest`, `--project-dir`, or `--lockfile` when
   the project overrides their defaults. Planning is read-only; inspect every
   create, update, remove, adoption, and conflict before proceeding. Finish
   when the plan contains only intended actions and understood conflicts.
6. Resolve conflicts, then run `dev-kit apply`. Commit the manifest and
   regenerated `dev-kit.lock.json`; keep `.dev-kit/` local. Finish when a second
   plan reports only unchanged resources and setup tasks.

## Manifest

Use skill names or family names in `include`; subtract selections with
`exclude`. Built-in families such as `effect` are intentional bundles. Include
this skill as `dev-kit` when project agents should carry the toolkit
procedure. Skills bundled inside installed packages need the exact
`<package>#<skill>` selector; the copied output flattens that identity into
one directory name (`@tanstack/table-core#core` → `tanstack-table-core-core`)
and rewrites the copied frontmatter `name:` to match.

```jsonc
{
  "$schema": "./node_modules/@danieljvdm/dev-kit/schema/dev-kit.schema.json",
  "include": ["dev-kit", "effect"],
  "setup": {
    "agentInstructions": { "enabled": true },
    "claudeInstructions": { "enabled": true },
    "vitePlus": {
      "hooks": { "enabled": true },
      "workflow": { "enabled": true },
    },
    "worktrunk": {
      "config": { "enabled": true },
    },
  },
  "targets": {
    "agents": { "enabled": true, "mode": "copy" },
    "claude": { "enabled": true, "mode": "symlink" },
  },
}
```

Prefer a copied `.agents/skills` target as the project-local source of truth;
use symlinks for additional harness discovery paths. Keep every target path
project-relative and separate from the manifest, lock, and state paths.

## Managed instructions

`setup.agentInstructions` manages marked sections in the project-root
`AGENTS.md` and preserves handwritten content around them; edit only outside
the markers. The section points agents at this skill, renders the repository's
command policy from live evidence (a direct `vite-plus` dependency makes `vp`
the only front door; otherwise Bun runs declared root scripts), and adds the
Effect guide pointer when the installed `effect` package ships one. Treat
duplicate, overlapping, reversed, or unmatched managed markers as a conflict
rather than guessing which content Dev Kit owns. Never substitute another
script runner or call raw `tsc`, test, lint, or format binaries when a project
command exists.

`setup.claudeInstructions` manages `CLAUDE.md` as a relative symlink to
`AGENTS.md`. Disabling either task removes only unchanged managed content and
leaves handwritten content in place.

## Vite+ setup

The repository always owns `vite.config.ts`. Compose
`createRecommendedVitePlusConfig` from `@danieljvdm/dev-kit/vite-plus` there,
adding repository/framework-generated paths (and any overridden harness target
paths) through `ignorePatterns`. Spread the returned top-level config before
local options, spread a returned nested block before overriding it, and merge
nested collections such as `lint.rules` so the recommended rules stay active.
The factory provides matching Oxlint/Oxfmt ignores, staged checks, and
separate `check` and pure `typecheck` tasks; standalone Oxc projects import
`recommendedOxlintConfig`/`recommendedOxfmtConfig` directly.

Run the Effect-patched compiler separately with `vp run typecheck`; neither
Oxlint's bundled `tsgolint` nor Vite+'s native lint path uses the Effect patch,
and Vite+ 0.2.6 does not execute the preset's JavaScript-plugin rules — use
standalone Oxlint when `effect/*` or
`stylistic/padding-line-between-statements` must be enforced. The `effect`
plugin's scope-sensitive rules stay consumer-scoped: enable them per path in
Effect-owned code, with exceptions for tests and host boundaries.

`setup.vitePlus.hooks` converges the Git-ignored `.vite-hooks/_` dispatcher by
running the project-local `vp config --no-agent`, recreating it in linked
worktrees. It requires a direct `vite-plus` dependency, refuses to replace an
unrelated `core.hooksPath`, and is skipped per invocation with
`VITE_GIT_HOOKS=0` or `HUSKY=0`.

## Scaffolds

`setup.vitePlus.workflow` and `setup.worktrunk.config` are create-only
scaffolds: apply writes the file only when it is missing, records nothing in
the lock, and never reads, updates, or removes an existing file — the
repository owns it from creation and edits it directly. When the shipped
template improves, diff the repository's file against the installed template
under `node_modules/@danieljvdm/dev-kit/templates/` and merge what fits.

- `setup.vitePlus.workflow` scaffolds `.github/workflows/check.yml`. It
  requires direct Dev Kit, compatible Vite+, Effect, Effect TypeScript-Go, and
  native TypeScript dependencies with `setup.effectTsgo.enabled`. Add
  preparation steps or a custom typecheck command by editing the YAML.
- `setup.worktrunk.config` scaffolds `.config/wt.toml`: a
  copy-ignored-then-install pre-start pipeline, a full-validation pre-merge
  hook, and a commented per-worktree dev-server block to enable deliberately.
  Hook commands render for the repository's runner — `vp` with a direct
  `vite-plus` dependency, otherwise the detected package manager's install
  command with `bun run check` from a declared root `check` script. Keep
  user-level Worktrunk settings such as worktree-path templates out of the
  project config; each user approves the hooks once with
  `wt config approvals add`.

## Ownership and conflicts

Dev-kit adopts an existing destination only when its digest exactly matches a
committed lock entry. Local receipts authorize later updates and cleanup only
while the managed output still matches its recorded digest.

Preserve a conflicting path and inspect it:

- For an unknown destination, choose a different target or deliberately move
  the user-owned content before applying.
- For a modified managed destination, reconcile the local edits or restore its
  recorded content before applying.
- For a locked-plan mismatch, run an unlocked apply only when intentionally
  updating desired state, review the new lock, and commit it.

Retain `.dev-kit/state.json` across routine applies and branch changes so its
receipts can update or remove previously applied outputs safely.

## Lifecycle

Run `dev-kit gitignore` to add `.repos/` and `.dev-kit/` additively
(`--dry-run` to preview). For one lifecycle entry point, configure:

```jsonc
{
  "scripts": {
    "postinstall": "dev-kit apply",
  },
}
```

This intentionally refreshes the committed lock and owned outputs when an
installed Dev Kit or selected package-skill version changes; review and commit
those changes with the dependency update. Keep `dev-kit apply --locked` as a
verification command, never the local lifecycle, and never run an unlocked
apply before locked verification. Invoke locked consumer verification as
`bun ./node_modules/@danieljvdm/dev-kit/bin/dev-kit.mjs apply --locked` so a
package script named `dev-kit` cannot shadow the executable.

## Effect setup tasks

`setup.effectSource` converges the ignored `.repos/effect` checkout on the
`effect@<version>` tag matching the installed package. It skips CI, preserves
a dirty or unrelated destination, and never deletes the checkout when
disabled. Diagnose with `dev-kit effect sync --dry-run`; override
`packageName`, `path`, or `repository` only for a compatible Effect
distribution.

`setup.effectTsgo` validates and patches the project-local native TypeScript
compiler. Install the exact `@effect/tsgo` and `typescript` versions required
by the installed dev-kit, point `tsconfig.json` at
`./node_modules/@effect/tsgo/schema.json`, and configure the
`@effect/language-service` plugin with the `recommendedEffectTsgoPlugin`
profile — copy the exact JSON from the package README. In monorepos, child
`compilerOptions.plugins` arrays replace rather than merge the root array, so
workspace configs must inherit the root plugin without redeclaring it, and the
`src/**/*.ts` override must be relative to the config that contains it.
Diagnose with `dev-kit tsgo patch --dry-run`; use `--force` only after the
user accepts a potentially commit-incompatible TypeScript binary.

## Current boundary

Manage skill outputs, the `setup.agentInstructions` marked sections, the
`setup.claudeInstructions` link, the `setup.vitePlus.hooks` dispatcher, the
`setup.effectSource` checkout, and the explicit `setup.effectTsgo` task. The
`setup.vitePlus.workflow` and `setup.worktrunk.config` scaffolds belong to the
repository once created. `vite.config.ts`, dependency, and `tsconfig.json`
contributions remain deliberate user-owned edits.

# Legacy ejection

Treat ejection as a migration of ongoing behavior, followed by deterministic
release of Dev Kit ownership.

## Inventory

Read `dev-kit.jsonc`, `dev-kit.lock.json`, `.dev-kit/state.json` when present,
`package.json`, agent instructions, CI, Vite+/Oxlint/Oxfmt configuration, Git
hooks, Effect setup, and every `@danieljvdm/dev-kit` import or executable call.
Run:

```bash
bunx @danieljvdm/dev-kit@latest eject --dry-run
```

Finish the inventory with every managed skill, modified managed output, runtime
config import, lifecycle command, and enabled setup task accounted for.

## Materialize

Replace Dev Kit config imports with local repository-owned configuration. Copy
small custom plugins or helpers locally; keep a focused package dependency only
when the repository intentionally consumes that package at runtime.

Translate recurring setup tasks into repository-owned mechanisms before removing
the dependency. Effect compiler patching, source checkouts, Git hook generation,
and CI preparation must continue to work after a fresh clone and install.

Preserve agent instructions while removing the managed wrapper and obsolete
claims that Dev Kit owns the repository. Preserve local skill edits. The eject
command converts safe legacy skill outputs into tracked repo-owned skills and
adds origin receipts for optional future updates.

## Release ownership

Resolve every conflict reported by dry-run, then run:

```bash
bunx @danieljvdm/dev-kit@latest eject
```

Inspect the diff and run a fresh install plus the full repository validation.
Search again for Dev Kit imports, lifecycle calls, the manifest, the lock, and
managed markers. Finish when Dev Kit is absent from dependency resolution and
the repository retains all intended behavior.

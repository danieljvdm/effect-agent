# Effect scripts and CLIs

Use Effect for new executable TypeScript scripts in an Effect repository.
For an existing script, follow repository policy and the requested scope.
A small maintenance edit does not require converting the whole script;
undertake that migration when requested or necessary for the change.

When changing Effect behavior, read `node_modules/effect/AGENTS.md` and the
relevant CLI or process reference. A help-text-only edit in an existing script
needs its declared entrypoint and help output; it does not require API research.
Use installed signatures and sibling Effect programs when implementing behavior.

## Choose the reference

- Designing public arguments, flags, or subcommands:
  [command design](command-design.md).
- Files, environment, terminal, subprocesses, or runtime differences:
  [processes and platform](processes-and-platform.md).
- Executable startup, import safety, or verification:
  [entrypoints and testing](entrypoints-and-testing.md).

## Boundaries

Use Effect platform services for capabilities. When a required capability is
missing, isolate the runtime call in a typed Effect adapter. Keep fixed scripts
simple; they need no command tree when they expose no public command syntax.

Arguments and flags own CLI input; schemas own structured input and output.
Handlers adapt input and presentation, services own application behavior, and
Layers supply capabilities. One executable edge owns the runtime and signals.
Keep expected failures typed and preserve a nonzero exit for failure.

Keep stdout stable for machine-readable output and send diagnostics elsewhere.
Automation must be able to supply required inputs without a terminal. For
consequential mutations, expose a preview or dry-run when useful and honor
existing authorization. Prompt only on intentionally interactive paths.

Verify through the declared entrypoint with the repository's command authority.
Test only the changed behavior and follow repository policy for committed tests.

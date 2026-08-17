# Repository setup

## Inspect

Classify the work as an empty repository, an established repository, or a new
package inside an established workspace. Read the files that reveal:

- package manager and workspace topology;
- runtime and deployment targets;
- TypeScript inheritance and module boundaries;
- formatting, linting, typechecking, tests, builds, and Git hooks;
- CI and release expectations;
- existing agent instructions and skills.

For an empty repository, establish Git and package metadata first, install the
chosen toolchain, then read documentation shipped by those dependencies before
writing framework-specific source.

## Model

Turn requested capabilities into one repository model. Let features contribute
requirements, while shared configuration receives one coherent implementation.
For example, Worker bindings, Effect compiler settings, generated source ignores,
and workspace typechecking should converge in the same TypeScript and Vite+
design instead of being patched by isolated recipes.

Keep architectural choices proportional to evidence. Ask about product or
deployment decisions that change package boundaries. Derive routine names,
paths, and scripts from nearby conventions.

## Apply

Write ordinary project files without generated ownership markers. Prefer local,
readable configuration over a persistent preset dependency. Add focused runtime
or tooling dependencies only when the repository actually executes their code.

For an established repository, preserve working conventions and integrate the
requested capability at their natural seams. For an empty repository, create
the smallest runnable vertical slice before filling out auxiliary tooling.

Generate agent instructions from live project evidence. Record command authority,
Effect documentation routing when applicable, and non-obvious boundaries; leave
discoverable script names and directory listings to the repository itself.

## Verify

Install through the selected package manager, run the repository's full command
authority, and exercise a runtime or build entry point for each new application.
Inspect the final diff for placeholder text, unused dependencies, stale generated
paths, accidental Dev Kit imports, and configuration that only works in the
creating worktree.

Finish with a repository that can be cloned and operated without Dev Kit.

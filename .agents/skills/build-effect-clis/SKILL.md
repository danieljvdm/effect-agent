---
name: build-effect-clis
description: Write and maintain every executable script and command-line application in an Effect repository as an Effect program. Use when creating or changing any script, one-off automation, CI check, deploy/release/build glue, package-script entrypoint, CLI command, arguments, flags, prompts, child process, filesystem or environment workflow, Node/Bun entrypoint, or integration test—including extending an existing plain-TypeScript script.
---

# Build Effect Scripts and CLIs

## Effect-first scope

Every executable script and CLI in an Effect repository is an Effect program.
This includes one-off scripts, CI checks, deploy/release/build glue, files under
`scripts/`, package-script targets, migrations, and application entrypoints—not
only polished command-line tools.

Apply this rule when modifying code as well as when creating it. When a task
touches an existing plain-TypeScript script, convert the whole script to Effect
in the same change; matching the surrounding file's style or minimizing the
diff is not a valid exception. Prefer the repository's established Effect
patterns, including those in sibling scripts, over legacy patterns in the file
being converted.

Leave a script outside Effect only for a good, concrete technical or user
constraint that makes Effect unsuitable. Explicitly state that reason before
proceeding and in the final handoff, and keep the exception as narrow as
possible. Convenience, one-off status, and existing plain-TypeScript style are
not sufficient reasons.

Use Effect platform services for filesystem, path, environment, terminal, and
child-process work. Raw `node:*` or Bun runtime imports, `process.env`, `fs`,
`path`, `child_process`, and synchronous helpers such as `execFileSync` do not
belong in script workflows. If the installed Effect platform has no required
capability, isolate the runtime call in an explicit boundary adapter whose API
returns an Effect with typed errors, and document why that adapter is required.

Treat each executable as an Effect application. For a CLI, the `Command` tree
owns the user-facing contract, handlers adapt decoded input into application
workflows, services own capabilities, and the executable entrypoint supplies
platform Layers and runs the program. A fixed automation script with no public
arguments may export an Effect workflow directly instead of inventing a
`Command` tree. Do not introduce a separate CLI framework for command work.

Effect CLI and process APIs are version-sensitive. Read the target repository's
`node_modules/effect/AGENTS.md` completely, follow its CLI and child-process
references, and confirm exact signatures from the installed declarations before
editing.

## Build the executable boundary

1. Inventory the existing executable entrypoints, package scripts, command
   tree, shared flags, prompts, application services, platform Layers, output
   modes, and subprocess helpers. Finish when every way to invoke and test the
   affected scripts or CLI is known, including sibling Effect scripts whose
   patterns should replace legacy plain-TypeScript style.
2. When the executable accepts public arguments or flags, read
   [command-design.md](references/command-design.md). Define arguments and flags
   with `Argument` and `Flag`, compose commands with `Command`, and give every
   public input useful help. Use `Effect.fn` handlers and yield the root command
   when a subcommand needs shared parent input.
3. Keep handlers thin. Decode user and file input at the boundary, enforce
   cross-input invariants, then call an application service. Keep persistence,
   network calls, orchestration, retries, and transactions in services.
4. Keep expected operational failures typed with `Schema.TaggedError`; map
   platform failures into application-owned errors near the adapter that knows
   what the operation means. Let defects remain defects.
5. Read [entrypoints-and-testing.md](references/entrypoints-and-testing.md).
   Export the command tree or fixed script workflow without running it, wire
   one Node or Bun entrypoint, and verify the applicable success, expected
   failure, help, parsing, JSON, dry-run, and confirmation paths.

## Optional branches

- Read [processes-and-platform.md](references/processes-and-platform.md) when a
  script or command reads files, inspects the environment, starts child
  processes, streams their output, or differs between Node and Bun.
- Use `Prompt` only for an intentionally interactive path. Keep required inputs
  expressible as arguments or flags so automation never depends on a terminal.
- Add `--dry-run` for commands that mutate important state and `--yes` for
  explicitly authorized non-interactive confirmation. Never prompt in JSON or
  CI-oriented modes.

## CLI ownership rules

- Let `Command`, `Argument`, and `Flag` own syntax, defaults, aliases, examples,
  and help text.
- Let schemas own untrusted structured input and machine-readable output.
- Let handlers own CLI-to-application mapping and presentation selection.
- Let services own reusable behavior and external capabilities.
- Let Layers own implementations and runtime dependencies.
- Let the executable entrypoint own `Command.run`, platform provisioning,
  scopes, signal handling, and `NodeRuntime.runMain` or `BunRuntime.runMain`.
- Keep stdout stable for primary or machine-readable output. Send diagnostics
  and progress elsewhere; never mix prose into JSON output.

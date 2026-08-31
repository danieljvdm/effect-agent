# Learning more about the Effect

This repository uses the Effect Typescript library.

Before writing any Effect code, first read `node_modules/effect/AGENTS.md`
**completely**, and follow the links in the file when required.

If you need to learn more about particular Effect apis and concepts that the
guide doesn't cover, search through the source code in `node_modules/effect/src`.

# Effect Atom client boundary

This repository consumes APIs through Effect Atom clients (`@effect/atom-react`).
Keep business logic in Effect: compose multi-step client workflows as atoms,
declare cross-query invalidation as reactivity keys on mutations, and keep
promise-mode dispatches at the React boundary logic-free — no `.then` chains
in components or routes.

## Project command policy

Vite+ is the unified toolchain and command authority for this repository. It wraps Vite, Rolldown, Vitest, tsdown, Oxlint, Oxfmt, and Vite Task behind the `vp` CLI; Vite+ is distinct from Vite.

Run `vp help` for available commands and `vp <command> --help` for command-specific options. Documentation is available locally in `node_modules/vite-plus/docs` and online at https://viteplus.dev/guide/.

Use these repository commands:

- Install dependencies: `vp install`.
- Full validation: `vp run check`.
- Static checks: `vp check`.
- Format check: `vp fmt --check`; format fixes: `vp fmt`.
- Lint only: `vp lint`; lint fixes: `vp lint --fix`.
- Tests only: `vp test`.
- Other repository tasks and package scripts: `vp run <task>`.
- Toolchain or runtime troubleshooting: run `vp env doctor` and include its output when asking for help.

Do not use `bun run`, `npm run`, `pnpm run`, or `yarn run` in this repository. Do not invoke underlying tools such as `tsc`, `vitest`, `oxlint`, or `oxfmt` directly; use the Vite+ entry points above.

# Instructions for implementation agents

This repository is designed to be implemented by a large, parallel AI-assisted project. Every
agent must preserve a common domain language, dependency direction, and durability contract.

## Required reading

Before editing code:

1. Read `README.md`.
2. Read `GLOSSARY.md` when changing domain concepts or public terminology.
3. Read `docs/TOOLCHAIN.md`.
4. Read the relevant guide, API comments, and neighboring tests for the modules in scope.
5. Read `node_modules/effect/AGENTS.md` before writing Effect code (the canonical Effect
   guidance; `.agents/skills` carries the focused task skills).
6. Read `.agents/skills/build-effect-clis/SKILL.md` before creating or changing repository
   scripts.
7. Inspect neighboring package tests before introducing a new pattern.

Keep user-facing behavior in existing guides, implementation contracts beside the code, and
regression evidence in tests. Explain change rationale in the pull request. Do not create separate
specifications, planning documents, decision registers, ADRs, roadmaps, or evidence logs.

## Non-negotiable architecture rules

1. Public asynchronous operations return `Effect` or `Stream`, not naked `Promise` values.
2. Expected failures remain typed in `E`; dependency requirements remain visible in `R`.
3. Effect `Schema` is the canonical source for persisted, transported, tool, and structured model
   values.
4. Every acquired resource belongs to `Scope`. The engine must not create daemon fibers.
5. Use the pinned Effect v4 AI primitives directly. Do not introduce framework-owned copies of
   Effect AI `Tool`, `Toolkit`, `LanguageModel`, `Prompt`, `Response`, or `Model`.
6. Provider SDK values never become canonical thread records. Effect AI values may be used
   by the interpreter, but durable records remain explicit, versioned Schemas.
7. The canonical log is append-only. Projections and checkpoints are disposable derivatives.
8. No code may claim exactly-once external side-effect execution.
9. An unresolved ordinary tool call is never automatically replayed after ownership loss.
10. Tool/model/subagent concurrency is bounded and deterministic at commit time. Tool batches use
    Effect structured concurrency and Semaphore permits rather than a separate Promise scheduler.
11. Security decisions are fail-closed. Model output is untrusted input.
12. Node platform assumptions must not enter core domain modules.

## Package dependency direction

```text
core <- engine <- capabilities
core <- sandbox <- sandbox-local
core <- sandbox <- capabilities
core <- engine <- thread <- storage adapters
engine + thread + sandbox + selected adapters <- platform packages
core + engine <- testing
core + engine + capabilities <- effect-agent (umbrella) <- pr-review
```

An inward package must not import an outward package. If a feature appears to require that, define
or deepen an inward port and implement an outward adapter.

Framework code lives only in `packages/*`; do not create an `apps/` workspace. Runnable consumer
benches live in `examples/*`, remain leaf workspaces, and may depend inward on public framework
packages and `@effect-agent/testing`. Create a new framework package only for a
genuinely new framework concern agreed with the repository owner. Provider integrations remain upstream Effect AI Layers, not framework provider
packages.

## Toolchain rules

- Bun `1.3.14` is the package manager. Use `catalog:` for shared dependencies and `workspace:*`
  for repository packages.
- The root catalog is the single source for the exact Effect v4 version. Do not pin Effect
  independently in a package.
- After changing an Effect-family version, run `vp install` and `vp run check`.
- Contributor skills under `.agents/skills` are repo-owned, each tracked by its own
  `.dev-kit-origin.json` receipt. Check for upstream updates with
  `bunx @danieljvdm/dev-kit@latest skills status`, and fast-forward an unmodified skill with
  `bunx @danieljvdm/dev-kit@latest skills update <name>`; a skill with local edits is left for an
  agent merge instead of being overwritten. Add a new skill from the approved catalog with
  `bunx @danieljvdm/dev-kit@latest skills add <name>`.
- Contributor agent skills are repository tooling. They are not runtime Skill definitions and
  must not be imported by `@effect-agent/*`.
- Before handoff, run `vp run ready`.

## Change discipline

- Add or update Effect Schema definitions before implementing new wire or persisted values.
- Add type tests for inferred `E` and `R` whenever Agent or Effect AI composition changes.
- Add deterministic tests for every new state transition.
- Add failpoints before and after every new durable mutation.
- Update existing guides or API comments when a change affects their documented behavior.
- Explain rejected alternatives in the pull request when a future agent could reasonably
  re-propose them.
- Do not silently widen errors to `unknown`, `Error`, or `any`.
- Do not use type assertions to cross a schema boundary.
- Do not build persistence migration tooling during private development. Incompatible development
  data may be reset, but must fail clearly rather than decode incorrectly.
- Write changesets as one or two imperative sentences naming the consumer-visible change. Add only
  a short usage example or an explicit BEHAVIOR CHANGE note when consumers must act; keep IDs,
  root-cause, review and test stories, and implementation mechanics in the pull request.

## Parallel work

Parallel agents must own disjoint packages or documents. Shared domain schemas, error unions,
journal records, and public exports require one designated integrator. Before merging parallel
branches, run:

1. `vp run ready`;
2. adapter contract suites;
3. generated schema fixture checks;
4. relevant crash/fault tests.

## Completion standard

A feature is not complete merely because the happy path works. It is complete when:

- its interface, invariants, and error modes are documented;
- success, expected failure, defect, timeout, and interruption paths are tested;
- resource finalizers are verified;
- durable crash points are specified when persistence is involved;
- security and telemetry behavior are defined;
- public examples compile;
- no forbidden dependency crosses into core.

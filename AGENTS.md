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
6. Read `.agents/skills/effect-development/references/cli/index.md` before creating or
   changing repository scripts.
7. Inspect neighboring package tests before introducing a new pattern.

Keep user-facing behavior in existing guides, implementation contracts beside the code, and
regression evidence in tests. Explain change rationale in the pull request. Do not create separate
specifications, planning documents, decision registers, ADRs, roadmaps, or evidence logs.

## Documentation

Documentation is for humans learning the library. Guides must be terse and explain
concepts succinctly: what a feature does, how it fits, and how to use it.

- Lead with the mental model and ownership boundaries. Use small architecture or
  flow diagrams and only the code snippets essential to understanding and usage.
- Put detailed options, defaults, and API behavior in scannable reference pages.
  Link to runnable examples for complete setup.
- Keep implementation contracts in source, schemas, and API comments. LLMs can
  read the code; do not turn user guides into agent context or implementation audits.
- Keep crucial caveats beside the relevant concept; link to reference details.
- Edit the page as a whole. Do not append feature inventories, change histories,
  or long defensive explanations to an otherwise focused guide.

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
ai-decision <- ai-typesafe
ai-decision <- effect-agent capabilities
effect-agent <- storage adapters
effect-agent <- workflow
effect-agent + selected adapters <- platform packages
effect-agent <- sandbox-local
effect-agent <- testing
effect-agent <- pr-review
```

Within `packages/effect-agent/src`, dependencies point inward:
`core <- engine <- capabilities <- durable` and `core <- sandbox <- capabilities`.
Public module paths address these implementations directly; source directories are not separate
packages. Keep core and sandbox contracts platform-neutral. The export check enforces these
internal boundaries as well as package imports.

An inward package must not import an outward package. If a feature appears to require that, define
or deepen an inward port and implement an outward adapter.

Framework code lives only in `packages/*`; do not create an `apps/` workspace. Runnable consumer
benches live in `examples/*`, remain leaf workspaces, and may depend inward on public framework
packages and `@effect-agent/testing`. Create a new framework package only for a
genuinely new framework concern agreed with the repository owner. Provider integrations remain upstream Effect AI Layers, not framework provider
packages.

## Toolchain rules

- Bun `1.4.2` is the package manager. Use `catalog:` for shared dependencies and `workspace:*`
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
- For lockfile-only PR fixes, push after `vp install --frozen-lockfile` passes; finish full validation afterward.

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
- Keep supported persisted-format upgrades narrow and adapter-owned, atomic and data-preserving.
  Unsupported or ambiguous data must fail clearly without mutation; never reset supported data.
  Do not introduce a general migration framework.
- Write changesets as one or two imperative sentences naming the consumer-visible change. Add only
  a short usage example or an explicit BEHAVIOR CHANGE note when consumers must act; keep IDs,
  root-cause, review and test stories, and implementation mechanics in the pull request.

## Test discipline

Follow `.agents/skills/testing/SKILL.md`.

- Every test must protect a plausible regression with an independent observable assertion and
  distinct coverage beyond its neighbors. Avoid implementation mirrors and redundant smoke tests.
- Use the cheapest faithful boundary; exercise decision tables over typed results while retaining
  real wiring, adapter, restart, security and cleanup guarantees in the PR gate.
- Size loops from quotas, pages, replay horizons or documented scale regressions. Reduced fault
  sweeps must account for every scenario/location pair and expose new or lost routes.
- Compare equivalent workloads before claiming savings. Reuse successful checks for unchanged
  inputs, state the limits of resource assertions and explain material coverage tradeoffs.

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

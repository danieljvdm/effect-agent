---
name: effect-ts
description: Use this skill whenever working in a repository that uses Effect, even if the current task is in a new file or the user does not explicitly ask for Effect help. Apply it to Effect patterns, services, layers, schemas, streams, runtimes, typed errors, observability, testing, HTTP, SQL, command-line scripts, project automation, and supporting tooling.
---

# Effect Expert

Expert guidance for programming with the Effect library, covering error handling, dependency injection, composability, and testing patterns.

## Version Baseline

This skill targets Effect v4. The guidance was last reviewed against
`effect@4.0.0-beta.102` and the matching `@effect/*` v4 packages.

Before changing code, determine the version installed by the target repository.
The target repository's manifest and lockfile are authoritative. Do not migrate
or upgrade it merely to match this review baseline.

Read `./references/version-and-source.md` before installing packages, resolving
version-sensitive APIs, or using upstream source as evidence.

## Research Strategy

Effect has many ways to accomplish the same task. Proactively research best practices when working with Effect patterns, especially for moderate to high complexity tasks.

Use the local guides in `./references/` first. They are the preferred source for best practices, conventions, and common implementation patterns.

Only go directly to the canonical Effect source when:

- the guides do not cover the question
- you need exact API details or signatures
- you need deeper implementation details
- you need to verify a behavior against the source

### Research Sources

1. Local skill guides first. Start with the relevant files in `./references/`
   before doing deeper research.
2. Codebase patterns second. Examine similar patterns in the current project
   before implementing. If Effect patterns already exist, follow them for
   consistency unless they conflict with an explicit local policy in this
   skill.
3. Installed package source third. Use the target repository's resolved
   `effect` and `@effect/*` packages for exact exports, declarations, and API
   signatures.
4. Version-matched upstream source last. For gaps in the guides, complex type
   errors, unclear behavior, or implementation details, inspect the canonical
   `Effect-TS/effect` source at the tag or commit matching the installed
   version. A project-local source checkout is optional, never a prerequisite.

### When To Research

- Always research for services, layers, or complex dependency injection.
- Always research for error handling with multiple error types or complex error hierarchies.
- Always research for stream-based operations and reactive patterns.
- Always research for resource management with scoped effects and cleanup.
- Always research for concurrent or performance-critical code.
- Always research for unfamiliar testing patterns.
- Research when needed for complex refactors from promises or try/catch into Effect.
- Research when needed for new service dependencies or layer restructuring.
- Research when needed for custom error types or extensions of existing error hierarchies.
- Research when needed for integrations with external systems such as databases, APIs, or third-party services.

### Research Approach

- Focus on canonical, readable, and maintainable solutions rather than clever optimizations.
- Verify suggested approaches against existing codebase patterns when those
  patterns exist.
- When multiple approaches are possible, prefer the most idiomatic Effect
  solution supported by the codebase, these local policies, and version-matched
  Effect source.

### Codebase Pattern Discovery

When working in a project that uses Effect, check for existing patterns before implementing new code:

1. Search for Effect imports and existing module usage to understand current conventions.
2. Identify how services and layers are structured in the project.
3. Note how errors are defined and propagated.
4. Examine how Effect code is tested in the project.

If no Effect patterns exist in the codebase, proceed using these guides and
canonical patterns from version-matched Effect source and examples. Do not
block on missing codebase patterns or a missing source checkout.

### Feature Discovery

When you need to discover available Effect modules, packages, or capabilities, search `./references/features.md` first.

- Use it to identify the right package or module for a task.
- Treat listed repo paths as discovery hints. Confirm them against the target
  repository's installed version before relying on exact names or signatures.
- Use it before inventing custom abstractions when Effect may already provide the functionality.

### Guide Discovery

When the task touches one of these areas, consult the matching guide before implementing:

- `./references/guide-effect.md` for core `Effect` usage patterns, common constructors, composition, provisioning, and runtime boundaries
- `./references/guide-error-handling.md` for defining errors, schema-based errors, failure handling, defects, and interrupts
- `./references/guide-layers.md` for service ownership, service design, layer
  construction, dependency visibility, composition, and provisioning
- `./references/guide-observability.md` for `Effect.fn`, spans, structured
  logging, metrics, annotations, and telemetry wiring
- `./references/guide-retries.md` for retry policies, retry conditions, fallback strategies, and `ExecutionPlan`
- `./references/guide-schedule.md` for retries, repeats, backoff, polling, cron, and schedule composition
- `./references/guide-schema.md` for schema-first application modeling,
  service contracts, transformations, unions, recursion, and branded types
- `./references/guide-sql.md` for Effect SQL usage, transactions, resolvers, schema-aware SQL, and migrations
- `./references/guide-testing.md` for `@effect/vitest`, deterministic testing,
  honest test Layers, property tests, and protocol round trips
- `./references/guide-cli.md` for Effect-powered command-line scripts and
  project automation
- `./references/guide-http-boundaries.md` for `HttpApi` contracts, handlers,
  DTOs, transport errors, and route boundaries
- `./references/audit-services.md` for a complete service and type-boundary
  audit workflow
- `./references/guide-type-safety-and-boundaries.md` for `unknown`, assertions,
  runtime shape checks, external decoding, and boundary ownership

Treat each topic guide as the single authority for that topic. Do not skip the
guides and jump straight to source unless source-level confirmation is needed
or the guides do not answer the question.

## Effect Principles

Apply these core principles when writing Effect code.

## Installation

When installing Effect v4 packages in a user repository:

- use the current `effect@beta` version only for a new v4 installation
- preserve the repository's resolved v4 beta for existing installations unless
  the user asks for an upgrade
- keep `effect` and all v4 `@effect/*` packages on the exact same beta version
- install only the packages needed for the user's runtime and actual task

### Version Rules

- `effect@latest` is still the v3 release line; use `effect@beta` when creating
  a new v4 repository.
- Resolve the beta tag at installation time. Do not assume the review baseline
  is still current.
- If you install any v4 `@effect/*` package, make sure it and `effect` use the
  same exact beta version.
- Do not mix v3 integration packages with Effect v4 packages.

### Package Selection

Choose packages based on the runtime and the work being done.

- core library: `effect@beta`
- Node.js runtime needs: install the matching `@effect/platform-node@beta`
- browser runtime needs: install the matching `@effect/platform-browser@beta`
- Bun runtime needs: install the matching `@effect/platform-bun@beta`
- Vitest integration needs: install the matching `@effect/vitest@beta`
- OpenTelemetry integration needs: install the matching
  `@effect/opentelemetry@beta`

Install additional `@effect/*` packages only when the user task actually needs them.

### Practical Rule

- start with the current resolved `effect@beta`
- add matching v4 `@effect/*` packages only as needed by runtime and features
- pin the full installed Effect v4 package set to the same exact beta

### Error Handling

- Use Effect's typed error system instead of throwing exceptions.
- Define descriptive error types with proper error propagation.
- Prefer `Schema.TaggedErrorClass` when the error can be schema-defined.
- Use `Effect.fail`, `Effect.catchTag`, and `Effect.catch` for error control flow.

### Dependency Injection

- Implement dependency injection using services and layers.
- Define services with `Context.Service`.
- Compose layers with `Layer.merge` and `Layer.provide`.
- Use `Effect.provide` to inject dependencies at the edge, avoid providing locally.
- Keep services encapsulated; avoid exporting trivial accessor wrappers that only forward to one service method.

### Composability

- Leverage Effect composability for complex operations.
- Use appropriate constructors such as `Effect.succeed`, `Effect.fail`, `Effect.tryPromise`, `Effect.try`, and `Effect.sync`.
- Apply proper resource management with scoped effects.
- Chain operations with `Effect.flatMap`, `Effect.map`, and `Effect.tap`.

### Business Logic Functions

- Prefer `Effect.fn` for reusable business-logic functions that return `Effect`.
- Prefer `Effect.fn` over raw `Effect.gen` definitions even when the function takes no arguments.
- If you do not want an explicit named span, use `Effect.fn` without a span name.
- Do not use `Effect.fnUntraced` as the default.
- Use `Effect.fnUntraced` only for edge cases with a concrete low-level reason, such as measured hot-path overhead.

### TypeScript Preferences

- Do not use `any` in Effect application, service, or workflow code.
- Decode external values instead of asserting their shape.
- Isolate unavoidable compiler or framework adapter assertions at the
  narrowest boundary and document the contract they bridge.
- Do not use `namespace` to hide services, layers, or mutable state.
- Prefer correct typing, schema-driven decoding, narrowing, and proper generic constraints instead of forcing types.
- If a value comes from an external boundary, validate or decode it instead of asserting its type.
- If a type is hard to express, simplify the design or introduce a properly typed helper instead of using unsafe TypeScript.
- For layers, do not hide them inside `namespace` blocks. Prefer either `static` members on the service class or plain exported layer constants.

### Code Quality

- Write type-safe code that leverages Effect's type system.
- Use `Effect.gen` for readable sequential code.
- Implement proper testing patterns using Effect testing utilities.
- Prefer existing Effect primitives before introducing custom helpers.
- Prefer `Schema.Class` / `Schema.TaggedClass` variants over plain `Schema.Struct` for named reusable schemas when possible.

### Explaining Solutions

When providing solutions, explain the Effect concepts being used and why they
fit the specific use case. If you encounter patterns not covered in local
references, prefer consistency with the codebase when possible and otherwise
rely on installed declarations and version-matched canonical Effect source.

## References

- `./references/features.md`
- `./references/guide-effect.md`
- `./references/guide-error-handling.md`
- `./references/guide-layers.md`
- `./references/guide-observability.md`
- `./references/guide-retries.md`
- `./references/guide-schedule.md`
- `./references/guide-schema.md`
- `./references/guide-sql.md`
- `./references/guide-testing.md`
- `./references/guide-cli.md`
- `./references/guide-http-boundaries.md`
- `./references/audit-services.md`
- `./references/guide-type-safety-and-boundaries.md`
- `./references/version-and-source.md`

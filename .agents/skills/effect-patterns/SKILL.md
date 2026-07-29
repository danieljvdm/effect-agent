---
name: effect-patterns
description: Apply TypeScript Effect patterns for schema-first domain models, services, layers, typed errors, logging, testing, and HTTP boundaries. Use when writing or reviewing Effect application code, domain contracts, service workflows, API handlers, runtime boundaries, or tests. For command-line scripts and project automation, also use effect-cli.
---

# Effect Patterns

Use this skill as a router for focused Effect reference files. Read only the
reference files that match the code being changed.

## Reference Map

- For schema-first domain models, service inputs/results, branded identifiers,
  schema-backed expected errors, and boundary validation, read
  `references/schema-first-modeling.md`.
- For reusable Effect helpers, `Effect.fn`, Promise boundaries, and expected
  failures, read `references/functions-and-errors.md`.
- For services, dependency tags, layer builders, and domain abstractions, read
  `references/services-and-layers.md`.
- For a service-design audit across a codebase, package, feature slice, or
  diff, read `references/service-design-audit.md`,
  `references/services-and-layers.md`, and `references/testing.md`.
- For structured logs, annotations, and log spans, read
  `references/logging.md`.
- For tests around Effect services, workflows, schemas, and runtime boundaries,
  read `references/testing.md`.
- For `HttpApi` contracts, handlers, DTOs, transport errors, and route
  boundaries, read `references/http-boundaries.md`.

## Working Rules

1. Read nearby code before applying a pattern. Prefer the repository's current
   naming, layer conventions, test helpers, and runtime boundaries.
2. Keep business logic in services and workflows. Keep request, CLI, worker,
   and transport handlers thin.
3. Model expected failures in the error channel with schema-backed tagged
   errors.
4. Make services and layers yield runtime dependencies from the Effect
   environment. Never pass environment objects, config, clients, databases,
   clocks, loggers, or other dependencies into layer factories.
5. Prefer Effect platform APIs and service layers over direct global/runtime
   calls in code that should be testable.
6. Validate with the narrowest meaningful typecheck, test, or command path.

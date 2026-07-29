# ADR-0009: Keep runnable consumer benches in leaf example workspaces

Status: **Accepted**

## Context

ADR-0006 excluded application workspaces while the framework boundaries were still speculative.
Phase 0 now has a real public Agent Definition, explicit Model Binding, interpreter, semantic
event stream, deterministic Model, and Travel Planner fixture. A browser bench is useful for
exercising those contracts, but placing UI dependencies in a framework package would violate the
inward dependency direction.

Package-local tests remain the authoritative correctness evidence. They are not a convenient
interactive surface for chat input, Tool state, event inspection, and structured output.

## Decision

- Keep framework and platform code in phase-gated `packages/*`.
- Keep runnable consumer demonstrations in private `examples/*` workspaces.
- Treat every example as a leaf: it may consume public framework packages and
  `@effect-agent/testing`, but no framework package may import it.
- Include examples in the root type-check, test, and build gates.
- Keep `apps/` absent until a separately approved deployable product boundary exists.
- Reuse deterministic fixtures rather than maintaining a second mocked runtime in the UI.
- Make provider-backed or hosted profiles opt-in; an example adds no persistence, durability, or
  deployment guarantee by existing.
- Use TanStack Start, Effect Atom, Tailwind CSS, shadcn/ui on Base UI, and Vercel AI Elements-style
  components for the Phase 0 browser bench.

## Consequences

- `examples/demo` can show the actual Phase 0 event stream without polluting framework manifests
  with React or browser dependencies.
- The root catalog remains the single shared dependency-version source.
- UI drift is constrained because the demo imports the same Travel Planner Schemas, Layers, and
  scripted turns used by the tests.
- Browser build failures block `bun run ready`.
- ADR-0006 remains the historical reason `apps/` and premature product packages were excluded;
  this ADR supersedes only its prohibition on leaf example workspaces.

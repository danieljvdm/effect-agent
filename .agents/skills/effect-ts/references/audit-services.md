# Auditing Effect Services

Use this for an audit across a codebase, package, feature slice, or diff. Apply
the service and test rules from the other references selected by
`effect-ts`.

## Contents

- [Establish Local Rules](#1-establish-local-rules)
- [Build The Inventory](#2-build-the-inventory)
- [Trace Authority And Requirements](#3-trace-authority-and-requirements)
- [Classify Each Candidate](#4-classify-each-candidate)
- [Audit Test Strategies](#5-audit-test-strategies)
- [Report Actionable Findings](#6-report-actionable-findings)

## 1. Establish Local Rules

Read the project architecture guidance, Effect conventions, pinned Effect
version or source, and relevant version-matched examples. Record the governing
conventions and APIs that later decisions must follow.

Complete this step when the applicable project guidance and pinned source
examples are named.

## 2. Build The Inventory

Enumerate source and test files, then find:

- Every `Context.Service`, tag, `Layer`, `make`, `provide`, and
  `provideService`.
- Service-shaped interfaces or classes with effectful methods.
- Dependencies passed through parameters, properties, constructors, callbacks,
  options bags, or Layers.
- Direct access to time, randomness, cryptography, IDs, configuration, HTTP,
  persistence, registries, renderers, filesystems, runtime bindings, and
  mutable globals.
- Test fakes, in-memory implementations, module mocks, and hand-built
  `Layer.succeed` values.
- Public Effects with `unknown` or `any` error types.
- Assertions, non-null assertions, custom type predicates, structural probes,
  JSON parsing, Promise catch mappers, and `throw` statements.
- Local schemas, codecs, JSON types, and runtime helpers that overlap Effect or
  platform APIs.

Record one row per discovered service or candidate:

| Field         | Question                                                    |
| ------------- | ----------------------------------------------------------- |
| Owner         | Which module owns the capability's meaning?                 |
| Contract      | Where are its interface and tag?                            |
| Construction  | Does construction yield every runtime dependency?           |
| Production    | Who owns the concrete implementation and Layer choice?      |
| Tests         | Does it have an intentional and honest substitute strategy? |
| Consumers     | Are capabilities yielded or drilled as values?              |
| Requirements  | Do requirements remain visible to the composition root?     |
| Type boundary | Who owns decoding, narrowing, and error translation?        |
| Verdict       | Keep, deepen, relocate, merge, remove, or create?           |

Build a companion type-safety inventory using
[`guide-type-safety-and-boundaries.md`](guide-type-safety-and-boundaries.md).
Attach each
occurrence to its owning service or boundary and record its input provenance,
intended type or error, and target disposition.

Complete this step when every discovered service, tag, Layer, service-shaped
candidate, and type-safety occurrence appears exactly once.

## 3. Trace Authority And Requirements

For each inventory row:

1. Trace one caller-visible operation to every effect it performs.
2. Mark where each dependency first appears and whether code yields it, passes
   it, captures it, or provides it concretely.
3. Verify that the module selecting a concrete Layer owns that implementation
   choice.
4. Check project-compatible Effect capabilities before recommending an
   application wrapper.
5. Follow every requirement to a composition root or an explicit value
   boundary.

Inspect dependency drilling, Layer arguments, dependency bags, handler-builder
service values, inner `Effect.provide` calls, direct runtime access, and
contracts or Layers scattered across unrelated owners.

Complete this step when every capability has an unbroken path from use to its
composition root or documented value boundary.

## 4. Classify Each Candidate

Apply the authority seam and deletion tests, then assign one classification:

- **Built-in Effect capability** — yield the existing capability.
- **Application-owned authority** — define a narrow port beside the operation
  that needs it.
- **Technology adapter** — implement an application-owned port in the adapter.
- **Request or domain value** — keep the value explicit.
- **Framework boundary** — contain the framework-required API in its adapter or
  composition root.
- **Pass-through abstraction** — fold it into the real owner.

Prefer an existing owner or a merge of duplicated capabilities over a generic
registry or dependency bag.

Complete this step when every candidate has one evidence-backed classification
and each service-or-value decision states why the alternative was rejected.

## 5. Audit Test Strategies

Record how tests replace or control each production service. Verify that
exported test and in-memory Layers implement the behavior their names promise,
and keep focused partial fixtures local to their tests.

Complete this step when every production service has an intentional test
strategy or an explicit production-only rationale.

## 6. Report Actionable Findings

Prioritize by correctness and requirement visibility:

- **P0** — hidden authority, unsafe direct runtime access, broken or duplicated
  capability, wrong Layer ownership, untyped expected failures, or unchecked
  external data.
- **P1** — repeated dependency drilling, hidden requirements, scattered service
  ownership, manual shape discovery, unjustified assertions, custom substitutes
  for Effect APIs, or a missing intentional test strategy.
- **P2** — naming or co-location cleanup that should accompany a nearby
  refactor.

For each finding include:

1. File, line, or symbol evidence.
2. The hidden requirement or caller burden.
3. The smallest target shape using the project's Effect conventions.
4. The composition-root and test impact.
5. The behavior and modules that should remain unchanged.

End with explicit keep decisions for values, pure functions, framework
boundaries, correctly separated ports and adapters, and request-scoped
services.

Complete the audit when every inventory row and type-safety occurrence has a
disposition, every proposed change names its owner and target shape, and every
proposal remains evidence-backed and scoped to observed code.

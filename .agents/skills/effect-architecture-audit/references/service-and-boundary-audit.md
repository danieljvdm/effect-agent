# Service And Boundary Audit

## Contents

- [Establish authority](#establish-authority)
- [Build the inventory](#build-the-inventory)
- [Trace requirements](#trace-requirements)
- [Classify candidates](#classify-candidates)
- [Audit type boundaries](#audit-type-boundaries)
- [Audit test substitutes](#audit-test-substitutes)
- [Report findings](#report-findings)

## Establish authority

Read the repository's architecture guidance and version-matched Effect package
instructions. Treat installed declarations and source as the authority for API
signatures. Record local conventions that affect service ownership, adapter
placement, error translation, and testing.

## Build the inventory

Find every:

- Effect service, tag, Layer, constructor, and provisioning call.
- Service-shaped interface, class, dependency bag, registry, or callback.
- Direct use of time, randomness, IDs, configuration, credentials, HTTP,
  persistence, filesystems, runtime bindings, or mutable globals.
- Test Layer, fake, in-memory implementation, and module mock.
- Public Effect with an `unknown` or overly broad error channel.
- `any`, assertion, non-null assertion, custom predicate, structural probe,
  unvalidated JSON parse, Promise rejection mapper, and expected throw.
- Schema or codec that overlaps another representation of the same logical
  model.

Record one row per service or candidate:

| Field        | Question                                             |
| ------------ | ---------------------------------------------------- |
| Owner        | Which module owns the capability's meaning?          |
| Contract     | Where is its public contract defined?                |
| Construction | Are all construction requirements visible?           |
| Production   | Who chooses the concrete implementation?             |
| Consumers    | Is the capability yielded or drilled as a value?     |
| Boundary     | Who owns decoding, narrowing, and error translation? |
| Tests        | Is its substitute strategy intentional and honest?   |
| Verdict      | Keep, deepen, relocate, merge, remove, or create?    |

Complete the inventory when each discovered capability and unsafe boundary
appears exactly once.

## Trace requirements

For each row:

1. Trace one caller-visible operation through every effect it performs.
2. Mark where each dependency first appears and whether code yields, passes,
   captures, or concretely provides it.
3. Verify that the module selecting a concrete Layer owns that implementation
   choice.
4. Follow every Layer requirement to a composition root or an explicit value
   boundary.
5. Check version-matched Effect capabilities before recommending an
   application wrapper.

Flag hidden requirements created by inner provisioning, dependency bags,
handler-builder service values, mutable registries, or Layer selection inside
business operations. Prefer provision at a program or subsystem edge. Avoid
thin exported helpers that only yield a service and forward one method.

## Classify candidates

Assign one evidence-backed classification:

- **Built-in Effect capability** — yield the existing capability directly.
- **Application-owned authority** — define a narrow port beside the operation
  whose policy gives it meaning.
- **Technology adapter** — implement an application-owned port at the external
  boundary.
- **Request or domain value** — keep deterministic, per-call data explicit.
- **Framework boundary** — contain framework-required state or assertions in
  the adapter or composition root.
- **Pass-through abstraction** — fold it into the actual owner.

Apply the deletion test: if removing an abstraction leaves equally clear code
without spreading authority, resource ownership, or policy, remove it. Prefer
an existing owner or a merge over a generic registry.

## Audit type boundaries

Assign every `unknown` value a boundary owner.

- Reserve `unknown` for external input, foreign library output, and opaque
  diagnostic causes.
- Decode structured external data once, at the earliest boundary that owns its
  meaning.
- Pass decoded values and concrete error unions through internal services.
- Keep unavoidable framework assertions at the narrowest adapter and document
  the contract they bridge.
- Remove runtime narrowing already guaranteed by the inferred type.
- Preserve irreducible foreign causes as diagnostic fields inside concrete
  domain errors instead of broadening public error channels.

Prefer one source Schema per logical model. Use Schema transformations or
derived variants when a storage or transport representation differs without
changing the model's meaning. Create a separate Schema only for a real semantic
difference.

Keep a helper only when it owns domain policy, reusable refinement, repeated
non-trivial mechanics, or a meaningful observability boundary. Inline one-use
tag comparisons and structural checks that add no meaning.

## Audit test substitutes

Record the intended test strategy for every production service.

- Make a shared test Layer's name match the behavior it fully provides.
- Keep partial fixtures local instead of publishing them as general in-memory
  implementations.
- Use a real local adapter when persistence, transactions, serialization, or
  lifecycle behavior is the subject under test.
- Keep timing deterministic; never add wall-clock sleeps to stabilize a test.
- Test protocol round trips when encoding or decoding behavior is a durable,
  regression-prone public seam.

An explicit production-only rationale is valid when a substitute would not
protect durable behavior.

## Report findings

Prioritize findings by impact:

- **P0** — hidden authority, unsafe runtime access, wrong implementation
  ownership, unchecked external data, or expected failures escaping the typed
  channel.
- **P1** — dependency drilling, hidden Layer requirements, duplicated
  capabilities, manual shape discovery, unjustified assertions, pass-through
  abstractions, or dishonest test substitutes.
- **P2** — naming and co-location cleanup that belongs with a nearby change.

For each finding, include:

1. File, line, or symbol evidence.
2. The hidden requirement or caller burden.
3. The smallest target shape and its owner.
4. Composition-root and testing impact.
5. Behavior and modules that must remain unchanged.

End with explicit keep decisions for pure functions, request values, framework
boundaries, correctly separated ports and adapters, and services whose
requirements already remain visible.

# Effect Type Safety And Boundaries

Use this when reviewing `unknown`, `any`, assertions, runtime shape checks,
JSON or provider parsing, Promise rejection causes, thrown exceptions, and
small helper functions around typed Effect code.

## Contents

- [Assign Boundary Ownership](#assign-boundary-ownership)
- [Match Typed Values Directly](#match-typed-values-directly)
- [Let Schema Own Runtime Shape](#let-schema-own-runtime-shape)
- [Search Effect Before Adding Local Code](#search-effect-before-adding-local-code)
- [Audit Assertions And Manual Narrowing](#audit-assertions-and-manual-narrowing)
- [Keep Helpers Meaningful](#keep-helpers-meaningful)
- [Preserve Typed Error Channels](#preserve-typed-error-channels)
- [Completion Check](#completion-check)

## Assign Boundary Ownership

Identify who owns each value's type before inspecting it at runtime.

- Reserve `unknown` for external input, foreign library output, and opaque
  causes retained for diagnostics.
- Decode external values with the owning Schema at their earliest application
  boundary.
- Pass decoded values and concrete error unions through internal services.
- Expose public service methods as `Effect.Effect<A, DomainErrorUnion, R>`.
- Keep an irreducible foreign cause in a `cause: Schema.Unknown` field inside a
  concrete schema-backed error.

Complete this step when every `unknown` has a named boundary owner.

## Match Typed Values Directly

Read the inferred value and error types before adding a runtime guard.

- Match discriminated errors with their `_tag`, nested reason tags,
  `Effect.catchTag`, or `Effect.catchTags`.
- Access fields directly after the Effect error channel or Schema has already
  established their type.
- Use `instanceof` when runtime class identity is part of the owning API's
  contract.
- Inline a single-use boolean condition when it states the branch clearly.

Extract a predicate when it provides reusable narrowing or owns a meaningful
domain policy.

## Let Schema Own Runtime Shape

Choose the Schema adapter that matches the caller's control flow:

| Intent                    | Adapter                             |
| ------------------------- | ----------------------------------- |
| Boolean type guard        | `Schema.is(Model)`                  |
| Optional tolerant decode  | `Schema.decodeUnknownOption(Model)` |
| Typed Effect failure      | `Schema.decodeUnknownEffect(Model)` |
| JSON string decode        | `Schema.fromJsonString(Model)`      |
| Any JSON-compatible value | `Schema.Json`                       |

Define small schemas for provider responses, SDK payloads, persisted data, and
other structured external values. Decode once in the adapter and return the
schema-derived type.

## Search Effect Before Adding Local Code

Search the project's pinned Effect version and platform packages before
creating a schema, codec, type guard, JSON type, or runtime helper. Check:

- `Schema` for existing data models, codecs, guards, and transformations.
- `Encoding` for base64, hex, and text encoding.
- `FileSystem`, `Path`, `HttpClient`, `Clock`, `Random`, and other platform
  services for runtime capabilities.

Adopt the built-in value and type together when they model the same concept.
For example, `Schema.Json` supplies both the recursive JSON schema and its
`Schema.Json` type.

## Audit Assertions And Manual Narrowing

Inventory:

- `as any`, `as unknown as`, branded casts, and non-null assertions.
- Custom type predicates and generic helpers such as `isRecord`.
- `typeof value === "object"`, property probes, `in`, `Array.isArray`, and
  `JSON.parse` used to discover structured external data.

Give each occurrence one disposition:

1. Decode the boundary value with its Schema.
2. Isolate a compiler or framework adapter assertion at the narrowest boundary
   and document the contract it bridges.
3. Remove narrowing already guaranteed by the inferred type.

## Keep Helpers Meaningful

Extract a helper when it owns domain policy, provides reusable type refinement,
removes repeated non-trivial mechanics, or creates a named observability
boundary with `Effect.fn`.

Inline one-use property checks and tag comparisons. Apply the deletion test:
if removing the helper leaves equally clear typed code, keep the code inline.

## Preserve Typed Error Channels

- Represent recoverable failures with schema-backed tagged errors.
- Map `Effect.tryPromise` rejection causes to the concrete service error at the
  Promise boundary.
- Distinguish failures by caller action when recovery differs, such as
  permission, download, save, and share errors.
- Preserve the original foreign value as diagnostic context inside the tagged
  error.
- Use synchronous throws for framework-required hooks, impossible invariants,
  defects, and thunks immediately captured by `Effect.try` or
  `Effect.tryPromise`.
- Scope `ThrowStatement` lint restrictions to Effect workflow and service
  modules whose expected failures belong in the error channel. Give boundary
  adapters, invariant utilities, tests, and captured thunks explicit scopes.

## Completion Check

Before completing a change, account for every modified `unknown`, `any`,
assertion, type predicate, structural probe, JSON parser, Promise catch mapper,
throw, and public Effect error type. Each occurrence should resolve to a typed
internal value, a Schema-owned boundary, or a narrow documented adapter.

Use these probes during an audit:

- A typed error tag hidden behind a one-line predicate.
- A provider payload inspected through `isRecord` and property probes.
- A raw Promise rejection broadening a service error channel to `unknown`.
- A custom recursive JSON schema or `JsonValue` type.

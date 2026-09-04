# Verification

Follow repository testing policy. Use existing checks and direct requests when
sufficient; add committed tests only for an authorized or evidenced regression.
Choose from the scenarios below based on the changed boundary, rather than
creating a test suite for every endpoint edit.

Test the contract spine at its seams. Use the repository's established Effect
test integration and command authority.

## Contract tests

- Decode and re-encode representative request, success, and error values.
- Prove branded path/query/header values reject invalid wire input.
- Encode any `Schema.Class` headers exactly as generated clients receive them.
- Assert each expected error carries the intended HTTP status and body encoding.
- Compare or smoke-test generated OpenAPI when the public contract changes.

## Unknown codec audit

Inventory every `Schema.decodeUnknown*` and `Schema.encodeUnknown*` call in the
changed scope. Record a concrete untyped-boundary justification for each one,
such as `JSON.parse`, `Response.json`, an external message, or a persistence API
whose declared result is actually `unknown`. Replace any call whose input is
already the schema's `Encoded` or `Type` with the corresponding typed `Effect`,
`Sync`, `Exit`, `Option`, `Result`, or `Promise` codec.

An unknown codec is not a valid workaround for a `Schema.Class` or other static
type mismatch: map or construct the correct typed value instead. If the
repository enforces this policy with a lint warning, keep any necessary local
suppression documented with the same concrete boundary justification.

## Server tests

- Build every changed group and fail the test if an endpoint handler is missing.
- Provide deterministic test layers for application services and middleware.
- Prove each handler exposes only errors declared by its endpoint; exercise
  expected reason mapping and unexpected failure defects separately.
- Exercise success, declared failure, malformed input, and middleware rejection.
- Assert cross-field boundary invariants before the service workflow runs.
- Exercise raw routes separately and prove they enforce middleware-equivalent
  identity/security rules.

Use the installed `HttpApiTest` or an in-memory `HttpClient` when available;
otherwise run the built HTTP application against representative requests. Avoid
mocking below the contract so heavily that request encoding and response
decoding are skipped.

## Client tests

- Derive the client from the same root API used by the server.
- Provide every `requiredForClient` middleware with
  `HttpApiMiddleware.layerClient` and assert that it transforms the request.
- Assert params, query, headers, payload, and expected errors at least once for
  every changed request shape.
- For Atom clients, select the relevant scenarios from the
  [Effect Atom testing reference](../atom/effect-atom-testing.md); use a deterministic HTTP
  layer so request encoding, invalidation, and lifecycle remain observable.

## Completion matrix

Account for every changed endpoint across these columns:

| Contract                                         | Middleware                       | Handler/service                  | Client/query/mutation                          | Tests                            |
| ------------------------------------------------ | -------------------------------- | -------------------------------- | ---------------------------------------------- | -------------------------------- |
| Params, query, headers, payload, success, errors | Scope, provided services, errors | Identifier, invariants, workflow | Typed call shape, identity, cache/invalidation | Round-trip and boundary behavior |

Use the matrix when it helps trace a cross-boundary change. Completion means
the changed behavior is verified, untyped boundaries are justified, and the
repository's required checks pass. Empty test cells do not mandate new tests.

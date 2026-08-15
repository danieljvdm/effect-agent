---
name: build-effect-apis
description: Build contract-first Effect HTTP APIs. Use when defining shared HttpApiEndpoint/HttpApiGroup contracts, implementing HttpApiBuilder or HttpApiServer handlers and middleware, assembling server runtimes and OpenAPI docs, or serving on Cloudflare Workers/effect-cf. For consuming an API from client state, use $effect-atom-state.
---

# Build Effect APIs

Treat the shared `HttpApi` value as the **contract spine**: schemas, server,
OpenAPI, and clients all derive from it. Keep transport contracts isomorphic;
keep runtime behavior in handlers, services, layers, and client state modules.

Effect HTTP APIs are version-sensitive. Read the target repository's manifests
and lockfile, inspect its existing imports, and confirm exact signatures from
the installed package declarations before editing. In current Effect v4 betas
the server module is `HttpApiBuilder`; a request that mentions `HttpApiServer`
may refer to the same server-building responsibility from another version.

## Build the contract spine

1. Inventory the existing API root, groups, endpoints, schemas, transport
   errors, middleware, handler layers, client construction, and runtime edge.
   Finish when every existing contract consumer and provider is identified.
2. Read [shared-contracts.md](references/shared-contracts.md), then change the
   shared domain package first: schemas and errors, one endpoint contract,
   group composition, root API composition, and public exports. Finish when
   server and client can import the same API value without runtime-specific
   dependencies entering the domain package.
3. Read [server-and-middleware.md](references/server-and-middleware.md), then
   implement middleware and every changed group handler. Keep handlers as
   decoded boundary adapters into application services and assemble all
   requirements at the runtime edge. Finish when each endpoint identifier has
   exactly one handler and every declared middleware has a provided layer.
4. Route consumer changes through the `$effect-atom-state` skill: deriving
   `AtomHttpApi` or direct `HttpApiClient` clients, query and mutation atoms,
   reactivity keys, and React integration all live there. Finish when consumers
   call the shared contract rather than redefining wire types or using ad hoc
   `fetch` for declared endpoints.
5. Read [verification.md](references/verification.md). Run the repository's
   format, lint, typecheck, and test commands. Finish when changed schemas
   round-trip, middleware failures use declared error channels, and server and
   client agree on every request shape.

## Optional branches

- Read [runtime-assembly.md](references/runtime-assembly.md) when wiring a
  conventional Node/Bun server, generated API docs, process entrypoint, or
  serverless web handler.
- Read [cloudflare-workers.md](references/cloudflare-workers.md) when the server
  runs on Cloudflare Workers or uses `effect-cf`, bindings, Durable Objects,
  Queues, WebSockets, streaming, or raw byte routes.

## Choose typed Schema codecs by default

Match the codec to the static type at the call site. When decoding a value that
is already typed as the schema's `Encoded` type, use `Schema.decodeEffect` or
the typed `Schema.decodeSync`, `Schema.decodeExit`, `Schema.decodeOption`,
`Schema.decodeResult`, or `Schema.decodePromise` variant. When encoding a value
that is already typed as the schema's `Type`, use `Schema.encodeEffect` or the
corresponding typed `Schema.encodeSync`, `Schema.encodeExit`,
`Schema.encodeOption`, `Schema.encodeResult`, or `Schema.encodePromise` variant.

Reserve `Schema.decodeUnknown*` and `Schema.encodeUnknown*` for genuinely
untyped boundaries: values from `JSON.parse`, `Response.json`, external
messages, or persistence APIs whose declared result is actually `unknown`.
Never choose an unknown codec to bypass a `Schema.Class` or other static type
mismatch. Map the source value or construct the correct schema `Type` first,
then use the typed encoder; similarly, establish the correct `Encoded` value
before using a typed decoder.

This rule is toolchain-neutral. A repository may reinforce it with a lint
warning and a documented local suppression for a justified untyped boundary,
but the boundary and type reasoning remain the source of truth.

## Boundary rules

- Let schemas own wire validation, encoding, status metadata, and branded IDs.
- Let endpoint files own route inputs, success, and expected transport errors.
- Let middleware own cross-cutting request behavior and request-scoped services.
- Let handlers own transport-to-application mapping and boundary invariants.
- Let application services own orchestration, persistence, retries, and
  transactions.

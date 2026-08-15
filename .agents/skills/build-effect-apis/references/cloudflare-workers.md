# Cloudflare Workers and effect-cf

Use `effect-cf` to turn Worker bindings and entrypoints into Effect services and
layers. Confirm its compatibility with the repository's exact Effect v4 beta.

## Runtime edge

Assemble application services, `HttpApiBuilder` routes, the Effect HTTP
platform/router requirements, and binding layers once, then hand the final
layer and fetch effect to `Worker.make` or another effect-cf definition.

```ts
import { Effect, Layer } from "effect";
import { HttpRouter } from "effect/unstable/http";
import { Worker } from "effect-cf";

const WorkerLive = Layer.mergeAll(ApplicationServicesLive, ApiRoutesLive);

const fetch = Effect.gen(function* () {
  const request = yield* Worker.NativeRequest;
  const router = yield* HttpRouter.HttpRouter;
  return yield* router.asHttpEffect();
});

export default Worker.make(WorkerLive, { fetch });
```

The concrete Effect HTTP platform prerequisites differ by version and runtime.
Let the `HttpApiBuilder.layer` requirements drive the layer graph and verify it
with typecheck plus a Worker startup/dry-run command.

## Typed bindings

Represent D1, R2, KV, Queues, Durable Object namespaces, and configuration as
services/layers at the Worker boundary. Application services depend on those
tags, not on a raw `env` object. Keep Wrangler binding names synchronized with
the layers and generated Worker types.

`effect-cf` typed Durable Object and Queue definitions can serve both as the
contract and as the producer/namespace service. Use deterministic Durable
Object names for domain ownership, keep RPC errors typed, and design Queue
consumers for at-least-once delivery.

## Raw route escape hatches

JSON request/response routes fit `HttpApi`. WebSocket upgrades, streaming
responses, passthrough `Response` values, and large or direct byte transfers may
need a raw adapter at the Worker edge.

Dispatch raw routes before `router.asHttpEffect()`, decode every path/query/
header value with shared schemas, then fall through to the generated API router.
Mirror all relevant security, tenancy, tracing, and rate-limit behavior because
`HttpApi` middleware does not run for routes that bypass it.

For uploads, verify declared size, maximum size, media type, capability, and
checksum before storage. For WebSockets, validate the upgrade request and
identity before forwarding it to a Durable Object or session service.

Apply runtime-wide CORS around both raw and `HttpApi` routes. Include every
custom request header and method, and test preflight behavior. Treat OpenAPI as
documentation only for declared `HttpApi` endpoints; document raw routes
separately when clients need them.

## Operational checks

- Generate Worker binding types after Wrangler binding changes.
- Run local migrations before local server tests.
- Run the repository's startup or deployment dry-run check after layer changes.
- Keep remote resource creation, migrations, and deployment as explicit
  user-authorized operations.
- Test Queue idempotency, Durable Object naming, and raw-route middleware parity.

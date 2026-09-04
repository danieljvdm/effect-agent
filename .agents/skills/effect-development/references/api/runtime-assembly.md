# Standard runtime assembly

Use this branch for conventional Node/Bun processes, generated documentation,
and Web-standard handlers. Confirm platform package names against the installed
versions.

## Compose routes before choosing the runtime

```ts
import { NodeHttpServer, NodeRuntime } from "@effect/platform-node";
import { Layer } from "effect";
import { HttpRouter, HttpServer } from "effect/unstable/http";
import { HttpApiBuilder, HttpApiScalar } from "effect/unstable/httpapi";
import { createServer } from "node:http";
import { ApplicationApi } from "@app/domain/http";

const ApplicationHttpLive = Layer.mergeAll(ProjectsHandlersLive, AuthenticateLive).pipe(
  Layer.provide(ApplicationServicesLive),
);

const ApiRoutes = HttpApiBuilder.layer(ApplicationApi, {
  openapiPath: "/openapi.json",
}).pipe(Layer.provide(ApplicationHttpLive));

const DocsRoute = HttpApiScalar.layer(ApplicationApi, { path: "/docs" });
const AllRoutes = Layer.mergeAll(ApiRoutes, DocsRoute);
```

Build the typed API route and documentation route from the same contract. Merge
raw routes here only when the API schema cannot represent their transport, and
ensure they independently enforce equivalent identity and security behavior.

## Run a long-lived server

```ts
const HttpServerLive = HttpRouter.serve(AllRoutes).pipe(
  Layer.provide(NodeHttpServer.layer(createServer, { port: 3000 })),
);

Layer.launch(HttpServerLive).pipe(NodeRuntime.runMain);
```

Use the matching platform server layer for Bun or another runtime. Keep the
platform choice at this composition root.

## Export a Web handler

```ts
export const { handler, dispose } = HttpRouter.toWebHandler(
  AllRoutes.pipe(Layer.provide(HttpServer.layerServices)),
);
```

Use the Web handler for serverless and framework adapters that accept standard
`Request` and `Response` values. Preserve and call `dispose` when the host has a
lifecycle hook; do not recreate the handler and its runtime for every request.

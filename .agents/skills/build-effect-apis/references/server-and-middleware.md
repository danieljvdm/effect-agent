# Server handlers and middleware

Implement the shared contract with group handlers, application services, and
layers. Confirm names against the installed Effect version; examples here use
the Effect v4 `effect/unstable/httpapi` surface.

- [Implement thin group handlers](#implement-thin-group-handlers)
- [Declare security middleware in the shared contract](#declare-security-middleware-in-the-shared-contract)
- [Implement security middleware on the server](#implement-security-middleware-on-the-server)
- [Assemble layers at the runtime edge](#assemble-layers-at-the-runtime-edge)
- [Use response escape hatches deliberately](#use-response-escape-hatches-deliberately)

## Implement thin group handlers

Resolve application services once in the group builder, then map every endpoint
identifier to one handler. Use `Effect.fn` for callbacks that return Effects.

```ts
import { Effect } from "effect";
import { HttpApiBuilder } from "effect/unstable/httpapi";
import { ApplicationApi } from "@app/domain/http";
import { Projects } from "../services/Projects";

export const ProjectsHandlersLive = HttpApiBuilder.group(
  ApplicationApi,
  "projects",
  Effect.fn("ProjectsApi.handlers")(function* (handlers) {
    const projects = yield* Projects;

    return handlers
      .handle("listProjects", () => projects.list().pipe(Effect.orDie))
      .handle("getProject", ({ params }) =>
        projects
          .get(params.projectId)
          .pipe(Effect.catchReasons("ProjectsError", { ProjectNotFound: Effect.fail }, Effect.die)),
      );
  }),
);
```

Handlers receive decoded `params`, `query`, `headers`, and `payload`. Check
cross-field invariants at this boundary when schemas cannot express them, then
call one application workflow. Keep persistence, transactions, retries, and
multi-service orchestration in services.

Reconcile the service error channel with the endpoint contract here. Use
`Effect.orDie` when an endpoint declares no service failures. Use
`Effect.catchReason`, `Effect.catchReasons`, or `Effect.unwrapReason` when an
application service wraps domain reasons: re-fail only reasons declared by the
endpoint and turn unexpected infrastructure or invariant failures into defects.
Finish when each handler's expected error type is a subset of the endpoint's
declared error schema.

## Declare security middleware in the shared contract

Keep the middleware declaration in the shared contract package so server,
OpenAPI, and generated clients see the same requirement. Keep its implementation
out of that package.

```ts
// packages/domain/src/http/authorization.ts
import { Context, Schema } from "effect";
import { HttpApiGroup, HttpApiMiddleware, HttpApiSecurity } from "effect/unstable/httpapi";

export class CurrentActor extends Context.Service<CurrentActor, { readonly id: string }>()(
  "app/CurrentActor",
) {}

export class Unauthorized extends Schema.TaggedError<Unauthorized>()(
  "Unauthorized",
  { message: Schema.String },
  { httpApiStatus: 401 },
) {}

export class Authenticate extends HttpApiMiddleware.Service<
  Authenticate,
  {
    readonly provides: CurrentActor;
    readonly requires: never;
  }
>()("app/Authenticate", {
  requiredForClient: true,
  security: { bearer: HttpApiSecurity.bearer },
  error: Unauthorized,
}) {}

export const ProjectsApi = HttpApiGroup.make("projects")
  .add(GetProjectEndpoint, ListProjectsEndpoint)
  .middleware(Authenticate)
  .prefix("/projects");
```

`requiredForClient` makes the generated client require a matching client-side
middleware layer. The security declaration also owns credential decoding and
OpenAPI security metadata; do not manually decode an `Authorization` header for
this case.

## Implement security middleware on the server

A security middleware implementation is keyed by the names in its `security`
object. It can use server-only services without leaking them into clients.

```ts
// apps/api/src/http/Authenticate.ts
import { Effect, Layer, Redacted } from "effect";
import { Authenticate, CurrentActor, Unauthorized } from "@app/domain/http";
import { Actors } from "../services/Actors";

export const AuthenticateLive = Layer.effect(
  Authenticate,
  Effect.gen(function* () {
    const actors = yield* Actors;

    return Authenticate.of({
      bearer: Effect.fn("Authenticate.bearer")(function* (httpEffect, { credential }) {
        const actor = yield* actors
          .authenticate(Redacted.value(credential))
          .pipe(
            Effect.catch(() => Effect.fail(new Unauthorized({ message: "Invalid bearer token" }))),
          );

        return yield* Effect.provideService(httpEffect, CurrentActor, actor);
      }),
    });
  }),
);
```

Use ordinary `HttpApiMiddleware.Service` implementations for request logging,
tenancy, correlation IDs, or other cross-cutting work that is not a declared
security scheme. Those implementations may access `HttpServerRequest`, but they
must decode raw headers, cookies, and external values before providing services.
When middleware uses `requires` and `provides`, confirm ordering from the
installed docs: the outer middleware that requires a service is attached before
the inner middleware that provides it.

Every error a middleware can return belongs in its `error` schema. Keep the
server layer separate from the shared declaration so importing the contract
never pulls secrets, repositories, or platform services into a client bundle.

## Assemble layers at the runtime edge

```ts
const ProjectsHandlersProvided = ProjectsHandlersLive.pipe(Layer.provide(ApplicationServicesLive));

const ApiLive = HttpApiBuilder.layer(ApplicationApi, {
  openapiPath: "/api/openapi.json",
}).pipe(
  Layer.provide(ProjectsHandlersProvided),
  Layer.provide(AuthenticateLive),
  Layer.provideMerge(HttpRuntimePrerequisitesLive),
);
```

The exact HTTP platform/router/server layers vary by runtime. Keep that wiring
at the entrypoint, provide all group and middleware layers, and let the type
system expose missing requirements. Verify that the OpenAPI path composes with
any API prefix as intended.

For a conventional Node/Bun process, generated docs route, or serverless web
handler, read [runtime-assembly.md](runtime-assembly.md).

Transport-wide middleware such as CORS may wrap the final HTTP application.
Keep API middleware for contract-visible behavior and runtime HTTP middleware
for concerns that also apply to raw or non-API routes.

## Use response escape hatches deliberately

Return typed success values and declared errors for ordinary endpoints. Use
`HttpServerResponse` when the boundary must pass through an upstream response,
redirect, set cookies, stream, or control a non-default success status or body.
Use Effect request and cookie APIs to parse boundary state. Reserve unsafe JSON
response constructors for values whose safety is established elsewhere; encode
ordinary typed 4xx failures through the endpoint's declared error schemas.

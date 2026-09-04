# Effect API clients

Derive clients from the shared `HttpApi`; keep request types, response types,
and expected errors owned by the contract.

- [Inventory the client boundary](#inventory-the-client-boundary)
- [Build one Atom API service](#build-one-atom-api-service)
- [Define queries](#define-queries)
- [Define mutations and invalidation](#define-mutations-and-invalidation)
- [Use a direct client outside React](#use-a-direct-client-outside-react)

## Inventory the client boundary

Trace the affected queries and mutations through their `RegistryProvider`,
runtime, `AtomHttpApi.Service`, and reactivity keys. Expand the inspection only
when ownership or invalidation crosses another boundary. Confirm installed
`effect` and `@effect/atom-react` versions before copying signatures.

## Build one Atom API service

Create one module-scoped runtime factory and one `AtomHttpApi.Service`. Share a
memo map when multiple client services must reuse layers.

```ts
import { Effect, Layer } from "effect";
import { FetchHttpClient, HttpClient, HttpClientRequest } from "effect/unstable/http";
import { HttpApiMiddleware } from "effect/unstable/httpapi";
import { Atom, AtomHttpApi } from "effect/unstable/reactivity";
import { ApplicationApi, Authenticate } from "@app/domain";

export const AuthenticateClient = HttpApiMiddleware.layerClient(
  Authenticate,
  Effect.fn("AuthenticateClient")(function* ({ next, request }) {
    const token = yield* readAccessToken;
    return yield* next(HttpClientRequest.bearerToken(request, token));
  }),
);

const ApiHttpClient = Layer.mergeAll(FetchHttpClient.layer, AuthenticateClient);

export const appRuntime = Atom.context({
  memoMap: Layer.makeMemoMapUnsafe(),
});

export const ApiClient = AtomHttpApi.Service()("ApiClient", {
  api: ApplicationApi,
  httpClient: ApiHttpClient,
  runtime: appRuntime,
  baseUrl: "/api",
  transformClient: (client) =>
    HttpClient.mapRequest(client, (request) =>
      HttpClientRequest.setHeader(request, "x-request-id", requestId()),
    ),
});
```

Define the runtime, service, key constructors, queries, and mutations outside
React renders. Keep browser-only identity access behind the client boundary.
When contract middleware sets `requiredForClient`, satisfy it with
`HttpApiMiddleware.layerClient` in the `httpClient` layer. Reserve
`transformClient` for transport-wide behavior such as correlation headers,
base URL changes, tracing, or retries; do not bypass declared API security with
an unrelated raw header transform.

## Define queries

```ts
export const projectKeys = {
  collection: ["projects"] as const,
  project: (id: ProjectId) => [`project:${id}`] as const,
};

export const projectAtom = Atom.family((projectId: ProjectId) =>
  ApiClient.query("projects", "getProject", {
    params: { projectId },
    timeToLive: "5 minutes",
    reactivityKeys: projectKeys.project(projectId),
  }).pipe(Atom.swr({ staleTime: "30 seconds", revalidateOnMount: true })),
);
```

`query(group, endpoint, request)` returns an `Atom<AsyncResult<...>>`. The
service memoizes encoded request keys internally; a public `Atom.family` still
expresses domain ownership and cache policy through a stable scalar or Effect
`Hash`/`Equal` key.

Query options have distinct jobs:

- `timeToLive` controls idle registry retention;
- `reactivityKeys` registers invalidation subscriptions;
- `serializationKey` enables decoded-only hydration serialization and is not
  the runtime cache key;
- `responseMode` changes the response and error shape.

Keep secrets out of serialization keys, URL state, hydration payloads, and
client-visible layers.

## Define mutations and invalidation

```ts
export const updateProjectMutation = ApiClient.mutation("projects", "updateProject");

const updateProject = useAtomSet(updateProjectMutation, { mode: "promise" });

await updateProject({
  params: { projectId },
  payload: patch,
  reactivityKeys: [...projectKeys.collection, ...projectKeys.project(projectId)],
});
```

Successful mutations invalidate matching keys; failed mutations do not.
Centralize reactivity-key constructors because mismatched strings fail silently.
When one mutation affects several exact keys, flatten their arrays; an array
containing key arrays registers those nested arrays as different keys.

Exact array keys invalidate only exact matches. Record-form keys are
hierarchical in current Effect v4 implementations: a property registers both
its broad name and each `property:id` combination. Confirm this against the
installed version before relying on it.

Choose invalidation breadth from the server write:

- invalidate an entity key when only its detail changed;
- invalidate the collection when membership, ordering, totals, or filters can
  change;
- invalidate every affected namespace when a write crosses aggregates.

Combine invalidation with manual refresh only when two requests are intended.
Keep mutation-dependent sequencing and optimistic state in the owning workflow
atom. React renders and dispatches; view-local input and presentation state can
stay in React. See [workflows](effect-atom-workflows.md) when the action needs
optimistic updates, navigation, or other follow-up behavior.

## Use a direct client outside React

```ts
import { Effect } from "effect";
import { FetchHttpClient, HttpClientRequest } from "effect/unstable/http";
import { HttpApiClient, HttpApiMiddleware } from "effect/unstable/httpapi";
import { ApplicationApi, Authenticate } from "@app/domain";
import { AccessToken, AccessTokenLive } from "./AccessToken";

const AuthenticateClient = HttpApiMiddleware.layerClient(
  Authenticate,
  Effect.fn("AuthenticateClient")(function* ({ next, request }) {
    const token = yield* AccessToken;
    return yield* next(HttpClientRequest.bearerToken(request, token.value));
  }),
);

const program = Effect.gen(function* () {
  const client = yield* HttpApiClient.make(ApplicationApi, {
    baseUrl: "https://api.example.com",
  });
  return yield* client.projects.getProject({ params: { projectId } });
}).pipe(Effect.provide([FetchHttpClient.layer, AuthenticateClient, AccessTokenLive]));
```

Use the direct client in services, scripts, tests, and non-React applications.
Provide every required contract middleware at the client composition root.
Transform the underlying `HttpClient` once for non-contract headers, tracing,
retry, or base behavior rather than repeating it at every call site.

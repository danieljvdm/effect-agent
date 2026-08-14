# Effect Atom HTTP queries and invalidation

## Build one API service

`AtomHttpApi.Service` generates a typed client, a runtime, query atoms, and mutation functions. Pass the shared runtime factory so API services share the intended `Layer.MemoMap`:

```ts
import { FetchHttpClient } from "effect/unstable/http";
import { AtomHttpApi } from "effect/unstable/reactivity";
import { appAtomRuntime } from "./atom-runtime";
import { Api } from "./api";

export const ApiClient = AtomHttpApi.Service()("ApiClient", {
  api: Api,
  httpClient: FetchHttpClient.layer,
  baseUrl: "/api",
  runtime: appAtomRuntime,
});
```

Compile examples against the installed Effect version; the reactivity and HTTP APIs are unstable.

## Queries

`query(group, endpoint, request)` returns an `Atom<AsyncResult<...>>`. The service internally memoizes request keys with a family. A public `Atom.family` remains useful for expressing domain ownership with a simple, stable parameter and applying one cache policy.

Query options have separate roles:

- `timeToLive`: finite values apply idle TTL; infinity keeps the query alive;
- `reactivityKeys`: register the query for refresh after matching invalidation;
- `serializationKey`: make decoded-only results serializable for hydration; it is not the runtime cache key;
- `responseMode`: changes the response and error shape.

Never place secrets in `serializationKey`, URL state, hydration payloads, or client-visible layers.

## Mutations and action ownership

Create mutation atoms once, then invoke them from the component or workflow that owns the action:

```ts
export const updateProject = ApiClient.mutation("projects", "update");

// In the initiating component:
const mutate = useAtomSet(updateProject, { mode: "promise" });
await mutate({
  params: { projectId },
  payload: patch,
  reactivityKeys: { projects: [projectId] },
});
```

The mutation's `reactivityKeys` are invalidated only after the request succeeds. Failed mutations do not invalidate. Keep navigation, toasts, dialog closure, and optimistic UI at the action owner; keep shared server-state refresh in reactivity keys.

If the component also renders the mutation result, use `useAtom(updateProject)` instead of pairing `useAtomValue` with `useAtomSet`. A module-scoped mutation atom is shared registry state, so an unconditional cleanup write of `Atom.Interrupt` can cancel work owned by another consumer and publishes an interrupted failure.

Keep a multi-step mutation sequence in an owner that lives for the whole sequence. When navigation can unmount the initiating route after one request succeeds, a component-owned sequential fan-out can leave a partially completed operation. Use one stable workflow atom or service when client ownership is sufficient; use one server-side command or durable workflow when completion must survive browser navigation or disconnects.

## Use one key vocabulary

Array keys represent independent keys. Record keys support hierarchical broad-plus-entity invalidation:

```ts
const listKeys = { projects: [] };
const detailKeys = { projects: [projectId] };

ApiClient.query("projects", "list", { reactivityKeys: listKeys });
ApiClient.query("projects", "get", { params: { projectId }, reactivityKeys: detailKeys });

// This mutation invalidates the broad namespace and this entity key.
await mutate({ params: { projectId }, payload: patch, reactivityKeys: detailKeys });
```

Record semantics register the property name and each `property:id` combination. Consequently, `{ projects: [projectId] }` is hierarchical: it invalidates both the broad `projects` namespace and the specific `projects:projectId` key. Every record-form project query also subscribed to that broad namespace can refresh. Use this when an entity write may affect lists or aggregates.

For exact entity-only invalidation, use namespaced primitive array keys consistently instead:

```ts
const projectKey = (id: string) => `project:${id}`;

const detailKeys = [projectKey(projectId)];
const collectionKeys = ["projects"];

ApiClient.query("projects", "get", { params: { projectId }, reactivityKeys: detailKeys });
await mutate({ params: { projectId }, payload: patch, reactivityKeys: detailKeys });
```

Use `collectionKeys` for the collection query and for mutations that can change membership or ordering.

Standardize key constructors in one module when several endpoints share them; mismatched strings fail silently.

Choose invalidation breadth from the server write:

- invalidate an exact array-form entity key when only one detail changed;
- invalidate the collection key, or use hierarchical record keys, when list membership, ordering, totals, or filters can change;
- invalidate multiple record properties when a write affects related aggregates.

Do not both invalidate and manually refresh the same query unless two requests are intentional.

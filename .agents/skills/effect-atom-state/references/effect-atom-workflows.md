# Effect Atom workflows

Multi-step client actions are Effects composing atoms. Components dispatch
them and render `AsyncResult` state; no orchestration crosses the React
boundary. Confirm exact signatures against the installed
`effect/unstable/reactivity` declarations before copying.

- [Define workflow atoms with Atom.fn](#define-workflow-atoms-with-atomfn)
- [Compose atoms through the fn context](#compose-atoms-through-the-fn-context)
- [Overlay optimistic query values](#overlay-optimistic-query-values)
- [Dispatch from React](#dispatch-from-react)
- [Reinforce the boundary with a lint rule](#reinforce-the-boundary-with-a-lint-rule)

## Define workflow atoms with Atom.fn

`Atom.fn<Input>()(effect)` creates a writable atom: writing an input runs the
effect, and the atom's value is the `AsyncResult` of the latest run. The
effect receives `(input, get: Atom.FnContext)`.

Bare `Atom.fn` accepts no reactivity keys. When the workflow needs Effect
services or invalidation, create it through a runtime factory —
`AtomHttpApi.Service` exposes its own as `Client.runtime.fn`:

```ts
import { Effect } from "effect";
import { Atom, Reactivity } from "effect/unstable/reactivity";

export const updateProject = ApiClient.runtime.fn(
  Effect.fnUntraced(function* (input: { readonly projectId: ProjectId; readonly patch: Patch }) {
    const client = yield* ApiClient;

    return yield* Reactivity.mutation(
      client.projects.updateProject({
        params: { projectId: input.projectId },
        payload: input.patch,
      }),
      [...projectKeys.collection, ...projectKeys.project(input.projectId)],
    );
  }),
);
```

Prefer `Client.mutation(group, endpoint)` with call-site `reactivityKeys` for a
single request; wrap the client call in `runtime.fn` with
`Reactivity.mutation(effect, keys)` when the key set depends on the input or
the workflow spans several requests. Either way, keys invalidate only when the
effect succeeds.

Invalidation spans clients: every `AtomHttpApi.Service` built on the same
runtime factory shares one `Reactivity` instance, so a mutation on one client
invalidates query keys registered by another. A service given its own
`Atom.context()` gets a separate `Reactivity` and cannot invalidate the rest —
share one runtime factory across API services on purpose.

## Compose atoms through the fn context

Inside the effect, the fn context is the composition surface:

- `get.setResult(fnAtom, input)` writes another fn atom and returns an Effect
  of its settled result — the await-another-workflow primitive;
- `get.set(stateAtom, value)` writes a state atom;
- `get(atom)` reads the current value **untracked** — the workflow never
  subscribes, so mutating a state atom from inside the effect cannot
  re-trigger it.

Optimistic echo with rollback, keyed by the owning entity so the atoms
dispose with it:

```ts
import { Effect, Exit } from "effect";

interface PendingComment {
  readonly id: string;
  readonly body: string;
  readonly status: "sending" | "queued";
}

export const pendingCommentsAtom = Atom.family((_projectId: ProjectId) =>
  Atom.make<ReadonlyArray<PendingComment>>([]),
);

export const sendCommentWithEcho = Atom.family((projectId: ProjectId) =>
  Atom.fn<{ readonly body: string }>()(
    Effect.fnUntraced(function* (input, get) {
      const pending = pendingCommentsAtom(projectId);
      const id = yield* Effect.sync(() => crypto.randomUUID());

      get.set(pending, [...get(pending), { id, body: input.body, status: "sending" }]);

      const exit = yield* Effect.exit(get.setResult(sendComment, { projectId, body: input.body }));

      get.set(
        pending,
        Exit.isSuccess(exit)
          ? get(pending).map((entry) =>
              entry.id === id ? { ...entry, status: "queued" as const } : entry,
            )
          : get(pending).filter((entry) => entry.id !== id),
      );

      yield* exit;
    }),
  ),
);
```

The pending list lives in an `Atom.family` state atom, not component
`useState`, so the workflow owns append, settle, and rollback while any
component can render it. Re-raise the exit so the dispatching leaf still
observes failure.

## Overlay optimistic query values

When the optimistic value is the query's own value rather than a sidecar list,
wrap the query with `Atom.optimistic` and drive it with `Atom.optimisticFn`:

```ts
import { AsyncResult } from "effect/unstable/reactivity";

export const timelineAtom = Atom.family((projectId: ProjectId) =>
  Atom.optimistic(timelineQuery(projectId)),
);

export const sendMessage = Atom.family((projectId: ProjectId) =>
  Atom.optimisticFn(timelineAtom(projectId), {
    reducer: (current, message: MessageDto) =>
      AsyncResult.map(current, (page) => ({
        ...page,
        messages: [...page.messages, message],
      })),
    fn: submitMessage(projectId),
  }),
);
```

The reducer computes the provisional value shown while the mutation runs; a
successful transition refreshes the source query, and a failure rolls the
value back to the latest source value. Consumers render `timelineAtom` and
never see the seam.

## Dispatch from React

A promise-mode dispatch is handed bare to a leaf component whose contract is
promise-shaped; presentation derives from the atom's `AsyncResult`:

```tsx
const [sendResult, send] = useAtom(sendMessage(projectId), { mode: "promise" });
const hint = AsyncResult.isSuccess(sendResult) && !sendResult.waiting ? "sent" : undefined;

return <Composer hint={hint} onSubmit={(body) => send({ body })} />;
```

The returned promise resolves with the success value and rejects with the
squashed failure cause; use `mode: "promiseExit"` when the leaf needs the full
`Exit`. Anything more than returning the promise — chaining a refresh, echo
bookkeeping, sequencing a second mutation — belongs in the workflow atom.

## Reinforce the boundary with a lint rule

A repository can back the logic-free boundary with a lint warning on `then`
scoped to component and route modules, mirroring the typed-codec lint pattern:

```ts
{
  files: ["apps/web/src/components/**", "apps/web/src/routes/**"],
  rules: {
    "no-restricted-properties": [
      "warn",
      {
        property: "then",
        message:
          "Compose the workflow in Effect (Atom.fn + reactivity keys) and return promise-mode dispatches bare to the leaf component.",
      },
    ],
  },
}
```

Keep any justified suppression local and documented; the boundary reasoning,
not the lint rule, remains the source of truth.

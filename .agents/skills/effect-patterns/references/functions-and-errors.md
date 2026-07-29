# Effect Functions And Errors

Use this when writing reusable Effect helpers or expected failure paths.

## Effectful Functions

Prefer `Effect.fn("MeaningfulName")` for reusable helpers that return Effects.

```ts
const decodePayload = Effect.fn("decodePayload")(function* (input: unknown) {
  return yield* Schema.decodeUnknownEffect(Payload)(input);
});
```

Use plain `Effect.gen(...)` for one-off inline bodies, entrypoint programs, and
local non-reused control flow.

Use `Effect.fnUntraced` only when tracing overhead is intentionally avoided,
such as very hot low-level methods.

## Promise Boundaries

Use `Effect.tryPromise` around external Promise APIs.

Do not wrap Effect-native APIs in `Effect.tryPromise`.

## Expected Errors

Expected failures are values in the error channel.

Follow the schema-first error pattern in
[`schema-first-modeling.md`](schema-first-modeling.md).

```ts
class WorkspaceNotFound extends Schema.TaggedErrorClass<WorkspaceNotFound>()(
  "WorkspaceNotFound",
  {
    workspaceId: WorkspaceId,
  },
) {}
```

In generators, use the explicit stop-control-flow shape when failing with an
expected error.

```ts
const loadWorkspace = Effect.fn("loadWorkspace")(function* (workspaceId: WorkspaceId) {
  return yield* Effect.fail(new WorkspaceNotFound({ workspaceId }));
});
```

Convert external failures into domain or service errors when callers can react.
Let truly unexpected failures die only at an explicit runtime boundary.

## Avoid

- `throw` for expected failures.
- Plain `Error` in public expected error unions.
- Catch-and-rethrow.
- Blanket `try/catch`.
- `Effect.orDie` inside domain or service logic for recoverable failures.

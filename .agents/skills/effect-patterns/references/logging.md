# Effect Logging

Use this when adding logs in Effect-based API, worker, Durable Object, and
service code.

## Rules

Use `Effect.logInfo`, `Effect.logDebug`, `Effect.logWarning`,
`Effect.logError`, and related Effect logging APIs.

Log inside the Effect being returned.

Use small structured payloads:

- IDs
- counts
- booleans
- operation names

Include errors as structured attributes, not stringified messages.

Prefer stable event-style messages for server and runtime logs. Use concise
sentence-style messages for domain workflow logs when they are clearer.

Runtime boundaries provide logger layers and common annotations. Business logic
should not create module-level logger instances.

## Log Annotations

Use `Effect.annotateLogs` for structured metadata that should appear on every
log line inside an effect. It accepts either a record or a single key/value
pair. Effect stores these annotations in fiber context, and logger backends read
them from the current fiber when a log is emitted.

Annotate at the highest meaningful level possible. Put request, route, tenant,
operation, workflow, and job identifiers around the effect that owns that whole
unit of work instead of repeating them on every log line.

```ts
const runCheckout = (request: CheckoutRequest) =>
  Effect.gen(function* () {
    yield* Effect.logInfo("loading checkout state");
    yield* validateCart(request.cart);
    yield* Effect.logInfo("submitting payment");
    yield* submitPayment(request.payment);
  }).pipe(
    Effect.annotateLogs({
      operation: "checkout",
      requestId: request.id,
      cartId: request.cart.id,
    }),
    Effect.withLogSpan("checkout"),
  );
```

Avoid repeating the same annotations at each log site:

<!-- prettier-ignore-start -->

```ts
// Avoid
yield* Effect.logInfo("loading checkout state").pipe(
  Effect.annotateLogs({ operation: "checkout", requestId }),
);
yield* Effect.logInfo("submitting payment").pipe(
  Effect.annotateLogs({ operation: "checkout", requestId }),
);
```

<!-- prettier-ignore-end -->

Add narrower annotations only when metadata belongs to a nested operation, item,
or attempt rather than the whole parent effect:

<!-- prettier-ignore-start -->

```ts
yield* Effect.forEach(lineItems, (lineItem) =>
  reserveInventory(lineItem).pipe(
    Effect.annotateLogs("lineItemId", lineItem.id),
    Effect.withLogSpan("reserveInventory"),
  ),
);
```

<!-- prettier-ignore-end -->

Use `Effect.annotateLogsScoped` only when an acquired scope should carry
annotations across multiple effects until the scope closes. Prefer ordinary
`Effect.annotateLogs` for most request and operation boundaries.

## Avoid

- `console.*` in new Effect code.
- Custom logging wrapper APIs around `Effect.log*`.
- Broad module-wide annotations when request, operation, or workflow-level
  annotations are more precise.
- Dual old/new logging paths.

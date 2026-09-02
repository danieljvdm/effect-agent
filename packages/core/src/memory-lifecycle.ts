import { Context, Effect, Layer, Schema } from "effect";

import { MemoryContent, MemorySourceReference } from "./memory.ts";

const Identity = Schema.NonEmptyString.check(Schema.isMaxLength(1_024));
const Timestamp = Schema.Finite.check(Schema.isGreaterThanOrEqualTo(0));

/** A host-selected namespace prevents accidental cross-tenant document lookup. */
export class MemoryKey extends Schema.Class<MemoryKey>("@effect-agent/core/MemoryKey")({
  namespace: Identity,
  id: MemorySourceReference.fields.id,
}) {}

const KnownSource = Schema.Struct({
  ...MemorySourceReference.fields,
  revision: Identity,
});
const AccessScopes = Schema.Array(Identity).check(
  Schema.isMaxLength(128),
  Schema.makeFilter((scopes) => new Set(scopes).size === scopes.length, {
    expected: "unique access scopes",
  }),
);
const DocumentFields = {
  version: Schema.Literal(1),
  key: MemoryKey,
  source: KnownSource,
  generation: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: Number.MAX_SAFE_INTEGER })),
  predecessor: Schema.NullOr(MemorySourceReference),
  modifiedAt: Timestamp,
};

export class ActiveMemoryDocument extends Schema.TaggedClass<ActiveMemoryDocument>()(
  "ActiveMemoryDocument",
  {
    ...DocumentFields,
    content: MemoryContent,
    /** Explicit application-defined scope names. Empty grants no recall access. */
    scopes: AccessScopes,
  },
) {}

/** Terminal tombstone. Reusing its ID cannot restore the source through delayed work. */
export class WithdrawnMemoryDocument extends Schema.TaggedClass<WithdrawnMemoryDocument>()(
  "WithdrawnMemoryDocument",
  {
    ...DocumentFields,
    reason: Schema.String.check(Schema.isMaxLength(4_096)),
  },
) {}

export const MemoryDocument = Schema.Union([ActiveMemoryDocument, WithdrawnMemoryDocument]);
export type MemoryDocument = typeof MemoryDocument.Type;

const WriteFields = {
  key: MemoryKey,
  /** The same operation ID must always carry exactly the same Schema-encoded command. */
  operationId: Identity,
};

export const MemoryWrite = Schema.Union([
  Schema.TaggedStruct("Put", {
    ...WriteFields,
    expectedRevision: Schema.NullOr(Identity),
    locator: MemorySourceReference.fields.locator,
    content: MemoryContent,
    scopes: AccessScopes,
  }),
  Schema.TaggedStruct("Withdraw", {
    ...WriteFields,
    expectedRevision: Identity,
    reason: WithdrawnMemoryDocument.fields.reason,
  }),
]);
export type MemoryWrite = typeof MemoryWrite.Type;

export class MemoryConflict extends Schema.TaggedError<MemoryConflict>()("MemoryConflict", {
  key: MemoryKey,
  expectedRevision: Schema.NullOr(Identity),
  actualRevision: Schema.NullOr(Identity),
}) {}

export class MemoryWithdrawn extends Schema.TaggedError<MemoryWithdrawn>()("MemoryWithdrawn", {
  key: MemoryKey,
  revision: Identity,
}) {}

export class MemoryOperationConflict extends Schema.TaggedError<MemoryOperationConflict>()(
  "MemoryOperationConflict",
  {
    key: MemoryKey,
    operationId: Identity,
  },
) {}

export class MemoryStorageError extends Schema.TaggedError<MemoryStorageError>()(
  "MemoryStorageError",
  {
    operation: Schema.NonEmptyString,
    reason: Schema.Literals(["invalid-input", "unavailable", "corrupt", "incompatible"]),
  },
) {}

export const MemoryMutationPoint = Schema.Literals([
  "memory:initialize:before",
  "memory:initialize:after",
  "memory:change:before",
  "memory:change:after-state",
  "memory:change:after-receipt",
  "memory:change:after",
]);
export type MemoryMutationPoint = typeof MemoryMutationPoint.Type;

export class MemoryMutationFailure extends Schema.TaggedError<MemoryMutationFailure>()(
  "MemoryMutationFailure",
  {
    point: MemoryMutationPoint,
  },
) {}

/** Explicit fault-injection seam; production adapters install the no-op Layer. */
export class MemoryMutationFailpoint extends Context.Service<
  MemoryMutationFailpoint,
  {
    readonly hit: (point: MemoryMutationPoint) => Effect.Effect<void, MemoryMutationFailure>;
  }
>()("@effect-agent/core/MemoryMutationFailpoint") {
  static readonly layer = Layer.succeed(this, { hit: () => Effect.void });
}

export type MemoryWriteError =
  | MemoryStorageError
  | MemoryConflict
  | MemoryWithdrawn
  | MemoryOperationConflict
  | MemoryMutationFailure;

/**
 * Read capability independent of editing. A get begun after a committed write must observe
 * that write or a later revision. An adapter unable to provide that view must fail typed.
 * Host code binds namespace and access scope; cached candidates do not implement this port.
 */
export class MemoryReader extends Context.Service<
  MemoryReader,
  {
    readonly get: (key: MemoryKey) => Effect.Effect<MemoryDocument | null, MemoryStorageError>;
  }
>()("@effect-agent/core/MemoryReader") {}

/**
 * Optional conditional writer. Implement only where atomic expected-revision checks and
 * durable idempotency receipts can be guaranteed. Apply authorization before calling it.
 * Reconcile an existing operation receipt before evaluating tombstones or expected revisions:
 * identical commands return the original result; changed commands fail MemoryOperationConflict.
 * Successful withdrawal excludes checks begun afterward; already captured views may finish.
 * Receipts and original Thread history are separate retention concerns.
 */
export class MemoryWriter extends Context.Service<
  MemoryWriter,
  {
    readonly change: (write: MemoryWrite) => Effect.Effect<MemoryDocument, MemoryWriteError>;
  }
>()("@effect-agent/core/MemoryWriter") {}

/**
 * Transition mechanics for an authoritative document store. Revisions identify this readable
 * source, not the original activity from which a consumer extracted it. The adapter reconciles
 * receipts first and owns atomicity and the clock. Original evidence belongs in provenance.
 */
export const applyMemoryWrite = Effect.fn("applyMemoryWrite")(function* (
  current: MemoryDocument | null,
  write: MemoryWrite,
  modifiedAt: number,
) {
  if (
    current !== null &&
    (current.key.namespace !== write.key.namespace ||
      current.key.id !== write.key.id ||
      current.source.id !== write.key.id)
  ) {
    return yield* MemoryStorageError.make({
      operation: "memory transition identity",
      reason: "corrupt",
    });
  }
  if (current?._tag === "WithdrawnMemoryDocument") {
    return yield* MemoryWithdrawn.make({ key: write.key, revision: current.source.revision });
  }
  const actualRevision = current?.source.revision ?? null;
  if (actualRevision !== write.expectedRevision) {
    return yield* MemoryConflict.make({
      key: write.key,
      expectedRevision: write.expectedRevision,
      actualRevision,
    });
  }
  const generation = (current?.generation ?? 0) + 1;
  const fields = {
    version: 1 as const,
    key: write.key,
    source: {
      id: write.key.id,
      locator: write._tag === "Put" ? write.locator : (current?.source.locator ?? ""),
      revision: String(generation),
    },
    generation,
    predecessor: current?.source ?? null,
    modifiedAt,
  };
  const next =
    write._tag === "Put"
      ? {
          _tag: "ActiveMemoryDocument" as const,
          ...fields,
          content: write.content,
          scopes: write.scopes,
        }
      : { _tag: "WithdrawnMemoryDocument" as const, ...fields, reason: write.reason };
  return yield* Schema.decodeUnknownEffect(MemoryDocument)(next).pipe(
    Effect.mapError(() =>
      MemoryStorageError.make({ operation: "memory transition", reason: "invalid-input" }),
    ),
  );
});

import type { MemoryReader } from "@effect-agent/core";
import {
  MemoryAccess,
  MemoryConflict,
  MemoryDocument,
  MemoryLookup,
  MemoryMutationFailure,
  MemoryNamespace,
  MemoryOperationConflict,
  MemoryRecallError,
  MemoryRecallLimits,
  MemoryStorageError,
  MemoryWithdrawn,
  MemoryWrite,
  MemoryWriter,
  revalidateMemoryLookup,
  MemoryIndexSearch,
  MemoryIndexError,
  SemanticMemoryError,
  SemanticMemoryProfile,
  SemanticCandidateLimits,
  SemanticCandidateResult,
  revalidateSemanticMemoryCandidates,
} from "@effect-agent/core";
import { Principal } from "@effect-agent/thread";
import { Clock, Context, Effect, Encoding, Schema } from "effect";

export class MemoryRpcError extends Schema.TaggedError<MemoryRpcError>()("MemoryRpcError", {
  reason: Schema.Literals(["denied", "protocol", "budget", "timeout", "unavailable"]),
}) {}

export class MemoryRpcLimits extends Schema.Class<MemoryRpcLimits>(
  "@effect-agent/storage-cloudflare/MemoryRpcLimits",
)({
  maxRequestBytes: Schema.Int.check(Schema.isBetween({ minimum: 256, maximum: 4_194_304 })),
  maxResponseBytes: Schema.Int.check(Schema.isBetween({ minimum: 256, maximum: 16_777_216 })),
  maxSourceBytes: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 67_108_864 })),
  maxSources: MemoryRecallLimits.fields.maxSources,
  timeoutMillis: MemoryRecallLimits.fields.timeoutMillis,
}) {}

export const defaultMemoryRpcLimits = MemoryRpcLimits.make({
  maxRequestBytes: 1_048_576,
  maxResponseBytes: 4_194_304,
  maxSourceBytes: 16_777_216,
  maxSources: 16,
  timeoutMillis: 10_000,
});

const RequestFields = {
  version: Schema.Literal(1),
  access: MemoryAccess.Wire,
  /** Host-authenticated identity, never copied from model input. The owner still authorizes it. */
  principal: Principal,
  deadlineMillis: Schema.Finite.check(Schema.isGreaterThanOrEqualTo(0)),
};

const RevalidateRequest = Schema.TaggedStruct("Revalidate", {
  ...RequestFields,
  lookup: MemoryLookup,
  limits: MemoryRecallLimits,
});

const ChangeRequest = Schema.TaggedStruct("Change", { ...RequestFields, write: MemoryWrite.Wire });

const SemanticRequest: Schema.TaggedStruct<
  "RevalidateSemantic",
  typeof RequestFields & {
    readonly found: typeof MemoryIndexSearch.Wire;
    readonly profile: typeof SemanticMemoryProfile;
    readonly limits: typeof SemanticCandidateLimits;
  }
> = Schema.TaggedStruct("RevalidateSemantic", {
  ...RequestFields,
  found: MemoryIndexSearch.Wire,
  profile: SemanticMemoryProfile,
  limits: SemanticCandidateLimits,
});

export const MemoryOwnerRequest: Schema.Union<
  [typeof RevalidateRequest, typeof ChangeRequest, typeof SemanticRequest]
> = Schema.Union([RevalidateRequest, ChangeRequest, SemanticRequest]);

export type MemoryOwnerRequest = typeof MemoryOwnerRequest.Type;

export const MemoryOwnerFailure = Schema.Union([
  MemoryRpcError,
  MemoryStorageError,
  MemoryRecallError,
  MemoryConflict,
  MemoryWithdrawn,
  MemoryOperationConflict,
  MemoryMutationFailure,
  MemoryIndexError,
  SemanticMemoryError,
]);

export type MemoryOwnerFailure = typeof MemoryOwnerFailure.Type;

export const MemoryOwnerResponse = Schema.Union([
  Schema.TaggedStruct("Lookup", { access: MemoryAccess.Wire, lookup: MemoryLookup }),
  Schema.TaggedStruct("Changed", { access: MemoryAccess.Wire, document: MemoryDocument.Wire }),
  Schema.TaggedStruct("Semantic", { access: MemoryAccess.Wire, result: SemanticCandidateResult }),
  Schema.TaggedStruct("Failed", { failure: MemoryOwnerFailure }),
]);

export type MemoryOwnerResponse = typeof MemoryOwnerResponse.Type;

/** Fail-closed application policy. Authorize the namespace, principal, scope, and full command. */
export class MemoryOwnerAuthorizer extends Context.Service<
  MemoryOwnerAuthorizer,
  {
    readonly authorize: (request: MemoryOwnerRequest) => Effect.Effect<void, MemoryRpcError>;
  }
>()("@effect-agent/storage-cloudflare/MemoryOwnerAuthorizer") {}

/** Canonical namespace derived from this object's idFromName identity, never its request. */
export class MemoryOwnerIdentity extends Context.Service<
  MemoryOwnerIdentity,
  {
    readonly namespace: MemoryNamespace.Any;
  }
>()("@effect-agent/storage-cloudflare/MemoryOwnerIdentity") {}

export const memoryWireBytes = (text: string): number => Encoding.encodeHex(text).length / 2;

export const decodeMemoryWire = Effect.fn("decodeMemoryWire")(function* <A, I>(
  schema: Schema.Codec<A, I, never>,
  raw: unknown,
  maxBytes: number,
) {
  const text = yield* Schema.decodeUnknownEffect(Schema.String)(raw).pipe(
    Effect.mapError(() => MemoryRpcError.make({ reason: "protocol" })),
  );

  if (text.length > maxBytes || memoryWireBytes(text) > maxBytes)
    return yield* MemoryRpcError.make({ reason: "budget" });

  return yield* Schema.decodeEffect(Schema.fromJsonString(schema))(text).pipe(
    Effect.mapError(() => MemoryRpcError.make({ reason: "protocol" })),
  );
});

export const encodeMemoryWire = Effect.fn("encodeMemoryWire")(function* <A, I>(
  schema: Schema.Codec<A, I, never, never>,
  value: A,
  maxBytes: number,
) {
  const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(schema))(value).pipe(
    Effect.mapError(() => MemoryRpcError.make({ reason: "protocol" })),
  );

  if (encoded.length > maxBytes || memoryWireBytes(encoded) > maxBytes)
    return yield* MemoryRpcError.make({ reason: "budget" });

  return encoded;
});

/** One local read per distinct candidate source, with no per-document network calls. */
export const handleMemoryOwnerRequest = Effect.fn("MemoryOwner.handleRequest")(function* (
  raw: unknown,
  limits: MemoryRpcLimits = defaultMemoryRpcLimits,
): Effect.fn.Return<
  string,
  never,
  MemoryOwnerIdentity | MemoryOwnerAuthorizer | MemoryReader | MemoryWriter
> {
  const result = yield* Effect.gen(function* (): Effect.fn.Return<
    MemoryOwnerResponse,
    MemoryOwnerFailure,
    MemoryOwnerIdentity | MemoryOwnerAuthorizer | MemoryReader | MemoryWriter
  > {
    limits = yield* Schema.decodeUnknownEffect(MemoryRpcLimits)(limits).pipe(
      Effect.mapError(() => MemoryRpcError.make({ reason: "protocol" })),
    );
    const request = yield* decodeMemoryWire(MemoryOwnerRequest, raw, limits.maxRequestBytes);
    const { namespace } = yield* MemoryOwnerIdentity;

    if (
      !MemoryNamespace.equals(namespace, request.access.namespace) ||
      (request._tag === "Change" &&
        !MemoryNamespace.equals(namespace, request.write.key.namespace)) ||
      (request._tag === "RevalidateSemantic" &&
        request.found.candidates.some(
          (candidate) => !MemoryNamespace.equals(namespace, candidate.key.namespace),
        ))
    )
      return yield* MemoryRpcError.make({ reason: "denied" });

    const remaining = Math.min(
      limits.timeoutMillis,
      request.deadlineMillis - (yield* Clock.currentTimeMillis),
    );

    if (remaining <= 0) return yield* MemoryRpcError.make({ reason: "timeout" });

    return yield* Effect.gen(function* (): Effect.fn.Return<
      MemoryOwnerResponse,
      MemoryOwnerFailure,
      MemoryOwnerAuthorizer | MemoryReader | MemoryWriter
    > {
      const authorizer = yield* MemoryOwnerAuthorizer;

      yield* authorizer.authorize(request);
      if (request._tag === "Change") {
        const writer = yield* MemoryWriter;

        return {
          _tag: "Changed",
          access: request.access,
          document: yield* writer.change(request.write),
        };
      }
      if (request._tag === "RevalidateSemantic") {
        if (
          new Set(request.found.candidates.map((candidate) => candidate.key.id)).size >
          limits.maxSources
        )
          return yield* MemoryRpcError.make({ reason: "budget" });

        const result = yield* revalidateSemanticMemoryCandidates(
          request.found,
          request.access,
          request.profile,
          {
            ...request.limits,
            maxSourceBytes: Math.min(
              limits.maxSourceBytes,
              request.limits.maxSourceBytes ?? 16_777_216,
            ),
            maxOutputBytes: Math.min(
              limits.maxResponseBytes,
              request.limits.maxOutputBytes ?? 16_777_216,
            ),
          },
        );

        return { _tag: "Semantic", access: request.access, result };
      }

      const count =
        request.lookup._tag === "Found"
          ? new Set(request.lookup.passages.map((passage) => passage.source.id)).size
          : 0;

      if (count > Math.min(limits.maxSources, request.limits.maxSources))
        return yield* MemoryRpcError.make({ reason: "budget" });

      const lookup = yield* revalidateMemoryLookup(request.lookup, request.access, {
        maxSourceBytes: limits.maxSourceBytes,
        maxInputBytes: Math.min(limits.maxSourceBytes, request.limits.maxInputBytes ?? 16_777_216),
      });

      return { _tag: "Lookup", access: request.access, lookup };
    }).pipe(
      Effect.scoped,
      Effect.timeoutOrElse({
        duration: remaining,
        orElse: () => Effect.fail(MemoryRpcError.make({ reason: "timeout" })),
      }),
    );
  }).pipe(
    Effect.flatMap((response) =>
      encodeMemoryWire(MemoryOwnerResponse, response, limits.maxResponseBytes),
    ),
    Effect.result,
  );

  if (result._tag === "Success") return result.success;

  return yield* encodeMemoryWire(
    MemoryOwnerResponse,
    {
      _tag: "Failed",
      failure: result.failure,
    },
    limits.maxResponseBytes,
  ).pipe(
    Effect.catch(() =>
      Schema.encodeEffect(Schema.fromJsonString(MemoryOwnerResponse))({
        _tag: "Failed",
        failure: MemoryRpcError.make({ reason: "budget" }),
      }).pipe(Effect.orDie),
    ),
  );
});

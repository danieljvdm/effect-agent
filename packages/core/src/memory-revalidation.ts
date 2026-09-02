import { Effect, Encoding, Schema } from "effect";

import {
  MemoryDocument,
  MemoryKey,
  MemoryReader,
  MemoryScope,
  MemoryStorageError,
} from "./memory-lifecycle.ts";
import type { MemoryNamespace } from "./memory-namespace.ts";
import { MemoryLookup, MemoryPassage, MemoryRecallError, MemoryRecallLimits } from "./memory.ts";

const RevalidationLimits = Schema.Struct({
  maxInputBytes: MemoryRecallLimits.fields.maxInputBytes,
  maxSourceBytes: MemoryRecallLimits.fields.maxInputBytes,
});

/** Bind these values in host code. Neither is a model-selectable retrieval parameter. */
class MemoryAccessWire extends Schema.Class<MemoryAccessWire>(
  "@effect-agent/capabilities/MemoryAccess",
)({
  namespace: MemoryKey.Wire.fields.namespace,
  scope: MemoryScope,
}) {}

export type MemoryAccess<Namespace extends MemoryNamespace.Any = MemoryNamespace.Any> = Omit<
  MemoryAccessWire,
  "namespace"
> & { readonly namespace: Namespace };

export const MemoryAccess = {
  Wire: MemoryAccessWire,
  make: <Namespace extends MemoryNamespace.Any>(
    fields: MemoryAccess<Namespace>,
  ): MemoryAccess<Namespace> =>
    Object.assign(Schema.decodeUnknownSync(MemoryAccessWire)(fields), {
      namespace: fields.namespace,
    }),
};

/**
 * Resolve derivative candidates against the current authoritative source and host-bound access.
 * The reader needs no write capability. A same-revision excerpt is kept only if its exact text
 * occurs in the current source; all attribution and metadata come from that source. Otherwise
 * the whole current document replaces it. Aggregate output JSON is bounded before retention,
 * including duplicates, by maxInputBytes, defaulting to 16 MiB with a 64 MiB maximum.
 * maxSourceBytes optionally bounds aggregate encoded authoritative documents, once per source.
 * One authoritative document is decoded at a time; reader-side allocations precede these bounds.
 *
 * There is no cache. One invocation reads each distinct source once. Checks begun after a
 * successful withdrawal or access revocation exclude that source; an in-flight snapshot can
 * finish, including its same-Turn overflow retry. Original history and prior outputs remain.
 */
export const revalidateMemoryLookup = Effect.fn("revalidateMemoryLookup")(function* (
  lookup: MemoryLookup,
  access: MemoryAccess,
  limits: Pick<MemoryRecallLimits, "maxInputBytes"> & { readonly maxSourceBytes?: number } = {},
) {
  const decodedLimits = yield* Schema.decodeUnknownEffect(RevalidationLimits)(limits).pipe(
    Effect.mapError(() =>
      MemoryRecallError.make({ reason: "invalid-input", message: "Invalid revalidation limits" }),
    ),
  );

  const decodedAccess = yield* Schema.decodeUnknownEffect(MemoryAccess.Wire)(access).pipe(
    Effect.mapError(() =>
      MemoryRecallError.make({ reason: "invalid-input", message: "Invalid host memory access" }),
    ),
  );

  const decoded = yield* Schema.decodeUnknownEffect(MemoryLookup)(lookup).pipe(
    Effect.mapError(() =>
      MemoryRecallError.make({ reason: "invalid-input", message: "Malformed memory candidates" }),
    ),
  );

  if (decoded._tag !== "Found") return decoded;
  const reader = yield* MemoryReader;

  const groups = new Map<
    string,
    Array<{ readonly candidate: MemoryPassage; readonly index: number }>
  >();

  for (const [index, candidate] of decoded.passages.entries()) {
    const group = groups.get(candidate.source.id);

    if (group === undefined) groups.set(candidate.source.id, [{ candidate, index }]);
    else group.push({ candidate, index });
  }
  const passages: Array<{ readonly passage: MemoryPassage; readonly index: number }> = [];
  const maxInputBytes = decodedLimits.maxInputBytes ?? 16_777_216;
  let inputBytes = 0;
  let sourceBytes = 0;

  for (const [sourceId, group] of groups) {
    yield* Effect.yieldNow;
    const key = MemoryKey.make({ namespace: decodedAccess.namespace, id: sourceId });

    const document = yield* reader
      .get(key)
      .pipe(
        Effect.flatMap((value) =>
          Schema.decodeUnknownEffect(Schema.NullOr(MemoryDocument.Wire))(value).pipe(
            Effect.mapError(() =>
              MemoryStorageError.make({ operation: "validate source view", reason: "corrupt" }),
            ),
          ),
        ),
      );

    if (
      document !== null &&
      (document.key.namespace.address !== key.namespace.address ||
        document.key.id !== key.id ||
        document.source.id !== key.id)
    ) {
      return yield* MemoryStorageError.make({
        operation: "validate source identity",
        reason: "corrupt",
      });
    }
    if (
      document === null ||
      document._tag === "WithdrawnMemoryDocument" ||
      !document.scopes.includes(decodedAccess.scope)
    )
      continue;
    if (decodedLimits.maxSourceBytes !== undefined) {
      sourceBytes += Encoding.encodeHex(JSON.stringify(document)).length / 2;
      if (sourceBytes > decodedLimits.maxSourceBytes)
        return yield* MemoryRecallError.make({
          reason: "budget",
          sourceId,
          message: "Authoritative memory source byte budget exceeded",
        });
    }
    for (const { candidate, index } of group) {
      const sameExcerpt =
        document.source.revision === candidate.source.revision &&
        document.content.text.includes(candidate.content.text);

      const passage = MemoryPassage.make({
        version: 1,
        authority: decodedAccess.namespace.address,
        source: document.source,
        passageId: sameExcerpt ? candidate.passageId : "document",
        content: {
          ...document.content,
          text: sameExcerpt ? candidate.content.text : document.content.text,
        },
      });

      const encoded = JSON.stringify(passage);
      const remaining = maxInputBytes - inputBytes;

      const encodedBytes =
        encoded.length <= remaining ? Encoding.encodeHex(encoded).length / 2 : undefined;

      if (encodedBytes === undefined || encodedBytes > remaining) {
        return yield* MemoryRecallError.make({
          reason: "budget",
          sourceId,
          message: "Revalidated memory input encoding budget exceeded",
        });
      }
      inputBytes += encodedBytes;
      passages.push({ passage, index });
    }
  }

  return passages.length === 0
    ? { _tag: "NoMatch" as const }
    : {
        _tag: "Found" as const,
        passages: passages
          .sort((left, right) => left.index - right.index)
          .map(({ passage }) => passage),
      };
});

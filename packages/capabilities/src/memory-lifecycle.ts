import {
  MemoryDocument,
  MemoryKey,
  MemoryLookup,
  MemoryPassage,
  MemoryReader,
  MemoryRecallError,
  MemoryStorageError,
} from "@effect-agent/core";
import { Effect, Schema } from "effect";

/** Bind these values in host code. Neither is a model-selectable retrieval parameter. */
export class MemoryAccess extends Schema.Class<MemoryAccess>(
  "@effect-agent/capabilities/MemoryAccess",
)({
  namespace: MemoryKey.fields.namespace,
  scope: Schema.NonEmptyString.check(Schema.isMaxLength(1_024)),
}) {}

/**
 * Resolve derivative candidates against the current authoritative source and host-bound access.
 * The reader needs no write capability. A same-revision excerpt is kept only if its exact text
 * occurs in the current source; all attribution and metadata come from that source. Otherwise
 * the whole current document replaces it and normal recall selection enforces the budget.
 *
 * There is no cache. One invocation reads each distinct source once. Checks begun after a
 * successful withdrawal or access revocation exclude that source; an in-flight snapshot can
 * finish, including its same-Turn overflow retry. Original history and prior outputs remain.
 */
export const revalidateMemoryLookup = Effect.fn("revalidateMemoryLookup")(function* (
  lookup: MemoryLookup,
  access: MemoryAccess,
) {
  const decodedAccess = yield* Schema.decodeUnknownEffect(MemoryAccess)(access).pipe(
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
  const documents = new Map<string, MemoryDocument | null>();
  const passages: Array<MemoryPassage> = [];
  for (const candidate of decoded.passages) {
    let document = documents.get(candidate.source.id);
    if (document === undefined) {
      const key = MemoryKey.make({ namespace: decodedAccess.namespace, id: candidate.source.id });
      document = yield* reader
        .get(key)
        .pipe(
          Effect.flatMap((value) =>
            Schema.decodeUnknownEffect(Schema.NullOr(MemoryDocument))(value).pipe(
              Effect.mapError(() =>
                MemoryStorageError.make({ operation: "validate source view", reason: "corrupt" }),
              ),
            ),
          ),
        );
      if (
        document !== null &&
        (document.key.namespace !== key.namespace ||
          document.key.id !== key.id ||
          document.source.id !== key.id)
      ) {
        return yield* MemoryStorageError.make({
          operation: "validate source identity",
          reason: "corrupt",
        });
      }
      documents.set(candidate.source.id, document);
    }
    if (
      document === null ||
      document._tag === "WithdrawnMemoryDocument" ||
      !document.scopes.includes(decodedAccess.scope)
    )
      continue;
    const sameExcerpt =
      document.source.revision === candidate.source.revision &&
      document.content.text.includes(candidate.content.text);
    passages.push(
      MemoryPassage.make({
        version: 1,
        source: document.source,
        passageId: sameExcerpt ? candidate.passageId : "document",
        content: {
          ...document.content,
          text: sameExcerpt ? candidate.content.text : document.content.text,
        },
      }),
    );
  }
  return passages.length === 0
    ? { _tag: "NoMatch" as const }
    : { _tag: "Found" as const, passages };
});

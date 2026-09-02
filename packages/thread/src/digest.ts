import type { PlatformError } from "effect";
import { Crypto, Effect, Encoding, Schema } from "effect";

import type { DefinitionDigestInput } from "./records.ts";
import { CanonicalBatch, DefinitionDigests, Digest } from "./records.ts";

export class DigestError extends Schema.TaggedError<DigestError>()("DigestError", {
  message: Schema.String,
  cause: Schema.optionalKey(Schema.Defect()),
}) {}

const JsonArray = Schema.Array(Schema.Json);
const isJsonArray = Schema.is(JsonArray);

const canonicalJson = (value: Schema.Json): string => {
  if (
    value === null ||
    typeof value === "boolean" ||
    typeof value === "number" ||
    typeof value === "string"
  ) {
    return JSON.stringify(value);
  }
  if (isJsonArray(value)) {
    return `[${value.map(canonicalJson).join(",")}]`;
  }

  const entries = Object.entries(value).sort(([left], [right]) =>
    left < right ? -1 : left > right ? 1 : 0,
  );

  return `{${entries
    .map(([key, entry]) => `${JSON.stringify(key)}:${canonicalJson(entry)}`)
    .join(",")}}`;
};

/** Digest a JSON value using a stable, locale-independent object-key ordering (UTF-16 code units, RFC 8785 style). */
export const digestJson = Effect.fn("Thread.digestJson")(function* (
  value: Schema.Json,
): Effect.fn.Return<Digest, DigestError, Crypto.Crypto> {
  const crypto = yield* Crypto.Crypto;

  const bytes = yield* Effect.fromResult(
    Encoding.decodeHex(Encoding.encodeHex(canonicalJson(value))),
  ).pipe(Effect.mapError(() => DigestError.make({ message: "Canonical JSON encoding failed" })));

  const digest = yield* crypto
    .digest("SHA-256", bytes)
    .pipe(
      Effect.mapError((error: PlatformError.PlatformError) =>
        DigestError.make({ message: `SHA-256 failed: ${error.message}`, cause: error }),
      ),
    );

  return yield* Schema.decodeUnknownEffect(Digest)(Encoding.encodeHex(digest)).pipe(
    Effect.mapError(() => DigestError.make({ message: "SHA-256 returned an invalid digest" })),
  );
});

/** Digest a canonical batch together with the prior tail to form an append-only hash chain. */
export const digestCanonicalBatch = Effect.fn("Thread.digestCanonicalBatch")(
  (
    previousTailDigest: Digest,
    batch: CanonicalBatch,
  ): Effect.Effect<Digest, DigestError, Crypto.Crypto> =>
    Schema.encodeEffect(CanonicalBatch)(batch).pipe(
      Effect.mapError(() => DigestError.make({ message: "Canonical batch encoding failed" })),
      Effect.flatMap((encoded) =>
        digestJson({
          previousTailDigest,
          batch: encoded,
        }),
      ),
    ),
);

/** Digest one schema-encoded Agent, Model, or Toolkit definition. */
export const digestDefinition = Effect.fn("Thread.digestDefinition")((definition: Schema.Json) =>
  digestJson(definition),
);

/** Digest all replay-relevant definitions without hiding which authority changed. */
export const digestDefinitions = Effect.fn("Thread.digestDefinitions")(
  (
    definitions: DefinitionDigestInput,
  ): Effect.Effect<DefinitionDigests, DigestError, Crypto.Crypto> =>
    Effect.all({
      agent: digestDefinition(definitions.agent),
      model: digestDefinition(definitions.model),
      tools: digestDefinition(definitions.tools),
    }).pipe(Effect.map((digests) => DefinitionDigests.make(digests))),
);

export const EMPTY_TAIL_DIGEST = Schema.decodeSync(Digest)(
  "0000000000000000000000000000000000000000000000000000000000000000",
);

import { Crypto, Effect, Encoding, PlatformError, Schema } from "effect";

import { CanonicalBatch, DefinitionDigests, Digest } from "./records.ts";

export class DigestError extends Schema.TaggedErrorClass<DigestError>()("DigestError", {
  message: Schema.String,
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
  const keys = Object.keys(value).sort();
  return `{${keys.map((key) => `${JSON.stringify(key)}:${canonicalJson(value[key]!)}`).join(",")}}`;
};

/** Digest a JSON value using a stable object-key ordering. */
export const digestJson = (value: Schema.Json): Effect.Effect<Digest, DigestError, Crypto.Crypto> =>
  Effect.gen(function* () {
    const crypto = yield* Crypto.Crypto;
    const bytes = yield* Effect.fromResult(
      Encoding.decodeHex(Encoding.encodeHex(canonicalJson(value))),
    ).pipe(Effect.mapError((error) => DigestError.make({ message: error.message })));
    const digest = yield* crypto
      .digest("SHA-256", bytes)
      .pipe(
        Effect.mapError((error: PlatformError.PlatformError) =>
          DigestError.make({ message: `SHA-256 failed: ${error.message}` }),
        ),
      );
    return yield* Schema.decodeUnknownEffect(Digest)(Encoding.encodeHex(digest)).pipe(
      Effect.mapError((error) => DigestError.make({ message: error.message })),
    );
  });

/** Digest a canonical batch together with the prior tail to form an append-only hash chain. */
export const digestCanonicalBatch = (
  previousTailDigest: Digest,
  batch: CanonicalBatch,
): Effect.Effect<Digest, DigestError, Crypto.Crypto> =>
  Schema.encodeEffect(CanonicalBatch)(batch).pipe(
    Effect.mapError((error) => DigestError.make({ message: error.message })),
    Effect.flatMap((encoded) =>
      digestJson({
        previousTailDigest,
        batch: encoded,
      }),
    ),
  );

/** Digest one schema-encoded Agent, Model, or Toolkit definition. */
export const digestDefinition = digestJson;

/** Digest all replay-relevant definitions without hiding which authority changed. */
export const digestDefinitions = (definitions: {
  readonly agent: Schema.Json;
  readonly model: Schema.Json;
  readonly tools: Schema.Json;
}): Effect.Effect<DefinitionDigests, DigestError, Crypto.Crypto> =>
  Effect.all({
    agent: digestDefinition(definitions.agent),
    model: digestDefinition(definitions.model),
    tools: digestDefinition(definitions.tools),
  }).pipe(Effect.map((digests) => DefinitionDigests.make(digests)));

export const EMPTY_TAIL_DIGEST = Schema.decodeSync(Digest)(
  "0000000000000000000000000000000000000000000000000000000000000000",
);

import { Encoding, Schema } from "effect";

const Identity = Schema.NonEmptyString.check(Schema.isMaxLength(1_024));
const Locator = Schema.NonEmptyString.check(Schema.isMaxLength(8_192));
const Timestamp = Schema.Number.check(Schema.isFinite(), Schema.isGreaterThanOrEqualTo(0));

/** Consumer-defined JSON metadata, bounded independently of readable content. */
export const MemoryMetadata = Schema.Record(
  Schema.String.check(Schema.isMaxLength(128)),
  Schema.Json,
).check(
  Schema.makeFilter(
    (metadata) =>
      Object.keys(metadata).length <= 64 &&
      Encoding.encodeHex(JSON.stringify(metadata)).length / 2 <= 8_192,
    { expected: "at most 64 metadata keys and 8192 UTF-8 bytes" },
  ),
);

/** Identity in the consumer's authoritative source. `null` means revision is unknown. */
export class MemorySourceReference extends Schema.Class<MemorySourceReference>(
  "@effect-agent/core/MemorySourceReference",
)({
  id: Identity,
  locator: Locator,
  revision: Schema.NullOr(Identity),
}) {}

/**
 * Evidence of who said something, where, when, and who observed it. Interpretation is
 * consumer-defined, for example "proposal" or "inference". Repeated references retain
 * the same originId; they are not independent corroboration. Unknown activity time is null.
 */
export class MemoryAttribution extends Schema.Class<MemoryAttribution>(
  "@effect-agent/core/MemoryAttribution",
)({
  originId: Identity,
  speaker: Identity,
  observers: Schema.Array(Identity).check(
    Schema.isMaxLength(128),
    Schema.makeFilter((observers) => new Set(observers).size === observers.length, {
      expected: "unique observers",
    }),
  ),
  locator: Locator,
  activityAt: Schema.NullOr(Timestamp),
  interpretation: Schema.NonEmptyString.check(Schema.isMaxLength(4_096)),
}) {}

/**
 * Readable authoritative content, independent of any index, model, or fixed memory taxonomy.
 * Recording/extraction time never substitutes for the original source activity time.
 */
export class MemoryContent extends Schema.Class<MemoryContent>("@effect-agent/core/MemoryContent")({
  text: Schema.NonEmptyString.check(Schema.isMaxLength(1_048_576)),
  attributions: Schema.Array(MemoryAttribution).check(
    Schema.isMinLength(1),
    Schema.isMaxLength(128),
  ),
  metadata: MemoryMetadata,
  recordedAt: Timestamp,
  extractedAt: Schema.optionalKey(Timestamp),
}) {}

/** A bounded passage supplied directly by a document reader or external retriever. */
export class MemoryPassage extends Schema.Class<MemoryPassage>("@effect-agent/core/MemoryPassage")({
  version: Schema.Literal(1),
  /** Host-bound authority for composition, not model-visible metadata or an access grant. */
  authority: Schema.optionalKey(Schema.NonEmptyString.check(Schema.isMaxLength(4_096))),
  source: MemorySourceReference,
  passageId: Identity,
  content: MemoryContent,
}) {}

/** No-match, unavailable, and insufficient freshness are distinct consumer-visible outcomes. */
export const MemoryLookup = Schema.Union([
  Schema.TaggedStruct("Found", {
    passages: Schema.Array(MemoryPassage).check(Schema.isMaxLength(128)),
  }),
  Schema.TaggedStruct("NoMatch", {}),
  Schema.TaggedStruct("Unavailable", {
    message: Schema.String.check(Schema.isMaxLength(4_096)),
  }),
  Schema.TaggedStruct("InsufficientFreshness", {
    message: Schema.String.check(Schema.isMaxLength(4_096)),
  }),
]);
export type MemoryLookup = typeof MemoryLookup.Type;

/** Output bounds cover the complete rendered reference text, including citations and provenance. */
export class MemoryRecallLimits extends Schema.Class<MemoryRecallLimits>(
  "@effect-agent/core/MemoryRecallLimits",
)({
  maxSources: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 16 })),
  maxItems: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 128 })),
  maxBytes: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 4_194_304 })),
  maxTokens: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 1_048_576 })),
  /** Aggregate UTF-8 JSON passage input, including omitted/duplicate candidates. Defaults to 16 MiB. */
  maxInputBytes: Schema.optionalKey(
    Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 67_108_864 })),
  ),
  timeoutMillis: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 60_000 })),
}) {}

/** A recall contract or essential-source requirement could not be satisfied. */
export class MemoryRecallError extends Schema.TaggedError<MemoryRecallError>()(
  "MemoryRecallError",
  {
    reason: Schema.Literals([
      "invalid-input",
      "unavailable",
      "insufficient-freshness",
      "budget",
      "timeout",
    ]),
    sourceId: Schema.optionalKey(Identity),
    message: Schema.String.check(Schema.isMaxLength(4_096)),
  },
) {}

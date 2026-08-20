import { RunEvent } from "@effect-agent/core";
import { Context, Effect, Layer, Schema } from "effect";

const MAX_REDACTED_PREVIEW_BYTES = 8 * 1024;
const MAX_REDACTION_NODES = 4_096;
const MAX_REDACTION_DEPTH = 16;

const utf8Bytes = (value: string): number => {
  let total = 0;
  for (const character of value) {
    const codePoint = character.codePointAt(0) ?? 0;
    total += codePoint <= 0x7f ? 1 : codePoint <= 0x7ff ? 2 : codePoint <= 0xffff ? 3 : 4;
  }
  return total;
};

/** Branded evidence that a preview passed through the configured structural Redactor. */
export const RedactedPreview = Schema.String.pipe(
  Schema.refine((value): value is string => utf8Bytes(value) <= MAX_REDACTED_PREVIEW_BYTES, {
    expected: `redacted preview of at most ${MAX_REDACTED_PREVIEW_BYTES} UTF-8 bytes`,
  }),
  Schema.brand("@effect-agent/capabilities/RedactedPreview"),
);
export type RedactedPreview = typeof RedactedPreview.Type;

/** Structural redaction failed closed rather than returning unreviewed caller text. */
export class RedactionError extends Schema.TaggedError<RedactionError>()("RedactionError", {
  reason: Schema.Literals(["input-too-deep", "input-too-large", "encoding-failed"]),
  message: Schema.String,
}) {}

/** Redaction owns the boundary from decoded Tool values to bounded audit/model previews. */
export class Redactor extends Context.Service<
  Redactor,
  {
    readonly redact: (decodedValue: unknown) => Effect.Effect<RedactedPreview, RedactionError>;
  }
>()("@effect-agent/capabilities/Redactor") {}

interface RedactionCounter {
  nodes: number;
}

const redactValue = (value: unknown, depth: number, counter: RedactionCounter): Schema.Json => {
  if (depth > MAX_REDACTION_DEPTH) {
    throw RedactionError.make({
      reason: "input-too-deep",
      message: `Decoded value exceeds redaction depth ${MAX_REDACTION_DEPTH}`,
    });
  }
  counter.nodes += 1;
  if (counter.nodes > MAX_REDACTION_NODES) {
    throw RedactionError.make({
      reason: "input-too-large",
      message: `Decoded value exceeds redaction node count ${MAX_REDACTION_NODES}`,
    });
  }
  if (Array.isArray(value)) {
    return value.map((item) => redactValue(item, depth + 1, counter));
  }
  if (value !== null && typeof value === "object") {
    const redacted: Record<string, Schema.Json> = {};
    for (const [key, item] of Object.entries(value)) {
      redacted[key] = redactValue(item, depth + 1, counter);
    }
    return redacted;
  }
  // The default policy is deliberately type-aware and fail-closed: it preserves
  // object/array shape for reviewer context but never copies a decoded scalar,
  // regardless of its field name. Applications that need visible values provide
  // a Tool-specific Redactor after validating those values against that Tool's
  // input Schema.
  if (value === null) return "[REDACTED:null]";
  if (typeof value === "string") return "[REDACTED:string]";
  if (typeof value === "number") return "[REDACTED:number]";
  if (typeof value === "boolean") return "[REDACTED:boolean]";
  return "[REDACTED:unsupported]";
};

const truncateUtf8 = (value: string, maxBytes: number): string => {
  if (utf8Bytes(value) <= maxBytes) {
    return value;
  }
  const suffix = "…";
  const suffixBytes = utf8Bytes(suffix);
  let output = "";
  let bytes = 0;
  for (const character of value) {
    const characterBytes = utf8Bytes(character);
    if (bytes + characterBytes + suffixBytes > maxBytes) {
      break;
    }
    output += character;
    bytes += characterBytes;
  }
  return `${output}${suffix}`;
};

const encodeRunEvent = Schema.encodeEffect(RunEvent);

/**
 * One composed step from live Run Events to structurally redacted transcript lines
 * (P7 WP7 friction fix; SEC-008/testing.md §12 "structurally redacted" live transcripts):
 * Schema-encode each event through the canonical `RunEvent` union, then pass the encoded
 * value through the configured `Redactor` — so the safe path is the short path and no live
 * suite hand-assembles (or accidentally skips) the encode→redact pair. Engine-constructed
 * events always encode, so an encode failure is a defect, never a silent omission.
 */
export const redactedTranscript = (
  events: Iterable<RunEvent>,
): Effect.Effect<ReadonlyArray<RedactedPreview>, RedactionError, Redactor> =>
  Effect.gen(function* () {
    const redactor = yield* Redactor;
    const lines: Array<RedactedPreview> = [];
    for (const event of events) {
      const encoded = yield* encodeRunEvent(event).pipe(Effect.orDie);
      lines.push(yield* redactor.redact(encoded));
    }
    return lines;
  });

/**
 * Default structural implementation. It exposes only collection shape and
 * scalar types, so an ordinary field such as `note` cannot bypass redaction.
 * Tool-specific Layers may expose explicitly reviewed fields instead.
 */
export const StructuralRedactorLive = Layer.succeed(Redactor)({
  redact: (decodedValue) =>
    Effect.try({
      try: () => {
        const structurallyRedacted = redactValue(decodedValue, 0, { nodes: 0 });
        return truncateUtf8(JSON.stringify(structurallyRedacted), MAX_REDACTED_PREVIEW_BYTES);
      },
      catch: (cause) =>
        Schema.is(RedactionError)(cause)
          ? cause
          : RedactionError.make({
              reason: "encoding-failed",
              message: "Could not encode the structurally redacted preview",
            }),
    }).pipe(
      Effect.flatMap((preview) =>
        Schema.decodeUnknownEffect(RedactedPreview)(preview).pipe(
          Effect.mapError(() =>
            RedactionError.make({
              reason: "encoding-failed",
              message: "Redacted preview did not satisfy its bounded schema",
            }),
          ),
        ),
      ),
    ),
});

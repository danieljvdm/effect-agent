import { RunId } from "@effect-agent/core/Identifiers";
import { contextWindowId } from "@effect-agent/engine/Compaction";
import { ContextHistoryError, ContextHistorySearch } from "@effect-agent/engine/ContextHistory";
import { Effect, Schema } from "effect";
import { Prompt } from "effect/unstable/ai";

import type { CanonicalRecordEnvelope } from "./Records.ts";
import { CanonicalSequence, PersistedJson, RecordId } from "./Records.ts";

/** Full retained recall text from one canonical record; never raw discarded Tool output. */
export class ContextHistoryEvidence extends Schema.Class<ContextHistoryEvidence>(
  "@effect-agent/thread/ContextHistoryEvidence",
)({
  recordId: RecordId,
  sequence: CanonicalSequence,
  runId: RunId,
  text: Schema.NonEmptyString,
}) {}

/** The committing position and covered position are distinct; membership uses coversThrough. */
export class ContextHistoryBoundary extends Schema.Class<ContextHistoryBoundary>(
  "@effect-agent/thread/ContextHistoryBoundary",
)({
  sequence: CanonicalSequence,
  coversThrough: CanonicalSequence,
  windowId: Schema.NonEmptyString,
}) {}

/** An empty projection still consumes its canonical position in an index watermark. */
export class ContextHistoryProjection extends Schema.Class<ContextHistoryProjection>(
  "@effect-agent/thread/ContextHistoryProjection",
)({
  evidence: Schema.optionalKey(ContextHistoryEvidence),
  boundary: Schema.optionalKey(ContextHistoryBoundary),
}) {}

const unavailable = () =>
  ContextHistoryError.make({
    reason: "unavailable",
    message: "Canonical context history is unavailable",
  });

const jsonText = Effect.fn("ThreadContextHistoryProjection.jsonText")(function* (value: unknown) {
  const json = yield* Schema.decodeUnknownEffect(PersistedJson)(value).pipe(
    Effect.mapError(unavailable),
  );

  return JSON.stringify(json);
});

const promptText = Effect.fn("ThreadContextHistoryProjection.promptText")(function* (
  value: PersistedJson,
) {
  const prompt = yield* Schema.decodeUnknownEffect(Prompt.Prompt)(value).pipe(
    Effect.mapError(unavailable),
  );

  const messages: Array<string> = [];

  for (const message of prompt.content) {
    // Provider options, system instructions, reasoning, and attachment bytes are not recall data.
    if (message.role === "system") continue;
    const parts: Array<string> = [];

    for (const part of message.content) {
      switch (part.type) {
        case "text":
          parts.push(part.text);
          break;
        case "tool-call":
          parts.push(`tool call ${part.name} (${part.id}):\n${yield* jsonText(part.params)}`);
          break;
        case "tool-result":
          parts.push(
            `tool result ${part.name} (${part.id}; ${part.isFailure ? "failure" : "success"}):\n${yield* jsonText(part.result)}`,
          );
          break;
        case "file":
          parts.push("[attachment omitted]");
          break;
        case "reasoning":
        case "tool-approval-request":
        case "tool-approval-response":
          break;
      }
    }
    if (parts.length > 0) messages.push(`${message.role}:\n${parts.join("\n")}`);
  }

  return messages.join("\n\n");
});

const retainedEvidence = Effect.fn("ThreadContextHistoryProjection.evidence")(function* (
  envelope: CanonicalRecordEnvelope,
): Effect.fn.Return<ContextHistoryEvidence | undefined, ContextHistoryError> {
  const payload = envelope.record.payload;
  let text: string;

  if (payload._tag === "ModelResponseRecorded") {
    text = yield* promptText(payload.messages);
  } else if (payload._tag === "ModelCompleted") {
    text =
      payload.messages === undefined
        ? `assistant output:\n${yield* jsonText(payload.output)}`
        : yield* promptText(payload.messages);
  } else if (payload._tag === "ToolCallSettled") {
    text = `tool result ${payload.toolName} (${payload.toolCallId}; ${payload.isFailure ? "failure" : "success"}):\n${yield* jsonText(payload.result)}`;
  } else {
    // Raw submission input can contain fields excluded by the Agent's input projection.
    // Step outputs, approvals, failure diagnostics, and operational records stay private.
    return undefined;
  }

  return text.length === 0
    ? undefined
    : ContextHistoryEvidence.make({
        recordId: envelope.record.recordId,
        sequence: envelope.sequence,
        runId: payload.runId,
        text,
      });
});

/**
 * Project one decoded canonical record without reading storage or retaining mutable state.
 * Only model transcript text and settled Tool results are evidence. Instructions, reasoning,
 * attachment bytes, raw submissions, Steps, approvals, and diagnostics remain private. Tool
 * truncation envelopes are rendered as retained; discarded bytes cannot be reconstructed.
 *
 * An index must process contiguous records, including empty projections, and commit its rows
 * and watermark atomically. Validate nondecreasing coversThrough across rollover boundaries.
 * Filter both evidence and committing boundary sequences to the operation's captured tail.
 * Authorization and canonical verification of index candidates remain the host's responsibility.
 */
export const project = Effect.fn("ThreadContextHistoryProjection.project")(function* (
  envelope: CanonicalRecordEnvelope,
): Effect.fn.Return<ContextHistoryProjection, ContextHistoryError> {
  const payload = envelope.record.payload;

  if (payload._tag === "CompactionCreated" && payload.kind === "rollover") {
    if (payload.coversThrough >= envelope.sequence) return yield* unavailable();

    return ContextHistoryProjection.make({
      boundary: ContextHistoryBoundary.make({
        sequence: envelope.sequence,
        coversThrough: payload.coversThrough,
        windowId: contextWindowId(payload.runId, payload.turn),
      }),
    });
  }
  const evidence = yield* retainedEvidence(envelope);

  return ContextHistoryProjection.make(evidence === undefined ? {} : { evidence });
});

/** JavaScript case folding for literal substring lookup; do not substitute SQL lower or FTS. */
export const normalizeText = (text: string): string => text.toLowerCase();

/** Validate, trim, and case-fold a search query using the native history contract. */
export const normalizeQuery = Effect.fn("ThreadContextHistoryProjection.normalizeQuery")(function* (
  query: string,
): Effect.fn.Return<string, ContextHistoryError> {
  const decoded = yield* Schema.decodeEffect(ContextHistorySearch.fields.query)(query).pipe(
    Effect.mapError(() =>
      ContextHistoryError.make({
        reason: "invalid-input",
        message: "Invalid context history search",
      }),
    ),
  );

  const normalized = normalizeText(decoded.trim());

  if (normalized.length === 0)
    return yield* ContextHistoryError.make({
      reason: "invalid-input",
      message: "Context history search requires non-whitespace text",
    });

  return normalized;
});

/** Match a normalized query and retain the native 200-character lead and 2,000-character page. */
export const matchText = (text: string, normalizedQuery: string): string | undefined => {
  const index = normalizeText(text).indexOf(normalizedQuery);

  if (index < 0) return undefined;
  const start = Math.max(0, index - 200);

  return text.slice(start, start + 2_000);
};

/**
 * Boundaries must be in canonical order with nondecreasing coversThrough and belong to the
 * same captured Thread tail as the evidence. An index may supply only its last boundary whose
 * coversThrough is strictly less than evidence.sequence. Later Runs inherit that window;
 * evidence before the first rollover keeps its own Run's initial identity.
 */
export const windowIdFor = (
  evidence: ContextHistoryEvidence,
  boundaries: ReadonlyArray<ContextHistoryBoundary>,
): string => {
  let windowId = contextWindowId(evidence.runId, 0);

  for (const boundary of boundaries) {
    if (evidence.sequence <= boundary.coversThrough) break;
    windowId = boundary.windowId;
  }

  return windowId;
};

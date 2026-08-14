import { Context, Crypto, Effect, Encoding, Schema } from "effect";
import type { Prompt } from "effect/unstable/ai";
import { ConversationId } from "@effect-agent/core";

import { ConversationMessage, ConversationSnapshot, ConversationText } from "./conversation.ts";

const MAX_CONTEXT_MESSAGES = 1_024;
const MAX_CONTEXT_MESSAGE_BYTES = 4 * 1024 * 1024;
const MAX_SOURCE_SEQUENCES = 1_024;
const MAX_RETAINED_FACTS = 256;
const MAX_RETAINED_FACT_BYTES = 1024 * 1024;
const MAX_COMPACTIONS = 16;

const SourceSequences = Schema.Array(Schema.Natural).check(
  Schema.isMaxLength(MAX_SOURCE_SEQUENCES),
);

/** A model-context message derived from, but never replacing, source history. */
export class ModelContextMessage extends Schema.Class<ModelContextMessage>(
  "@effect-agent/capabilities/ModelContextMessage",
)({
  role: Schema.Literals(["system", "user", "assistant", "tool"]),
  content: ConversationText,
  sourceSequences: SourceSequences,
}) {}

/** A fact retained explicitly by compaction, independent of the generated prose summary. */
export class RetainedFact extends Schema.Class<RetainedFact>(
  "@effect-agent/capabilities/RetainedFact",
)({
  fact: Schema.String.check(Schema.isMaxLength(4 * 1024)),
  sourceSequences: SourceSequences,
}) {}

const encodedBytes = (value: string): number => Encoding.encodeHex(value).length / 2;

const ModelContextMessages = Schema.Array(ModelContextMessage)
  .check(Schema.isMaxLength(MAX_CONTEXT_MESSAGES))
  .pipe(
    Schema.refine(
      (messages): messages is ReadonlyArray<ModelContextMessage> =>
        messages.reduce((total, message) => total + encodedBytes(message.content), 0) <=
        MAX_CONTEXT_MESSAGE_BYTES,
      { expected: `model context totaling at most ${MAX_CONTEXT_MESSAGE_BYTES} UTF-8 bytes` },
    ),
  );

const RetainedFacts = Schema.Array(RetainedFact)
  .check(Schema.isMaxLength(MAX_RETAINED_FACTS))
  .pipe(
    Schema.refine(
      (facts): facts is ReadonlyArray<RetainedFact> =>
        facts.reduce((total, retained) => total + encodedBytes(retained.fact), 0) <=
        MAX_RETAINED_FACT_BYTES,
      { expected: `retained facts totaling at most ${MAX_RETAINED_FACT_BYTES} UTF-8 bytes` },
    ),
  );

const SourceDigest = Schema.String.check(Schema.isPattern(/^sha256:[a-f0-9]{64}$/));

/** Versioned, replayable artifact that covers a prefix or middle range without deleting source. */
export class CompactionArtifact extends Schema.Class<CompactionArtifact>(
  "@effect-agent/capabilities/CompactionArtifact",
)({
  version: Schema.Literal(1),
  conversationId: ConversationId,
  coversFrom: Schema.Natural,
  coversThrough: Schema.Natural,
  summary: ModelContextMessage,
  retainedFacts: RetainedFacts,
  tokenEstimate: Schema.Natural,
  sourceDigest: SourceDigest,
  compactorVersion: Schema.NonEmptyString,
}) {}

/** Context exposed to a model, with source history kept intact for audit and reconstruction. */
export class PreparedModelContext extends Schema.Class<PreparedModelContext>(
  "@effect-agent/capabilities/PreparedModelContext",
)({
  source: ConversationSnapshot,
  messages: ModelContextMessages,
  compactions: Schema.Array(CompactionArtifact).check(Schema.isMaxLength(MAX_COMPACTIONS)),
}) {}

/** Transform failure is named and bounded so model context preparation has a closed error channel. */
export class ContextTransformError extends Schema.TaggedErrorClass<ContextTransformError>()(
  "ContextTransformError",
  { transformId: Schema.NonEmptyString, message: Schema.String },
) {}

/** Source encoding or hashing failed before an artifact could be trusted. */
export class CompactionDigestError extends Schema.TaggedErrorClass<CompactionDigestError>()(
  "CompactionDigestError",
  { message: Schema.String },
) {}

/** Compaction artifacts that do not bind to the exact source range are rejected before use. */
export class InvalidCompactionArtifact extends Schema.TaggedErrorClass<InvalidCompactionArtifact>()(
  "InvalidCompactionArtifact",
  { message: Schema.String },
) {}

/** A transform or compaction exceeded the finite model-view collection bounds. */
export class ContextLimitExceeded extends Schema.TaggedErrorClass<ContextLimitExceeded>()(
  "ContextLimitExceeded",
  {
    limit: Schema.Literals(["messages", "message-bytes", "compactions"]),
    limitValue: Schema.Natural,
    observedValue: Schema.Natural,
  },
) {}

/**
 * Ordered context transform. It receives and returns only the model-visible
 * message view, making replacement of the authoritative source impossible.
 */
export interface ContextTransform {
  readonly id: string;
  readonly version: string;
  readonly apply: (
    messages: ReadonlyArray<ModelContextMessage>,
  ) => Effect.Effect<ReadonlyArray<ModelContextMessage>, ContextTransformError>;
}

/** Optional authority for obtaining an artifact, typically through a separately metered Model. */
export class ContextCompactor extends Context.Service<
  ContextCompactor,
  {
    readonly compact: (
      snapshot: ConversationSnapshot,
    ) => Effect.Effect<CompactionArtifact, ContextTransformError>;
  }
>()("@effect-agent/capabilities/ContextCompactor") {}

const messageText = (message: Prompt.Message): string => {
  if (message.role === "system") return message.content;
  const content = message.content
    .map((part) => {
      if (part.type === "text" || part.type === "reasoning") return part.text;
      return `[${part.type}]`;
    })
    .join("\n");
  return content.slice(0, 64 * 1024);
};

const asModelMessages = (snapshot: ConversationSnapshot): ReadonlyArray<ModelContextMessage> =>
  snapshot.messages.map((entry) =>
    ModelContextMessage.make({
      role: entry.message.role,
      content: messageText(entry.message),
      sourceSequences: [entry.sequence],
    }),
  );

const validateModelView = (
  messages: ReadonlyArray<ModelContextMessage>,
  transformId: string,
): Effect.Effect<ReadonlyArray<ModelContextMessage>, ContextTransformError> => {
  if (messages.length > MAX_CONTEXT_MESSAGES) {
    return Effect.fail(
      ContextTransformError.make({
        transformId,
        message: `Transform produced ${messages.length} messages; maximum is ${MAX_CONTEXT_MESSAGES}`,
      }),
    );
  }
  const bytes = messages.reduce((total, message) => total + encodedBytes(message.content), 0);
  return bytes > MAX_CONTEXT_MESSAGE_BYTES
    ? Effect.fail(
        ContextTransformError.make({
          transformId,
          message: `Transform produced ${bytes} UTF-8 bytes; maximum is ${MAX_CONTEXT_MESSAGE_BYTES}`,
        }),
      )
    : Effect.succeed(messages);
};

/** Apply transforms in declaration order, always restoring the original source snapshot. */
export const prepareModelContext = (
  snapshot: ConversationSnapshot,
  transforms: ReadonlyArray<ContextTransform> = [],
): Effect.Effect<PreparedModelContext, ContextTransformError> =>
  transforms
    .reduce<Effect.Effect<ReadonlyArray<ModelContextMessage>, ContextTransformError>>(
      (messages, transform) =>
        messages.pipe(
          Effect.flatMap(transform.apply),
          Effect.flatMap((transformed) => validateModelView(transformed, transform.id)),
        ),
      Effect.succeed(asModelMessages(snapshot)),
    )
    .pipe(
      Effect.map((messages) =>
        PreparedModelContext.make({
          source: snapshot,
          messages,
          compactions: [],
        }),
      ),
    );

const exactSourceRange = (
  snapshot: ConversationSnapshot,
  coversFrom: number,
  coversThrough: number,
): Effect.Effect<ReadonlyArray<ConversationMessage>, InvalidCompactionArtifact> => {
  if (coversFrom > coversThrough || coversThrough >= snapshot.nextSequence) {
    return Effect.fail(
      InvalidCompactionArtifact.make({
        message: "Compaction artifact covers an invalid source range",
      }),
    );
  }
  const selected = snapshot.messages.filter(
    (message) => message.sequence >= coversFrom && message.sequence <= coversThrough,
  );
  const expectedCount = coversThrough - coversFrom + 1;
  if (
    selected.length !== expectedCount ||
    selected[0]?.sequence !== coversFrom ||
    selected[selected.length - 1]?.sequence !== coversThrough
  ) {
    return Effect.fail(
      InvalidCompactionArtifact.make({
        message: "Compaction source range is not contiguous in authoritative history",
      }),
    );
  }
  return Effect.succeed(selected);
};

const utf8Bytes = (value: string): Uint8Array => {
  const hex = Encoding.encodeHex(value);
  const bytes = new Uint8Array(hex.length / 2);
  for (let index = 0; index < bytes.length; index += 1) {
    bytes[index] = Number.parseInt(hex.slice(index * 2, index * 2 + 2), 16);
  }
  return bytes;
};

/** Compute a cryptographic digest over the canonical encoded exact source range. */
export const digestCompactionSource = Effect.fn("digestCompactionSource")(function* (
  snapshot: ConversationSnapshot,
  coversFrom: number,
  coversThrough: number,
) {
  const selected = yield* exactSourceRange(snapshot, coversFrom, coversThrough);
  const encoded = yield* Schema.encodeEffect(Schema.Array(ConversationMessage))(selected).pipe(
    Effect.mapError((error) =>
      CompactionDigestError.make({
        message: `Could not encode compaction source: ${error.message}`,
      }),
    ),
  );
  const crypto = yield* Crypto.Crypto;
  const digest = yield* crypto.digest("SHA-256", utf8Bytes(JSON.stringify(encoded))).pipe(
    Effect.mapError((error) =>
      CompactionDigestError.make({
        message: `Could not digest compaction source: ${error.message}`,
      }),
    ),
  );
  return `sha256:${Encoding.encodeHex(digest)}`;
});

/**
 * Replace only the model-view messages fully covered by the artifact range
 * with its summary. The digest is recomputed against the exact source slice
 * before the artifact can influence context. Messages that are not fully
 * covered — including transform-synthesized messages without provenance and
 * messages whose provenance only partially overlaps the range — remain
 * visible, and the summary is inserted before the first message derived
 * entirely from later source.
 */
export const applyCompaction = Effect.fn("applyCompaction")(function* (
  context: PreparedModelContext,
  artifact: CompactionArtifact,
) {
  if (artifact.conversationId !== context.source.conversationId) {
    return yield* InvalidCompactionArtifact.make({
      message: "Compaction artifact belongs to another Conversation",
    });
  }
  const actualDigest = yield* digestCompactionSource(
    context.source,
    artifact.coversFrom,
    artifact.coversThrough,
  );
  if (actualDigest !== artifact.sourceDigest) {
    return yield* InvalidCompactionArtifact.make({
      message: "Compaction source digest does not match authoritative history",
    });
  }
  const provenanceInRange = (sequences: ReadonlyArray<number>): boolean =>
    sequences.length > 0 &&
    sequences.every(
      (sequence) => sequence >= artifact.coversFrom && sequence <= artifact.coversThrough,
    );
  if (
    !provenanceInRange(artifact.summary.sourceSequences) ||
    artifact.retainedFacts.some((fact) => !provenanceInRange(fact.sourceSequences))
  ) {
    return yield* InvalidCompactionArtifact.make({
      message: "Compaction summary or retained-fact provenance falls outside its covered range",
    });
  }
  if (context.compactions.length >= MAX_COMPACTIONS) {
    return yield* ContextLimitExceeded.make({
      limit: "compactions",
      limitValue: MAX_COMPACTIONS,
      observedValue: context.compactions.length + 1,
    });
  }

  const fullyCovered = (message: ModelContextMessage): boolean =>
    message.sourceSequences.length > 0 &&
    message.sourceSequences.every(
      (sequence) => sequence >= artifact.coversFrom && sequence <= artifact.coversThrough,
    );
  const kept = context.messages.filter((message) => !fullyCovered(message));
  const summaryAt = kept.findIndex(
    (message) =>
      message.sourceSequences.length > 0 &&
      message.sourceSequences.every((sequence) => sequence > artifact.coversThrough),
  );
  const messages = yield* validateModelView(
    summaryAt === -1
      ? [...kept, artifact.summary]
      : [...kept.slice(0, summaryAt), artifact.summary, ...kept.slice(summaryAt)],
    `compaction:${artifact.compactorVersion}`,
  );
  return PreparedModelContext.make({
    source: context.source,
    messages,
    compactions: [...context.compactions, artifact],
  });
});

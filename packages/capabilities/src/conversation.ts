import { Clock, Context, DateTime, Effect, Encoding, Layer, Ref, Schema } from "effect";
import { Prompt } from "effect/unstable/ai";
import { ConversationId, RunId } from "@effect-agent/core";

/** Bound applied to one text projection before it can enter a model context. */
export const ConversationText = Schema.String.check(Schema.isMaxLength(64 * 1024));
const MAX_CONVERSATION_MESSAGES = 1_024;
const MAX_CONVERSATION_CONTENT_BYTES = 4 * 1024 * 1024;
const MAX_EPHEMERAL_CONVERSATIONS = 256;
const MAX_EPHEMERAL_CONTENT_BYTES = 64 * 1024 * 1024;

/** One append-only native Effect AI message in an ephemeral conversation. */
export class ConversationMessage extends Schema.Class<ConversationMessage>(
  "@effect-agent/capabilities/ConversationMessage",
)({
  conversationId: ConversationId,
  sequence: Schema.Natural,
  runId: Schema.optionalKey(RunId),
  message: Prompt.Message,
  encodedBytes: Schema.Natural.check(Schema.isLessThanOrEqualTo(MAX_CONVERSATION_CONTENT_BYTES)),
  timestamp: Schema.DateTimeUtcFromString,
}) {}

/** Immutable view suitable as exact official Prompt history for a subsequent Run. */
export class ConversationSnapshot extends Schema.Class<ConversationSnapshot>(
  "@effect-agent/capabilities/ConversationSnapshot",
)({
  version: Schema.Literal(1),
  conversationId: ConversationId,
  nextSequence: Schema.Natural,
  contentBytes: Schema.Natural.check(Schema.isLessThanOrEqualTo(MAX_CONVERSATION_CONTENT_BYTES)),
  messages: Schema.Array(ConversationMessage).check(Schema.isMaxLength(MAX_CONVERSATION_MESSAGES)),
}) {}

/** Portable snapshot of ephemeral state. It is not a durable canonical log. */
export class ConversationExport extends Schema.Class<ConversationExport>(
  "@effect-agent/capabilities/ConversationExport",
)({
  format: Schema.Literal("effect-agent/ephemeral-conversation@1"),
  exportedAt: Schema.DateTimeUtcFromString,
  snapshot: ConversationSnapshot,
}) {}

/** Native message accepted by the append-only conversation. */
export class ConversationAppend extends Schema.Class<ConversationAppend>(
  "@effect-agent/capabilities/ConversationAppend",
)({
  runId: Schema.optionalKey(RunId),
  message: Prompt.Message,
}) {}

/** The requested ephemeral Conversation has not been created in this process Scope. */
export class ConversationNotFound extends Schema.TaggedErrorClass<ConversationNotFound>()(
  "ConversationNotFound",
  { conversationId: ConversationId },
) {}

/** A process-local Conversation store reached an explicit count or encoded-content bound. */
export class ConversationLimitExceeded extends Schema.TaggedErrorClass<ConversationLimitExceeded>()(
  "ConversationLimitExceeded",
  {
    conversationId: ConversationId,
    limit: Schema.Literals(["messages", "content-bytes", "conversations", "store-content-bytes"]),
    limitValue: Schema.Natural,
    observedValue: Schema.Natural,
  },
) {}

/** Engine history was not an append-only extension of the official snapshot. */
export class ConversationHistoryDiverged extends Schema.TaggedErrorClass<ConversationHistoryDiverged>()(
  "ConversationHistoryDiverged",
  { conversationId: ConversationId, message: Schema.String },
) {}

/** Native Prompt encoding failed before a bounded state mutation. */
export class ConversationEncodingError extends Schema.TaggedErrorClass<ConversationEncodingError>()(
  "ConversationEncodingError",
  { conversationId: ConversationId, message: Schema.String },
) {}

export type ConversationError =
  | ConversationNotFound
  | ConversationLimitExceeded
  | ConversationHistoryDiverged
  | ConversationEncodingError;

/** Reconstruct exact Effect AI Prompt history, including tools, reasoning, files, and options. */
export const conversationPrompt = (snapshot: ConversationSnapshot): Prompt.Prompt =>
  Prompt.fromMessages(snapshot.messages.map((entry) => entry.message));

/** Process-local multi-Run conversation state. Scope closure releases all in-memory state. */
export class EphemeralConversations extends Context.Service<
  EphemeralConversations,
  {
    readonly create: (
      conversationId: ConversationId,
    ) => Effect.Effect<ConversationSnapshot, ConversationLimitExceeded>;
    readonly append: (
      conversationId: ConversationId,
      message: ConversationAppend,
    ) => Effect.Effect<
      ConversationSnapshot,
      ConversationNotFound | ConversationLimitExceeded | ConversationEncodingError
    >;
    /**
     * Record an engine-emitted full history only when it is an append-only
     * extension of the exact official Prompt already stored.
     */
    readonly recordHistory: (
      conversationId: ConversationId,
      runId: RunId,
      history: Prompt.Prompt,
    ) => Effect.Effect<ConversationSnapshot, ConversationError>;
    readonly snapshot: (
      conversationId: ConversationId,
    ) => Effect.Effect<ConversationSnapshot, ConversationNotFound>;
    readonly export: (
      conversationId: ConversationId,
    ) => Effect.Effect<ConversationExport, ConversationNotFound>;
  }
>()("@effect-agent/capabilities/EphemeralConversations") {}

type AppendResult =
  | { readonly _tag: "not-found" }
  | {
      readonly _tag: "failure";
      readonly error: ConversationNotFound | ConversationLimitExceeded | ConversationEncodingError;
    }
  | { readonly _tag: "success"; readonly value: ConversationSnapshot };
type CreateResult =
  | { readonly _tag: "failure"; readonly error: ConversationLimitExceeded }
  | { readonly _tag: "success"; readonly value: ConversationSnapshot };

const utf8Bytes = (value: string): number => Encoding.encodeHex(value).length / 2;

const encodeMessage = (
  conversationId: ConversationId,
  message: Prompt.Message,
): Effect.Effect<string, ConversationEncodingError> =>
  Schema.encodeEffect(Prompt.Message)(message).pipe(
    Effect.map((encoded) => JSON.stringify(encoded)),
    Effect.mapError((error) =>
      ConversationEncodingError.make({
        conversationId,
        message: `Could not encode native Effect AI message: ${error.message}`,
      }),
    ),
  );

const findSnapshot = (
  state: ReadonlyMap<ConversationId, ConversationSnapshot>,
  conversationId: ConversationId,
): Effect.Effect<ConversationSnapshot, ConversationNotFound> => {
  const snapshot = state.get(conversationId);
  return snapshot === undefined
    ? Effect.fail(ConversationNotFound.make({ conversationId }))
    : Effect.succeed(snapshot);
};

const totalStoreBytes = (
  conversations: ReadonlyMap<ConversationId, ConversationSnapshot>,
): number => {
  let total = 0;
  for (const snapshot of conversations.values()) total += snapshot.contentBytes;
  return total;
};

const appendEncoded = (
  conversations: ReadonlyMap<ConversationId, ConversationSnapshot>,
  conversationId: ConversationId,
  append: ConversationAppend,
  encoded: string,
  timestamp: DateTime.Utc,
): readonly [AppendResult, ReadonlyMap<ConversationId, ConversationSnapshot>] => {
  const current = conversations.get(conversationId);
  if (current === undefined) return [{ _tag: "not-found" }, conversations];
  if (current.messages.length >= MAX_CONVERSATION_MESSAGES) {
    return [
      {
        _tag: "failure",
        error: ConversationLimitExceeded.make({
          conversationId,
          limit: "messages",
          limitValue: MAX_CONVERSATION_MESSAGES,
          observedValue: current.messages.length + 1,
        }),
      },
      conversations,
    ];
  }
  const messageBytes = utf8Bytes(encoded);
  const contentBytes = current.contentBytes + messageBytes;
  if (contentBytes > MAX_CONVERSATION_CONTENT_BYTES) {
    return [
      {
        _tag: "failure",
        error: ConversationLimitExceeded.make({
          conversationId,
          limit: "content-bytes",
          limitValue: MAX_CONVERSATION_CONTENT_BYTES,
          observedValue: contentBytes,
        }),
      },
      conversations,
    ];
  }
  const storeBytes = totalStoreBytes(conversations) + messageBytes;
  if (storeBytes > MAX_EPHEMERAL_CONTENT_BYTES) {
    return [
      {
        _tag: "failure",
        error: ConversationLimitExceeded.make({
          conversationId,
          limit: "store-content-bytes",
          limitValue: MAX_EPHEMERAL_CONTENT_BYTES,
          observedValue: storeBytes,
        }),
      },
      conversations,
    ];
  }
  const message = ConversationMessage.make({
    conversationId,
    sequence: current.nextSequence,
    ...(append.runId === undefined ? {} : { runId: append.runId }),
    message: append.message,
    encodedBytes: messageBytes,
    timestamp,
  });
  const next = ConversationSnapshot.make({
    version: current.version,
    conversationId: current.conversationId,
    nextSequence: current.nextSequence + 1,
    contentBytes,
    messages: [...current.messages, message],
  });
  return [{ _tag: "success", value: next }, new Map(conversations).set(conversationId, next)];
};

/** Layer whose state is scoped to the consumer; it intentionally has no persistence semantics. */
export const EphemeralConversationsLive = Layer.effect(
  EphemeralConversations,
  Effect.gen(function* () {
    const state = yield* Ref.make<ReadonlyMap<ConversationId, ConversationSnapshot>>(new Map());

    const append = (
      conversationId: ConversationId,
      message: ConversationAppend,
    ): Effect.Effect<
      ConversationSnapshot,
      ConversationNotFound | ConversationLimitExceeded | ConversationEncodingError
    > =>
      Effect.gen(function* () {
        const encoded = yield* encodeMessage(conversationId, message.message);
        const timestamp = DateTime.toUtc(DateTime.makeUnsafe(yield* Clock.currentTimeMillis));
        const result = yield* Ref.modify(state, (conversations) =>
          appendEncoded(conversations, conversationId, message, encoded, timestamp),
        );
        if (result._tag === "not-found") {
          return yield* ConversationNotFound.make({ conversationId });
        }
        if (result._tag === "failure") return yield* result.error;
        return result.value;
      });

    return EphemeralConversations.of({
      create: (conversationId) =>
        Effect.gen(function* () {
          const result = yield* Ref.modify(
            state,
            (
              conversations,
            ): readonly [CreateResult, ReadonlyMap<ConversationId, ConversationSnapshot>] => {
              const existing = conversations.get(conversationId);
              if (existing !== undefined) {
                return [{ _tag: "success" as const, value: existing }, conversations] as const;
              }
              if (conversations.size >= MAX_EPHEMERAL_CONVERSATIONS) {
                return [
                  {
                    _tag: "failure" as const,
                    error: ConversationLimitExceeded.make({
                      conversationId,
                      limit: "conversations",
                      limitValue: MAX_EPHEMERAL_CONVERSATIONS,
                      observedValue: conversations.size + 1,
                    }),
                  },
                  conversations,
                ] as const;
              }
              const created = ConversationSnapshot.make({
                version: 1,
                conversationId,
                nextSequence: 0,
                contentBytes: 0,
                messages: [],
              });
              return [
                { _tag: "success" as const, value: created },
                new Map(conversations).set(conversationId, created),
              ] as const;
            },
          );
          if (result._tag === "failure") return yield* result.error;
          return result.value;
        }),
      append,
      recordHistory: (conversationId, historyRunId, history) =>
        Effect.gen(function* () {
          const current = yield* Ref.get(state).pipe(
            Effect.flatMap((all) => findSnapshot(all, conversationId)),
          );
          const currentEncoded = yield* Effect.forEach(current.messages, (entry) =>
            encodeMessage(conversationId, entry.message),
          );
          const incomingEncoded = yield* Effect.forEach(history.content, (message) =>
            encodeMessage(conversationId, message),
          );
          if (
            incomingEncoded.length < currentEncoded.length ||
            currentEncoded.some((encoded, index) => encoded !== incomingEncoded[index])
          ) {
            return yield* ConversationHistoryDiverged.make({
              conversationId,
              message: "Engine history is not an append-only extension of official history",
            });
          }
          let next = current;
          for (const message of history.content.slice(currentEncoded.length)) {
            next = yield* append(
              conversationId,
              ConversationAppend.make({
                runId: historyRunId,
                message,
              }),
            );
          }
          return next;
        }),
      snapshot: (conversationId) =>
        Ref.get(state).pipe(Effect.flatMap((all) => findSnapshot(all, conversationId))),
      export: (conversationId) =>
        Effect.gen(function* () {
          const snapshot = yield* findSnapshot(yield* Ref.get(state), conversationId);
          return ConversationExport.make({
            format: "effect-agent/ephemeral-conversation@1",
            exportedAt: DateTime.toUtc(DateTime.makeUnsafe(yield* Clock.currentTimeMillis)),
            snapshot,
          });
        }),
    });
  }),
);

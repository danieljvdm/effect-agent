import {
  ConversationId,
  IdGenerator,
  type InputPromptSource,
  type RunDispositionDeclaration,
} from "@effect-agent/core";
import { AgentRuntime, type RunOptions, type RuntimeBinding } from "@effect-agent/engine";
import { DateTime, Effect, Schema } from "effect";
import { Prompt, type Tool } from "effect/unstable/ai";

import {
  BatchId,
  CanonicalBatch,
  DeploymentId,
  ModelCompleted,
  PersistedJson,
  ProducerEpoch,
  ProducerId,
  RecordEnvelope,
  RecordId,
  RunCompleted,
  UserInputRecorded,
} from "./records.ts";
import { promptFromCanonicalRecords } from "./run-journal.ts";
import {
  ConversationExportRequest,
  ConversationMaterialization,
  ConversationStore,
  FencedAppendRequest,
} from "./store.ts";

/** Retained history cannot be encoded, exceeds its bounds, or belongs to durable accepted work. */
export class PersistentConversationError extends Schema.TaggedError<PersistentConversationError>()(
  "PersistentConversationError",
  {
    conversationId: ConversationId,
    message: Schema.String,
    cause: Schema.optionalKey(Schema.Defect()),
  },
) {}

/** History owns identity and persistence; these hooks only customize ephemeral execution. */
export interface PersistentConversationOptions<Error = never, Requirements = never> extends Pick<
  RunOptions<Error, Requirements>,
  | "approval"
  | "context"
  | "budget"
  | "toolAuthorization"
  | "estimateCostMicrousd"
  | "scheduling"
  | "bufferLimits"
  | "toolCallAllowance"
  | "turnAllowance"
> {
  readonly conversationId: ConversationId;
}

const HISTORY_EPOCH = Schema.decodeSync(ProducerEpoch)(0);
const HISTORY_DEPLOYMENT = Schema.decodeSync(DeploymentId)("persistent-history");
const batchId = Schema.decodeSync(BatchId);
const producer = Schema.decodeSync(ProducerId);
const recordId = Schema.decodeSync(RecordId);

type InstructionResult<Instructions, Input> = Instructions extends (input: Input) => infer Result
  ? Result
  : Instructions;
type InstructionError<Instructions, Input> =
  InstructionResult<Instructions, Input> extends Effect.Effect<infer _A, infer E, infer _R>
    ? E
    : never;
type InstructionRequirements<Instructions, Input> =
  InstructionResult<Instructions, Input> extends Effect.Effect<infer _A, infer _E, infer R>
    ? R
    : never;

/** Reconstruct the next Run's Prompt from canonical records, without acquiring append ownership. */
const load = Effect.fn("PersistentConversations.load")(function* (conversationId: ConversationId) {
  const store = yield* ConversationStore;
  const exported = yield* store.export(ConversationExportRequest.make({ conversationId }));
  return yield* promptFromCanonicalRecords(exported.records);
});

/**
 * Run immediately and retain the entire successful Run in one canonical batch. A failure,
 * defect, timeout, or interruption before append retains none of this Run. Nothing admits a
 * Submission, resumes an interrupted Run, or retries model/Tool execution.
 *
 * History producers share epoch zero and use the loaded tail as a compare-and-append token.
 * Concurrent Runs may both execute external effects; only one may append against that tail.
 * The loser fails with AppendConflict. A durable ownership claim fences this producer with
 * FenceRejected. Keep history and durable admission in separate Conversations.
 *
 * A store failure after append may mean the entire batch committed. Inspect retained history
 * before retrying: external effects are never exactly-once. Adapter materialize/append failpoints
 * cover both sides of the only persistent mutations. Provider resources close before append.
 */
const run = Effect.fn("PersistentConversations.run")(function* <
  InputSchema extends Schema.Top,
  OutputSchema extends Schema.Top,
  Instructions,
  Tools extends Record<string, Tool.Any>,
  Provider,
  ModelProvides,
  ModelRequires,
  HookError = never,
  HookRequirements = never,
  IError = InstructionError<Instructions, InputSchema["Type"]>,
  IRequirements = InstructionRequirements<Instructions, InputSchema["Type"]>,
  Disposition extends RunDispositionDeclaration<OutputSchema["Type"], Schema.Top> | undefined =
    undefined,
  InputPrompt extends InputPromptSource<InputSchema["Type"], unknown, unknown> | undefined =
    undefined,
>(
  agent: RuntimeBinding<
    InputSchema,
    OutputSchema,
    Instructions,
    Tools,
    Provider,
    ModelProvides,
    ModelRequires,
    IError,
    IRequirements,
    Disposition,
    InputPrompt
  >,
  input: unknown,
  options: PersistentConversationOptions<HookError, HookRequirements>,
) {
  const { conversationId } = options;
  const store = yield* ConversationStore;
  const ids = yield* IdGenerator;
  const runId = yield* ids.nextRunId;
  const producerId = producer(`history:${runId}`);
  const error = (message: string, cause?: unknown) =>
    PersistentConversationError.make({
      conversationId,
      message,
      ...(cause === undefined ? {} : { cause }),
    });
  const persisted = (value: unknown) =>
    Schema.decodeUnknownEffect(PersistedJson)(value).pipe(
      Effect.mapError((cause) => error("Run data exceeds canonical persistence bounds", cause)),
    );

  // No epoch is advanced here. A claimed durable Conversation rejects this materialization.
  yield* store.materialize(
    ConversationMaterialization.make({ conversationId, producerEpoch: HISTORY_EPOCH }),
  );
  const base = yield* store.export(ConversationExportRequest.make({ conversationId }));
  if (
    base.records.some(
      ({ record }) =>
        (record.payload._tag === "UserInputRecorded" &&
          record.payload.submissionId !== undefined) ||
        record.payload._tag === "SubmissionSettled" ||
        record.payload._tag === "AbortRequested",
    )
  ) {
    return yield* error(
      "This Conversation belongs to durable accepted work; use a separate history Conversation",
    );
  }
  const history = yield* promptFromCanonicalRecords(base.records);
  const createdAt = yield* DateTime.now;
  const record = (id: string, payload: RecordEnvelope["payload"], timestamp = createdAt) =>
    RecordEnvelope.make({
      recordId: recordId(id),
      family: "conversation",
      schemaVersion: 1,
      deploymentId: HISTORY_DEPLOYMENT,
      createdAt: timestamp,
      payload,
    });
  const staged: {
    input: RecordEnvelope | undefined;
    messages: PersistedJson | undefined;
  } = { input: undefined, messages: undefined };

  const runOptions: RunOptions<HookError | PersistentConversationError, HookRequirements> = {
    ...options,
    runId,
    history,
    onInput: Effect.fn("PersistentConversations.stageInput")(function* (encodedInput: unknown) {
      staged.input = record(
        `history-input:${runId}`,
        UserInputRecorded.make({
          kind: "user",
          runId,
          input: yield* persisted(encodedInput),
        }),
      );
    }),
    onHistory: Effect.fn("PersistentConversations.stageHistory")(function* (next: Prompt.Prompt) {
      // Encode while Run resources are alive. Input may itself contain native Tool exchanges;
      // retain their order without interpreting them as executed durable Turns.
      staged.messages = yield* Schema.encodeEffect(Prompt.Prompt)(
        Prompt.fromMessages(next.content.slice(history.content.length)),
      ).pipe(
        Effect.mapError((cause) => error("Run history could not be encoded", cause)),
        Effect.flatMap(persisted),
      );
    }),
  };
  const { result, completion } = yield* Effect.scoped(
    Effect.gen(function* () {
      const active = yield* AgentRuntime.start(agent, input, runOptions);
      const result = yield* active.await;
      const completion = (yield* active.events).find((event) => event._tag === "RunCompleted");
      if (completion === undefined) return yield* error("Run completed without its encoded result");
      return { result, completion };
    }),
  );
  if (staged.input === undefined || staged.messages === undefined) {
    return yield* error("Run completed without its encoded input and history");
  }
  const output = yield* persisted(completion.output);
  const runDisposition =
    completion.runDisposition === undefined
      ? undefined
      : yield* persisted(completion.runDisposition);
  const completedAt = yield* DateTime.now;
  const modelCompleted = yield* ModelCompleted.makeEffect({
    runId,
    output,
    messages: staged.messages,
  }).pipe(Effect.mapError((cause) => error("Run history is not a canonical Prompt", cause)));
  const batch = CanonicalBatch.make({
    batchId: batchId(`history:${runId}`),
    producerId,
    records: [
      staged.input,
      record(`history-output:${runId}`, modelCompleted, completedAt),
      record(
        `run-completed:${runId}`,
        RunCompleted.make({
          runId,
          output,
          ...(runDisposition === undefined ? {} : { runDisposition }),
          ...(result.finishReason === "budget-exhausted"
            ? { finishReason: result.finishReason, exhausted: result.exhausted }
            : {}),
        }),
        completedAt,
      ),
    ],
  });
  yield* store.append(
    FencedAppendRequest.make({
      conversationId,
      batch,
      producerEpoch: HISTORY_EPOCH,
      expectedTailSequence: base.tailSequence,
      expectedTailDigest: base.tailDigest,
    }),
  );
  return result;
});

/** Immediate execution with retained canonical history and no accepted-work recovery. */
export const PersistentConversations = { load, run };

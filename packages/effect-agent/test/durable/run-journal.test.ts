import { NodeCrypto } from "@effect/platform-node";
import { describe, expect, layer } from "@effect/vitest";
import { DateTime, Effect, Layer, Ref, Schema, Stream } from "effect";
import * as Agent from "effect-agent/agent";
import * as AgentRuntime from "effect-agent/agent-runtime";
import { contextWindowId, contextWindowMessage } from "effect-agent/compaction";
import { digestCanonicalBatch, EMPTY_TAIL_DIGEST } from "effect-agent/digest";
import { ThreadId, SubmissionId, ToolCallId } from "effect-agent/identifiers";
import {
  BatchId,
  CanonicalBatch,
  CanonicalRecordEnvelope,
  CanonicalSequence,
  DeploymentId,
  Digest,
  ModelResponseRecorded,
  ObservationOffset,
  ProducerId,
  RecordEnvelope,
  ToolCallPrepared,
  ToolCallUnknown,
  ToolOperation,
} from "effect-agent/records";
import {
  promptFromCanonicalRecords,
  projectRunJournal,
  projectRunJournalStream,
  runIdForSubmission,
  toolCallPreparedRecordId,
  toolCallUnknownRecordId,
  turnCanonicalBatch,
  turnIdForRun,
  turnResponseBatch,
  turnResultsBatch,
} from "effect-agent/run-journal";
import { ThreadHistory } from "effect-agent/thread-history";
import { Selection, Snapshot } from "effect-agent/tool-exposure";
import { summarizeModelUsage } from "effect-agent/usage";
import { LanguageModel, Model, Prompt, Tool, Toolkit, type Response } from "effect/unstable/ai";

import { JournalCheckpointSeed } from "../../src/durable/internal/journal-checkpoint.ts";
import { makeJournalMetadata } from "../../src/durable/internal/journal-metadata.ts";

const SUBMISSION_ID = Schema.decodeSync(SubmissionId)("submission-journal");
const RUN_ID = runIdForSubmission(SUBMISSION_ID);
const LATER_RUN_ID = runIdForSubmission(Schema.decodeSync(SubmissionId)("submission-later"));
const RUN_NONE_ID = runIdForSubmission(Schema.decodeSync(SubmissionId)("none"));
const CALL_ONE = Schema.decodeSync(ToolCallId)("call-1");
const CALL_TWO = Schema.decodeSync(ToolCallId)("call-2");
const PRODUCER_ID = Schema.decodeSync(ProducerId)("producer-journal");
const DEPLOYMENT_ID = Schema.decodeSync(DeploymentId)("deployment-journal");
const CREATED_AT = DateTime.toUtc(DateTime.makeUnsafe(1_000));
const THREAD_ID = Schema.decodeSync(ThreadId)("thread-journal");

/** One tool-declaring Turn: instructions + input + assistant declaration + two tool results. */
const toolTurnAppended: ReadonlyArray<Prompt.Message> = [
  Prompt.makeMessage("system", { content: "Answer as JSON." }),
  Prompt.makeMessage("user", {
    content: [Prompt.makePart("text", { text: '{"question":"book?"}' })],
  }),
  Prompt.makeMessage("assistant", {
    content: [
      Prompt.makePart("tool-call", {
        id: "call-1",
        name: "book_flight",
        params: { destination: "Kyoto" },
        providerExecuted: false,
      }),
      Prompt.makePart("tool-call", {
        id: "call-2",
        name: "book_lodging",
        params: { nights: 3 },
        providerExecuted: false,
      }),
    ],
  }),
  Prompt.makeMessage("tool", {
    content: [
      Prompt.makePart("tool-result", {
        id: "call-1",
        name: "book_flight",
        result: { bookingRef: "flight-42" },
        isFailure: false,
        providerExecuted: false,
      }),
      Prompt.makePart("tool-result", {
        id: "call-2",
        name: "book_lodging",
        result: { bookingRef: "lodging-7" },
        isFailure: false,
        providerExecuted: false,
      }),
    ],
  }),
];

const completionTurnAppended: ReadonlyArray<Prompt.Message> = [
  Prompt.makeMessage("assistant", {
    content: [
      Prompt.makePart("tool-call", {
        id: "call-1",
        name: "post_message",
        params: { message: "Your flight is booked." },
        providerExecuted: false,
      }),
    ],
  }),
  Prompt.makeMessage("tool", {
    content: [
      Prompt.makePart("tool-result", {
        id: "call-1",
        name: "post_message",
        result: { messageId: "message-42" },
        isFailure: false,
        providerExecuted: false,
      }),
    ],
  }),
];

const finalTurnAppended: ReadonlyArray<Prompt.Message> = [
  Prompt.makeMessage("assistant", {
    content: [Prompt.makePart("text", { text: '{"answer":"Booked."}' })],
  }),
];

const turnInput = (
  appended: ReadonlyArray<Prompt.Message>,
  turn = 1,
  runId = RUN_ID,
  usage?: { readonly inputTokens: number; readonly outputTokens: number },
) => ({
  runId,
  turn,
  turnId: turnIdForRun(runId, turn),
  appended,
  producerId: PRODUCER_ID,
  deploymentId: DEPLOYMENT_ID,
  createdAt: CREATED_AT,
  ...(usage === undefined ? {} : { usage }),
});

const envelopeAt = (sequence: number, record: RecordEnvelope): CanonicalRecordEnvelope =>
  CanonicalRecordEnvelope.make({
    threadId: THREAD_ID,
    batchId: Schema.decodeSync(BatchId)(`batch-journal-${sequence}`),
    sequence: Schema.decodeSync(CanonicalSequence)(sequence),
    offset: Schema.decodeSync(ObservationOffset)(`memory:${sequence}`),
    record,
  });

const envelopesOf = (batches: ReadonlyArray<CanonicalBatch>): Array<CanonicalRecordEnvelope> => {
  const envelopes: Array<CanonicalRecordEnvelope> = [];

  for (const batch of batches) {
    for (const record of batch.records) {
      envelopes.push(envelopeAt(envelopes.length + 1, record));
    }
  }

  return envelopes;
};

const textOfPrompt = (prompt: Prompt.Prompt): string =>
  prompt.content
    .map((message) =>
      typeof message.content === "string"
        ? message.content
        : message.content.map((part) => (part.type === "text" ? part.text : "")).join(""),
    )
    .join("\n");

const auditRecord = (
  recordId: string,
  payload: (typeof RecordEnvelope.Encoded)["payload"],
): RecordEnvelope =>
  Schema.decodeSync(RecordEnvelope)({
    recordId,
    family: "thread",
    schemaVersion: 1,
    createdAt: "2026-08-12T12:00:00.000Z",
    deploymentId: "deployment-journal",
    payload,
  });

const toolResults = (prompt: Prompt.Prompt): ReadonlyArray<unknown> =>
  prompt.content.flatMap((message) =>
    typeof message.content === "string"
      ? []
      : message.content.flatMap((part) => (part.type === "tool-result" ? [part.result] : [])),
  );

describe("run journal batch split (plan §2.1)", () => {
  layer(NodeCrypto.layer)((it) => {
    it.effect(
      "journals native provider results after the engine freezes snapshot array prototypes",
      () =>
        Effect.gen(function* () {
          const action = {
            type: "search",
            queries: ["Tahoe private hot tub"],
            sources: [{ type: "url", url: "https://www.tahoegetaways.com/" }],
          };

          const actionSchema = Schema.Struct({
            type: Schema.Literal("search"),
            queries: Schema.Array(Schema.String),
            sources: Schema.Array(
              Schema.Struct({ type: Schema.Literal("url"), url: Schema.String }),
            ),
          });

          const search = Tool.providerDefined({
            id: "test.web_search",
            customName: "HostedSearch",
            providerName: "web_search",
            parameters: Schema.Struct({ action: actionSchema }),
            success: Schema.Struct({ action: actionSchema, status: Schema.String }),
          })(undefined);

          const definition = Agent.make("journal-provider-search", {
            input: Schema.String,
            output: Schema.String,
            instructions: "Search once and answer as JSON.",
            toolkit: Toolkit.make(search),
            policy: { maxTurns: 1, maxToolCalls: 1, maxDuration: "30 seconds", toolConcurrency: 1 },
          });

          const parts: ReadonlyArray<Response.StreamPartEncoded> = [
            {
              type: "tool-call",
              id: "search-1",
              name: "HostedSearch",
              params: { action },
              providerExecuted: true,
            },
            {
              type: "tool-result",
              id: "search-1",
              name: "HostedSearch",
              result: { action, status: "completed" },
              providerExecuted: true,
              isFailure: false,
            },
            { type: "text-start", id: "answer" },
            { type: "text-delta", id: "answer", delta: '"Found a listing."' },
            { type: "text-end", id: "answer" },
            { type: "finish", reason: "stop", usage: { inputTokens: {}, outputTokens: {} } },
          ];

          const model = Model.make(
            "scripted",
            "journal-search",
            Layer.effect(
              LanguageModel.LanguageModel,
              LanguageModel.make({
                generateText: () => Effect.succeed([]),
                streamText: () => Stream.fromIterable(parts),
              }),
            ),
          );

          const retained = yield* Ref.make(Prompt.empty);

          yield* AgentRuntime.run(Agent.withModel(definition, model), "Find a listing", {
            onHistory: (history) => Ref.set(retained, history),
          }).pipe(Effect.provide([ThreadHistory.layer]));
          const history = yield* Ref.get(retained);

          const providerResult = history.content.flatMap((message) =>
            message.role === "assistant"
              ? message.content.filter((part) => part.type === "tool-result")
              : [],
          )[0];

          if (providerResult === undefined)
            return yield* Effect.die("Expected the staged provider result");

          const frozen = Schema.decodeUnknownSync(
            Schema.Struct({
              action: Schema.Struct({ queries: Schema.Unknown, sources: Schema.Unknown }),
            }),
          )(providerResult.result);

          for (const array of [frozen.action.queries, frozen.action.sources]) {
            expect(Array.isArray(array)).toBe(true);
            expect(Object.getPrototypeOf(array)).toBeNull();
            expect(Object.isFrozen(array)).toBe(true);
          }
          const batch = yield* turnResponseBatch(turnInput(history.content));

          const plainBatch = yield* Schema.decodeEffect(Schema.fromJsonString(CanonicalBatch))(
            JSON.stringify(Schema.encodeSync(CanonicalBatch)(batch)),
          );

          expect(yield* digestCanonicalBatch(EMPTY_TAIL_DIGEST, batch)).toBe(
            yield* digestCanonicalBatch(EMPTY_TAIL_DIGEST, plainBatch),
          );
          const projected = yield* projectRunJournal(envelopesOf([batch]), RUN_ID);
          const encoded = Schema.encodeSync(Prompt.Prompt)(projected.prompt);

          expect(JSON.stringify(encoded)).toContain("https://www.tahoegetaways.com/");
          expect(JSON.stringify(encoded)).toContain("Tahoe private hot tub");
          const replayed = yield* Ref.make(false);

          const replayModel = Model.make(
            "scripted",
            "journal-search-replay",
            Layer.effect(
              LanguageModel.LanguageModel,
              LanguageModel.make({
                generateText: () => Effect.succeed([]),
                streamText: (request) =>
                  Stream.unwrap(
                    Effect.gen(function* () {
                      const result = request.prompt.content.flatMap((message) =>
                        message.role === "assistant"
                          ? message.content.filter((part) => part.type === "tool-result")
                          : [],
                      )[0];

                      expect(result).toMatchObject({
                        providerExecuted: true,
                        result: { action, status: "completed" },
                      });
                      yield* Ref.set(replayed, true);

                      return Stream.fromIterable(
                        parts.filter(
                          (part) => part.type !== "tool-call" && part.type !== "tool-result",
                        ),
                      );
                    }),
                  ),
              }),
            ),
          );

          yield* AgentRuntime.run(
            Agent.withModel(definition, replayModel),
            "Use the previous search",
            {
              history: projected.prompt,
            },
          ).pipe(Effect.provide([ThreadHistory.layer]));
          expect(yield* Ref.get(replayed)).toBe(true);
        }),
    );

    // Authorization failed before preparation, but later history claimed the action may have run.
    it.effect(
      "distinguishes an undispatched historical call from a prepared unknown operation",
      () =>
        Effect.gen(function* () {
          const response = yield* turnResponseBatch(turnInput(toolTurnAppended));
          const record = response.records[0]!;

          if (record.payload._tag !== "ModelResponseRecorded")
            return yield* Effect.die("Expected a model response");

          const operations = [CALL_ONE, CALL_TWO].map((toolCallId, index) =>
            ToolOperation.make({
              toolCallId,
              toolName: index === 0 ? "book_flight" : "book_lodging",
              executionClass: "uncertain",
              executionKind: "ordinary",
              replay: Digest.make("f".repeat(64)),
            }),
          );

          const records = [
            envelopeAt(
              1,
              RecordEnvelope.make({
                ...record,
                payload: ModelResponseRecorded.make({
                  ...record.payload,
                  toolOperations: operations,
                }),
              }),
            ),
            envelopeAt(
              2,
              RecordEnvelope.make({
                ...record,
                recordId: toolCallPreparedRecordId(RUN_ID, 1, CALL_TWO),
                payload: ToolCallPrepared.make({
                  ...operations[1]!,
                  runId: RUN_ID,
                  turnId: turnIdForRun(RUN_ID, 1),
                  turn: 1,
                  parameters: { nights: 3 },
                  parametersDigest: Digest.make("e".repeat(64)),
                }),
              }),
            ),
          ];

          const later = yield* projectRunJournal(records, LATER_RUN_ID);

          expect(toolResults(later.prompt)).toEqual([
            expect.objectContaining({ _tag: "ToolUnavailable", execution: "not-executed" }),
            expect.objectContaining({ _tag: "ToolOutcomeUnknown" }),
          ]);
          const recovering = yield* projectRunJournal(records, RUN_ID);

          expect(toolResults(recovering.prompt)).toEqual([]);
          expect(records.some(({ record }) => record.payload._tag === "ToolCallSettled")).toBe(
            false,
          );

          const unknown = envelopeAt(
            3,
            RecordEnvelope.make({
              ...record,
              recordId: toolCallUnknownRecordId(RUN_ID, 1, CALL_ONE),
              payload: ToolCallUnknown.make({
                runId: RUN_ID,
                turn: 1,
                toolCallId: CALL_ONE,
                toolName: "book_flight",
                reason: "The external outcome was not recorded",
              }),
            }),
          );

          const uncertain = yield* projectRunJournal([...records, unknown], LATER_RUN_ID);

          expect(toolResults(uncertain.prompt)[0]).toMatchObject({ _tag: "ToolOutcomeUnknown" });

          const readonly = yield* projectRunJournal(
            [
              envelopeAt(
                1,
                RecordEnvelope.make({
                  ...record,
                  payload: ModelResponseRecorded.make({
                    ...record.payload,
                    toolOperations: operations.map((operation) =>
                      ToolOperation.make({
                        ...operation,
                        executionClass: "readonly",
                      }),
                    ),
                  }),
                }),
              ),
            ],
            LATER_RUN_ID,
          );

          expect(toolResults(readonly.prompt)).toEqual([
            expect.objectContaining({ _tag: "ToolOutcomeUnknown" }),
            expect.objectContaining({ _tag: "ToolOutcomeUnknown" }),
          ]);
        }),
    );

    it.effect("replaces unknown history beside its original call after another Run completes", () =>
      Effect.gen(function* () {
        const original = yield* turnCanonicalBatch(turnInput(toolTurnAppended));

        const later = yield* turnCanonicalBatch(
          turnInput(
            [
              Prompt.makeMessage("user", {
                content: [Prompt.makePart("text", { text: "Answer while I wait." })],
              }),
              ...completionTurnAppended,
            ],
            1,
            LATER_RUN_ID,
          ),
        );

        const prefix = [original.records[0]!, original.records[1]!, ...later.records].map(
          (record, index) => envelopeAt(index + 1, record),
        );

        const pending = yield* projectRunJournal(prefix, RUN_NONE_ID);

        expect(toolResults(pending.prompt)).toEqual([
          { bookingRef: "flight-42" },
          expect.objectContaining({ _tag: "ToolOutcomeUnknown" }),
          { messageId: "message-42" },
        ]);

        const resolved = yield* projectRunJournal(
          [...prefix, envelopeAt(prefix.length + 1, original.records[2]!)],
          RUN_NONE_ID,
        );

        expect(resolved.prompt.content.map((message) => message.role)).toEqual([
          "user",
          "assistant",
          "tool",
          "user",
          "assistant",
          "tool",
        ]);
        expect(
          resolved.prompt.content
            .filter((message) => message.role === "tool")
            .map((message) =>
              message.content
                .filter((part) => part.type === "tool-result")
                .map((part) => [part.id, part.result]),
            ),
        ).toEqual([
          [
            ["call-1", { bookingRef: "flight-42" }],
            ["call-2", { bookingRef: "lodging-7" }],
          ],
          [["call-1", { messageId: "message-42" }]],
        ]);
        expect(toolResults(pending.prompt)[1]).toMatchObject({ _tag: "ToolOutcomeUnknown" });

        const resumedLater = yield* projectRunJournal(
          [...prefix, envelopeAt(prefix.length + 1, original.records[2]!)],
          LATER_RUN_ID,
        );

        expect(toolResults(resumedLater.prompt)).toEqual(toolResults(resolved.prompt));
        expect(
          prefix.filter(({ record }) => record.payload._tag === "ToolCallSettled"),
        ).toHaveLength(2);
      }),
    );

    it.effect("does not reserve the real run:none identity for canonical prompt projection", () =>
      Effect.gen(function* () {
        expect(RUN_NONE_ID).toBe("run:none");
        const response = yield* turnResponseBatch(turnInput(toolTurnAppended, 1, RUN_NONE_ID));
        const records = envelopesOf([response]);

        const recovering = yield* projectRunJournal(records, RUN_NONE_ID);

        expect(recovering.prompt.content.map((message) => message.role)).toEqual([
          "system",
          "user",
          "assistant",
        ]);

        const canonicalPrompt = yield* promptFromCanonicalRecords(records);

        expect(canonicalPrompt.content.map((message) => message.role)).toEqual([
          "user",
          "assistant",
          "tool",
        ]);
      }),
    );

    it.effect("matches Tool settlements by Run, Turn, and call identity", () =>
      Effect.gen(function* () {
        const firstTurn: ReadonlyArray<Prompt.Message> = [
          Prompt.makeMessage("system", { content: "Answer as JSON." }),
          Prompt.makeMessage("user", {
            content: [Prompt.makePart("text", { text: '{"question":"book twice?"}' })],
          }),
          Prompt.makeMessage("assistant", {
            content: [
              Prompt.makePart("tool-call", {
                id: CALL_ONE,
                name: "book_flight",
                params: { destination: "Kyoto" },
                providerExecuted: false,
              }),
            ],
          }),
          Prompt.makeMessage("tool", {
            content: [
              Prompt.makePart("tool-result", {
                id: CALL_ONE,
                name: "book_flight",
                result: { bookingRef: "flight-42" },
                isFailure: false,
                providerExecuted: false,
              }),
            ],
          }),
        ];

        const secondTurn: ReadonlyArray<Prompt.Message> = [
          Prompt.makeMessage("assistant", {
            content: [
              Prompt.makePart("tool-call", {
                id: CALL_ONE,
                name: "book_flight",
                params: { destination: "Osaka" },
                providerExecuted: false,
              }),
            ],
          }),
        ];

        const firstResponse = yield* turnResponseBatch(turnInput(firstTurn));
        const firstResults = yield* turnResultsBatch(turnInput(firstTurn));
        const secondResponse = yield* turnResponseBatch(turnInput(secondTurn, 2));
        const records = envelopesOf([firstResponse, firstResults, secondResponse]);

        const recovering = yield* projectRunJournal(records, RUN_ID);

        expect(recovering.prompt.content.map((message) => message.role)).toEqual([
          "system",
          "user",
          "assistant",
          "tool",
          "assistant",
        ]);

        const later = yield* projectRunJournal(records, LATER_RUN_ID);

        expect(later.prompt.content.map((message) => message.role)).toEqual([
          "user",
          "assistant",
          "tool",
          "assistant",
          "tool",
        ]);
        expect(later.prompt.content.filter((message) => message.role === "assistant")).toHaveLength(
          2,
        );
        expect(
          later.prompt.content.flatMap((message) =>
            message.role === "tool"
              ? message.content.filter((part) => part.type === "tool-result").map((part) => part.id)
              : [],
          ),
        ).toEqual([CALL_ONE, CALL_ONE]);
      }),
    );
  });
});

describe("engine compaction records and projection (RUN-026)", () => {
  const messageText = (message: Prompt.Message): string =>
    typeof message.content === "string"
      ? message.content
      : message.content
          .map((part) => ("text" in part && typeof part.text === "string" ? part.text : ""))
          .join("");

  const promptText = (prompt: Prompt.Prompt): string => prompt.content.map(messageText).join("\n");

  interface CompactionOverrides {
    readonly kind?: "clear-tool-results" | "summarize" | "rollover";
    readonly coversThrough?: number;
    readonly summary?: string | undefined;
    readonly handoff?: string | undefined;
    readonly runId?: string;
    readonly turn?: number;
  }

  const compactionPayload = (
    overrides: CompactionOverrides,
  ): (typeof RecordEnvelope.Encoded)["payload"] => {
    const summary = "summary" in overrides ? overrides.summary : "Goal: book the Kyoto trip";

    return {
      _tag: "CompactionCreated",
      runId: overrides.runId ?? `${LATER_RUN_ID}`,
      turn: overrides.turn ?? 1,
      kind: overrides.kind ?? "summarize",
      coversThrough: overrides.coversThrough ?? 3,
      // `optionalKey` fields must be ABSENT, not undefined.
      ...(summary === undefined ? {} : { summary }),
      ...(overrides.handoff === undefined ? {} : { handoff: overrides.handoff }),
    };
  };

  /** Second Turn of the owning Run: one more assistant declaration + result. */
  const secondToolTurn: ReadonlyArray<Prompt.Message> = [
    Prompt.makeMessage("assistant", {
      content: [
        Prompt.makePart("tool-call", {
          id: "call-2",
          name: "book_lodging",
          params: { nights: 3 },
          providerExecuted: false,
        }),
      ],
    }),
    Prompt.makeMessage("tool", {
      content: [
        Prompt.makePart("tool-result", {
          id: "call-2",
          name: "book_lodging",
          result: { bookingRef: "lodging-7" },
          isFailure: false,
          providerExecuted: false,
        }),
      ],
    }),
  ];

  layer(NodeCrypto.layer)((it) => {
    // Independent Runs can both resume, so neither continuation nor compaction may absorb the other:
    // https://github.com/danieljvdm/effect-agent/commit/8fc53ad9eb6b110ca6faaaebbb6dbba08e3c292f
    it.effect("preserves both interleaved Run contexts and their complete Thread history", () =>
      Effect.gen(function* () {
        const first = yield* turnCanonicalBatch({
          ...turnInput(toolTurnAppended),
          runScopedPrefixLength: 2,
        });

        const next = yield* turnCanonicalBatch(
          turnInput(
            [
              Prompt.makeMessage("user", {
                content: [Prompt.makePart("text", { text: "Answer while I wait." })],
              }),
              ...completionTurnAppended,
            ],
            1,
            LATER_RUN_ID,
          ),
        );

        const continuation = yield* turnCanonicalBatch(turnInput(finalTurnAppended, 2));

        for (const settled of [true, false]) {
          const original = [
            ...first.records,
            ...(settled ? next.records : next.records.slice(0, 1)),
          ].map((record, index) => envelopeAt(index + 1, record));

          const before = yield* projectRunJournal(original, LATER_RUN_ID);

          const interleaved = [
            ...original,
            ...continuation.records.map((record, index) =>
              envelopeAt(original.length + index + 1, record),
            ),
          ];

          expect(yield* projectRunJournal(interleaved, LATER_RUN_ID)).toEqual(before);
          expect(
            textOfPrompt((yield* projectRunJournal(interleaved, RUN_ID)).prompt),
          ).not.toContain("Answer while I wait.");
          for (const kind of ["rollover", "clear-tool-results"] as const) {
            const records = [
              ...interleaved,
              envelopeAt(
                interleaved.length + 1,
                auditRecord(
                  `interleaved-${kind}`,
                  compactionPayload({
                    kind,
                    runId: RUN_ID,
                    turn: 3,
                    coversThrough: interleaved.length,
                    summary: undefined,
                    handoff: "The original trip is booked.",
                  }),
                ),
              ),
            ];

            expect(yield* projectRunJournal(records, LATER_RUN_ID)).toEqual(before);
            const lengths: Array<number> = [];

            const complete = (yield* projectRunJournalStream(
              Stream.fromIterable(records),
              undefined,
              ({ promptLength }) => lengths.push(promptLength),
            )).prompt;

            expect(textOfPrompt(complete)).toContain("Answer while I wait.");
            expect(toolResults(complete)).toContainEqual(
              settled
                ? { messageId: "message-42" }
                : expect.objectContaining({ _tag: "ToolOutcomeUnknown" }),
            );
            expect(lengths.at(-1)).toBe(complete.content.length);
            if (kind === "rollover") {
              expect((yield* projectRunJournal(records, RUN_ID)).contextWindowId).toBe(
                contextWindowId(RUN_ID, 3),
              );
            }
          }
        }
      }),
    );

    // The same interleaving must retain each independently compacted context:
    // https://github.com/danieljvdm/effect-agent/commit/8fc53ad9eb6b110ca6faaaebbb6dbba08e3c292f
    it.effect("does not restore retired exchanges when independent Runs both compact", () =>
      Effect.gen(function* () {
        const first = yield* turnCanonicalBatch(turnInput(toolTurnAppended));
        const next = yield* turnCanonicalBatch(turnInput(completionTurnAppended, 1, LATER_RUN_ID));
        const continuation = yield* turnCanonicalBatch(turnInput(finalTurnAppended, 2));

        for (const kind of ["clear-tool-results", "rollover"] as const) {
          const records = envelopesOf([first, next]);

          records.push(
            envelopeAt(
              records.length + 1,
              auditRecord(
                "second-run-compaction",
                compactionPayload({
                  kind,
                  runId: LATER_RUN_ID,
                  turn: 2,
                  coversThrough: records.length,
                  summary: undefined,
                  handoff: "The correction is recorded.",
                }),
              ),
            ),
          );
          for (const record of continuation.records)
            records.push(envelopeAt(records.length + 1, record));
          records.push(
            envelopeAt(
              records.length + 1,
              auditRecord(
                "first-run-compaction",
                compactionPayload({
                  kind,
                  runId: RUN_ID,
                  turn: 3,
                  coversThrough: records.length,
                  summary: undefined,
                  handoff: "The original trip is booked.",
                }),
              ),
            ),
          );
          const projected = yield* projectRunJournal(records, RUN_NONE_ID);
          const history = yield* promptFromCanonicalRecords(records);

          expect(projected.prompt).toEqual(history);
          if (kind === "clear-tool-results") {
            expect(toolResults(history)).toEqual([
              "[tool result cleared by compaction]",
              "[tool result cleared by compaction]",
              "[tool result cleared by compaction]",
            ]);
          } else {
            expect(toolResults(history)).toEqual([]);
            expect(textOfPrompt(history)).toContain("The correction is recorded.");
            expect(textOfPrompt(history)).toContain("The original trip is booked.");
            expect(textOfPrompt(history)).not.toContain('"answer":"Booked."');
          }
          records.push(
            envelopeAt(
              records.length + 1,
              auditRecord(
                "combined-summary",
                compactionPayload({
                  kind: "summarize",
                  runId: RUN_NONE_ID,
                  coversThrough: records.length,
                  summary: "Both requests are complete.",
                }),
              ),
            ),
          );
          const combined = yield* promptFromCanonicalRecords(records);

          expect(combined.content).toHaveLength(1);
          expect(textOfPrompt(combined)).toContain("Both requests are complete.");
          expect(toolResults(combined)).toEqual([]);
        }
      }),
    );

    for (const kind of ["rollover", "clear-tool-results"] as const)
      it.effect(
        `${kind} cannot cover an incomplete Tool batch even when its available results are below the cutoff`,
        () =>
          Effect.gen(function* () {
            const batch = yield* turnCanonicalBatch({
              ...turnInput(toolTurnAppended),
              runScopedPrefixLength: 2,
            });

            const records = envelopesOf([batch]).slice(0, 2);

            const rollover = envelopeAt(
              3,
              auditRecord(
                "incomplete-rollover",
                compactionPayload({
                  kind,
                  runId: RUN_ID,
                  turn: 2,
                  summary: undefined,
                  coversThrough: 2,
                  handoff: "Uncommitted handoff",
                }),
              ),
            );

            const projection = yield* projectRunJournal([...records, rollover], RUN_ID);

            expect(projection.contextWindowId).toBeUndefined();
            expect(promptText(projection.prompt)).not.toContain("Uncommitted handoff");
            expect(projection.prompt.content.slice(0, 2)).toEqual(toolTurnAppended.slice(0, 2));
            expect(toolResults(projection.prompt)).toEqual([{ bookingRef: "flight-42" }]);

            const metadata = makeJournalMetadata(RUN_ID);

            for (const record of [...records, rollover]) metadata.add(record);

            const source = Stream.fromIterable([...records, rollover]);

            const prepared = yield* projectRunJournalStream(
              source,
              RUN_ID,
              undefined,
              undefined,
              metadata.snapshot(),
            );

            expect(prepared).toEqual(projection);
          }),
      );

    it.effect.each(["before", "after", "current", "response-after-terminal"] as const)(
      "compacts invisible incomplete prior batches only after their canonical termination: %s",
      (position) =>
        Effect.gen(function* () {
          const batch = yield* turnCanonicalBatch(
            turnInput(toolTurnAppended, 1, RUN_ID, { inputTokens: 100, outputTokens: 10 }),
          );

          const incomplete = envelopesOf([batch]).slice(0, 2);

          const next = yield* turnCanonicalBatch(
            turnInput(secondToolTurn, 1, LATER_RUN_ID, { inputTokens: 200, outputTokens: 20 }),
          );

          const postTerminalResponse = yield* turnResponseBatch(
            turnInput(secondToolTurn, 2, RUN_ID, { inputTokens: 300, outputTokens: 30 }),
          );

          const records = [...incomplete];
          const owner = position === "current" ? RUN_ID : LATER_RUN_ID;

          const terminalRecord = auditRecord("terminal-RunFailed", {
            _tag: "RunFailed",
            runId: RUN_ID,
            failure: { message: "failed" },
          });

          if (
            position === "before" ||
            position === "current" ||
            position === "response-after-terminal"
          )
            records.push(envelopeAt(records.length + 1, terminalRecord));
          if (position === "response-after-terminal") {
            for (const record of postTerminalResponse.records)
              records.push(envelopeAt(records.length + 1, record));
          }
          for (const record of next.records) records.push(envelopeAt(records.length + 1, record));
          const baseline = yield* projectRunJournal(records, owner);

          expect(baseline.usage).toMatchObject(
            position === "current"
              ? { inputTokens: 100, outputTokens: 10 }
              : { inputTokens: 200, outputTokens: 20 },
          );
          const through = records.length;

          records.push(
            envelopeAt(
              records.length + 1,
              auditRecord(
                "terminal-prefix-compaction",
                compactionPayload({
                  kind: "rollover",
                  runId: owner,
                  turn: 2,
                  summary: undefined,
                  coversThrough: through,
                  handoff: "Retained handoff",
                }),
              ),
            ),
          );
          if (position === "after") records.push(envelopeAt(records.length + 1, terminalRecord));
          const replay = yield* projectRunJournal(records, owner);

          if (position === "before") {
            expect(replay.contextWindowId).toBe(contextWindowId(owner, 2));
            expect(replay.prompt.content).toEqual([
              contextWindowMessage(contextWindowId(owner, 2), "Retained handoff"),
            ]);
            for (const projectionOwner of [RUN_ID, undefined]) {
              const otherBaseline = yield* projectRunJournalStream(
                Stream.fromIterable(records.slice(0, through)),
                projectionOwner,
              );

              const otherView = yield* projectRunJournalStream(
                Stream.fromIterable(records),
                projectionOwner,
              );

              expect(otherView.usage).toEqual(otherBaseline.usage);
              expect(otherView.policyUsage).toEqual(otherBaseline.policyUsage);
              if (projectionOwner === RUN_ID) {
                // A later independent Run cannot rewrite this Run's resume context:
                // https://github.com/danieljvdm/effect-agent/commit/8fc53ad9eb6b110ca6faaaebbb6dbba08e3c292f
                expect(otherView).toEqual(otherBaseline);
                expect(otherBaseline.usage).toMatchObject({
                  inputTokens: 100,
                  outputTokens: 10,
                });
              } else {
                expect(otherView.contextWindowId).toBe(contextWindowId(owner, 2));
              }
            }
          } else {
            expect(replay.prompt).toEqual(baseline.prompt);
            expect(replay.contextWindowId).toBeUndefined();
          }
          expect(replay.usage).toEqual(baseline.usage);
          expect(replay.policyUsage).toEqual(baseline.policyUsage);
          expect(records.slice(0, incomplete.length)).toEqual(incomplete);
        }),
    );

    it.effect(
      "streams a validated summary without decoding covered responses, but rejects split coverage",
      () =>
        Effect.gen(function* () {
          const turn = yield* turnCanonicalBatch(turnInput(toolTurnAppended));

          const records = envelopesOf([turn]).map((envelope) => {
            const payload = envelope.record.payload;

            return payload._tag !== "ModelResponseRecorded"
              ? envelope
              : CanonicalRecordEnvelope.make({
                  ...envelope,
                  record: RecordEnvelope.make({
                    ...envelope.record,
                    payload: { ...payload, messages: { archived: true } },
                  }),
                });
          });

          const summarize = envelopeAt(
            records.length + 1,
            auditRecord("stream-summary", compactionPayload({ coversThrough: 3 })),
          );

          const source = Stream.fromIterable([...records, summarize]);

          const projected = yield* projectRunJournalStream(source, LATER_RUN_ID);

          expect(promptText(projected.prompt)).toContain("Goal: book the Kyoto trip");
          expect(toolResults(projected.prompt)).toEqual([]);

          const split = envelopeAt(
            records.length + 1,
            auditRecord("stream-split", compactionPayload({ coversThrough: 1 })),
          );

          const failure = yield* projectRunJournalStream(
            Stream.fromIterable([...records, split]),
            LATER_RUN_ID,
          ).pipe(Effect.flip);

          expect(failure._tag).toBe("RunJournalError");
        }),
    );

    it.effect(
      "keeps prepared metadata bound to its captured prefix when later evidence arrives",
      () =>
        Effect.gen(function* () {
          const batch = yield* turnCanonicalBatch(turnInput(toolTurnAppended));
          const records = envelopesOf([batch]).slice(0, 2);

          const replacement = envelopeAt(
            records.length + 1,
            auditRecord("captured-summary", compactionPayload({ coversThrough: records.length })),
          );

          const prefix = [...records, replacement];
          const metadata = makeJournalMetadata(LATER_RUN_ID);

          for (const record of prefix) metadata.add(record);
          const captured = metadata.snapshot();
          const settled = envelopesOf([batch])[2];

          if (settled === undefined) return yield* Effect.die("Expected a settled Tool fixture");

          // Late settlement evidence would invalidate the summary in a newer prefix.
          const late = envelopeAt(prefix.length + 1, settled.record);

          metadata.add(late);

          const projected = yield* projectRunJournalStream(
            Stream.fromIterable(prefix),
            LATER_RUN_ID,
            undefined,
            undefined,
            captured,
          );

          expect(promptText(projected.prompt)).toContain("Goal: book the Kyoto trip");
          expect(toolResults(projected.prompt)).toEqual([]);
          expect(projected).toEqual(yield* projectRunJournal(prefix, LATER_RUN_ID));

          const newer = yield* projectRunJournalStream(
            Stream.fromIterable([...prefix, late]),
            LATER_RUN_ID,
            undefined,
            undefined,
            metadata.snapshot(),
          );

          expect(promptText(newer.prompt)).not.toContain("Goal: book the Kyoto trip");
          expect(newer).toEqual(yield* projectRunJournal([...prefix, late], LATER_RUN_ID));
        }),
    );
  });
});

layer(NodeCrypto.layer)("Tool exposure journal", (it) => {
  it.effect(
    "retains last declared replacement across partial settles, compaction, and checkpoints",
    () =>
      Effect.gen(function* () {
        const initial = Selection.make({ toolNames: ["initial"] });
        const expected = Selection.make({ toolNames: ["last"] });

        const response = yield* turnResponseBatch({
          ...turnInput(toolTurnAppended),
          toolExposure: Snapshot.make({
            exposedToolNames: ["book_flight", "book_lodging"],
            selection: initial,
          }),
        });

        const results = yield* turnResultsBatch({
          ...turnInput(toolTurnAppended),
          toolSelections: new Map([
            [CALL_ONE, Selection.make({ toolNames: ["first"] })],
            [CALL_TWO, expected],
          ]),
        });

        const first = results.records[0]!;
        const second = results.records[1]!;
        const declaration = envelopeAt(1, response.records[0]!);
        const partial = [declaration, envelopeAt(2, second)];

        expect((yield* projectRunJournal(partial, RUN_ID)).toolSelection).toEqual(initial);
        const records = [...partial, envelopeAt(3, first)];
        const projected = yield* projectRunJournal(records, RUN_ID);

        expect(projected.toolSelection).toEqual(expected);
        expect((yield* projectRunJournal(records, LATER_RUN_ID)).toolSelection).toBeUndefined();

        const compacted = envelopeAt(
          4,
          auditRecord("exposure-rollover", {
            _tag: "CompactionCreated",
            runId: RUN_ID,
            turn: 2,
            kind: "rollover",
            coversThrough: 3,
            handoff: "Continue.",
          }),
        );

        expect((yield* projectRunJournal([...records, compacted], RUN_ID)).toolSelection).toEqual(
          expected,
        );

        const seed = JournalCheckpointSeed.make({
          runId: RUN_ID,
          throughSequence: CanonicalSequence.make(3),
          firstSequence: CanonicalSequence.make(1),
          committedTurns: projected.committedTurns,
          policyUsage: projected.policyUsage,
          modelCalls: projected.usage.modelCalls,
          unobservedModelCalls: 0,
          inputTokens: projected.usage.inputTokens,
          outputTokens: projected.usage.outputTokens,
          lastInputTokens: projected.usage.lastInputTokens,
          lastOutputTokens: projected.usage.lastOutputTokens,
          costMicrousd: projected.usage.costMicrousd,
          summarizedModelUsage: yield* summarizeModelUsage(projected.usage.modelUsage),
          compaction: compacted,
          toolSelection: expected,
        });

        expect(
          (yield* projectRunJournalStream(
            Stream.fromIterable([compacted]),
            RUN_ID,
            undefined,
            seed,
          )).toolSelection,
        ).toEqual(expected);

        const hostReplacement = yield* turnCanonicalBatch({
          ...turnInput(finalTurnAppended),
          turn: 2,
          turnId: turnIdForRun(RUN_ID, 2),
          toolExposure: Snapshot.make({
            exposedToolNames: [],
            selection: Selection.make({ toolNames: [] }),
          }),
        });

        expect(
          (yield* projectRunJournal(
            [...records, envelopeAt(4, hostReplacement.records[0]!)],
            RUN_ID,
          )).toolSelection?.toolNames,
        ).toEqual([]);
      }),
  );
});

import { ConversationId, SubmissionId, ToolCallId } from "@effect-agent/core";
import { NodeCrypto } from "@effect/platform-node";
import { describe, expect, it, layer } from "@effect/vitest";
import { DateTime, Effect, Schema } from "effect";
import { Prompt } from "effect/unstable/ai";

import {
  BatchId,
  CanonicalRecordEnvelope,
  CanonicalSequence,
  childConversationIdFor,
  childIdempotencyKeyFor,
  DeploymentId,
  ObservationOffset,
  ProducerId,
  RecordEnvelope,
  RunJournalError,
  modelResponseRecordId,
  promptFromCanonicalRecords,
  projectRunJournal,
  runIdForSubmission,
  subagentJoinBatchId,
  subagentJoinedRecordId,
  subagentLineageBatchId,
  subagentLineageRecordId,
  subagentRequestedBatchId,
  subagentRequestedRecordId,
  subagentStartedBatchId,
  subagentStartedRecordId,
  toolCallSettledRecordId,
  turnCanonicalBatch,
  turnIdForRun,
  turnPreparedBatchId,
  turnResponseBatch,
  turnResponseBatchId,
  turnResultsBatch,
  turnResultsBatchId,
  type CanonicalBatch,
} from "../src/index.ts";

const SUBMISSION_ID = Schema.decodeSync(SubmissionId)("submission-journal");
const RUN_ID = runIdForSubmission(SUBMISSION_ID);
const LATER_RUN_ID = runIdForSubmission(Schema.decodeSync(SubmissionId)("submission-later"));
const CALL_ONE = Schema.decodeSync(ToolCallId)("call-1");
const CALL_TWO = Schema.decodeSync(ToolCallId)("call-2");
const PRODUCER_ID = Schema.decodeSync(ProducerId)("producer-journal");
const DEPLOYMENT_ID = Schema.decodeSync(DeploymentId)("deployment-journal");
const CREATED_AT = DateTime.toUtc(DateTime.makeUnsafe(1_000));
const CONVERSATION_ID = Schema.decodeSync(ConversationId)("conversation-journal");

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

const turnInput = (appended: ReadonlyArray<Prompt.Message>, turn = 1, runId = RUN_ID) => ({
  runId,
  turn,
  turnId: turnIdForRun(runId, turn),
  appended,
  producerId: PRODUCER_ID,
  deploymentId: DEPLOYMENT_ID,
  createdAt: CREATED_AT,
});

const envelopeAt = (sequence: number, record: RecordEnvelope): CanonicalRecordEnvelope =>
  CanonicalRecordEnvelope.make({
    conversationId: CONVERSATION_ID,
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

const auditRecord = (
  recordId: string,
  payload: (typeof RecordEnvelope.Encoded)["payload"],
): RecordEnvelope =>
  Schema.decodeSync(RecordEnvelope)({
    recordId,
    family: "conversation",
    schemaVersion: 1,
    createdAt: "2026-08-12T12:00:00.000Z",
    deploymentId: "deployment-journal",
    payload,
  });

describe("run journal batch split (plan §2.1)", () => {
  layer(NodeCrypto.layer)((it) => {
    it.effect("splits a tool Turn into response and results batches with stable identities", () =>
      Effect.gen(function* () {
        const response = yield* turnResponseBatch(turnInput(toolTurnAppended));
        expect(response.batchId).toBe(turnResponseBatchId(RUN_ID, 1));
        expect(response.batchId).toBe(`turn-response:${RUN_ID}:1`);
        expect(response.records.map((record) => record.recordId)).toEqual([
          modelResponseRecordId(RUN_ID, 1),
        ]);
        expect(response.records[0]?.payload._tag).toBe("ModelResponseRecorded");

        const results = yield* turnResultsBatch(turnInput(toolTurnAppended));
        expect(results.batchId).toBe(turnResultsBatchId(RUN_ID, 1));
        expect(results.batchId).toBe(`turn-results:${RUN_ID}:1`);
        // Declaration order, record identity unchanged from the P4 single-batch shape.
        expect(results.records.map((record) => record.recordId)).toEqual([
          toolCallSettledRecordId(RUN_ID, 1, CALL_ONE),
          toolCallSettledRecordId(RUN_ID, 1, CALL_TWO),
        ]);
        for (const record of results.records) {
          expect(record.payload._tag).toBe("ToolCallSettled");
        }
        expect(turnPreparedBatchId(RUN_ID, 1)).toBe(`turn-prepared:${RUN_ID}:1`);

        // The split is deterministic: rebuilding yields byte-identical batches (honest replay).
        const responseAgain = yield* turnResponseBatch(turnInput(toolTurnAppended));
        const encodedAgain = yield* Schema.encodeEffect(RecordEnvelope)(responseAgain.records[0]!);
        const encodedFirst = yield* Schema.encodeEffect(RecordEnvelope)(response.records[0]!);
        expect(encodedAgain).toEqual(encodedFirst);
      }),
    );

    it.effect("replays split-batch commits to the same prompt as P4 single-batch commits", () =>
      Effect.gen(function* () {
        const single = yield* turnCanonicalBatch(turnInput(toolTurnAppended));
        const response = yield* turnResponseBatch(turnInput(toolTurnAppended));
        const results = yield* turnResultsBatch(turnInput(toolTurnAppended));

        const singleProjection = yield* projectRunJournal(envelopesOf([single]), RUN_ID);
        const splitProjection = yield* projectRunJournal(envelopesOf([response, results]), RUN_ID);

        expect(splitProjection.committedTurns).toBe(singleProjection.committedTurns);
        expect(splitProjection.prompt).toEqual(singleProjection.prompt);
        expect(splitProjection.historyBefore).toEqual(singleProjection.historyBefore);
      }),
    );

    it.effect("does not replay an incomplete assistant Tool turn into a later Run", () =>
      Effect.gen(function* () {
        const failedResponse = yield* turnResponseBatch(turnInput(toolTurnAppended));
        const records = envelopesOf([failedResponse]);

        const recovering = yield* projectRunJournal(records, RUN_ID);
        expect(
          recovering.prompt.content.some(
            (message) =>
              message.role === "assistant" &&
              message.content.some((part) => part.type === "tool-call" && part.id === CALL_ONE),
          ),
        ).toBe(true);

        const later = yield* projectRunJournal(records, LATER_RUN_ID);
        expect(later.prompt.content.map((message) => message.role)).toEqual(["system", "user"]);
        expect(
          later.prompt.content.some(
            (message) =>
              message.role === "assistant" &&
              message.content.some((part) => part.type === "tool-call"),
          ),
        ).toBe(false);
        expect(later.historyBefore).toEqual(later.prompt);
      }),
    );

    it.effect("does not reserve the real run:none identity for canonical prompt projection", () =>
      Effect.gen(function* () {
        const runNone = runIdForSubmission(Schema.decodeSync(SubmissionId)("none"));
        expect(runNone).toBe("run:none");
        const response = yield* turnResponseBatch(turnInput(toolTurnAppended, 1, runNone));
        const records = envelopesOf([response]);

        const recovering = yield* projectRunJournal(records, runNone);
        expect(recovering.prompt.content.map((message) => message.role)).toEqual([
          "system",
          "user",
          "assistant",
        ]);

        const canonicalPrompt = yield* promptFromCanonicalRecords(records);
        expect(canonicalPrompt.content.map((message) => message.role)).toEqual(["system", "user"]);
      }),
    );

    it.effect("reports an invalid persisted Tool Call ID as a journal error", () =>
      Effect.gen(function* () {
        const malformedTurn: ReadonlyArray<Prompt.Message> = [
          Prompt.makeMessage("assistant", {
            content: [
              Prompt.makePart("tool-call", {
                id: "",
                name: "book_flight",
                params: { destination: "Kyoto" },
                providerExecuted: false,
              }),
            ],
          }),
        ];
        const response = yield* turnResponseBatch(turnInput(malformedTurn));

        const error = yield* promptFromCanonicalRecords(envelopesOf([response])).pipe(Effect.flip);
        expect(error).toBeInstanceOf(RunJournalError);
        expect(error.message).toBe("Failed to decode a declared Tool Call ID");
      }),
    );

    it.effect(
      "omits a partially settled application Tool batch from later Runs without classifying provider calls",
      () =>
        Effect.gen(function* () {
          const response = yield* turnResponseBatch(turnInput(toolTurnAppended));
          const results = yield* turnResultsBatch(turnInput(toolTurnAppended));
          const firstSettled = results.records[0]!;
          const partialRecords = [response.records[0]!, firstSettled].map((record, index) =>
            envelopeAt(index + 1, record),
          );

          const recovering = yield* projectRunJournal(partialRecords, RUN_ID);
          const recoveringAssistant = recovering.prompt.content.find(
            (message) => message.role === "assistant",
          );
          expect(
            recoveringAssistant?.role === "assistant"
              ? recoveringAssistant.content
                  .filter((part) => part.type === "tool-call")
                  .map((part) => part.id)
              : [],
          ).toEqual([CALL_ONE, CALL_TWO]);
          const recoveringTool = recovering.prompt.content.find(
            (message) => message.role === "tool",
          );
          expect(
            recoveringTool?.role === "tool"
              ? recoveringTool.content
                  .filter((part) => part.type === "tool-result")
                  .map((part) => part.id)
              : [],
          ).toEqual([CALL_ONE]);

          const later = yield* projectRunJournal(partialRecords, LATER_RUN_ID);
          expect(later.prompt.content.map((message) => message.role)).toEqual(["system", "user"]);
          expect(later.prompt.content.some((message) => message.role === "assistant")).toBe(false);
          expect(later.prompt.content.some((message) => message.role === "tool")).toBe(false);

          const providerTurn: ReadonlyArray<Prompt.Message> = [
            Prompt.makeMessage("system", { content: "Answer as JSON." }),
            Prompt.makeMessage("user", {
              content: [Prompt.makePart("text", { text: '{"question":"search?"}' })],
            }),
            Prompt.makeMessage("assistant", {
              content: [
                Prompt.makePart("tool-call", {
                  id: "provider-call",
                  name: "web_search",
                  params: { query: "Kyoto" },
                  providerExecuted: true,
                }),
              ],
            }),
          ];
          const providerResponse = yield* turnResponseBatch(turnInput(providerTurn));
          const providerLater = yield* projectRunJournal(
            envelopesOf([providerResponse]),
            LATER_RUN_ID,
          );
          expect(providerLater.prompt.content.map((message) => message.role)).toEqual([
            "system",
            "user",
            "assistant",
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
          "system",
          "user",
          "assistant",
          "tool",
        ]);
        expect(later.prompt.content.filter((message) => message.role === "assistant")).toHaveLength(
          1,
        );
        expect(
          later.prompt.content.flatMap((message) =>
            message.role === "tool"
              ? message.content.filter((part) => part.type === "tool-result").map((part) => part.id)
              : [],
          ),
        ).toEqual([CALL_ONE]);
      }),
    );

    it.effect("projects prepared/unknown/step/approval/resolution records transparently", () =>
      Effect.gen(function* () {
        const response = yield* turnResponseBatch(turnInput(toolTurnAppended));
        const results = yield* turnResultsBatch(turnInput(toolTurnAppended));
        const [firstSettled, secondSettled] = results.records;

        const prepared = auditRecord(`tool-prepared:${RUN_ID}:1:call-1`, {
          _tag: "ToolCallPrepared",
          runId: RUN_ID,
          turnId: turnIdForRun(RUN_ID, 1),
          turn: 1,
          toolCallId: "call-1",
          toolName: "book_flight",
          parameters: { destination: "Kyoto" },
          parametersDigest: "a".repeat(64),
        });
        const approvalRequested = auditRecord(`approval-request:${RUN_ID}:1:call-1`, {
          _tag: "ToolApprovalRequested",
          runId: RUN_ID,
          turnId: turnIdForRun(RUN_ID, 1),
          turn: 1,
          toolCallId: "call-1",
          toolName: "book_flight",
          parametersDigest: "a".repeat(64),
        });
        const approvalDecided = auditRecord(`approval-decision:${RUN_ID}:1:call-1`, {
          _tag: "ToolApprovalDecided",
          runId: RUN_ID,
          turn: 1,
          toolCallId: "call-1",
          decision: "approved",
          resolver: "operator",
          reason: "reviewed",
        });
        const step = auditRecord(`step:${RUN_ID}:call-1:reserve-flight`, {
          _tag: "ToolStepSettled",
          runId: RUN_ID,
          toolCallId: "call-1",
          stepName: "reserve-flight",
          output: { bookingRef: "flight-42" },
          outputDigest: "b".repeat(64),
        });
        const unknown = auditRecord(`tool-unknown:${RUN_ID}:1:call-2`, {
          _tag: "ToolCallUnknown",
          runId: RUN_ID,
          turn: 1,
          toolCallId: "call-2",
          toolName: "book_lodging",
          reason: "worker lost mid-handler",
        });
        const resolved = auditRecord(`tool-resolved:${RUN_ID}:1:call-2`, {
          _tag: "ToolCallResolved",
          runId: RUN_ID,
          toolCallId: "call-2",
          resolution: "completed-with-result",
          author: "operator",
          reason: "supplier store shows the booking",
        });
        const interrupted = auditRecord(`interrupted:${RUN_ID}:2`, {
          _tag: "ModelResponseInterrupted",
          runId: RUN_ID,
          supersededEpoch: 2,
          attemptId: "attempt-1",
          reason: "superseded a prior owner",
        });

        // Audit records interleave everywhere a real coordinator can put them — including
        // BETWEEN the two ToolCallSettled records (the late-settle resolution path).
        const interleaved = [
          response.records[0]!,
          approvalRequested,
          approvalDecided,
          prepared,
          step,
          interrupted,
          firstSettled!,
          unknown,
          resolved,
          secondSettled!,
        ].map((record, index) => envelopeAt(index + 1, record));

        const plain = yield* projectRunJournal(envelopesOf([response, results]), RUN_ID);
        const transparent = yield* projectRunJournal(interleaved, RUN_ID);

        // One Turn, one Tool message: the audit records neither add prompt content nor split
        // the contiguous settled group.
        expect(transparent.committedTurns).toBe(plain.committedTurns);
        expect(transparent.prompt).toEqual(plain.prompt);
        expect(transparent.prompt.content.map((message) => message.role)).toEqual([
          "system",
          "user",
          "assistant",
          "tool",
        ]);
        const toolMessage = transparent.prompt.content.at(-1);
        expect(toolMessage?.role === "tool" ? toolMessage.content.length : undefined).toBe(2);
      }),
    );

    it.effect("subagent lifecycle records are prompt-transparent (S2, spec §5/§11)", () =>
      Effect.gen(function* () {
        const response = yield* turnResponseBatch(turnInput(toolTurnAppended));
        const results = yield* turnResultsBatch(turnInput(toolTurnAppended));
        const [firstSettled, secondSettled] = results.records;

        const requested = auditRecord(`subagent-requested:${RUN_ID}:call-1`, {
          _tag: "SubagentRequested",
          runId: RUN_ID,
          turnId: turnIdForRun(RUN_ID, 1),
          turn: 1,
          toolCallId: "call-1",
          delegationId: "delegation-destination-research",
          targetAgentId: "destination-researcher",
          targetDigests: { agent: "a".repeat(64), model: "b".repeat(64), tools: "c".repeat(64) },
          childInput: { destination: "Kyoto" },
          childInputDigest: "a".repeat(64),
          grantDigest: "b".repeat(64),
          reservationId: "reservation-1",
          reservationDigest: "c".repeat(64),
          childConversationId: childConversationIdFor(SUBMISSION_ID, CALL_ONE),
          childPrincipal: "tenant-a",
          childIdempotencyKey: childIdempotencyKeyFor(RUN_ID, CALL_ONE),
        });
        const started = auditRecord(`subagent-started:${RUN_ID}:call-1`, {
          _tag: "SubagentStarted",
          runId: RUN_ID,
          toolCallId: "call-1",
          childConversationId: childConversationIdFor(SUBMISSION_ID, CALL_ONE),
          childSubmissionId: "submission-child-1",
          childReceiptId: "receipt-child-1",
          childRunId: "run:submission-child-1",
        });
        const joined = auditRecord(`subagent-joined:${RUN_ID}:call-1`, {
          _tag: "SubagentJoined",
          runId: RUN_ID,
          toolCallId: "call-1",
          childSubmissionId: "submission-child-1",
          childSettlementId: "settlement:submission-child-1",
          childOutcome: "completed",
          childResultDigest: "a".repeat(64),
          projectedResultDigest: "b".repeat(64),
          usageSummary: { turns: 1, toolCalls: 0 },
          reservationId: "reservation-1",
          finalAccounting: { consumed: { turns: 1 }, released: { turns: 3 } },
        });
        const lineage = auditRecord(`subagent-lineage:${CONVERSATION_ID}`, {
          _tag: "SubagentLineageRecorded",
          parentLink: {
            delegationId: "delegation-destination-research",
            parentAgentId: "travel-coordinator",
            parentConversationId: "conversation-parent",
            parentRunId: RUN_ID,
            parentToolCallId: "call-1",
            depth: 1,
          },
          parentSubmissionId: SUBMISSION_ID,
          childDefinitionDigests: {
            agent: "a".repeat(64),
            model: "b".repeat(64),
            tools: "c".repeat(64),
          },
          childInputDigest: "a".repeat(64),
          grantDigest: "b".repeat(64),
        });

        // The lifecycle records interleave everywhere a real coordinator can put them —
        // including BETWEEN the two ToolCallSettled records, because the atomic join batch
        // commits SubagentJoined beside the call's ToolCallSettled record (SUB-019). None of
        // them adds prompt content, none splits the contiguous settled group, and the child
        // transcript never enters the parent prompt (SUB-015).
        const interleaved = [
          response.records[0]!,
          requested,
          started,
          firstSettled!,
          joined,
          secondSettled!,
          lineage,
        ].map((record, index) => envelopeAt(index + 1, record));

        const plain = yield* projectRunJournal(envelopesOf([response, results]), RUN_ID);
        const transparent = yield* projectRunJournal(interleaved, RUN_ID);

        expect(transparent.committedTurns).toBe(plain.committedTurns);
        expect(transparent.prompt).toEqual(plain.prompt);
        const toolMessage = transparent.prompt.content.at(-1);
        expect(toolMessage?.role === "tool" ? toolMessage.content.length : undefined).toBe(2);
      }),
    );

    it.effect("fails typed when a results batch has no terminal Tool results", () =>
      Effect.gen(function* () {
        const noToolTurn: ReadonlyArray<Prompt.Message> = [
          Prompt.makeMessage("assistant", {
            content: [Prompt.makePart("text", { text: '{"answer":"done"}' })],
          }),
        ];
        const failure = yield* Effect.flip(turnResultsBatch(turnInput(noToolTurn)));
        expect(failure._tag).toBe("RunJournalError");

        // And the response builder rejects a Turn with no model-visible messages at all.
        const onlyTools: ReadonlyArray<Prompt.Message> = [
          Prompt.makeMessage("tool", {
            content: [
              Prompt.makePart("tool-result", {
                id: "call-1",
                name: "book_flight",
                result: { ok: true },
                isFailure: false,
                providerExecuted: false,
              }),
            ],
          }),
        ];
        const responseFailure = yield* Effect.flip(turnResponseBatch(turnInput(onlyTools)));
        expect(responseFailure._tag).toBe("RunJournalError");

        const badTurn = yield* Effect.flip(turnResponseBatch(turnInput(toolTurnAppended, 0)));
        expect(badTurn._tag).toBe("RunJournalError");
      }),
    );
  });
});

describe("S2 subagent deterministic identities (plan §3.1)", () => {
  it("derives the record, batch, and child identities from the parent Run and Tool Call pair", () => {
    expect(subagentRequestedRecordId(RUN_ID, CALL_ONE)).toBe(`subagent-requested:${RUN_ID}:call-1`);
    expect(subagentRequestedBatchId(RUN_ID, CALL_ONE)).toBe(`subagent-requested:${RUN_ID}:call-1`);
    expect(subagentStartedRecordId(RUN_ID, CALL_ONE)).toBe(`subagent-started:${RUN_ID}:call-1`);
    expect(subagentStartedBatchId(RUN_ID, CALL_ONE)).toBe(`subagent-started:${RUN_ID}:call-1`);
    expect(subagentJoinBatchId(RUN_ID, CALL_ONE)).toBe(`subagent-join:${RUN_ID}:call-1`);
    expect(subagentJoinedRecordId(RUN_ID, CALL_ONE)).toBe(`subagent-joined:${RUN_ID}:call-1`);
    // The atomic join batch pairs the joined record with the EXISTING per-call settled
    // identity, so `commitPendingTurn`'s record-identity dedupe covers both paths (SUB-019).
    expect(toolCallSettledRecordId(RUN_ID, 1, CALL_ONE)).toBe(`tool-settled:${RUN_ID}:1:call-1`);
    expect(subagentLineageRecordId(CONVERSATION_ID)).toBe(`subagent-lineage:${CONVERSATION_ID}`);
    expect(subagentLineageBatchId(CONVERSATION_ID)).toBe(`subagent-lineage:${CONVERSATION_ID}`);
    // D4 child identity derivations (SUB-016: idempotent establishment by construction).
    expect(childConversationIdFor(SUBMISSION_ID, CALL_ONE)).toBe(
      "subagent:submission-journal:call-1",
    );
    expect(childIdempotencyKeyFor(RUN_ID, CALL_ONE)).toBe("subagent:run:submission-journal:call-1");
  });
});

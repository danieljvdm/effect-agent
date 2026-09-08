import { ContextHistoryError } from "@effect-agent/engine/ContextHistory";
import {
  CanonicalRecordEnvelope,
  PersistedJson,
  type RecordEnvelope,
} from "@effect-agent/thread/Records";
import * as Projection from "@effect-agent/thread/ThreadContextHistoryProjection";
import { describe, expect, it } from "@effect/vitest";
import { Cause, Effect, Exit, Schema } from "effect";
import { Prompt } from "effect/unstable/ai";

const record = (sequence: number, payload: typeof RecordEnvelope.Encoded.payload) =>
  Schema.decodeSync(CanonicalRecordEnvelope)({
    threadId: "thread-1",
    batchId: `batch:${sequence}`,
    sequence,
    offset: `offset:${sequence}`,
    record: {
      recordId: `record:${sequence}`,
      family: "thread",
      schemaVersion: 1,
      createdAt: "2026-09-04T00:00:00.000Z",
      deploymentId: "test",
      payload,
    },
  });

const promptJson = (prompt: Prompt.Prompt) =>
  Schema.decodeUnknownSync(PersistedJson)(Schema.encodeSync(Prompt.Prompt)(prompt));

const output = (sequence: number, runId = "run-1") =>
  record(sequence, { _tag: "ModelCompleted", runId, output: { answer: "visible" } });

const rollover = (sequence: number, coversThrough: number, turn: number) =>
  record(sequence, {
    _tag: "CompactionCreated",
    runId: "run-1",
    kind: "rollover",
    coversThrough,
    turn,
  });

// The public projection is the privacy and lexical contract shared by native and indexed recall.
describe("context history projection", () => {
  it.effect("renders only retained evidence and keeps truncation envelopes intact", () =>
    Effect.gen(function* () {
      const truncated = {
        _tag: "TruncatedToolResult",
        head: "retained head",
        tail: "retained tail",
        originalBytes: 100_000,
      };

      const projected = yield* Projection.project(
        record(9, {
          _tag: "ToolCallSettled",
          runId: "run-1",
          toolCallId: "call-1",
          toolName: "read_file",
          isFailure: true,
          result: truncated,
        }),
      );

      expect(projected.boundary).toBeUndefined();
      expect(projected.evidence).toMatchObject({
        recordId: "record:9",
        sequence: 9,
        runId: "run-1",
        text: 'tool result read_file (call-1; failure):\n{"_tag":"TruncatedToolResult","head":"retained head","tail":"retained tail","originalBytes":100000}',
      });
      expect((yield* Projection.project(output(10))).evidence?.text).toBe(
        'assistant output:\n{"answer":"visible"}',
      );
    }),
  );

  it.effect("excludes private prompt parts and prefers retained messages over model output", () =>
    Effect.gen(function* () {
      const messages = promptJson(
        Prompt.make([
          Prompt.makeMessage("system", { content: "private system" }),
          Prompt.makeMessage("user", {
            content: [
              Prompt.makePart("text", { text: "visible request" }),
              Prompt.makePart("file", { mediaType: "image/png", data: "AQID" }),
            ],
          }),
          Prompt.makeMessage("assistant", {
            content: [
              Prompt.makePart("reasoning", { text: "private reasoning" }),
              Prompt.makePart("text", { text: "visible answer" }),
              Prompt.makePart("tool-call", {
                id: "call-1",
                name: "lookup",
                params: { query: "retained" },
                providerExecuted: false,
              }),
              Prompt.makePart("tool-approval-request", {
                approvalId: "private approval",
                toolCallId: "call-1",
              }),
            ],
          }),
          Prompt.makeMessage("tool", {
            content: [
              Prompt.makePart("tool-result", {
                id: "call-1",
                name: "lookup",
                isFailure: false,
                result: { value: "retained" },
                providerExecuted: false,
              }),
              Prompt.makePart("tool-approval-response", {
                approvalId: "private approval",
                approved: true,
                reason: "private reason",
              }),
            ],
          }),
        ]),
      );

      const expected =
        'user:\nvisible request\n[attachment omitted]\n\nassistant:\nvisible answer\ntool call lookup (call-1):\n{"query":"retained"}\n\ntool:\ntool result lookup (call-1; success):\n{"value":"retained"}';

      const completed = yield* Projection.project(
        record(1, {
          _tag: "ModelCompleted",
          runId: "run-1",
          output: "private fallback",
          messages,
        }),
      );

      const response = yield* Projection.project(
        record(2, {
          _tag: "ModelResponseRecorded",
          runId: "run-1",
          turnId: "turn-1",
          turn: 1,
          messages,
          messagesDigest: "a".repeat(64),
        }),
      );

      expect(completed.evidence?.text).toBe(expected);
      expect(response.evidence?.text).toBe(expected);
    }),
  );

  it.effect("returns empty projections for non-evidence without manufacturing a window", () =>
    Effect.gen(function* () {
      const privateRecords = [
        record(1, {
          _tag: "UserInputRecorded",
          kind: "user",
          runId: "run-1",
          input: { secret: "raw" },
        }),
        record(2, {
          _tag: "ToolStepSettled",
          runId: "run-1",
          toolCallId: "call-1",
          stepName: "private",
          output: "private",
          outputDigest: "a".repeat(64),
        }),
        record(3, { _tag: "RunFailed", runId: "run-1", failure: { secret: "diagnostic" } }),
        record(4, {
          _tag: "CompactionCreated",
          runId: "run-1",
          kind: "summarize",
          coversThrough: 3,
          turn: 2,
          summary: "private summary",
        }),
        record(5, {
          _tag: "ModelCompleted",
          runId: "run-1",
          output: "unused",
          messages: promptJson(Prompt.make([Prompt.makeMessage("system", { content: "private" })])),
        }),
      ];

      for (const item of privateRecords) {
        const projected = yield* Projection.project(item);

        expect(projected.evidence).toBeUndefined();
        expect(projected.boundary).toBeUndefined();
      }
    }),
  );

  it.effect("assigns by covered position rather than commit position, including later Runs", () =>
    Effect.gen(function* () {
      const first = (yield* Projection.project(rollover(5, 2, 2))).boundary!;
      const second = (yield* Projection.project(rollover(8, 6, 3))).boundary!;
      const equalCoverage = (yield* Projection.project(rollover(9, 6, 4))).boundary!;

      expect(first).toEqual(
        Projection.ContextHistoryBoundary.make({
          sequence: first.sequence,
          coversThrough: first.coversThrough,
          windowId: "context:run-1:2",
        }),
      );
      expect(first.sequence).toBe(5);
      expect(first.coversThrough).toBe(2);
      for (const [position, runId, expected] of [
        [2, "run-0", "context:run-0:0"],
        [3, "run-1", "context:run-1:2"],
        [6, "run-1", "context:run-1:2"],
        [7, "run-1", "context:run-1:4"],
        [10, "run-2", "context:run-1:4"],
      ] as const) {
        const evidence = (yield* Projection.project(output(position, runId))).evidence!;

        expect(Projection.windowIdFor(evidence, [first, second, equalCoverage])).toBe(expected);
      }
      const later = (yield* Projection.project(output(10, "run-2"))).evidence!;

      expect(Projection.windowIdFor(later, [equalCoverage])).toBe("context:run-1:4");
    }),
  );

  it.effect("rejects a boundary that covers its own or a future canonical record", () =>
    Effect.gen(function* () {
      for (const coversThrough of [4, 5]) {
        const result = yield* Effect.exit(Projection.project(rollover(4, coversThrough, 2)));

        expect(Exit.isFailure(result) && Cause.squash(result.cause)).toEqual(
          ContextHistoryError.make({
            reason: "unavailable",
            message: "Canonical context history is unavailable",
          }),
        );
      }
    }),
  );

  it.effect("shares Unicode case folding, literal matching, and bounded native snippets", () =>
    Effect.gen(function* () {
      const query = yield* Projection.normalizeQuery("  ÉCOLE[1]%_  ");

      expect(query).toBe("école[1]%_");
      expect(Projection.normalizeText("ÉCOLE[1]%_")).toBe(query);
      expect(Projection.matchText("An École[1]%_ entry", query)).toBe("An École[1]%_ entry");
      expect(Projection.matchText("An École[1]%_ entry", "école.*")).toBeUndefined();
      const late = `${"x".repeat(300)}TARGET${"y".repeat(3_000)}`;

      expect(Projection.matchText(late, "target")).toBe(
        `${"x".repeat(200)}TARGET${"y".repeat(1_794)}`,
      );
      for (const invalid of ["", "  ", "x".repeat(257)]) {
        const result = yield* Effect.exit(Projection.normalizeQuery(invalid));

        expect(Exit.isFailure(result) && Cause.squash(result.cause)).toMatchObject({
          reason: "invalid-input",
        });
      }
    }),
  );
});

type Equal<L, R> =
  (<T>() => T extends L ? 1 : 2) extends <T>() => T extends R ? 1 : 2 ? true : false;
type Assert<T extends true> = T;
type Projected = ReturnType<typeof Projection.project>;
type FailureProof = Assert<Equal<Effect.Error<Projected>, ContextHistoryError>>;
type RequirementProof = Assert<Equal<Effect.Services<Projected>, never>>;
it("projects one record without storage dependencies or widened failures", () => {
  const proof: readonly [FailureProof, RequirementProof] = [true, true];

  expect(proof).toEqual([true, true]);
});

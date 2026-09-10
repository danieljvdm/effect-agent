import { ThreadExport } from "@effect-agent/thread/ThreadStore";
import { DateTime, Schema } from "effect";
import { Prompt } from "effect/unstable/ai";
import { renderToStaticMarkup } from "react-dom/server";
import { expect, it } from "vite-plus/test";

import { ActivityPanel } from "../src/components/activity-panel.tsx";
import { PlannerActivity, PlannerSnapshot } from "../src/domain.ts";
import { plannerActivity } from "../src/server/activity.ts";

const prompt = (content: Prompt.AssistantMessage["content"]) =>
  Schema.encodeSync(Prompt.Prompt)(Prompt.make([Prompt.makeMessage("assistant", { content })]));

const records = (entries: ReadonlyArray<readonly [number, unknown]>) =>
  Schema.decodeUnknownSync(ThreadExport)({
    format: "effect-agent/thread@1",
    threadId: "conversation",
    tailSequence: entries.length,
    tailDigest: "0".repeat(64),
    records: entries.map(([ms, payload], index) => ({
      threadId: "conversation",
      batchId: "batch",
      sequence: index + 1,
      offset: `offset-${index}`,
      record: {
        recordId: `record-${index}`,
        family: "thread",
        schemaVersion: 1,
        createdAt: DateTime.formatIso(DateTime.makeUnsafe(ms)),
        deploymentId: "test",
        payload,
      },
    })),
  }).records;

const runStart = {
  _tag: "RunStarted",
  runId: "run",
  policyAccountingVersion: 1,
  maxDurationMillis: 180_000,
};

const modelResponse = (content: Prompt.AssistantMessage["content"]) => ({
  _tag: "ModelResponseRecorded",
  runId: "run",
  turnId: "turn",
  turn: 1,
  messages: prompt(content),
  messagesDigest: "0".repeat(64),
  inputTokens: 120,
  outputTokens: 40,
});

const settled = (outcome: string, extras = {}) => ({
  _tag: "SubmissionSettled",
  submissionId: "submission",
  settlementId: "settlement",
  receiptId: "receipt",
  runId: "run",
  outcome,
  ...extras,
});

it("projects model calls, arguments, results, usage and journal timing without changing the log", () => {
  const source = records([
    [
      0,
      {
        _tag: "UserInputRecorded",
        submissionId: "submission",
        kind: "user",
        input: {
          message: "When should I go?",
          selectedTripId: "tahoe",
          publication: null,
          settings: { model: "gpt-6-astra", reasoningEffort: "high", fast: true },
        },
      },
    ],
    [100, runStart],
    [
      2100,
      modelResponse([
        Prompt.makePart("tool-call", {
          id: "get",
          name: "get_trip",
          providerExecuted: false,
          params: { tripId: "tahoe" },
        }),
      ]),
    ],
    [
      2600,
      {
        _tag: "ToolCallSettled",
        runId: "run",
        toolCallId: "get",
        toolName: "get_trip",
        result: { title: "Tahoe", dates: null },
        isFailure: false,
      },
    ],
    [5000, { _tag: "RunCompleted", runId: "run", output: "Try mid-October." }],
    [5100, settled("completed", { result: "Try mid-October." })],
  ]);

  const before = JSON.stringify(source);
  const trace = plannerActivity(source);

  expect(Schema.is(Schema.Array(PlannerActivity))(trace)).toBe(true);
  expect(trace[0]?.runId).toBe("run");
  expect(trace[0]?.details?.[0]?.text).toContain('"reasoningEffort": "high"');
  expect(trace.find((event) => event.kind === "usage")).toMatchObject({
    elapsedMs: 2000,
    durationMs: 2000,
    timestamp: "1970-01-01T00:00:02.100Z",
  });
  const tool = trace.find((event) => event.text === "get_trip: completed");

  expect(tool).toMatchObject({
    elapsedMs: 2500,
    durationMs: 500,
    durationLabel: "Request to recorded result",
  });
  expect(tool?.details?.find((entry) => entry.label === "Arguments")?.text).toContain(
    '"tripId": "tahoe"',
  );
  expect(tool?.details?.find((entry) => entry.label === "Result")?.text).toContain(
    '"title": "Tahoe"',
  );
  expect(trace.at(-1)?.elapsedMs).toBe(5000);
  expect(JSON.stringify(source)).toBe(before);
});

it("shows rejected completion batches and complete safe diagnostics without claiming the calls ran", () => {
  const message = "Completion Tool deliver_response must be the only Tool Call in its batch";

  const trace = plannerActivity(
    records([
      [0, runStart],
      [
        1000,
        modelResponse([
          Prompt.makePart("tool-call", {
            id: "get",
            name: "get_trip",
            providerExecuted: false,
            params: { tripId: "tahoe" },
          }),
          Prompt.makePart("tool-call", {
            id: "deliver",
            name: "deliver_response",
            providerExecuted: false,
            params: { message: "October", content: null },
          }),
        ]),
      ],
      [1010, settled("failed", { result: { errorTag: "ModelProtocolError", message } })],
    ]),
  );

  expect(trace.filter((event) => event.kind === "tool")).toEqual([]);
  expect(trace.find((event) => event.kind === "usage")?.details?.[0]?.text).toContain(
    "deliver_response",
  );
  expect(trace.at(-1)).toMatchObject({
    kind: "failure",
    elapsedMs: 1010,
    text: `ModelProtocolError: ${message}`,
  });
  expect(trace.at(-1)?.details?.[0]?.text).toContain(message);
});

it("bounds details, excludes provider reasoning and credentials, and keeps legacy intervals unknown", () => {
  const trace = plannerActivity(
    records([
      [
        1000,
        modelResponse([
          Prompt.makePart("reasoning", { text: "private reasoning must stay private" }),
          Prompt.makePart("tool-call", {
            id: "search",
            name: "web_search",
            params: {
              query: "Tahoe",
              apiKey: "do-not-expose",
              headers: { Authorization: "secret" },
            },
            providerExecuted: true,
          }),
          Prompt.makePart("tool-result", {
            id: "search",
            name: "web_search",
            result: { sources: ["https://example.com"], token: "secret" },
            isFailure: false,
            providerExecuted: true,
          }),
        ]),
      ],
      [
        1100,
        {
          _tag: "ToolCallSettled",
          runId: "legacy",
          toolCallId: "large",
          toolName: "read_page",
          result: { body: "x".repeat(20_000) },
          isFailure: false,
        },
      ],
    ]),
  );

  const text = JSON.stringify(trace);

  expect(text).toContain("Tahoe");
  expect(text).toContain("https://example.com");
  expect(text).not.toContain("private reasoning");
  expect(text).not.toContain("do-not-expose");
  expect(text).not.toContain("secret");
  expect(trace[0]?.elapsedMs).toBeUndefined();
  expect(trace[0]?.durationMs).toBeUndefined();
  expect(trace.at(-1)?.details?.[0]).toMatchObject({ truncated: true });
  expect(Schema.is(Schema.Array(PlannerActivity))(trace)).toBe(true);
});

it("preserves timeout and interruption diagnostics without inventing model time after recovery", () => {
  const trace = plannerActivity(
    records([
      [0, runStart],
      [
        2000,
        {
          _tag: "ModelResponseInterrupted",
          runId: "run",
          attemptId: "next-attempt",
          supersededEpoch: 1,
          reason: "Ownership lost",
        },
      ],
      [3000, modelResponse([Prompt.makePart("text", { text: "Recovered" })])],
      [
        180_000,
        settled("failed", {
          policyLimit: "duration",
          result: { errorTag: "AgentPolicyError", message: "Time allowance exhausted" },
        }),
      ],
    ]),
  );

  expect(trace[1]?.text).toBe("Model response interrupted");
  expect(trace[2]?.durationMs).toBeUndefined();
  expect(trace.at(-1)?.elapsedMs).toBe(180_000);
  expect(trace.at(-1)?.details?.[0]?.text).toContain('"policyLimit": "duration"');
});

it("renders expandable trace evidence as escaped text with timestamps and interval labels", () => {
  const snapshot = PlannerSnapshot.make({
    conversationId: "conversation",
    messages: [],
    trips: [],
    pending: 0,
    pendingSubmissionIds: [],
    usage: {
      model: "gpt-6-astra",
      inputTokens: 120,
      outputTokens: 40,
      estimatedCostMicrousd: null,
    },
    activity: [
      {
        id: "1",
        kind: "tool",
        text: "get_trip: completed",
        timestamp: "2026-09-09T22:00:00.123Z",
        elapsedMs: 3500,
        durationMs: 500,
        durationLabel: "Request to recorded result",
        runId: "run",
        details: [{ label: "Result", text: '<script>alert("x")</script>', truncated: false }],
      },
    ],
  });

  const html = renderToStaticMarkup(
    <ActivityPanel snapshot={snapshot} progress={null} onClose={() => {}} />,
  );

  expect(html).toContain("22:00:00.123");
  expect(html).toContain("T+ 3.5 s");
  expect(html).toContain("Request to recorded result: 500 ms");
  expect(html).toContain("&lt;script&gt;");
  expect(html).not.toContain("<script>");
  expect(html).toContain("<details");
});

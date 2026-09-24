import { Effect, Schema } from "effect";
import { ThreadId, RunId, TurnId, ToolCallId } from "effect-agent/identifiers";
import { WorkerUpdate, type FrameworkMessage } from "effect-agent/worker";
import { expect, it } from "vite-plus/test";

import { publicationAuthorization } from "../src/server/security.ts";

const authority = {
  threadId: Schema.decodeSync(ThreadId)("owner"),
  runId: Schema.decodeSync(RunId)("run"),
  turnId: Schema.decodeSync(TurnId)("turn"),
  turn: 1,
};

const grant = {
  message: "Publish this trip",
  selectedTripId: "lisbon",
  publication: { tripId: "lisbon", expectedRevision: 2 },
};

const authorize = (input: unknown, parameters: unknown, frameworkMessage?: FrameworkMessage) =>
  Effect.runPromise(
    publicationAuthorization.authorize({
      ...authority,
      input,
      frameworkMessage,
      call: {
        toolCallId: Schema.decodeSync(ToolCallId)("publish"),
        toolName: "publish_trip_site",
        parameters,
        executionClass: "uncertain",
        executionKind: "ordinary",
      },
    }),
  );

it("allows only the admitted selected trip and revision, ignoring model claims of consent", async () => {
  expect(await authorize(grant, { tripId: "lisbon", expectedRevision: 2 })).toEqual({
    _tag: "allowed",
  });
  for (const [input, parameters] of [
    [
      { ...grant, publication: null },
      { tripId: "lisbon", expectedRevision: 2 },
    ],
    [grant, { tripId: "kyoto", expectedRevision: 2 }],
    [grant, { tripId: "lisbon", expectedRevision: 3 }],
  ])
    expect((await authorize(input, parameters))._tag).toBe("denied");
});

it("does not reuse a retained publication grant when a worker report starts a run", async () => {
  const message = Schema.decodeSync(WorkerUpdate)({
    _tag: "WorkerUpdate",
    schemaVersion: 1,
    worker: {
      schemaVersion: 1,
      delegationId: "research_scout",
      targetAgentId: "travel-research-scout-v4",
      threadId: "scout",
    },
    update: {
      schemaVersion: 1,
      agentId: "travel-research-scout-v4",
      threadId: "scout",
      runId: "scout-run",
      updateId: "milestone",
      sequence: 1,
      value: { summary: "Public evidence", sources: ["https://visitlisboa.com"] },
    },
  });

  expect((await authorize(grant, { tripId: "lisbon", expectedRevision: 2 }, message))._tag).toBe(
    "denied",
  );
});

import { Effect, Schema, Stream } from "effect";
import { RunId, ThreadId, ToolCallId, TurnId } from "effect-agent/identifiers";
import { RunToolAuthorization } from "effect-agent/run-options";
import { ThreadExport, ThreadStore, ThreadStoreError } from "effect-agent/thread-store";
import { WorkerUpdate } from "effect-agent/worker";
import { expect, it } from "vite-plus/test";

import { ResearchAuthorizationLive } from "../src/research/runtime.ts";

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

const report = Schema.decodeSync(WorkerUpdate)({
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

const user = {
  _tag: "UserInputRecorded",
  runId: authority.runId,
  kind: "user",
  input: grant,
};

// Even a report carrying a valid PlannerInput must not grant user authority.
const update = { ...user, kind: "steering", messageAdmission: report };

const response = {
  _tag: "ModelResponseRecorded",
  runId: authority.runId,
  turnId: authority.turnId,
  turn: authority.turn,
  messages: { content: [] },
  messagesDigest: "0".repeat(64),
};

const authorize = (
  payloads: ReadonlyArray<unknown>,
  toolName: string,
  options: { readonly unavailable?: boolean; readonly revision?: number } = {},
) => {
  const history = Schema.decodeUnknownSync(ThreadExport)({
    format: "effect-agent/thread@1",
    threadId: authority.threadId,
    tailSequence: payloads.length,
    tailDigest: "0".repeat(64),
    records: payloads.map((payload, index) => ({
      threadId: authority.threadId,
      batchId: `batch-${index}`,
      sequence: index + 1,
      offset: `offset-${index}`,
      record: {
        recordId: `record-${index}`,
        family: "thread",
        schemaVersion: 1,
        createdAt: "2026-09-14T00:00:00.000Z",
        deploymentId: "test",
        payload,
      },
    })),
  });

  const store = ThreadStore.of({
    materialize: () => Effect.die("Authorization cannot materialize"),
    append: () => Effect.die("Authorization cannot append"),
    export: () =>
      options.unavailable
        ? Effect.fail(ThreadStoreError.make({ operation: "export", message: "Unavailable" }))
        : Effect.succeed(history),
    observe: () => Stream.die("Authorization cannot observe"),
    inspectTail: () => Effect.die("Unexpected tail read"),
    read: () => Stream.die("Unexpected history read"),
  });

  return Effect.runPromise(
    Effect.flatMap(RunToolAuthorization, (authorization) =>
      authorization.authorize({
        ...authority,
        input: grant,
        call: {
          toolCallId: Schema.decodeSync(ToolCallId)("call"),
          toolName,
          parameters: { tripId: "lisbon", expectedRevision: options.revision ?? 2 },
          executionClass: "uncertain",
          executionKind: "ordinary",
        },
      }),
    ).pipe(Effect.provide(ResearchAuthorizationLive), Effect.provideService(ThreadStore, store)),
  );
};

it.each(["research_scout_start", "research_scout_follow_up", "publish_trip_site"])(
  "binds %s authority to the latest canonical input visible to that model turn",
  async (tool) => {
    for (const [records, expected] of [
      [[user, response], "allowed"],
      [[user, update, response], "denied"],
      [[user, update, user, response], "allowed"],
      [[user, response, update], "allowed"],
      [[user, update, response, user], "denied"],
      [[user, update, { ...user, runId: "another-run" }, response], "denied"],
      [[response], "denied"],
      [[user], "denied"],
      [[user, { ...response, turnId: "another-turn" }], "denied"],
      [[user, { ...response, runId: "another-run" }], "denied"],
      [[user, { ...response, turn: 2 }], "denied"],
    ] as const)
      expect((await authorize(records, tool))._tag).toBe(expected);
  },
);

it("rechecks the canonical publication grant's selected revision and fails closed on unavailable history", async () => {
  const revised = {
    ...user,
    input: { ...grant, publication: { ...grant.publication, expectedRevision: 3 } },
  };

  expect((await authorize([user, update, revised, response], "publish_trip_site"))._tag).toBe(
    "denied",
  );
  expect(
    (await authorize([user, update, revised, response], "publish_trip_site", { revision: 3 }))._tag,
  ).toBe("allowed");
  expect(
    (await authorize([user, response], "research_scout_start", { unavailable: true }))._tag,
  ).toBe("denied");
});

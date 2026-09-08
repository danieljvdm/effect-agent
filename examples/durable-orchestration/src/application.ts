import * as Messaging from "@effect-agent/capabilities/Messaging";
import * as Subagent from "@effect-agent/capabilities/Subagent";
import { MessageStatus } from "@effect-agent/core/Messaging";
import { IdempotencyKey, Receipt } from "@effect-agent/core/Receipt";
import { WorkerSummary } from "@effect-agent/core/Worker";
import { MessagingHost } from "@effect-agent/engine/MessagingHost";
import { SubagentHost } from "@effect-agent/engine/SubagentHost";
import { DurableAgentRuntime } from "@effect-agent/thread/DurableAgentRuntime";
import { ThreadExportRequest, ThreadStore } from "@effect-agent/thread/ThreadStore";
import { Effect, Schema } from "effect";

import { advisorPeer, buildA, buildB, coordinator, principal, rootThread } from "./agents.ts";

export const Command = Schema.Union([
  Schema.Struct({
    action: Schema.Literal("launch"),
    key: IdempotencyKey,
    mission: Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(160)),
  }),
  Schema.Struct({
    action: Schema.Literal("continue"),
    key: IdempotencyKey,
    note: Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(160)),
  }),
  Schema.Struct({
    action: Schema.Literal("recommend"),
    key: IdempotencyKey,
    question: Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(160)),
  }),
]);

export type Command = typeof Command.Type;
export const CommandResult = Schema.Union([Receipt, MessageStatus]);

export const Snapshot = Schema.Struct({
  threadId: Schema.String,
  inputs: Schema.Natural,
  settled: Schema.Natural,
  runs: Schema.Natural,
  reports: Schema.Array(Schema.Json),
  recommendations: Schema.Array(Schema.Json),
  workers: Schema.Array(WorkerSummary),
  depths: Schema.Array(Schema.Natural),
  outcomes: Schema.Array(Schema.Literals(["completed", "failed", "aborted"])),
});

/** Both hosts execute the same Effects; only persistence, transport and lifecycle differ. */
export const execute = Effect.fn("Orchestration.execute")(function* (command: Command) {
  const runtime = yield* DurableAgentRuntime;

  if (command.action === "recommend") {
    const host = yield* runtime.messagingHost({ sourceThreadId: rootThread, principal });

    return yield* Messaging.send(
      advisorPeer,
      { question: command.question },
      { idempotencyKey: command.key },
    ).pipe(Effect.provideService(MessagingHost, host));
  }

  return yield* runtime.submitRegistered(
    coordinator,
    command.action === "launch"
      ? { _tag: "Launch", mission: command.mission }
      : { _tag: "Continue", note: command.note },
    { threadId: rootThread, principal, idempotencyKey: command.key },
  );
});

export const snapshot = Effect.gen(function* () {
  const runtime = yield* DurableAgentRuntime;
  const store = yield* ThreadStore;
  const source = yield* store.export(ThreadExportRequest.make({ threadId: rootThread }));

  const inputs = source.records.flatMap(({ record }) =>
    record.payload._tag === "UserInputRecorded" ? [record.payload] : [],
  );

  const settlements = source.records.flatMap(({ record }) =>
    record.payload._tag === "SubmissionSettled" ? [record.payload] : [],
  );

  const reports = inputs.flatMap((value) => {
    const input = value.input;

    return typeof input === "object" && input !== null && "_tag" in input && input._tag === "Report"
      ? [input]
      : [];
  });

  const recommendations = inputs.flatMap((value) => {
    const input = value.input;

    return typeof input === "object" &&
      input !== null &&
      "_tag" in input &&
      input._tag === "Recommendation"
      ? [input]
      : [];
  });

  const host = yield* runtime.workerHost({ sourceThreadId: rootThread, principal });
  const a = yield* Subagent.list(buildA).pipe(Effect.provideService(SubagentHost, host));
  const b = yield* Subagent.list(buildB).pipe(Effect.provideService(SubagentHost, host));
  const workers = [...a.items, ...b.items];
  const depths = [0];

  for (const worker of workers) {
    const log = yield* store
      .export(ThreadExportRequest.make({ threadId: worker.worker.threadId }))
      .pipe(Effect.catchTag("ThreadNotMaterialized", () => Effect.succeed(undefined)));

    if (log === undefined) continue;
    for (const { record } of log.records) {
      if (record.payload._tag === "WorkerOriginRecorded") depths.push(record.payload.origin.depth);
      if (record.payload._tag === "SubagentRequested" && record.payload.depth !== undefined)
        depths.push(record.payload.depth);
    }
  }

  return {
    threadId: rootThread,
    inputs: inputs.length,
    settled: settlements.length,
    runs: new Set(settlements.flatMap((value) => (value.runId === undefined ? [] : [value.runId])))
      .size,
    reports,
    recommendations,
    workers,
    depths: [...new Set(depths)].sort(),
    outcomes: settlements.map((value) => value.outcome),
  };
});

/** A finite demonstration. Reusing its explicit keys safely reconnects to existing admissions. */
export const demonstration = Effect.gen(function* () {
  const runtime = yield* DurableAgentRuntime;

  const launch = yield* runtime.submitRegistered(
    coordinator,
    { _tag: "Launch", mission: "Design a small durable feature" },
    {
      threadId: rootThread,
      principal,
      idempotencyKey: Schema.decodeSync(IdempotencyKey)("launch-v1"),
    },
  );

  yield* runtime.awaitSettlement(launch);
  yield* execute({
    action: "recommend",
    key: Schema.decodeSync(IdempotencyKey)("recommend-v1"),
    question: "What should the builders prioritize?",
  });
  for (;;) {
    const state = yield* snapshot;

    if (state.reports.length >= 2 && state.workers.every((worker) => worker.state === "idle"))
      break;
    yield* Effect.sleep("25 millis");
  }
  yield* execute({
    action: "continue",
    key: Schema.decodeSync(IdempotencyKey)("continue-v1"),
    note: "Also verify the restart path",
  });
  for (;;) {
    const state = yield* snapshot;

    if (
      state.reports.length >= 3 &&
      state.recommendations.length === 1 &&
      state.inputs === state.settled &&
      state.workers.every((worker) => worker.state === "idle")
    )
      return state;
    yield* Effect.sleep("25 millis");
  }
}).pipe(Effect.timeout("60 seconds"));

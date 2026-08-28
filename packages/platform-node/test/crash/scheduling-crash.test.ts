import {
  ScheduleAuthorizer,
  ScheduleRecord,
  Scheduling,
  defaultSchedulingLimits,
} from "@effect-agent/session";
import { NodeServices } from "@effect/platform-node";
import { SqliteClient } from "@effect/sql-sqlite-node";
import { expect, it } from "@effect/vitest";
import {
  Context,
  Duration,
  Effect,
  Fiber,
  FileSystem,
  Layer,
  Option,
  Result,
  Schema,
  Stream,
} from "effect";
import { ChildProcess, type ChildProcessSpawner } from "effect/unstable/process";
import * as SqlClientService from "effect/unstable/sql/SqlClient";

import { NodeDurableHost, NodeScheduling } from "../../src/index.ts";
import {
  type SchedulingCrashBoundary,
  SchedulingCrashMarker,
  schedulingCrashAgent,
  schedulingCrashCreateOptions,
  schedulingCrashScheduleId,
  schedulingCrashScope,
} from "./scheduling-worker.ts";

class CountRow extends Schema.Class<CountRow>("SchedulingCrashTest/CountRow")({
  row_count: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
}) {}

class StoredScheduleRow extends Schema.Class<StoredScheduleRow>(
  "SchedulingCrashTest/StoredScheduleRow",
)({
  record_json: Schema.String,
}) {}

class SubmissionStateRow extends Schema.Class<SubmissionStateRow>(
  "SchedulingCrashTest/SubmissionStateRow",
)({
  state: Schema.String,
}) {}

interface ExpectedCrashState {
  readonly scheduleCount: 0 | 1;
  readonly submissionCount: 0 | 1;
  readonly pending: boolean | null;
  readonly receipt: boolean | null;
}

const cases: ReadonlyArray<
  readonly [boundary: SchedulingCrashBoundary, expected: ExpectedCrashState]
> = [
  [
    "schedule:insert:before",
    { scheduleCount: 0, submissionCount: 0, pending: null, receipt: null },
  ],
  [
    "schedule:insert:after",
    { scheduleCount: 1, submissionCount: 0, pending: false, receipt: false },
  ],
  [
    "schedule:prepare:after",
    { scheduleCount: 1, submissionCount: 0, pending: true, receipt: false },
  ],
  [
    "schedule:admission:after",
    { scheduleCount: 1, submissionCount: 1, pending: true, receipt: false },
  ],
  [
    "schedule:complete:before",
    { scheduleCount: 1, submissionCount: 1, pending: true, receipt: false },
  ],
  [
    "schedule:complete:after",
    { scheduleCount: 1, submissionCount: 1, pending: false, receipt: true },
  ],
];

const authorizerLayer = Layer.succeed(ScheduleAuthorizer)({
  manage: () => Effect.void,
  prepare: () => Effect.succeed({ policyId: "node-crash-policy", decisionId: "allowed" }),
});

const recoveryLayer = (filename: string) =>
  NodeScheduling.layer({
    limits: { ...defaultSchedulingLimits, recoveryPollMillis: 100 },
  }).pipe(
    Layer.provideMerge(
      NodeDurableHost.layerStack({
        filename,
        deploymentId: "node-scheduling-crash-deployment",
        producerId: "node-scheduling-crash-recovery",
        wakeScanInterval: 1_000,
      }),
    ),
    Layer.provide(authorizerLayer),
  );

const decodeRows = <A, I>(schema: Schema.Codec<A, I, never>, rows: ReadonlyArray<unknown>) =>
  Schema.decodeUnknownEffect(schema)(rows);

const inspectDatabase = (filename: string) =>
  Effect.scoped(
    Effect.gen(function* () {
      const sql = yield* SqlClientService.SqlClient;
      const scheduleCounts = yield* sql<Record<string, unknown>>`
        SELECT COUNT(*) AS row_count FROM effect_agent_schedules
      `;
      const submissionCounts = yield* sql<Record<string, unknown>>`
        SELECT COUNT(*) AS row_count FROM effect_agent_submissions
      `;
      const scheduleRows = yield* sql<Record<string, unknown>>`
        SELECT record_json FROM effect_agent_schedules
      `;
      const submissionRows = yield* sql<Record<string, unknown>>`
        SELECT state FROM effect_agent_submissions
      `;
      const decodedScheduleCounts = yield* decodeRows(Schema.Array(CountRow), scheduleCounts);
      const decodedSubmissionCounts = yield* decodeRows(Schema.Array(CountRow), submissionCounts);
      const decodedSchedules = yield* decodeRows(Schema.Array(StoredScheduleRow), scheduleRows);
      const decodedSubmissions = yield* decodeRows(
        Schema.Array(SubmissionStateRow),
        submissionRows,
      );
      const records = yield* Effect.forEach(decodedSchedules, (row) =>
        Schema.decodeEffect(Schema.fromJsonString(ScheduleRecord))(row.record_json),
      );
      if (decodedScheduleCounts.length !== 1 || decodedSubmissionCounts.length !== 1) {
        return yield* Effect.die("SQLite count query returned an invalid shape");
      }
      return {
        scheduleCount: decodedScheduleCounts[0].row_count,
        submissionCount: decodedSubmissionCounts[0].row_count,
        records,
        submissionStates: decodedSubmissions.map((row) => row.state),
      };
    }),
  ).pipe(Effect.provide(SqliteClient.layer({ filename })));

const awaitMarker = (
  child: ChildProcessSpawner.ChildProcessHandle,
  boundary: SchedulingCrashBoundary,
) =>
  child.stdout.pipe(
    Stream.decodeText(),
    Stream.splitLines,
    Stream.mapEffect((line) =>
      Schema.decodeUnknownEffect(Schema.fromJsonString(SchedulingCrashMarker))(line),
    ),
    Stream.runHead,
    Effect.timeout(Duration.seconds(10)),
    Effect.flatMap(
      Option.match({
        onNone: () => Effect.die(`Child did not reach ${boundary}`),
        onSome: (marker) =>
          marker.boundary === boundary
            ? Effect.succeed(marker)
            : Effect.die(`Child reached ${marker.boundary} instead of ${boundary}`),
      }),
    ),
  );

const waitForCompletion = (scheduling: Scheduling["Service"]) =>
  Effect.gen(function* () {
    while (true) {
      const snapshot = yield* scheduling.get(schedulingCrashScope, schedulingCrashScheduleId);
      if (snapshot.pending === null && snapshot.lastReceipt !== null) return snapshot;
      yield* Effect.yieldNow;
    }
  }).pipe(Effect.timeoutOption(Duration.seconds(10)));

it.live(
  "recovers every scheduling durable boundary after a real SIGKILL",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const directory = yield* fs.makeTempDirectoryScoped({
          prefix: "effect-agent-node-scheduling-crash-",
        });
        const childCwd = (yield* fs.exists("test/crash/scheduling-worker-entry.ts"))
          ? "."
          : "packages/platform-node";

        for (const [boundary, expected] of cases) {
          const filename = `${directory}/${boundary.replaceAll(":", "-")}.sqlite`;
          const child = yield* ChildProcess.make(
            process.execPath,
            ["--experimental-transform-types", "test/crash/scheduling-worker-entry.ts"],
            {
              cwd: childCwd,
              env: {
                EFFECT_AGENT_SCHEDULE_DB: filename,
                EFFECT_AGENT_SCHEDULE_BOUNDARY: boundary,
              },
              extendEnv: true,
              stdin: "ignore",
              stdout: "pipe",
              stderr: "pipe",
            },
          );
          const stderrFiber = yield* Effect.forkScoped(
            Stream.mkString(Stream.decodeText(child.stderr)),
          );
          yield* awaitMarker(child, boundary);
          yield* child.kill({ killSignal: "SIGKILL" });
          const exit = yield* child.exitCode.pipe(Effect.result);
          yield* Fiber.join(stderrFiber);
          expect({ boundary, killed: Result.isFailure(exit) }).toEqual({ boundary, killed: true });

          const crashed = yield* inspectDatabase(filename);
          const crashedRecord = crashed.records[0];
          expect({
            boundary,
            scheduleCount: crashed.scheduleCount,
            submissionCount: crashed.submissionCount,
            recordCount: crashed.records.length,
            submissionStates: crashed.submissionStates,
            pending: crashedRecord === undefined ? null : crashedRecord.pending !== null,
            receipt: crashedRecord === undefined ? null : crashedRecord.lastReceipt !== null,
          }).toEqual({
            boundary,
            scheduleCount: expected.scheduleCount,
            submissionCount: expected.submissionCount,
            recordCount: expected.scheduleCount,
            submissionStates: expected.submissionCount === 0 ? [] : ["ready"],
            pending: expected.pending,
            receipt: expected.receipt,
          });

          yield* Effect.scoped(
            Effect.gen(function* () {
              const context = yield* Layer.build(recoveryLayer(filename));
              const scheduling = Context.get(context, Scheduling);
              yield* scheduling.create(
                schedulingCrashAgent,
                { question: "survive SIGKILL" },
                schedulingCrashCreateOptions,
              );
              const completedOption = yield* waitForCompletion(scheduling);
              expect({ boundary, completed: Option.isSome(completedOption) }).toEqual({
                boundary,
                completed: true,
              });
            }),
          );

          const recovered = yield* inspectDatabase(filename);
          expect({
            boundary,
            scheduleCount: recovered.scheduleCount,
            submissionCount: recovered.submissionCount,
            submissionStateCount: recovered.submissionStates.length,
            recordCount: recovered.records.length,
            pending: recovered.records[0]?.pending === null,
            receipt: recovered.records[0]?.lastReceipt !== null,
          }).toEqual({
            boundary,
            scheduleCount: 1,
            submissionCount: 1,
            submissionStateCount: 1,
            recordCount: 1,
            pending: true,
            receipt: true,
          });
        }
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  120_000,
);

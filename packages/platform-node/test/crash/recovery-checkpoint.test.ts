import * as Agent from "@effect-agent/core/Agent";
import { NodeDurableHost } from "@effect-agent/platform-node/NodeDurableHost";
import { DurableAgentRuntime } from "@effect-agent/thread/DurableAgentRuntime";
import { LoadCheckpointRequest, ThreadStore } from "@effect-agent/thread/ThreadStore";
import { NodeFileSystem } from "@effect/platform-node";
import { expect, layer } from "@effect/vitest";
import { Effect, Layer, Option, Stream } from "effect";
import { LanguageModel, Model, type Prompt } from "effect/unstable/ai";

import {
  CHECKPOINT_HANDOFF,
  CHECKPOINT_INSTRUCTIONS,
  CRASH_QUESTION,
  checkpointContextLayer,
  checkpointDefinition,
  checkpointParts,
  decodeThreadId,
  makeCheckpointToolLayer,
  supplierCounts,
} from "./fixtures.ts";
import {
  CHILD_LEASE_MS,
  assertConvergence,
  expectKilled,
  lookupByKey,
  readLog,
  runWorkerToExit,
  waitOutChildLease,
  withCrashSite,
  withHost,
  withRuntime,
} from "./harness.ts";

const crashPoints = [
  { name: "runtime before save", killAt: "checkpoint:before-save", present: false },
  { name: "runtime after save", killAt: "checkpoint:after-save", present: true },
  { name: "SQLite before save", killAtStorage: "save-recovery-checkpoint:before", present: false },
  { name: "SQLite after save", killAtStorage: "save-recovery-checkpoint:after", present: true },
] as const;

// Each child exits without finalizers after committing the native compaction. Reopening the
// same SQLite file must treat that canonical record as authoritative with or without its cache.
layer(NodeFileSystem.layer, { excludeTestServices: true })(
  "recovery checkpoint process loss",
  (it) => {
    it.effect.each(crashPoints)(
      "recovers after $name",
      (point) =>
        withCrashSite((site) =>
          Effect.gen(function* () {
            const thread = "checkpoint-crash";
            const key = "checkpoint-crash";
            const threadId = decodeThreadId(thread);

            const child = yield* runWorkerToExit({
              db: site.db,
              scenario: "run-checkpoint",
              thread,
              key,
              supplierDir: site.supplier,
              leaseMillis: CHILD_LEASE_MS,
              ...point,
            });

            expectKilled(child);
            expect(supplierCounts(site.supplier)).toEqual({
              "checkpoint-read:checkpoint-read-1": 1,
              "checkpoint-read:checkpoint-read-2": 1,
            });

            const before = yield* withRuntime(
              site.db,
              Effect.gen(function* () {
                const store = yield* ThreadStore;
                const cache = store.recoveryCheckpoints;

                if (cache === undefined)
                  throw new Error("SQLite recovery checkpoints are required");
                const checkpoint = yield* cache.load(LoadCheckpointRequest.make({ threadId }));
                const records = yield* readLog(thread);
                const snapshot = yield* lookupByKey(thread, key);

                const compactions = records.filter(
                  ({ record }) => record.payload._tag === "CompactionCreated",
                );

                expect(Option.isSome(checkpoint)).toBe(point.present);
                expect(compactions).toHaveLength(1);
                expect(JSON.stringify(compactions)).toContain(CHECKPOINT_HANDOFF);
                expect(
                  records.filter(({ record }) => record.payload._tag === "ModelResponseRecorded"),
                ).toHaveLength(2);
                expect(
                  records.filter(({ record }) => record.payload._tag === "ToolCallSettled"),
                ).toHaveLength(2);

                return { records, submissionId: snapshot.submissionId };
              }),
            );

            yield* waitOutChildLease;
            yield* withHost(
              site.db,
              Effect.gen(function* () {
                const host = yield* NodeDurableHost;
                const runtime = yield* DurableAgentRuntime;

                const report = host.startupRecovery.find(
                  (entry) => entry.submissionId === before.submissionId,
                );

                expect(report?.decision._tag).toBe("ResumeFromTurnBoundary");
                expect(report?.disposition).toBe("deferred");
                const requests: Array<Prompt.Prompt> = [];

                const model = Model.make(
                  "scripted",
                  "crash-harness",
                  Layer.effect(
                    LanguageModel.LanguageModel,
                    LanguageModel.make({
                      generateText: () => Effect.succeed([]),
                      streamText: (request) => {
                        requests.push(request.prompt);

                        return Stream.fromIterable(checkpointParts(requests.length + 2));
                      },
                    }),
                  ),
                );

                const settlements = yield* runtime
                  .processThread(Agent.withModel(checkpointDefinition, model), threadId)
                  .pipe(Effect.provide(makeCheckpointToolLayer(site.supplier)));

                expect(settlements).toHaveLength(1);
                expect(settlements[0]?.outcome).toBe("completed");
                expect(settlements[0]?.usageSummary?.modelCalls).toBe(4);
                expect(settlements[0]?.usageSummary?.inputTokens.total).toBe(400);
                expect(settlements[0]?.usageSummary?.outputTokens.total).toBe(40);
                expect(requests).toHaveLength(2);
                for (const request of requests) {
                  const prompt = JSON.stringify(request);

                  expect(prompt).toContain(CRASH_QUESTION);
                  expect(prompt).toContain(CHECKPOINT_INSTRUCTIONS);
                  expect(prompt).toContain(CHECKPOINT_HANDOFF);
                  expect(prompt).not.toContain('"id":"checkpoint-call-1"');
                  expect(prompt).not.toContain('"id":"checkpoint-call-2"');
                }
                const first = JSON.stringify(requests[0]);

                expect(first).toContain("turn 3/4");
                expect(first).toContain("tool-calls 2/3");
                expect(first).toContain("tokens 220/");
                const last = JSON.stringify(requests[1]);

                expect(last).toContain("turn 4/4");
                expect(last).toContain("tool-calls 3/3");
                expect(last).toContain("tokens 330/");

                const records = yield* readLog(thread);

                expect(records.slice(0, before.records.length)).toEqual(before.records);
                expect(
                  records.filter(({ record }) => record.payload._tag === "RunStarted"),
                ).toHaveLength(1);
                expect(
                  records.filter(({ record }) => record.payload._tag === "CompactionCreated"),
                ).toHaveLength(1);
                expect(
                  records.filter(({ record }) => record.payload._tag === "ModelResponseRecorded"),
                ).toHaveLength(4);
                expect(
                  records.filter(({ record }) => record.payload._tag === "ToolCallSettled"),
                ).toHaveLength(3);
                yield* assertConvergence(thread, [before.submissionId], {
                  site,
                  counts: {
                    "checkpoint-read:checkpoint-read-1": 1,
                    "checkpoint-read:checkpoint-read-2": 1,
                    "checkpoint-read:checkpoint-read-3": 1,
                  },
                });
              }),
              { runContext: checkpointContextLayer },
            );
          }),
        ),
      30_000,
    );
  },
);

import * as Subagent from "@effect-agent/capabilities/Subagent";
import { SubagentHost } from "@effect-agent/engine/SubagentHost";
import { DurableAgentRuntime } from "@effect-agent/thread/DurableAgentRuntime";
import { MessageDeliveryStore } from "@effect-agent/thread/MessageDelivery";
import { ThreadExportRequest, ThreadStore } from "@effect-agent/thread/ThreadStore";
import { runInDurableObject } from "cloudflare:test";
import { Effect } from "effect";
import { DurableObject } from "effect-cf";
import { expect, it } from "vite-plus/test";

import { CloudflareThreadClient } from "../src/CloudflareThreadClient.ts";
import {
  backgroundSource,
  backgroundWorkers,
  backgroundWakeDropPrefixes,
  backgroundReportSource,
  backgroundReportProjections,
  backgroundReportingWorkers,
  backgroundReportGates,
} from "./background-worker-fixture.ts";
import {
  decodeIdempotencyKey,
  decodeThreadId,
  TEST_PRINCIPAL,
  submitOptions,
  armRuntimeEviction,
  armedEvictionsRemaining,
} from "./fixtures.ts";
import { allSettled, drainAlarmsUntil, runClient, stubFor, readCanonical } from "./harness.ts";
import { droppedMessageWakes } from "./message-delivery-fixture.ts";

const evict = async (thread: string) => {
  await runInDurableObject(stubFor(thread), (_instance, state) => {
    state.abort("background worker test eviction");
  }).catch(() => undefined);
};

const withOwner = <A, E>(
  source: string,
  use: (host: SubagentHost["Service"]) => Effect.Effect<A, E>,
) =>
  runInDurableObject(stubFor(source), (instance) =>
    instance[DurableObject.RunSymbol](
      Effect.gen(function* () {
        const runtime = yield* DurableAgentRuntime;

        const host = yield* runtime.workerHost({
          sourceThreadId: decodeThreadId(source),
          principal: TEST_PRINCIPAL,
        });

        return yield* use(host);
      }),
    ),
  );

it("reopens a background worker and admits follow-up input across Objects after its source has settled", async () => {
  const source = `background-cf-${crypto.randomUUID()}`;

  const sourceReceipt = await runClient(
    Effect.flatMap(CloudflareThreadClient, (client) =>
      client.submit(
        { definition: backgroundSource },
        { question: "initialize" },
        submitOptions(source, "source"),
      ),
    ),
  );

  await drainAlarmsUntil(source, allSettled(source));
  backgroundWakeDropPrefixes.add("worker:");
  droppedMessageWakes.add(source);

  const started = await withOwner(source, (host) =>
    Subagent.start(
      backgroundWorkers,
      { question: "first" },
      { idempotencyKey: decodeIdempotencyKey("first") },
    ).pipe(Effect.provideService(SubagentHost, host)),
  );

  droppedMessageWakes.add(source);
  droppedMessageWakes.add(started.worker.threadId);
  try {
    expect(
      await withOwner(source, (host) =>
        Subagent.start(
          backgroundWorkers,
          { question: "first" },
          { idempotencyKey: decodeIdempotencyKey("first") },
        ).pipe(Effect.provideService(SubagentHost, host)),
      ),
    ).toEqual(started);
    await drainAlarmsUntil(started.worker.threadId, allSettled(started.worker.threadId));
    await drainAlarmsUntil(source, async () => {
      const result = await withOwner(source, (host) =>
        Subagent.inspect(backgroundWorkers, started.worker, started.receipt).pipe(
          Effect.provideService(SubagentHost, host),
        ),
      );

      return result._tag === "Settled";
    });
    await evict(source);
    await evict(started.worker.threadId);

    const next = await withOwner(source, (host) =>
      Subagent.followUp(
        backgroundWorkers,
        started.worker,
        { question: "second" },
        { idempotencyKey: decodeIdempotencyKey("second") },
      ).pipe(Effect.provideService(SubagentHost, host)),
    );

    expect(next.threadId).toBe(started.worker.threadId);
    expect(next.submissionId).not.toBe(started.receipt.submissionId);
    await drainAlarmsUntil(started.worker.threadId, allSettled(started.worker.threadId));

    const outcome = await withOwner(source, (host) =>
      Subagent.inspect(backgroundWorkers, started.worker, next).pipe(
        Effect.provideService(SubagentHost, host),
      ),
    );

    expect(outcome).toMatchObject({
      _tag: "Settled",
      outcome: "completed",
      result: { answer: "done" },
    });

    const sourceLog = await runInDurableObject(stubFor(source), (instance) =>
      instance[DurableObject.RunSymbol](
        Effect.flatMap(ThreadStore, (store) =>
          store.export(ThreadExportRequest.make({ threadId: decodeThreadId(source) })),
        ),
      ),
    );

    expect(
      sourceLog.records.filter(({ record }) => record.payload._tag === "WorkerInputCompleted"),
    ).toHaveLength(2);
    expect(
      sourceLog.records
        .filter(({ record }) => record.payload._tag === "SubmissionSettled")
        .map(({ record }) => record.payload),
    ).toMatchObject([{ submissionId: sourceReceipt.submissionId, outcome: "completed" }]);
    await drainAlarmsUntil(source, async () => {
      const deliveries = await runInDurableObject(stubFor(source), (instance) =>
        instance[DurableObject.RunSymbol](
          Effect.flatMap(MessageDeliveryStore, (store) =>
            store.list({ ownerThreadId: decodeThreadId(source), limit: 100 }),
          ),
        ),
      );

      return (
        deliveries.items.length === 2 && deliveries.items.every((row) => row.status === "processed")
      );
    });
  } finally {
    backgroundWakeDropPrefixes.delete("worker:");
    droppedMessageWakes.delete(source);
    droppedMessageWakes.delete(started.worker.threadId);
  }
}, 20_000);

it("delivers one frozen report after child eviction and source eviction with all wake hints dropped", async () => {
  const source = `background-cf-report-${crypto.randomUUID()}`;

  await runClient(
    Effect.flatMap(CloudflareThreadClient, (client) =>
      client.submit(
        { definition: backgroundReportSource },
        { question: "launch complete" },
        submitOptions(source, "source"),
      ),
    ),
  );
  await drainAlarmsUntil(source, allSettled(source));
  backgroundWakeDropPrefixes.add("worker:");
  droppedMessageWakes.add(source);

  const started = await withOwner(source, (host) =>
    Subagent.start(
      backgroundReportingWorkers,
      { question: source },
      { idempotencyKey: decodeIdempotencyKey("first") },
    ).pipe(Effect.provideService(SubagentHost, host)),
  );

  droppedMessageWakes.add(started.worker.threadId);
  try {
    await withOwner(source, (host) =>
      Subagent.followUp(
        backgroundReportingWorkers,
        started.worker,
        { question: "joined input" },
        { idempotencyKey: decodeIdempotencyKey("joined") },
      ).pipe(Effect.provideService(SubagentHost, host)),
    );
    armRuntimeEviction(started.worker.threadId, "worker:after-report-append");
    backgroundReportGates.add(source);
    await evict(source);
    await drainAlarmsUntil(started.worker.threadId, async () => {
      const records = await readCanonical(started.worker.threadId);

      return (
        records.some(({ record }) => record.payload._tag === "WorkerReportPrepared") &&
        armedEvictionsRemaining(started.worker.threadId) === 0
      );
    });
    await evict(started.worker.threadId);
    await drainAlarmsUntil(started.worker.threadId, async () => {
      const rows = await runInDurableObject(stubFor(started.worker.threadId), (instance) =>
        instance[DurableObject.RunSymbol](
          Effect.flatMap(MessageDeliveryStore, (store) =>
            store.list({ ownerThreadId: started.worker.threadId, limit: 100 }),
          ),
        ),
      );

      return rows.items.length === 1 && rows.items[0]?.status !== "pending";
    });
    await drainAlarmsUntil(source, async () => {
      const records = await readCanonical(source);

      return (
        records.filter(({ record }) => record.payload._tag === "SubmissionSettled").length === 2
      );
    });
    await drainAlarmsUntil(started.worker.threadId, async () => {
      const rows = await runInDurableObject(stubFor(started.worker.threadId), (instance) =>
        instance[DurableObject.RunSymbol](
          Effect.flatMap(MessageDeliveryStore, (store) =>
            store.list({ ownerThreadId: started.worker.threadId, limit: 100 }),
          ),
        ),
      );

      return rows.items.length === 1 && rows.items[0]?.status === "processed";
    });
    const child = await readCanonical(started.worker.threadId);

    const reports = child.flatMap(({ record }) =>
      record.payload._tag === "WorkerReportPrepared" ? [record.payload] : [],
    );

    expect(reports).toHaveLength(1);
    const report = reports[0];

    if (report === undefined) throw new Error("Expected frozen report");
    expect(backgroundReportProjections.get(report.runId)).toBe(1);

    const childSettlements = child.flatMap(({ record }) =>
      record.payload._tag === "SubmissionSettled" ? [record.payload] : [],
    );

    expect(childSettlements).toHaveLength(2);
    expect(childSettlements.map((row) => row.runId)).toEqual([report.runId, report.runId]);
    const sourceRecords = await readCanonical(source);

    const inputs = sourceRecords.flatMap(({ record }) =>
      record.payload._tag === "UserInputRecorded" ? [record.payload] : [],
    );

    expect(inputs).toHaveLength(2);
    expect(inputs[1]?.input).toEqual({ question: `report:${report.runId}:done` });
    expect(inputs[1]?.runId).not.toBe(inputs[0]?.runId);
    expect(
      sourceRecords.flatMap(({ record }) =>
        record.payload._tag === "SubmissionSettled" ? [record.payload.outcome] : [],
      ),
    ).toEqual(["completed", "completed"]);
  } finally {
    backgroundReportGates.delete(source);
    backgroundWakeDropPrefixes.delete("worker:");
    droppedMessageWakes.delete(source);
    droppedMessageWakes.delete(started.worker.threadId);
  }
}, 20_000);

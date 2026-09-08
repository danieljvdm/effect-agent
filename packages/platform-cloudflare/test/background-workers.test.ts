import * as Subagent from "@effect-agent/capabilities/Subagent";
import { SubagentHost } from "@effect-agent/engine/SubagentHost";
import { DurableAgentRuntime } from "@effect-agent/thread/DurableAgentRuntime";
import { MessageDeliveryStore } from "@effect-agent/thread/MessageDelivery";
import { ThreadExportRequest, ThreadStore } from "@effect-agent/thread/ThreadStore";
import { runDurableObjectAlarm, runInDurableObject } from "cloudflare:test";
import { Effect } from "effect";
import { DurableObject } from "effect-cf";
import { expect, it } from "vite-plus/test";

import { CloudflareThreadClient } from "../src/CloudflareThreadClient.ts";
import {
  backgroundSource,
  independentBudgetSource,
  independentBudgetWorkers,
  independentBudgetGrants,
  independentBudgetAdmissionOutages,
  independentBudgetAuthorityCalls,
  independentBudgetGates,
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

// Regression: https://github.com/danieljvdm/effect-agent/pull/358
it("funds native persona Runs independently while joins, eviction and scouts retain one allowance", async () => {
  const source = `background-cf-independent-${crypto.randomUUID()}`;

  await runClient(
    Effect.flatMap(CloudflareThreadClient, (client) =>
      client.submit(
        { definition: independentBudgetSource },
        { question: "initialize" },
        submitOptions(source, "source"),
      ),
    ),
  );
  await drainAlarmsUntil(source, allSettled(source));

  const launch = () =>
    withOwner(source, (host) =>
      Subagent.start(
        independentBudgetWorkers,
        { question: `${source}:task:1` },
        { idempotencyKey: decodeIdempotencyKey("first"), budgetScope: "worker-run" },
      ).pipe(Effect.provideService(SubagentHost, host)),
    );

  await expect(launch()).rejects.toThrow();
  independentBudgetGrants.add(source);
  backgroundWakeDropPrefixes.add("worker:");
  droppedMessageWakes.add(source);
  const started = await launch();

  droppedMessageWakes.add(started.worker.threadId);

  const finish = (thread = started.worker.threadId) =>
    drainAlarmsUntil(thread, async () => {
      const records = await readCanonical(thread);

      for (const { record } of records) {
        if (record.payload._tag === "SubagentRequested") {
          await drainAlarmsUntil(
            record.payload.childThreadId,
            allSettled(record.payload.childThreadId),
          );
        }
      }

      return allSettled(thread)();
    });

  const sibling = await withOwner(source, (host) =>
    Subagent.start(
      independentBudgetWorkers,
      { question: `${source}:task:4` },
      { idempotencyKey: decodeIdempotencyKey("sibling"), budgetScope: "worker-run" },
    ).pipe(Effect.provideService(SubagentHost, host)),
  );

  droppedMessageWakes.add(sibling.worker.threadId);
  try {
    expect(await launch()).toEqual(started);
    const firstAlarm = runDurableObjectAlarm(stubFor(started.worker.threadId)).catch(() => false);

    await expect
      .poll(async () =>
        (await readCanonical(started.worker.threadId)).some(
          ({ record }) => record.payload._tag === "RunStarted",
        ),
      )
      .toBe(true);
    const siblingAlarm = runDurableObjectAlarm(stubFor(sibling.worker.threadId)).catch(() => false);

    await expect
      .poll(async () =>
        (await readCanonical(sibling.worker.threadId)).some(
          ({ record }) => record.payload._tag === "RunStarted",
        ),
      )
      .toBe(true);
    await expect(
      withOwner(source, (host) =>
        Subagent.start(
          independentBudgetWorkers,
          { question: `${source}:task:5` },
          { idempotencyKey: decodeIdempotencyKey("third-active"), budgetScope: "worker-run" },
        ).pipe(Effect.provideService(SubagentHost, host)),
      ),
    ).rejects.toThrow();
    // The root has toolConcurrency=1, but two persona Runs are live and root work remains runnable.
    await runClient(
      Effect.flatMap(CloudflareThreadClient, (client) =>
        client.submit(
          { definition: independentBudgetSource },
          { question: "responsive root" },
          submitOptions(source, "responsive"),
        ),
      ),
    );
    await drainAlarmsUntil(source, allSettled(source));

    const joined = await withOwner(source, (host) =>
      Subagent.followUp(
        independentBudgetWorkers,
        started.worker,
        { question: "continue the active task" },
        { idempotencyKey: decodeIdempotencyKey("joined") },
      ).pipe(Effect.provideService(SubagentHost, host)),
    );

    armRuntimeEviction(started.worker.threadId, "turn:after-response-append");
    independentBudgetGates.add(`${source}:task:1`);
    await firstAlarm;
    await finish();
    expect(armedEvictionsRemaining(started.worker.threadId)).toBe(0);
    const firstLog = await readCanonical(started.worker.threadId);

    const starts = firstLog.flatMap(({ record }) =>
      record.payload._tag === "RunStarted" ? [record.payload] : [],
    );

    expect(starts).toHaveLength(1);

    const settled = firstLog.flatMap(({ record }) =>
      record.payload._tag === "SubmissionSettled" ? [record.payload] : [],
    );

    expect(settled).toHaveLength(2);
    expect(settled.map((row) => row.runId)).toEqual([starts[0]?.runId, starts[0]?.runId]);
    expect(settled.map((row) => row.outcome)).toEqual(["completed", "completed"]);
    expect(settled.map((row) => row.submissionId)).toContain(joined.submissionId);
    for (const round of [2, 3]) {
      await evict(source);
      await evict(started.worker.threadId);
      independentBudgetGates.add(`${source}:task:${round}`);

      const next = await withOwner(source, (host) =>
        Subagent.followUp(
          independentBudgetWorkers,
          started.worker,
          { question: `${source}:task:${round}` },
          { idempotencyKey: decodeIdempotencyKey(`later-${round}`) },
        ).pipe(Effect.provideService(SubagentHost, host)),
      );

      expect(next.threadId).toBe(started.worker.threadId);
      expect(next.submissionId).not.toBe(started.receipt.submissionId);
      await finish();
    }
    independentBudgetGates.add(`${source}:task:4`);
    await siblingAlarm;
    await finish(sibling.worker.threadId);
    const siblingLog = await readCanonical(sibling.worker.threadId);

    expect(siblingLog.filter(({ record }) => record.payload._tag === "RunStarted")).toHaveLength(1);
    const workerLog = await readCanonical(started.worker.threadId);

    const origins = workerLog.flatMap(({ record }) =>
      record.payload._tag === "WorkerOriginRecorded" ? [record.payload.origin] : [],
    );

    expect(origins).toHaveLength(1);
    expect(origins[0]).toMatchObject({
      budgetScope: "worker-run",
      depth: 1,
      source: { threadId: source },
    });
    expect(origins[0]?.policy.tokenBudget).toBeUndefined();
    expect(origins[0]?.policy.costBudgetMicrousd).toBeUndefined();

    const runs = workerLog.flatMap(({ record }) =>
      record.payload._tag === "RunStarted" ? [record.payload.runId] : [],
    );

    expect(new Set(runs).size).toBe(3);

    const scouts = workerLog.flatMap(({ record }) =>
      record.payload._tag === "SubagentRequested" ? [record.payload] : [],
    );

    expect(scouts).toHaveLength(3);
    expect(new Set(scouts.map((row) => row.runId)).size).toBe(3);
    expect(scouts.map((row) => row.depth)).toEqual([2, 2, 2]);
    expect(scouts.map((row) => row.budget?.caps)).toEqual(
      Array.from({ length: 3 }, () =>
        expect.objectContaining({ maxInputTokens: 20, maxOutputTokens: 30, maxCostMicrousd: 2 }),
      ),
    );
    expect(scouts.map((row) => row.policy?.tokenBudget)).toEqual([50, 50, 50]);
    expect(scouts.map((row) => row.policy?.costBudgetMicrousd)).toEqual([2, 2, 2]);

    const reservations = workerLog.flatMap(({ record }) =>
      record.payload._tag === "SubtreeBudgetReserved" ? [record.payload] : [],
    );

    expect(reservations).toHaveLength(3);
    expect(new Set(reservations.map((row) => row.sourceSubmissionId)).size).toBe(3);
    for (const scout of scouts) {
      const child = await readCanonical(scout.childThreadId);

      expect(
        child.flatMap(({ record }) =>
          record.payload._tag === "SubagentLineageRecorded"
            ? [record.payload.parentLink.depth]
            : [],
        ),
      ).toEqual([2]);
    }
    const rootLog = await readCanonical(source);

    expect(
      rootLog.filter(({ record }) => record.payload._tag === "SubtreeBudgetReserved"),
    ).toHaveLength(0);
    expect(
      rootLog.filter(({ record }) => record.payload._tag === "WorkerInputRequested"),
    ).toHaveLength(5);
    expect(
      rootLog.filter(({ record }) => record.payload._tag === "SubmissionSettled"),
    ).toHaveLength(2);
  } finally {
    independentBudgetGrants.delete(source);
    independentBudgetAuthorityCalls.delete(source);
    for (const round of [1, 2, 3, 4]) independentBudgetGates.delete(`${source}:task:${round}`);
    droppedMessageWakes.delete(sibling.worker.threadId);
    backgroundWakeDropPrefixes.delete("worker:");
    droppedMessageWakes.delete(source);
    droppedMessageWakes.delete(started.worker.threadId);
  }
}, 30_000);

// Regression: https://github.com/danieljvdm/effect-agent/pull/358
it("retries unavailable worker funding admission with the same durable input identity", async () => {
  const source = `background-cf-independent-outage-${crypto.randomUUID()}`;

  await runClient(
    Effect.flatMap(CloudflareThreadClient, (client) =>
      client.submit(
        { definition: independentBudgetSource },
        { question: "initialize" },
        submitOptions(source, "source"),
      ),
    ),
  );
  await drainAlarmsUntil(source, allSettled(source));
  independentBudgetGrants.add(source);
  independentBudgetAdmissionOutages.add(source);
  droppedMessageWakes.add(source);

  const launch = () =>
    withOwner(source, (host) =>
      Subagent.start(
        backgroundWorkers,
        { question: "recover the accepted intent" },
        { idempotencyKey: decodeIdempotencyKey("outage"), budgetScope: "worker-run" },
      ).pipe(Effect.provideService(SubagentHost, host)),
    );

  const deliveries = () =>
    runInDurableObject(stubFor(source), (instance) =>
      instance[DurableObject.RunSymbol](
        Effect.flatMap(MessageDeliveryStore, (store) =>
          store.list({ ownerThreadId: decodeThreadId(source), limit: 100 }),
        ),
      ),
    );

  try {
    await expect(launch()).rejects.toThrow();
    const pending = await deliveries();

    expect(pending.items).toHaveLength(1);
    const retained = pending.items[0];

    if (retained === undefined) throw new Error("Expected retained worker input");
    expect(retained.status).toBe("pending");
    expect(retained.receipt).toBeNull();
    independentBudgetAdmissionOutages.delete(source);
    await drainAlarmsUntil(source, async () => {
      const receipt = (await deliveries()).items[0]?.receipt;

      return receipt !== undefined && receipt !== null;
    });
    const recovered = await launch();

    expect(recovered.worker).toEqual(retained.envelope.workerAdmission?.origin.worker);
    const accepted = await deliveries();

    expect(accepted.items).toHaveLength(1);
    expect(accepted.items[0]?.key).toEqual(retained.key);
    expect(accepted.items[0]?.receipt).toEqual(recovered.receipt);
    expect(await launch()).toEqual(recovered);
    await drainAlarmsUntil(recovered.worker.threadId, allSettled(recovered.worker.threadId));
    const log = await readCanonical(recovered.worker.threadId);

    expect(log.filter(({ record }) => record.payload._tag === "RunStarted")).toHaveLength(1);
    const sourceLog = await readCanonical(source);

    expect(
      sourceLog.filter(({ record }) => record.payload._tag === "WorkerInputRequested"),
    ).toHaveLength(1);
  } finally {
    independentBudgetGrants.delete(source);
    independentBudgetAdmissionOutages.delete(source);
    independentBudgetAuthorityCalls.delete(source);
    droppedMessageWakes.delete(source);
  }
}, 20_000);

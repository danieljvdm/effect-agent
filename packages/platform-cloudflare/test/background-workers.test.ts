import { runDurableObjectAlarm, runInDurableObject } from "cloudflare:test";
import { Effect, Schema } from "effect";
import { AgentPolicy } from "effect-agent/agent-policy";
import { DurableAgentRuntime } from "effect-agent/durable-agent-runtime";
import type { SubmissionId } from "effect-agent/identifiers";
import { MessageDeliveryStore } from "effect-agent/message-delivery";
import { MessageAdmission, type MessageStatus } from "effect-agent/messaging";
import * as Subagent from "effect-agent/subagent";
import { SubagentHost } from "effect-agent/subagent-host";
import { ThreadExportRequest, ThreadStore } from "effect-agent/thread-store";
import { WorkerCompletion, WorkerUpdate } from "effect-agent/worker";
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
  backgroundStandardReportSource,
  backgroundReportProjections,
  backgroundReportingWorkers,
  backgroundReportGates,
  capturedPolicySource,
  capturedPolicyWorkers,
  capturedConcurrency,
  privateProgressRoutes,
  customRuntimeThreads,
  backgroundUpdateStarts,
  backgroundUpdatePrompts,
  backgroundUpdateSource,
  backgroundUpdateWorkers,
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
import {
  armWorkerInputContention,
  workerInputContentions,
} from "./helpers/worker-input-contention.ts";
import { droppedMessageWakes } from "./message-delivery-fixture.ts";

const evict = async (thread: string) => {
  await runInDurableObject(stubFor(thread), (_instance, state) => {
    state.abort("background worker test eviction");
  }).catch(() => undefined);
};

const withOwner = <A, E>(
  source: string,
  use: (host: SubagentHost["Service"]) => Effect.Effect<A, E>,
  sourceSubmissionId?: SubmissionId,
) =>
  runInDurableObject(stubFor(source), (instance) =>
    instance[DurableObject.RunSymbol](
      Effect.gen(function* () {
        const runtime = yield* DurableAgentRuntime;

        const host = yield* runtime.workerHost({
          sourceThreadId: decodeThreadId(source),
          principal: TEST_PRINCIPAL,
          ...(sourceSubmissionId === undefined ? {} : { sourceSubmissionId }),
        });

        return yield* use(host);
      }),
    ),
  );

it("delivers an accepted worker update before completion after eviction with only alarms and no wake hints", async () => {
  const source = `background-cf-update-${crypto.randomUUID()}`;

  const sourceReceipt = await runClient(
    Effect.flatMap(CloudflareThreadClient, (client) =>
      client.submit(
        { definition: backgroundUpdateSource },
        { question: source },
        submitOptions(source, "source"),
      ),
    ),
  );

  await drainAlarmsUntil(source, allSettled(source));
  backgroundWakeDropPrefixes.add("worker:");
  droppedMessageWakes.add(source);

  const started = await withOwner(
    source,
    (host) =>
      Subagent.start(
        backgroundUpdateWorkers,
        { question: source },
        { idempotencyKey: decodeIdempotencyKey("hotel") },
      ).pipe(Effect.provideService(SubagentHost, host)),
    sourceReceipt.submissionId,
  );

  droppedMessageWakes.add(started.worker.threadId);

  const deliveries = () =>
    runInDurableObject(stubFor(started.worker.threadId), (instance) =>
      instance[DurableObject.RunSymbol](
        Effect.flatMap(MessageDeliveryStore, (store) =>
          store.list({ ownerThreadId: started.worker.threadId, limit: 100 }),
        ),
      ),
    );

  try {
    armRuntimeEviction(started.worker.threadId, "update:after-canonical-append");
    await evict(source);
    backgroundUpdateStarts.add(source);
    await drainAlarmsUntil(
      started.worker.threadId,
      async () => armedEvictionsRemaining(started.worker.threadId) === 0,
    );
    expect(armedEvictionsRemaining(started.worker.threadId)).toBe(0);
    const interrupted = await readCanonical(started.worker.threadId);

    const accepted = interrupted.flatMap(({ record }) =>
      record.payload._tag === "AgentUpdateEmitted" ? [record.payload.update] : [],
    );

    expect(accepted).toHaveLength(1);
    expect(interrupted.filter(({ record }) => record.payload._tag === "RunCompleted")).toHaveLength(
      0,
    );

    // The post-append failpoint already evicted the child. A second uncontrolled
    // eviction could instead kill recovery's newly acquired delivery lease.
    const recoveredAlarm = runDurableObjectAlarm(stubFor(started.worker.threadId)).catch(
      () => false,
    );

    await expect
      .poll(async () => (await deliveries()).items.some((row) => row.receipt !== null))
      .toBe(true);
    await drainAlarmsUntil(
      source,
      async () =>
        (await readCanonical(source)).filter(
          ({ record }) => record.payload._tag === "SubmissionSettled",
        ).length === 2,
    );
    const parent = await readCanonical(source);

    const inputs = parent.flatMap(({ record }) =>
      record.payload._tag === "UserInputRecorded" ? [record.payload] : [],
    );

    const update = Schema.decodeUnknownSync(WorkerUpdate)(inputs[1]?.messageAdmission);

    expect(update.worker).toEqual(started.worker);
    expect(update.update).toEqual(accepted[0]);
    expect(update.update.value).toEqual({ _tag: "AreaConcern", area: "Rosebank" });
    expect(inputs[1]?.runId).not.toBe(inputs[0]?.runId);
    expect(
      parent.flatMap(({ record }) =>
        record.payload._tag === "SubmissionSettled" ? [record.payload] : [],
      ),
    ).toMatchObject([{ outcome: "completed" }, { outcome: "completed" }]);
    expect(backgroundUpdatePrompts.at(-1)).toContain("AreaConcern");
    expect(backgroundUpdatePrompts.at(-1)).toContain(started.worker.threadId);
    expect(
      (await readCanonical(started.worker.threadId)).filter(
        ({ record }) => record.payload._tag === "RunCompleted",
      ),
    ).toHaveLength(0);
    expect((await deliveries()).items).toHaveLength(1);
    await recoveredAlarm;
    await expect
      .poll(
        async () =>
          (await readCanonical(started.worker.threadId)).filter(
            ({ record }) => record.payload._tag === "ToolCallUnknown",
          ).length,
      )
      .toBe(1);
    // The native update call lost its acknowledgement; ordinary tool recovery must not replay it.
    await withOwner(source, (host) =>
      Subagent.cancel(backgroundUpdateWorkers, started.worker, started.delivery.receipt!).pipe(
        Effect.provideService(SubagentHost, host),
      ),
    );
    await drainAlarmsUntil(started.worker.threadId, allSettled(started.worker.threadId));
    await drainAlarmsUntil(
      started.worker.threadId,
      async () =>
        (await deliveries()).items.length === 2 &&
        (await deliveries()).items.every((row) => row.receipt !== null),
    );
    await drainAlarmsUntil(
      source,
      async () =>
        (await readCanonical(source)).filter(
          ({ record }) => record.payload._tag === "SubmissionSettled",
        ).length === 3,
    );
    const completed = await readCanonical(source);

    const completions = completed.flatMap(({ record }) =>
      record.payload._tag === "UserInputRecorded" &&
      Schema.is(WorkerCompletion)(record.payload.messageAdmission)
        ? [record.payload.messageAdmission]
        : [],
    );

    expect(completions).toHaveLength(1);
    expect(completions[0]?.report.outcome).toBe("aborted");
    const child = await readCanonical(started.worker.threadId);

    expect(child.filter(({ record }) => record.payload._tag === "AgentUpdateEmitted")).toHaveLength(
      1,
    );
    expect(
      child.filter(({ record }) => record.payload._tag === "WorkerReportPrepared"),
    ).toHaveLength(1);
  } finally {
    backgroundUpdateStarts.delete(source);
    backgroundUpdatePrompts.length = 0;
    backgroundWakeDropPrefixes.delete("worker:");
    droppedMessageWakes.delete(source);
    droppedMessageWakes.delete(started.worker.threadId);
  }
}, 20_000);

// Regression: https://github.com/danieljvdm/effect-agent/commit/43882d187248665eaf7fd46950b3bc617edcb73d
// Multiple native evictions and scout Runs need the same budget as the adjacent lifecycle tests.
it("admits exact captured policies with one registered target and retains them through native eviction, joins and scouts", async () => {
  const source = `background-cf-independent-captured-${crypto.randomUUID()}`;

  const sourcePolicy = AgentPolicy.make({
    maxTurns: 9,
    maxToolCalls: 8,
    maxDuration: "30 seconds",
    toolConcurrency: 1,
  });

  const ownerReceipt = await runClient(
    Effect.flatMap(CloudflareThreadClient, (client) =>
      client.submit(
        { definition: capturedPolicySource },
        { question: "initialize", policy: sourcePolicy },
        submitOptions(source, "source"),
      ),
    ),
  );

  await drainAlarmsUntil(source, allSettled(source));
  // Read controls do not require an arbitrary owner input. Resolving a captured policy does.
  expect(
    await withOwner(source, (host) =>
      host.list({
        delegationId: independentBudgetWorkers.delegationId,
        target: independentBudgetWorkers.target,
        limit: 10,
      }),
    ),
  ).toEqual({ items: [], next: null });
  await expect(withOwner(source, (host) => host.context)).rejects.toThrow();
  const context = await withOwner(source, (host) => host.context, ownerReceipt.submissionId);

  expect(context.policy).toEqual(sourcePolicy);

  const firstPolicy = AgentPolicy.make({
    maxTurns: 4,
    maxToolCalls: 3,
    maxDuration: "30 seconds",
    toolConcurrency: 1,
    toolResultBounds: { maxBytes: 1024 },
  });

  const secondPolicy = AgentPolicy.make({ ...firstPolicy, maxTurns: 7, maxToolCalls: 5 });
  const firstDeclaration = capturedPolicyWorkers(firstPolicy);
  const secondDeclaration = capturedPolicyWorkers(secondPolicy);

  expect(firstDeclaration.target).toBe(secondDeclaration.target);
  independentBudgetGrants.add(source);
  capturedConcurrency.set(source, { owner: ownerReceipt.submissionId, limit: 2 });
  backgroundWakeDropPrefixes.add("worker:");
  droppedMessageWakes.add(source);

  const launch = (policy: AgentPolicy, task: number) =>
    withOwner(
      source,
      (host) =>
        Subagent.start(
          capturedPolicyWorkers(policy),
          { question: `${source}:task:${task}`, policy },
          { idempotencyKey: decodeIdempotencyKey(`captured-${task}`), budgetScope: "worker-run" },
        ).pipe(Effect.provideService(SubagentHost, host)),
      ownerReceipt.submissionId,
    );

  const [first, second] = await Promise.all([launch(firstPolicy, 1), launch(secondPolicy, 3)]);

  const inspect = (started: Awaited<ReturnType<typeof launch>>) =>
    withOwner(source, (host) =>
      Subagent.inspect(firstDeclaration, started.worker, started.delivery.message).pipe(
        Effect.provideService(SubagentHost, host),
      ),
    );

  const finish = (thread: string) =>
    drainAlarmsUntil(thread, async () => {
      for (const { record } of await readCanonical(thread)) {
        if (record.payload._tag === "SubagentRequested")
          await drainAlarmsUntil(
            record.payload.childThreadId,
            allSettled(record.payload.childThreadId),
          );
      }

      return allSettled(thread)();
    });

  try {
    // A launch can return pending while the source alarm owns admission.
    for (const started of [first, second]) {
      await expect
        .poll(() => inspect(started))
        .toMatchObject({
          message: started.delivery.message,
          status: "accepted",
          receipt: expect.objectContaining({ threadId: started.worker.threadId }),
        });
    }
    const acceptedFirst = await inspect(first);
    const third = await launch(firstPolicy, 4);

    await expect
      .poll(() => inspect(third))
      .toMatchObject({
        message: third.delivery.message,
        status: "refused",
        reason: "worker-capacity",
        receipt: null,
        settlement: null,
      });
    capturedConcurrency.set(source, { owner: ownerReceipt.submissionId, limit: 0 });
    expect(await launch(firstPolicy, 1)).toEqual({ ...first, delivery: acceptedFirst });
    const alarm = runDurableObjectAlarm(stubFor(first.worker.threadId)).catch(() => false);

    await expect
      .poll(async () =>
        (await readCanonical(first.worker.threadId)).some(
          ({ record }) => record.payload._tag === "RunStarted",
        ),
      )
      .toBe(true);

    const joined = await withOwner(
      source,
      (host) =>
        Subagent.followUp(
          firstDeclaration,
          first.worker,
          { question: "continue with the retained policy" },
          { idempotencyKey: decodeIdempotencyKey("captured-joined") },
        ).pipe(Effect.provideService(SubagentHost, host)),
      ownerReceipt.submissionId,
    );

    armRuntimeEviction(first.worker.threadId, "turn:after-response-append");
    independentBudgetGates.add(`${source}:task:1`);
    await alarm;
    await finish(first.worker.threadId);
    expect(armedEvictionsRemaining(first.worker.threadId)).toBe(0);
    await evict(source);
    await evict(first.worker.threadId);
    independentBudgetGates.add(`${source}:task:2`);
    capturedConcurrency.set(source, { owner: ownerReceipt.submissionId, limit: 2 });

    const later = await withOwner(
      source,
      (host) =>
        Subagent.followUp(
          firstDeclaration,
          first.worker,
          { question: `${source}:task:2`, policy: firstPolicy },
          { idempotencyKey: decodeIdempotencyKey("captured-later") },
        ).pipe(Effect.provideService(SubagentHost, host)),
      ownerReceipt.submissionId,
    );

    await finish(first.worker.threadId);
    independentBudgetGates.add(`${source}:task:3`);
    await finish(second.worker.threadId);
    for (const [started, policy] of [
      [first, firstPolicy],
      [second, secondPolicy],
    ] as const) {
      const records = await readCanonical(started.worker.threadId);

      const origins = records.flatMap(({ record }) =>
        record.payload._tag === "WorkerOriginRecorded" ? [record.payload.origin] : [],
      );

      expect(origins).toHaveLength(1);
      expect(origins[0]?.policy).toEqual(policy);
      expect(origins[0]?.budget.allocation.turns).toBe(policy.maxTurns + 1);
      expect(origins[0]?.policy.tokenBudget).toBeUndefined();
      expect(origins[0]?.policy.costBudgetMicrousd).toBeUndefined();

      const scouts = records.flatMap(({ record }) =>
        record.payload._tag === "SubagentRequested" ? [record.payload] : [],
      );

      expect(scouts).toHaveLength(started === first ? 2 : 1);
      for (const scout of scouts) {
        expect(scout.depth).toBe(2);
        expect(scout.policy?.tokenBudget).toBe(50);
        const child = await readCanonical(scout.childThreadId);

        expect(child.some(({ record }) => record.payload._tag === "ModelResponseRecorded")).toBe(
          true,
        );
        expect(
          child
            .filter(({ record }) => record.payload._tag === "SubmissionSettled")
            .map(({ record }) => record.payload),
        ).toEqual([expect.objectContaining({ outcome: "completed" })]);
      }
    }
    const records = await readCanonical(first.worker.threadId);

    const settlements = records.flatMap(({ record }) =>
      record.payload._tag === "SubmissionSettled" ? [record.payload] : [],
    );

    expect(settlements).toHaveLength(3);

    const initial = settlements.find(
      (row) => row.submissionId === acceptedFirst.receipt!.submissionId,
    );

    expect(
      settlements.find((row) => row.submissionId === joined.receipt!.submissionId)?.runId,
    ).toBe(initial?.runId);
    expect(
      settlements.find((row) => row.submissionId === later.receipt!.submissionId)?.runId,
    ).not.toBe(initial?.runId);
    expect(settlements.every((row) => row.outcome === "completed")).toBe(true);
  } finally {
    for (const task of [1, 2, 3]) independentBudgetGates.delete(`${source}:task:${task}`);
    independentBudgetGrants.delete(source);
    capturedConcurrency.delete(source);
    independentBudgetAuthorityCalls.delete(source);
    droppedMessageWakes.delete(source);
    backgroundWakeDropPrefixes.delete("worker:");
  }
}, 20_000);

// Regression: https://github.com/danieljvdm/effect-agent/commit/4600d240f44b1ef1fe9b0fc58f39e293a6434f85
it("drains private worker progress through rebuilt runtime maintenance into an idle parent", async () => {
  const source = `background-cf-custom-${crypto.randomUUID()}`;

  await runClient(
    Effect.flatMap(CloudflareThreadClient, (client) =>
      client.submit(
        { definition: backgroundSource },
        { question: "initialize" },
        submitOptions(source, "source"),
      ),
    ),
  );
  await drainAlarmsUntil(source, allSettled(source));

  const started = await withOwner(source, (host) =>
    Subagent.start(
      backgroundWorkers,
      { question: "private task" },
      { idempotencyKey: decodeIdempotencyKey("child") },
    ).pipe(Effect.provideService(SubagentHost, host)),
  );

  // The source alarm may win admission. Inspect the same retained operation without resending.
  await drainAlarmsUntil(
    source,
    async () =>
      (
        await withOwner(source, (host) =>
          Subagent.inspect(backgroundWorkers, started.worker, started.delivery.message).pipe(
            Effect.provideService(SubagentHost, host),
          ),
        )
      ).receipt !== null,
  );
  await drainAlarmsUntil(started.worker.threadId, allSettled(started.worker.threadId));
  customRuntimeThreads.add(started.worker.threadId);
  privateProgressRoutes.set(started.worker.threadId, decodeThreadId(source));
  droppedMessageWakes.add(started.worker.threadId);
  await evict(started.worker.threadId);
  try {
    const sent = await runInDurableObject(stubFor(started.worker.threadId), (instance) =>
      instance[DurableObject.RunSymbol](
        Effect.gen(function* () {
          const runtime = yield* DurableAgentRuntime;

          const host = yield* runtime.messagingHost({
            sourceThreadId: started.worker.threadId,
            principal: TEST_PRINCIPAL,
          });

          return yield* host.send({
            name: "private_progress",
            target: backgroundSource,
            encodedInput: { question: "private bounded progress" },
            idempotencyKey: decodeIdempotencyKey("progress"),
          });
        }),
      ),
    );

    expect(sent.status).toBe("pending");
    // A competing automatic pass can own delivery after a manual alarm returns. Drive the
    // native owners until the destination has applied this exact message, not merely until
    // the previously idle parent has no unsettled inputs.
    await drainAlarmsUntil(started.worker.threadId, async () => {
      await runDurableObjectAlarm(stubFor(source));

      return (await readCanonical(source)).some(
        ({ record }) =>
          record.payload._tag === "UserInputRecorded" &&
          Schema.is(MessageAdmission)(record.payload.messageAdmission) &&
          record.payload.messageAdmission.sender.threadId === started.worker.threadId &&
          record.payload.messageAdmission.peerName === "private_progress",
      );
    });
    await drainAlarmsUntil(source, allSettled(source));

    const destination = await readCanonical(source);

    const delivered = destination.filter(
      ({ record }) =>
        record.payload._tag === "UserInputRecorded" &&
        record.payload.messageAdmission !== undefined,
    );

    expect(delivered).toHaveLength(1);
    expect(delivered[0]?.record.payload).toMatchObject({
      input: { question: "private bounded progress" },
      messageAdmission: {
        sender: { threadId: started.worker.threadId },
        peerName: "private_progress",
      },
    });
    expect(
      destination.flatMap(({ record }) =>
        record.payload._tag === "SubmissionSettled" ? [record.payload.outcome] : [],
      ),
    ).toEqual(["completed", "completed"]);
  } finally {
    customRuntimeThreads.delete(started.worker.threadId);
    privateProgressRoutes.delete(started.worker.threadId);
    droppedMessageWakes.delete(started.worker.threadId);
  }
});

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
        Subagent.inspect(backgroundWorkers, started.worker, started.delivery.receipt!).pipe(
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

    expect(next.receipt!.threadId).toBe(started.worker.threadId);
    expect(next.receipt!.submissionId).not.toBe(started.delivery.receipt!.submissionId);
    await drainAlarmsUntil(started.worker.threadId, allSettled(started.worker.threadId));

    const outcome = await withOwner(source, (host) =>
      Subagent.inspect(backgroundWorkers, started.worker, next.receipt!).pipe(
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
    ).toHaveLength(1); // Source admission acknowledges completion; the child never appends here.
    expect(
      (await readCanonical(started.worker.threadId)).filter(
        ({ record }) => record.payload._tag === "WorkerInputCompleted",
      ),
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

for (const mode of ["custom", "standard"] as const)
  it(`${mode}: delivers one frozen report after child eviction and source eviction with all wake hints dropped`, async () => {
    const source = `background-cf-report-${crypto.randomUUID()}`;

    const sourceReceipt = await runClient(
      Effect.flatMap(CloudflareThreadClient, (client) =>
        client.submit(
          {
            definition:
              mode === "standard" ? backgroundStandardReportSource : backgroundReportSource,
          },
          { question: "launch complete" },
          submitOptions(source, "source"),
        ),
      ),
    );

    await drainAlarmsUntil(source, allSettled(source));
    backgroundWakeDropPrefixes.add("worker:");
    droppedMessageWakes.add(source);

    const started = await withOwner(
      source,
      (host) =>
        Subagent.start(
          backgroundReportingWorkers,
          { question: source },
          { idempotencyKey: decodeIdempotencyKey("first") },
        ).pipe(Effect.provideService(SubagentHost, host)),
      sourceReceipt.submissionId,
    );

    droppedMessageWakes.add(started.worker.threadId);
    try {
      const followUp = Subagent.followUp(
        backgroundReportingWorkers,
        started.worker,
        { question: "joined input" },
        { idempotencyKey: decodeIdempotencyKey("joined") },
      );

      const joined = await withOwner(source, (host) =>
        followUp.pipe(Effect.provideService(SubagentHost, host)),
      );

      // The alarm pump can own acceptance; inspect its stable identity without another command.
      await drainAlarmsUntil(
        source,
        async () =>
          (
            await withOwner(source, (host) =>
              Subagent.inspect(backgroundReportingWorkers, started.worker, joined.message).pipe(
                Effect.provideService(SubagentHost, host),
              ),
            )
          ).receipt !== null,
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
      // The armed post-append failpoint already evicted the child. Let its reconstructed
      // alarm finish delivery instead of injecting another crash at an uncontrolled lease.
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
      if (mode === "custom") expect(backgroundReportProjections.get(report.runId)).toBe(1);

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
      if (mode === "custom")
        expect(inputs[1]?.input).toEqual({ question: `report:${report.runId}:done` });
      else {
        expect(inputs[1]?.input).toEqual({ question: "launch complete" });
        const message = Schema.decodeUnknownSync(WorkerCompletion)(inputs[1]?.messageAdmission);

        expect(message.report).toMatchObject({
          worker: started.worker,
          runId: report.runId,
          outcome: "completed",
          result: { answer: "done" },
        });
      }
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
it("funds native persona Runs independently while joins, eviction and scouts retain one allowance", async ({
  onTestFinished,
  signal,
}) => {
  const source = `background-cf-independent-${crypto.randomUUID()}`;
  let firstAlarm: Promise<boolean> | undefined;
  let siblingAlarm: Promise<boolean> | undefined;
  let sourceAlarm: Promise<boolean> | undefined;
  let joining: Promise<unknown> | undefined;
  let arming: Promise<void> | undefined;
  let controlArmed = false;
  let controlledFollowUpInvocations = 0;
  let cleanupPromise: Promise<void> | undefined;

  const cleanup = () =>
    (cleanupPromise ??= (async () => {
      for (const round of [1, 2, 3, 4]) independentBudgetGates.add(`${source}:task:${round}`);
      try {
        await arming;
      } finally {
        try {
          if (controlArmed)
            await runInDurableObject(stubFor(source), () => {
              const control = workerInputContentions.get(source);

              control?.releaseCaller();
              control?.releaseAdmission();
            });
        } finally {
          const outcomes = await Promise.allSettled([
            joining,
            sourceAlarm,
            firstAlarm,
            siblingAlarm,
          ]);

          workerInputContentions.delete(source);
          independentBudgetGrants.delete(source);
          independentBudgetAuthorityCalls.delete(source);
          for (const round of [1, 2, 3, 4])
            independentBudgetGates.delete(`${source}:task:${round}`);
          backgroundWakeDropPrefixes.delete("worker:");
          droppedMessageWakes.delete(source);
          const rejected = outcomes.find((outcome) => outcome.status === "rejected");

          if (rejected?.status === "rejected") throw rejected.reason;
        }
      }
    })());

  onTestFinished(cleanup);

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
  let primaryFailure: unknown;

  try {
    expect(await launch()).toEqual(started);
    signal.throwIfAborted();
    firstAlarm = runDurableObjectAlarm(stubFor(started.worker.threadId)).catch(() => false);

    await expect
      .poll(async () =>
        (await readCanonical(started.worker.threadId)).some(
          ({ record }) => record.payload._tag === "RunStarted",
        ),
      )
      .toBe(true);
    signal.throwIfAborted();
    siblingAlarm = runDurableObjectAlarm(stubFor(sibling.worker.threadId)).catch(() => false);

    await expect
      .poll(async () =>
        (await readCanonical(sibling.worker.threadId)).some(
          ({ record }) => record.payload._tag === "RunStarted",
        ),
      )
      .toBe(true);

    const refused = await withOwner(source, (host) =>
      Subagent.start(
        independentBudgetWorkers,
        { question: `${source}:task:5` },
        { idempotencyKey: decodeIdempotencyKey("third-active"), budgetScope: "worker-run" },
      ).pipe(Effect.provideService(SubagentHost, host)),
    );

    expect(refused.delivery).toMatchObject({ status: "refused", reason: "worker-capacity" });
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

    const sourceDeliveries = () =>
      runInDurableObject(stubFor(source), (instance) =>
        instance[DurableObject.RunSymbol](
          Effect.flatMap(MessageDeliveryStore, (store) =>
            store.list({ ownerThreadId: decodeThreadId(source), limit: 100 }),
          ),
        ),
      );

    // Older inputs can only be observed or remain refused; only the new input may be admitted.
    await drainAlarmsUntil(source, async () => {
      const rows = await sourceDeliveries();

      expect(rows.next).toBeNull();

      return rows.items.every((row) => row.receipt !== null || row.status === "refused");
    });

    // The original caller loses the new delivery claim to the real source alarm.
    // Regression: https://github.com/danieljvdm/effect-agent/commit/cb1d297d3464850b5e4645a0d3b3a5062a1ba71b
    signal.throwIfAborted();
    controlArmed = true;
    arming = runInDurableObject(stubFor(source), () => armWorkerInputContention(source));
    void arming.catch(() => undefined);
    await arming;
    signal.throwIfAborted();

    const invokeFollowUp = (parameters: { question: string }, key: string, controlled = false) =>
      withOwner(source, (host) =>
        Effect.gen(function* () {
          if (controlled) {
            controlledFollowUpInvocations++;
            workerInputContentions.get(source)!.callerFiber = yield* Effect.fiberId;
          }

          return yield* Subagent.followUp(independentBudgetWorkers, started.worker, parameters, {
            idempotencyKey: decodeIdempotencyKey(key),
          }).pipe(Effect.provideService(SubagentHost, host));
        }),
      );

    const retainedInput = async (parameters: { question: string }) => {
      const rows = await sourceDeliveries();

      expect(rows.next).toBeNull();

      const matching = rows.items.filter(
        (row) =>
          row.envelope.threadId === started.worker.threadId &&
          JSON.stringify(row.envelope.workerAdmission?.parameters) === JSON.stringify(parameters),
      );

      expect(matching).toHaveLength(1);
      const row = matching[0]!;

      expect(row.envelope.input).toEqual(parameters);
      expect(row.envelope.workerAdmission?.origin.worker).toEqual(started.worker);
      expect(row.status).not.toBe("refused");

      return row;
    };

    const acceptFollowUp = async (delivery: MessageStatus) => {
      signal.throwIfAborted();
      if (delivery.receipt !== null) return delivery.receipt;

      const inspect = () =>
        withOwner(source, (host) =>
          Subagent.inspect(independentBudgetWorkers, started.worker, delivery.message).pipe(
            Effect.provideService(SubagentHost, host),
          ),
        );

      await drainAlarmsUntil(source, async () => (await inspect()).receipt !== null);
      const accepted = await inspect();

      expect(accepted.message).toEqual(delivery.message);
      expect(accepted.receipt?.threadId).toBe(started.worker.threadId);

      return accepted.receipt!;
    };

    const parameters = { question: "continue the active task" };
    const originalFollowUp = invokeFollowUp(parameters, "joined", true);

    joining = originalFollowUp;
    void originalFollowUp.catch(() => undefined);
    await expect
      .poll(() =>
        runInDurableObject(stubFor(source), () => workerInputContentions.get(source)?.paused),
      )
      .toBe(true);
    const retained = await retainedInput(parameters);

    expect(retained.receipt).toBeNull();
    signal.throwIfAborted();
    sourceAlarm = runDurableObjectAlarm(stubFor(source));
    void sourceAlarm.catch(() => undefined);
    await expect
      .poll(() =>
        runInDurableObject(stubFor(source), () => workerInputContentions.get(source)?.admitted),
      )
      .toBe(true);
    const claimed = await retainedInput(parameters);

    expect(claimed.key).toEqual(retained.key);
    expect(claimed.receipt).toBeNull();
    expect(claimed.leaseUntilMillis).not.toBeNull();
    await runInDurableObject(stubFor(source), () =>
      workerInputContentions.get(source)?.releaseCaller(),
    );
    const original = await originalFollowUp;

    expect(original).toEqual({
      message: retained.key,
      status: "pending",
      receipt: null,
      settlement: null,
      reason: null,
    });
    await runInDurableObject(stubFor(source), () => {
      workerInputContentions.get(source)?.releaseAdmission();
    });
    await sourceAlarm;
    const accepted = acceptFollowUp(original);

    joining = accepted;
    const joined = await accepted;

    expect((await retainedInput(parameters)).key).toEqual(retained.key);
    expect(
      await runInDurableObject(stubFor(source), () => {
        const control = workerInputContentions.get(source);

        return { acquired: control?.acquired, released: control?.released };
      }),
    ).toEqual({ acquired: 1, released: 1 });
    expect(controlledFollowUpInvocations).toBe(1);

    armRuntimeEviction(started.worker.threadId, "turn:after-response-append");
    independentBudgetGates.add(`${source}:task:1`);
    await firstAlarm;
    await finish();
    expect(armedEvictionsRemaining(started.worker.threadId)).toBe(0);
    const firstLog = await readCanonical(started.worker.threadId);

    expect(
      firstLog.filter(
        ({ record }) =>
          record.payload._tag === "UserInputRecorded" &&
          record.payload.submissionId === joined.submissionId,
      ),
    ).toHaveLength(1);

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
      signal.throwIfAborted();
      await evict(source);
      await evict(started.worker.threadId);
      independentBudgetGates.add(`${source}:task:${round}`);

      signal.throwIfAborted();
      const parameters = { question: `${source}:task:${round}` };

      const nextInput = invokeFollowUp(parameters, `later-${round}`).then(acceptFollowUp);

      joining = nextInput;
      const next = await nextInput;

      expect(next.threadId).toBe(started.worker.threadId);
      expect(next.submissionId).not.toBe(started.delivery.receipt!.submissionId);
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
  } catch (failure) {
    primaryFailure = failure;
  } finally {
    try {
      await cleanup();
    } catch (failure) {
      primaryFailure ??= failure;
    } finally {
      droppedMessageWakes.delete(sibling.worker.threadId);
      droppedMessageWakes.delete(started.worker.threadId);
    }
  }
  if (primaryFailure !== undefined) throw primaryFailure;
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

  try {
    const started = await launch();

    expect(started.delivery).toMatchObject({ status: "pending", receipt: null, reason: "storage" });

    const inspect = () =>
      withOwner(source, (host) =>
        Subagent.inspect(backgroundWorkers, started.worker, started.delivery.message).pipe(
          Effect.provideService(SubagentHost, host),
        ),
      );

    expect(await inspect()).toEqual(started.delivery);
    independentBudgetAdmissionOutages.delete(source);
    await drainAlarmsUntil(source, async () => (await inspect()).receipt !== null);
    const recovered = await inspect();

    expect(recovered.message).toEqual(started.delivery.message);
    expect(recovered.receipt?.threadId).toBe(started.worker.threadId);
    expect(await launch()).toMatchObject({
      worker: started.worker,
      delivery: { message: recovered.message, receipt: recovered.receipt },
    });
    await drainAlarmsUntil(started.worker.threadId, allSettled(started.worker.threadId));
    const log = await readCanonical(started.worker.threadId);

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

it("routes worker stop through its owning Object and keeps queued input fenced after native eviction", async () => {
  const source = `background-cf-report-stop-${crypto.randomUUID()}`;

  await runClient(
    Effect.flatMap(CloudflareThreadClient, (client) =>
      client.submit(
        { definition: backgroundSource },
        { question: "launch" },
        submitOptions(source, "source"),
      ),
    ),
  );
  await drainAlarmsUntil(source, allSettled(source));

  const started = await withOwner(source, (host) =>
    Subagent.start(
      backgroundReportingWorkers,
      { question: source },
      {
        idempotencyKey: submitOptions(source, "worker").idempotencyKey,
      },
    ).pipe(Effect.provideService(SubagentHost, host)),
  );

  await expect
    .poll(async () =>
      (await readCanonical(started.worker.threadId)).some(
        ({ record }) => record.payload._tag === "RunStarted",
      ),
    )
    .toBe(true);

  const steering = await withOwner(source, (host) =>
    Subagent.followUp(
      backgroundReportingWorkers,
      started.worker,
      { question: "correction" },
      {
        idempotencyKey: submitOptions(source, "steer").idempotencyKey,
      },
    ).pipe(Effect.provideService(SubagentHost, host)),
  );

  const stop = () =>
    withOwner(source, (host) =>
      Subagent.stop(backgroundReportingWorkers, started.worker, {
        idempotencyKey: submitOptions(source, "stop").idempotencyKey,
      }).pipe(Effect.provideService(SubagentHost, host)),
    );

  const acknowledged = await stop();

  await drainAlarmsUntil(started.worker.threadId, allSettled(started.worker.threadId));
  await evict(started.worker.threadId);
  await evict(source);
  expect(await stop()).toEqual(acknowledged);
  await drainAlarmsUntil(started.worker.threadId, allSettled(started.worker.threadId));

  const summary = await withOwner(source, (host) =>
    host.summary({ worker: started.worker, target: backgroundReportingWorkers.target }),
  );

  expect(summary).toMatchObject({
    state: "stopped",
    acceptedInput: { receipt: steering.receipt },
    appliedInput: { receipt: started.delivery.receipt },
    run: { hostReceipt: started.delivery.receipt, outcome: "aborted" },
  });
  const records = await readCanonical(started.worker.threadId);

  expect(records.filter(({ record }) => record.payload._tag === "RunStarted")).toHaveLength(1);
  expect(records.some(({ record }) => record.payload._tag === "ToolCallPrepared")).toBe(false);
  expect(
    await withOwner(source, (host) =>
      Subagent.followUp(
        backgroundReportingWorkers,
        started.worker,
        { question: "continue" },
        {
          idempotencyKey: submitOptions(source, "continue").idempotencyKey,
        },
      ).pipe(Effect.provideService(SubagentHost, host)),
    ),
  ).toMatchObject({ status: "refused", reason: "worker-stopped" });
});

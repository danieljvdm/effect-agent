import * as Agent from "@effect-agent/core/Agent";
import { AgentPolicy } from "@effect-agent/core/AgentPolicy";
import {
  DelegationId,
  ReceiptId,
  RunId,
  SubmissionId,
  ThreadId,
  ToolCallId,
} from "@effect-agent/core/Identifiers";
import { IdempotencyKey, JoinedToHost, QueueSequence, Receipt } from "@effect-agent/core/Receipt";
import {
  SubagentDelegationCaps,
  SubagentGrant,
  SubagentReservationAmounts,
} from "@effect-agent/core/SubagentContract";
import { ToolResultBounds } from "@effect-agent/core/ToolResult";
import { WorkerError } from "@effect-agent/core/Worker";
import {
  WorkerReportPreparationFailure,
  type StartWorkerRequest,
  type WorkerReporting,
} from "@effect-agent/engine/SubagentHost";
import { NodeCrypto } from "@effect/platform-node";
import { expect, layer } from "@effect/vitest";
import {
  Clock,
  Context,
  DateTime,
  Duration,
  Effect,
  Fiber,
  Layer,
  Option,
  Schema,
  Stream,
} from "effect";
import { TestClock } from "effect/testing";
import { Toolkit } from "effect/unstable/ai";

import { DurableWorkerBinding } from "../src/AgentRegistration.ts";
import {
  DurableRuntimeFailpoint,
  DurableRuntimeFailpointError,
  type DurableRuntimeFailpointLocation,
} from "../src/DurableFailpoint.ts";
import { makeWorkerRuntime, WorkerInputControl } from "../src/internal/worker-host.ts";
import {
  MessageDeliveryStore,
  applyMessageDeliveryChange,
  type MessageDeliveryRecord,
} from "../src/MessageDelivery.ts";
import {
  BatchId,
  CanonicalRecordEnvelope,
  CanonicalSequence,
  DefinitionDigests,
  DeploymentId,
  Digest,
  ObservationOffset,
  ProducerEpoch,
  ProducerId,
  RecordEnvelope,
  SubmissionSettled,
  SubmissionSettledRecord,
  SubtreeBudgetReserved,
  ThreadCreated,
  UserInputRecorded,
  type CanonicalRecordPayload,
} from "../src/Records.ts";
import {
  AbortIntent,
  AdmissionPolicyError,
  Principal,
  Settlement,
  SubmissionSnapshot,
  SubmissionLedger,
  submissionSettlementId,
} from "../src/SubmissionLedger.ts";
import { PendingSubmission, SettledSubmission } from "../src/SubmissionStatus.ts";
import { PreparedInput } from "../src/Subscription.ts";
import {
  WorkerBudgetAuthorizer,
  WorkerHostAuthorizer,
  WorkerHostConfig,
} from "../src/WorkerHost.ts";

class ReportService extends Context.Service<ReportService, string>()("test/ReportService") {}
class PrivateReportFailure extends Schema.TaggedError<PrivateReportFailure>()(
  "PrivateReportFailure",
  { secret: Schema.String },
) {}
import {
  AppendConflict,
  AppendResult,
  ThreadExport,
  ThreadNotMaterialized,
  ThreadTail,
  ThreadStore,
} from "../src/ThreadStore.ts";

const sourceId = Schema.decodeSync(ThreadId)("source");
const principal = Schema.decodeSync(Principal)("owner");
const digest = Schema.decodeSync(Digest)("a".repeat(64));
const definitions = DefinitionDigests.make({ agent: digest, model: digest, tools: digest });

const policy = AgentPolicy.make({
  maxTurns: 2,
  maxToolCalls: 2,
  maxDuration: "1 second",
  toolConcurrency: 2,
  toolResultBounds: ToolResultBounds.make({ maxBytes: 1_024 }),
});

const target = Agent.make("worker-target", {
  input: Schema.Struct({ text: Schema.String }),
  output: Schema.String,
  instructions: "Answer",
  toolkit: Toolkit.empty,
  policy,
});

const sourceAgent = Agent.make("source-agent", {
  input: Schema.String,
  output: Schema.String,
  instructions: "Delegate",
  toolkit: Toolkit.empty,
  policy: AgentPolicy.make({
    ...policy,
    maxTurns: 20,
    maxToolCalls: 20,
    maxDuration: "10 seconds",
  }),
});

const request = (key: string): StartWorkerRequest => ({
  delegationId: Schema.decodeSync(DelegationId)("research"),
  target,
  idempotencyKey: Schema.decodeSync(IdempotencyKey)(key),
  encodedInput: { text: key },
  encodedParameters: { note: key },
  policy,
  budget: {
    caps: SubagentDelegationCaps.make({
      maxTotalChildInvocations: 10,
      maxConcurrentChildren: 1,
      maxTurns: 20,
      maxToolCalls: 20,
      maxDurationMillis: 10_000,
    }),
    allocation: SubagentReservationAmounts.make({
      turns: 2,
      toolCalls: 2,
      durationMillis: 1_000,
      inputTokens: 0,
      outputTokens: 0,
      costMicrousd: 0,
      resultBytes: 1_024,
    }),
  },
  encodedGrant: SubagentGrant.make({ allowedToolNames: [], maxDepth: 1 }),
});

const reportWith = (
  prepare: WorkerReporting<WorkerReportPreparationFailure>["prepare"],
): WorkerReporting<WorkerReportPreparationFailure> => ({
  delegationId: Schema.decodeSync(DelegationId)("research"),
  target,
  input: sourceAgent.input,
  prepare,
});

const harness = Effect.fn("workerHostHarness")(function* (
  options: {
    readonly independentBudget?: boolean;
    readonly sourceReports?: ReadonlyArray<WorkerReporting<WorkerReportPreparationFailure>>;
    readonly targetReports?: ReadonlyArray<WorkerReporting<WorkerReportPreparationFailure>>;
  } = {},
) {
  const now = yield* Clock.currentTimeMillis;
  const logs = new Map<ThreadId, Array<CanonicalRecordEnvelope>>();
  const deliveries = new Map<string, MessageDeliveryRecord>();
  const submissions = new Map<SubmissionId, SubmissionSnapshot>();
  const settlements = new Map<SubmissionId, Settlement>();
  let failpoint: DurableRuntimeFailpointLocation | undefined;
  let denied: "read" | "send" | "control" | undefined;
  let joined: SubmissionId | undefined;
  let admissionFailure = false;
  let sequence = 0;
  const auth: Array<string> = [];

  const push = (threadId: ThreadId, payload: CanonicalRecordPayload, id: string) => {
    const records = logs.get(threadId) ?? [];

    records.push(
      CanonicalRecordEnvelope.make({
        threadId,
        batchId: Schema.decodeSync(BatchId)(id),
        sequence: Schema.decodeSync(CanonicalSequence)(records.length + 1),
        offset: Schema.decodeSync(ObservationOffset)(`${records.length + 1}`),
        record: RecordEnvelope.make({
          recordId: Schema.decodeSync(RecordEnvelope.fields.recordId)(id),
          schemaVersion: 1,
          family: "thread",
          createdAt: DateTime.makeUnsafe(now),
          deploymentId: Schema.decodeSync(DeploymentId)("test"),
          payload,
        }),
      }),
    );
    logs.set(threadId, records);
  };

  push(sourceId, ThreadCreated.make({ agentId: sourceAgent.id, definitions }), "source-created");
  const lookup = (id: SubmissionId) => Option.fromNullishOr(submissions.get(id));

  const status: WorkerInputControl["Service"]["status"] = (receipt) =>
    Effect.sync(() => {
      const settlement = settlements.get(receipt.submissionId);

      return settlement === undefined
        ? PendingSubmission.make({})
        : SettledSubmission.make({ settlement });
    });

  let runtime: Effect.Success<ReturnType<typeof makeWorkerRuntime>>;

  runtime = yield* makeWorkerRuntime({
    deploymentId: Schema.decodeSync(DeploymentId)("test"),
    producerId: Schema.decodeSync(ProducerId)("test"),
    settlementPollInterval: Duration.millis(5),
    bindings: [sourceAgent, target].map((definition) => ({
      definition,
      agentId: definition.id,
      digests: definitions,
      attempt: () => Effect.succeed(Option.none()),
      reporting:
        definition === sourceAgent ? (options.sourceReports ?? []) : (options.targetReports ?? []),
    })),
  }).pipe(
    Effect.provideService(WorkerBudgetAuthorizer, {
      authorize: () =>
        options.independentBudget === true
          ? Effect.void
          : WorkerError.make({ operation: "start", reason: "denied" }),
    }),
    Effect.provideService(WorkerHostConfig, {
      maxWorkersPerSource: 2,
      maxInputsPerWorker: 3,
      maxPendingInputsPerWorker: 2,
      lifetimeMillis: 60_000,
    }),
    Effect.provideService(WorkerHostAuthorizer, {
      authorize: (request) =>
        Effect.suspend(() => {
          auth.push(request.access);

          return request.access === denied ||
            request.principal !== principal ||
            !logs.has(request.sourceThreadId)
            ? WorkerError.make({ operation: request.operation, reason: "denied" })
            : Effect.succeed(principal);
        }),
    }),
    Effect.provideService(DurableRuntimeFailpoint, {
      hit: (point) =>
        Effect.suspend(() =>
          point === failpoint
            ? DurableRuntimeFailpointError.make({ location: point })
            : Effect.void,
        ),
    }),
    Effect.provideService(ThreadStore, {
      read: ({ threadId, afterSequence = 0, limit }) =>
        Stream.fromIterable(
          (logs.get(threadId) ?? [])
            .filter((entry) => entry.sequence > afterSequence)
            .slice(0, limit),
        ),
      export: ({ threadId }) =>
        Effect.suspend(() => {
          const records = logs.get(threadId);

          return records === undefined
            ? ThreadNotMaterialized.make({ threadId })
            : Effect.succeed(
                ThreadExport.make({
                  format: "effect-agent/thread@1",
                  threadId,
                  records: [...records],
                  tailSequence: Schema.decodeSync(CanonicalSequence)(records.length),
                  tailDigest: digest,
                }),
              );
        }),
      inspectTail: ({ threadId }) =>
        Effect.suspend(() => {
          const records = logs.get(threadId);

          return records === undefined
            ? ThreadNotMaterialized.make({ threadId })
            : Effect.succeed(
                ThreadTail.make({
                  threadId,
                  tailSequence: Schema.decodeSync(CanonicalSequence)(records.length),
                  tailDigest: digest,
                  producerEpoch: Schema.decodeSync(ProducerEpoch)(0),
                }),
              );
        }),
      append: (request) =>
        Effect.gen(function* () {
          yield* Effect.yieldNow;
          const records = logs.get(request.threadId) ?? [];

          if (records.length !== request.expectedTailSequence)
            return yield* AppendConflict.make({
              threadId: request.threadId,
              batchId: request.batch.batchId,
              reason: "tail",
            });
          const first = records.length + 1;

          for (const record of request.batch.records)
            push(request.threadId, record.payload, record.recordId);

          return AppendResult.make({
            firstSequence: Schema.decodeSync(CanonicalSequence)(first),
            lastSequence: Schema.decodeSync(CanonicalSequence)(records.length),
            tailDigest: digest,
            replayed: false,
          });
        }),

      materialize: () => Effect.die("Worker fixture materializes through input control"),
      observe: () => Stream.die("Worker fixture uses finite canonical reads"),
    }),
    Effect.provideService(MessageDeliveryStore, {
      get: ({ messageId }) => Effect.sync(() => deliveries.get(messageId) ?? null),
      insert: (record) =>
        Effect.sync(() => {
          deliveries.set(record.key.messageId, record);

          return record;
        }),
      change: ({ messageId }, change) =>
        Effect.gen(function* () {
          const changed = yield* Effect.fromResult(
            applyMessageDeliveryChange(deliveries.get(messageId)!, change),
          );

          deliveries.set(messageId, changed);

          return changed;
        }),
      list: ({ ownerThreadId, limit }) =>
        Effect.succeed({
          items: [...deliveries.values()]
            .filter((record) => record.key.ownerThreadId === ownerThreadId)
            .slice(0, limit),
          next: null,
        }),
      due: () =>
        Effect.succeed(
          [...deliveries.values()]
            .filter((record) => record.status === "pending")
            .map((record) => record.key),
        ),
      nextDeadline: () => Effect.succeed(null),
    }),
    Effect.provideService(SubmissionLedger, {
      lookup: (request) =>
        Effect.sync(() =>
          request._tag === "SubmissionLookupById"
            ? lookup(request.submissionId)
            : Option.fromNullishOr(
                [...submissions.values()].find(
                  (row) =>
                    row.threadId === request.threadId &&
                    row.principal === request.principal &&
                    row.idempotencyKey === request.idempotencyKey,
                ),
              ),
        ),

      capabilities: Effect.die("Worker fixture only implements ledger lookup"),
      scanNonterminal: Stream.die("Worker fixture only implements ledger lookup"),
      admit: () => Effect.die("Worker fixture only implements ledger lookup"),
      markReady: () => Effect.die("Worker fixture only implements ledger lookup"),
      resolveAdmission: () => Effect.die("Worker fixture only implements ledger lookup"),
      claim: () => Effect.die("Worker fixture only implements ledger lookup"),
      renewOwnership: () => Effect.die("Worker fixture only implements ledger lookup"),
      releaseOwnership: () => Effect.die("Worker fixture only implements ledger lookup"),
      markInputApplied: () => Effect.die("Worker fixture only implements ledger lookup"),
      reserveSettlement: () => Effect.die("Worker fixture only implements ledger lookup"),
      finalizeSettlement: () => Effect.die("Worker fixture only implements ledger lookup"),
      requestAbort: () => Effect.die("Worker fixture only implements ledger lookup"),
      readAbortIntent: () => Effect.die("Worker fixture only implements ledger lookup"),
      claimJoining: () => Effect.die("Worker fixture only implements ledger lookup"),
      markJoined: () => Effect.die("Worker fixture only implements ledger lookup"),
      revertJoining: () => Effect.die("Worker fixture only implements ledger lookup"),
      suspend: () => Effect.die("Worker fixture only implements ledger lookup"),
      recordApprovalDecision: () => Effect.die("Worker fixture only implements ledger lookup"),
      markUnknown: () => Effect.die("Worker fixture only implements ledger lookup"),
      recordUnknownResolution: () => Effect.die("Worker fixture only implements ledger lookup"),
      recordChildSettled: () => Effect.die("Worker fixture only implements ledger lookup"),
      reserveChildBudget: () => Effect.die("Worker fixture only implements ledger lookup"),
      attachChildToReservation: () => Effect.die("Worker fixture only implements ledger lookup"),
      beginChildBudgetRelease: () => Effect.die("Worker fixture only implements ledger lookup"),
      releaseChildBudget: () => Effect.die("Worker fixture only implements ledger lookup"),
      loadRecoverySnapshot: () => Effect.die("Worker fixture only implements ledger lookup"),
    }),
    Effect.provideService(WorkerInputControl, {
      submit: (envelope) =>
        Effect.gen(function* () {
          const metadata = envelope.workerAdmission!;

          const options = {
            threadId: envelope.threadId,
            definitions: envelope.definitions,
            principal: envelope.deliveryPrincipal,
            idempotencyKey: envelope.admissionKey,
            workerAdmission: metadata,
          };

          yield* runtime
            .validateAdmission(metadata, options, envelope.agentId, envelope.inputDigest)
            .pipe(
              Effect.mapError((error) =>
                AdmissionPolicyError.make({
                  reason: error.reason === "storage" ? "unavailable" : "refused",
                  code: `worker-${error.reason}`,
                }),
              ),
            );
          if (admissionFailure) return yield* Effect.die("simulated admission crash");

          const existing = [...submissions.values()].find(
            (row) => row.idempotencyKey === envelope.admissionKey,
          );

          if (existing !== undefined)
            return Receipt.make({
              threadId: existing.threadId,
              submissionId: existing.submissionId,
              receiptId: existing.receiptId,
              queueSequence: existing.queueSequence,
            });
          if (!logs.has(envelope.threadId))
            push(
              envelope.threadId,
              ThreadCreated.make({ agentId: target.id, definitions }),
              "child-created",
            );
          yield* runtime.ensureOrigin(metadata.origin).pipe(Effect.orDie);
          sequence++;
          const submissionId = Schema.decodeSync(SubmissionId)(`submission-${sequence}`);

          const receipt = Receipt.make({
            threadId: envelope.threadId,
            submissionId,
            receiptId: Schema.decodeSync(ReceiptId)(`receipt-${sequence}`),
            queueSequence: Schema.decodeSync(QueueSequence)(sequence),
          });

          submissions.set(
            submissionId,
            SubmissionSnapshot.make({
              ...receipt,
              principal,
              idempotencyKey: envelope.admissionKey,
              agentId: target.id,
              agentDigests: definitions,
              deploymentId: Schema.decodeSync(DeploymentId)("test"),
              inputPayload: envelope.input,
              inputDigest: envelope.inputDigest,
              state: "ready",
              createdAt: DateTime.makeUnsafe(now),
              workerAdmission: metadata,
            }),
          );

          return receipt;
        }),
      status,
      abort: (command) =>
        Effect.suspend(() =>
          command.submissionId === joined
            ? JoinedToHost.make({
                submissionId: command.submissionId,
                hostSubmissionId: [...submissions.keys()][0]!,
              })
            : Effect.succeed(
                AbortIntent.make({ ...command, requestedAt: DateTime.makeUnsafe(now) }),
              ),
        ),
    }),
  );
  const host = yield* runtime.acquire({ sourceThreadId: sourceId, principal });

  const settle = Effect.fn("workerHostHarness.settle")(function* (
    receipt: Receipt,
    result = "done",
    options: { readonly host?: Receipt; readonly abortedBeforeRun?: boolean } = {},
  ) {
    const row = submissions.get(receipt.submissionId)!;
    const settlementId = submissionSettlementId(row.submissionId);
    const outcome = options.abortedBeforeRun ? "aborted" : "completed";

    const runId = options.abortedBeforeRun
      ? undefined
      : Schema.decodeSync(SubmissionSettled.fields.runId)(
          `run:${options.host?.submissionId ?? row.submissionId}`,
        );

    if (runId !== undefined)
      push(
        row.threadId,
        UserInputRecorded.make({
          submissionId: row.submissionId,
          kind: options.host === undefined ? "user" : "steering",
          runId,
          input: row.inputPayload,
        }),
        `input:${row.submissionId}`,
      );

    push(
      row.threadId,
      Schema.decodeUnknownSync(Schema.toType(SubmissionSettledRecord))(
        SubmissionSettled.make({
          submissionId: row.submissionId,
          receiptId: row.receiptId,
          settlementId,
          outcome,
          ...(options.host === undefined && outcome === "completed" ? { result } : {}),
          ...(runId === undefined ? {} : { runId }),
        }),
      ),
      `settled:${row.submissionId}`,
    );
    yield* runtime.completeInput(row);

    const settlement = Settlement.make({
      submissionId: row.submissionId,
      receiptId: row.receiptId,
      settlementId,
      outcome,
      settledAt: DateTime.makeUnsafe(now),
    });

    settlements.set(row.submissionId, settlement);
    submissions.set(row.submissionId, SubmissionSnapshot.make({ ...row, state: "settled" }));
  });

  return {
    runtime,
    host,
    deliveries,
    logs,
    submissions,
    auth,
    settle,
    push,
    deny: (value: typeof denied) => {
      denied = value;
    },
    fail: (value: typeof failpoint) => {
      failpoint = value;
    },
    join: (id: SubmissionId) => {
      joined = id;
    },
    crashAdmission: (value: boolean) => {
      admissionFailure = value;
    },
  };
});

layer(NodeCrypto.layer)((it) => {
  // Regression: https://github.com/danieljvdm/effect-agent/pull/358
  it.effect(
    "requires host funding authority and preserves worker identity and structural limits",
    () =>
      Effect.gen(function* () {
        const denied = yield* harness();
        const base = request("independent");

        const independent: StartWorkerRequest = {
          ...base,
          budgetScope: "worker-run",
          budget: {
            ...base.budget,
            caps: SubagentDelegationCaps.make({
              maxTotalChildInvocations: 1,
              maxConcurrentChildren: 1,
              maxTurns: 2,
              maxToolCalls: 2,
              maxDurationMillis: 1_000,
            }),
          },
        };

        expect((yield* denied.host.start(independent).pipe(Effect.flip)).reason).toBe("denied");
        expect(denied.deliveries.size).toBe(0);
        const h = yield* harness({ independentBudget: true });
        const started = yield* h.host.start(independent);

        expect(yield* h.host.start(independent)).toEqual(started);
        const original = h.submissions.get(started.receipt.submissionId)?.workerAdmission?.origin;

        yield* h.settle(started.receipt);
        for (const name of ["second", "third"]) {
          const next = yield* h.host.followUp({
            worker: started.worker,
            target,
            idempotencyKey: Schema.decodeSync(IdempotencyKey)(name),
            encodedInput: { text: name },
            encodedParameters: { note: name },
          });

          expect(h.submissions.get(next.submissionId)?.workerAdmission?.origin).toEqual(original);
          yield* h.settle(next);
        }
        expect(original).toMatchObject({ budgetScope: "worker-run", depth: 1 });
        expect(
          h.logs
            .get(sourceId)
            ?.filter(({ record }) => record.payload._tag === "SubtreeBudgetReserved"),
        ).toHaveLength(0);
        expect(
          (yield* h.host
            .followUp({
              worker: started.worker,
              target,
              idempotencyKey: Schema.decodeSync(IdempotencyKey)("fourth"),
              encodedInput: { text: "fourth" },
              encodedParameters: {},
            })
            .pipe(Effect.flip)).reason,
        ).toBe("capacity");
      }),
  );
  it.effect("concurrent launches cannot oversubscribe one canonical source slot", () =>
    Effect.gen(function* () {
      const h = yield* harness();

      const outcomes = yield* Effect.forEach(
        ["left", "right"],
        (key) => h.host.start(request(key)).pipe(Effect.exit),
        { concurrency: 2 },
      );

      expect(outcomes.filter((exit) => exit._tag === "Success")).toHaveLength(1);
      expect(
        h.logs
          .get(sourceId)
          ?.filter(({ record }) => record.payload._tag === "WorkerInputRequested"),
      ).toHaveLength(1);
    }),
  );

  it.effect(
    "enforces source ceilings, encoded input, pending input and lifetime bounds before accepting work",
    () =>
      Effect.gen(function* () {
        const h = yield* harness();
        const ordinary = request("first");

        const excessive = {
          ...ordinary,
          budget: {
            ...ordinary.budget,
            caps: SubagentDelegationCaps.make({ ...ordinary.budget.caps, maxTurns: 21 }),
          },
        };

        expect((yield* h.host.start(excessive).pipe(Effect.flip)).reason).toBe("capacity");
        expect(
          (yield* h.host.start({ ...ordinary, encodedInput: { text: 42 } }).pipe(Effect.flip))
            .reason,
        ).toBe("corrupt");
        expect(h.deliveries.size).toBe(0);
        const first = yield* h.host.start(ordinary);

        const send = (key: string) =>
          h.host.followUp({
            worker: first.worker,
            target,
            idempotencyKey: Schema.decodeSync(IdempotencyKey)(key),
            encodedInput: { text: key },
            encodedParameters: { note: key },
          });

        yield* send("second");
        expect((yield* send("third").pipe(Effect.exit))._tag).toBe("Failure");
        yield* TestClock.adjust("61 seconds");
        expect((yield* send("late").pipe(Effect.flip)).reason).toBe("capacity");
      }),
  );

  it.effect(
    "interrupting await leaves accepted work pending without requesting control authority",
    () =>
      Effect.gen(function* () {
        const h = yield* harness();
        const first = yield* h.host.start(request("waiting"));

        const waiting = yield* h.host
          .await({ worker: first.worker, target, receipt: first.receipt })
          .pipe(Effect.forkChild);

        yield* Effect.yieldNow;
        yield* Fiber.interrupt(waiting);
        expect(h.auth).not.toContain("control");
        expect(
          (yield* h.host.inspect({ worker: first.worker, target, receipt: first.receipt }))._tag,
        ).toBe("Pending");
      }),
  );
  it.effect(
    "retains origin and per-input parameters; follow-up reuses the worker under one active slot",
    () =>
      Effect.gen(function* () {
        const h = yield* harness();
        const first = yield* h.host.start(request("first"));
        const replay = yield* h.host.start(request("first"));

        expect(replay).toEqual(first);

        const next = yield* h.host.followUp({
          worker: first.worker,
          target,
          idempotencyKey: Schema.decodeSync(IdempotencyKey)("next"),
          encodedInput: { text: "later" },
          encodedParameters: { note: "second" },
        });

        expect(next.threadId).toBe(first.worker.threadId);
        expect(next.submissionId).not.toBe(first.receipt.submissionId);
        expect(h.submissions.get(next.submissionId)?.workerAdmission?.origin).toEqual(
          h.submissions.get(first.receipt.submissionId)?.workerAdmission?.origin,
        );
        expect(h.submissions.get(next.submissionId)?.parentLinkage).toBeUndefined();
        expect(
          yield* h.host.inspect({ worker: first.worker, target, receipt: first.receipt }),
        ).toEqual({ _tag: "Pending", receipt: first.receipt });
        h.join(next.submissionId);
        expect(
          (yield* h.host.cancel({ worker: first.worker, target, receipt: next }).pipe(Effect.flip))
            ._tag,
        ).toBe("JoinedToHost");
        yield* h.settle(first.receipt);

        const status = yield* h.host.inspect({
          worker: first.worker,
          target,
          receipt: first.receipt,
        });

        expect(status._tag === "Settled" && status.encodedParameters).toEqual({ note: "first" });
        expect(
          (yield* h.host.list({ delegationId: first.worker.delegationId, target, limit: 10 }))
            .items[0]?.latestReceipt,
        ).toEqual(next);
      }),
  );

  it.effect("worker summaries remain active after the latest queued input is cancelled", () =>
    Effect.gen(function* () {
      const h = yield* harness();
      const first = yield* h.host.start(request("still-active"));

      const latest = yield* h.host.followUp({
        worker: first.worker,
        target,
        idempotencyKey: Schema.decodeSync(IdempotencyKey)("cancel-queued"),
        encodedInput: { text: "queued" },
        encodedParameters: { note: "queued" },
      });

      yield* h.host.cancel({ worker: first.worker, target, receipt: latest });
      yield* h.settle(latest, "unused", { abortedBeforeRun: true });
      expect(
        (yield* h.host.inspect({ worker: first.worker, target, receipt: first.receipt }))._tag,
      ).toBe("Pending");
      expect(
        yield* h.host.inspect({ worker: first.worker, target, receipt: latest }),
      ).toMatchObject({ _tag: "Settled", receipt: latest, outcome: "aborted" });

      const active = { worker: first.worker, latestReceipt: latest, state: "active" };

      expect(yield* h.host.summary({ worker: first.worker, target })).toEqual(active);
      expect(
        (yield* h.host.list({ delegationId: first.worker.delegationId, target, limit: 10 })).items,
      ).toEqual([active]);

      yield* h.settle(first.receipt);
      const idle = { ...active, state: "idle" };

      expect(yield* h.host.summary({ worker: first.worker, target })).toEqual(idle);
      expect(
        (yield* h.host.list({ delegationId: first.worker.delegationId, target, limit: 10 })).items,
      ).toEqual([idle]);
    }),
  );

  it.effect(
    "worker summaries choose the latest destination receipt when source intents arrive out of order",
    () =>
      Effect.gen(function* () {
        const h = yield* harness();
        const first = yield* h.host.start(request("initial"));

        yield* h.settle(first.receipt);

        const followUp = (key: string) =>
          h.host.followUp({
            worker: first.worker,
            target,
            idempotencyKey: Schema.decodeSync(IdempotencyKey)(key),
            encodedInput: { text: key },
            encodedParameters: { note: key },
          });

        h.fail("worker:after-source-append");
        expect((yield* followUp("earlier-intent").pipe(Effect.flip)).reason).toBe("storage");
        h.fail(undefined);
        expect(yield* h.host.summary({ worker: first.worker, target })).toEqual({
          worker: first.worker,
          latestReceipt: first.receipt,
          state: "starting",
        });
        const earlierReceipt = yield* followUp("later-intent");

        // The interrupted delivery's expiring claim must elapse before its admission replay.
        yield* TestClock.adjust("31 seconds");
        const latestReceipt = yield* followUp("earlier-intent");

        expect(latestReceipt.queueSequence).toBeGreaterThan(earlierReceipt.queueSequence);
        expect(
          h.logs
            .get(sourceId)!
            .flatMap(({ record }) =>
              record.payload._tag === "WorkerInputRequested"
                ? [record.payload.admission.parameters]
                : [],
            ),
        ).toEqual([{ note: "initial" }, { note: "earlier-intent" }, { note: "later-intent" }]);
        const active = { worker: first.worker, latestReceipt, state: "active" };

        expect(yield* h.host.summary({ worker: first.worker, target })).toEqual(active);
        expect(
          (yield* h.host.list({ delegationId: first.worker.delegationId, target, limit: 10 }))
            .items,
        ).toEqual([active]);
        yield* h.settle(earlierReceipt);
        yield* h.settle(latestReceipt);
        expect(yield* h.host.summary({ worker: first.worker, target })).toEqual({
          ...active,
          state: "idle",
        });
      }),
  );

  it.effect("reserves concurrency with canonical CAS and releases only acknowledged inputs", () =>
    Effect.gen(function* () {
      const h = yield* harness();
      const first = yield* h.host.start(request("first"));
      const other = yield* h.host.start(request("other")).pipe(Effect.exit);

      expect(other._tag).toBe("Failure");
      yield* h.settle(first.receipt);
      expect((yield* h.host.start(request("other")).pipe(Effect.flip)).reason).toBe("capacity");
      expect(
        [...h.deliveries.values()].filter((record) => record.status === "refused"),
      ).toHaveLength(1);
      const second = yield* h.host.start(request("replacement"));

      expect(second.worker.threadId).not.toBe(first.worker.threadId);

      const completed = h.logs
        .get(sourceId)
        ?.filter(({ record }) => record.payload._tag === "WorkerInputCompleted");

      expect(completed).toHaveLength(1);
      yield* h.runtime.completeInput(h.submissions.get(first.receipt.submissionId)!);
      expect(
        h.logs
          .get(sourceId)
          ?.filter(({ record }) => record.payload._tag === "WorkerInputCompleted"),
      ).toHaveLength(1);
    }),
  );

  it.effect(
    "shares one source-input subtree across attached and background descendants and restores it for later inputs",
    () =>
      Effect.gen(function* () {
        const h = yield* harness();
        const initial = request("builder");

        const tree = {
          ...initial,
          encodedGrant: SubagentGrant.make({ allowedToolNames: [], maxDepth: 2 }),
          budget: {
            ...initial.budget,
            descendantInvocations: 2,
            caps: SubagentDelegationCaps.make({ ...initial.budget.caps, maxConcurrentChildren: 2 }),
            allocation: SubagentReservationAmounts.make({
              ...initial.budget.allocation,
              turns: 4,
              toolCalls: 4,
              durationMillis: 2_000,
              resultBytes: 1_536,
            }),
          },
        };

        const builder = yield* h.host.start(tree);

        const childPolicy = AgentPolicy.make({
          ...policy,
          maxTurns: 1,
          maxToolCalls: 1,
          maxDuration: "500 millis",
          toolConcurrency: 1,
          toolResultBounds: ToolResultBounds.make({ maxBytes: 256 }),
        });

        const childBudget = {
          caps: SubagentDelegationCaps.make({
            maxTotalChildInvocations: 2,
            maxConcurrentChildren: 2,
            maxTurns: 2,
            maxToolCalls: 2,
            maxDurationMillis: 1_000,
            maxResultBytes: 512,
          }),
          allocation: SubagentReservationAmounts.make({
            turns: 1,
            toolCalls: 1,
            durationMillis: 500,
            inputTokens: 0,
            outputTokens: 0,
            costMicrousd: 0,
            resultBytes: 256,
          }),
        };

        const childGrant = SubagentGrant.make({ allowedToolNames: [], maxDepth: 2 });

        const attached = SubtreeBudgetReserved.make({
          reservationId: Schema.decodeSync(SubtreeBudgetReserved.fields.reservationId)(
            "attached-scout",
          ),
          sourceSubmissionId: builder.receipt.submissionId,
          childThreadId: Schema.decodeSync(ThreadId)("attached-scout"),
          lifetime: "attached",
          depth: 2,
          policy: childPolicy,
          grant: childGrant,
          budget: childBudget,
        });

        yield* h.runtime.reserveSubtree(builder.worker.threadId, attached);
        yield* h.runtime.reserveSubtree(builder.worker.threadId, attached);

        const modelHost = (receipt: Receipt) =>
          h.runtime.facet(
            {
              source: {
                _tag: "tool",
                agentId: target.id,
                threadId: builder.worker.threadId,
                runId: Schema.decodeSync(RunId)(`run:${receipt.submissionId}`),
                toolCallId: Schema.decodeSync(ToolCallId)("spawn"),
              },
              policy,
              depth: 1,
              grant: tree.encodedGrant,
            },
            principal,
            receipt.submissionId,
          );

        const nested = {
          ...request("background-scout"),
          policy: childPolicy,
          budget: childBudget,
          encodedGrant: childGrant,
        };

        const scout = yield* modelHost(builder.receipt).start(nested);

        expect(h.submissions.get(scout.receipt.submissionId)?.workerAdmission?.origin.depth).toBe(
          2,
        );
        expect(
          h.submissions.get(scout.receipt.submissionId)?.workerAdmission?.sourceSubmissionId,
        ).toBe(builder.receipt.submissionId);
        expect(
          (yield* h.runtime
            .reserveSubtree(
              builder.worker.threadId,
              SubtreeBudgetReserved.make({
                ...attached,
                reservationId: Schema.decodeSync(SubtreeBudgetReserved.fields.reservationId)(
                  "third",
                ),
                childThreadId: Schema.decodeSync(ThreadId)("third"),
              }),
            )
            .pipe(Effect.flip)).reason,
        ).toBe("capacity");

        const programmatic = yield* h.runtime.acquire({
          sourceThreadId: builder.worker.threadId,
          principal,
        });

        expect((yield* programmatic.start(nested).pipe(Effect.flip)).reason).toBe("denied");

        const nextInput = yield* h.host.followUp({
          worker: builder.worker,
          target,
          idempotencyKey: Schema.decodeSync(IdempotencyKey)("builder-next"),
          encodedInput: { text: "next" },
          encodedParameters: { note: "next" },
        });

        const next = yield* modelHost(nextInput).start({
          ...nested,
          idempotencyKey: Schema.decodeSync(IdempotencyKey)("second-input-scout"),
        });

        expect(
          h.submissions.get(next.receipt.submissionId)?.workerAdmission?.sourceSubmissionId,
        ).toBe(nextInput.submissionId);
      }),
  );

  it.effect(
    "reserves future descendant slots and refuses widening depth or lifetime authority",
    () =>
      Effect.gen(function* () {
        const h = yield* harness();
        const initial = request("bounded-builder");

        const builder = yield* h.host.start({
          ...initial,
          encodedGrant: SubagentGrant.make({
            allowedToolNames: ["read"],
            maxDepth: 2,
            childLifetimes: ["attached"],
          }),
          budget: {
            ...initial.budget,
            descendantInvocations: 1,
            allocation: SubagentReservationAmounts.make({
              ...initial.budget.allocation,
              turns: 4,
              toolCalls: 4,
              durationMillis: 2_000,
              resultBytes: 2_048,
            }),
          },
        });

        const reservation = SubtreeBudgetReserved.make({
          reservationId: Schema.decodeSync(SubtreeBudgetReserved.fields.reservationId)("scout"),
          sourceSubmissionId: builder.receipt.submissionId,
          childThreadId: Schema.decodeSync(ThreadId)("scout"),
          lifetime: "attached",
          depth: 2,
          policy,
          grant: SubagentGrant.make({
            allowedToolNames: ["read"],
            maxDepth: 2,
            childLifetimes: ["attached"],
          }),
          budget: {
            allocation: initial.budget.allocation,
            caps: SubagentDelegationCaps.make({
              maxTotalChildInvocations: 2,
              maxConcurrentChildren: 1,
            }),
          },
        });

        expect(
          (yield* h.runtime
            .reserveSubtree(
              builder.worker.threadId,
              SubtreeBudgetReserved.make({ ...reservation, depth: 3 }),
            )
            .pipe(Effect.flip)).reason,
        ).toBe("denied");
        expect(
          (yield* h.runtime
            .reserveSubtree(
              builder.worker.threadId,
              SubtreeBudgetReserved.make({ ...reservation, lifetime: "background" }),
            )
            .pipe(Effect.flip)).reason,
        ).toBe("denied");
        expect(
          (yield* h.runtime
            .reserveSubtree(
              builder.worker.threadId,
              SubtreeBudgetReserved.make({
                ...reservation,
                budget: { ...reservation.budget, descendantInvocations: 1 },
              }),
            )
            .pipe(Effect.flip)).reason,
        ).toBe("capacity");
        yield* h.runtime.reserveSubtree(builder.worker.threadId, reservation);
        expect(
          h.logs
            .get(builder.worker.threadId)
            ?.filter(({ record }) => record.payload._tag === "SubtreeBudgetReserved"),
        ).toHaveLength(1);
      }),
  );

  it.effect(
    "observes joined output from canonical host Run while preserving the requested Receipt",
    () =>
      Effect.gen(function* () {
        const h = yield* harness();
        const first = yield* h.host.start(request("host"));

        const member = yield* h.host.followUp({
          worker: first.worker,
          target,
          idempotencyKey: Schema.decodeSync(IdempotencyKey)("member"),
          encodedInput: { text: "member" },
          encodedParameters: { note: "member-parameters" },
        });

        const waiting = yield* h.host
          .await({
            worker: first.worker,
            target,
            receipt: member,
          })
          .pipe(Effect.forkChild);

        yield* Effect.yieldNow;
        yield* h.settle(first.receipt, "shared-result");
        yield* h.settle(member, "unused", { host: first.receipt });
        yield* TestClock.adjust("5 millis");
        const observed = yield* Fiber.join(waiting);

        expect(observed).toMatchObject({
          _tag: "Settled",
          receipt: member,
          runId: `run:${first.receipt.submissionId}`,
          encodedParameters: { note: "member-parameters" },
          encodedResult: "shared-result",
        });
        expect(yield* h.host.inspect({ worker: first.worker, target, receipt: member })).toEqual(
          observed,
        );
      }),
  );

  it.effect("preserves an absent Run identity for work aborted before execution", () =>
    Effect.gen(function* () {
      const h = yield* harness();
      const first = yield* h.host.start(request("never-ran"));

      yield* h.settle(first.receipt, "unused", { abortedBeforeRun: true });
      const observed = yield* h.host.inspect({ ...first, target });

      expect(observed).toMatchObject({ _tag: "Settled", outcome: "aborted", encodedResult: null });
      expect(observed).not.toHaveProperty("runId");
    }),
  );

  it.effect(
    "retains independently discoverable delivery before an interrupted admission and reuses it on retry",
    () =>
      Effect.gen(function* () {
        const h = yield* harness();

        h.crashAdmission(true);
        expect((yield* h.host.start(request("crash")).pipe(Effect.exit))._tag).toBe("Failure");
        expect(h.deliveries.size).toBe(1);
        const retained = [...h.deliveries.values()][0]!;

        expect(retained.receipt).toBeNull();
        expect(
          h.logs
            .get(sourceId)
            ?.filter(({ record }) => record.payload._tag === "WorkerInputRequested"),
        ).toHaveLength(1);
        h.crashAdmission(false);
        yield* TestClock.adjust("31 seconds");
        const started = yield* h.host.start(request("crash"));

        expect(started.worker.threadId).toBe(retained.envelope.threadId);
        expect(h.deliveries.size).toBe(1);
      }),
  );

  it.effect(
    "requires separate read, send and control grants; mismatched Receipt cannot broaden cancellation",
    () =>
      Effect.gen(function* () {
        const h = yield* harness();

        h.deny("send");
        expect((yield* h.host.start(request("denied")).pipe(Effect.flip)).reason).toBe("denied");
        expect(h.deliveries.size).toBe(0);
        h.deny(undefined);
        const first = yield* h.host.start(request("first"));

        h.deny("control");
        expect(
          (yield* h.host
            .cancel({ worker: first.worker, target, receipt: first.receipt })
            .pipe(Effect.flip))._tag,
        ).toBe("WorkerError");
        h.deny(undefined);

        const wrong = Receipt.make({
          ...first.receipt,
          receiptId: Schema.decodeSync(ReceiptId)("wrong"),
        });

        expect(
          (yield* h.host.cancel({ worker: first.worker, target, receipt: wrong }).pipe(Effect.flip))
            ._tag,
        ).toBe("WorkerError");
        expect(
          (yield* h.runtime
            .acquire({
              sourceThreadId: sourceId,
              principal: Schema.decodeSync(Principal)("intruder"),
            })
            .pipe(Effect.flip)).reason,
        ).toBe("denied");
      }),
  );

  it.effect(
    "a send grant can acquire source context without permission to read worker results",
    () =>
      Effect.gen(function* () {
        const h = yield* harness();

        h.deny("read");
        const sender = yield* h.runtime.acquire({ sourceThreadId: sourceId, principal });

        expect((yield* sender.context).source.threadId).toBe(sourceId);
        const accepted = yield* sender.start(request("send-only"));

        expect(
          (yield* sender
            .inspect({ worker: accepted.worker, target, receipt: accepted.receipt })
            .pipe(Effect.flip)).reason,
        ).toBe("denied");
      }),
  );

  for (const point of [
    "worker:before-report-append",
    "worker:after-report-append",
    "worker:before-report-delivery",
    "worker:after-report-delivery",
  ] as const) {
    it.effect(`repairs one frozen Run report after ${point}`, () =>
      Effect.gen(function* () {
        let calls = 0;

        const h = yield* harness({
          sourceReports: [
            reportWith(() =>
              Effect.sync(() => {
                calls++;

                return { encodedInput: "reported" };
              }),
            ),
          ],
        });

        const first = yield* h.host.start(request("report-crash"));

        expect(
          h.submissions.get(first.receipt.submissionId)?.workerAdmission?.origin.reporting
            ?.sourceDigests,
        ).toEqual(definitions);
        h.fail(point);
        expect((yield* h.settle(first.receipt).pipe(Effect.exit))._tag).toBe("Failure");
        h.fail(undefined);
        yield* h.runtime.completeInput(h.submissions.get(first.receipt.submissionId)!);
        yield* h.runtime.completeInput(h.submissions.get(first.receipt.submissionId)!);

        const decisions = h.logs
          .get(first.worker.threadId)!
          .filter(({ record }) => record.payload._tag === "WorkerReportPrepared");

        expect(decisions).toHaveLength(1);

        const rows = [...h.deliveries.values()].filter(
          (row) => row.key.ownerThreadId === first.worker.threadId,
        );

        expect(rows).toHaveLength(1);
        expect(rows[0]?.envelope.threadId).toBe(sourceId);
        expect(rows[0]?.envelope.input).toBe("reported");
        expect(rows[0]?.status).toBe("pending");
        expect(calls).toBe(point === "worker:before-report-append" ? 2 : 1);
        const payload = decisions[0]?.record.payload;

        if (payload?._tag !== "WorkerReportPrepared") throw new Error("missing report");
        expect((yield* Schema.decodeUnknownEffect(PreparedInput)(payload.envelope)).input).toBe(
          "reported",
        );
      }),
    );
  }

  it.effect(
    "captures report services outside Attempts and normalizes declared application failures",
    () =>
      Effect.gen(function* () {
        let fail = false;
        let released = 0;

        const model = Layer.effectContext<Agent.ModelServices, never, never>(
          Effect.die("Report capture must not acquire the model"),
        );

        const binding = yield* DurableWorkerBinding.make(
          { definition: sourceAgent, model },
          definitions,
          [
            {
              ...reportWith(() => Effect.succeed({ encodedInput: "unused" })),
              prepare: () =>
                Effect.gen(function* () {
                  yield* Effect.acquireRelease(Effect.void, () =>
                    Effect.sync(() => {
                      released++;
                    }),
                  );
                  const text = yield* ReportService;

                  if (fail) return yield* PrivateReportFailure.make({ secret: "private failure" });

                  return { encodedInput: text };
                }),
            },
          ],
        ).pipe(Effect.provideService(ReportService, "captured-service"));

        const h = yield* harness({ sourceReports: binding.reporting });
        const first = yield* h.host.start(request("captured"));

        yield* h.settle(first.receipt);
        expect(released).toBe(1);
        expect(
          [...h.deliveries.values()].find((row) => row.key.ownerThreadId === first.worker.threadId)
            ?.envelope.input,
        ).toBe("captured-service");

        const second = yield* h.host.followUp({
          worker: first.worker,
          target,
          idempotencyKey: Schema.decodeSync(IdempotencyKey)("failure"),
          encodedInput: { text: "second" },
          encodedParameters: { note: "second" },
        });

        fail = true;
        yield* h.settle(second);
        expect(released).toBe(2);
        expect(
          h.logs
            .get(first.worker.threadId)!
            .find(({ record }) => record.payload._tag === "WorkerReportRefused")?.record.payload,
        ).toMatchObject({ reason: "preparation" });
        expect(JSON.stringify(h.logs.get(first.worker.threadId))).not.toContain("private failure");
      }),
  );

  it.effect("projects the actual Run once for joined Receipts using host parameters", () =>
    Effect.gen(function* () {
      const observations: Array<unknown> = [];

      const h = yield* harness({
        sourceReports: [
          reportWith((report) =>
            Effect.sync(() => {
              observations.push(report.observation);

              return { encodedInput: "joined-report" };
            }),
          ),
        ],
      });

      const first = yield* h.host.start(request("host-input"));

      const second = yield* h.host.followUp({
        worker: first.worker,
        target,
        idempotencyKey: Schema.decodeSync(IdempotencyKey)("joined"),
        encodedInput: { text: "joined" },
        encodedParameters: { note: "member" },
      });

      yield* h.settle(first.receipt, "actual");
      yield* h.settle(second, "ignored", { host: first.receipt });
      expect(observations).toHaveLength(1);
      expect(observations[0]).toMatchObject({
        encodedParameters: { note: "host-input" },
        encodedResult: "actual",
        receipt: first.receipt,
      });
      expect(
        [...h.deliveries.values()].filter((row) => row.key.ownerThreadId === first.worker.threadId),
      ).toHaveLength(1);
    }),
  );

  for (const [reason, prepare] of [
    ["projection", () => WorkerReportPreparationFailure.make({ stage: "projection" })],
    ["input", () => Effect.succeed({ encodedInput: 123 })],
    ["defect", () => Effect.die("private mapper diagnostic")],
  ] as const)
    it.effect(`retains permanent bounded report ${reason} refusal`, () =>
      Effect.gen(function* () {
        let calls = 0;

        const h = yield* harness({
          sourceReports: [
            reportWith(() =>
              Effect.suspend(() => {
                calls++;

                return prepare();
              }),
            ),
          ],
        });

        const first = yield* h.host.start(request("refused"));

        yield* h.settle(first.receipt);
        yield* h.runtime.completeInput(h.submissions.get(first.receipt.submissionId)!);

        const decisions = h.logs
          .get(first.worker.threadId)!
          .filter(({ record }) => record.payload._tag === "WorkerReportRefused");

        expect(decisions).toHaveLength(1);
        expect(decisions[0]?.record.payload).toMatchObject({ reason });
        expect(JSON.stringify(decisions)).not.toContain("private mapper diagnostic");
        expect(calls).toBe(1);
        expect(
          [...h.deliveries.values()].filter(
            (row) => row.key.ownerThreadId === first.worker.threadId,
          ),
        ).toHaveLength(0);
      }),
    );

  it.effect("bounds report preparation and finalizes its resources on timeout", () =>
    Effect.gen(function* () {
      let finalized = 0;

      const h = yield* harness({
        sourceReports: [
          reportWith(() =>
            Effect.never.pipe(
              Effect.ensuring(
                Effect.sync(() => {
                  finalized++;
                }),
              ),
            ),
          ),
        ],
      });

      const first = yield* h.host.start(request("timeout"));
      const fiber = yield* h.settle(first.receipt).pipe(Effect.forkChild);

      yield* TestClock.adjust("5 seconds");
      yield* Fiber.join(fiber);
      expect(finalized).toBe(1);
      expect(
        h.logs
          .get(first.worker.threadId)!
          .find(({ record }) => record.payload._tag === "WorkerReportRefused")?.record.payload,
      ).toMatchObject({ reason: "timeout" });
    }),
  );

  it.effect("interrupted preparation leaves repairable work without a false refusal", () =>
    Effect.gen(function* () {
      let block = true;
      let finalized = 0;

      const h = yield* harness({
        sourceReports: [
          reportWith(() =>
            (block ? Effect.never : Effect.succeed({ encodedInput: "resumed" })).pipe(
              Effect.ensuring(
                Effect.sync(() => {
                  finalized++;
                }),
              ),
            ),
          ),
        ],
      });

      const first = yield* h.host.start(request("interrupted"));
      const fiber = yield* h.settle(first.receipt).pipe(Effect.forkChild);

      yield* Effect.yieldNow;
      yield* Fiber.interrupt(fiber);
      block = false;
      yield* h.runtime.completeInput(h.submissions.get(first.receipt.submissionId)!);
      expect(finalized).toBe(2);
      expect(
        h.logs
          .get(first.worker.threadId)!
          .filter(({ record }) => record.payload._tag === "WorkerReportRefused"),
      ).toHaveLength(0);
      expect(
        h.logs
          .get(first.worker.threadId)!
          .filter(({ record }) => record.payload._tag === "WorkerReportPrepared"),
      ).toHaveLength(1);
    }),
  );

  it.effect("omits reports for run-less aborts and refuses missing exact report code", () =>
    Effect.gen(function* () {
      const reports = [reportWith(() => Effect.succeed({ encodedInput: "reported" }))];
      const h = yield* harness({ sourceReports: reports });
      const first = yield* h.host.start(request("runless"));

      yield* h.settle(first.receipt, "", { abortedBeforeRun: true });
      expect(
        h.logs
          .get(first.worker.threadId)!
          .filter(({ record }) => record.payload._tag.startsWith("WorkerReport")),
      ).toHaveLength(0);

      const second = yield* h.host.followUp({
        worker: first.worker,
        target,
        idempotencyKey: Schema.decodeSync(IdempotencyKey)("next"),
        encodedInput: { text: "next" },
        encodedParameters: { note: "next" },
      });

      reports.splice(0);
      yield* h.settle(second);
      expect(
        h.logs
          .get(first.worker.threadId)!
          .find(({ record }) => record.payload._tag === "WorkerReportRefused")?.record.payload,
      ).toMatchObject({ reason: "declaration-unavailable" });
    }),
  );

  it.effect("observes a finite bounded history snapshot and resumes from canonical sequence", () =>
    Effect.gen(function* () {
      const h = yield* harness();
      const first = yield* h.host.start(request("history"));

      for (let index = 0; index < 520; index++)
        h.push(
          first.worker.threadId,
          UserInputRecorded.make({ kind: "user", input: `history-${index}` }),
          `history-${index}`,
        );

      const history = yield* h.host.observe({ worker: first.worker, target }).pipe(
        Stream.tap((entry) =>
          entry.sequence === 1
            ? Effect.sync(() =>
                h.push(
                  first.worker.threadId,
                  UserInputRecorded.make({ kind: "user", input: "after snapshot" }),
                  "after-snapshot",
                ),
              )
            : Effect.void,
        ),
        Stream.runCollect,
      );

      expect(history).toHaveLength(522);
      expect(history[521]?.sequence).toBe(522);
      expect(history[0]?.record).toMatchObject({ payload: { _tag: "ThreadCreated" } });

      const suffix = yield* h.host
        .observe({ worker: first.worker, target, after: 520 })
        .pipe(Stream.runCollect);

      expect(suffix.map((entry) => entry.sequence)).toEqual([521, 522, 523]);
      h.deny("read");
      expect(
        (yield* h.host
          .observe({ worker: first.worker, target })
          .pipe(Stream.runCollect, Effect.flip)).reason,
      ).toBe("denied");
    }),
  );

  it.effect(
    "charges an explicitly mapped report input to the receiving worker's original ancestor",
    () =>
      Effect.gen(function* () {
        const scoutId = Schema.decodeSync(DelegationId)("scout");

        const h = yield* harness({
          targetReports: [
            {
              delegationId: scoutId,
              target,
              input: target.input,
              destination: { delegationId: Schema.decodeSync(DelegationId)("research"), target },
              prepare: () =>
                Effect.succeed({
                  encodedInput: { text: "scout-result" },
                  encodedParameters: { note: "explicit-report-parameters" },
                }),
            },
          ],
        });

        const initial = request("report-builder");
        const grant = SubagentGrant.make({ allowedToolNames: [], maxDepth: 2 });

        const builder = yield* h.host.start({
          ...initial,
          encodedGrant: grant,
          budget: {
            ...initial.budget,
            descendantInvocations: 1,
            caps: SubagentDelegationCaps.make({ ...initial.budget.caps, maxConcurrentChildren: 2 }),
            allocation: SubagentReservationAmounts.make({
              ...initial.budget.allocation,
              turns: 3,
              toolCalls: 3,
              durationMillis: 1_500,
              resultBytes: 1_280,
            }),
          },
        });

        const childPolicy = AgentPolicy.make({
          ...policy,
          maxTurns: 1,
          maxToolCalls: 1,
          maxDuration: "500 millis",
          toolConcurrency: 1,
          toolResultBounds: ToolResultBounds.make({ maxBytes: 256 }),
        });

        const nested = h.runtime.facet(
          {
            source: {
              _tag: "tool",
              agentId: target.id,
              threadId: builder.worker.threadId,
              runId: Schema.decodeSync(RunId)("builder-run"),
              toolCallId: Schema.decodeSync(ToolCallId)("scout-call"),
            },
            policy,
            depth: 1,
            grant,
          },
          principal,
          builder.receipt.submissionId,
        );

        const scout = yield* nested.start({
          ...request("report-scout"),
          delegationId: scoutId,
          policy: childPolicy,
          encodedGrant: grant,
          budget: {
            caps: SubagentDelegationCaps.make({
              maxTotalChildInvocations: 1,
              maxConcurrentChildren: 1,
              maxTurns: 1,
              maxToolCalls: 1,
              maxDurationMillis: 500,
              maxResultBytes: 256,
            }),
            allocation: SubagentReservationAmounts.make({
              turns: 1,
              toolCalls: 1,
              durationMillis: 500,
              resultBytes: 256,
              inputTokens: 0,
              outputTokens: 0,
              costMicrousd: 0,
            }),
          },
        });

        yield* h.settle(scout.receipt);

        const report = [...h.deliveries.values()].find(
          (row) => row.key.ownerThreadId === scout.worker.threadId,
        )!;

        expect(report.envelope.threadId).toBe(builder.worker.threadId);
        expect(report.envelope.workerAdmission?.parameters).toEqual({
          note: "explicit-report-parameters",
        });
        expect(report.envelope.workerAdmission?.origin).toEqual(
          h.submissions.get(builder.receipt.submissionId)?.workerAdmission?.origin,
        );
        expect(report.envelope.workerAdmission?.sourceSubmissionId).toBeUndefined();
        const metadata = report.envelope.workerAdmission!;

        const options = {
          threadId: report.envelope.threadId,
          principal: report.envelope.deliveryPrincipal,
          idempotencyKey: report.envelope.admissionKey,
          definitions: report.envelope.definitions,
          workerAdmission: metadata,
        };

        yield* h.runtime.validateAdmission(
          metadata,
          options,
          report.envelope.agentId,
          report.envelope.inputDigest,
        );
        yield* h.runtime.validateAdmission(
          metadata,
          options,
          report.envelope.agentId,
          report.envelope.inputDigest,
        );

        const charged = h.logs
          .get(sourceId)!
          .filter(({ record }) => record.payload._tag === "WorkerInputRequested");

        expect(charged).toHaveLength(2);

        const subtree = h.logs
          .get(sourceId)!
          .filter(({ record }) => record.payload._tag === "SubtreeBudgetReserved");

        expect(subtree).toHaveLength(2);
        // The initial input and report occupy the same worker slot but exhaust its pending-input cap.
        expect(
          (yield* h.host
            .followUp({
              worker: builder.worker,
              target,
              idempotencyKey: Schema.decodeSync(IdempotencyKey)("beyond-report"),
              encodedInput: { text: "extra" },
              encodedParameters: { note: "extra" },
            })
            .pipe(Effect.flip)).reason,
        ).toBe("capacity");
      }),
  );

  for (const point of [
    "worker:before-subtree-append",
    "worker:after-subtree-append",
    "worker:before-source-append",
    "worker:after-source-append",
    "worker:before-origin-append",
    "worker:after-origin-append",
  ] as const) {
    it.effect(`recovers retained creation after ${point}`, () =>
      Effect.gen(function* () {
        const h = yield* harness();

        h.fail(point);
        expect((yield* h.host.start(request("failpoint")).pipe(Effect.exit))._tag).toBe("Failure");
        h.fail(undefined);
        yield* TestClock.adjust("31 seconds");
        const started = yield* h.host.start(request("failpoint"));

        expect(started.receipt.threadId).toBe(started.worker.threadId);
        expect(
          h.logs
            .get(sourceId)
            ?.filter(({ record }) => record.payload._tag === "WorkerInputRequested"),
        ).toHaveLength(1);
        expect(
          h.logs
            .get(started.worker.threadId)
            ?.filter(({ record }) => record.payload._tag === "WorkerOriginRecorded"),
        ).toHaveLength(1);
      }),
    );
  }
  for (const point of [
    "worker:before-completion-append",
    "worker:after-completion-append",
  ] as const) {
    it.effect(`repairs capacity acknowledgement after ${point}`, () =>
      Effect.gen(function* () {
        const h = yield* harness();
        const first = yield* h.host.start(request("completion"));

        h.fail(point);
        expect((yield* h.settle(first.receipt).pipe(Effect.exit))._tag).toBe("Failure");
        h.fail(undefined);
        yield* h.runtime.completeInput(h.submissions.get(first.receipt.submissionId)!);
        yield* h.runtime.completeInput(h.submissions.get(first.receipt.submissionId)!);
        expect(
          h.logs
            .get(sourceId)
            ?.filter(({ record }) => record.payload._tag === "WorkerInputCompleted"),
        ).toHaveLength(1);
      }),
    );
  }
});

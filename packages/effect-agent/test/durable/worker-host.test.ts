import { NodeCrypto } from "@effect/platform-node";
import { expect, layer } from "@effect/vitest";
import {
  Clock,
  Context,
  DateTime,
  Deferred,
  Duration,
  Effect,
  Fiber,
  Layer,
  Option,
  Schema,
  Stream,
} from "effect";
import * as Agent from "effect-agent/agent";
import { AgentPolicy } from "effect-agent/agent-policy";
import {
  DelegationId,
  ReceiptId,
  RunId,
  SubmissionId,
  ThreadId,
  ToolCallId,
} from "effect-agent/identifiers";
import { IdempotencyKey, JoinedToHost, QueueSequence, Receipt } from "effect-agent/receipt";
import * as Subagent from "effect-agent/subagent";
import {
  SubagentDelegationCaps,
  SubagentGrant,
  SubagentParentLink,
  SubagentReservationAmounts,
} from "effect-agent/subagent-contract";
import {
  SubagentHost,
  WorkerReportPreparationFailure,
  type StartWorkerRequest,
  type WorkerReporting,
} from "effect-agent/subagent-host";
import { ToolResultBounds } from "effect-agent/tool-result";
import { WorkerCompletion, WorkerError, WorkerUpdate } from "effect-agent/worker";
import { TestClock } from "effect/testing";
import { Toolkit } from "effect/unstable/ai";

import { automaticReporting } from "../../src/capabilities/internal/subagent-reporting.ts";
import { DurableWorkerBinding } from "../../src/durable/AgentRegistration.ts";
import { digestJson } from "../../src/durable/Digest.ts";
import {
  DurableRuntimeFailpoint,
  DurableRuntimeFailpointError,
  type DurableRuntimeFailpointLocation,
} from "../../src/durable/DurableFailpoint.ts";
import { makeAgentUpdateRuntime } from "../../src/durable/internal/agent-updates.ts";
import { makeWorkerRuntime, WorkerInputControl } from "../../src/durable/internal/worker-host.ts";
import { WorkerRuntime } from "../../src/durable/internal/worker-runtime.ts";
import {
  MessageDeliveryStore,
  defaultMessageDeliveryStoreLimits,
  applyMessageDeliveryChange,
  type MessageDeliveryRecord,
  type MessageDeliveryStoreLimits,
} from "../../src/durable/MessageDelivery.ts";
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
  RunStartedRecord,
  SubmissionSettled,
  SubmissionSettledRecord,
  SubtreeBudgetReserved,
  SubagentLineageRecorded,
  ThreadCreated,
  UserInputRecorded,
  WorkerInputRequested,
  WorkerOrigin,
  WorkerOriginRecorded,
  WorkerReportPrepared,
  ToolCallPrepared,
  ToolCallUnknown,
  type CanonicalRecordPayload,
} from "../../src/durable/Records.ts";
import { subagentLineageRecordId, workerOriginRecordId } from "../../src/durable/RunJournal.ts";
import {
  AbortIntent,
  AdmissionPolicyError,
  Principal,
  Settlement,
  SubmissionSnapshot,
  SubmissionLedger,
  submissionSettlementId,
} from "../../src/durable/SubmissionLedger.ts";
import { PendingSubmission, SettledSubmission } from "../../src/durable/SubmissionStatus.ts";
import { PreparedInput } from "../../src/durable/Subscription.ts";
import { WakeScheduler } from "../../src/durable/WakeScheduler.ts";
import {
  WorkerBudgetAuthorizer,
  WorkerConcurrencyResolver,
  WorkerHostAuthorizer,
  WorkerHostConfig,
  WorkerPolicyResolver,
} from "../../src/durable/WorkerHost.ts";

class ReportService extends Context.Service<ReportService, string>()("test/ReportService") {}
class PrivateReportFailure extends Schema.TaggedError<PrivateReportFailure>()(
  "PrivateReportFailure",
  { secret: Schema.String },
) {}
import {
  AppendConflict,
  AppendResult,
  FenceRejected,
  ThreadExport,
  ThreadIdentity,
  ThreadNotMaterialized,
  ThreadTail,
  ThreadStore,
  ThreadStoreError,
  type ThreadReadRequest,
} from "../../src/durable/ThreadStore.ts";

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

const reportDeclaration = Subagent.make("research", {
  target,
  parameters: Schema.Struct({ note: Schema.String }),
  prepareInput: ({ note }) => Effect.succeed({ text: note }),
});

const rawReport = automaticReporting(reportDeclaration);

const standardReport: WorkerReporting<WorkerReportPreparationFailure> = {
  ...rawReport,
  prepare: (report) =>
    rawReport
      .prepare(report)
      .pipe(Effect.mapError(() => WorkerReportPreparationFailure.make({ stage: "projection" }))),
};

const reportWith = (
  prepare: WorkerReporting<WorkerReportPreparationFailure>["prepare"] = standardReport.prepare,
): WorkerReporting<WorkerReportPreparationFailure> => ({ ...standardReport, prepare });

const harness = Effect.fn("workerHostHarness")(function* (
  options: {
    readonly independentBudget?: boolean;
    readonly limits?: Partial<typeof WorkerHostConfig.Service>;
    readonly deliveryLimits?: Partial<MessageDeliveryStoreLimits>;
    readonly maxStoredValueBytes?: number;
    readonly authorize?: (typeof WorkerHostAuthorizer.Service)["authorize"];
    readonly beforeRead?: (request: ThreadReadRequest) => Effect.Effect<void, ThreadStoreError>;
    readonly afterIdentity?: (snapshot: ThreadIdentity) => Effect.Effect<void, ThreadStoreError>;
    readonly sourceRevisions?: ReadonlyArray<{
      readonly definition: Agent.AnyDefinition;
      readonly digests: DefinitionDigests;
      readonly reporting?: ReadonlyArray<WorkerReporting<WorkerReportPreparationFailure>>;
    }>;
    readonly sourceReports?: ReadonlyArray<WorkerReporting<WorkerReportPreparationFailure>>;
    readonly targetReports?: ReadonlyArray<WorkerReporting<WorkerReportPreparationFailure>>;
  } = {},
) {
  const now = yield* Clock.currentTimeMillis;
  const logs = new Map<ThreadId, Array<CanonicalRecordEnvelope>>();
  const epochs = new Map<ThreadId, ProducerEpoch>();
  const appendAttempts: Array<{ readonly threadId: ThreadId; readonly epoch: ProducerEpoch }> = [];
  const rejectedAppends = { tail: 0, epoch: 0 };
  const reads = { exported: 0, paged: 0, worker: 0, exact: 0, identity: 0 };
  const deliveries = new Map<string, MessageDeliveryRecord>();
  const submissions = new Map<SubmissionId, SubmissionSnapshot>();
  const settlements = new Map<SubmissionId, Settlement>();
  let failpoint: DurableRuntimeFailpointLocation | undefined;
  let denied: "read" | "send" | "report" | "control" | undefined;
  let joined: SubmissionId | undefined;
  let admissionFailure = false;
  let isolatedThread: ThreadId | undefined;

  const checkThread = (threadId: ThreadId | undefined) => {
    if (isolatedThread !== undefined) expect(threadId).toBe(isolatedThread);
  };

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
    if (!epochs.has(threadId)) epochs.set(threadId, Schema.decodeSync(ProducerEpoch)(0));
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

  const runtimes = yield* makeWorkerRuntime({
    deploymentId: Schema.decodeSync(DeploymentId)("test"),
    producerId: Schema.decodeSync(ProducerId)("test"),
    settlementPollInterval: Duration.millis(5),
    bindings: [
      sourceAgent,
      target,
      ...(options.sourceRevisions ?? []).map((revision) => revision.definition),
    ]
      .filter(
        (definition, index, entries) =>
          entries.findLastIndex((entry) => entry.id === definition.id) === index,
      )
      .map((definition) => ({
        definition,
        agentId: definition.id,
        digests:
          options.sourceRevisions?.find((revision) => revision.definition === definition)
            ?.digests ?? definitions,
        attempt: () => Effect.succeed(Option.none()),
        reporting:
          definition === sourceAgent
            ? (options.sourceReports ?? [])
            : (options.sourceRevisions?.find((revision) => revision.definition === definition)
                ?.reporting ??
              options.targetReports ??
              []),
      })),
  }).pipe(
    Effect.flatMap((runtime) =>
      makeAgentUpdateRuntime({
        deploymentId: Schema.decodeSync(DeploymentId)("test"),
        producerId: Schema.decodeSync(ProducerId)("test"),
      }).pipe(
        Effect.provideService(WorkerRuntime, runtime),
        Effect.provide(WakeScheduler.layerNoop),
        Effect.map((updates) => ({ runtime, updates })),
      ),
    ),
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
      ...options.limits,
    }),
    Effect.provideService(WorkerHostAuthorizer, {
      authorize: (request) =>
        Effect.suspend(() => {
          checkThread(request.sourceThreadId);
          auth.push(request.access);

          return request.access === denied ||
            request.principal !== principal ||
            !logs.has(request.sourceThreadId)
            ? WorkerError.make({ operation: request.operation, reason: "denied" })
            : options.authorize === undefined
              ? Effect.succeed(principal)
              : options.authorize(request);
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
      readIdentity: ({ threadId }) =>
        Effect.gen(function* () {
          checkThread(threadId);
          const records = logs.get(threadId);

          if (records === undefined) return yield* ThreadNotMaterialized.make({ threadId });
          reads.identity++;
          const selected: Array<CanonicalRecordEnvelope> = [];

          for (const entry of [
            records[0],
            records.find(({ record }) => record.recordId === workerOriginRecordId(threadId)),
            records.find(({ record }) => record.recordId === subagentLineageRecordId(threadId)),
          ])
            if (entry !== undefined && !selected.includes(entry)) selected.push(entry);

          const snapshot = ThreadIdentity.make({
            threadId,
            tailSequence: Schema.decodeSync(CanonicalSequence)(records.length),
            tailDigest: digest,
            producerEpoch: epochs.get(threadId)!,
            records: selected,
          });

          yield* options.afterIdentity?.(snapshot) ?? Effect.void;

          return snapshot;
        }),
      read: (request) =>
        Stream.suspend(() => {
          checkThread(request.threadId);
          const all = logs.get(request.threadId) ?? [];
          const selection = "selection" in request ? request.selection : undefined;
          let records = all;

          if (selection?._tag === "RecordId") {
            records = all.filter((entry) => entry.record.recordId === selection.recordId);
            reads.exact++;
          } else if (selection?._tag === "WorkerExecution") {
            records = ["UserInputRecorded", "RunStarted"]
              .flatMap((tag) =>
                all
                  .filter(
                    ({ record: { payload } }) =>
                      payload._tag === tag && "runId" in payload && payload.runId !== undefined,
                  )
                  .slice(-1),
              )
              .sort((a, b) => a.sequence - b.sequence);
          } else if (selection?._tag === "RunInput") {
            records = all.filter(
              ({ record: { payload } }) =>
                payload._tag === "UserInputRecorded" &&
                payload.kind === "user" &&
                payload.runId === selection.runId,
            );
          } else if (selection?._tag === "WorkerState") {
            records = all.filter(({ record: { payload } }) =>
              payload._tag === "SubtreeBudgetReserved"
                ? payload.sourceSubmissionId === selection.sourceSubmissionId
                : payload._tag === "SubagentJoined"
                  ? payload.runId === `run:${selection.sourceSubmissionId}`
                  : [
                      "ThreadCreated",
                      "WorkerOriginRecorded",
                      "SubagentLineageRecorded",
                      "WorkerInputRequested",
                      "WorkerInputCompleted",
                    ].includes(payload._tag),
            );
            reads.worker += records.length;
          } else if (selection !== undefined)
            return Stream.die("Worker fixture only reads exact identities and accounting");
          const page = "selection" in request ? request.page : request;

          return Stream.fromIterable(
            records
              .filter((entry) => entry.sequence > (page.afterSequence ?? 0))
              .slice(0, page.limit)
              .map((entry) => {
                if (selection === undefined) reads.paged++;

                return entry;
              }),
          );
        }).pipe(Stream.onStart(Effect.suspend(() => options.beforeRead?.(request) ?? Effect.void))),
      export: ({ threadId }) =>
        Effect.suspend(() => {
          checkThread(threadId);
          const records = logs.get(threadId);

          reads.exported += records?.length ?? 0;

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
          checkThread(threadId);
          const records = logs.get(threadId);

          return records === undefined
            ? ThreadNotMaterialized.make({ threadId })
            : Effect.succeed(
                ThreadTail.make({
                  threadId,
                  tailSequence: Schema.decodeSync(CanonicalSequence)(records.length),
                  tailDigest: digest,
                  producerEpoch: epochs.get(threadId)!,
                }),
              );
        }),
      append: (request) =>
        Effect.gen(function* () {
          checkThread(request.threadId);
          yield* Effect.yieldNow;
          const records = logs.get(request.threadId) ?? [];

          appendAttempts.push({ threadId: request.threadId, epoch: request.producerEpoch });
          const currentEpoch = epochs.get(request.threadId)!;

          if (currentEpoch !== request.producerEpoch) {
            rejectedAppends.epoch++;

            return yield* FenceRejected.make({
              threadId: request.threadId,
              attemptedEpoch: request.producerEpoch,
              actualEpoch: currentEpoch,
            });
          }
          if (
            records.length !== request.expectedTailSequence ||
            digest !== request.expectedTailDigest
          ) {
            rejectedAppends.tail++;

            return yield* AppendConflict.make({
              threadId: request.threadId,
              batchId: request.batch.batchId,
              reason: "tail",
            });
          }
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
      limits: { ...defaultMessageDeliveryStoreLimits, ...options.deliveryLimits },
      maxStoredValueBytes: options.maxStoredValueBytes ?? 16 * 1_024 * 1_024,
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
      list: ({ ownerThreadId, limit, after, workerStarts, pendingWorker }) =>
        Effect.sync(() => {
          const rows = [...deliveries.values()]
            .filter((record) => {
              const origin = record.envelope.workerAdmission?.origin;

              return (
                record.key.ownerThreadId === ownerThreadId &&
                (after === undefined || record.key.messageId > after) &&
                (workerStarts === undefined ||
                  (origin?.worker.delegationId === workerStarts.delegationId &&
                    origin.worker.targetAgentId === workerStarts.targetAgentId &&
                    origin.firstMessageId === record.key.messageId)) &&
                (pendingWorker === undefined ||
                  (origin?.worker.threadId === pendingWorker &&
                    record.receipt === null &&
                    (record.status === "pending" || record.status === "parked")))
              );
            })
            .sort((a, b) => a.key.messageId.localeCompare(b.key.messageId));

          return {
            items: rows.slice(0, limit),
            next: rows.length > limit ? rows[limit - 1]!.key.messageId : null,
          };
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
      inspectWorker: (threadId) =>
        Effect.sync(() => {
          const rows = [...submissions.values()]
            .filter((row) => row.threadId === threadId)
            .sort((a, b) => a.queueSequence - b.queueSequence);

          return {
            latest: rows.at(-1) ?? null,
            active: rows.find((row) => row.state !== "settled") ?? null,
            stopped: false,
          };
        }),
      lookup: (request) =>
        Effect.sync(() => {
          checkThread(
            request._tag === "SubmissionLookupById"
              ? submissions.get(request.submissionId)?.threadId
              : request.threadId,
          );

          return request._tag === "SubmissionLookupById"
            ? lookup(request.submissionId)
            : Option.fromNullishOr(
                [...submissions.values()].find(
                  (row) =>
                    row.threadId === request.threadId &&
                    row.principal === request.principal &&
                    row.idempotencyKey === request.idempotencyKey,
                ),
              );
        }),

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
            .validateAdmission(
              metadata,
              options,
              envelope.agentId,
              envelope.inputDigest,
              envelope.input,
            )
            .pipe(
              Effect.mapError((error) =>
                AdmissionPolicyError.make({
                  reason:
                    error.reason === "storage" || error.reason === "unavailable"
                      ? "unavailable"
                      : "refused",
                  code: `worker-${error.reason}`,
                  cause: error,
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

  runtime = runtimes.runtime;
  const ownerId = Schema.decodeSync(SubmissionId)("report-owner");

  if (options.sourceReports?.length && !options.sourceRevisions?.length)
    submissions.set(
      ownerId,
      SubmissionSnapshot.make({
        submissionId: ownerId,
        threadId: sourceId,
        queueSequence: Schema.decodeSync(QueueSequence)(1),
        principal,
        idempotencyKey: Schema.decodeSync(IdempotencyKey)("report-owner"),
        agentId: sourceAgent.id,
        agentDigests: definitions,
        deploymentId: Schema.decodeSync(DeploymentId)("test"),
        inputPayload: "original parent input",
        inputDigest: digest,
        receiptId: Schema.decodeSync(ReceiptId)("report-owner"),
        state: "settled",
        createdAt: DateTime.makeUnsafe(now),
      }),
    );

  const host = yield* runtime.acquire({
    sourceThreadId: sourceId,
    principal,
    ...(options.sourceReports?.length && !options.sourceRevisions?.length
      ? { sourceSubmissionId: ownerId }
      : {}),
  });

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
    reads,
    updates: runtimes.updates,
    host,
    deliveries,
    logs,
    epochs,
    appendAttempts,
    rejectedAppends,
    submissions,
    auth,
    settle,
    push,
    isolate: (value: ThreadId | undefined) => {
      isolatedThread = value;
    },
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
  // Regression: https://github.com/danieljvdm/effect-agent/pull/621
  for (const advance of ["tail", "epoch"] as const)
    it.effect(`origin establishment retries a ${advance} advance after its identity snapshot`, () =>
      Effect.gen(function* () {
        let onIdentity: Effect.Effect<void, ThreadStoreError> = Effect.void;
        const h = yield* harness({ afterIdentity: () => Effect.suspend(() => onIdentity) });
        const started = yield* h.host.start(request(`origin-${advance}-takeover`));
        const threadId = started.worker.threadId;

        const origin = h.submissions.get(started.delivery.receipt!.submissionId)!.workerAdmission!
          .origin;

        h.logs.set(threadId, h.logs.get(threadId)!.slice(0, 1));
        h.appendAttempts.length = 0;
        const before = h.reads.identity;

        onIdentity = Effect.sync(() => {
          onIdentity = Effect.void;
          if (advance === "epoch") h.epochs.set(threadId, Schema.decodeSync(ProducerEpoch)(1));
          else
            h.push(threadId, UserInputRecorded.make({ kind: "steering", input: "raced" }), "raced");
        });
        yield* h.runtime.ensureOrigin(origin);
        expect(h.rejectedAppends[advance]).toBe(1);
        expect(h.reads.identity - before).toBe(2);
        expect(h.appendAttempts.map((attempt) => attempt.epoch)).toEqual(
          advance === "epoch" ? [0, 1] : [0, 0],
        );
        expect(h.logs.get(threadId)!.map(({ record }) => record.payload._tag)).toEqual(
          advance === "epoch"
            ? ["ThreadCreated", "WorkerOriginRecorded"]
            : ["ThreadCreated", "UserInputRecorded", "WorkerOriginRecorded"],
        );
      }),
    );

  // Regression: https://github.com/danieljvdm/effect-agent/commit/6a4f4f870
  it.effect(
    "origin establishment replays the same identity and refuses incompatible canonical ancestry",
    () =>
      Effect.gen(function* () {
        const h = yield* harness();
        const started = yield* h.host.start(request("origin-identity"));
        const threadId = started.worker.threadId;

        const origin = h.submissions.get(started.delivery.receipt!.submissionId)!.workerAdmission!
          .origin;

        const history = [...h.logs.get(threadId)!];

        yield* h.runtime.ensureOrigin(origin);
        expect(h.logs.get(threadId)).toEqual(history);

        const mismatch = yield* h.runtime
          .ensureOrigin({
            ...origin,
            firstMessageId: Schema.decodeSync(IdempotencyKey)("different-origin"),
          })
          .pipe(Effect.flip);

        expect(mismatch.reason).toBe("worker-mismatch");
        expect(h.logs.get(threadId)).toEqual(history);

        for (const incompatible of ["empty", "agent", "digests", "lineage"] as const) {
          h.logs.set(threadId, []);
          if (incompatible !== "empty")
            h.push(
              threadId,
              ThreadCreated.make({
                agentId: incompatible === "agent" ? sourceAgent.id : target.id,
                definitions:
                  incompatible === "digests"
                    ? DefinitionDigests.make({
                        ...definitions,
                        agent: Schema.decodeSync(Digest)("b".repeat(64)),
                      })
                    : definitions,
              }),
              "child-created",
            );
          if (incompatible === "lineage")
            h.push(
              threadId,
              SubagentLineageRecorded.make({
                parentLink: SubagentParentLink.make({
                  delegationId: origin.worker.delegationId,
                  parentAgentId: sourceAgent.id,
                  parentThreadId: sourceId,
                  parentRunId: Schema.decodeSync(RunId)("parent-run"),
                  parentToolCallId: Schema.decodeSync(ToolCallId)("parent-call"),
                  depth: 1,
                }),
                parentSubmissionId: Schema.decodeSync(SubmissionId)("parent-submission"),
                childDefinitionDigests: definitions,
                childInputDigest: digest,
                grantDigest: digest,
                policy: origin.policy,
                budget: origin.budget,
                grant: origin.grant,
              }),
              subagentLineageRecordId(threadId),
            );
          const before = [...h.logs.get(threadId)!];
          const refused = yield* h.runtime.ensureOrigin(origin).pipe(Effect.flip);

          expect(refused.reason).toBe("worker-mismatch");
          expect(h.logs.get(threadId)).toEqual(before);
        }
        h.logs.delete(threadId);
        expect((yield* h.runtime.ensureOrigin(origin).pipe(Effect.flip)).reason).toBe("storage");
      }),
  );

  it.effect("refuses a first reservation whose embedded worker or first message differs", () =>
    Effect.gen(function* () {
      for (const mismatch of ["worker", "message"] as const) {
        const h = yield* harness();
        const started = yield* h.host.start(request(`origin-${mismatch}`));
        const history = h.logs.get(sourceId)!;

        h.logs.set(
          sourceId,
          history.map((entry) => {
            const payload = entry.record.payload;

            if (payload._tag !== "WorkerInputRequested") return entry;

            return {
              ...entry,
              record: {
                ...entry.record,
                payload: WorkerInputRequested.make({
                  ...payload,
                  admission: {
                    ...payload.admission,
                    ...(mismatch === "message"
                      ? { messageId: Schema.decodeSync(IdempotencyKey)("wrong-first") }
                      : {}),
                    origin: {
                      ...payload.admission.origin,
                      worker:
                        mismatch === "worker"
                          ? { ...payload.admission.origin.worker, threadId: sourceId }
                          : payload.admission.origin.worker,
                    },
                  },
                }),
              },
            };
          }),
        );

        const failure = yield* h.host
          .followUp({
            worker: started.worker,
            target,
            idempotencyKey: Schema.decodeSync(IdempotencyKey)("followup"),
            encodedInput: { text: "next" },
            encodedParameters: {},
          })
          .pipe(Effect.flip);

        expect(failure.reason).toBe("worker-mismatch");
      }
    }),
  );

  it.effect(
    "worker start and follow-up exclude irrelevant conversation history from admission reads",
    () =>
      Effect.gen(function* () {
        const h = yield* harness({ limits: { maxPendingInputsPerWorker: 2 } });

        // Cross the ordinary recovery horizon; admission must still read only native authority/accounting.
        for (let i = 0; i < 4097; i++)
          h.push(
            sourceId,
            UserInputRecorded.make({ kind: "follow-up", input: `history-${i}` }),
            `history-${i}`,
          );
        const initial = request("bounded-start");

        yield* h.host.context;
        yield* h.host.resolveTargetPolicy({ target, encodedInput: initial.encodedInput });
        const started = yield* h.host.start(initial);

        const followUp = yield* h.host.followUp({
          worker: started.worker,
          target,
          idempotencyKey: Schema.decodeSync(IdempotencyKey)("bounded-followup"),
          encodedInput: { text: "followup" },
          encodedParameters: {},
        });

        expect(followUp.receipt!.threadId).toBe(started.worker.threadId);
        expect(yield* h.host.start(initial)).toEqual(started);
        expect(h.reads.exported).toBe(0);
        expect(h.reads.paged).toBeLessThan(32);
        expect(h.reads.worker).toBeLessThan(64);
      }),
  );

  it.effect("keeps terminal uncertain inputs current while allowing their native report", () =>
    Effect.gen(function* () {
      const h = yield* harness({
        sourceReports: [reportWith()],
      });

      const initial = request("uncertain-worker");
      const first = yield* h.host.start(initial);
      const runId = Schema.decodeSync(RunId)(`run:${first.delivery.receipt!.submissionId}`);
      const toolCallId = Schema.decodeSync(ToolCallId)("external-action");

      h.push(
        first.worker.threadId,
        ToolCallPrepared.make({
          runId,
          turnId: Schema.decodeSync(ToolCallPrepared.fields.turnId)("turn"),
          turn: 1,
          toolCallId,
          toolName: "supplier",
          parameters: { original: true },
          parametersDigest: digest,
        }),
        "prepared-action",
      );
      h.push(
        first.worker.threadId,
        ToolCallUnknown.make({
          runId,
          turn: 1,
          toolCallId,
          toolName: "supplier",
          reason: "lost reply",
        }),
        "unknown-action",
      );
      yield* h.settle(first.delivery.receipt!, "reported result");
      expect(
        h.logs
          .get(first.worker.threadId)!
          .filter(({ record }) => record.payload._tag === "WorkerInputCompleted"),
      ).toEqual([]);
      expect(yield* h.host.start(initial)).toEqual(first);
      expect(
        [...h.deliveries.values()].some(
          (entry) => entry.key.ownerThreadId === first.worker.threadId,
        ),
      ).toBe(true);
    }),
  );

  it.effect("distinguishes transient worker pressure from permanent input exhaustion", () =>
    Effect.gen(function* () {
      const h = yield* harness({ limits: { maxInputsPerWorker: 1, maxActiveWorkersPerSource: 1 } });
      const first = yield* h.host.start(request("capacity-first"));

      expect((yield* h.host.start(request("capacity-second"))).delivery).toMatchObject({
        status: "refused",
        reason: "worker-capacity",
      });

      const pending = [...h.deliveries.values()].find(
        (row) =>
          row.envelope.workerAdmission !== undefined &&
          row.envelope.workerAdmission.origin.worker.threadId !== first.worker.threadId,
      )!;

      const metadata = pending.envelope.workerAdmission!;

      expect(
        yield* h.runtime
          .validateAdmission(
            metadata,
            {
              threadId: pending.envelope.threadId,
              principal,
              idempotencyKey: pending.envelope.admissionKey,
              definitions,
            },
            pending.envelope.agentId,
            pending.envelope.inputDigest,
            pending.envelope.input,
          )
          .pipe(Effect.flip),
      ).toMatchObject({ reason: "capacity", retryable: true });

      const permanent = yield* h.host.followUp({
        worker: first.worker,
        target,
        idempotencyKey: Schema.decodeSync(IdempotencyKey)("exhausted"),
        encodedInput: { text: "extra" },
        encodedParameters: { note: "extra" },
      });

      expect(permanent).toMatchObject({ status: "refused", reason: "worker-capacity" });
      yield* h.settle(first.delivery.receipt!);
      expect((yield* h.host.start(request("capacity-third"))).worker.threadId).not.toBe(
        first.worker.threadId,
      );
    }),
  );

  for (const point of [
    "update:before-canonical-append",
    "update:after-canonical-append",
    "update:before-delivery-insert",
    "update:after-delivery-insert",
    "stored-byte-capacity",
    "source-unavailable",
  ] as const) {
    it.effect(`repairs an accepted parent update after ${point} without re-emission`, () =>
      Effect.gen(function* () {
        const h = yield* harness({
          deliveryLimits: { maxPendingUpdatesPerOwner: 1 },
          ...(point === "stored-byte-capacity" ? { maxStoredValueBytes: 64 } : {}),
          sourceReports: [
            {
              delegationId: Schema.decodeSync(DelegationId)("research"),
              target,
              prepare: (report) =>
                Schema.decodeUnknownEffect(WorkerCompletion)({
                  _tag: "WorkerCompletion",
                  schemaVersion: 1,
                  budgetExhausted: false,
                  report: {
                    ...report.observation,
                    worker: report.worker,
                    result: report.observation.encodedResult,
                  },
                }).pipe(
                  Effect.map((message) => ({ message })),
                  Effect.mapError(() =>
                    WorkerReportPreparationFailure.make({ stage: "projection" }),
                  ),
                ),
            },
          ],
        });

        const ownerId = Schema.decodeSync(SubmissionId)("owner-input");

        h.submissions.set(
          ownerId,
          SubmissionSnapshot.make({
            submissionId: ownerId,
            threadId: sourceId,
            queueSequence: Schema.decodeSync(QueueSequence)(1),
            principal,
            idempotencyKey: Schema.decodeSync(IdempotencyKey)("owner-input"),
            agentId: sourceAgent.id,
            agentDigests: definitions,
            deploymentId: Schema.decodeSync(DeploymentId)("test"),
            inputPayload: "original parent input",
            inputDigest: digest,
            receiptId: Schema.decodeSync(ReceiptId)("owner-receipt"),
            state: "settled",
            createdAt: DateTime.makeUnsafe(0),
          }),
        );

        const host = yield* h.runtime.acquire({
          sourceThreadId: sourceId,
          sourceSubmissionId: ownerId,
          principal,
        });

        const started = yield* host.start(request("report-updates"));
        const submission = h.submissions.get(started.delivery.receipt!.submissionId)!;
        const runId = Schema.decodeSync(RunId)(`run:${submission.submissionId}`);

        h.push(
          started.worker.threadId,
          RunStartedRecord.make({ runId, policyAccountingVersion: 1, maxDurationMillis: 10_000 }),
          "run-started",
        );

        const emission = {
          submission,
          runId,
          producerEpoch: Schema.decodeSync(ProducerEpoch)(0),
          definitions,
          updateId: Schema.decodeSync(IdempotencyKey)("finding"),
          value: { finding: "area concern" },
        };

        if (point === "stored-byte-capacity") {
          expect(yield* h.updates.emit(emission).pipe(Effect.flip)).toMatchObject({
            reason: "capacity",
          });
          expect(
            h.logs
              .get(started.worker.threadId)
              ?.filter(({ record }) => record.payload._tag === "AgentUpdateEmitted"),
          ).toHaveLength(0);

          return;
        }
        if (point === "source-unavailable") {
          const sourceLength = h.logs.get(sourceId)!.length;

          h.isolate(started.worker.threadId);
          h.deny("report");
          yield* h.updates.emit(emission);
          yield* h.settle(started.delivery.receipt!);
          h.isolate(undefined);
          expect(h.logs.get(sourceId)).toHaveLength(sourceLength);

          const published = [...h.deliveries.values()].filter(
            (row) => row.key.ownerThreadId === started.worker.threadId,
          );

          expect(published).toHaveLength(2);
          for (const row of published) {
            const message = row.envelope.messageAdmission;

            if (!Schema.is(WorkerUpdate)(message) && !Schema.is(WorkerCompletion)(message))
              throw new Error("Expected a framework report");

            const options = {
              threadId: row.envelope.threadId,
              principal: row.envelope.deliveryPrincipal,
              idempotencyKey: row.envelope.admissionKey,
              definitions: row.envelope.definitions,
            };

            // The destination's live permission check is independent of publication.
            expect(
              yield* h.runtime
                .validateCompletion(
                  message,
                  options,
                  row.envelope.agentId,
                  row.envelope.inputDigest,
                )
                .pipe(Effect.flip),
            ).toMatchObject({ reason: "denied" });
            h.deny(undefined);
            expect(
              yield* h.runtime.validateCompletion(
                message,
                options,
                row.envelope.agentId,
                row.envelope.inputDigest,
              ),
            ).toEqual(message);
            h.deny("report");
          }
          h.deny(undefined);

          const steered = yield* host.followUp({
            worker: started.worker,
            target,
            idempotencyKey: Schema.decodeSync(IdempotencyKey)("steering-after-report"),
            encodedInput: { text: "new information" },
            encodedParameters: { note: "steering" },
          });

          expect(h.submissions.get(steered.receipt!.submissionId)?.inputPayload).toEqual({
            text: "new information",
          });
          yield* host.cancel({ worker: started.worker, target, receipt: steered.receipt! });

          return;
        }
        h.fail(point);
        expect((yield* h.updates.emit(emission).pipe(Effect.exit))._tag).toBe("Failure");
        h.fail(undefined);
        if (point !== "update:before-canonical-append") {
          expect(
            yield* h.updates
              .emit({ ...emission, updateId: Schema.decodeSync(IdempotencyKey)("overflow") })
              .pipe(Effect.flip),
          ).toMatchObject({ reason: "capacity" });
          yield* h.updates.repair(started.worker.threadId);
        }
        const update = yield* h.updates.emit(emission);

        const rows = [...h.deliveries.values()].filter(
          (row) => row.key.ownerThreadId === started.worker.threadId,
        );

        expect(rows).toHaveLength(1);
        const row = rows[0]!;

        expect(row.envelope.input).toBe("original parent input");

        const message = yield* Schema.decodeUnknownEffect(WorkerUpdate)(
          row.envelope.messageAdmission,
        );

        expect(message.update).toEqual(update);

        const options = {
          threadId: row.envelope.threadId,
          principal: row.envelope.deliveryPrincipal,
          idempotencyKey: row.envelope.admissionKey,
          definitions: row.envelope.definitions,
        };

        expect(
          yield* h.runtime.validateCompletion(
            message,
            options,
            row.envelope.agentId,
            row.envelope.inputDigest,
          ),
        ).toEqual(message);
        expect(
          yield* h.runtime
            .validateCompletion(
              { ...message, update: { ...message.update, value: { finding: "forged" } } },
              options,
              row.envelope.agentId,
              row.envelope.inputDigest,
            )
            .pipe(Effect.flip),
        ).toMatchObject({ reason: "denied" });
        h.deny("report");
        expect(
          yield* h.runtime
            .validateCompletion(message, options, row.envelope.agentId, row.envelope.inputDigest)
            .pipe(Effect.flip),
        ).toMatchObject({ reason: "denied" });
        h.deny(undefined);
        yield* h.settle(started.delivery.receipt!);

        const completion = [...h.deliveries.values()].find((entry) =>
          Schema.is(WorkerCompletion)(entry.envelope.messageAdmission),
        );

        expect(completion?.predecessor).toBe(row.key.messageId);
      }),
    );
  }

  for (const point of [
    "update:before-canonical-append",
    "update:after-canonical-append",
  ] as const) {
    it.effect(`retains one update across ${point} and rejects changed replay`, () =>
      Effect.gen(function* () {
        const h = yield* harness();
        const started = yield* h.host.start(request("updating"));
        const submission = h.submissions.get(started.delivery.receipt!.submissionId)!;
        const runId = Schema.decodeSync(RunId)(`run:${submission.submissionId}`);

        h.push(
          started.worker.threadId,
          RunStartedRecord.make({ runId, policyAccountingVersion: 1, maxDurationMillis: 10_000 }),
          "run-started",
        );

        const input = {
          submission,
          runId,
          producerEpoch: Schema.decodeSync(ProducerEpoch)(0),
          definitions,
          updateId: Schema.decodeSync(IdempotencyKey)("finding"),
          value: { finding: "first" },
        };

        h.fail(point);
        expect((yield* h.updates.emit(input).pipe(Effect.exit))._tag).toBe("Failure");
        h.fail(undefined);
        const accepted = yield* h.updates.emit(input);

        expect(accepted).toMatchObject({ sequence: 1, value: { finding: "first" } });
        expect(yield* h.updates.emit(input)).toEqual(accepted);
        expect(
          yield* h.updates.emit({ ...input, value: { finding: "changed" } }).pipe(Effect.flip),
        ).toMatchObject({ reason: "conflict" });
        expect(
          h.logs
            .get(started.worker.threadId)
            ?.filter(({ record }) => record.payload._tag === "AgentUpdateEmitted"),
        ).toHaveLength(1);
        expect(
          yield* h.updates
            .emit({ ...input, updateId: Schema.decodeSync(IdempotencyKey)("second"), maxCount: 1 })
            .pipe(Effect.flip),
        ).toMatchObject({ reason: "capacity" });
        expect(
          yield* h.updates
            .emit({
              ...input,
              updateId: Schema.decodeSync(IdempotencyKey)("oversized"),
              maxBytes: 1,
            })
            .pipe(Effect.flip),
        ).toMatchObject({ reason: "capacity" });
        yield* h.settle(started.delivery.receipt!);
        expect(
          yield* h.updates
            .emit({ ...input, updateId: Schema.decodeSync(IdempotencyKey)("late") })
            .pipe(Effect.flip),
        ).toMatchObject({ reason: "identity" });
      }),
    );
  }

  it.effect("serializes competing durable updates and rejects stale emitting ownership", () =>
    Effect.gen(function* () {
      const h = yield* harness();
      const started = yield* h.host.start(request("parallel-updates"));
      const submission = h.submissions.get(started.delivery.receipt!.submissionId)!;
      const runId = Schema.decodeSync(RunId)(`run:${submission.submissionId}`);

      h.push(
        started.worker.threadId,
        RunStartedRecord.make({ runId, policyAccountingVersion: 1, maxDurationMillis: 10_000 }),
        "run-started",
      );

      const base = {
        submission,
        runId,
        producerEpoch: Schema.decodeSync(ProducerEpoch)(0),
        definitions,
        value: { finding: "parallel" },
      };

      const accepted = yield* Effect.forEach(
        ["first", "second", "third"],
        (key) => h.updates.emit({ ...base, updateId: Schema.decodeSync(IdempotencyKey)(key) }),
        { concurrency: 3 },
      );

      expect(accepted.map((update) => update.sequence).sort()).toEqual([1, 2, 3]);
      expect(
        yield* h.updates
          .emit({
            ...base,
            producerEpoch: Schema.decodeSync(ProducerEpoch)(2),
            updateId: Schema.decodeSync(IdempotencyKey)("stale"),
          })
          .pipe(Effect.flip),
      ).toMatchObject({ _tag: "LedgerError" });
    }),
  );

  it.effect("uses an admitted root Agent upgrade for workers without changing child lineage", () =>
    Effect.gen(function* () {
      const upgraded = Agent.make("upgraded-source-agent", {
        input: sourceAgent.input,
        output: sourceAgent.output,
        instructions: "Delegate and receive research reports",
        toolkit: Toolkit.empty,
        policy: sourceAgent.policy,
      });

      const upgradedDigests = DefinitionDigests.make({
        ...definitions,
        agent: Schema.decodeSync(Digest)("d".repeat(64)),
      });

      const ownerId = Schema.decodeSync(SubmissionId)("upgraded-owner");

      const owner = SubmissionSnapshot.make({
        submissionId: ownerId,
        threadId: sourceId,
        queueSequence: Schema.decodeSync(QueueSequence)(1),
        principal,
        idempotencyKey: Schema.decodeSync(IdempotencyKey)("upgraded-owner"),
        agentId: upgraded.id,
        agentDigests: upgradedDigests,
        deploymentId: Schema.decodeSync(DeploymentId)("test"),
        inputPayload: "research this existing conversation",
        inputDigest: digest,
        receiptId: Schema.decodeSync(ReceiptId)("upgraded-owner"),
        state: "ready",
        createdAt: DateTime.makeUnsafe(0),
      });

      const h = yield* harness({
        sourceRevisions: [
          {
            definition: upgraded,
            digests: upgradedDigests,
            reporting: [reportWith()],
          },
        ],
      });

      const legacy = yield* h.host.start(request("legacy-worker"));

      yield* h.settle(legacy.delivery.receipt!);
      h.submissions.set(ownerId, owner);

      const host = yield* h.runtime.acquire({
        sourceThreadId: sourceId,
        principal,
        sourceSubmissionId: ownerId,
      });

      expect(
        yield* host.inspect({ worker: legacy.worker, target, message: legacy.delivery.message }),
      ).toEqual(legacy.delivery);

      const followUp = yield* host.followUp({
        worker: legacy.worker,
        target,
        idempotencyKey: Schema.decodeSync(IdempotencyKey)("upgraded-follow-up"),
        encodedInput: { text: "continue existing work" },
        encodedParameters: { note: "continue existing work" },
      });

      expect(h.submissions.get(followUp.receipt!.submissionId)?.workerAdmission?.origin).toEqual(
        h.submissions.get(legacy.delivery.receipt!.submissionId)?.workerAdmission?.origin,
      );
      yield* h.settle(followUp.receipt!);
      const started = yield* host.start(request("upgraded-scout"));
      const child = h.submissions.get(started.delivery.receipt!.submissionId)!;

      expect(child.workerAdmission?.origin.source.agentId).toBe(upgraded.id);
      expect(child.workerAdmission?.origin.reporting?.sourceDigests).toEqual(upgradedDigests);
      yield* h.settle(started.delivery.receipt!);
      expect(
        [...h.deliveries.values()]
          .filter((row) => row.envelope.threadId === sourceId)
          .map((row) => row.envelope),
      ).toEqual([
        expect.objectContaining({
          agentId: upgraded.id,
          definitions: upgradedDigests,
          input: "research this existing conversation",
        }),
      ]);
      expect(h.logs.get(sourceId)?.[0]?.record.payload).toEqual(
        ThreadCreated.make({ agentId: sourceAgent.id, definitions }),
      );

      // Current source code can retry the original worker despite older admission digests.
      // The retry still carries the original worker identity and frozen delivery.
      const count = h.logs.get(sourceId)?.length;

      h.submissions.set(ownerId, SubmissionSnapshot.make({ ...owner, agentDigests: definitions }));
      expect(yield* host.start(request("upgraded-scout"))).toEqual(started);
      expect(h.logs.get(sourceId)?.length).toBe(count);
      h.submissions.set(
        ownerId,
        SubmissionSnapshot.make({ ...owner, threadId: started.worker.threadId }),
      );
      expect((yield* host.start(request("wrong-source")).pipe(Effect.flip)).reason).toBe("denied");

      // Root upgrades cannot be used to replace a worker's admitted target Agent.
      h.submissions.set(
        child.submissionId,
        SubmissionSnapshot.make({ ...child, agentId: upgraded.id, agentDigests: upgradedDigests }),
      );
      expect(
        (yield* h.runtime
          .acquire({
            sourceThreadId: started.worker.threadId,
            principal,
            sourceSubmissionId: child.submissionId,
          })
          .pipe(Effect.flip)).reason,
      ).toBe("denied");
    }),
  );

  it.effect(
    "reconciles a retained launch across Runs before fresh preparation and preserves argument conflicts",
    () =>
      Effect.gen(function* () {
        let denyFresh = false;

        const h = yield* harness().pipe(
          Effect.provideService(WorkerPolicyResolver, {
            resolveSource: () => Effect.succeed(Option.none()),
            resolveTarget: (request) =>
              denyFresh && request._tag === "InitialInput"
                ? WorkerError.make({ operation: "start", reason: "denied" })
                : Effect.succeed(Option.none()),
          }),
        );

        const original = request("retained-command");
        const first = yield* h.host.start(original);

        denyFresh = true;

        const later = h.runtime.facet(
          {
            source: {
              _tag: "tool",
              agentId: sourceAgent.id,
              threadId: sourceId,
              runId: Schema.decodeSync(RunId)("later-run"),
              toolCallId: Schema.decodeSync(ToolCallId)("retry"),
            },
            policy: sourceAgent.policy,
            depth: 0,
          },
          principal,
        );

        expect(yield* later.start(original)).toEqual(first);
        for (const changed of [
          { ...original, encodedInput: { text: "changed" } },
          { ...original, encodedParameters: { note: "changed" } },
          { ...original, policy: AgentPolicy.make({ ...policy, maxTurns: 1 }) },
          { ...original, toolCallAllowance: 1 },
        ])
          expect((yield* later.start(changed).pipe(Effect.flip)).reason).toBe(
            "idempotency-conflict",
          );
        h.deny("send");
        expect((yield* later.start(original).pipe(Effect.flip)).reason).toBe("denied");
        expect(h.submissions.size).toBe(1);
      }),
  );

  for (const pending of [false, true])
    it.effect(
      `public start replays the retained capture before preparation (pending=${pending})`,
      () =>
        Effect.gen(function* () {
          class PreparationFailed extends Schema.TaggedError<PreparationFailed>()(
            "PreparationFailed",
            {},
          ) {}
          let preparations = 0;
          let throwPreparation = false;
          let denyFresh = false;

          const h = yield* harness().pipe(
            Effect.provideService(WorkerPolicyResolver, {
              resolveSource: () => Effect.succeed(Option.none()),
              resolveTarget: (request) =>
                denyFresh &&
                request._tag === "InitialInput" &&
                request.source._tag === "tool" &&
                request.source.runId === "later-run"
                  ? WorkerError.make({ operation: "start", reason: "denied" })
                  : Effect.succeed(Option.none()),
            }),
          );

          const declarationFor = (maxToolCalls: number) =>
            Subagent.make("research", {
              target,
              parameters: Schema.Struct({ note: Schema.String }),
              failure: PreparationFailed,
              prepareInput: ({ note }, caller) =>
                Effect.gen(function* () {
                  preparations++;
                  if (throwPreparation) return yield* new PreparationFailed();

                  return {
                    text: `${note}:${caller.source === "tool" ? caller.parent.runId : "programmatic"}`,
                  };
                }),
              policy: Subagent.SubagentPolicy.make({
                maxChildren: 10,
                maxConcurrency: 1,
                maxTurns: 2,
                maxToolCalls,
                maxDuration: "1 second",
              }),
              toolCallAllowance: { default: maxToolCalls },
            });

          const declaration = declarationFor(2);

          const facet = (run: string) =>
            h.runtime.facet(
              {
                source: {
                  _tag: "tool",
                  agentId: sourceAgent.id,
                  threadId: sourceId,
                  runId: Schema.decodeSync(RunId)(run),
                  toolCallId: Schema.decodeSync(ToolCallId)(`call:${run}`),
                },
                policy: sourceAgent.policy,
                depth: 0,
              },
              principal,
            );

          const key = Schema.decodeSync(IdempotencyKey)("public-command");

          const start = Subagent.start(
            declaration,
            { note: "immutable brief" },
            { idempotencyKey: key },
          );

          if (pending) h.fail("worker:before-source-append");
          const first = yield* start.pipe(Effect.provideService(SubagentHost, facet("first-run")));
          // Use the retained store's exact envelope; no conversation/history reconstruction.
          const envelope = [...h.deliveries.values()][0]?.envelope;

          expect(envelope?.input).toEqual({ text: "immutable brief:first-run" });
          expect(first.delivery.receipt === null).toBe(pending);
          h.fail(undefined);
          if (pending) yield* TestClock.adjust("31 seconds");
          denyFresh = true;
          const later = facet("later-run");
          const replay = yield* start.pipe(Effect.provideService(SubagentHost, later));

          expect(replay.worker).toEqual(first.worker);
          expect(replay.delivery.message).toEqual(first.delivery.message);
          if (!pending) expect(replay).toEqual(first);
          throwPreparation = true;
          expect(yield* start.pipe(Effect.provideService(SubagentHost, later))).toEqual(replay);
          expect(
            yield* Subagent.start(
              declarationFor(1),
              { note: "immutable brief" },
              { idempotencyKey: key },
            ).pipe(Effect.provideService(SubagentHost, later)),
          ).toEqual(replay);
          for (const changed of [
            Subagent.start(declaration, { note: "changed brief" }, { idempotencyKey: key }),
            Subagent.start(
              declaration,
              { note: "immutable brief" },
              { idempotencyKey: key, budgetScope: "worker-run" },
            ),
          ])
            expect(
              yield* changed.pipe(Effect.provideService(SubagentHost, later), Effect.flip),
            ).toMatchObject({ reason: "idempotency-conflict" });
          h.deny("send");
          expect(
            yield* start.pipe(Effect.provideService(SubagentHost, later), Effect.flip),
          ).toMatchObject({ reason: "denied" });
          h.deny(undefined);
          expect(preparations).toBe(1);
          expect(h.deliveries.size).toBe(1);
          expect([...h.deliveries.values()][0]?.envelope).toEqual(envelope);
          expect(h.submissions.size).toBe(1);
          expect(
            yield* Subagent.start(
              declaration,
              { note: "fresh brief" },
              {
                idempotencyKey: Schema.decodeSync(IdempotencyKey)("preparation-error"),
              },
            ).pipe(Effect.provideService(SubagentHost, later), Effect.flip),
          ).toEqual(new PreparationFailed());
          throwPreparation = false;
          expect(
            yield* Subagent.start(
              declaration,
              { note: "fresh brief" },
              { idempotencyKey: Schema.decodeSync(IdempotencyKey)("fresh-command") },
            ).pipe(Effect.provideService(SubagentHost, later), Effect.flip),
          ).toMatchObject({ reason: "denied" });
          expect(h.deliveries.size).toBe(1);
        }),
    );

  // Regression: https://github.com/danieljvdm/effect-agent/commit/4ff21e2a4c3735be34955e7d5caf64f623d33f81
  // Reuse the public-start retained-command fixture from #568 for follow-up correction provenance.
  for (const pending of [false, true])
    it.effect(
      `public follow-up replays its original correction across Runs without preparation (pending=${pending})`,
      () =>
        Effect.gen(function* () {
          class PreparationFailed extends Schema.TaggedError<PreparationFailed>()(
            "PreparationFailed",
            {},
          ) {}
          let preparations = 0;
          let throwPreparation = false;
          const h = yield* harness();
          const first = yield* h.host.start(request("original task"));

          const declaration = Subagent.make("research", {
            target,
            parameters: Schema.Struct({ note: Schema.String }),
            failure: PreparationFailed,
            prepareInput: ({ note }, caller) =>
              Effect.gen(function* () {
                preparations++;
                if (throwPreparation) return yield* new PreparationFailed();

                return {
                  text: `${note}:${caller.source === "tool" ? caller.parent.runId : "programmatic"}`,
                };
              }),
          });

          const facet = (run: string) =>
            h.runtime.facet(
              {
                source: {
                  _tag: "tool",
                  agentId: sourceAgent.id,
                  threadId: sourceId,
                  runId: Schema.decodeSync(RunId)(run),
                  toolCallId: Schema.decodeSync(ToolCallId)(`call:${run}`),
                },
                policy: sourceAgent.policy,
                depth: 0,
              },
              principal,
            );

          const worker = Schema.decodeSync(Subagent.Worker(declaration))(first.worker);
          const key = Schema.decodeSync(IdempotencyKey)("explicit-correction");

          const followUp = Subagent.followUp(
            declaration,
            worker,
            { note: "corrected brief" },
            { idempotencyKey: key },
          );

          if (pending) h.fail("worker:before-source-append");

          const correction = yield* followUp.pipe(
            Effect.provideService(SubagentHost, facet("human-correction-run")),
          );

          const envelope = structuredClone(
            h.deliveries.get(correction.message.messageId)!.envelope,
          );

          expect(envelope.input).toEqual({ text: "corrected brief:human-correction-run" });
          expect(envelope.workerAdmission?.origin).toEqual(
            h.deliveries.get(first.delivery.message.messageId)!.envelope.workerAdmission?.origin,
          );
          expect(correction.receipt === null).toBe(pending);
          h.fail(undefined);
          if (pending) yield* TestClock.adjust("31 seconds");
          const later = facet("later-run");
          const replay = yield* followUp.pipe(Effect.provideService(SubagentHost, later));

          expect(replay.message).toEqual(correction.message);
          expect(replay.receipt).not.toBeNull();
          if (!pending) expect(replay).toEqual(correction);
          throwPreparation = true;
          expect(yield* followUp.pipe(Effect.provideService(SubagentHost, later))).toEqual(replay);
          expect(
            yield* Subagent.followUp(
              declaration,
              worker,
              { note: "changed brief" },
              { idempotencyKey: key },
            ).pipe(Effect.provideService(SubagentHost, later), Effect.flip),
          ).toMatchObject({ reason: "idempotency-conflict" });
          for (const changed of [
            {
              encodedInput: { text: "different capture" },
              encodedParameters: { note: "corrected brief" },
            },
            { encodedInput: envelope.input, encodedParameters: { note: "different parameters" } },
          ])
            expect(
              yield* later
                .followUp({ worker, target, idempotencyKey: key, ...changed })
                .pipe(Effect.flip),
            ).toMatchObject({ reason: "idempotency-conflict" });
          h.deny("send");
          expect(
            yield* followUp.pipe(Effect.provideService(SubagentHost, later), Effect.flip),
          ).toMatchObject({ reason: "denied" });
          h.deny(undefined);
          expect(preparations).toBe(1);
          expect(h.deliveries.get(correction.message.messageId)!.envelope).toEqual(envelope);
          expect(h.submissions.get(replay.receipt!.submissionId)?.inputPayload).toEqual(
            envelope.input,
          );
          expect(h.submissions.size).toBe(2);
          expect(
            yield* Subagent.followUp(
              declaration,
              worker,
              { note: "new correction" },
              {
                idempotencyKey: Schema.decodeSync(IdempotencyKey)("fresh-command"),
              },
            ).pipe(Effect.provideService(SubagentHost, later), Effect.flip),
          ).toEqual(new PreparationFailed());
          expect(h.deliveries.size).toBe(2);
        }),
    );

  // Equal-input regression: https://github.com/danieljvdm/effect-agent/commit/3ab9045fc293d09a22801c7d881c4d89e562461a
  for (const [preparation, denied] of [
    ["changed", undefined],
    ["failed", undefined],
    ["failed", "caller"],
    ["failed", "policy"],
    ["equal", "caller"],
    ["equal", "policy"],
  ] as const)
    it.effect(
      `public follow-up reconciles concurrent retention with current authority (${preparation}, denied=${denied ?? "none"})`,
      () =>
        Effect.gen(function* () {
          class PreparationFailed extends Schema.TaggedError<PreparationFailed>()(
            "PreparationFailed",
            {},
          ) {}
          let denyPolicy = false;

          const h = yield* harness().pipe(
            Effect.provideService(WorkerPolicyResolver, {
              resolveSource: () => Effect.succeed(Option.none()),
              resolveTarget: () =>
                denyPolicy
                  ? WorkerError.make({ operation: "followUp", reason: "denied" })
                  : Effect.succeed(Option.none()),
            }),
          );

          const first = yield* h.host.start(request("original task"));
          const preparing = yield* Deferred.make<void>();
          const resume = yield* Deferred.make<void>();

          const declaration = Subagent.make("research", {
            target,
            parameters: Schema.Struct({ note: Schema.String }),
            failure: PreparationFailed,
            prepareInput: ({ note }) => Effect.succeed({ text: note }),
          });

          const worker = Schema.decodeSync(Subagent.Worker(declaration))(first.worker);
          const key = Schema.decodeSync(IdempotencyKey)("racing-correction");

          const loser = yield* Subagent.followUp(
            {
              ...declaration,
              prepareInput: () =>
                Effect.gen(function* () {
                  yield* Deferred.succeed(preparing, undefined);
                  yield* Deferred.await(resume);
                  if (preparation === "failed") return yield* new PreparationFailed();

                  return {
                    text:
                      preparation === "equal" ? "original correction" : "later correction capture",
                  };
                }),
            },
            worker,
            { note: "original correction" },
            { idempotencyKey: key },
          ).pipe(Effect.provideService(SubagentHost, h.host), Effect.result, Effect.forkChild);

          yield* Deferred.await(preparing);

          const winner = yield* Subagent.followUp(
            declaration,
            worker,
            { note: "original correction" },
            {
              idempotencyKey: key,
            },
          ).pipe(Effect.provideService(SubagentHost, h.host));

          const envelope = structuredClone(h.deliveries.get(winner.message.messageId)!.envelope);

          if (denied === "caller") h.deny("send");
          if (denied === "policy") denyPolicy = true;
          yield* Deferred.succeed(resume, undefined);
          const result = yield* Fiber.join(loser);

          if (denied !== undefined)
            expect(result).toMatchObject({ _tag: "Failure", failure: { reason: "denied" } });
          else expect(result).toMatchObject({ _tag: "Success", success: winner });
          expect(h.deliveries.get(winner.message.messageId)!.envelope).toEqual(envelope);
          expect(envelope.input).toEqual({ text: "original correction" });
          expect(h.deliveries.size).toBe(2);
          expect(h.submissions.size).toBe(2);
        }),
    );

  // Regression: https://github.com/danieljvdm/effect-agent/commit/43882d187248665eaf7fd46950b3bc617edcb73d
  it.effect(
    "serializes source-aware active slots across raced starts, steering and idle reactivation",
    () =>
      Effect.gen(function* () {
        let limit = 1;

        const h = yield* harness({
          independentBudget: true,
          limits: { maxWorkersPerSource: 10, maxInputsPerWorker: 10, maxPendingInputsPerWorker: 3 },
        }).pipe(
          Effect.provideService(WorkerConcurrencyResolver, {
            resolve: (request) =>
              Effect.sync(() => {
                expect(request.source.threadId).toBe(sourceId);
                expect(request.principal).toBe(principal);
                expect(request.sourceSubmission).toBeUndefined();

                return Option.some({ maxActiveWorkersPerSource: limit });
              }),
          }),
        );

        const start = (key: string) => h.host.start({ ...request(key), budgetScope: "worker-run" });

        const raced = yield* Effect.forEach(
          [start("slot-a"), start("slot-b")],
          (effect) => effect,
          { concurrency: "unbounded" },
        );

        expect(raced.map((result) => result.delivery.status).sort()).toEqual([
          "accepted",
          "refused",
        ]);
        const first = raced.find((result) => result.delivery.status === "accepted")!;

        limit = 0;

        const follow = (key: string) =>
          h.host.followUp({
            worker: first.worker,
            target,
            idempotencyKey: Schema.decodeSync(IdempotencyKey)(key),
            encodedInput: { text: key },
            encodedParameters: { note: key },
          });

        const steering = yield* follow("active-steering");

        expect(yield* follow("active-steering")).toEqual(steering);
        expect((yield* start("blocked-zero")).delivery).toMatchObject({
          status: "refused",
          reason: "worker-capacity",
        });
        yield* h.settle(first.delivery.receipt!);
        // A queued/steering input still owns the slot after the first receipt settles.
        expect((yield* start("blocked-pending")).delivery).toMatchObject({
          status: "refused",
          reason: "worker-capacity",
        });
        yield* h.settle(steering.receipt!, "steered", { host: first.delivery.receipt! });
        expect(yield* follow("idle-blocked")).toMatchObject({
          status: "refused",
          reason: "worker-capacity",
        });
        limit = 1;
        const later = yield* follow("idle-later");

        expect(later.receipt!.threadId).toEqual(first.worker.threadId);
        expect(h.submissions.get(later.receipt!.submissionId)?.workerAdmission?.origin).toEqual(
          h.submissions.get(first.delivery.receipt!.submissionId)?.workerAdmission?.origin,
        );
        yield* h.settle(later.receipt!);
        limit = 1_000;
        yield* start("host-one");
        yield* start("host-two");
        expect((yield* start("host-three")).delivery).toMatchObject({
          status: "refused",
          reason: "worker-capacity",
        });
      }),
  );
  // Regression: https://github.com/danieljvdm/effect-agent/commit/43882d187248665eaf7fd46950b3bc617edcb73d
  it.effect(
    "retries unavailable source capacity with the same exact owner and reservation after append loss",
    () =>
      Effect.gen(function* () {
        let unavailable = true;
        let calls = 0;

        const dependency = Object.assign(new Error("Capacity store unavailable"), {
          _tag: "CapacityStorageError",
          code: "CONNECTION_RESET",
          privatePayload: "private capacity data",
        });

        const ownerId = Schema.decodeSync(SubmissionId)("capacity-owner");

        const h = yield* harness({ independentBudget: true }).pipe(
          Effect.provideService(WorkerConcurrencyResolver, {
            resolve: (request) =>
              Effect.suspend(() => {
                calls++;
                expect(request.source.threadId).toBe(sourceId);
                expect(request.sourceSubmission?.submissionId).toBe(ownerId);
                expect(request.sourceSubmission?.inputPayload).toBe("captured concurrency one");

                return unavailable
                  ? WorkerError.make({
                      operation: "start",
                      reason: "unavailable",
                      cause: dependency,
                    })
                  : Effect.succeed(Option.some({ maxActiveWorkersPerSource: 1 }));
              }),
          }),
        );

        h.submissions.set(
          ownerId,
          SubmissionSnapshot.make({
            submissionId: ownerId,
            receiptId: Schema.decodeSync(ReceiptId)("capacity-owner-receipt"),
            threadId: sourceId,
            principal,
            idempotencyKey: Schema.decodeSync(IdempotencyKey)("owner"),
            queueSequence: Schema.decodeSync(QueueSequence)(1),
            agentId: sourceAgent.id,
            agentDigests: definitions,
            deploymentId: Schema.decodeSync(DeploymentId)("test"),
            inputPayload: "captured concurrency one",
            inputDigest: digest,
            state: "settled",
            createdAt: DateTime.makeUnsafe(yield* Clock.currentTimeMillis),
          }),
        );

        const host = yield* h.runtime.acquire({
          sourceThreadId: sourceId,
          sourceSubmissionId: ownerId,
          principal,
        });

        const start = () => host.start({ ...request("retry-capacity"), budgetScope: "worker-run" });

        // Retention survives admission failure; its diagnostic stays private in the delivery row.
        const pending = yield* start();

        expect(pending.delivery).toMatchObject({
          status: "pending",
          receipt: null,
          reason: "storage",
        });
        expect(pending.delivery).not.toHaveProperty("cause");
        const diagnostic = [...h.deliveries.values()][0]?.lastFailureDiagnostic;

        expect(diagnostic).toMatchObject({
          _tag: "Error",
          errorTag: "ScheduledInputRetryable",
          cause: {
            errorTag: "AdmissionPolicyError",
            code: "worker-unavailable",
            cause: {
              errorTag: "WorkerError",
              reason: { _tag: "Value", value: "unavailable" },
              cause: {
                errorTag: "CapacityStorageError",
                message: "Capacity store unavailable",
                code: "CONNECTION_RESET",
                stack: dependency.stack,
              },
            },
          },
        });
        expect(JSON.stringify(diagnostic)).not.toContain("private capacity data");
        expect(calls).toBe(1);
        expect([...h.deliveries.values()][0]?.status).toBe("pending");
        const envelope = [...h.deliveries.values()][0]!.envelope;

        expect(
          (yield* h.runtime
            .validateAdmission(
              envelope.workerAdmission!,
              {
                threadId: envelope.threadId,
                principal: envelope.deliveryPrincipal,
                definitions: envelope.definitions,
                idempotencyKey: envelope.admissionKey,
              },
              envelope.agentId,
              envelope.inputDigest,
              envelope.input,
            )
            .pipe(Effect.flip)).reason,
        ).toBe("unavailable");
        expect(
          h.logs
            .get(sourceId)
            ?.filter(({ record }) => record.payload._tag === "WorkerInputRequested"),
        ).toHaveLength(0);
        unavailable = false;
        yield* TestClock.adjust("1 second");
        h.fail("worker:after-source-append");
        expect((yield* start()).delivery).toMatchObject({ status: "pending", reason: "storage" });
        h.fail(undefined);
        unavailable = true;
        const attempts = calls;

        yield* TestClock.adjust("31 seconds");
        const recovered = yield* start();

        expect(calls).toBe(attempts);
        expect(yield* start()).toEqual(recovered);
        expect(
          h.logs
            .get(sourceId)
            ?.filter(({ record }) => record.payload._tag === "WorkerInputRequested"),
        ).toHaveLength(1);
      }),
  );
  // Regression: https://github.com/danieljvdm/effect-agent/commit/43882d187248665eaf7fd46950b3bc617edcb73d
  it.effect("uses current report code while preserving the original owner and worker origin", () =>
    Effect.gen(function* () {
      const ownerId = Schema.decodeSync(SubmissionId)("source-owner");
      const revisedDigest = Schema.decodeSync(Digest)("b".repeat(64));
      const revisedDigests = DefinitionDigests.make({ ...definitions, agent: revisedDigest });

      const revised = Agent.make(sourceAgent.id, {
        input: sourceAgent.input,
        output: sourceAgent.output,
        instructions: "Revised source",
        toolkit: Toolkit.empty,
        policy: AgentPolicy.make({ ...sourceAgent.policy, maxTurns: 30 }),
      });

      const snapshot = SubmissionSnapshot.make({
        submissionId: ownerId,
        threadId: sourceId,
        queueSequence: Schema.decodeSync(QueueSequence)(1),
        principal,
        idempotencyKey: Schema.decodeSync(IdempotencyKey)("source-owner"),
        agentId: sourceAgent.id,
        agentDigests: revisedDigests,
        deploymentId: Schema.decodeSync(DeploymentId)("test"),
        inputPayload: "immutable capture",
        inputDigest: digest,
        receiptId: Schema.decodeSync(ReceiptId)("source-owner"),
        state: "ready",
        createdAt: DateTime.makeUnsafe(0),
      });

      const reportsA = [reportWith()];
      const legacy = yield* harness({ sourceReports: reportsA });

      legacy.submissions.set(ownerId, snapshot);

      const context = {
        source: { _tag: "programmatic" as const, threadId: sourceId, agentId: sourceAgent.id },
        policy: revised.policy,
        depth: 0,
      };

      const legacyFacet = legacy.runtime.facet(context, principal, ownerId);

      expect((yield* legacyFacet.context).policy).toEqual(revised.policy);
      const started = yield* legacyFacet.start(request("legacy-owner"));

      expect(started.delivery.receipt!.threadId).toBe(started.worker.threadId);
      yield* legacy.settle(started.delivery.receipt!);
      expect(
        [...legacy.deliveries.values()]
          .filter((row) => row.envelope.threadId === sourceId)
          .map((row) => row.envelope),
      ).toEqual([expect.objectContaining({ definitions, input: "immutable capture" })]);

      // A new, verified owner must not need the code that created the conversation years ago.
      const recreated = yield* harness({
        sourceRevisions: [{ definition: revised, digests: revisedDigests }],
      });

      const history = recreated.logs.get(sourceId)!;
      const first = history[0]!;
      const created = first.record.payload;

      if (created._tag !== "ThreadCreated") throw new Error("Expected original Thread identity");

      const original = {
        ...first,
        record: {
          ...first.record,
          payload: ThreadCreated.make({
            ...created,
            definitions: { ...definitions, agent: Schema.decodeSync(Digest)("d".repeat(64)) },
          }),
        },
      };

      recreated.logs.set(sourceId, [original, ...history.slice(1)]);
      recreated.submissions.set(ownerId, snapshot);

      const currentOwner = yield* recreated.runtime.acquire({
        sourceThreadId: sourceId,
        sourceSubmissionId: ownerId,
        principal,
      });

      expect((yield* currentOwner.context).policy).toEqual(revised.policy);
      expect(
        (yield* currentOwner.start(request("current-owner"))).delivery.receipt!.threadId,
      ).toBeDefined();
      expect(recreated.logs.get(sourceId)?.[0]).toEqual(original);

      const laterOwnerId = Schema.decodeSync(SubmissionId)("later-source-owner");

      const laterDigests = DefinitionDigests.make({
        ...definitions,
        agent: Schema.decodeSync(Digest)("c".repeat(64)),
      });

      const laterDefinition = Agent.make(sourceAgent.id, {
        input: sourceAgent.input,
        output: sourceAgent.output,
        instructions: "Later source",
        toolkit: Toolkit.empty,
        policy: AgentPolicy.make({ ...sourceAgent.policy, maxTurns: 40 }),
      });

      let reportsC = 0;
      const reportOwners: Array<SubmissionId | undefined> = [];

      const opted = yield* harness({
        // Regression: https://github.com/danieljvdm/effect-agent/commit/4600d240f44b1ef1fe9b0fc58f39e293a6434f85
        // A strict owner needs the original locator even after its Run is idle.
        authorize: (request) =>
          Effect.gen(function* () {
            if (request.operation === "followUp") {
              reportOwners.push(request.sourceSubmissionId);
              if (request.sourceSubmissionId === undefined)
                return yield* WorkerError.make({
                  operation: request.operation,
                  reason: "denied",
                });
            }

            return principal;
          }),
        sourceReports: reportsA,
        sourceRevisions: [
          {
            definition: laterDefinition,
            digests: laterDigests,
            reporting: [
              reportWith((report) =>
                standardReport.prepare(report).pipe(
                  Effect.tap(() =>
                    Effect.sync(() => {
                      reportsC++;
                    }),
                  ),
                ),
              ),
            ],
          },
        ],
      }).pipe(
        Effect.provideService(WorkerPolicyResolver, {
          resolveSource: (request) =>
            Effect.gen(function* () {
              if (request.submission === undefined)
                return yield* WorkerError.make({ operation: "start", reason: "unavailable" });
              if (request.submission.submissionId === laterOwnerId) {
                expect(request.definition).toBe(laterDefinition);
                expect(request.definitions).toEqual(laterDigests);

                return Option.some(laterDefinition.policy);
              }
              expect(request.definition).toBe(laterDefinition);
              expect(request.definitions).toEqual(revisedDigests);
              expect(request.submission.inputPayload).toBe("immutable capture");

              return Option.some(revised.policy);
            }),
          resolveTarget: () => Effect.succeed(Option.none()),
        }),
      );

      opted.submissions.set(ownerId, snapshot);
      expect((yield* opted.host.context.pipe(Effect.flip)).reason).toBe("unavailable");
      expect(
        yield* opted.host.list({ target, delegationId: request("list").delegationId, limit: 10 }),
      ).toEqual({ items: [], next: null });

      const selected = yield* opted.runtime.acquire({
        sourceThreadId: sourceId,
        principal,
        sourceSubmissionId: ownerId,
      });

      expect((yield* selected.context).policy).toEqual(revised.policy);
      const worker = yield* selected.start(request("owner-B-worker"));

      const origin = opted.submissions.get(worker.delivery.receipt!.submissionId)!.workerAdmission!
        .origin;

      expect(origin.reporting?.sourceDigests).toEqual(laterDigests);
      yield* opted.settle(worker.delivery.receipt!);
      opted.submissions.set(
        laterOwnerId,
        SubmissionSnapshot.make({
          ...snapshot,
          submissionId: laterOwnerId,
          agentDigests: laterDigests,
          inputPayload: "later immutable capture",
        }),
      );

      const laterSource = yield* opted.runtime.acquire({
        sourceThreadId: sourceId,
        principal,
        sourceSubmissionId: laterOwnerId,
      });

      const next = yield* laterSource.followUp({
        worker: worker.worker,
        target,
        idempotencyKey: Schema.decodeSync(IdempotencyKey)("owner-C-followup"),
        encodedInput: { text: "next" },
        encodedParameters: { note: "next" },
      });

      yield* opted.settle(next.receipt!);
      expect(opted.submissions.get(next.receipt!.submissionId)!.workerAdmission!.origin).toEqual(
        origin,
      );
      expect(reportsC).toBe(2);
      expect(reportOwners).toEqual([laterOwnerId]);
      expect(
        [...opted.deliveries.values()]
          .filter((row) => row.envelope.threadId === sourceId)
          .map((row) => row.envelope),
      ).toEqual([
        expect.objectContaining({ definitions: laterDigests, input: "immutable capture" }),
        expect.objectContaining({ definitions: laterDigests, input: "immutable capture" }),
      ]);
    }),
  );

  // Regression: https://github.com/danieljvdm/effect-agent/commit/43882d187248665eaf7fd46950b3bc617edcb73d
  it.effect(
    "revalidates captured authority across a reserved admission retry and freezes later worker policy",
    () =>
      Effect.gen(function* () {
        const captured = AgentPolicy.make({
          ...policy,
          maxTurns: 7,
          maxToolCalls: 6,
          maxDuration: "4 seconds",
        });

        let unavailable = false;
        let replaceRetained = false;
        let initialCalls = 0;

        const h = yield* harness({ independentBudget: true }).pipe(
          Effect.provideService(WorkerPolicyResolver, {
            resolveSource: () => Effect.succeed(Option.none()),
            resolveTarget: (input) =>
              Effect.gen(function* () {
                if (unavailable)
                  return yield* WorkerError.make({ operation: "start", reason: "unavailable" });
                if (input._tag === "RetainedWorker")
                  return Option.some(replaceRetained ? policy : input.origin.policy);
                initialCalls++;
                expect(input.input).toEqual({ text: "captured" });
                expect(input.definition).toBe(target);

                return Option.some(captured);
              }),
          }),
        );

        const base = request("captured");

        const start: StartWorkerRequest = {
          ...base,
          policy: captured,
          budgetScope: "worker-run",
          budget: {
            caps: SubagentDelegationCaps.make({
              maxTotalChildInvocations: 1,
              maxConcurrentChildren: 1,
              maxTurns: 8,
              maxToolCalls: 7,
              maxDurationMillis: 5_000,
            }),
            allocation: SubagentReservationAmounts.make({
              ...base.budget.allocation,
              turns: 8,
              toolCalls: 7,
              durationMillis: 5_000,
            }),
            descendantInvocations: 1,
          },
        };

        h.fail("worker:after-source-append");
        expect((yield* h.host.start(start)).delivery).toMatchObject({
          status: "pending",
          reason: "storage",
        });
        h.fail(undefined);
        const delivery = [...h.deliveries.values()][0]!;
        const metadata = delivery.envelope.workerAdmission!;

        const admissionOptions = {
          threadId: delivery.envelope.threadId,
          definitions: delivery.envelope.definitions,
          principal,
          idempotencyKey: delivery.envelope.admissionKey,
        };

        unavailable = true;
        expect(
          (yield* h.runtime
            .validateAdmission(
              metadata,
              admissionOptions,
              target.id,
              delivery.envelope.inputDigest,
              delivery.envelope.input,
            )
            .pipe(Effect.flip)).reason,
        ).toBe("unavailable");
        expect(h.submissions.size).toBe(0);
        unavailable = false;
        yield* TestClock.adjust("1 second");
        const started = yield* h.host.start(start);

        expect(initialCalls).toBeGreaterThanOrEqual(3);

        const origin = h.submissions.get(started.delivery.receipt!.submissionId)!.workerAdmission!
          .origin;

        expect(origin.policy).toEqual(captured);
        expect(
          h.logs
            .get(sourceId)!
            .filter(({ record }) => record.payload._tag === "WorkerInputRequested"),
        ).toHaveLength(1);
        yield* h.settle(started.delivery.receipt!);

        const followup = {
          worker: started.worker,
          target,
          idempotencyKey: Schema.decodeSync(IdempotencyKey)("later"),
          encodedInput: { text: "different later input" },
          encodedParameters: { note: "later" },
        };

        replaceRetained = true;
        expect((yield* h.host.followUp(followup).pipe(Effect.flip)).reason).toBe("worker-mismatch");
        expect(h.deliveries.size).toBe(1);
        replaceRetained = false;
        const next = yield* h.host.followUp(followup);

        expect(h.submissions.get(next.receipt!.submissionId)!.workerAdmission!.origin).toEqual(
          origin,
        );
      }),
  );

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

        const original = h.submissions.get(started.delivery.receipt!.submissionId)?.workerAdmission
          ?.origin;

        yield* h.settle(started.delivery.receipt!);
        for (const name of ["second", "third"]) {
          const next = yield* h.host.followUp({
            worker: started.worker,
            target,
            idempotencyKey: Schema.decodeSync(IdempotencyKey)(name),
            encodedInput: { text: name },
            encodedParameters: { note: name },
          });

          expect(h.submissions.get(next.receipt!.submissionId)?.workerAdmission?.origin).toEqual(
            original,
          );
          yield* h.settle(next.receipt!);
        }
        expect(original).toMatchObject({ budgetScope: "worker-run", depth: 1 });
        expect(
          h.logs
            .get(sourceId)
            ?.filter(({ record }) => record.payload._tag === "SubtreeBudgetReserved"),
        ).toHaveLength(0);
        expect(
          (yield* h.host.followUp({
            worker: started.worker,
            target,
            idempotencyKey: Schema.decodeSync(IdempotencyKey)("fourth"),
            encodedInput: { text: "fourth" },
            encodedParameters: {},
          })).reason,
        ).toBe("worker-capacity");
      }),
  );
  it.effect("concurrent launches cannot oversubscribe one canonical source slot", () =>
    Effect.gen(function* () {
      const h = yield* harness();

      const outcomes = yield* Effect.forEach(
        ["left", "right"],
        (key) => h.host.start(request(key)),
        { concurrency: 2 },
      );

      expect(outcomes.map((result) => result.delivery.status).sort()).toEqual([
        "accepted",
        "refused",
      ]);
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
        expect(yield* send("third")).toMatchObject({
          status: "refused",
          reason: "worker-capacity",
        });
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
          .await({ worker: first.worker, target, receipt: first.delivery.receipt! })
          .pipe(Effect.forkChild);

        yield* Effect.yieldNow;
        yield* Fiber.interrupt(waiting);
        expect(h.auth).not.toContain("control");
        expect(
          yield* h.host.inspect({ worker: first.worker, target, receipt: first.delivery.receipt! }),
        ).toMatchObject({ _tag: "Pending" });
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

        expect(next.receipt!.threadId).toBe(first.worker.threadId);
        expect(next.receipt!.submissionId).not.toBe(first.delivery.receipt!.submissionId);
        expect(h.submissions.get(next.receipt!.submissionId)?.workerAdmission?.origin).toEqual(
          h.submissions.get(first.delivery.receipt!.submissionId)?.workerAdmission?.origin,
        );
        expect(h.submissions.get(next.receipt!.submissionId)?.parentLinkage).toBeUndefined();
        expect(
          yield* h.host.inspect({ worker: first.worker, target, receipt: first.delivery.receipt! }),
        ).toEqual({ _tag: "Pending", receipt: first.delivery.receipt! });
        h.join(next.receipt!.submissionId);
        expect(
          (yield* h.host
            .cancel({ worker: first.worker, target, receipt: next.receipt! })
            .pipe(Effect.flip))._tag,
        ).toBe("JoinedToHost");
        yield* h.settle(first.delivery.receipt!);

        const status = yield* h.host.inspect({
          worker: first.worker,
          target,
          receipt: first.delivery.receipt!,
        });

        expect(status).toMatchObject({ _tag: "Settled", encodedParameters: { note: "first" } });
        expect(
          (yield* h.host.list({ delegationId: first.worker.delegationId, target, limit: 10 }))
            .items[0]?.latestReceipt,
        ).toEqual(next.receipt);
      }),
  );

  it.effect("worker summaries retry a selected-read failure after a concurrent append", () =>
    Effect.gen(function* () {
      let onRead: Effect.Effect<void, ThreadStoreError> = Effect.void;
      let selectedReads = 0;

      const h = yield* harness({
        beforeRead: (request) =>
          Effect.suspend(() => {
            if (!("selection" in request) || request.selection._tag !== "WorkerExecution")
              return Effect.void;
            selectedReads++;

            return onRead;
          }),
      });

      const first = yield* h.host.start(request("concurrent-summary"));
      const receipt = first.delivery.receipt;

      if (receipt === null) return yield* Effect.die("Expected an admitted worker input");
      const runId = Schema.decodeSync(RunId)(`run:${receipt.submissionId}`);

      onRead = Effect.sync(() => {
        onRead = Effect.void;
        h.push(
          first.worker.threadId,
          UserInputRecorded.make({
            submissionId: receipt.submissionId,
            kind: "user",
            runId,
            input: { text: "concurrent-summary" },
          }),
          "concurrent-input",
        );
      }).pipe(
        Effect.andThen(
          ThreadStoreError.make({ operation: "selected read", message: "Canonical tail changed" }),
        ),
      );

      const summary = yield* h.host.summary({ worker: first.worker, target });

      expect(summary.appliedInput).toMatchObject({
        receipt,
        messageId: first.delivery.message.messageId,
        runId,
      });
      expect(summary.watermark.canonicalSequence).toBe(summary.appliedInput?.sequence);
      expect(selectedReads).toBe(2);
    }),
  );

  for (const watermark of ["unchanged", "unavailable"] as const)
    it.effect(
      `worker summaries preserve a selected-read error when the watermark is ${watermark}`,
      () =>
        Effect.gen(function* () {
          let selectedReads = 0;

          const readFailure = ThreadStoreError.make({
            operation: "selected read",
            message: "Canonical tail changed",
          });

          let onRead: Effect.Effect<void, ThreadStoreError> = readFailure;

          const h = yield* harness({
            beforeRead: (request) =>
              Effect.suspend(() => {
                if (!("selection" in request) || request.selection._tag !== "WorkerExecution")
                  return Effect.void;
                selectedReads++;

                return onRead;
              }),
          });

          const first = yield* h.host.start(request("failed-summary"));

          if (watermark === "unavailable")
            onRead = Effect.sync(() => h.logs.delete(first.worker.threadId)).pipe(
              Effect.andThen(readFailure),
            );

          const failure = yield* h.host
            .list({ delegationId: first.worker.delegationId, target, limit: 1 })
            .pipe(Effect.flip);

          expect(failure).toMatchObject({ operation: "list", reason: "storage" });
          expect(failure.cause).toBe(readFailure);
          expect(selectedReads).toBe(1);
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

      yield* h.host.cancel({ worker: first.worker, target, receipt: latest.receipt! });
      yield* h.settle(latest.receipt!, "unused", { abortedBeforeRun: true });
      expect(
        yield* h.host.inspect({ worker: first.worker, target, receipt: first.delivery.receipt! }),
      ).toMatchObject({ _tag: "Pending" });
      expect(
        yield* h.host.inspect({ worker: first.worker, target, receipt: latest.receipt! }),
      ).toMatchObject({ _tag: "Settled", receipt: latest.receipt, outcome: "aborted" });

      const active = { worker: first.worker, latestReceipt: latest.receipt, state: "active" };

      const beforeSummaryReads = { ...h.reads };

      expect(yield* h.host.summary({ worker: first.worker, target })).toMatchObject(active);
      expect(
        (yield* h.host.list({ delegationId: first.worker.delegationId, target, limit: 10 })).items,
      ).toMatchObject([active]);

      expect(h.reads.exported).toBe(beforeSummaryReads.exported);
      expect(h.reads.paged).toBe(beforeSummaryReads.paged);
      expect(h.reads.worker).toBe(beforeSummaryReads.worker);
      yield* h.settle(first.delivery.receipt!);
      const idle = { ...active, state: "idle" };

      expect(yield* h.host.summary({ worker: first.worker, target })).toMatchObject(idle);
      expect(
        (yield* h.host.list({ delegationId: first.worker.delegationId, target, limit: 10 })).items,
      ).toMatchObject([idle]);
    }),
  );

  it.effect(
    "worker summaries choose the latest destination receipt when source intents arrive out of order",
    () =>
      Effect.gen(function* () {
        const h = yield* harness();
        const first = yield* h.host.start(request("initial"));

        yield* h.settle(first.delivery.receipt!);

        const followUp = (key: string) =>
          h.host.followUp({
            worker: first.worker,
            target,
            idempotencyKey: Schema.decodeSync(IdempotencyKey)(key),
            encodedInput: { text: key },
            encodedParameters: { note: key },
          });

        h.fail("worker:after-source-append");
        expect(yield* followUp("earlier-intent")).toMatchObject({
          status: "pending",
          reason: "storage",
        });
        h.fail(undefined);
        expect(yield* h.host.summary({ worker: first.worker, target })).toMatchObject({
          worker: first.worker,
          latestReceipt: first.delivery.receipt!,
          state: "starting",
        });
        const earlierReceipt = yield* followUp("later-intent");

        // The interrupted delivery's expiring claim must elapse before its admission replay.
        yield* TestClock.adjust("31 seconds");
        const latestReceipt = yield* followUp("earlier-intent");

        expect(latestReceipt.receipt!.queueSequence).toBeGreaterThan(
          earlierReceipt.receipt!.queueSequence,
        );
        expect(
          h.logs
            .get(sourceId)!
            .flatMap(({ record }) =>
              record.payload._tag === "WorkerInputRequested"
                ? [record.payload.admission.parameters]
                : [],
            ),
        ).toEqual([{ note: "initial" }, { note: "earlier-intent" }, { note: "later-intent" }]);

        const active = {
          worker: first.worker,
          latestReceipt: latestReceipt.receipt,
          state: "active",
        };

        expect(yield* h.host.summary({ worker: first.worker, target })).toMatchObject(active);
        expect(
          (yield* h.host.list({ delegationId: first.worker.delegationId, target, limit: 10 }))
            .items,
        ).toMatchObject([active]);
        yield* h.settle(earlierReceipt.receipt!);
        yield* h.settle(latestReceipt.receipt!);
        expect(yield* h.host.summary({ worker: first.worker, target })).toMatchObject({
          ...active,
          state: "idle",
        });
      }),
  );

  it.effect("reserves concurrency with canonical CAS and releases only acknowledged inputs", () =>
    Effect.gen(function* () {
      const h = yield* harness();
      const first = yield* h.host.start(request("first"));
      const other = yield* h.host.start(request("other"));

      expect(other.delivery).toMatchObject({ status: "refused", reason: "worker-capacity" });
      yield* h.settle(first.delivery.receipt!);
      expect(yield* h.host.start(request("other"))).toEqual(other);
      expect(
        [...h.deliveries.values()].filter((record) => record.status === "refused"),
      ).toHaveLength(1);
      const second = yield* h.host.start(request("replacement"));

      expect(second.worker.threadId).not.toBe(first.worker.threadId);

      const completed = h.logs
        .get(first.worker.threadId)
        ?.filter(({ record }) => record.payload._tag === "WorkerInputCompleted");

      expect(completed).toHaveLength(1);
      yield* h.runtime.completeInput(h.submissions.get(first.delivery.receipt!.submissionId)!);
      expect(
        h.logs
          .get(first.worker.threadId)
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
          sourceSubmissionId: builder.delivery.receipt!.submissionId,
          childThreadId: Schema.decodeSync(ThreadId)("attached-scout"),
          lifetime: "attached",
          depth: 2,
          policy: childPolicy,
          grant: childGrant,
          budget: childBudget,
        });

        yield* h.runtime.reserveSubtree(builder.worker.threadId, attached);
        yield* h.runtime.reserveSubtree(builder.worker.threadId, attached);

        const canonical = h.logs.get(builder.worker.threadId)!;
        const header = canonical[0]!;

        h.logs.set(builder.worker.threadId, [
          {
            ...header,
            record: {
              ...header.record,
              payload: UserInputRecorded.make({ kind: "follow-up", input: "malformed-prefix" }),
            },
          },
          ...canonical.map((record) => ({
            ...record,
            sequence: Schema.decodeSync(CanonicalSequence)(record.sequence + 1),
          })),
        ]);
        expect(
          (yield* h.runtime.reserveSubtree(builder.worker.threadId, attached).pipe(Effect.flip))
            .reason,
        ).toBe("not-found");
        h.logs.set(builder.worker.threadId, canonical);

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

        const scout = yield* modelHost(builder.delivery.receipt!).start(nested);

        expect(
          h.submissions.get(scout.delivery.receipt!.submissionId)?.workerAdmission?.origin.depth,
        ).toBe(2);
        expect(
          h.submissions.get(scout.delivery.receipt!.submissionId)?.workerAdmission
            ?.sourceSubmissionId,
        ).toBe(builder.delivery.receipt!.submissionId);
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

        const next = yield* modelHost(nextInput.receipt!).start({
          ...nested,
          idempotencyKey: Schema.decodeSync(IdempotencyKey)("second-input-scout"),
        });

        expect(
          h.submissions.get(next.delivery.receipt!.submissionId)?.workerAdmission
            ?.sourceSubmissionId,
        ).toBe(nextInput.receipt!.submissionId);
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
          sourceSubmissionId: builder.delivery.receipt!.submissionId,
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
            receipt: member.receipt!,
          })
          .pipe(Effect.forkChild);

        yield* Effect.yieldNow;
        yield* h.settle(first.delivery.receipt!, "shared-result");
        yield* h.settle(member.receipt!, "unused", { host: first.delivery.receipt! });
        yield* TestClock.adjust("5 millis");
        const observed = yield* Fiber.join(waiting);

        expect(observed).toMatchObject({
          _tag: "Settled",
          receipt: member.receipt,
          runId: `run:${first.delivery.receipt!.submissionId}`,
          encodedParameters: { note: "member-parameters" },
          encodedResult: "shared-result",
        });
        expect(
          yield* h.host.inspect({ worker: first.worker, target, receipt: member.receipt! }),
        ).toEqual(observed);
      }),
  );

  it.effect("preserves an absent Run identity for work aborted before execution", () =>
    Effect.gen(function* () {
      const h = yield* harness();
      const first = yield* h.host.start(request("never-ran"));

      yield* h.settle(first.delivery.receipt!, "unused", { abortedBeforeRun: true });

      const observed = yield* h.host.inspect({
        worker: first.worker,
        receipt: first.delivery.receipt!,
        target,
      });

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
            .cancel({ worker: first.worker, target, receipt: first.delivery.receipt! })
            .pipe(Effect.flip))._tag,
        ).toBe("WorkerError");
        h.deny(undefined);

        const wrong = Receipt.make({
          ...first.delivery.receipt!,
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
            .inspect({ worker: accepted.worker, target, receipt: accepted.delivery.receipt! })
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
            reportWith((report) =>
              standardReport.prepare(report).pipe(
                Effect.tap(() =>
                  Effect.sync(() => {
                    calls++;
                  }),
                ),
              ),
            ),
          ],
        });

        const first = yield* h.host.start(request("report-crash"));

        expect(
          h.submissions.get(first.delivery.receipt!.submissionId)?.workerAdmission?.origin.reporting
            ?.sourceDigests,
        ).toEqual(definitions);
        h.fail(point);
        expect((yield* h.settle(first.delivery.receipt!).pipe(Effect.exit))._tag).toBe("Failure");
        h.fail(undefined);
        yield* h.runtime.completeInput(h.submissions.get(first.delivery.receipt!.submissionId)!);
        yield* h.runtime.completeInput(h.submissions.get(first.delivery.receipt!.submissionId)!);

        const decisions = h.logs
          .get(first.worker.threadId)!
          .filter(({ record }) => record.payload._tag === "WorkerReportPrepared");

        expect(decisions).toHaveLength(1);

        const rows = [...h.deliveries.values()].filter(
          (row) => row.key.ownerThreadId === first.worker.threadId,
        );

        expect(rows).toHaveLength(1);
        expect(rows[0]?.envelope.threadId).toBe(sourceId);
        expect(rows[0]?.envelope.input).toBe("original parent input");
        expect(rows[0]?.envelope.messageAdmission).toMatchObject({
          report: { outcome: "completed", result: { output: "done" } },
        });
        expect(rows[0]?.status).toBe("pending");
        expect(calls).toBe(point === "worker:before-report-append" ? 2 : 1);
        const payload = decisions[0]?.record.payload;

        if (payload?._tag !== "WorkerReportPrepared") throw new Error("missing report");
        expect((yield* Schema.decodeUnknownEffect(PreparedInput)(payload.envelope)).input).toBe(
          "original parent input",
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

        const background = Subagent.background(
          Subagent.make("research", {
            ...reportDeclaration,
            success: Schema.String,
            failure: PrivateReportFailure,
            projectResult: () =>
              Effect.gen(function* () {
                yield* Effect.acquireRelease(Effect.void, () =>
                  Effect.sync(() => {
                    released++;
                  }),
                );
                const text = yield* ReportService;

                if (fail) return yield* PrivateReportFailure.make({ secret: "private failure" });

                return text;
              }),
          }),
          { start: true, reportToParent: true },
        );

        const binding = yield* DurableWorkerBinding.make(
          {
            definition: Agent.make("source-agent", {
              input: sourceAgent.input,
              output: sourceAgent.output,
              instructions: sourceAgent.instructions,
              policy: sourceAgent.policy,
              toolkit: background.toolkit,
            }),
            model,
          },
          definitions,
        ).pipe(
          Effect.provide(background.layer),
          Effect.provideService(ReportService, "captured-service"),
        );

        const h = yield* harness({ sourceReports: binding.reporting });
        const first = yield* h.host.start(request("captured"));

        yield* h.settle(first.delivery.receipt!);
        expect(released).toBe(1);
        expect(
          [...h.deliveries.values()].find((row) => row.key.ownerThreadId === first.worker.threadId)
            ?.envelope.messageAdmission,
        ).toMatchObject({ report: { result: "captured-service" } });

        const second = yield* h.host.followUp({
          worker: first.worker,
          target,
          idempotencyKey: Schema.decodeSync(IdempotencyKey)("failure"),
          encodedInput: { text: "second" },
          encodedParameters: { note: "second" },
        });

        fail = true;
        yield* h.settle(second.receipt!);
        expect(released).toBe(2);
        expect(
          h.logs
            .get(first.worker.threadId)!
            .find(({ record }) => record.payload._tag === "WorkerReportRefused")?.record.payload,
        ).toMatchObject({ reason: "preparation" });
        expect(JSON.stringify(h.logs.get(first.worker.threadId))).not.toContain("private failure");
      }),
  );

  it.effect.each([false, true])(
    "retains historical custom-report evidence (prepared: %s)",
    (prepared) =>
      Effect.gen(function* () {
        const h = yield* harness({ sourceReports: [reportWith()] });
        const started = yield* h.host.start(request("historical-custom"));
        const receipt = started.delivery.receipt!;
        const runId = Schema.decodeSync(RunId)(`run:${receipt.submissionId}`);

        h.push(
          started.worker.threadId,
          ToolCallUnknown.make({
            runId,
            turn: 1,
            toolName: "external-action",
            toolCallId: Schema.decodeSync(ToolCallId)("unresolved-action"),
            reason: "interrupted",
          }),
          "unknown-action",
        );

        if (prepared) {
          h.fail("worker:after-report-append");
          yield* h.settle(receipt).pipe(Effect.exit);
          h.fail(undefined);
        }
        const snapshot = h.submissions.get(receipt.submissionId)!;
        const admission = snapshot.workerAdmission!;

        const origin = yield* Schema.decodeEffect(Schema.toType(WorkerOrigin))({
          ...admission.origin,
          reporting: {
            sourceDigests: definitions,
            destinationDelegationId: request("legacy").delegationId,
          },
        });

        // Reconstruct predecessor records, without introducing an executable legacy registration.
        h.submissions.set(
          receipt.submissionId,
          SubmissionSnapshot.make({
            ...snapshot,
            workerAdmission: { ...admission, origin },
          }),
        );
        for (const [threadId, records] of h.logs) {
          const restored = yield* Effect.forEach(
            records,
            Effect.fnUntraced(function* (entry) {
              const payload = entry.record.payload;
              let historical: CanonicalRecordPayload = payload;

              if (
                payload._tag === "WorkerOriginRecorded" &&
                payload.origin.worker.threadId === started.worker.threadId
              )
                historical = WorkerOriginRecorded.make({ ...payload, origin });
              if (
                payload._tag === "WorkerInputRequested" &&
                payload.admission.origin.worker.threadId === started.worker.threadId
              )
                historical = WorkerInputRequested.make({
                  ...payload,
                  admission: { ...payload.admission, origin },
                });
              if (payload._tag === "WorkerReportPrepared") {
                const { messageAdmission: _message, ...envelope } =
                  yield* Schema.decodeUnknownEffect(PreparedInput)(payload.envelope);

                historical = WorkerReportPrepared.make({
                  ...payload,
                  envelope: yield* Schema.encodeEffect(PreparedInput)({
                    ...envelope,
                    input: "saved custom report",
                    inputDigest: yield* digestJson("saved custom report"),
                  }).pipe(Effect.flatMap(Schema.decodeUnknownEffect(Schema.Json))),
                });
              }

              const encoded = yield* Schema.encodeEffect(RecordEnvelope)(
                RecordEnvelope.make({ ...entry.record, payload: historical }),
              );

              return { ...entry, record: yield* Schema.decodeEffect(RecordEnvelope)(encoded) };
            }),
          );

          h.logs.set(threadId, restored);
        }
        if (prepared) yield* h.runtime.completeInput(h.submissions.get(receipt.submissionId)!);
        else yield* h.settle(receipt);
        yield* h.runtime.completeInput(h.submissions.get(receipt.submissionId)!);

        const records = h.logs.get(started.worker.threadId)!;

        expect(
          records.filter(({ record }) => record.payload._tag === "ToolCallUnknown"),
        ).toHaveLength(1);
        expect(
          records.filter(({ record }) => record.payload._tag === "WorkerInputCompleted"),
        ).toHaveLength(0);

        const reports = records.filter(({ record }) =>
          record.payload._tag.startsWith("WorkerReport"),
        );

        expect(reports).toHaveLength(1);
        if (prepared) {
          expect(reports[0]?.record.payload._tag).toBe("WorkerReportPrepared");

          const deliveries = [...h.deliveries.values()].filter(
            (row) => row.key.ownerThreadId === started.worker.threadId,
          );

          expect(deliveries).toHaveLength(1);
          expect(deliveries[0]?.envelope.input).toBe("saved custom report");
          expect(deliveries[0]?.envelope.messageAdmission).toBeUndefined();
        } else {
          expect(reports[0]?.record.payload).toMatchObject({
            _tag: "WorkerReportRefused",
            reason: "declaration-unavailable",
          });
          expect(
            [...h.deliveries.values()].filter(
              (row) => row.key.ownerThreadId === started.worker.threadId,
            ),
          ).toHaveLength(0);
        }
      }),
  );

  // Regression: https://linear.app/reve-ai/issue/KOM-269
  it.effect.each(["worker:before-report-append", "worker:after-report-append"] as const)(
    "filters a waiting completion through registration and recovers after %s",
    (point) =>
      Effect.gen(function* () {
        let selections = 0;
        let projections = 0;

        const background = Subagent.background(
          Subagent.make("research", {
            ...reportDeclaration,
            success: Schema.String,
            projectResult: (output) => {
              projections++;

              return Effect.succeed(output);
            },
          }),
          {
            start: true,
            reportToParent: true,
            reportCompletion: (report) => {
              selections++;

              return (
                report.observation.outcome !== "completed" ||
                report.observation.encodedResult !== "waiting"
              );
            },
          },
        );

        const binding = yield* DurableWorkerBinding.make(
          {
            definition: Agent.make("source-agent", {
              input: sourceAgent.input,
              output: sourceAgent.output,
              instructions: sourceAgent.instructions,
              policy: sourceAgent.policy,
              toolkit: background.toolkit,
            }),
            model: Layer.effectContext<Agent.ModelServices, never, never>(
              Effect.die("Report capture must not acquire the model"),
            ),
          },
          definitions,
        ).pipe(Effect.provide(background.layer));

        const h = yield* harness({ sourceReports: binding.reporting });
        const first = yield* h.host.start(request("waiting"));

        h.fail(point);
        expect((yield* h.settle(first.delivery.receipt!, "waiting").pipe(Effect.exit))._tag).toBe(
          "Failure",
        );
        h.fail(undefined);
        yield* h.runtime.completeInput(h.submissions.get(first.delivery.receipt!.submissionId)!);
        yield* h.runtime.completeInput(h.submissions.get(first.delivery.receipt!.submissionId)!);
        expect(projections).toBe(0);
        expect(selections).toBe(point === "worker:before-report-append" ? 2 : 1);
        expect(
          h.logs
            .get(first.worker.threadId)!
            .filter(({ record }) => record.payload._tag === "WorkerReportRefused")
            .map(({ record }) => record.payload),
        ).toEqual([expect.objectContaining({ reason: "filtered" })]);
        expect(
          [...h.deliveries.values()].filter(
            (row) => row.key.ownerThreadId === first.worker.threadId,
          ),
        ).toHaveLength(0);

        const final = yield* h.host.followUp({
          worker: first.worker,
          target,
          idempotencyKey: Schema.decodeSync(IdempotencyKey)("final"),
          encodedInput: { text: "final" },
          encodedParameters: { note: "final" },
        });

        yield* h.settle(final.receipt!, "insurer and policy");
        yield* h.runtime.completeInput(h.submissions.get(final.receipt!.submissionId)!);
        expect(projections).toBe(1);

        const delivered = [...h.deliveries.values()].filter(
          (row) => row.key.ownerThreadId === first.worker.threadId,
        );

        expect(delivered).toHaveLength(1);
        expect(delivered[0]?.envelope.messageAdmission).toMatchObject({
          report: { outcome: "completed", result: "insurer and policy" },
        });
      }),
  );

  it.effect("retains a bounded refusal when the completion filter throws", () =>
    Effect.gen(function* () {
      let selections = 0;

      const h = yield* harness({
        sourceReports: [
          {
            ...standardReport,
            reportCompletion: () => {
              selections++;
              throw new Error("private filter diagnostic");
            },
          },
        ],
      });

      const first = yield* h.host.start(request("filter-defect"));

      yield* h.settle(first.delivery.receipt!);
      yield* h.runtime.completeInput(h.submissions.get(first.delivery.receipt!.submissionId)!);
      expect(selections).toBe(1);

      const refusals = h.logs
        .get(first.worker.threadId)!
        .filter(({ record }) => record.payload._tag === "WorkerReportRefused");

      expect(refusals).toHaveLength(1);
      expect(refusals[0]?.record.payload).toMatchObject({ reason: "defect" });
      expect(JSON.stringify(refusals)).not.toContain("private filter diagnostic");
      expect(
        [...h.deliveries.values()].filter((row) => row.key.ownerThreadId === first.worker.threadId),
      ).toHaveLength(0);
    }),
  );

  it.effect("projects the actual Run once for joined Receipts using host parameters", () =>
    Effect.gen(function* () {
      const observations: Array<unknown> = [];

      const h = yield* harness({
        sourceReports: [
          reportWith((report) =>
            standardReport.prepare(report).pipe(
              Effect.tap(() =>
                Effect.sync(() => {
                  observations.push(report.observation);
                }),
              ),
            ),
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

      yield* h.settle(first.delivery.receipt!, "actual");
      yield* h.settle(second.receipt!, "ignored", { host: first.delivery.receipt! });
      expect(observations).toHaveLength(1);
      expect(observations[0]).toMatchObject({
        encodedParameters: { note: "host-input" },
        encodedResult: "actual",
        receipt: first.delivery.receipt!,
      });
      expect(
        [...h.deliveries.values()].filter((row) => row.key.ownerThreadId === first.worker.threadId),
      ).toHaveLength(1);
    }),
  );

  for (const [reason, prepare] of [
    ["projection", () => WorkerReportPreparationFailure.make({ stage: "projection" })],
    [
      "input",
      (report: Parameters<typeof standardReport.prepare>[0]) =>
        standardReport.prepare(report).pipe(
          Effect.map(({ message }) => ({
            message: { ...message, budgetExhausted: !message.budgetExhausted },
          })),
        ),
    ],
    ["defect", () => Effect.die("private mapper diagnostic")],
  ] as const)
    it.effect(`retains permanent bounded report ${reason} refusal`, () =>
      Effect.gen(function* () {
        let calls = 0;

        const h = yield* harness({
          sourceReports: [
            reportWith((report) =>
              Effect.suspend(() => {
                calls++;

                return prepare(report);
              }),
            ),
          ],
        });

        const first = yield* h.host.start(request("refused"));

        yield* h.settle(first.delivery.receipt!);
        yield* h.runtime.completeInput(h.submissions.get(first.delivery.receipt!.submissionId)!);

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
      const fiber = yield* h.settle(first.delivery.receipt!).pipe(Effect.forkChild);

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
          reportWith((report) =>
            (block ? Effect.never : standardReport.prepare(report)).pipe(
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
      const fiber = yield* h.settle(first.delivery.receipt!).pipe(Effect.forkChild);

      yield* Effect.yieldNow;
      yield* Fiber.interrupt(fiber);
      block = false;
      yield* h.runtime.completeInput(h.submissions.get(first.delivery.receipt!.submissionId)!);
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
      const reports = [reportWith()];
      const h = yield* harness({ sourceReports: reports });
      const first = yield* h.host.start(request("runless"));

      yield* h.settle(first.delivery.receipt!, "", { abortedBeforeRun: true });
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
      yield* h.settle(second.receipt!);
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

  it.effect("charges a standard report input to the receiving worker's original ancestor", () =>
    Effect.gen(function* () {
      const scoutId = Schema.decodeSync(DelegationId)("scout");

      const scoutReport = automaticReporting(
        Subagent.make("scout", {
          target,
          parameters: reportDeclaration.parameters,
          prepareInput: reportDeclaration.prepareInput,
        }),
      );

      const h = yield* harness({
        limits: {
          maxInputsPerWorker: 2,
          maxUpdateInputsPerWorker: 1,
          maxPendingUpdateInputsPerWorker: 1,
        },
        targetReports: [
          {
            ...scoutReport,
            prepare: (report) =>
              scoutReport
                .prepare(report)
                .pipe(
                  Effect.mapError(() =>
                    WorkerReportPreparationFailure.make({ stage: "projection" }),
                  ),
                ),
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
        builder.delivery.receipt!.submissionId,
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

      {
        const submission = h.submissions.get(scout.delivery.receipt!.submissionId)!;
        const runId = Schema.decodeSync(RunId)(`run:${submission.submissionId}`);

        h.push(
          scout.worker.threadId,
          RunStartedRecord.make({ runId, policyAccountingVersion: 1, maxDurationMillis: 10_000 }),
          "scout-run",
        );
        yield* h.updates.emit({
          submission,
          runId,
          producerEpoch: Schema.decodeSync(ProducerEpoch)(0),
          definitions,
          updateId: Schema.decodeSync(IdempotencyKey)("scout-finding"),
          value: { finding: "area concern" },
        });

        const update = [...h.deliveries.values()].find((row) =>
          Schema.is(WorkerUpdate)(row.envelope.messageAdmission),
        )!;

        const metadata = update.envelope.workerAdmission!;

        expect(metadata.reportKind).toBe("update");

        const options = {
          threadId: update.envelope.threadId,
          principal: update.envelope.deliveryPrincipal,
          idempotencyKey: update.envelope.admissionKey,
          definitions: update.envelope.definitions,
          workerAdmission: metadata,
          messageAdmission: update.envelope.messageAdmission,
        };

        expect(
          yield* h.runtime
            .validateAdmission(
              metadata,
              { ...options, messageAdmission: undefined },
              update.envelope.agentId,
              update.envelope.inputDigest,
              update.envelope.input,
            )
            .pipe(Effect.flip),
        ).toMatchObject({ reason: "denied" });
        yield* h.runtime.validateAdmission(
          metadata,
          options,
          update.envelope.agentId,
          update.envelope.inputDigest,
          update.envelope.input,
        );
      }

      yield* h.settle(scout.delivery.receipt!);

      const report = [...h.deliveries.values()].find(
        (row) =>
          row.key.ownerThreadId === scout.worker.threadId &&
          !Schema.is(WorkerUpdate)(row.envelope.messageAdmission),
      )!;

      expect(report.envelope.threadId).toBe(builder.worker.threadId);
      expect(report.envelope.workerAdmission?.parameters).toEqual({
        note: "report-builder",
      });
      expect(report.envelope.workerAdmission?.origin).toEqual(
        h.submissions.get(builder.delivery.receipt!.submissionId)?.workerAdmission?.origin,
      );
      expect(report.envelope.workerAdmission?.sourceSubmissionId).toBeUndefined();
      {
        expect(report.envelope.input).toEqual({ text: "report-builder" });
        expect(Schema.is(WorkerCompletion)(report.envelope.messageAdmission)).toBe(true);
      }
      const metadata = report.envelope.workerAdmission!;

      const options = {
        threadId: report.envelope.threadId,
        principal: report.envelope.deliveryPrincipal,
        idempotencyKey: report.envelope.admissionKey,
        definitions: report.envelope.definitions,
        workerAdmission: metadata,
      };

      {
        const message = yield* Schema.decodeUnknownEffect(WorkerCompletion)(
          report.envelope.messageAdmission,
        );

        const validate = (
          completion = message,
          admission: Parameters<typeof h.runtime.validateCompletion>[1] = options,
          inputDigest = report.envelope.inputDigest,
        ) =>
          h.runtime.validateCompletion(completion, admission, report.envelope.agentId, inputDigest);

        expect(yield* validate()).toEqual(message);
        for (const changed of [
          { ...options, threadId: sourceId },
          { ...options, principal: Schema.decodeSync(Principal)("other") },
          { ...options, idempotencyKey: Schema.decodeSync(IdempotencyKey)("other") },
          { ...options, workerAdmission: undefined },
          {
            ...options,
            definitions: DefinitionDigests.make({
              ...options.definitions,
              agent: Schema.decodeSync(Digest)("b".repeat(64)),
            }),
          },
        ])
          expect((yield* validate(message, changed).pipe(Effect.flip)).reason).toBe("denied");
        expect(
          (yield* validate({ ...message, budgetExhausted: true }).pipe(Effect.flip)).reason,
        ).toBe("denied");
        expect(
          (yield* validate({
            ...message,
            report: { ...message.report, runId: Schema.decodeSync(RunId)("unprepared-run") },
          }).pipe(Effect.flip)).reason,
        ).toBe("denied");
        expect(
          (yield* validate(message, options, Schema.decodeSync(Digest)("b".repeat(64))).pipe(
            Effect.flip,
          )).reason,
        ).toBe("denied");
        h.deny("report");
        expect((yield* validate().pipe(Effect.flip)).reason).toBe("denied");
        h.deny(undefined);
      }

      yield* h.runtime.validateAdmission(
        metadata,
        options,
        report.envelope.agentId,
        report.envelope.inputDigest,
        report.envelope.input,
      );
      yield* h.runtime.validateAdmission(
        metadata,
        options,
        report.envelope.agentId,
        report.envelope.inputDigest,
        report.envelope.input,
      );

      const charged = h.logs
        .get(sourceId)!
        .filter(({ record }) => record.payload._tag === "WorkerInputRequested");

      expect(charged).toHaveLength(3);

      const subtree = h.logs
        .get(sourceId)!
        .filter(({ record }) => record.payload._tag === "SubtreeBudgetReserved");

      expect(subtree).toHaveLength(3);
      // The initial input and report occupy the same worker slot but exhaust its pending-input cap.
      expect(
        (yield* h.host.followUp({
          worker: builder.worker,
          target,
          idempotencyKey: Schema.decodeSync(IdempotencyKey)("beyond-report"),
          encodedInput: { text: "extra" },
          encodedParameters: { note: "extra" },
        })).reason,
      ).toBe("worker-capacity");
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
        const interrupted = yield* h.host.start(request("failpoint")).pipe(Effect.exit);

        if (point === "worker:before-origin-append" || point === "worker:after-origin-append")
          expect(interrupted._tag).toBe("Failure");
        else
          expect(interrupted).toMatchObject({
            _tag: "Success",
            value: { delivery: { status: "pending", receipt: null, reason: "storage" } },
          });
        h.fail(undefined);
        yield* TestClock.adjust("31 seconds");
        const started = yield* h.host.start(request("failpoint"));

        expect(started.delivery.receipt!.threadId).toBe(started.worker.threadId);
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
        expect((yield* h.settle(first.delivery.receipt!).pipe(Effect.exit))._tag).toBe("Failure");
        h.fail(undefined);
        yield* h.runtime.completeInput(h.submissions.get(first.delivery.receipt!.submissionId)!);
        yield* h.runtime.completeInput(h.submissions.get(first.delivery.receipt!.submissionId)!);
        expect(
          h.logs
            .get(first.worker.threadId)
            ?.filter(({ record }) => record.payload._tag === "WorkerInputCompleted"),
        ).toHaveLength(1);
      }),
    );
  }
});

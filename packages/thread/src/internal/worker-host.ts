import type * as Agent from "@effect-agent/core/Agent";
import { AgentPolicy } from "@effect-agent/core/AgentPolicy";
import { Update, UpdateError } from "@effect-agent/core/AgentUpdates";
import { ThreadId, type AgentId, type SubmissionId } from "@effect-agent/core/Identifiers";
import { IdempotencyKey, Receipt } from "@effect-agent/core/Receipt";
import { SubagentDelegationCaps, SubagentGrant } from "@effect-agent/core/SubagentContract";
import {
  WorkerCompletion,
  FrameworkMessage,
  WorkerUpdate,
  WorkerError,
  WorkerHistoryEntry,
  type WorkerContext,
  WorkerRef,
} from "@effect-agent/core/Worker";
import {
  type SubagentHost,
  type WorkerObservation,
  type WorkerReceiptRequest,
  type WorkerRunReport,
} from "@effect-agent/engine/SubagentHost";
import {
  Cause,
  Clock,
  Context,
  Crypto,
  DateTime,
  Duration,
  Effect,
  Option,
  Schema,
  Stream,
} from "effect";

import { digestJson } from "../Digest.ts";
import type {
  DurableAbortFailure,
  DurableAwaitFailure,
  DurableSubmitFailure,
  DurableSubmitOptions,
} from "../DurableAgentRuntime.ts";
import {
  DurableRuntimeFailpoint,
  type DurableRuntimeFailpointLocation,
} from "../DurableFailpoint.ts";
import {
  MessageDeliveryDriver,
  MessageDeliveryStore,
  prepareMessageDelivery,
} from "../MessageDelivery.ts";
import { PreparedInputAdmission } from "../PreparedInputAdmission.ts";
import {
  BatchId,
  CanonicalBatch,
  CanonicalSequence,
  type Digest,
  type DeploymentId,
  type ProducerId,
  RecordEnvelope,
  RecordId,
  WorkerAdmission,
  WorkerInputRequested,
  WorkerOrigin,
  WorkerOriginRecorded,
  WorkerInputCompleted,
  WorkerReportPrepared,
  WorkerReportRefused,
  WorkerReportingIntent,
  SubtreeBudgetReserved,
  PersistedJson,
} from "../Records.ts";
import {
  type ScheduledInputFailure,
  ScheduledInputRefused,
  ScheduledInputRetryable,
} from "../Schedule.ts";
import {
  SubmissionLedger,
  LedgerError,
  AbortCommand,
  type AbortIntent,
  type Principal,
  type Settlement,
  SubmissionLookupById,
  SubmissionLookupByKey,
  type SubmissionSnapshot,
} from "../SubmissionLedger.ts";
import type { SubmissionStatus } from "../SubmissionStatus.ts";
import { PreparedInput } from "../Subscription.ts";
import {
  ThreadStore,
  FencedAppendRequest,
  ThreadExportRequest,
  ThreadRead,
  ThreadTailRequest,
} from "../ThreadStore.ts";
import {
  WorkerBudgetAuthorizer,
  WorkerConcurrencyLimit,
  WorkerConcurrencyResolver,
  WorkerHostAuthorizer,
  WorkerHostConfig,
  WorkerPolicyResolver,
  type WorkerPolicyTarget,
} from "../WorkerHost.ts";
import {
  definitionDigestsEqual,
  resolveDefinitionBinding,
  type ResolvedBinding,
} from "./agent-registration.ts";
import { lastWorkerReportMessageId } from "./agent-updates.ts";

const failure = (operation: WorkerError["operation"], reason: WorkerError["reason"]) =>
  WorkerError.make({ operation, reason });

const storageFailure = (operation: WorkerError["operation"]) => () => failure(operation, "storage");

const decode = <S extends Schema.Top>(
  schema: S,
  value: unknown,
  operation: WorkerError["operation"],
) =>
  Schema.decodeUnknownEffect(Schema.toType(schema))(value).pipe(
    Effect.mapError(() => failure(operation, "corrupt")),
  );

const sameOrigin = Schema.toEquivalence(WorkerOrigin);
const sameAdmission = Schema.toEquivalence(WorkerAdmission);
const sameReporting = Schema.toEquivalence(Schema.UndefinedOr(WorkerReportingIntent));
const sameJson = Schema.toEquivalence(PersistedJson);
const sameCaps = Schema.toEquivalence(SubagentDelegationCaps);
const samePolicy = Schema.toEquivalence(AgentPolicy);

const amountCaps = [
  ["turns", "maxTurns"],
  ["toolCalls", "maxToolCalls"],
  ["durationMillis", "maxDurationMillis"],
  ["inputTokens", "maxInputTokens"],
  ["outputTokens", "maxOutputTokens"],
  ["costMicrousd", "maxCostMicrousd"],
  ["resultBytes", "maxResultBytes"],
] as const;

const sourceCaps = (policy: AgentPolicy): SubagentDelegationCaps =>
  SubagentDelegationCaps.make({
    maxTotalChildInvocations: policy.maxToolCalls,
    maxConcurrentChildren: policy.toolConcurrency,
    maxTurns: policy.maxTurns,
    maxToolCalls: policy.maxToolCalls,
    maxDurationMillis: Math.ceil(Duration.toMillis(policy.maxDuration)),
    maxResultBytes: Math.min(
      Number.MAX_SAFE_INTEGER,
      policy.toolResultBounds.maxBytes * policy.maxToolCalls,
    ),
    ...(policy.tokenBudget === undefined
      ? {}
      : { maxInputTokens: policy.tokenBudget, maxOutputTokens: policy.tokenBudget }),
    ...(policy.costBudgetMicrousd === undefined
      ? {}
      : { maxCostMicrousd: policy.costBudgetMicrousd }),
  });

const withinPolicy = (origin: WorkerOrigin, source: AgentPolicy, target: AgentPolicy): boolean => {
  const policy = origin.policy;
  const allocation = origin.budget.allocation;
  const caps = origin.budget.caps;
  const ceilings = sourceCaps(source);

  for (const [, name] of origin.budgetScope === "worker-run" ? [] : amountCaps) {
    if (caps[name] !== undefined && ceilings[name] !== undefined && caps[name] > ceilings[name])
      return false;
  }
  if (
    origin.budgetScope !== "worker-run" &&
    ((caps.maxTotalChildInvocations !== undefined &&
      caps.maxTotalChildInvocations > source.maxToolCalls) ||
      (caps.maxConcurrentChildren !== undefined &&
        caps.maxConcurrentChildren > source.toolConcurrency))
  )
    return false;
  const tokenCeiling = Math.min(source.tokenBudget ?? Infinity, target.tokenBudget ?? Infinity);

  const costCeiling = Math.min(
    source.costBudgetMicrousd ?? Infinity,
    target.costBudgetMicrousd ?? Infinity,
  );

  return (
    policy.maxTurns <= Math.min(allocation.turns, source.maxTurns, target.maxTurns) &&
    policy.maxToolCalls <=
      Math.min(allocation.toolCalls, source.maxToolCalls, target.maxToolCalls) &&
    Duration.toMillis(policy.maxDuration) <=
      Math.min(
        allocation.durationMillis,
        Duration.toMillis(source.maxDuration),
        Duration.toMillis(target.maxDuration),
      ) &&
    policy.toolConcurrency <= Math.min(source.toolConcurrency, target.toolConcurrency) &&
    policy.toolResultBounds.maxBytes <=
      Math.min(source.toolResultBounds.maxBytes, target.toolResultBounds.maxBytes) &&
    (tokenCeiling === Infinity ||
      (policy.tokenBudget !== undefined && policy.tokenBudget <= tokenCeiling)) &&
    (costCeiling === Infinity ||
      (policy.costBudgetMicrousd !== undefined && policy.costBudgetMicrousd <= costCeiling)) &&
    (source.tokenBudget === undefined ||
      (allocation.inputTokens > 0 &&
        allocation.outputTokens > 0 &&
        policy.tokenBudget !== undefined &&
        policy.tokenBudget <= allocation.inputTokens + allocation.outputTokens)) &&
    (source.costBudgetMicrousd === undefined ||
      (policy.costBudgetMicrousd !== undefined &&
        policy.costBudgetMicrousd <= allocation.costMicrousd))
  );
};

export interface WorkerRuntimeOptions {
  readonly bindings: ReadonlyArray<ResolvedBinding>;
  readonly deploymentId: DeploymentId;
  readonly producerId: ProducerId;
  readonly settlementPollInterval: Duration.Duration;
}

/** Admission and cancellation enter the owning runtime, never the raw ledger. */
export class WorkerInputControl extends Context.Service<
  WorkerInputControl,
  {
    readonly submit: (envelope: PreparedInput) => Effect.Effect<Receipt, DurableSubmitFailure>;
    readonly status: (
      receipt: Receipt,
    ) => Effect.Effect<SubmissionStatus, DurableAwaitFailure | ScheduledInputFailure>;
    readonly abort: (command: AbortCommand) => Effect.Effect<AbortIntent, DurableAbortFailure>;
  }
>()("@effect-agent/thread/internal/WorkerInputControl") {}

/** No in-memory ownership: every mutation is reserved in source canonical history with CAS. */
export const makeWorkerRuntime = Effect.fn("WorkerHost.make")(function* (
  options: WorkerRuntimeOptions,
) {
  const control = yield* WorkerInputControl;
  // A routed read may belong to another owner; capture it before exposing worker operations.
  const admission = yield* Effect.serviceOption(PreparedInputAdmission);

  const deps = {
    ...options,
    ...control,
    store: yield* ThreadStore,
    ledger: yield* SubmissionLedger,
    deliveries: yield* Effect.serviceOption(MessageDeliveryStore),
    crypto: yield* Crypto.Crypto,
    authorizer: yield* WorkerHostAuthorizer,
    budgetAuthorizer: yield* WorkerBudgetAuthorizer,
    policyResolver: yield* WorkerPolicyResolver,
    concurrencyResolver: yield* WorkerConcurrencyResolver,
    limits: yield* WorkerHostConfig,
    failpoint: yield* DurableRuntimeFailpoint,
    status: Option.getOrUndefined(admission)?.submissionStatus ?? control.status,
  };

  const withCrypto = <A, E>(effect: Effect.Effect<A, E, Crypto.Crypto>) =>
    Effect.provideService(effect, Crypto.Crypto, deps.crypto);

  const authorizeBudget = Effect.fn("WorkerHost.authorizeBudget")(function* (
    origin: WorkerOrigin,
    principal: Principal,
  ) {
    if (origin.budgetScope !== "worker-run") return;
    // Independence changes accounting ownership, never delegation generations.
    if (origin.depth !== 1) return yield* failure("start", "denied");

    yield* deps.budgetAuthorizer.authorize({
      source: origin.source,
      principal,
      worker: origin.worker,
      policy: origin.policy,
      budget: origin.budget,
    });
  });

  const read = (threadId: ThreadId, operation: WorkerError["operation"]) =>
    deps.store
      .export(ThreadExportRequest.make({ threadId }))
      .pipe(Effect.mapError(storageFailure(operation)));

  const hit = (location: DurableRuntimeFailpointLocation, operation: WorkerError["operation"]) =>
    deps.failpoint.hit(location).pipe(Effect.mapError(storageFailure(operation)));

  const append = Effect.fn("WorkerHost.append")(
    function* (
      threadId: ThreadId,
      id: string,
      payload:
        | WorkerInputRequested
        | WorkerOriginRecorded
        | WorkerInputCompleted
        | WorkerReportPrepared
        | WorkerReportRefused
        | SubtreeBudgetReserved,
      current: Effect.Success<ReturnType<typeof read>>,
      phase: "source" | "origin" | "completion" | "subtree" | "report",
    ) {
      const operation = "start";
      const tail = yield* deps.store.inspectTail(ThreadTailRequest.make({ threadId }));

      // Never append against a tail newer than the prefix whose capacity was checked.
      if (tail.tailSequence !== current.tailSequence || tail.tailDigest !== current.tailDigest)
        return false;
      yield* hit(`worker:before-${phase}-append`, operation);

      const result = yield* deps.store
        .append(
          FencedAppendRequest.make({
            threadId,
            producerEpoch: tail.producerEpoch,
            expectedTailSequence: current.tailSequence,
            expectedTailDigest: current.tailDigest,
            batch: CanonicalBatch.make({
              batchId: Schema.decodeSync(BatchId)(id),
              producerId: deps.producerId,
              records: [
                RecordEnvelope.make({
                  recordId: Schema.decodeSync(RecordId)(id),
                  family: "thread",
                  schemaVersion: 1,
                  createdAt: DateTime.makeUnsafe(
                    payload._tag === "WorkerInputRequested"
                      ? payload.admission.createdAtMillis
                      : payload._tag === "WorkerOriginRecorded"
                        ? payload.origin.createdAtMillis
                        : payload._tag === "WorkerInputCompleted"
                          ? payload.completedAtMillis
                          : yield* Clock.currentTimeMillis,
                  ),
                  deploymentId: deps.deploymentId,
                  payload,
                }),
              ],
            }),
          }),
        )
        .pipe(
          Effect.as(true),
          Effect.catchTag(["AppendConflict", "FenceRejected"], () => Effect.succeed(false)),
        );

      if (result) yield* hit(`worker:after-${phase}-append`, operation);

      return result;
    },
    Effect.mapError(storageFailure("start")),
  );

  const requests = (records: Effect.Success<ReturnType<typeof read>>["records"]) =>
    records.flatMap(({ record }) =>
      record.payload._tag === "WorkerInputRequested" ? [record.payload] : [],
    );

  const reportIntent = Effect.fn("WorkerHost.reportIntent")(function* (
    source: ResolvedBinding,
    target: ResolvedBinding,
    delegationId: WorkerRef["delegationId"],
  ): Effect.fn.Return<WorkerOrigin["reporting"], WorkerError> {
    const reports = source.reporting?.filter((entry) => entry.delegationId === delegationId) ?? [];
    const report = reports[0];

    if (report === undefined) return undefined;
    if (
      reports.length !== 1 ||
      !Object.is(report.target, target.definition) ||
      (report.mode !== "standard" && !Object.is(report.input, source.definition.input)) ||
      (report.destination !== undefined && !Object.is(report.destination.target, source.definition))
    )
      return yield* failure("start", "declaration-unavailable");

    return {
      sourceDigests: source.digests,
      ...(report.mode === undefined ? {} : { mode: report.mode }),
      ...(report.destination === undefined
        ? {}
        : { destinationDelegationId: report.destination.delegationId }),
    };
  });

  const sourceAuthority = Effect.fn("WorkerHost.sourceAuthority")(function* (
    threadId: ThreadId,
    submissionId?: SubmissionId,
  ) {
    const current = yield* read(threadId, "start");
    const created = current.records[0]?.record.payload;

    if (created?._tag !== "ThreadCreated") return yield* failure("start", "not-found");

    const binding = deps.bindings.find(
      (entry) =>
        entry.agentId === created.agentId &&
        definitionDigestsEqual(entry.digests, created.definitions),
    );

    const worker = current.records.find(
      ({ record }) => record.payload._tag === "WorkerOriginRecorded",
    )?.record.payload;

    const attached = current.records.find(
      ({ record }) => record.payload._tag === "SubagentLineageRecorded",
    )?.record.payload;

    const nested =
      worker?._tag === "WorkerOriginRecorded" || attached?._tag === "SubagentLineageRecorded";

    let ownerSubmission: SubmissionSnapshot | undefined;

    if (nested && submissionId === undefined) return yield* failure("start", "denied");
    if (submissionId !== undefined) {
      const submission = yield* deps.ledger
        .lookup(SubmissionLookupById.make({ submissionId }))
        .pipe(Effect.mapError(storageFailure("start")));

      if (
        Option.isNone(submission) ||
        submission.value.threadId !== threadId ||
        (nested && submission.value.agentId !== created.agentId)
      )
        return yield* failure("start", "denied");
      ownerSubmission = submission.value;
      if (
        worker?._tag === "WorkerOriginRecorded" &&
        (submission.value.workerAdmission === undefined ||
          !sameOrigin(submission.value.workerAdmission.origin, worker.origin))
      )
        return yield* failure("start", "denied");
      if (
        attached?._tag === "SubagentLineageRecorded" &&
        (submission.value.parentLinkage?.parentSubmissionId !== attached.parentSubmissionId ||
          submission.value.parentLinkage.parentToolCallId !== attached.parentLink.parentToolCallId)
      )
        return yield* failure("start", "denied");
    }

    if (worker?._tag === "WorkerOriginRecorded")
      return {
        current,
        binding,
        submission: ownerSubmission,
        policyOverride: Option.none<AgentPolicy>(),
        policy: worker.origin.policy,
        budget: worker.origin.budget,
        budgetScope: worker.origin.budgetScope,
        grant: worker.origin.grant,
        depth: worker.origin.depth,
      };
    if (attached?._tag === "SubagentLineageRecorded") {
      if (
        attached.policy === undefined ||
        attached.budget === undefined ||
        attached.grant === undefined
      )
        return yield* failure("start", "denied");

      return {
        current,
        binding,
        submission: ownerSubmission,
        policyOverride: Option.none<AgentPolicy>(),
        policy: attached.policy,
        budget: attached.budget,
        budgetScope: undefined,
        grant: attached.grant,
        depth: attached.parentLink.depth,
      };
    }

    const ownerBinding =
      ownerSubmission === undefined
        ? undefined
        : deps.bindings.find(
            (entry) =>
              entry.agentId === ownerSubmission.agentId &&
              definitionDigestsEqual(entry.digests, ownerSubmission.agentDigests),
          );

    const selectedBinding = ownerBinding ?? binding;

    const changedRootAgent =
      ownerSubmission !== undefined && ownerSubmission.agentId !== created.agentId;

    // A root conversation may admit a new registered Agent after deployment.
    // Its immutable owner input selects authority; child lineage never changes.
    if (selectedBinding === undefined || (changedRootAgent && ownerBinding === undefined))
      return yield* failure("start", "declaration-unavailable");

    const selected = yield* deps.policyResolver.resolveSource({
      threadId,
      definition: selectedBinding.definition,
      definitions: ownerSubmission?.agentDigests ?? selectedBinding.digests,
      ...(ownerSubmission === undefined ? {} : { submission: ownerSubmission }),
    });

    if (Option.isSome(selected) && ownerSubmission !== undefined && ownerBinding === undefined)
      return yield* failure("start", "declaration-unavailable");

    const effectiveBinding =
      Option.isSome(selected) || changedRootAgent ? selectedBinding : binding;

    if (effectiveBinding === undefined) return yield* failure("start", "declaration-unavailable");

    const policy = Option.isNone(selected)
      ? effectiveBinding.definition.policy
      : yield* decode(AgentPolicy, selected.value, "start");

    return {
      current,
      binding: effectiveBinding,
      submission: ownerSubmission,
      policyOverride: Option.map(selected, () => policy),
      policy,
      budget: undefined,
      budgetScope: undefined,
      grant: undefined,
      depth: 0,
    };
  });

  const resolveTargetPolicy = Effect.fn("WorkerHost.resolveTargetPolicy")(function* (
    request: WorkerPolicyTarget,
  ) {
    const selected = yield* deps.policyResolver.resolveTarget(request);

    if (Option.isNone(selected)) return selected;
    const policy = yield* decode(AgentPolicy, selected.value, "start");

    if (request._tag === "RetainedWorker" && !samePolicy(policy, request.origin.policy))
      return yield* failure("start", "worker-mismatch");

    return Option.some(policy);
  });

  const reserveSubtree = Effect.fn("WorkerHost.reserveSubtree")(function* (
    sourceThreadId: ThreadId,
    requested: SubtreeBudgetReserved,
  ): Effect.fn.Return<void, WorkerError> {
    const payload = yield* decode(SubtreeBudgetReserved, requested, "start");
    const recordId = `subtree:${payload.reservationId}`;

    for (let attempt = 0; attempt < 16; attempt++) {
      const source = yield* sourceAuthority(sourceThreadId, payload.sourceSubmissionId);

      const previous = source.current.records.find(({ record }) => record.recordId === recordId)
        ?.record.payload;

      if (previous !== undefined) {
        if (
          previous._tag !== "SubtreeBudgetReserved" ||
          !Schema.toEquivalence(SubtreeBudgetReserved)(previous, payload)
        )
          return yield* failure("start", "idempotency-conflict");

        return;
      }
      if (payload.depth !== source.depth + 1 || payload.depth > payload.grant.maxDepth)
        return yield* failure("start", "denied");
      const inherited = source.grant;
      const lifetimes = inherited?.childLifetimes;

      if (
        inherited !== undefined &&
        (payload.depth > inherited.maxDepth ||
          payload.grant.maxDepth > inherited.maxDepth ||
          (lifetimes !== undefined && !lifetimes.includes(payload.lifetime)) ||
          payload.grant.allowedToolNames.some(
            (name) => !inherited.allowedToolNames.includes(name),
          ) ||
          (lifetimes !== undefined &&
            (payload.grant.childLifetimes ?? ["attached", "background"]).some(
              (lifetime) => !lifetimes.includes(lifetime),
            )))
      )
        return yield* failure("start", "denied");

      const rows = source.current.records.flatMap(({ record }) =>
        record.payload._tag === "SubtreeBudgetReserved" &&
        record.payload.sourceSubmissionId === payload.sourceSubmissionId
          ? [record.payload]
          : [],
      );

      const caps = payload.budget.caps;

      if (rows.some((row) => !sameCaps(row.budget.caps, caps)))
        return yield* failure("start", "capacity");

      const own = {
        turns: source.policy.maxTurns,
        toolCalls: source.policy.maxToolCalls,
        durationMillis: Math.ceil(Duration.toMillis(source.policy.maxDuration)),
        inputTokens: source.policy.tokenBudget ?? source.budget?.allocation.inputTokens ?? 0,
        outputTokens: source.policy.tokenBudget ?? source.budget?.allocation.outputTokens ?? 0,
        costMicrousd:
          source.policy.costBudgetMicrousd ?? source.budget?.allocation.costMicrousd ?? 0,
        resultBytes: source.policy.toolResultBounds.maxBytes,
      };

      const ceilings = sourceCaps(source.policy);
      const child = payload.policy;
      const allocation = payload.budget.allocation;

      if (
        child.maxTurns > Math.min(source.policy.maxTurns, allocation.turns) ||
        child.maxToolCalls > Math.min(source.policy.maxToolCalls, allocation.toolCalls) ||
        Duration.toMillis(child.maxDuration) >
          Math.min(Duration.toMillis(source.policy.maxDuration), allocation.durationMillis) ||
        child.toolConcurrency > source.policy.toolConcurrency ||
        child.toolResultBounds.maxBytes > source.policy.toolResultBounds.maxBytes ||
        (source.policy.tokenBudget !== undefined &&
          (child.tokenBudget === undefined ||
            child.tokenBudget > source.policy.tokenBudget ||
            child.tokenBudget > allocation.inputTokens + allocation.outputTokens)) ||
        (source.policy.costBudgetMicrousd !== undefined &&
          (child.costBudgetMicrousd === undefined ||
            child.costBudgetMicrousd >
              Math.min(source.policy.costBudgetMicrousd, allocation.costMicrousd)))
      )
        return yield* failure("start", "capacity");
      for (const [, cap] of amountCaps) {
        if (caps[cap] !== undefined && ceilings[cap] !== undefined && caps[cap] > ceilings[cap])
          return yield* failure("start", "capacity");
      }
      if (
        (caps.maxTotalChildInvocations ?? 0) > source.policy.maxToolCalls ||
        (caps.maxConcurrentChildren ?? 0) > source.policy.toolConcurrency
      )
        return yield* failure("start", "capacity");
      const slots = 1 + (payload.budget.descendantInvocations ?? 0);

      const chargedSlots = rows.reduce(
        (sum, row) => sum + 1 + (row.budget.descendantInvocations ?? 0),
        slots,
      );

      const slotLimit = Math.min(
        caps.maxTotalChildInvocations ?? Infinity,
        source.budget === undefined
          ? source.policy.maxToolCalls
          : (source.budget.descendantInvocations ?? 0),
      );

      if (chargedSlots > slotLimit) return yield* failure("start", "capacity");
      for (const [amount, cap] of amountCaps) {
        // Zero is the encoded absence of an allocation when this dimension is unconfigured.
        // It must not become a new cumulative token or dollar ceiling in a descendant.
        if (
          source.budgetScope === "worker-run" &&
          (amount === "inputTokens" || amount === "outputTokens" || amount === "costMicrousd") &&
          source.budget !== undefined &&
          source.budget.caps[cap] === undefined &&
          source.budget.allocation[amount] === 0 &&
          ceilings[cap] === undefined
        ) {
          const charged = rows.reduce(
            (sum, row) => sum + row.budget.allocation[amount],
            allocation[amount],
          );

          if (charged > (caps[cap] ?? Infinity)) return yield* failure("start", "capacity");
          continue;
        }

        const residual =
          source.budget === undefined
            ? (ceilings[cap] ?? Infinity)
            : Math.max(0, source.budget.allocation[amount] - own[amount]);

        const limit = Math.min(caps[cap] ?? Infinity, residual);

        const charged = rows.reduce(
          (sum, row) => sum + row.budget.allocation[amount],
          payload.budget.allocation[amount],
        );

        if (charged > limit) return yield* failure("start", "capacity");
      }

      // Include unreconciled reservations conservatively; only canonical completion frees an
      // active slot. A worker Thread consumes one slot even when multiple inputs safely join it.
      const completedWorkers = new Set<string>(
        source.current.records.flatMap(({ record }) =>
          record.payload._tag === "WorkerInputCompleted" ? [record.payload.messageId] : [],
        ),
      );

      const completedAttached = new Set<string>(
        source.current.records.flatMap(({ record }) =>
          record.payload._tag === "SubagentJoined" ? [record.payload.reservationId] : [],
        ),
      );

      const active = new Set(
        rows
          .filter((row) =>
            row.lifetime === "background"
              ? !completedWorkers.has(row.reservationId)
              : !completedAttached.has(row.reservationId),
          )
          .map((row) => row.childThreadId),
      );

      if (
        !active.has(payload.childThreadId) &&
        active.size >=
          Math.min(caps.maxConcurrentChildren ?? Infinity, source.policy.toolConcurrency)
      )
        return yield* failure("start", "capacity");
      if (yield* append(sourceThreadId, recordId, payload, source.current, "subtree")) return;
    }

    return yield* failure("start", "storage");
  });

  const reservation = Effect.fn("WorkerHost.reserveInput")(function* (
    admission: WorkerAdmission,
    inputDigest: Digest,
    input: PersistedJson,
    principal: Principal,
  ): Effect.fn.Return<void, WorkerError> {
    const origin = admission.origin;
    const id = `worker-input:${admission.messageId}`;

    for (let attempt = 0; attempt < 16; attempt++) {
      const source = yield* sourceAuthority(origin.source.threadId, admission.sourceSubmissionId);
      const current = source.current;
      const rows = requests(current.records);
      const existing = current.records.find(({ record }) => record.recordId === id)?.record.payload;

      if (existing !== undefined) {
        if (
          existing._tag !== "WorkerInputRequested" ||
          existing.inputDigest !== inputDigest ||
          !sameAdmission(existing.admission, admission)
        )
          return yield* failure("start", "idempotency-conflict");
      }
      const first = current.records[0]?.record.payload;

      if (first?._tag !== "ThreadCreated") return yield* failure("start", "denied");

      const own = rows.filter(
        (row) => row.admission.origin.worker.threadId === origin.worker.threadId,
      );

      const origins = new Map(
        rows.map((row) => [row.admission.origin.worker.threadId, row.admission.origin]),
      );

      const prior = origins.get(origin.worker.threadId);

      if (
        prior === undefined &&
        (source.submission?.agentId ?? first.agentId) !== origin.source.agentId
      )
        return yield* failure("start", "denied");
      if (prior !== undefined && !sameOrigin(prior, origin))
        return yield* failure("start", "worker-mismatch");
      if (prior === undefined && admission.messageId !== origin.firstMessageId)
        return yield* failure("start", "not-found");
      const caps = origin.budget.caps;

      const sourceBinding = source.binding;

      const targetBinding = deps.bindings.find(
        (entry) =>
          entry.agentId === origin.worker.targetAgentId &&
          definitionDigestsEqual(entry.digests, origin.targetDigests),
      );

      if (sourceBinding === undefined || targetBinding === undefined)
        return yield* failure("start", "declaration-unavailable");
      yield* Schema.decodeUnknownEffect(Schema.toEncoded(targetBinding.definition.input))(
        input,
      ).pipe(Effect.mapError(() => failure("start", "corrupt")));

      const targetRequest = {
        definition: targetBinding.definition,
        definitions: targetBinding.digests,
        source: origin.source,
      };

      const selected = yield* resolveTargetPolicy(
        admission.messageId === origin.firstMessageId
          ? {
              ...targetRequest,
              _tag: "InitialInput",
              input,
              inputDigest,
              ...(source.submission === undefined ? {} : { sourceSubmission: source.submission }),
            }
          : { ...targetRequest, _tag: "RetainedWorker", origin: prior ?? origin },
      );

      const independent = origin.budgetScope === "worker-run";

      const targetPolicy = Option.isSome(selected)
        ? selected.value
        : independent
          ? targetBinding.definition.policy
          : AgentPolicy.resolve(
              targetBinding.definition.policyOverrides ?? targetBinding.definition.policy,
              source.policy,
            );

      // Captured authority is checked on replay too, before the retained reservation returns.
      // Static callers retain the old idempotent-return behavior.
      if (
        (existing === undefined || Option.isSome(selected)) &&
        !withinPolicy(origin, independent ? targetPolicy : source.policy, targetPolicy)
      )
        return yield* failure("start", "capacity");
      if (existing !== undefined) return;
      const now = yield* Clock.currentTimeMillis;

      if (now >= origin.expiresAtMillis) return yield* failure("start", "capacity");
      if (
        (prior === undefined && origins.size >= deps.limits.maxWorkersPerSource) ||
        own.filter((row) => row.admission.reportKind === admission.reportKind).length >=
          (admission.reportKind === "update"
            ? (deps.limits.maxUpdateInputsPerWorker ?? 256)
            : deps.limits.maxInputsPerWorker)
      )
        return yield* failure("start", "capacity");
      if (
        prior === undefined &&
        !sameReporting(
          yield* reportIntent(sourceBinding, targetBinding, origin.worker.delegationId),
          origin.reporting,
        )
      )
        return yield* failure("start", "denied");
      if (independent && source.depth !== 0) return yield* failure("start", "denied");
      const ceilings = sourceCaps(source.policy);

      const conservedRows = rows.filter((row) => row.admission.origin.budgetScope !== "worker-run");

      const chargedRows =
        source.depth === 0
          ? conservedRows
          : conservedRows.filter(
              (row) => row.admission.sourceSubmissionId === admission.sourceSubmissionId,
            );

      // Independent origins do not belong to this pool. Preserve the retained conserved
      // origins' cap identity check separately from the current input's charge selection.
      if (
        !independent &&
        conservedRows.some((row) => !sameCaps(row.admission.origin.budget.caps, caps))
      )
        return yield* failure("start", "capacity");
      if (
        !independent &&
        chargedRows.reduce(
          (sum, row) => sum + 1 + (row.admission.origin.budget.descendantInvocations ?? 0),
          1 + (origin.budget.descendantInvocations ?? 0),
        ) > Math.min(caps.maxTotalChildInvocations ?? Infinity, source.policy.maxToolCalls)
      )
        return yield* failure("start", "capacity");
      // Worker-owned Run usage is already durable in the destination's Run journal. Each
      // Receipt may join an existing Run, so admitting input must never mint an allowance.
      for (const [amount, cap] of independent ? [] : amountCaps) {
        const limit = Math.min(caps[cap] ?? Infinity, ceilings[cap] ?? Infinity);

        if (
          chargedRows.reduce(
            (sum, row) => sum + row.admission.origin.budget.allocation[amount],
            origin.budget.allocation[amount],
          ) > limit
        )
          return yield* failure("start", "capacity");
      }

      // Child owners append acknowledgements before ledger finalization. This works through
      // routed canonical storage; source-local delivery rows are never read from a child owner.
      const completed = new Set(
        current.records.flatMap(({ record }) =>
          record.payload._tag === "WorkerInputCompleted" ? [record.payload.messageId] : [],
        ),
      );

      const pendingRows = rows.filter((row) => !completed.has(row.admission.messageId));
      const activeWorkers = new Set(pendingRows.map((row) => row.admission.origin.worker.threadId));

      const pendingOwn = pendingRows.filter(
        (row) =>
          row.admission.origin.worker.threadId === origin.worker.threadId &&
          row.admission.reportKind === admission.reportKind,
      ).length;

      // Resolve inside the source CAS loop: independently delivered starts must compete
      // against one canonical prefix. A conflict repeats both authority and capacity reads.
      const selectedConcurrency = yield* deps.concurrencyResolver.resolve({
        source: origin.source,
        worker: origin.worker,
        principal,
        ...(source.submission === undefined ? {} : { sourceSubmission: source.submission }),
      });

      const sourceConcurrency = Option.isSome(selectedConcurrency)
        ? (yield* decode(WorkerConcurrencyLimit, selectedConcurrency.value, "start"))
            .maxActiveWorkersPerSource
        : Infinity;

      if (
        pendingOwn >=
          (admission.reportKind === "update"
            ? (deps.limits.maxPendingUpdateInputsPerWorker ?? 32)
            : deps.limits.maxPendingInputsPerWorker) ||
        (!activeWorkers.has(origin.worker.threadId) &&
          activeWorkers.size >=
            Math.min(
              sourceConcurrency,
              deps.limits.maxActiveWorkersPerSource ?? source.policy.toolConcurrency,
              independent
                ? Infinity
                : Math.min(caps.maxConcurrentChildren ?? Infinity, source.policy.toolConcurrency),
            ))
      )
        return yield* WorkerError.make({ operation: "start", reason: "capacity", retryable: true });
      if (!independent)
        yield* reserveSubtree(
          origin.source.threadId,
          SubtreeBudgetReserved.make({
            reservationId: Schema.decodeSync(SubtreeBudgetReserved.fields.reservationId)(
              admission.messageId,
            ),
            ...(admission.sourceSubmissionId === undefined
              ? {}
              : { sourceSubmissionId: admission.sourceSubmissionId }),
            childThreadId: origin.worker.threadId,
            lifetime: "background",
            depth: origin.depth,
            policy: origin.policy,
            grant: origin.grant,
            budget: origin.budget,
          }),
        );
      if (
        yield* append(
          origin.source.threadId,
          id,
          WorkerInputRequested.make({ admission, inputDigest }),
          current,
          "source",
        )
      )
        return;
    }

    return yield* failure("start", "storage");
  });

  const ensureOrigin = Effect.fn("WorkerHost.ensureOrigin")(function* (origin: WorkerOrigin) {
    for (let attempt = 0; attempt < 16; attempt++) {
      const current = yield* read(origin.worker.threadId, "start");

      const existing = current.records.find(
        ({ record }) => record.payload._tag === "WorkerOriginRecorded",
      )?.record.payload;

      if (existing?._tag === "WorkerOriginRecorded") {
        if (!sameOrigin(existing.origin, origin)) return yield* failure("start", "worker-mismatch");

        return;
      }
      const first = current.records[0]?.record.payload;

      if (
        first?._tag !== "ThreadCreated" ||
        first.agentId !== origin.worker.targetAgentId ||
        !definitionDigestsEqual(first.definitions, origin.targetDigests) ||
        current.records.some(({ record }) => record.payload._tag === "SubagentLineageRecorded")
      )
        return yield* failure("start", "worker-mismatch");
      if (
        yield* append(
          origin.worker.threadId,
          `worker-origin:${origin.worker.threadId}`,
          WorkerOriginRecorded.make({ origin }),
          current,
          "origin",
        )
      )
        return;
    }

    return yield* failure("start", "storage");
  });

  const validateAdmission = Effect.fn("WorkerHost.validateAdmission")(function* (
    unvalidated: WorkerAdmission,
    options: DurableSubmitOptions,
    agentId: Agent.AnyDefinition["id"],
    inputDigest: Digest,
    input: PersistedJson,
  ) {
    const admission = yield* decode(WorkerAdmission, unvalidated, "start");

    if (admission.reportKind === "update") {
      if (!Schema.is(WorkerUpdate)(options.messageAdmission))
        return yield* failure("start", "denied");
      yield* validateCompletion(options.messageAdmission, options, agentId, inputDigest);
    }

    yield* deps.authorizer.authorize({
      sourceThreadId: admission.origin.source.threadId,
      ...(admission.sourceSubmissionId === undefined
        ? {}
        : { sourceSubmissionId: admission.sourceSubmissionId }),
      principal: options.principal,
      operation: "start",
      access: "send",
      worker: admission.origin.worker,
    });
    yield* authorizeBudget(admission.origin, options.principal);
    if (
      admission.origin.worker.threadId !== options.threadId ||
      admission.origin.worker.targetAgentId !== agentId ||
      !definitionDigestsEqual(admission.origin.targetDigests, options.definitions) ||
      options.idempotencyKey !== admission.messageId ||
      (admission.deliveryPrincipal !== undefined &&
        admission.deliveryPrincipal !== options.principal)
    )
      return yield* failure("start", "worker-mismatch");
    yield* reservation(admission, inputDigest, input, options.principal);

    return admission;
  });

  const prepareUpdate = Effect.fn("WorkerHost.prepareUpdate")(
    function* (update: Update, submission: SubmissionSnapshot, messageId: IdempotencyKey) {
      const origin = submission.workerAdmission?.origin;
      const intent = origin?.reporting;

      if (origin === undefined || intent?.mode !== "standard") return undefined;
      if (update.threadId !== submission.threadId || update.agentId !== origin.worker.targetAgentId)
        return yield* failure("followUp", "worker-mismatch");

      const sourceBinding = deps.bindings.find(
        (entry) =>
          entry.agentId === origin.source.agentId &&
          definitionDigestsEqual(entry.digests, intent.sourceDigests),
      );

      const targetBinding = deps.bindings.find(
        (entry) =>
          entry.agentId === origin.worker.targetAgentId &&
          definitionDigestsEqual(entry.digests, origin.targetDigests),
      );

      const reports =
        sourceBinding?.reporting?.filter(
          (entry) => entry.delegationId === origin.worker.delegationId,
        ) ?? [];

      if (
        sourceBinding === undefined ||
        targetBinding === undefined ||
        reports.length !== 1 ||
        reports[0]?.mode !== "standard" ||
        reports[0].target !== targetBinding.definition ||
        !definitionDigestsEqual(submission.agentDigests, origin.targetDigests)
      )
        return yield* failure("followUp", "declaration-unavailable");
      const sourceHistory = yield* read(origin.source.threadId, "followUp");

      const first = requests(sourceHistory.records).find(
        (row) => row.admission.messageId === origin.firstMessageId,
      );

      if (first === undefined || !sameOrigin(first.admission.origin, origin))
        return yield* failure("followUp", "corrupt");

      const source = yield* sourceAuthority(
        origin.source.threadId,
        first.admission.sourceSubmissionId,
      );

      if (
        source.binding === undefined ||
        source.submission === undefined ||
        !definitionDigestsEqual(source.binding.digests, intent.sourceDigests)
      )
        return yield* failure("followUp", "declaration-unavailable");

      const recorded = source.current.records.find(
        ({ record }) => record.payload._tag === "WorkerOriginRecorded",
      )?.record.payload;

      const receivingOrigin =
        recorded?._tag === "WorkerOriginRecorded" ? recorded.origin : undefined;

      if (source.depth !== 0 && receivingOrigin === undefined)
        return yield* failure("followUp", "denied");
      const now = yield* Clock.currentTimeMillis;

      const deadlineAtMillis = Math.min(
        origin.expiresAtMillis,
        receivingOrigin?.expiresAtMillis ?? Infinity,
      );

      if (now >= deadlineAtMillis) return yield* failure("followUp", "denied");
      const sourceSubmission = source.submission;

      const input = yield* Schema.decodeUnknownEffect(
        Schema.toEncoded(sourceBinding.definition.input),
      )(source.submission.inputPayload).pipe(
        Effect.flatMap(() =>
          Schema.decodeUnknownEffect(PersistedJson)(sourceSubmission.inputPayload),
        ),
        Effect.mapError(() => failure("followUp", "corrupt")),
      );

      let workerAdmission: WorkerAdmission | undefined;
      let principal = submission.principal;

      if (receivingOrigin !== undefined) {
        const retained = source.submission.workerAdmission;

        if (retained === undefined || !sameOrigin(retained.origin, receivingOrigin))
          return yield* failure("followUp", "denied");
        principal = source.submission.principal;
        workerAdmission = {
          origin: receivingOrigin,
          reportKind: "update",
          messageId,
          parameters: retained.parameters,
          createdAtMillis: now,
          ...(retained.sourceSubmissionId === undefined
            ? {}
            : { sourceSubmissionId: retained.sourceSubmissionId }),
        };
      }

      const sourceSubmissionId =
        receivingOrigin === undefined
          ? first.admission.sourceSubmissionId
          : workerAdmission?.sourceSubmissionId;

      const authorized = yield* deps.authorizer.authorize({
        sourceThreadId: receivingOrigin?.source.threadId ?? origin.source.threadId,
        ...(sourceSubmissionId === undefined ? {} : { sourceSubmissionId }),
        principal,
        operation: "followUp",
        access: "send",
        worker: receivingOrigin?.worker ?? origin.worker,
      });

      const message = WorkerUpdate.make({
        _tag: "WorkerUpdate",
        schemaVersion: 1,
        worker: origin.worker,
        update,
      });

      const envelope: PreparedInput = {
        schemaVersion: 1,
        threadId: origin.source.threadId,
        deliveryPrincipal: authorized,
        agentId: origin.source.agentId,
        definitions: intent.sourceDigests,
        input,
        inputDigest: yield* withCrypto(digestJson(input)).pipe(
          Effect.mapError(storageFailure("followUp")),
        ),
        admissionKey: messageId,
        authorization: { policyId: "worker-report", decisionId: messageId },
        messageAdmission: message,
        ...(workerAdmission === undefined
          ? {}
          : { workerAdmission: { ...workerAdmission, deliveryPrincipal: authorized } }),
      };

      return {
        messageId,
        envelope: yield* Schema.encodeEffect(PreparedInput)(envelope).pipe(
          Effect.flatMap(Schema.decodeUnknownEffect(PersistedJson)),
          Effect.mapError(storageFailure("followUp")),
        ),
        createdAtMillis: now,
        deadlineAtMillis,
      };
    },
    Effect.mapError((error) =>
      error.reason === "storage" || error.reason === "corrupt"
        ? LedgerError.make({ operation: "worker-update", message: error.reason })
        : UpdateError.make({ reason: error.reason === "denied" ? "denied" : "unavailable" }),
    ),
  );

  const reportRun = Effect.fn("WorkerHost.reportRun")(function* (
    submission: SubmissionSnapshot,
    history: Effect.Success<ReturnType<typeof read>>,
  ): Effect.fn.Return<void, WorkerError> {
    const admission = submission.workerAdmission;
    const intent = admission?.origin.reporting;

    if (admission === undefined || intent === undefined) return;
    const origin = admission.origin;

    const member = history.records.find(
      ({ record }) =>
        record.payload._tag === "SubmissionSettled" &&
        record.payload.submissionId === submission.submissionId,
    )?.record.payload;

    if (member?._tag !== "SubmissionSettled") return yield* failure("inspect", "corrupt");
    if (member.runId === undefined) return; // No actual Run means no fabricated report identity.
    const runId = member.runId;

    const host = history.records.find(
      ({ record }) => record.payload._tag === "SubmissionSettled" && record.payload.runId === runId,
    )?.record.payload;

    if (host?._tag !== "SubmissionSettled") return yield* failure("inspect", "corrupt");

    const messageId = yield* withCrypto(
      digestJson(["worker-report", submission.threadId, runId]),
    ).pipe(
      Effect.map((value) => Schema.decodeSync(IdempotencyKey)(`report:${value}`)),
      Effect.mapError(storageFailure("inspect")),
    );

    const recordId = `worker-report:${messageId}`;

    const prepare = Effect.fn("WorkerHost.prepareReport")(function* (): Effect.fn.Return<
      WorkerReportPrepared | WorkerReportRefused,
      WorkerError
    > {
      const refused = (reason: WorkerReportRefused["reason"]) =>
        WorkerReportRefused.make({ runId, messageId, reason });

      const sourceBinding = deps.bindings.find(
        (entry) =>
          entry.agentId === origin.source.agentId &&
          definitionDigestsEqual(entry.digests, intent.sourceDigests),
      );

      const targetBinding = deps.bindings.find(
        (entry) =>
          entry.agentId === origin.worker.targetAgentId &&
          definitionDigestsEqual(entry.digests, origin.targetDigests),
      );

      const reports =
        sourceBinding?.reporting?.filter(
          (entry) => entry.delegationId === origin.worker.delegationId,
        ) ?? [];

      const descriptor = reports[0];

      if (
        sourceBinding === undefined ||
        targetBinding === undefined ||
        descriptor === undefined ||
        reports.length !== 1 ||
        !Object.is(descriptor.target, targetBinding.definition) ||
        (descriptor.mode !== "standard" &&
          !Object.is(descriptor.input, sourceBinding.definition.input)) ||
        descriptor.mode !== intent.mode ||
        descriptor.destination?.delegationId !== intent.destinationDelegationId ||
        (descriptor.destination !== undefined &&
          !Object.is(descriptor.destination.target, sourceBinding.definition))
      )
        return refused("declaration-unavailable");

      const selected = yield* deps.ledger
        .lookup(SubmissionLookupById.make({ submissionId: host.submissionId }))
        .pipe(Effect.mapError(storageFailure("inspect")));

      if (
        Option.isNone(selected) ||
        selected.value.threadId !== submission.threadId ||
        selected.value.receiptId !== host.receiptId ||
        selected.value.workerAdmission === undefined ||
        !sameOrigin(selected.value.workerAdmission.origin, origin)
      )
        return yield* failure("inspect", "corrupt");
      const hostSubmission = selected.value;
      const hostAdmission = selected.value.workerAdmission;

      const sourceHistory = yield* read(origin.source.threadId, "inspect");

      const firstInput = requests(sourceHistory.records).find(
        (row) => row.admission.messageId === origin.firstMessageId,
      );

      if (firstInput === undefined || !sameOrigin(firstInput.admission.origin, origin))
        return yield* failure("inspect", "corrupt");

      // Reporting stays with the original owner even when a later input came from another
      // source revision. Per-input parameters still belong to the settled Run.
      const source = yield* sourceAuthority(
        origin.source.threadId,
        firstInput.admission.sourceSubmissionId,
      );

      if (
        source.binding === undefined ||
        !definitionDigestsEqual(source.binding.digests, intent.sourceDigests)
      )
        return refused("declaration-unavailable");

      const sourceOriginRecord = source.current.records.find(
        ({ record }) => record.payload._tag === "WorkerOriginRecorded",
      )?.record.payload;

      const sourceOrigin =
        sourceOriginRecord?._tag === "WorkerOriginRecorded" ? sourceOriginRecord.origin : undefined;

      if (source.depth !== 0 && sourceOrigin === undefined) return refused("destination");
      if (
        descriptor.mode !== "standard" &&
        sourceOrigin !== undefined &&
        (descriptor.destination === undefined ||
          descriptor.destination.delegationId !== sourceOrigin.worker.delegationId ||
          descriptor.destination.target.id !== sourceOrigin.worker.targetAgentId)
      )
        return refused("destination");
      if (sourceOrigin === undefined && descriptor.destination !== undefined)
        return refused("destination");
      const now = yield* Clock.currentTimeMillis;

      const deadlineAtMillis = Math.min(
        origin.expiresAtMillis,
        sourceOrigin?.expiresAtMillis ?? Infinity,
      );

      if (now >= deadlineAtMillis) return refused("expired");

      const context: WorkerContext = {
        source: origin.source,
        policy: source.policy,
        depth: source.depth,
        ...(source.grant === undefined ? {} : { grant: source.grant }),
      };

      const report: WorkerRunReport = {
        worker: origin.worker,
        context,
        observation: {
          _tag: "Settled",
          receipt: Receipt.make({
            threadId: hostSubmission.threadId,
            submissionId: hostSubmission.submissionId,
            receiptId: hostSubmission.receiptId,
            queueSequence: hostSubmission.queueSequence,
          }),
          runId,
          settlementId: host.settlementId,
          outcome: host.outcome,
          encodedParameters: hostAdmission.parameters,
          encodedResult: host.result ?? null,
          budgetExhausted: host.finishReason === "budget-exhausted",
        },
      };

      const projection = yield* Effect.suspend(() => descriptor.prepare(report)).pipe(
        Effect.timeout(
          Math.min(deps.limits.reportPreparationTimeoutMillis ?? 5_000, deadlineAtMillis - now),
        ),
        Effect.map((value) => ({ _tag: "Prepared" as const, value })),
        Effect.catchTag("WorkerReportPreparationFailure", (error) =>
          Effect.succeed(refused(error.stage)),
        ),
        Effect.catchTag("TimeoutError", () => Effect.succeed(refused("timeout"))),
        Effect.catchCause((cause) =>
          Cause.hasInterrupts(cause) ? Effect.failCause(cause) : Effect.succeed(refused("defect")),
        ),
      );

      if (projection._tag === "WorkerReportRefused") return projection;

      if (descriptor.mode === "standard") {
        const message = projection.value.message;

        if (source.submission === undefined || !Schema.is(WorkerCompletion)(message))
          return refused("input");
        if (
          !Schema.toEquivalence(WorkerRef)(message.report.worker, origin.worker) ||
          !Schema.toEquivalence(Receipt)(message.report.receipt, report.observation.receipt) ||
          message.report.runId !== runId ||
          message.report.settlementId !== host.settlementId ||
          message.report.outcome !== host.outcome ||
          message.budgetExhausted !== report.observation.budgetExhausted
        )
          return refused("input");
      } else if (projection.value.message !== undefined) return refused("input");

      const reportInput =
        descriptor.mode === "standard"
          ? source.submission?.inputPayload
          : projection.value.encodedInput;

      const validated = yield* Schema.decodeUnknownEffect(
        Schema.toEncoded(sourceBinding.definition.input),
      )(reportInput).pipe(
        Effect.flatMap(() => Schema.decodeUnknownEffect(PersistedJson)(reportInput)),
        Effect.option,
      );

      if (Option.isNone(validated)) return refused("input");
      const input = validated.value;

      const inputDigest = yield* withCrypto(digestJson(input)).pipe(
        Effect.mapError(storageFailure("inspect")),
      );

      let workerAdmission: WorkerAdmission | undefined;
      let principal = hostSubmission.principal;

      if (sourceOrigin !== undefined) {
        const ownerId =
          descriptor.mode === "standard"
            ? firstInput.admission.sourceSubmissionId
            : hostAdmission.sourceSubmissionId;

        if (ownerId === undefined) return refused("destination");

        const sourceSnapshot = yield* deps.ledger
          .lookup(SubmissionLookupById.make({ submissionId: ownerId }))
          .pipe(Effect.mapError(storageFailure("inspect")));

        if (
          Option.isNone(sourceSnapshot) ||
          sourceSnapshot.value.threadId !== origin.source.threadId ||
          sourceSnapshot.value.workerAdmission === undefined ||
          !sameOrigin(sourceSnapshot.value.workerAdmission.origin, sourceOrigin)
        )
          return refused("destination");

        const parameters = yield* Schema.decodeUnknownEffect(PersistedJson)(
          descriptor.mode === "standard"
            ? sourceSnapshot.value.workerAdmission.parameters
            : projection.value.encodedParameters,
        ).pipe(Effect.option);

        if (Option.isNone(parameters)) return refused("destination");
        principal = sourceSnapshot.value.principal;
        workerAdmission = {
          origin: sourceOrigin,
          messageId,
          parameters: parameters.value,
          createdAtMillis: now,
          ...(sourceSnapshot.value.workerAdmission.sourceSubmissionId === undefined
            ? {}
            : { sourceSubmissionId: sourceSnapshot.value.workerAdmission.sourceSubmissionId }),
        };
      }

      // Reporting remains owned by the original allocator, even when a later input joins
      // or starts another Run. Nested reports authorize the enclosing worker's source owner.
      const authorizationSourceSubmissionId =
        sourceOrigin === undefined
          ? firstInput.admission.sourceSubmissionId
          : workerAdmission?.sourceSubmissionId;

      const authorized = yield* deps.authorizer
        .authorize({
          sourceThreadId: sourceOrigin?.source.threadId ?? origin.source.threadId,
          ...(authorizationSourceSubmissionId === undefined
            ? {}
            : { sourceSubmissionId: authorizationSourceSubmissionId }),
          principal,
          operation: "followUp",
          access: "send",
          worker: sourceOrigin?.worker ?? origin.worker,
        })
        .pipe(
          Effect.map(Option.some),
          Effect.catchTag("WorkerError", (error) =>
            error.reason === "storage" || error.reason === "unavailable"
              ? Effect.fail(error)
              : Effect.succeed(Option.none<Principal>()),
          ),
        );

      if (Option.isNone(authorized)) return refused("denied");

      const envelope: PreparedInput = {
        schemaVersion: 1,
        threadId: origin.source.threadId,
        deliveryPrincipal: authorized.value,
        agentId: origin.source.agentId,
        definitions: intent.sourceDigests,
        input,
        inputDigest,
        admissionKey: messageId,
        authorization: { policyId: "worker-report", decisionId: messageId },
        ...(projection.value.message === undefined
          ? {}
          : { messageAdmission: projection.value.message }),
        ...(workerAdmission === undefined
          ? {}
          : { workerAdmission: { ...workerAdmission, deliveryPrincipal: authorized.value } }),
      };

      const encoded = yield* Schema.encodeEffect(PreparedInput)(envelope).pipe(
        Effect.flatMap(Schema.decodeUnknownEffect(PersistedJson)),
        Effect.mapError(storageFailure("inspect")),
      );

      return WorkerReportPrepared.make({
        runId,
        messageId,
        envelope: encoded,
        createdAtMillis: now,
        deadlineAtMillis,
        ...(lastWorkerReportMessageId(history.records) === undefined
          ? {}
          : { predecessor: lastWorkerReportMessageId(history.records) }),
      });
    });

    let decision: WorkerReportPrepared | WorkerReportRefused | undefined;

    for (let attempt = 0; attempt < 16; attempt++) {
      const current = yield* read(submission.threadId, "inspect");

      const existing = current.records.find(({ record }) => record.recordId === recordId)?.record
        .payload;

      if (existing !== undefined) {
        if (
          (existing._tag !== "WorkerReportPrepared" && existing._tag !== "WorkerReportRefused") ||
          existing.runId !== runId ||
          existing.messageId !== messageId
        )
          return yield* failure("inspect", "corrupt");
        decision = existing;
        break;
      }
      decision ??= yield* prepare();
      if (yield* append(submission.threadId, recordId, decision, current, "report")) break;
      if (attempt === 15) return yield* failure("inspect", "storage");
    }
    if (decision?._tag !== "WorkerReportPrepared") return;
    if (Option.isNone(deps.deliveries)) return yield* failure("inspect", "unavailable");

    const envelope = yield* Schema.decodeUnknownEffect(PreparedInput)(decision.envelope).pipe(
      Effect.mapError(() => failure("inspect", "corrupt")),
    );

    const prepared = yield* withCrypto(
      prepareMessageDelivery({
        key: { ownerThreadId: submission.threadId, messageId },
        envelope,
        createdAtMillis: decision.createdAtMillis,
        deadlineAtMillis: decision.deadlineAtMillis,
        ...(decision.predecessor === undefined ? {} : { predecessor: decision.predecessor }),
      }),
    ).pipe(Effect.mapError(storageFailure("inspect")));

    // Until this insertion commits the child ledger stays nonterminal. Afterwards its independent
    // due index survives both child and source settlement, including Object eviction.
    yield* hit("worker:before-report-delivery", "inspect");
    yield* deps.deliveries.value.insert(prepared).pipe(Effect.mapError(storageFailure("inspect")));
    yield* hit("worker:after-report-delivery", "inspect");
  });

  const completeInput = Effect.fn("WorkerHost.completeInput")(function* (
    submission: SubmissionSnapshot,
  ) {
    const admission = submission.workerAdmission;

    if (admission === undefined) return;
    const child = yield* read(submission.threadId, "inspect");

    yield* reportRun(submission, child);

    const settledEnvelope = child.records.find(
      ({ record }) =>
        record.payload._tag === "SubmissionSettled" &&
        record.payload.submissionId === submission.submissionId,
    )?.record;

    const settled = settledEnvelope?.payload;

    if (
      settled?._tag !== "SubmissionSettled" ||
      settled.receiptId !== submission.receiptId ||
      settledEnvelope === undefined
    )
      return yield* failure("inspect", "corrupt");

    const payload = WorkerInputCompleted.make({
      messageId: admission.messageId,
      workerThreadId: submission.threadId,
      submissionId: submission.submissionId,
      receiptId: submission.receiptId,
      settlementId: settled.settlementId,
      completedAtMillis: DateTime.toEpochMillis(settledEnvelope.createdAt),
    });

    for (let attempt = 0; attempt < 16; attempt++) {
      const source = yield* read(admission.origin.source.threadId, "inspect");

      const existing = source.records.find(
        ({ record }) =>
          record.payload._tag === "WorkerInputCompleted" &&
          record.payload.messageId === admission.messageId,
      )?.record.payload;

      if (existing !== undefined) {
        if (
          existing._tag !== "WorkerInputCompleted" ||
          !Schema.toEquivalence(WorkerInputCompleted)(existing, payload)
        )
          return yield* failure("inspect", "corrupt");

        return;
      }
      if (
        yield* append(
          admission.origin.source.threadId,
          `worker-completed:${admission.messageId}`,
          payload,
          source,
          "completion",
        )
      )
        return;
    }

    return yield* failure("inspect", "storage");
  });

  const facet = (
    context: WorkerContext,
    principal: Principal,
    sourceSubmissionId?: SubmissionId,
  ): SubagentHost["Service"] => {
    const authorize = (
      operation: WorkerError["operation"],
      access: "context" | "read" | "send" | "control",
      worker?: WorkerRef,
    ) =>
      deps.authorizer.authorize({
        sourceThreadId: context.source.threadId,
        ...(sourceSubmissionId === undefined ? {} : { sourceSubmissionId }),
        principal,
        operation,
        access,
        ...(worker === undefined ? {} : { worker }),
      });

    const binding = (target: Agent.AnyDefinition, operation: WorkerError["operation"]) =>
      resolveDefinitionBinding(deps.bindings, target).pipe(
        Effect.mapError(() => failure(operation, "declaration-unavailable")),
      );

    const preparedTarget = Effect.fn("WorkerHost.preparedTarget")(function* (
      target: Agent.AnyDefinition,
      encodedInput: unknown,
    ) {
      const resolved = yield* binding(target, "start");

      yield* Schema.decodeUnknownEffect(Schema.toEncoded(target.input))(encodedInput).pipe(
        Effect.mapError(() => failure("start", "corrupt")),
      );
      const input = yield* decode(PersistedJson, encodedInput, "start");

      const inputDigest = yield* withCrypto(digestJson(input)).pipe(
        Effect.mapError(storageFailure("start")),
      );

      const source = yield* sourceAuthority(context.source.threadId, sourceSubmissionId);

      const policy = yield* resolveTargetPolicy({
        _tag: "InitialInput",
        definition: resolved.definition,
        definitions: resolved.digests,
        source: context.source,
        ...(source.submission === undefined ? {} : { sourceSubmission: source.submission }),
        input,
        inputDigest,
      });

      return { resolved, source, policy };
    });

    const findOrigin = Effect.fn("WorkerHost.findOrigin")(function* (
      worker: WorkerRef,
      target: Agent.AnyDefinition,
      operation: WorkerError["operation"],
    ) {
      const resolved = yield* binding(target, operation);

      if (worker.targetAgentId !== target.id) return yield* failure(operation, "worker-mismatch");
      const source = yield* read(context.source.threadId, operation);

      const origin = requests(source.records).find(
        (row) => row.admission.origin.worker.threadId === worker.threadId,
      )?.admission.origin;

      if (origin === undefined) return yield* failure(operation, "not-found");
      if (
        origin.worker.delegationId !== worker.delegationId ||
        !definitionDigestsEqual(origin.targetDigests, resolved.digests)
      )
        return yield* failure(operation, "worker-mismatch");

      return origin;
    });

    const receiptInput = Effect.fn("WorkerHost.receiptInput")(function* (
      request: WorkerReceiptRequest,
      operation: WorkerError["operation"],
    ) {
      const origin = yield* findOrigin(request.worker, request.target, operation);
      const receipt = request.receipt;

      const snapshot = yield* deps.ledger
        .lookup(SubmissionLookupById.make({ submissionId: receipt.submissionId }))
        .pipe(Effect.mapError(storageFailure(operation)));

      if (Option.isNone(snapshot)) return yield* failure(operation, "not-found");
      const row = snapshot.value;

      if (
        row.threadId !== request.worker.threadId ||
        row.threadId !== receipt.threadId ||
        row.receiptId !== receipt.receiptId ||
        row.queueSequence !== receipt.queueSequence ||
        row.workerAdmission === undefined ||
        !sameOrigin(row.workerAdmission.origin, origin)
      )
        return yield* failure(operation, "receipt-mismatch");

      return row.workerAdmission;
    });

    const observation = Effect.fn("WorkerHost.observation")(function* (
      request: WorkerReceiptRequest,
      admission: WorkerAdmission,
      settlement: Settlement,
    ): Effect.fn.Return<WorkerObservation, WorkerError> {
      const history = yield* read(request.worker.threadId, "inspect");

      const record = history.records.find(
        ({ record }) =>
          record.payload._tag === "SubmissionSettled" &&
          record.payload.submissionId === settlement.submissionId,
      )?.record.payload;

      if (
        record?._tag !== "SubmissionSettled" ||
        record.settlementId !== settlement.settlementId ||
        record.receiptId !== request.receipt.receiptId ||
        record.outcome !== settlement.outcome
      )
        return yield* failure("inspect", "corrupt");

      const input = history.records.find(
        ({ record }) =>
          record.payload._tag === "UserInputRecorded" &&
          record.payload.submissionId === settlement.submissionId,
      )?.record.payload;

      const inputRunId = input?._tag === "UserInputRecorded" ? input.runId : undefined;

      if (inputRunId !== undefined && record.runId !== undefined && inputRunId !== record.runId)
        return yield* failure("inspect", "corrupt");
      const runId = record.runId ?? inputRunId;

      // The host settlement precedes the joined members and carries the Run's actual output.
      // Both identities come from canonical history; no foreign recovery or finalization is needed.
      const host =
        runId === undefined
          ? undefined
          : history.records.find(
              ({ record }) =>
                record.payload._tag === "SubmissionSettled" && record.payload.runId === runId,
            )?.record.payload;

      const result = host?._tag === "SubmissionSettled" ? host : record;

      if (result.outcome !== record.outcome) return yield* failure("inspect", "corrupt");

      return {
        _tag: "Settled",
        receipt: request.receipt,
        settlementId: settlement.settlementId,
        ...(result.runId === undefined ? {} : { runId: result.runId }),
        outcome: settlement.outcome,
        encodedParameters: admission.parameters,
        encodedResult: result.result ?? null,
        budgetExhausted: result.finishReason === "budget-exhausted",
      };
    });

    const send = Effect.fn("WorkerHost.send")(function* (
      origin: WorkerOrigin,
      messageId: IdempotencyKey,
      encodedInput: unknown,
      encodedParameters: unknown,
      principal: Principal,
      operation: "start" | "followUp",
    ) {
      if (Option.isNone(deps.deliveries)) return yield* failure(operation, "unavailable");
      const deliveries = deps.deliveries.value;
      const input = yield* decode(PersistedJson, encodedInput, operation);
      const parameters = yield* decode(PersistedJson, encodedParameters, operation);

      const inputDigest = yield* withCrypto(digestJson(input)).pipe(
        Effect.mapError(storageFailure(operation)),
      );

      const key = { ownerThreadId: context.source.threadId, messageId };
      const saved = yield* deliveries.get(key).pipe(Effect.mapError(storageFailure(operation)));

      if (saved !== null) {
        const metadata = saved.envelope.workerAdmission;

        if (
          metadata === undefined ||
          !sameOrigin(metadata.origin, origin) ||
          !sameJson(metadata.parameters, parameters) ||
          saved.envelope.inputDigest !== inputDigest ||
          !sameJson(saved.envelope.input, input)
        )
          return yield* failure(operation, "idempotency-conflict");
        if (saved.receipt !== null) return saved.receipt;
      } else {
        const now = yield* Clock.currentTimeMillis;

        if (now >= origin.expiresAtMillis) return yield* failure(operation, "capacity");

        const envelope: PreparedInput = {
          schemaVersion: 1,
          threadId: origin.worker.threadId,
          deliveryPrincipal: principal,
          agentId: origin.worker.targetAgentId,
          definitions: origin.targetDigests,
          input,
          inputDigest,
          admissionKey: messageId,
          authorization: { policyId: "worker-host", decisionId: messageId },
          workerAdmission: {
            origin,
            messageId,
            parameters,
            createdAtMillis: now,
            deliveryPrincipal: principal,
            ...(sourceSubmissionId === undefined ? {} : { sourceSubmissionId }),
          },
        };

        const prepared = yield* withCrypto(
          prepareMessageDelivery({
            key,
            envelope,
            createdAtMillis: now,
            deadlineAtMillis: origin.expiresAtMillis,
          }),
        ).pipe(Effect.mapError(storageFailure(operation)));

        yield* deliveries
          .insert(prepared)
          .pipe(
            Effect.mapError((error) =>
              failure(
                operation,
                error._tag === "MessageDeliveryError" && error.reason === "capacity"
                  ? "capacity"
                  : "storage",
              ),
            ),
          );
      }

      // Direct callers and automatic pumps run the same claim/accept/refuse protocol. A
      // conclusive refusal is retained before it becomes visible to this caller.
      const processed = yield* MessageDeliveryDriver.pipe(
        Effect.flatMap((driver) => driver.process(key)),
        Effect.provide(MessageDeliveryDriver.layer({ batchSize: 1, concurrency: 1 })),
        Effect.provideService(MessageDeliveryStore, deliveries),
        Effect.provideService(PreparedInputAdmission, {
          submit: (prepared) =>
            deps
              .submit(prepared)
              .pipe(
                Effect.mapError((error) =>
                  error._tag === "AdmissionConflict" || error._tag === "AgentInputError"
                    ? ScheduledInputRefused.make({ code: error._tag })
                    : error._tag === "AdmissionPolicyError" && error.reason === "refused"
                      ? ScheduledInputRefused.make({ code: error.code })
                      : ScheduledInputRetryable.make({ reason: "storage" }),
                ),
              ),
          submissionStatus: (receipt) =>
            deps
              .status(receipt)
              .pipe(Effect.mapError(() => ScheduledInputRetryable.make({ reason: "storage" }))),
        }),
        Effect.provideService(Crypto.Crypto, deps.crypto),
        Effect.mapError(storageFailure(operation)),
      );

      if (processed.receipt !== null) return processed.receipt;
      if (processed.status === "refused") {
        const reason = Schema.decodeUnknownOption(WorkerError.fields.reason)(
          processed.refusal?.startsWith("worker-")
            ? processed.refusal.slice("worker-".length)
            : processed.refusal === "AdmissionConflict"
              ? "idempotency-conflict"
              : processed.refusal === "AgentInputError"
                ? "corrupt"
                : undefined,
        );

        return yield* failure(
          operation,
          Option.getOrElse(reason, () => "denied"),
        );
      }

      // A live claim or a future delivery deadline is not a storage failure. Keep the
      // receipt-only success contract: retention alone does not mean the child started.
      return yield* failure(
        operation,
        processed.status === "pending" && processed.retry.lastFailure === null
          ? "delivery-pending"
          : "storage",
      );
    });

    const messageIdFor = (parts: ReadonlyArray<string>) =>
      withCrypto(digestJson(parts)).pipe(
        Effect.map((digest) => Schema.decodeSync(IdempotencyKey)(`worker:${digest}`)),
        Effect.mapError(storageFailure("start")),
      );

    const summarize = Effect.fn("WorkerHost.summarize")(function* (
      origin: WorkerOrigin,
      all: ReadonlyArray<WorkerInputRequested>,
      operation: "list" | "inspect",
    ) {
      yield* authorize(operation, "read", origin.worker);
      let latestReceipt: Receipt | null = null;
      let active = false;
      let starting = false;

      for (const row of all) {
        if (row.admission.origin.worker.threadId !== origin.worker.threadId) continue;

        const delivery = Option.isNone(deps.deliveries)
          ? null
          : yield* deps.deliveries.value
              .get({
                ownerThreadId: context.source.threadId,
                messageId: row.admission.messageId,
              })
              .pipe(Effect.mapError(storageFailure(operation)));

        const principal = row.admission.deliveryPrincipal ?? delivery?.envelope.deliveryPrincipal;

        const found =
          principal === undefined
            ? Option.none()
            : yield* deps.ledger
                .lookup(
                  SubmissionLookupByKey.make({
                    threadId: origin.worker.threadId,
                    principal,
                    idempotencyKey: row.admission.messageId,
                  }),
                )
                .pipe(Effect.mapError(storageFailure(operation)));

        if (Option.isNone(found)) {
          if (delivery?.status !== "refused") starting = true;
          continue;
        }
        active ||= found.value.state !== "settled";
        if (latestReceipt === null || found.value.queueSequence > latestReceipt.queueSequence) {
          latestReceipt = {
            threadId: found.value.threadId,
            submissionId: found.value.submissionId,
            receiptId: found.value.receiptId,
            queueSequence: found.value.queueSequence,
          };
        }
      }

      return {
        worker: origin.worker,
        latestReceipt,
        state: active
          ? ("active" as const)
          : starting || latestReceipt === null
            ? ("starting" as const)
            : ("idle" as const),
      };
    });

    return {
      context: Effect.gen(function* () {
        yield* authorize("context", "context");
        if (context.depth !== 0) return context;
        const source = yield* sourceAuthority(context.source.threadId, sourceSubmissionId);

        return {
          ...context,
          policy: Option.getOrElse(source.policyOverride, () => context.policy),
        };
      }),
      resolveTargetPolicy: Effect.fn("WorkerHost.resolvePreparedTargetPolicy")(function* (request) {
        yield* authorize("start", "send");

        return (yield* preparedTarget(request.target, request.encodedInput)).policy;
      }),
      start: Effect.fn("WorkerHost.start")(function* (request) {
        const principal = yield* authorize("start", "send");

        if (context.depth !== 0 && sourceSubmissionId === undefined)
          return yield* failure("start", "denied");
        const prepared = yield* preparedTarget(request.target, request.encodedInput);
        const resolved = prepared.resolved;
        const sourcePolicy = Option.getOrElse(prepared.source.policyOverride, () => context.policy);

        const grant = yield* Schema.decodeUnknownEffect(SubagentGrant)(request.encodedGrant).pipe(
          Effect.mapError(() => failure("start", "corrupt")),
        );

        const messageId = yield* messageIdFor([
          context.source.threadId,
          request.delegationId,
          request.idempotencyKey,
        ]);

        if (Option.isNone(deps.deliveries)) return yield* failure("start", "unavailable");

        const existing = yield* deps.deliveries.value
          .get({ ownerThreadId: context.source.threadId, messageId })
          .pipe(Effect.mapError(storageFailure("start")));

        const now = yield* Clock.currentTimeMillis;
        const previousOrigin = existing?.envelope.workerAdmission?.origin;
        const sourceBinding = prepared.source.binding;

        if (sourceBinding === undefined) return yield* failure("start", "declaration-unavailable");

        const reporting =
          previousOrigin === undefined
            ? yield* reportIntent(sourceBinding, resolved, request.delegationId)
            : previousOrigin.reporting;

        if (reporting?.mode === "standard" && sourceSubmissionId === undefined)
          return yield* failure("start", "denied");

        const origin = yield* decode(
          WorkerOrigin,
          {
            worker: {
              schemaVersion: 1,
              delegationId: request.delegationId,
              targetAgentId: request.target.id,
              threadId: Schema.decodeSync(ThreadId)(messageId),
            },
            source: existing?.envelope.workerAdmission?.origin.source ?? context.source,
            targetDigests: resolved.digests,
            policy: request.policy,
            budget: request.budget,
            ...(request.budgetScope === undefined ? {} : { budgetScope: request.budgetScope }),
            grant,
            depth: context.depth + 1,
            firstMessageId: messageId,
            createdAtMillis: existing?.envelope.workerAdmission?.origin.createdAtMillis ?? now,
            expiresAtMillis:
              existing?.envelope.workerAdmission?.origin.expiresAtMillis ??
              now + deps.limits.lifetimeMillis,
            ...(reporting === undefined ? {} : { reporting }),
            ...(request.toolCallAllowance === undefined
              ? {}
              : { toolCallAllowance: request.toolCallAllowance }),
          },
          "start",
        );

        yield* authorizeBudget(origin, principal);

        const targetPolicy = Option.isSome(prepared.policy)
          ? prepared.policy.value
          : origin.budgetScope === "worker-run"
            ? resolved.definition.policy
            : AgentPolicy.resolve(
                resolved.definition.policyOverrides ?? resolved.definition.policy,
                sourcePolicy,
              );

        if (
          !withinPolicy(
            origin,
            origin.budgetScope === "worker-run" ? targetPolicy : sourcePolicy,
            targetPolicy,
          )
        )
          return yield* failure("start", "capacity");

        const receipt = yield* send(
          origin,
          messageId,
          request.encodedInput,
          request.encodedParameters,
          principal,
          "start",
        );

        return { worker: origin.worker, receipt };
      }),
      followUp: Effect.fn("WorkerHost.followUp")(function* (request) {
        const principal = yield* authorize("followUp", "send", request.worker);
        const origin = yield* findOrigin(request.worker, request.target, "followUp");

        yield* resolveTargetPolicy({
          _tag: "RetainedWorker",
          definition: request.target,
          definitions: origin.targetDigests,
          source: origin.source,
          origin,
        });

        yield* Schema.decodeUnknownEffect(Schema.toEncoded(request.target.input))(
          request.encodedInput,
        ).pipe(Effect.mapError(() => failure("followUp", "corrupt")));

        const messageId = yield* messageIdFor([
          context.source.threadId,
          request.worker.threadId,
          "followUp",
          request.idempotencyKey,
        ]);

        return yield* send(
          origin,
          messageId,
          request.encodedInput,
          request.encodedParameters,
          principal,
          "followUp",
        );
      }),
      inspect: Effect.fn("WorkerHost.inspect")(function* (request) {
        yield* authorize("inspect", "read", request.worker);
        const admission = yield* receiptInput(request, "inspect");

        const status = yield* deps
          .status(request.receipt)
          .pipe(Effect.mapError(storageFailure("inspect")));

        return status._tag === "pending"
          ? { _tag: "Pending" as const, receipt: request.receipt }
          : yield* observation(request, admission, status.settlement);
      }),
      summary: Effect.fn("WorkerHost.inspectWorker")(function* (request) {
        yield* authorize("inspect", "read", request.worker);
        const origin = yield* findOrigin(request.worker, request.target, "inspect");
        const source = yield* read(context.source.threadId, "inspect");

        return yield* summarize(origin, requests(source.records), "inspect");
      }),
      observe: (request) =>
        Stream.unwrap(
          Effect.gen(function* () {
            yield* authorize("observe", "read", request.worker);
            yield* findOrigin(request.worker, request.target, "observe");
            const after = request.after ?? 0;

            if (!Number.isSafeInteger(after) || after < 0)
              return yield* failure("observe", "corrupt");

            const tail = yield* deps.store
              .inspectTail(ThreadTailRequest.make({ threadId: request.worker.threadId }))
              .pipe(Effect.mapError(storageFailure("observe")));

            return Stream.paginate(after, (cursor) =>
              Effect.gen(function* () {
                if (cursor >= tail.tailSequence) return [[], Option.none<number>()] as const;
                const limit = Math.min(256, tail.tailSequence - cursor);

                const page = yield* deps.store
                  .read(
                    ThreadRead.make({
                      threadId: request.worker.threadId,
                      afterSequence: Schema.decodeSync(CanonicalSequence)(cursor),
                      limit,
                    }),
                  )
                  .pipe(Stream.runCollect, Effect.mapError(storageFailure("observe")));

                if (
                  page.length === 0 ||
                  page.length > limit ||
                  page.some(
                    (entry, index) =>
                      entry.threadId !== request.worker.threadId ||
                      entry.sequence !== cursor + index + 1,
                  )
                )
                  return yield* failure("observe", "corrupt");

                const entries = yield* Effect.forEach(page, (entry) =>
                  Schema.encodeEffect(RecordEnvelope)(entry.record).pipe(
                    Effect.flatMap((record) =>
                      Schema.decodeUnknownEffect(WorkerHistoryEntry)({
                        sequence: entry.sequence,
                        recordId: entry.record.recordId,
                        record,
                      }),
                    ),
                    Effect.mapError(() => failure("observe", "corrupt")),
                  ),
                );

                const next = cursor + page.length;

                return [
                  entries,
                  next >= tail.tailSequence ? Option.none<number>() : Option.some(next),
                ] as const;
              }),
            );
          }),
        ),
      await: Effect.fn("WorkerHost.await")(function* (request) {
        yield* authorize("await", "read", request.worker);
        const admission = yield* receiptInput(request, "await");

        while (true) {
          const status = yield* deps
            .status(request.receipt)
            .pipe(Effect.mapError(storageFailure("await")));

          if (status._tag === "settled")
            return yield* observation(request, admission, status.settlement);
          // Poll the same host-routed read used by inspect. Interruption stops only this wait.
          yield* Effect.sleep(deps.settlementPollInterval);
        }
      }),
      list: Effect.fn("WorkerHost.list")(function* (request) {
        yield* authorize("list", "read");
        yield* binding(request.target, "list");
        if (!Number.isSafeInteger(request.limit) || request.limit < 1 || request.limit > 100)
          return yield* failure("list", "capacity");
        const source = yield* read(context.source.threadId, "list");
        const all = requests(source.records);

        const origins = [
          ...new Map(
            all.map((row) => [row.admission.origin.worker.threadId, row.admission.origin]),
          ).values(),
        ]
          .filter(
            (origin) =>
              origin.worker.delegationId === request.delegationId &&
              origin.worker.targetAgentId === request.target.id &&
              (request.after === undefined || origin.worker.threadId > request.after),
          )
          .sort((left, right) => left.worker.threadId.localeCompare(right.worker.threadId));

        const selected = origins.slice(0, request.limit);

        const items = yield* Effect.forEach(selected, (origin) => summarize(origin, all, "list"));

        return {
          items,
          next:
            origins.length > selected.length
              ? (selected[selected.length - 1]?.worker.threadId ?? null)
              : null,
        };
      }),
      cancel: Effect.fn("WorkerHost.cancel")(function* (request) {
        const principal = yield* authorize("cancel", "control", request.worker);

        yield* receiptInput(request, "cancel");
        yield* deps
          .abort(
            AbortCommand.make({
              submissionId: request.receipt.submissionId,
              author: principal,
              reason: "Worker owner requested cancellation of this Receipt",
            }),
          )
          .pipe(
            Effect.mapError((error) =>
              error._tag === "JoinedToHost" ? error : failure("cancel", "storage"),
            ),
          );
      }),
    };
  };

  const validateCompletion = Effect.fn("WorkerHost.validateCompletion")(function* (
    unvalidated: FrameworkMessage,
    options: DurableSubmitOptions,
    agentId: AgentId,
    inputDigest: Digest,
  ) {
    const message = yield* Schema.decodeUnknownEffect(FrameworkMessage)(unvalidated).pipe(
      Effect.mapError(() => failure("followUp", "corrupt")),
    );

    const child = yield* read(
      message._tag === "WorkerCompletion"
        ? message.report.worker.threadId
        : message.worker.threadId,
      "followUp",
    );

    const frozen =
      message._tag === "WorkerCompletion"
        ? child.records.flatMap(({ record }) =>
            record.payload._tag === "WorkerReportPrepared" &&
            record.payload.runId === message.report.runId
              ? [record.payload.envelope]
              : [],
          )[0]
        : child.records.flatMap(({ record }) =>
            record.payload._tag === "AgentUpdateEmitted" &&
            Schema.toEquivalence(Update)(record.payload.update, message.update) &&
            record.payload.delivery !== undefined
              ? [record.payload.delivery.envelope]
              : [],
          )[0];

    if (frozen === undefined) return yield* failure("followUp", "denied");

    const envelope = yield* Schema.decodeUnknownEffect(PreparedInput)(frozen).pipe(
      Effect.mapError(() => failure("followUp", "corrupt")),
    );

    if (
      !Schema.is(FrameworkMessage)(envelope.messageAdmission) ||
      !Schema.toEquivalence(FrameworkMessage)(envelope.messageAdmission, message) ||
      envelope.threadId !== options.threadId ||
      envelope.agentId !== agentId ||
      envelope.inputDigest !== inputDigest ||
      envelope.deliveryPrincipal !== options.principal ||
      envelope.admissionKey !== options.idempotencyKey ||
      !definitionDigestsEqual(envelope.definitions, options.definitions) ||
      !Schema.toEquivalence(Schema.UndefinedOr(WorkerAdmission))(
        envelope.workerAdmission,
        options.workerAdmission,
      )
    )
      return yield* failure("followUp", "denied");

    const recorded = child.records.find(
      ({ record }) => record.payload._tag === "WorkerOriginRecorded",
    )?.record.payload;

    if (recorded?._tag !== "WorkerOriginRecorded" || recorded.origin.reporting?.mode !== "standard")
      return yield* failure("followUp", "denied");
    const origin = recorded.origin;
    const source = yield* read(origin.source.threadId, "followUp");

    const first = requests(source.records).find(
      (row) => row.admission.messageId === origin.firstMessageId,
    );

    if (first === undefined || !sameOrigin(first.admission.origin, origin))
      return yield* failure("followUp", "denied");
    const receiving = envelope.workerAdmission;

    const sourceSubmissionId =
      receiving === undefined ? first.admission.sourceSubmissionId : receiving.sourceSubmissionId;

    const principal = yield* deps.authorizer.authorize({
      sourceThreadId: receiving?.origin.source.threadId ?? origin.source.threadId,
      ...(sourceSubmissionId === undefined ? {} : { sourceSubmissionId }),
      principal: options.principal,
      operation: "followUp",
      access: "send",
      worker: receiving?.origin.worker ?? origin.worker,
    });

    if (principal !== options.principal) return yield* failure("followUp", "denied");

    return message;
  });

  const acquire = Effect.fn("WorkerHost.acquire")(function* (request: {
    readonly sourceThreadId: ThreadId;
    readonly principal: Principal;
    readonly sourceSubmissionId?: SubmissionId;
  }) {
    const { sourceThreadId, principal } = request;

    yield* deps.authorizer.authorize({
      sourceThreadId,
      ...(request.sourceSubmissionId === undefined
        ? {}
        : { sourceSubmissionId: request.sourceSubmissionId }),
      principal,
      operation: "context",
      access: "context",
    });
    const current = yield* read(sourceThreadId, "context");
    const created = current.records[0]?.record.payload;

    if (created?._tag !== "ThreadCreated") return yield* failure("context", "not-found");

    const resolved = deps.bindings.find(
      (entry) =>
        entry.agentId === created.agentId &&
        definitionDigestsEqual(entry.digests, created.definitions),
    );

    if (resolved === undefined) return yield* failure("context", "declaration-unavailable");

    const origin = current.records.find(
      ({ record }) => record.payload._tag === "WorkerOriginRecorded",
    )?.record.payload;

    const attached = current.records.find(
      ({ record }) => record.payload._tag === "SubagentLineageRecorded",
    )?.record.payload;

    const retained =
      origin?._tag === "WorkerOriginRecorded"
        ? origin.origin
        : attached?._tag === "SubagentLineageRecorded"
          ? attached
          : undefined;

    const selected =
      request.sourceSubmissionId === undefined
        ? undefined
        : yield* sourceAuthority(sourceThreadId, request.sourceSubmissionId);

    return facet(
      {
        source: {
          _tag: "programmatic",
          threadId: sourceThreadId,
          agentId: selected?.submission?.agentId ?? created.agentId,
        },
        policy:
          retained?.policy ?? selected?.binding?.definition.policy ?? resolved.definition.policy,
        depth:
          origin?._tag === "WorkerOriginRecorded"
            ? origin.origin.depth
            : attached?._tag === "SubagentLineageRecorded"
              ? attached.parentLink.depth
              : 0,
        ...(retained?.grant === undefined ? {} : { grant: retained.grant }),
      },
      principal,
      request.sourceSubmissionId,
    );
  });

  return {
    facet,
    prepareUpdate,
    acquire,
    validateCompletion,
    validateAdmission,
    ensureOrigin,
    completeInput,
    reserveSubtree,
  };
});

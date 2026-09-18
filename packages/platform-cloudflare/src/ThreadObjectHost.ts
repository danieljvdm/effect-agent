import {
  decodePortRequest,
  encodePortResponse,
  LedgerLookupResult,
  PortFailed,
  PortProtocolError,
  PortSucceeded,
  type PortRequest,
  type PortResponse,
} from "@effect-agent/storage-cloudflare/port-protocol";
import { Effect, Layer, Option, Schema, Stream } from "effect";
import {
  IntegrityReport,
  ObligationReport,
  ObligationThresholds,
  RecoveryExplanation,
  RetryCommand,
  RetryRefused,
} from "effect-agent/admin";
import { DigestError } from "effect-agent/digest";
import {
  DurableAgentRuntime,
  DurableRuntimeConfig,
  RecoveryReport,
  type DurableSubmitAgent,
} from "effect-agent/durable-agent-runtime";
import { DurableRuntimeFailpointError } from "effect-agent/durable-failpoint";
import { type AgentId, type ThreadId } from "effect-agent/identifiers";
import { SubmissionId } from "effect-agent/identifiers";
import {
  OperationAuthorizationRequest,
  OperationAuthorizer,
  OperationDenied,
} from "effect-agent/operation-authorizer";
import { PersistedJson } from "effect-agent/records";
import { RunJournalError } from "effect-agent/run-journal";
import {
  AdmissionPolicyError,
  LedgerError,
  OwnershipLost,
  SettlementConflict,
  SubmissionLedger,
  SubmissionLookupByKey,
} from "effect-agent/submission-ledger";
import {
  AppendConflict,
  ThreadNotMaterialized,
  ThreadRead,
  ThreadStore,
  ThreadStoreError,
  FenceRejected,
} from "effect-agent/thread-store";
import { WakeScheduler } from "effect-agent/wake-scheduler";

import {
  ThreadMaintenance,
  DurableAlarmError,
  DurableAlarmService,
  ThreadMutationGate,
  publishCommitted,
  type MaintenancePassFailure,
} from "./Alarm.ts";
import { AdmissionLimitExceeded, CloudflareDurableRuntimeConfig } from "./CloudflareConfig.ts";
import {
  ThreadObjectIdentity,
  ThreadObjectPlacement,
  DurableObjectContext,
} from "./CloudflareHostBindings.ts";
import {
  AbortRecorded,
  ApprovalRecorded,
  HostFailed,
  HostProtocolError,
  ObservedPage,
  ProgressObserved,
  ProgressCancelled,
  SettlementReached,
  SubmissionStatusResponse,
  SubmitSucceeded,
  UnknownResolutionRecorded,
  boundHostDiagnostic,
  decodeAbortCommand,
  decodeAwaitProgressRequest,
  decodeCancelProgressRequest,
  decodeApprovalDecisionCommand,
  decodeObservePageRequest,
  decodeReceipt,
  decodeSubmitRequest,
  decodeUnknownResolutionCommand,
  encodeHostResponse,
  type HostFailure,
  type HostResponse,
  type SubmitRequest,
} from "./CloudflareThreadClientHost.ts";
import {
  layerConfig,
  ThreadObjectPorts,
  type CloudflareDurableRuntimeOptions,
  type CloudflareDurableRuntimeServices,
  type CloudflareBootstrapServices,
} from "./internal/layers.ts";
import { ProgressWaitRegistry } from "./internal/progress-wait.ts";

export {
  layer,
  layerConfig,
  layerHostConfig,
  layerInHost,
  ThreadObjectPorts,
  type ThreadPublicationOptions as PublicationOptions,
  type CloudflareDurableRuntimeOptions as RuntimeOptions,
  type CloudflareDurableRuntimeServices as Services,
  type CloudflareDurableRuntimeInitializationError as InitializationError,
  type CloudflareBootstrapServices as BootstrapServices,
} from "./internal/layers.ts";

/**
 * `ThreadObject.make(application, options)` — the Thread Durable Object
 * (plan §1.4,
 * D-P6-1): a factory returning a class that applications export from their Worker entry.
 * One SQLite-backed Object per Thread is the serialized owner (durability §6); the
 * Object never runs `runResolvedWorker`'s infinite loop — each ingress event or alarm runs
 * ONE bounded `runRecovery` + `processThreadHead` pass, and the persisted alarm
 * (the single multiplexed slot, D-P6-2) finishes accepted work across evictions WITHOUT any
 * incoming request.
 * `Services` exposes the same owner `SqlClient` used by the Thread stores. Compose optional
 * local repositories after `ThreadObject.layer`; never acquire another independently locked
 * SQL client for the same Object. Exposing the client installs no additional storage schemas.
 *
 * Constructor gate (`blockConcurrencyWhile`) is LOCAL-ONLY: schema migration and the
 * exact-version check, configuration decode, and the defensive ensure-alarm half of the
 * alarm invariant. It deliberately does NOT run the recovery pass: parent recovery can
 * require child-Object reads and vice versa, and two Objects blocked in constructor gates
 * awaiting each other's RPC would deadlock (plan §1.4). Instead every pass runs
 * `runRecovery` BEFORE any claim, so reconciliation still strictly precedes new work.
 */

type EndpointServices =
  | CloudflareDurableRuntimeServices
  | CloudflareBootstrapServices
  | DurableObjectContext;

/** Classify only a decoded port request so new protocol members cannot bypass pre-arming. */
const isMutatingPortRequest = (request: PortRequest): boolean => {
  switch (request._tag) {
    case "LedgerAdmit":
    case "LedgerMarkReady":
    case "LedgerRequestAbort":
    case "LedgerRecordChildSettled":
    case "StoreMaterialize":
    case "StoreAppend":
      return true;
    case "LedgerLookup":
    case "LedgerResolveAdmission":
    case "StoreReadPage":
    case "StoreInspectTail":
    case "StoreCountPeerMessages":
    case "StoreExport":
    case "MessageDeliveryList":
      return false;
  }
  request satisfies never;

  return false;
};

/** The literal encoded `PortFailed(PortProtocolError)` fallback (same shape as WP2's). */
const encodedPortProtocolFailure = (message: string): unknown => ({
  _tag: "PortFailed",
  failure: { _tag: "PortProtocolError", message: boundHostDiagnostic(message) },
});

const protocolFailure = (context: string) => (error: { readonly message: string }) =>
  HostProtocolError.make({
    message: boundHostDiagnostic(`${context}: ${error.message}`),
  });

/** Fold one endpoint's typed failures into the uniform `HostResponse` envelope. */
const respond = <Result extends HostResponse, Failure extends HostFailure>(
  effect: Effect.Effect<Result, Failure, EndpointServices>,
): Effect.Effect<HostResponse, never, EndpointServices> =>
  effect.pipe(
    Effect.map((result): HostResponse => result),
    Effect.catch((failure) => Effect.succeed<HostResponse>(HostFailed.make({ failure }))),
  );

/** Encode the response envelope; an unencodable response degrades to a protocol failure. */
const encodeResponse = (response: HostResponse): Effect.Effect<unknown> =>
  encodeHostResponse(response).pipe(
    Effect.catch((error) =>
      Effect.succeed<unknown>({
        _tag: "HostFailed",
        failure: {
          _tag: "HostProtocolError",
          message: boundHostDiagnostic(`The host response could not be encoded: ${error.message}`),
        },
      }),
    ),
  );

const utf8Bytes = (value: PersistedJson): number =>
  new TextEncoder().encode(JSON.stringify(value)).length;

/**
 * The admission-limits gate, BEFORE `runtime.submit` touches the ledger (exit gate
 * "resource limits are checked before admission"; DEPLOY-007). A replayed idempotency key is
 * exempt: its accepted-work obligation already exists, and returning the original Receipt
 * consumes no new quota. Refusals are typed `AdmissionLimitExceeded` and nothing is written.
 */
const gateAdmissionLimits = Effect.fn("ThreadObject.gateAdmissionLimits")(function* (
  threadId: ThreadId,
  request: {
    readonly principal: SubmissionLookupByKey["principal"];
    readonly idempotencyKey: SubmissionLookupByKey["idempotencyKey"];
    readonly inputPayload: PersistedJson;
  },
) {
  const config = yield* CloudflareDurableRuntimeConfig;
  const ledger = yield* SubmissionLedger;
  const { ctx } = yield* DurableObjectContext;

  const existing = yield* ledger.lookup(
    SubmissionLookupByKey.make({
      threadId,
      principal: request.principal,
      idempotencyKey: request.idempotencyKey,
    }),
  );

  if (Option.isSome(existing)) return;

  const inputBytes = utf8Bytes(request.inputPayload);

  if (inputBytes > config.limits.maxInputBytes) {
    return yield* AdmissionLimitExceeded.make({
      limit: "input-bytes",
      actual: inputBytes,
      maximum: config.limits.maxInputBytes,
    });
  }

  const nonterminal = yield* ledger.scanNonterminal.pipe(
    Stream.filter((submission) => submission.threadId === threadId),
    Stream.runCollect,
  );

  if (nonterminal.length >= config.limits.maxQueueDepthPerLane) {
    return yield* AdmissionLimitExceeded.make({
      limit: "queue-depth",
      actual: nonterminal.length,
      maximum: config.limits.maxQueueDepthPerLane,
    });
  }

  const databaseBytes = yield* Effect.sync(() => ctx.storage.sql.databaseSize);

  if (databaseBytes > config.limits.maxDatabaseBytes) {
    return yield* AdmissionLimitExceeded.make({
      limit: "database-bytes",
      actual: databaseBytes,
      maximum: config.limits.maxDatabaseBytes,
    });
  }
});

/**
 * The submit-capable projection of an Agent Binding on the OBJECT side: the input arrived
 * already encoded through the real input schema on the Worker side (`client.ts`), so the
 * Object admits the canonical `PersistedJson` payload as-is; the resolved Binding re-derives
 * everything else from the stored `(agentId, agentDigests)` at claim time (SUB-023).
 */
const passthroughSubmitAgent = (agentId: AgentId): DurableSubmitAgent<typeof PersistedJson> => ({
  definition: {
    id: agentId,
    input: PersistedJson,
  },
});

/** A physical owner may hold other Threads; an addressed request cannot act on their IDs. */
const lookupAddressedSubmission = Effect.fn("ThreadObject.lookupAddressedSubmission")(function* (
  submissionId: SubmissionId,
) {
  const { threadId } = yield* ThreadObjectIdentity;
  const ports = yield* ThreadObjectPorts;
  const submission = yield* ports.lookupSubmission(submissionId);

  if (Option.isSome(submission) && submission.value.threadId !== threadId)
    return yield* HostProtocolError.make({ message: "The Submission belongs to another Thread" });

  return submission;
});

const requireSubmissionThread = Effect.fn("ThreadObject.requireSubmissionThread")(function* (
  submissionId: SubmissionId,
) {
  const submission = yield* lookupAddressedSubmission(submissionId);

  if (Option.isNone(submission))
    return yield* LedgerError.make({
      operation: "addressed Submission lookup",
      message: "The addressed Thread has no such Submission",
    });
});

const requireReceiptThread = Effect.fn("ThreadObject.requireReceiptThread")(function* (
  threadId: ThreadId,
) {
  const identity = yield* ThreadObjectIdentity;

  if (threadId !== identity.threadId)
    return yield* HostProtocolError.make({ message: "The Receipt belongs to another Thread" });
});

const requirePortThread = (request: PortRequest) => {
  switch (request._tag) {
    case "MessageDeliveryList":
      return requireReceiptThread(request.request.ownerThreadId);
    case "LedgerLookup":
      return request.request._tag === "SubmissionLookupById"
        ? lookupAddressedSubmission(request.request.submissionId).pipe(Effect.asVoid)
        : requireReceiptThread(request.request.threadId);
    case "LedgerMarkReady":
    case "LedgerRequestAbort":
      return requireSubmissionThread(request.request.submissionId);
    case "LedgerRecordChildSettled":
      return requireSubmissionThread(request.request.parentSubmissionId);
    case "LedgerAdmit":
    case "LedgerResolveAdmission":
    case "StoreMaterialize":
    case "StoreAppend":
    case "StoreReadPage":
    case "StoreInspectTail":
    case "StoreCountPeerMessages":
    case "StoreExport":
      return requireReceiptThread(request.request.threadId);
  }
  request satisfies never;
};

/**
 * Admit an already Schema-decoded request to a logical Thread in this physical owner.
 * Custom hosts validate local placement before calling this Effect and provide their same
 * runtime/maintenance instances. The native endpoint uses this path too: queue limits,
 * idempotent receipts and the pre-admission generation/alarm commit have one owner.
 */
export const submit = Effect.fn("ThreadObject.submit")(function* (
  threadId: ThreadId,
  request: SubmitRequest,
) {
  const placement = yield* ThreadObjectPlacement;

  if (!placement.ownsThread(threadId))
    return yield* HostProtocolError.make({ message: "The Thread belongs to another Object" });
  const mutations = yield* ThreadMutationGate;
  const runtime = yield* DurableAgentRuntime;

  yield* gateAdmissionLimits(threadId, request);

  return yield* mutations.withMutation(
    runtime
      .submit(passthroughSubmitAgent(request.agentId), request.inputPayload, {
        threadId,
        principal: request.principal,
        idempotencyKey: request.idempotencyKey,
        ...(request.admissionGroup === undefined ? {} : { admissionGroup: request.admissionGroup }),
        ...(request.admissionFence === undefined ? {} : { admissionFence: request.admissionFence }),
        ...(request.workerAdmission === undefined
          ? {}
          : { workerAdmission: request.workerAdmission }),
        ...(request.messageAdmission === undefined
          ? {}
          : { messageAdmission: request.messageAdmission }),
        definitions: request.definitions,
      })
      .pipe(Effect.tap(() => publishCommitted)),
  );
});

const submitEndpoint = (encoded: unknown): Effect.Effect<unknown, never, EndpointServices> =>
  decodeSubmitRequest(encoded).pipe(
    Effect.mapError(protocolFailure("The submit request could not be decoded")),
    Effect.flatMap((request) =>
      Effect.gen(function* () {
        const identity = yield* ThreadObjectIdentity;
        const receipt = yield* submit(identity.threadId, request);

        return SubmitSucceeded.make({ receipt });
      }),
    ),
    respond,
    Effect.flatMap(encodeResponse),
  );

const submissionStatusEndpoint = (
  encoded: unknown,
): Effect.Effect<unknown, never, EndpointServices> =>
  decodeReceipt(encoded).pipe(
    Effect.mapError(protocolFailure("The receipt could not be decoded")),
    Effect.flatMap((receipt) =>
      Effect.gen(function* () {
        const authorizer = yield* OperationAuthorizer;

        yield* authorizer.authorize(
          OperationAuthorizationRequest.make({
            operation: "awaitSettlement",
            threadId: receipt.threadId,
            submissionId: receipt.submissionId,
          }),
        );
        yield* requireReceiptThread(receipt.threadId);
        yield* requireSubmissionThread(receipt.submissionId);
        const runtime = yield* DurableAgentRuntime;

        return SubmissionStatusResponse.make({ status: yield* runtime.submissionStatus(receipt) });
      }),
    ),
    respond,
    Effect.flatMap(encodeResponse),
  );

const awaitSettlementEndpoint = (
  encoded: unknown,
): Effect.Effect<unknown, never, EndpointServices> =>
  decodeReceipt(encoded).pipe(
    Effect.mapError(protocolFailure("The receipt could not be decoded")),
    Effect.flatMap((receipt) =>
      Effect.gen(function* () {
        const authorizer = yield* OperationAuthorizer;

        yield* authorizer.authorize(
          OperationAuthorizationRequest.make({
            operation: "awaitSettlement",
            threadId: receipt.threadId,
            submissionId: receipt.submissionId,
          }),
        );
        yield* requireReceiptThread(receipt.threadId);
        yield* requireSubmissionThread(receipt.submissionId);
        const runtime = yield* DurableAgentRuntime;
        const settlement = yield* runtime.awaitSettlement(receipt);

        return SettlementReached.make({ settlement });
      }),
    ),
    respond,
    Effect.flatMap(encodeResponse),
  );

const awaitProgressEndpoint = (encoded: unknown): Effect.Effect<unknown, never, EndpointServices> =>
  decodeAwaitProgressRequest(encoded).pipe(
    Effect.mapError(protocolFailure("The progress request could not be decoded")),
    Effect.flatMap((request) =>
      Effect.gen(function* () {
        const identity = yield* ThreadObjectIdentity;
        const runtime = yield* DurableAgentRuntime;
        const registry = yield* ProgressWaitRegistry;

        yield* Effect.scoped(
          Effect.gen(function* () {
            const cancelled = yield* registry.subscribe(
              JSON.stringify([identity.threadId, request.waiterId]),
            );

            yield* Effect.raceFirst(
              runtime.awaitProgress(identity.threadId, request.afterSequence),
              cancelled,
            );
          }),
        );

        return ProgressObserved.make();
      }),
    ),
    respond,
    Effect.flatMap(encodeResponse),
  );

const cancelProgressEndpoint = (
  encoded: unknown,
): Effect.Effect<unknown, never, EndpointServices> =>
  decodeCancelProgressRequest(encoded).pipe(
    Effect.mapError(protocolFailure("The progress cancellation could not be decoded")),
    Effect.flatMap((request) =>
      Effect.gen(function* () {
        const identity = yield* ThreadObjectIdentity;
        const registry = yield* ProgressWaitRegistry;

        yield* registry.cancel(JSON.stringify([identity.threadId, request.waiterId]));

        return ProgressCancelled.make();
      }),
    ),
    respond,
    Effect.flatMap(encodeResponse),
  );

const observePageEndpoint = (encoded: unknown): Effect.Effect<unknown, never, EndpointServices> =>
  decodeObservePageRequest(encoded).pipe(
    Effect.mapError(protocolFailure("The observe request could not be decoded")),
    Effect.flatMap((request) =>
      Effect.gen(function* () {
        const identity = yield* ThreadObjectIdentity;
        const store = yield* ThreadStore;
        // The same fail-closed authorization seam the runtime's `observe` consults (P7 WP1);
        // the default reference preserves the possession behavior.
        const authorizer = yield* OperationAuthorizer;

        yield* authorizer.authorize(
          OperationAuthorizationRequest.make({
            operation: "observe",
            threadId: identity.threadId,
          }),
        );

        const records = yield* Stream.runCollect(
          store.read(
            ThreadRead.make({
              threadId: identity.threadId,
              ...(request.afterSequence === undefined
                ? {}
                : { afterSequence: request.afterSequence }),
              limit: request.limit,
            }),
          ),
        );

        return ObservedPage.make({ records: [...records] });
      }),
    ),
    respond,
    Effect.flatMap(encodeResponse),
  );

const abortEndpoint = (encoded: unknown): Effect.Effect<unknown, never, EndpointServices> =>
  decodeAbortCommand(encoded).pipe(
    Effect.mapError(protocolFailure("The abort command could not be decoded")),
    Effect.flatMap((command) =>
      Effect.gen(function* () {
        const authorizer = yield* OperationAuthorizer;

        yield* authorizer.authorize(
          OperationAuthorizationRequest.make({
            operation: "abort",
            submissionId: command.submissionId,
          }),
        );
        yield* requireSubmissionThread(command.submissionId);
        const maintenance = yield* ThreadMaintenance;
        const runtime = yield* DurableAgentRuntime;
        const intent = yield* maintenance.withMutation(runtime.abort(command));

        return AbortRecorded.make({ intent });
      }),
    ),
    respond,
    Effect.flatMap(encodeResponse),
  );

const resolveApprovalEndpoint = (
  encoded: unknown,
): Effect.Effect<unknown, never, EndpointServices> =>
  decodeApprovalDecisionCommand(encoded).pipe(
    Effect.mapError(protocolFailure("The approval command could not be decoded")),
    Effect.flatMap((command) =>
      Effect.gen(function* () {
        const authorizer = yield* OperationAuthorizer;

        yield* authorizer.authorize(
          OperationAuthorizationRequest.make({
            operation: "resolveApproval",
            submissionId: command.submissionId,
          }),
        );
        yield* requireSubmissionThread(command.submissionId);
        const maintenance = yield* ThreadMaintenance;
        const runtime = yield* DurableAgentRuntime;
        const intent = yield* maintenance.withMutation(runtime.resolveApproval(command));

        return ApprovalRecorded.make({ intent });
      }),
    ),
    respond,
    Effect.flatMap(encodeResponse),
  );

const resolveUnknownEndpoint = (
  encoded: unknown,
): Effect.Effect<unknown, never, EndpointServices> =>
  decodeUnknownResolutionCommand(encoded).pipe(
    Effect.mapError(protocolFailure("The resolution command could not be decoded")),
    Effect.flatMap((command) =>
      Effect.gen(function* () {
        const authorizer = yield* OperationAuthorizer;

        yield* authorizer.authorize(
          OperationAuthorizationRequest.make({
            operation: "resolveUnknown",
            submissionId: command.submissionId,
          }),
        );
        yield* requireSubmissionThread(command.submissionId);
        const maintenance = yield* ThreadMaintenance;
        const runtime = yield* DurableAgentRuntime;
        const intent = yield* maintenance.withMutation(runtime.resolveUnknown(command));

        return UnknownResolutionRecorded.make({ intent });
      }),
    ),
    respond,
    Effect.flatMap(encodeResponse),
  );

// ---------------------------------------------------------------------------
// P7 administrative entry points (plan §3): explain/verify/retry/obligations over the SAME
// envelope discipline as the host protocol — closed request/response Schema unions, typed
// failures that re-decode to identical tags, protocol anomalies answered typed. The envelopes
// live here (not `client.ts`) because no Worker-side client consumption exists yet; `wake`
// already exists as the `wake()` entry point.
// ---------------------------------------------------------------------------

/** Explain one Submission (`submissionId` present) or every nonterminal lane member. */
export class AdminExplainRequest extends Schema.Class<AdminExplainRequest>(
  "@effect-agent/platform-cloudflare/AdminExplainRequest",
)({
  submissionId: Schema.optionalKey(SubmissionId),
}) {}

/** Verify carries no parameters — the addressed Object IS the lane. */
export class AdminVerifyRequest extends Schema.Class<AdminVerifyRequest>(
  "@effect-agent/platform-cloudflare/AdminVerifyRequest",
)({}) {}

/** Every typed failure of the four admin entry points, plus the protocol's own errors. */
export const AdminFailure = Schema.Union([
  AdmissionPolicyError,
  OperationDenied,
  RetryRefused,
  LedgerError,
  RunJournalError,
  DigestError,
  OwnershipLost,
  SettlementConflict,
  ThreadStoreError,
  ThreadNotMaterialized,
  AppendConflict,
  FenceRejected,
  DurableRuntimeFailpointError,
  DurableAlarmError,
  HostProtocolError,
]);

export type AdminFailure = typeof AdminFailure.Type;

export class ExplainedRecovery extends Schema.TaggedClass<ExplainedRecovery>(
  "@effect-agent/platform-cloudflare/ExplainedRecovery",
)("ExplainedRecovery", {
  explanations: Schema.Array(RecoveryExplanation).check(Schema.isMaxLength(1_024)),
}) {}

export class VerifiedIntegrity extends Schema.TaggedClass<VerifiedIntegrity>(
  "@effect-agent/platform-cloudflare/VerifiedIntegrity",
)("VerifiedIntegrity", {
  report: IntegrityReport,
}) {}

export class RetryExecuted extends Schema.TaggedClass<RetryExecuted>(
  "@effect-agent/platform-cloudflare/RetryExecuted",
)("RetryExecuted", {
  report: RecoveryReport,
}) {}

export class ObligationsScanned extends Schema.TaggedClass<ObligationsScanned>(
  "@effect-agent/platform-cloudflare/ObligationsScanned",
)("ObligationsScanned", {
  report: ObligationReport,
}) {}

/** The admin entry point failed TYPED on the Object; the failure re-decodes verbatim. */
export class AdminFailed extends Schema.TaggedClass<AdminFailed>(
  "@effect-agent/platform-cloudflare/AdminFailed",
)("AdminFailed", {
  failure: AdminFailure,
}) {}

/** The uniform answer of one admin entry point. Callers narrow by the tag their call implies. */
export const AdminResponse = Schema.Union([
  ExplainedRecovery,
  VerifiedIntegrity,
  RetryExecuted,
  ObligationsScanned,
  AdminFailed,
]);

export type AdminResponse = typeof AdminResponse.Type;

export const decodeAdminExplainRequest = Schema.decodeUnknownEffect(AdminExplainRequest);
export const decodeAdminVerifyRequest = Schema.decodeUnknownEffect(AdminVerifyRequest);
export const decodeRetryCommand = Schema.decodeUnknownEffect(RetryCommand);
export const decodeObligationThresholds = Schema.decodeUnknownEffect(ObligationThresholds);
export const encodeAdminResponse = Schema.encodeEffect(AdminResponse);
export const decodeAdminResponse = Schema.decodeUnknownEffect(AdminResponse);

/** Fold one admin endpoint's typed failures into the uniform `AdminResponse` envelope. */
const respondAdmin = <Result extends AdminResponse, Failure extends AdminFailure>(
  effect: Effect.Effect<Result, Failure, EndpointServices>,
): Effect.Effect<AdminResponse, never, EndpointServices> =>
  effect.pipe(
    Effect.map((result): AdminResponse => result),
    Effect.catch((failure) => Effect.succeed<AdminResponse>(AdminFailed.make({ failure }))),
  );

/** Encode the admin response envelope; an unencodable response degrades to a protocol failure. */
const encodeAdminResponseTotal = (response: AdminResponse): Effect.Effect<unknown> =>
  encodeAdminResponse(response).pipe(
    Effect.catch((error) =>
      Effect.succeed<unknown>({
        _tag: "AdminFailed",
        failure: {
          _tag: "HostProtocolError",
          message: boundHostDiagnostic(`The admin response could not be encoded: ${error.message}`),
        },
      }),
    ),
  );

const explainEndpoint = (encoded: unknown): Effect.Effect<unknown, never, EndpointServices> =>
  decodeAdminExplainRequest(encoded).pipe(
    Effect.mapError(protocolFailure("The explain request could not be decoded")),
    Effect.flatMap((request) =>
      Effect.gen(function* () {
        const identity = yield* ThreadObjectIdentity;
        const runtime = yield* DurableAgentRuntime;

        const explanations =
          request.submissionId === undefined
            ? yield* runtime.explainThread(identity.threadId)
            : [yield* runtime.explain(request.submissionId)];

        return ExplainedRecovery.make({ explanations });
      }),
    ),
    respondAdmin,
    Effect.flatMap(encodeAdminResponseTotal),
  );

const verifyEndpoint = (encoded: unknown): Effect.Effect<unknown, never, EndpointServices> =>
  decodeAdminVerifyRequest(encoded).pipe(
    Effect.mapError(protocolFailure("The verify request could not be decoded")),
    Effect.flatMap(() =>
      Effect.gen(function* () {
        const identity = yield* ThreadObjectIdentity;
        const runtime = yield* DurableAgentRuntime;
        const report = yield* runtime.verify(identity.threadId);

        return VerifiedIntegrity.make({ report });
      }),
    ),
    respondAdmin,
    Effect.flatMap(encodeAdminResponseTotal),
  );

const retryEndpoint = (encoded: unknown): Effect.Effect<unknown, never, EndpointServices> =>
  decodeRetryCommand(encoded).pipe(
    Effect.mapError(protocolFailure("The retry command could not be decoded")),
    Effect.flatMap((command) =>
      Effect.gen(function* () {
        const maintenance = yield* ThreadMaintenance;
        const runtime = yield* DurableAgentRuntime;
        // Retry may repair durable state, so its generation + alarm commit before the mutation.
        const report = yield* maintenance.withMutation(runtime.retry(command));

        return RetryExecuted.make({ report });
      }),
    ),
    respondAdmin,
    Effect.flatMap(encodeAdminResponseTotal),
  );

const obligationsEndpoint = (encoded: unknown): Effect.Effect<unknown, never, EndpointServices> =>
  decodeObligationThresholds(encoded).pipe(
    Effect.mapError(protocolFailure("The obligation thresholds could not be decoded")),
    Effect.flatMap((thresholds) =>
      Effect.gen(function* () {
        const runtime = yield* DurableAgentRuntime;
        const report = yield* runtime.scanObligations(thresholds);

        return ObligationsScanned.make({ report });
      }),
    ),
    respondAdmin,
    Effect.flatMap(encodeAdminResponseTotal),
  );

/**
 * Owner-side `portCall`: wrap a mutating envelope in the same pre-armed generation protocol as
 * public RPC (a routed mutation committed by THIS Object must already carry the alarm that will
 * finish it), execute on the LOCAL facets (never the routed decorators), then arm an immediate
 * alarm so the mutated lane is processed promptly. Protocol anomalies answer
 * `PortFailed(PortProtocolError)`.
 */
const encodePortResponseTotal = (response: PortResponse) =>
  encodePortResponse(response).pipe(
    Effect.catch((error) =>
      Effect.succeed(
        encodedPortProtocolFailure(`The port response could not be encoded: ${error.message}`),
      ),
    ),
  );

const portGuardFailure = (failure: LedgerError | HostProtocolError): PortFailed =>
  PortFailed.make({
    failure:
      failure._tag === "LedgerError"
        ? failure
        : PortProtocolError.make({ message: "The port request is not for the addressed Thread" }),
  });

export const portCall = (
  encoded: unknown,
): Effect.Effect<
  unknown,
  never,
  ThreadObjectPorts | ThreadMaintenance | DurableAlarmService | ThreadObjectIdentity
> =>
  Effect.gen(function* () {
    const ports = yield* ThreadObjectPorts;
    const maintenance = yield* ThreadMaintenance;
    const alarm = yield* DurableAlarmService;

    const decoded = yield* decodePortRequest(encoded).pipe(
      Effect.map((request) => ({ _tag: "success" as const, request })),
      Effect.catch((error) => Effect.succeed({ _tag: "failure" as const, message: error.message })),
    );

    if (decoded._tag === "failure") {
      return encodedPortProtocolFailure(
        `The port request could not be decoded: ${decoded.message}`,
      );
    }
    if (
      decoded.request._tag === "LedgerLookup" &&
      decoded.request.request._tag === "SubmissionLookupById"
    ) {
      const response = yield* lookupAddressedSubmission(decoded.request.request.submissionId).pipe(
        Effect.map((submission) =>
          PortSucceeded.make({
            result: LedgerLookupResult.make(
              Option.isSome(submission) ? { submission: submission.value } : {},
            ),
          }),
        ),
        Effect.catch((failure) => Effect.succeed(portGuardFailure(failure))),
      );

      return yield* encodePortResponseTotal(response);
    }
    const identityCheck = yield* requirePortThread(decoded.request).pipe(Effect.result);

    if (identityCheck._tag === "Failure")
      return yield* encodePortResponseTotal(portGuardFailure(identityCheck.failure));
    const mutating = isMutatingPortRequest(decoded.request);

    const handled = yield* (
      mutating
        ? maintenance.withMutation(ports.handle(decoded.request))
        : ports.handle(decoded.request)
    ).pipe(Effect.exit);

    if (handled._tag === "Failure") {
      // Without the committed generation/alarm the invariant cannot be promised; refuse before
      // the port mutation runs. `ports.handle` itself is total, so this is the maintenance error.
      return encodedPortProtocolFailure(
        "The owner Object could not arm its maintenance alarm before the mutation.",
      );
    }

    const response = yield* encodePortResponseTotal(handled.value);

    if (mutating) {
      // Prompt processing hint; the pre-armed alarm already guarantees convergence.
      yield* alarm.scheduleNow.pipe(
        Effect.catch((error) =>
          Effect.logWarning("ThreadObject.portCall: immediate re-arm failed", error),
        ),
      );
    }

    return response;
  });

const wakeEndpoint: Effect.Effect<void, never, EndpointServices> = Effect.gen(function* () {
  const identity = yield* ThreadObjectIdentity;
  const wake = yield* WakeScheduler;

  // Route the remote hint through this incarnation's scheduler so scoped progress waiters and
  // the alarm receive the same hint. Delivery remains droppable; canonical storage is authority.
  yield* wake.notify(identity.threadId);
});

/** The per-Thread wire operations supported by native and application-owned endpoints. */
export const ThreadRpcOperation = Schema.Literals([
  "submitEncoded",
  "submissionStatusEncoded",
  "awaitSettlementEncoded",
  "awaitProgressEncoded",
  "cancelProgressEncoded",
  "observePage",
  "abortEncoded",
  "resolveApprovalEncoded",
  "resolveUnknownEncoded",
  "portCall",
  "wake",
]);

export type ThreadRpcOperation = typeof ThreadRpcOperation.Type;

export const rpc = {
  submitEncoded: submitEndpoint,
  submissionStatusEncoded: submissionStatusEndpoint,
  awaitSettlementEncoded: awaitSettlementEndpoint,
  awaitProgressEncoded: awaitProgressEndpoint,
  cancelProgressEncoded: cancelProgressEndpoint,
  observePage: observePageEndpoint,
  abortEncoded: abortEndpoint,
  resolveApprovalEncoded: resolveApprovalEndpoint,
  resolveUnknownEncoded: resolveUnknownEndpoint,
  portCall,
  wake: () => wakeEndpoint,
} satisfies Record<
  ThreadRpcOperation,
  (encoded: unknown) => Effect.Effect<unknown, never, EndpointServices>
>;

/**
 * Bind an addressed request to its logical Thread while sharing one physical runtime. This
 * validates local placement before invoking the same native handlers. Receipts, Submission
 * commands and port envelopes must match this identity; progress cancellation is Thread-scoped.
 * The producer comes from the actual runtime configuration, never from caller input. These
 * guards supplement the existing current model/Tool and operation authorization policies.
 */
export const handleRpc = Effect.fn("ThreadObject.handleRpc")(function* (
  threadId: ThreadId,
  operation: ThreadRpcOperation,
  encoded: unknown,
) {
  const placement = yield* ThreadObjectPlacement;

  if (!placement.ownsThread(threadId))
    return yield* HostProtocolError.make({ message: "The Thread belongs to another Object" });
  const { producerId } = yield* DurableRuntimeConfig;

  return yield* rpc[operation](encoded).pipe(
    Effect.provideService(ThreadObjectIdentity, { threadId, producerId }),
  );
});

export const alarm: Effect.Effect<void, MaintenancePassFailure, EndpointServices> = Effect.gen(
  function* () {
    const maintenance = yield* ThreadMaintenance;

    // Typed pass failures propagate: the rejected promise makes workerd retry the alarm
    // (at-least-once delivery), and the dirty generation retains a committed slot meanwhile.
    yield* maintenance.pass;
  },
);

export const initialize: Effect.Effect<void, MaintenancePassFailure, EndpointServices> = Effect.gen(
  function* () {
    // Forcing ThreadMaintenance forces the whole Layer stack: migration + exact-version
    // check + configuration decode (DEPLOY-008 fails typed here, before any mutation), then
    // the defensive local ensure-alarm half of the invariant. LOCAL-ONLY by construction.
    const maintenance = yield* ThreadMaintenance;

    yield* maintenance.ensureAlarm;
  },
);

/** Compose the shared runtime; the native host owns construction gating and event scopes. */
export const makeRuntime = <A, E, R>(
  application: Layer.Layer<CloudflareDurableRuntimeServices | A, E, R>,
  options: CloudflareDurableRuntimeOptions,
) => application.pipe(Layer.provideMerge(layerConfig(options)));

/** Administrative operations share the runtime but keep their independent wire schemas. */
export const administrativeRpc = {
  explainEncoded: explainEndpoint,
  verifyEncoded: verifyEndpoint,
  retryEncoded: retryEndpoint,
  obligationsEncoded: obligationsEndpoint,
};

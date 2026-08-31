import type { AgentId } from "@effect-agent/core";
import { SubmissionId } from "@effect-agent/core";
import {
  AppendConflict,
  ConversationNotMaterialized,
  ConversationRead,
  ConversationStore,
  ConversationStoreError,
  DigestError,
  DurableAgentRuntime,
  DurableRuntimeFailpointError,
  FenceRejected,
  IntegrityReport,
  LedgerError,
  ObligationReport,
  ObligationThresholds,
  OperationAuthorizationRequest,
  OperationAuthorizer,
  OperationDenied,
  OwnershipLost,
  PersistedJson,
  RecoveryExplanation,
  RecoveryReport,
  RetryCommand,
  RetryRefused,
  RunJournalError,
  SettlementConflict,
  SubmissionLedger,
  SubmissionLookupByKey,
  WakeScheduler,
  type DurableSubmitAgent,
} from "@effect-agent/session";
import {
  decodePortRequest,
  encodePortResponse,
  type PortRequest,
} from "@effect-agent/storage-cloudflare";
import { Effect, Layer, Option, Schema, Stream } from "effect";
import {
  DurableObject as EffectCfDurableObject,
  DurableObjectState as EffectCfDurableObjectState,
  WorkerEnvironment,
} from "effect-cf";

import {
  ConversationMaintenance,
  DurableAlarmError,
  DurableAlarmService,
  type MaintenancePassFailure,
} from "./alarm.ts";
import {
  ConversationObjectIdentity,
  DurableObjectContext,
  ConversationObjectNamespace,
  conversationNamespaceFromEnv,
  type CloudflareBindingError,
} from "./bindings.ts";
import {
  AbortRecorded,
  ApprovalRecorded,
  HostFailed,
  HostProtocolError,
  ObservedPage,
  ProgressObserved,
  ProgressCancelled,
  SettlementReached,
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
} from "./client.ts";
import { AdmissionLimitExceeded, CloudflareDurableRuntimeConfig } from "./config.ts";
import {
  layerConfig,
  ConversationObjectPorts,
  type CloudflareDurableRuntimeInitializationError,
  type CloudflareDurableRuntimeOptions,
  type CloudflareDurableRuntimeServices,
  type CloudflareBootstrapServices,
} from "./layers.ts";
import { ProgressWaitRegistry } from "./progress-wait.ts";

export {
  layer,
  layerConfig,
  type CloudflareDurableRuntimeOptions as RuntimeOptions,
  type CloudflareDurableRuntimeServices as Services,
  type CloudflareDurableRuntimeInitializationError as InitializationError,
  type CloudflareBootstrapServices as BootstrapServices,
} from "./layers.ts";

/**
 * `ConversationObject.make(application, options)` — the Conversation Durable Object
 * (plan §1.4,
 * D-P6-1): a factory returning a class that applications export from their Worker entry.
 * One SQLite-backed Object per Conversation is the serialized owner (durability §6); the
 * Object never runs `runResolvedWorker`'s infinite loop — each ingress event or alarm runs
 * ONE bounded `runRecovery` + `processConversationResolved` pass, and the persisted alarm
 * (the single multiplexed slot, D-P6-2) finishes accepted work across evictions WITHOUT any
 * incoming request.
 *
 * Constructor gate (`blockConcurrencyWhile`) is LOCAL-ONLY: schema migration and the
 * exact-version check, configuration decode, and the defensive ensure-alarm half of the
 * alarm invariant. It deliberately does NOT run the recovery pass: parent recovery can
 * require child-Object reads and vice versa, and two Objects blocked in constructor gates
 * awaiting each other's RPC would deadlock (plan §1.4). Instead every pass runs
 * `runRecovery` BEFORE any claim, so reconciliation still strictly precedes new work.
 */

/** Construction options for one deployed Conversation Object class. */
export interface Options<
  ApplicationServices = never,
  EventServices = never,
  EventLayerError = never,
> extends CloudflareDurableRuntimeOptions {
  /** Accept transient native RPC tracing through effect-cf; disabled by default. */
  readonly rpcTracing?: boolean;
  /**
   * Name of the Worker `env` binding carrying THIS class's `DurableObjectNamespace` — the
   * Object's route back to sibling Conversation Objects for the WP2 cross-Object port calls
   * and remote wakes (DEPLOY-010: the binding enters through a Layer, never ambiently).
   */
  readonly namespaceBinding: string;
  /** Acquired and finalized per native event, with access to the complete application runtime. */
  readonly eventLayer?: Layer.Layer<
    EventServices,
    EventLayerError,
    | RuntimeServices
    | ApplicationServices
    | EffectCfDurableObjectState.DurableObjectState
    | WorkerEnvironment
  >;
}

type EndpointServices =
  | CloudflareDurableRuntimeServices
  | CloudflareBootstrapServices
  | DurableObjectContext;
type RuntimeServices = EndpointServices | ConversationObjectNamespace;
type ConversationObjectInitializationError =
  | CloudflareDurableRuntimeInitializationError
  | CloudflareBindingError
  | MaintenancePassFailure;

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
    case "StoreExport":
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
const gateAdmissionLimits = Effect.fn("ConversationObject.gateAdmissionLimits")(
  function* (request: {
    readonly principal: SubmissionLookupByKey["principal"];
    readonly idempotencyKey: SubmissionLookupByKey["idempotencyKey"];
    readonly inputPayload: PersistedJson;
  }) {
    const identity = yield* ConversationObjectIdentity;
    const config = yield* CloudflareDurableRuntimeConfig;
    const ledger = yield* SubmissionLedger;
    const { ctx } = yield* DurableObjectContext;

    const existing = yield* ledger.lookup(
      SubmissionLookupByKey.make({
        conversationId: identity.conversationId,
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

    // One Conversation per Object (durability §5): the local scan IS this lane's queue.
    const nonterminal = yield* Stream.runCollect(ledger.scanNonterminal);
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
  },
);

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

const submitEndpoint = (encoded: unknown): Effect.Effect<unknown, never, EndpointServices> =>
  decodeSubmitRequest(encoded).pipe(
    Effect.mapError(protocolFailure("The submit request could not be decoded")),
    Effect.flatMap((request) =>
      Effect.gen(function* () {
        const identity = yield* ConversationObjectIdentity;
        const maintenance = yield* ConversationMaintenance;
        const runtime = yield* DurableAgentRuntime;
        yield* gateAdmissionLimits(request);
        // Alarm invariant: the generation + alarm commit BEFORE the admission, and maintenance
        // cannot acknowledge that generation until this mutation leaves its public RPC seam.
        const receipt = yield* maintenance.withMutation(
          runtime.submit(passthroughSubmitAgent(request.agentId), request.inputPayload, {
            conversationId: identity.conversationId,
            principal: request.principal,
            idempotencyKey: request.idempotencyKey,
            definitions: request.definitions,
          }),
        );
        return SubmitSucceeded.make({ receipt });
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
        const identity = yield* ConversationObjectIdentity;
        const runtime = yield* DurableAgentRuntime;
        const registry = yield* ProgressWaitRegistry;
        yield* Effect.scoped(
          Effect.gen(function* () {
            const cancelled = yield* registry.subscribe(request.waiterId);
            yield* Effect.raceFirst(
              runtime.awaitProgress(identity.conversationId, request.afterSequence),
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
        const registry = yield* ProgressWaitRegistry;
        yield* registry.cancel(request.waiterId);
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
        const identity = yield* ConversationObjectIdentity;
        const store = yield* ConversationStore;
        // The same fail-closed authorization seam the runtime's `observe` consults (P7 WP1);
        // the default reference preserves the possession behavior.
        const authorizer = yield* OperationAuthorizer;
        yield* authorizer.authorize(
          OperationAuthorizationRequest.make({
            operation: "observe",
            conversationId: identity.conversationId,
          }),
        );
        const records = yield* Stream.runCollect(
          store.read(
            ConversationRead.make({
              conversationId: identity.conversationId,
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
        const maintenance = yield* ConversationMaintenance;
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
        const maintenance = yield* ConversationMaintenance;
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
        const maintenance = yield* ConversationMaintenance;
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
  OperationDenied,
  RetryRefused,
  LedgerError,
  RunJournalError,
  DigestError,
  OwnershipLost,
  SettlementConflict,
  ConversationStoreError,
  ConversationNotMaterialized,
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
        const identity = yield* ConversationObjectIdentity;
        const runtime = yield* DurableAgentRuntime;
        const explanations =
          request.submissionId === undefined
            ? yield* runtime.explainConversation(identity.conversationId)
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
        const identity = yield* ConversationObjectIdentity;
        const runtime = yield* DurableAgentRuntime;
        const report = yield* runtime.verify(identity.conversationId);
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
        const maintenance = yield* ConversationMaintenance;
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
const portCallEndpoint = (encoded: unknown): Effect.Effect<unknown, never, EndpointServices> =>
  Effect.gen(function* () {
    const ports = yield* ConversationObjectPorts;
    const maintenance = yield* ConversationMaintenance;
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
    const response = yield* encodePortResponse(handled.value).pipe(
      Effect.catch((error) =>
        Effect.succeed(
          encodedPortProtocolFailure(`The port response could not be encoded: ${error.message}`),
        ),
      ),
    );
    if (mutating) {
      // Prompt processing hint; the pre-armed alarm already guarantees convergence.
      yield* alarm.scheduleNow.pipe(
        Effect.catch((error) =>
          Effect.logWarning("ConversationObject.portCall: immediate re-arm failed", error),
        ),
      );
    }
    return response;
  });

const wakeEndpoint: Effect.Effect<void, never, EndpointServices> = Effect.gen(function* () {
  const identity = yield* ConversationObjectIdentity;
  const wake = yield* WakeScheduler;
  // Route the remote hint through this incarnation's scheduler so scoped progress waiters and
  // the alarm receive the same hint. Delivery remains droppable; canonical storage is authority.
  yield* wake.notify(identity.conversationId);
});

const alarmEndpoint: Effect.Effect<void, MaintenancePassFailure, EndpointServices> = Effect.gen(
  function* () {
    const maintenance = yield* ConversationMaintenance;
    // Typed pass failures propagate: the rejected promise makes workerd retry the alarm
    // (at-least-once delivery), and the dirty generation retains a committed slot meanwhile.
    yield* maintenance.pass;
  },
);

const gateEndpoint: Effect.Effect<void, MaintenancePassFailure, EndpointServices> = Effect.gen(
  function* () {
    // Forcing ConversationMaintenance forces the whole Layer stack: migration + exact-version
    // check + configuration decode (DEPLOY-008 fails typed here, before any mutation), then
    // the defensive local ensure-alarm half of the invariant. LOCAL-ONLY by construction.
    const maintenance = yield* ConversationMaintenance;
    yield* maintenance.ensureAlarm;
  },
);

/**
 * Adapter from effect-cf's native Durable Object services to Effect Agent's existing platform
 * ports. effect-cf owns the cached ManagedRuntime and supplies these values once per Object
 * incarnation; the durable runtime continues to depend only on the narrow services below.
 */
const effectCfPlatformLayer = (
  namespaceBinding: string,
  rpcTracing = false,
): Layer.Layer<
  DurableObjectContext | ConversationObjectNamespace,
  CloudflareBindingError,
  EffectCfDurableObjectState.DurableObjectState | WorkerEnvironment
> => {
  const context = Layer.effect(DurableObjectContext)(
    Effect.gen(function* () {
      const state = yield* EffectCfDurableObjectState.DurableObjectState;
      const env = yield* WorkerEnvironment;
      return DurableObjectContext.of({ ctx: state.raw, env });
    }),
  );
  const namespace = Layer.effect(ConversationObjectNamespace)(
    Effect.gen(function* () {
      const env = yield* WorkerEnvironment;
      const binding = yield* conversationNamespaceFromEnv(env, namespaceBinding);
      return ConversationObjectNamespace.of({
        namespace: binding,
        ...(rpcTracing === true ? { rpcTracing: namespaceBinding } : {}),
      });
    }),
  );
  return Layer.merge(context, namespace);
};

/** The public endpoints and effect-cf invocation hook of one Conversation Object instance. */
export interface Instance<EventServices = never> extends InstanceType<
  EffectCfDurableObject.DurableObjectClass<Record<never, never>, RuntimeServices | EventServices>
> {
  submitEncoded(encoded: unknown, traceContext?: unknown): Promise<unknown>;
  awaitSettlementEncoded(encoded: unknown, traceContext?: unknown): Promise<unknown>;
  awaitProgressEncoded(encoded: unknown, traceContext?: unknown): Promise<unknown>;
  cancelProgressEncoded(encoded: unknown, traceContext?: unknown): Promise<unknown>;
  observePage(encoded: unknown, traceContext?: unknown): Promise<unknown>;
  abortEncoded(encoded: unknown, traceContext?: unknown): Promise<unknown>;
  resolveApprovalEncoded(encoded: unknown, traceContext?: unknown): Promise<unknown>;
  resolveUnknownEncoded(encoded: unknown, traceContext?: unknown): Promise<unknown>;
  explainEncoded(encoded: unknown): Promise<unknown>;
  verifyEncoded(encoded: unknown): Promise<unknown>;
  retryEncoded(encoded: unknown): Promise<unknown>;
  obligationsEncoded(encoded: unknown): Promise<unknown>;
  portCall(encoded: unknown): Promise<unknown>;
  wake(): Promise<void>;
  alarm(alarmInfo?: AlarmInvocationInfo): Promise<void> | void;
}

/** The constructor shape workerd instantiates for each Conversation Object. */
export interface Class<EventServices = never> {
  new (ctx: DurableObjectState, env: Cloudflare.Env): Instance<EventServices>;
}

/**
 * Export a composed application Layer as a native Durable Object class.
 * Bootstrap services are provided to the whole graph before it acquires, so application Layers
 * can yield effect-cf's WorkerEnvironment and DurableObjectState, derived identity, and Crypto.
 * Application dependencies remain visible until Layer.provide satisfies them. effect-cf owns the
 * cached ManagedRuntime, native RPC methods, event scopes, and telemetry flushing.
 * Initialization is local and bounded inside the constructor gate. Cloudflare eviction does not
 * guarantee finalizers; put resources requiring timely release in scoped operations or eventLayer.
 */
export const make = <
  ApplicationServices,
  ApplicationError,
  EventServices = never,
  EventLayerError = never,
>(
  applicationLayer: Layer.Layer<
    CloudflareDurableRuntimeServices | ApplicationServices,
    ApplicationError,
    | CloudflareBootstrapServices
    | EffectCfDurableObjectState.DurableObjectState
    | WorkerEnvironment
    | DurableObjectContext
    | ConversationObjectNamespace
  >,
  options: Options<ApplicationServices, EventServices, EventLayerError>,
): Class<ApplicationServices | EventServices> => {
  const application = applicationLayer.pipe(
    Layer.provideMerge(layerConfig(options)),
    Layer.provideMerge(effectCfPlatformLayer(options.namespaceBinding, options.rpcTracing)),
  );

  // The storage/config Layer must acquire inside Cloudflare's constructor gate. effect-cf owns
  // the ManagedRuntime, while this effectContext ensures its first Layer build enters the gate
  // before migration, compatibility checks, or alarm inspection touch Object storage.
  const runtime: Layer.Layer<
    RuntimeServices | ApplicationServices,
    ConversationObjectInitializationError | ApplicationError,
    EffectCfDurableObjectState.DurableObjectState | WorkerEnvironment
  > = Layer.effectContext(
    Effect.gen(function* () {
      const state = yield* EffectCfDurableObjectState.DurableObjectState;
      const scope = yield* Effect.scope;
      return yield* state.blockConcurrencyWhile(
        Effect.gen(function* () {
          const services = yield* Layer.buildWithScope(application, scope);
          yield* gateEndpoint.pipe(Effect.provide(services));
          return services;
        }),
      );
    }),
  );

  const rpc = {
    submitEncoded: (encoded: unknown) => submitEndpoint(encoded),
    awaitSettlementEncoded: (encoded: unknown) => awaitSettlementEndpoint(encoded),
    awaitProgressEncoded: (encoded: unknown) => awaitProgressEndpoint(encoded),
    cancelProgressEncoded: (encoded: unknown) => cancelProgressEndpoint(encoded),
    observePage: (encoded: unknown) => observePageEndpoint(encoded),
    abortEncoded: (encoded: unknown) => abortEndpoint(encoded),
    resolveApprovalEncoded: (encoded: unknown) => resolveApprovalEndpoint(encoded),
    resolveUnknownEncoded: (encoded: unknown) => resolveUnknownEndpoint(encoded),
    explainEncoded: (encoded: unknown) => explainEndpoint(encoded),
    verifyEncoded: (encoded: unknown) => verifyEndpoint(encoded),
    retryEncoded: (encoded: unknown) => retryEndpoint(encoded),
    obligationsEncoded: (encoded: unknown) => obligationsEndpoint(encoded),
    portCall: (encoded: unknown) => portCallEndpoint(encoded),
    wake: () => wakeEndpoint,
  } satisfies EffectCfDurableObject.DurableObjectRpc<
    RuntimeServices | ApplicationServices | EventServices
  >;

  const EffectCfConversationObject = EffectCfDurableObject.make<
    RuntimeServices | ApplicationServices,
    ConversationObjectInitializationError | ApplicationError,
    EventServices,
    EventLayerError,
    typeof rpc
  >(runtime, {
    ...(options.rpcTracing === true ? { rpcTracing: { service: options.namespaceBinding } } : {}),
    ...(options.eventLayer === undefined ? {} : { eventLayer: options.eventLayer }),
    // Force the gated runtime Layer when Cloudflare loads this Object incarnation. Recovery stays
    // in each bounded pass so cross-Object initialization cannot deadlock.
    initialize: Effect.void,
    rpc,
    alarm: () => alarmEndpoint,
  });

  // effect-cf's class type keeps `alarm` optional even when the handler option is present. This
  // concrete override reflects this factory's stronger contract while delegating execution to
  // the effect-cf runtime unchanged.
  class ConversationObject extends EffectCfConversationObject {
    override alarm(alarmInfo?: AlarmInvocationInfo): Promise<void> | void {
      return super.alarm?.(alarmInfo);
    }
  }

  return ConversationObject;
};

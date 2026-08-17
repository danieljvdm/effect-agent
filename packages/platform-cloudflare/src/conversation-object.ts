import type { AgentId } from "@effect-agent/core";
import { SubmissionId } from "@effect-agent/core";
import {
  AppendConflict,
  type ChildAdmissionAuthorizer,
  ChildAdmissionDenied,
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
  OperationCaller,
  OperationDenied,
  OperationMutationPreparationError,
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
  type DurableSubmitAgent,
} from "@effect-agent/session";
import { decodePortRequestMutation } from "@effect-agent/storage-cloudflare";
import type { DurableObject as CloudflareDurableObject } from "cloudflare:workers";
import { Effect, Layer, Option, Schema, Stream } from "effect";
import {
  DurableObject as EffectCfDurableObject,
  DurableObjectState as EffectCfDurableObjectState,
  WorkerEnvironment,
} from "effect-cf";

import {
  ConversationMaintenance,
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
  SettlementReached,
  SubmitSucceeded,
  UnknownResolutionRecorded,
  boundHostDiagnostic,
  decodeAbortHostRequest,
  decodeApprovalHostRequest,
  decodeAwaitSettlementRequest,
  decodeObservePageRequest,
  decodeSubmitRequest,
  decodeUnknownResolutionHostRequest,
  encodeHostResponse,
  type HostFailure,
  type HostResponse,
} from "./client.ts";
import { AdmissionLimitExceeded, CloudflareDurableRuntimeConfig } from "./config.ts";
import {
  CloudflareDurableRuntime,
  ConversationObjectPorts,
  type CloudflareDurableRuntimeInitializationError,
  type CloudflareDurableRuntimeOptions,
  type CloudflareDurableRuntimeServices,
} from "./layers.ts";

/**
 * `makeConversationObjectClass(options, authorization, observability?)` — the Conversation Durable Object
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
export interface ConversationObjectOptions extends CloudflareDurableRuntimeOptions {
  /**
   * Name of the Worker `env` binding carrying THIS class's `DurableObjectNamespace` — the
   * Object's route back to sibling Conversation Objects for the WP2 cross-Object port calls
   * and remote wakes (DEPLOY-010: the binding enters through a Layer, never ambiently).
   */
  readonly namespaceBinding: string;
}

type EndpointServices =
  | CloudflareDurableRuntimeServices
  | DurableObjectContext
  | OperationAuthorizer;
type RuntimeServices = EndpointServices | ConversationObjectNamespace | ChildAdmissionAuthorizer;
type ConversationObjectInitializationError =
  | CloudflareDurableRuntimeInitializationError
  | CloudflareBindingError
  | MaintenancePassFailure;

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

const utf8Bytes = (value: unknown): number =>
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
        // Alarm invariant: the alarm commits BEFORE the admission it will finish (D-P6-2).
        yield* maintenance.preArm;
        const receipt = yield* runtime.submit(
          passthroughSubmitAgent(request.agentId),
          request.inputPayload,
          {
            conversationId: identity.conversationId,
            principal: request.principal,
            idempotencyKey: request.idempotencyKey,
            definitions: request.definitions,
          },
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
  decodeAwaitSettlementRequest(encoded).pipe(
    Effect.mapError(protocolFailure("The receipt could not be decoded")),
    Effect.flatMap((request) =>
      Effect.gen(function* () {
        const runtime = yield* DurableAgentRuntime;
        const settlement = yield* runtime.awaitSettlement(request.receipt, request.caller);
        return SettlementReached.make({ settlement });
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
        // The same required fail-closed authorization seam the runtime's `observe` consults.
        // The platform composition root must choose and provide the policy explicitly.
        const authorizer = yield* OperationAuthorizer;
        const authorization = OperationAuthorizationRequest.make({
          operation: "observe",
          principal: request.caller.principal,
          conversationId: identity.conversationId,
        });
        yield* authorizer.authorize(authorization);
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
        yield* Effect.forEach(records, () => authorizer.authorize(authorization), {
          discard: true,
        });
        return ObservedPage.make({ records: [...records] });
      }),
    ),
    respond,
    Effect.flatMap(encodeResponse),
  );

const abortEndpoint = (encoded: unknown): Effect.Effect<unknown, never, EndpointServices> =>
  decodeAbortHostRequest(encoded).pipe(
    Effect.mapError(protocolFailure("The abort command could not be decoded")),
    Effect.flatMap((request) =>
      Effect.gen(function* () {
        const runtime = yield* DurableAgentRuntime;
        const intent = yield* runtime.abort(request.command, request.caller);
        return AbortRecorded.make({ intent });
      }),
    ),
    respond,
    Effect.flatMap(encodeResponse),
  );

const resolveApprovalEndpoint = (
  encoded: unknown,
): Effect.Effect<unknown, never, EndpointServices> =>
  decodeApprovalHostRequest(encoded).pipe(
    Effect.mapError(protocolFailure("The approval command could not be decoded")),
    Effect.flatMap((request) =>
      Effect.gen(function* () {
        const runtime = yield* DurableAgentRuntime;
        const intent = yield* runtime.resolveApproval(request.command, request.caller);
        return ApprovalRecorded.make({ intent });
      }),
    ),
    respond,
    Effect.flatMap(encodeResponse),
  );

const resolveUnknownEndpoint = (
  encoded: unknown,
): Effect.Effect<unknown, never, EndpointServices> =>
  decodeUnknownResolutionHostRequest(encoded).pipe(
    Effect.mapError(protocolFailure("The resolution command could not be decoded")),
    Effect.flatMap((request) =>
      Effect.gen(function* () {
        const runtime = yield* DurableAgentRuntime;
        const intent = yield* runtime.resolveUnknown(request.command, request.caller);
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
  caller: OperationCaller,
}) {}

/** Verify carries no parameters — the addressed Object IS the lane. */
export class AdminVerifyRequest extends Schema.Class<AdminVerifyRequest>(
  "@effect-agent/platform-cloudflare/AdminVerifyRequest",
)({ caller: OperationCaller }) {}

export class AdminRetryRequest extends Schema.Class<AdminRetryRequest>(
  "@effect-agent/platform-cloudflare/AdminRetryRequest",
)({ command: RetryCommand, caller: OperationCaller }) {}

export class AdminObligationsRequest extends Schema.Class<AdminObligationsRequest>(
  "@effect-agent/platform-cloudflare/AdminObligationsRequest",
)({ thresholds: ObligationThresholds, caller: OperationCaller }) {}

/** Every typed failure of the four admin entry points, plus the protocol's own errors. */
export const AdminFailure = Schema.Union([
  OperationDenied,
  OperationMutationPreparationError,
  ChildAdmissionDenied,
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
export const decodeAdminRetryRequest = Schema.decodeUnknownEffect(AdminRetryRequest);
export const decodeAdminObligationsRequest = Schema.decodeUnknownEffect(AdminObligationsRequest);
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
            ? yield* runtime.explainConversation(identity.conversationId, request.caller)
            : [yield* runtime.explain(request.submissionId, request.caller)];
        return ExplainedRecovery.make({ explanations });
      }),
    ),
    respondAdmin,
    Effect.flatMap(encodeAdminResponseTotal),
  );

const verifyEndpoint = (encoded: unknown): Effect.Effect<unknown, never, EndpointServices> =>
  decodeAdminVerifyRequest(encoded).pipe(
    Effect.mapError(protocolFailure("The verify request could not be decoded")),
    Effect.flatMap((request) =>
      Effect.gen(function* () {
        const identity = yield* ConversationObjectIdentity;
        const runtime = yield* DurableAgentRuntime;
        const report = yield* runtime.verify(identity.conversationId, request.caller);
        return VerifiedIntegrity.make({ report });
      }),
    ),
    respondAdmin,
    Effect.flatMap(encodeAdminResponseTotal),
  );

const retryEndpoint = (encoded: unknown): Effect.Effect<unknown, never, EndpointServices> =>
  decodeAdminRetryRequest(encoded).pipe(
    Effect.mapError(protocolFailure("The retry command could not be decoded")),
    Effect.flatMap((request) =>
      Effect.gen(function* () {
        const runtime = yield* DurableAgentRuntime;
        const report = yield* runtime.retry(request.command, request.caller);
        return RetryExecuted.make({ report });
      }),
    ),
    respondAdmin,
    Effect.flatMap(encodeAdminResponseTotal),
  );

const obligationsEndpoint = (encoded: unknown): Effect.Effect<unknown, never, EndpointServices> =>
  decodeAdminObligationsRequest(encoded).pipe(
    Effect.mapError(protocolFailure("The obligation thresholds could not be decoded")),
    Effect.flatMap((request) =>
      Effect.gen(function* () {
        const runtime = yield* DurableAgentRuntime;
        const report = yield* runtime.scanObligations(request.thresholds, request.caller);
        return ObligationsScanned.make({ report });
      }),
    ),
    respondAdmin,
    Effect.flatMap(encodeAdminResponseTotal),
  );

/**
 * Owner-side `portCall`: pre-arm before a mutating envelope (a routed admission committed by
 * THIS Object must already carry the alarm that will finish it), execute on the LOCAL facets
 * (never the routed decorators), then arm an immediate alarm so the mutated lane is
 * processed promptly. Protocol anomalies answer `PortFailed(PortProtocolError)`.
 */
const portCallEndpoint = (encoded: unknown): Effect.Effect<unknown, never, EndpointServices> =>
  Effect.gen(function* () {
    const ports = yield* ConversationObjectPorts;
    const maintenance = yield* ConversationMaintenance;
    const alarm = yield* DurableAlarmService;
    // Mutation classification belongs to the storage protocol's closed union.
    // A malformed envelope cannot execute a mutation, so let the canonical
    // handler answer its protocol failure without touching the alarm slot.
    const mutating = yield* decodePortRequestMutation(encoded).pipe(
      Effect.orElseSucceed(() => false),
    );
    if (mutating) {
      const preArmed = yield* maintenance.preArm.pipe(Effect.exit);
      if (preArmed._tag === "Failure") {
        // Without the committed alarm the invariant cannot be promised; refuse the mutation.
        return encodedPortProtocolFailure(
          "The owner Object could not arm its maintenance alarm before the mutation.",
        );
      }
    }
    const response = yield* ports.handle(encoded);
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
  const alarm = yield* DurableAlarmService;
  // Wake hints are droppable by contract: a failed alarm write is logged and swallowed; the
  // sender's own alarm/scan pairing (or this Object's next entry point) restores liveness.
  yield* alarm.scheduleNow.pipe(
    Effect.catch((error) => Effect.logWarning("ConversationObject.wake dropped", error)),
  );
});

const alarmEndpoint: Effect.Effect<void, MaintenancePassFailure, EndpointServices> = Effect.gen(
  function* () {
    const maintenance = yield* ConversationMaintenance;
    // Typed pass failures propagate: the rejected promise makes workerd retry the alarm
    // (at-least-once delivery), and the pass's own pre-arm keeps the slot committed meanwhile.
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
      return ConversationObjectNamespace.of({ namespace: binding });
    }),
  );
  return Layer.merge(context, namespace);
};

/** The public endpoint surface of one Conversation Object instance. */
export interface ConversationObjectInstance extends CloudflareDurableObject {
  submitEncoded(encoded: unknown): Promise<unknown>;
  awaitSettlementEncoded(encoded: unknown): Promise<unknown>;
  observePage(encoded: unknown): Promise<unknown>;
  abortEncoded(encoded: unknown): Promise<unknown>;
  resolveApprovalEncoded(encoded: unknown): Promise<unknown>;
  resolveUnknownEncoded(encoded: unknown): Promise<unknown>;
  explainEncoded(encoded: unknown): Promise<unknown>;
  verifyEncoded(encoded: unknown): Promise<unknown>;
  retryEncoded(encoded: unknown): Promise<unknown>;
  obligationsEncoded(encoded: unknown): Promise<unknown>;
  portCall(encoded: unknown): Promise<unknown>;
  wake(): Promise<void>;
  alarm(alarmInfo?: AlarmInvocationInfo): Promise<void> | void;
}

/** The constructor shape workerd instantiates for each Conversation Object. */
export interface ConversationObjectClass {
  new (ctx: DurableObjectState, env: Cloudflare.Env): ConversationObjectInstance;
}

/**
 * Build the application's Conversation Object class (export it from the Worker entry).
 * effect-cf owns the cached ManagedRuntime, native RPC methods, event scopes, and post-handler
 * OTLP flush scheduling for RPC and alarm events. The optional outer Layer is built per native
 * event, so a host can install Tracer/Logger/Metric services and `OtlpExporter.Flusher` without
 * Effect Agent owning exporter lifecycle machinery. The required authorization Layer makes both
 * session policy ports an explicit application composition decision.
 */
export type ConversationObjectAuthorizationLayer = Layer.Layer<
  OperationAuthorizer | ChildAdmissionAuthorizer
>;

export const makeConversationObjectClass = <EventLayerError = never, EventServices = never>(
  options: ConversationObjectOptions,
  authorization: ConversationObjectAuthorizationLayer,
  observability?: Layer.Layer<
    EventServices,
    EventLayerError,
    | DurableObjectContext
    | ConversationObjectNamespace
    | EffectCfDurableObjectState.DurableObjectState
    | WorkerEnvironment
  >,
): ConversationObjectClass => {
  const application: Layer.Layer<
    RuntimeServices,
    CloudflareDurableRuntimeInitializationError | CloudflareBindingError,
    EffectCfDurableObjectState.DurableObjectState | WorkerEnvironment
  > = CloudflareDurableRuntime.layer(options).pipe(
    Layer.provideMerge(effectCfPlatformLayer(options.namespaceBinding)),
    Layer.provideMerge(authorization),
  );

  // The storage/config Layer must acquire inside Cloudflare's constructor gate. effect-cf owns
  // the ManagedRuntime, while this effectContext ensures its first Layer build enters the gate
  // before migration, compatibility checks, or alarm inspection touch Object storage.
  const runtime: Layer.Layer<
    RuntimeServices,
    ConversationObjectInitializationError,
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
  } satisfies EffectCfDurableObject.DurableObjectRpc<RuntimeServices | EventServices>;

  const EffectCfConversationObject = EffectCfDurableObject.make<
    RuntimeServices,
    ConversationObjectInitializationError,
    EventServices,
    EventLayerError,
    typeof rpc
  >(runtime, {
    ...(observability === undefined ? {} : { eventLayer: observability }),
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

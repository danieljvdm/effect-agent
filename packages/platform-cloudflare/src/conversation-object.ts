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
  type DurableSubmitAgent,
} from "@effect-agent/session";
import { DurableObject } from "cloudflare:workers";
import { Effect, Layer, ManagedRuntime, Option, Schema, Stream } from "effect";

import { ConversationMaintenance, DurableAlarmError, DurableAlarmService } from "./alarm.ts";
import {
  ConversationObjectIdentity,
  DurableObjectContext,
  conversationNamespaceLayer,
  type CloudflareBindingError,
  type ConversationObjectNamespace,
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
  decodeAbortCommand,
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
  CloudflareDurableRuntime,
  ConversationObjectPorts,
  type CloudflareDurableRuntimeInitializationError,
  type CloudflareDurableRuntimeOptions,
  type CloudflareDurableRuntimeServices,
} from "./layers.ts";
import {
  flushCloudflareRuntimeTelemetry,
  makeCloudflareTelemetryFlushCoordinator,
  registerCloudflareTelemetryAfterNativeSettlement,
  withCloudflareNativeSpanFailure,
  type CloudflareTelemetryFlushCoordinator,
} from "./telemetry-internal.ts";
import { CloudflareRuntimeTelemetry } from "./telemetry.ts";

/**
 * `makeConversationObjectClass(options, telemetry?)` — the Conversation Durable Object (plan §1.4,
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

type ConversationObjectError<TelemetryError> =
  | CloudflareDurableRuntimeInitializationError
  | CloudflareBindingError
  | TelemetryError;

type EndpointServices = CloudflareDurableRuntimeServices | DurableObjectContext;

/** Port envelope tags whose owner-side execution durably mutates this Object's lane. */
const MUTATING_PORT_TAGS: ReadonlySet<string> = new Set([
  "LedgerAdmit",
  "LedgerMarkReady",
  "LedgerRequestAbort",
  "LedgerRecordChildSettled",
  "StoreMaterialize",
  "StoreAppend",
]);

const isMutatingPortRequest = (encoded: unknown): boolean =>
  typeof encoded === "object" &&
  encoded !== null &&
  "_tag" in encoded &&
  typeof encoded._tag === "string" &&
  MUTATING_PORT_TAGS.has(encoded._tag);

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
        yield* authorizer
          .authorize(
            OperationAuthorizationRequest.make({
              operation: "observe",
              conversationId: identity.conversationId,
            }),
          )
          .pipe(Effect.catchTag("OperationDenied", deniedToProtocolFailure));
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
        yield* maintenance.preArm;
        const intent = yield* runtime.abort(command);
        return AbortRecorded.make({ intent });
      }),
    ),
    respond,
    Effect.flatMap(encodeResponse),
  );

/**
 * The pre-P7 host protocol's failure union does not carry `OperationDenied` (the Worker client
 * predates the authorizer). This assembly always runs the default possession authorizer — no
 * `CloudflareDurableRuntimeOptions` authorizer lever exists yet — so a denial here is
 * unreachable today; if one ever surfaces it degrades to the protocol failure instead of an
 * out-of-contract throw. The four P7 admin entry points below carry `OperationDenied` typed.
 */
const deniedToProtocolFailure = (
  denied: OperationDenied,
): Effect.Effect<never, HostProtocolError> =>
  Effect.fail(
    HostProtocolError.make({
      message: boundHostDiagnostic(
        `The ${denied.operation} operation was denied: ${denied.reason}`,
      ),
    }),
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
        yield* maintenance.preArm;
        const intent = yield* runtime
          .resolveApproval(command)
          .pipe(Effect.catchTag("OperationDenied", deniedToProtocolFailure));
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
        yield* maintenance.preArm;
        const intent = yield* runtime
          .resolveUnknown(command)
          .pipe(Effect.catchTag("OperationDenied", deniedToProtocolFailure));
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
        // Alarm invariant: retry may repair durable state, so the alarm that will finish the
        // lane commits BEFORE the mutation (D-P6-2), exactly like abort.
        yield* maintenance.preArm;
        const report = yield* runtime.retry(command);
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
    const mutating = isMutatingPortRequest(encoded);
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

const alarmEndpoint: Effect.Effect<void, unknown, EndpointServices> = Effect.gen(function* () {
  const maintenance = yield* ConversationMaintenance;
  // Typed pass failures propagate: the rejected promise makes workerd retry the alarm
  // (at-least-once delivery), and the pass's own pre-arm keeps the slot committed meanwhile.
  yield* maintenance.pass;
});

const gateEndpoint: Effect.Effect<void, unknown, EndpointServices> = Effect.gen(function* () {
  // Forcing ConversationMaintenance forces the whole Layer stack: migration + exact-version
  // check + configuration decode (DEPLOY-008 fails typed here, before any mutation), then
  // the defensive local ensure-alarm half of the invariant. LOCAL-ONLY by construction.
  const maintenance = yield* ConversationMaintenance;
  yield* maintenance.ensureAlarm;
});

const NATIVE_ENTRYPOINTS = [
  "submit",
  "await_settlement",
  "observe",
  "abort",
  "resolve_approval",
  "resolve_unknown",
  "explain",
  "verify",
  "retry",
  "obligations",
  "port_call",
  "wake",
  "alarm",
] as const;

type NativeEntrypoint = (typeof NATIVE_ENTRYPOINTS)[number];

/**
 * Owner-side delivery measurement. The exact endpoint Cause is hidden behind a bounded typed
 * marker while the span closes, then restored together with any newly composed real reasons.
 */
const observeNativeEntrypoint = <A, E>(
  entrypoint: NativeEntrypoint,
  effect: Effect.Effect<A, E, EndpointServices>,
): Effect.Effect<A, E, EndpointServices> =>
  Effect.gen(function* () {
    const identity = yield* ConversationObjectIdentity;
    const config = yield* CloudflareDurableRuntimeConfig;
    return yield* withCloudflareNativeSpanFailure(effect, (masked) =>
      masked.pipe(
        Effect.withSpan(`effect_agent.cloudflare.conversation_object.${entrypoint}`, {
          kind: "server",
          attributes: {
            "effect_agent.cloudflare.entrypoint": entrypoint,
            "effect_agent.deployment.id": config.deploymentId,
            conversationId: identity.conversationId,
            producerId: identity.producerId,
          },
        }),
      ),
    );
  });

const flushNativeEntrypointTelemetry = Effect.flatMap(CloudflareDurableRuntimeConfig, (config) =>
  flushCloudflareRuntimeTelemetry(config.telemetryFlushTimeout),
);

/** The public endpoint surface of one Conversation Object instance. */
export interface ConversationObjectInstance extends DurableObject {
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
  alarm(): Promise<void>;
}

/** The constructor shape workerd instantiates for each Conversation Object. */
export interface ConversationObjectClass {
  new (ctx: DurableObjectState, env: Cloudflare.Env): ConversationObjectInstance;
}

/**
 * Build the application's Conversation Object class (export it from the Worker entry).
 * `telemetry` is the host composition boundary: it may require the Object context/namespace and
 * retain a typed acquisition error, while any additional output services remain available to the
 * cached runtime. `TelemetryError` stays the first explicit generic for source compatibility;
 * additional outputs infer from the Layer. Omitting it installs the content-free no-op service.
 * The explicit return type is what makes declaration emit possible: the class body carries a
 * private runtime field, and TS4094 rejects inferring an exported anonymous class type around it.
 */
export const makeConversationObjectClass = <
  TelemetryError = never,
  AdditionalTelemetryOutputs = never,
>(
  options: ConversationObjectOptions,
  telemetry?: Layer.Layer<
    CloudflareRuntimeTelemetry | AdditionalTelemetryOutputs,
    TelemetryError,
    DurableObjectContext | ConversationObjectNamespace
  >,
): ConversationObjectClass => {
  class ConversationObject extends DurableObject {
    readonly #runtime: ManagedRuntime.ManagedRuntime<
      EndpointServices,
      ConversationObjectError<TelemetryError>
    >;
    readonly #ctx: DurableObjectState;
    readonly #telemetryFlush: CloudflareTelemetryFlushCoordinator;

    constructor(ctx: DurableObjectState, env: Cloudflare.Env) {
      super(ctx, env);
      this.#ctx = ctx;
      const platform = Layer.mergeAll(
        DurableObjectContext.layer(ctx, env),
        conversationNamespaceLayer(env, options.namespaceBinding),
      );
      // Host observability is composed at the Worker edge, not hidden in runtime options. Its
      // typed acquisition failure remains in the ManagedRuntime error, while the Object context
      // and namespace are available to Layers that derive exporter configuration from `env`.
      const hostTelemetry = (telemetry ?? CloudflareRuntimeTelemetry.layerNoop).pipe(
        Layer.provideMerge(platform),
      );
      this.#runtime = ManagedRuntime.make(
        CloudflareDurableRuntime.layer(options).pipe(
          // Supplying the host Layer outermost makes its Logger/Tracer/Metric runtime
          // configuration observe application Layer acquisition and every native endpoint.
          Layer.provideMerge(hostTelemetry),
        ),
      );
      this.#telemetryFlush = makeCloudflareTelemetryFlushCoordinator(
        () => this.#runtime.runPromise(flushNativeEntrypointTelemetry),
        {
          onReservationDropped: () => {
            void this.#runtime.runSyncExit(
              Effect.logWarning(
                "Cloudflare telemetry delivery coalesced at the batch reservation limit",
              ).pipe(
                Effect.annotateLogs({
                  "effect_agent.cloudflare.telemetry.failure_kind": "reservation_limit",
                }),
              ),
            );
          },
        },
      );
      // The constructor gate: local-only checks; never the recovery pass (deadlock argument,
      // plan §1.4). A failure here fails every delivery with the typed construction error.
      ctx.blockConcurrencyWhile(() => this.#runtime.runPromise(gateEndpoint));
    }

    #runNative<A, E>(
      entrypoint: NativeEntrypoint,
      effect: Effect.Effect<A, E, EndpointServices>,
    ): Promise<A> {
      const delivery = this.#runtime.runPromise(observeNativeEntrypoint(entrypoint, effect));
      // Reserve synchronously with the current native delivery, but do not await export from the
      // RPC/alarm Promise. Each pending/trailing/queued batch waits for its assigned deliveries to
      // settle and only its first owner registers the shared background Promise. Even an
      // uninterruptible exporter therefore cannot delay a caller, suppress alarm retry, accumulate
      // concurrent exporter work, an unbounded waitUntil registration queue, or unbounded retained
      // delivery Promises. Excess arrivals are diagnosed and lossy-coalesced at the batch cap.
      // Registration failure and every asynchronous flush failure remain isolated from delivery.
      return registerCloudflareTelemetryAfterNativeSettlement(
        (background) => this.#ctx.waitUntil(background),
        delivery,
        this.#telemetryFlush.reserve,
        () => {
          // Native entrypoints run only after blockConcurrencyWhile has built the cached runtime.
          // Effect Logger invocation is synchronous; runSyncExit captures a broken logger without
          // starting an unscoped fiber or changing the delivery Promise. The caught platform Cause
          // is intentionally unavailable here: only a framework-owned classification is logged.
          void this.#runtime.runSyncExit(
            Effect.logError("Cloudflare telemetry waitUntil registration failed").pipe(
              Effect.annotateLogs({
                "effect_agent.cloudflare.telemetry.failure_kind": "wait_until_registration",
              }),
            ),
          );
        },
      );
    }

    async submitEncoded(encoded: unknown): Promise<unknown> {
      return this.#runNative("submit", submitEndpoint(encoded));
    }

    async awaitSettlementEncoded(encoded: unknown): Promise<unknown> {
      return this.#runNative("await_settlement", awaitSettlementEndpoint(encoded));
    }

    async observePage(encoded: unknown): Promise<unknown> {
      return this.#runNative("observe", observePageEndpoint(encoded));
    }

    async abortEncoded(encoded: unknown): Promise<unknown> {
      return this.#runNative("abort", abortEndpoint(encoded));
    }

    async resolveApprovalEncoded(encoded: unknown): Promise<unknown> {
      return this.#runNative("resolve_approval", resolveApprovalEndpoint(encoded));
    }

    async resolveUnknownEncoded(encoded: unknown): Promise<unknown> {
      return this.#runNative("resolve_unknown", resolveUnknownEndpoint(encoded));
    }

    async explainEncoded(encoded: unknown): Promise<unknown> {
      return this.#runNative("explain", explainEndpoint(encoded));
    }

    async verifyEncoded(encoded: unknown): Promise<unknown> {
      return this.#runNative("verify", verifyEndpoint(encoded));
    }

    async retryEncoded(encoded: unknown): Promise<unknown> {
      return this.#runNative("retry", retryEndpoint(encoded));
    }

    async obligationsEncoded(encoded: unknown): Promise<unknown> {
      return this.#runNative("obligations", obligationsEndpoint(encoded));
    }

    async portCall(encoded: unknown): Promise<unknown> {
      return this.#runNative("port_call", portCallEndpoint(encoded));
    }

    async wake(): Promise<void> {
      await this.#runNative("wake", wakeEndpoint);
    }

    override async alarm(): Promise<void> {
      await this.#runNative("alarm", alarmEndpoint);
    }
  }

  return ConversationObject;
};

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
  type ConversationObjectRpc,
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

/**
 * `makeConversationObjectClass(options)` — the Conversation Durable Object (plan §1.4,
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

/**
 * Public instance surface of the class returned by `makeConversationObjectClass`. It includes
 * the Worker/sibling RPC contract, the administrative RPC entry points, the platform alarm
 * handler, and the inherited Object context used by application subclasses.
 */
export interface ConversationObjectInstance
  extends DurableObject<Cloudflare.Env>, ConversationObjectRpc {
  explainEncoded(encoded: unknown): Promise<unknown>;
  verifyEncoded(encoded: unknown): Promise<unknown>;
  retryEncoded(encoded: unknown): Promise<unknown>;
  obligationsEncoded(encoded: unknown): Promise<unknown>;
  alarm(): Promise<void>;
}

/** Public constructor returned by `makeConversationObjectClass`. */
export interface ConversationObjectConstructor {
  new (ctx: DurableObjectState, env: Cloudflare.Env): ConversationObjectInstance;
}

type ConversationObjectError = CloudflareDurableRuntimeInitializationError | CloudflareBindingError;

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

/** Build the application's Conversation Object class (export it from the Worker entry). */
export const makeConversationObjectClass = (
  options: ConversationObjectOptions,
): ConversationObjectConstructor => {
  class ConversationObject extends DurableObject {
    readonly #runtime: ManagedRuntime.ManagedRuntime<EndpointServices, ConversationObjectError>;

    constructor(ctx: DurableObjectState, env: Cloudflare.Env) {
      super(ctx, env);
      this.#runtime = ManagedRuntime.make(
        CloudflareDurableRuntime.layer(options).pipe(
          Layer.provideMerge(
            Layer.mergeAll(
              DurableObjectContext.layer(ctx, env),
              conversationNamespaceLayer(env, options.namespaceBinding),
            ),
          ),
        ),
      );
      // The constructor gate: local-only checks; never the recovery pass (deadlock argument,
      // plan §1.4). A failure here fails every delivery with the typed construction error.
      void ctx.blockConcurrencyWhile(() => this.#runtime.runPromise(gateEndpoint));
    }

    async submitEncoded(encoded: unknown): Promise<unknown> {
      return this.#runtime.runPromise(submitEndpoint(encoded));
    }

    async awaitSettlementEncoded(encoded: unknown): Promise<unknown> {
      return this.#runtime.runPromise(awaitSettlementEndpoint(encoded));
    }

    async observePage(encoded: unknown): Promise<unknown> {
      return this.#runtime.runPromise(observePageEndpoint(encoded));
    }

    async abortEncoded(encoded: unknown): Promise<unknown> {
      return this.#runtime.runPromise(abortEndpoint(encoded));
    }

    async resolveApprovalEncoded(encoded: unknown): Promise<unknown> {
      return this.#runtime.runPromise(resolveApprovalEndpoint(encoded));
    }

    async resolveUnknownEncoded(encoded: unknown): Promise<unknown> {
      return this.#runtime.runPromise(resolveUnknownEndpoint(encoded));
    }

    async explainEncoded(encoded: unknown): Promise<unknown> {
      return this.#runtime.runPromise(explainEndpoint(encoded));
    }

    async verifyEncoded(encoded: unknown): Promise<unknown> {
      return this.#runtime.runPromise(verifyEndpoint(encoded));
    }

    async retryEncoded(encoded: unknown): Promise<unknown> {
      return this.#runtime.runPromise(retryEndpoint(encoded));
    }

    async obligationsEncoded(encoded: unknown): Promise<unknown> {
      return this.#runtime.runPromise(obligationsEndpoint(encoded));
    }

    async portCall(encoded: unknown): Promise<unknown> {
      return this.#runtime.runPromise(portCallEndpoint(encoded));
    }

    async wake(): Promise<void> {
      await this.#runtime.runPromise(wakeEndpoint);
    }

    override async alarm(): Promise<void> {
      await this.#runtime.runPromise(alarmEndpoint);
    }
  }

  return ConversationObject;
};

import {
  AdmissionIndeterminate,
  AdmissionConflict,
  ChildAttachmentSnapshot,
  JoinedToHost,
  LedgerError,
  RecoverySnapshot,
  SettlementConflict,
  SubmissionLedger,
  SubmissionLookupById,
  type SubmissionLookupByKey,
  type SubmissionSnapshot,
} from "@effect-agent/thread/SubmissionLedger";
import {
  AppendConflict,
  ThreadMaterialization,
  ThreadNotMaterialized,
  ThreadStore,
  ThreadStoreError,
  FenceRejected,
} from "@effect-agent/thread/ThreadStore";
import { Context, Effect, Layer, Option, Predicate, Schema, Stream } from "effect";

import {
  boundPortDiagnostic,
  decodePortRequest,
  decodePortResponse,
  encodePortRequest,
  encodePortResponse,
  LedgerAdmitCall,
  LedgerAdmitResult,
  LedgerLookupCall,
  LedgerLookupResult,
  LedgerMarkReadyCall,
  LedgerMarkReadyResult,
  LedgerRecordChildSettledCall,
  LedgerRecordChildSettledResult,
  LedgerRequestAbortCall,
  LedgerRequestAbortResult,
  LedgerResolveAdmissionCall,
  LedgerResolveAdmissionResult,
  PortFailed,
  PortProtocolError,
  PortSucceeded,
  StoreAppendCall,
  StoreAppendResult,
  StoreExportCall,
  StoreExportResult,
  StoreInspectTailCall,
  StoreInspectTailResult,
  StoreMaterializeCall,
  StoreMaterializeResult,
  StoreReadPageCall,
  StoreReadPageResult,
  type PortFailure,
  type PortRequest,
  type PortRequestEnvelope,
  type PortResponse,
  type PortResult,
} from "./PortProtocol.ts";

type ThreadId = ThreadMaterialization["threadId"];
type SubmissionId = SubmissionSnapshot["submissionId"];

const ThreadIdSchema = ThreadMaterialization.fields.threadId;
const decodeThreadId = Schema.decodeUnknownEffect(ThreadIdSchema);

/**
 * The ledger row bound routable Submission identities must respect (mirrors the local
 * facet's `MAX_IDENTIFIER_LENGTH`; the minting side already refuses longer identities typed
 * at admission, so a longer identity presented here cannot name any stored row).
 */
const MAX_ROUTABLE_SUBMISSION_ID_LENGTH = 1_024;

/** The `{uuidv7}` head of a DC-minted routable Submission identity. */
const UUID_HEAD_PATTERN =
  /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/;

/**
 * A transport could not deliver a port request to (or an answer from) the owning
 * Thread's Durable Object. `retryable` carries the platform's own stub signal when one
 * exists. This error never crosses the wire — it is the CALLER-side evidence that the
 * authority was unreachable, which is exactly the case `AdmissionIndeterminate` was
 * specified for (SUB-031).
 */
export class PortTransportError extends Schema.TaggedError<PortTransportError>()(
  "PortTransportError",
  {
    target: Schema.String,
    message: Schema.String,
    retryable: Schema.optionalKey(Schema.Boolean),
    cause: Schema.optionalKey(Schema.Defect()),
  },
) {}

const safeTransportDiagnostic = (cause: unknown): string => {
  try {
    const message = cause instanceof Error ? cause.message : cause;

    return boundPortDiagnostic(typeof message === "string" ? message : String(message));
  } catch {
    return "[unavailable transport diagnostic]";
  }
};

const transportRetryableSignal = (cause: unknown): boolean | undefined => {
  if (!Predicate.isObjectKeyword(cause)) return undefined;
  try {
    const signal = Reflect.get(cause, "retryable");

    return typeof signal === "boolean" ? signal : undefined;
  } catch {
    return undefined;
  }
};

/**
 * Build a `PortTransportError` from an arbitrary thrown transport cause, preserving the
 * platform stub's own `retryable` signal when present.
 */
export const portTransportFailure = (target: string, cause: unknown): PortTransportError => {
  const retryable = transportRetryableSignal(cause);

  return PortTransportError.make({
    target,
    message: safeTransportDiagnostic(cause),
    ...(retryable === undefined ? {} : { retryable }),
    cause,
  });
};

/**
 * Delivery of Schema-encoded port envelopes to the Durable Object that owns a FOREIGN
 * Thread (plan §1.3, D-P6-3). The shipped implementation (platform-cloudflare, WP3)
 * calls the owner's `portCall` over native Durable Object JS RPC via
 * `namespace.idFromName(threadId)`; the protocol is transport-agnostic and any carrier
 * that moves the encoded envelopes verbatim satisfies this service. Implementations MUST
 * surface every delivery problem as `PortTransportError` and must never fabricate an answer.
 */
export class ThreadPortTransport extends Context.Service<
  ThreadPortTransport,
  {
    readonly call: (
      threadId: ThreadId,
      request: PortRequestEnvelope,
    ) => Effect.Effect<unknown, PortTransportError>;
  }
>()("@effect-agent/storage-cloudflare/ThreadPortTransport") {}

/** Construction options shared by both routed port Layers. */
export interface RoutedPortOptions {
  /**
   * The Thread this Durable Object owns (the Object identity rule is
   * `namespace.idFromName(threadId)`). Requests addressed here execute on the local
   * facet; requests addressed anywhere else route through the transport or fail fast typed.
   */
  readonly localThreadId: ThreadId;
}

/** Where one port request must execute. */
type RouteTarget =
  | { readonly _tag: "local" }
  | { readonly _tag: "foreign"; readonly threadId: ThreadId };

const LOCAL: RouteTarget = { _tag: "local" };

/**
 * Parse a DC-minted routable Submission identity — `{uuidv7}:{threadId}`, split at the
 * FIRST `:` because the Thread tail may itself contain colons (D-P6-5). This adapter
 * minted the format at admission and is the ONLY component that parses it; identities that do
 * not carry the minted shape (no separator, non-UUID head, empty tail) fall back to the local
 * facet, which is the only authority this Object can consult without inventing an owner.
 * Identities beyond the ledger's 1,024-character row bound fail typed: the minting side
 * refused them at admission, so they cannot name any stored row anywhere.
 */
const routableSubmissionTarget = (
  localThreadId: ThreadId,
): ((operation: string, submissionId: string) => Effect.Effect<RouteTarget, LedgerError>) =>
  Effect.fn("DoPortRouting.routableSubmissionTarget")(function* (
    operation: string,
    submissionId: string,
  ): Effect.fn.Return<RouteTarget, LedgerError> {
    if (submissionId.length > MAX_ROUTABLE_SUBMISSION_ID_LENGTH) {
      return yield* LedgerError.make({
        operation,
        message:
          `A Submission identity of ${submissionId.length} characters exceeds the ` +
          `${MAX_ROUTABLE_SUBMISSION_ID_LENGTH}-character routable identity bound; admission ` +
          "refuses such identities, so it cannot name any stored row.",
      });
    }
    const separator = submissionId.indexOf(":");

    if (separator === -1) return LOCAL;
    if (!UUID_HEAD_PATTERN.test(submissionId.slice(0, separator))) return LOCAL;
    const tail = submissionId.slice(separator + 1);

    if (tail === localThreadId) return LOCAL;

    return yield* decodeThreadId(tail).pipe(
      Effect.map((threadId): RouteTarget => ({ _tag: "foreign", threadId })),
      Effect.orElseSucceed(() => LOCAL),
    );
  });

const NoAdditionalPortFailure = Schema.Never;
const AbortPortFailure = Schema.Union([SettlementConflict, JoinedToHost]);
const AppendPortFailure = Schema.Union([ThreadNotMaterialized, AppendConflict, FenceRejected]);

/**
 * The fail-fast refusal for any foreign operation OUTSIDE the closed route-capable subset
 * (plan §1.3): honesty over accidental distribution.
 */
const crossThreadLedgerError = (operation: string, target: string): LedgerError =>
  LedgerError.make({
    operation,
    message:
      `${operation} addressed to foreign Thread ${target} is not route-capable; the ` +
      "closed cross-Object subset is admit, markReady, lookup, resolveAdmission, " +
      "requestAbort, and recordChildSettled. Every other ledger operation is lane-local by " +
      "construction and must execute inside the owning Thread's Durable Object.",
  });

const crossThreadStoreError = (operation: string, target: string): ThreadStoreError =>
  ThreadStoreError.make({
    operation,
    message:
      `${operation} addressed to foreign Thread ${target} is not route-capable; the ` +
      "closed cross-Object subset is materialize, append, read (paged), inspectTail, and " +
      "export. Observation and checkpoints are lane-local by construction and must execute " +
      "inside the owning Thread's Durable Object.",
  });

const makeTransportCall = (transport: ThreadPortTransport["Service"]) =>
  Effect.fn("DoPortRouting.transportCall")(function* (target: ThreadId, call: PortRequest) {
    const encoded = yield* encodePortRequest(call).pipe(
      Effect.mapError((error) =>
        PortProtocolError.make({
          message: boundPortDiagnostic(`The port request could not be encoded: ${error.message}`),
        }),
      ),
    );

    const raw = yield* transport.call(target, encoded);

    return yield* decodePortResponse(raw).pipe(
      Effect.mapError((error) =>
        PortProtocolError.make({
          message: boundPortDiagnostic(`The port response could not be decoded: ${error.message}`),
        }),
      ),
    );
  });

type TransportCall = ReturnType<typeof makeTransportCall>;

const makeRoutedLedgerServices = Effect.fn("DoPortRouting.makeRoutedLedgerServices")(function* (
  options: RoutedPortOptions,
) {
  const local = yield* SubmissionLedger;
  const transport = yield* ThreadPortTransport;
  const transportCall: TransportCall = makeTransportCall(transport);
  const submissionTarget = routableSubmissionTarget(options.localThreadId);

  const routeFailure =
    (operation: string, target: string) =>
    (error: PortTransportError | PortProtocolError): LedgerError =>
      LedgerError.make({
        operation,
        message: boundPortDiagnostic(
          `Routed ${operation} to the Thread Object owning ${target} failed: ${error.message}`,
        ),
        cause: error,
      });

  /**
   * One routed ledger call: encode, deliver, decode, then narrow the uniform envelope to the
   * operation's own result and failure surface. A foreign `LedgerError` is always in-channel;
   * any failure outside the operation's declared surface — including protocol anomalies — is
   * folded into a `LedgerError` naming the anomaly instead of being erased or re-thrown raw.
   */
  const foreignLedgerCall = <ResultSchema extends Schema.Top, FailureSchema extends Schema.Top>(
    operation: string,
    target: ThreadId,
    call: PortRequest,
    resultSchema: ResultSchema,
    failureSchema: FailureSchema,
  ): Effect.Effect<ResultSchema["Type"], FailureSchema["Type"] | LedgerError> => {
    const isExpectedResult = Schema.is(resultSchema);
    const isExpectedFailure = Schema.is(failureSchema);

    return transportCall(target, call).pipe(
      Effect.mapError(routeFailure(operation, target)),
      Effect.flatMap(
        (response): Effect.Effect<ResultSchema["Type"], FailureSchema["Type"] | LedgerError> => {
          if (response._tag === "PortFailed") {
            const failure = response.failure;

            if (isExpectedFailure(failure)) return Effect.fail(failure);
            if (failure._tag === "LedgerError") return Effect.fail(failure);

            return Effect.fail(
              LedgerError.make({
                operation,
                message: boundPortDiagnostic(
                  `The Thread Object owning ${target} answered ${operation} with the ` +
                    `out-of-contract failure ${failure._tag}: ${failure.message}`,
                ),
                cause: failure,
              }),
            );
          }
          const result = response.result;

          if (!isExpectedResult(result)) {
            return Effect.fail(
              LedgerError.make({
                operation,
                message:
                  `The Thread Object owning ${target} answered ${operation} with the ` +
                  `mismatched result ${result._tag}.`,
              }),
            );
          }

          return Effect.succeed(result);
        },
      ),
      Effect.withSpan("DoPortRouting.foreignLedgerCall", {
        attributes: { operation, target },
      }),
    );
  };

  /**
   * Routed `resolveAdmission` — where the S2 tri-state becomes real (plan §1.3): when the
   * owning Object cannot be reached, or its answer cannot be understood, the routed adapter
   * answers `AdmissionIndeterminate{reason}` and NEVER `NotAdmitted` — an unreachable
   * authority proves nothing, and only `NotAdmitted` permits an admission attempt (SUB-031).
   * A typed `LedgerError` answered BY the authority still fails typed: the authority was
   * reached and reported its own storage failure.
   */
  const resolveForeignAdmission = (
    target: ThreadId,
    request: SubmissionLookupByKey,
  ): Effect.Effect<
    | AdmissionIndeterminate
    | Extract<PortResult, { readonly _tag: "LedgerResolveAdmissionResult" }>["resolution"],
    LedgerError
  > =>
    transportCall(target, LedgerResolveAdmissionCall.make({ request })).pipe(
      Effect.flatMap((response) => {
        if (response._tag === "PortFailed") {
          if (response.failure._tag === "LedgerError") return Effect.fail(response.failure);

          return Effect.succeed(
            AdmissionIndeterminate.make({
              reason: boundPortDiagnostic(
                `The Thread Object owning ${target} answered resolveAdmission with the ` +
                  `out-of-contract failure ${response.failure._tag}: ${response.failure.message}`,
              ),
            }),
          );
        }
        if (response.result._tag !== "LedgerResolveAdmissionResult") {
          return Effect.succeed(
            AdmissionIndeterminate.make({
              reason: boundPortDiagnostic(
                `The Thread Object owning ${target} answered resolveAdmission with the ` +
                  `mismatched result ${response.result._tag}.`,
              ),
            }),
          );
        }

        return Effect.succeed(response.result.resolution);
      }),
      Effect.catchTags({
        PortTransportError: (error) =>
          Effect.succeed(
            AdmissionIndeterminate.make({
              reason: boundPortDiagnostic(
                `The Thread Object owning ${target} is unreachable: ${error.message}`,
              ),
            }),
          ),
        PortProtocolError: (error) =>
          Effect.succeed(
            AdmissionIndeterminate.make({
              reason: boundPortDiagnostic(
                `The answer of the Thread Object owning ${target} could not be ` +
                  `understood: ${error.message}`,
              ),
            }),
          ),
      }),
      Effect.withSpan("DoPortRouting.resolveForeignAdmission", { attributes: { target } }),
    );

  const foreignLookupById = (
    operation: string,
    target: ThreadId,
    submissionId: SubmissionId,
  ): Effect.Effect<Option.Option<SubmissionSnapshot>, LedgerError> =>
    foreignLedgerCall(
      operation,
      target,
      LedgerLookupCall.make({ request: SubmissionLookupById.make({ submissionId }) }),
      LedgerLookupResult,
      NoAdditionalPortFailure,
    ).pipe(
      Effect.map((result) =>
        result.submission === undefined ? Option.none() : Option.some(result.submission),
      ),
    );

  /**
   * Enrich a LOCAL parent's recovery snapshot with the lane state of attached children whose
   * rows live in other Durable Objects (plan §1.3): markers first (the local facet already
   * consulted them), then a routed per-child `lookup` for any attached child that is neither
   * local nor marker-settled. A transport failure surfaces as `LedgerError` so the alarm
   * pass retries; the child's canonical Settlement remains the only authority (DUR-015).
   */
  const enrichChildAttachments = Effect.fn("DoPortRouting.enrichChildAttachments")(function* (
    snapshot: RecoverySnapshot,
  ): Effect.fn.Return<RecoverySnapshot, LedgerError> {
    const operation = "ledger load recovery snapshot";

    const attachments = new Map(
      snapshot.childAttachments.map((attachment) => [attachment.childSubmissionId, attachment]),
    );

    let enriched = false;

    for (const reservation of snapshot.childReservations) {
      const childSubmissionId = reservation.childSubmissionId;

      if (childSubmissionId === undefined || attachments.has(childSubmissionId)) continue;
      const target = yield* submissionTarget(operation, childSubmissionId);

      // A local or opaque child identity was already answered authoritatively by the local
      // facet; absence there means the child admission never committed.
      if (target._tag !== "foreign") continue;
      const child = yield* foreignLookupById(operation, target.threadId, childSubmissionId);

      if (Option.isNone(child)) continue;
      attachments.set(
        childSubmissionId,
        ChildAttachmentSnapshot.make({
          toolCallId: reservation.parentToolCallId,
          childSubmissionId,
          childState: child.value.state,
          ...(child.value.settledOutcome === undefined
            ? {}
            : { childOutcome: child.value.settledOutcome }),
        }),
      );
      enriched = true;
    }
    if (!enriched) return snapshot;
    // Rebuild in reservation (parent Tool Call) order, the order the local facet documents.
    const ordered: Array<ChildAttachmentSnapshot> = [];

    for (const reservation of snapshot.childReservations) {
      if (reservation.childSubmissionId === undefined) continue;
      const attachment = attachments.get(reservation.childSubmissionId);

      if (attachment !== undefined) ordered.push(attachment);
    }

    return RecoverySnapshot.make({ ...snapshot, childAttachments: ordered });
  });

  const routed = SubmissionLedger.of({
    capabilities: local.capabilities,

    admit: (request) =>
      request.threadId === options.localThreadId
        ? local.admit(request)
        : foreignLedgerCall(
            "ledger admit",
            request.threadId,
            LedgerAdmitCall.make({ request }),
            LedgerAdmitResult,
            AdmissionConflict,
          ).pipe(Effect.map((reply) => reply.result)),

    markReady: (request) =>
      submissionTarget("ledger mark ready", request.submissionId).pipe(
        Effect.flatMap((target) =>
          target._tag === "local"
            ? local.markReady(request)
            : foreignLedgerCall(
                "ledger mark ready",
                target.threadId,
                LedgerMarkReadyCall.make({ request }),
                LedgerMarkReadyResult,
                NoAdditionalPortFailure,
              ).pipe(Effect.asVoid),
        ),
      ),

    lookup: (request) =>
      request._tag === "SubmissionLookupById"
        ? submissionTarget("ledger lookup", request.submissionId).pipe(
            Effect.flatMap((target) =>
              target._tag === "local"
                ? local.lookup(request)
                : foreignLookupById("ledger lookup", target.threadId, request.submissionId),
            ),
          )
        : request.threadId === options.localThreadId
          ? local.lookup(request)
          : foreignLedgerCall(
              "ledger lookup",
              request.threadId,
              LedgerLookupCall.make({ request }),
              LedgerLookupResult,
              NoAdditionalPortFailure,
            ).pipe(
              Effect.map((result) =>
                result.submission === undefined ? Option.none() : Option.some(result.submission),
              ),
            ),

    resolveAdmission: (request) =>
      request.threadId === options.localThreadId
        ? local.resolveAdmission(request)
        : resolveForeignAdmission(request.threadId, request),

    requestAbort: (request) =>
      submissionTarget("ledger request abort", request.submissionId).pipe(
        Effect.flatMap((target) =>
          target._tag === "local"
            ? local.requestAbort(request)
            : foreignLedgerCall(
                "ledger request abort",
                target.threadId,
                LedgerRequestAbortCall.make({ request }),
                LedgerRequestAbortResult,
                AbortPortFailure,
              ).pipe(Effect.map((reply) => reply.intent)),
        ),
      ),

    recordChildSettled: (request) =>
      submissionTarget("ledger record child settled", request.parentSubmissionId).pipe(
        Effect.flatMap((target) =>
          target._tag === "local"
            ? local.recordChildSettled(request)
            : foreignLedgerCall(
                "ledger record child settled",
                target.threadId,
                LedgerRecordChildSettledCall.make({ request }),
                LedgerRecordChildSettledResult,
                NoAdditionalPortFailure,
              ).pipe(Effect.map((reply) => reply.outcome)),
        ),
      ),

    // Every operation below is lane-local by construction (plan §1.3): a foreign address is
    // an out-of-contract call and fails fast typed instead of being quietly distributed.
    claim: (request) =>
      request.threadId === options.localThreadId
        ? local.claim(request)
        : Effect.fail(crossThreadLedgerError("ledger claim", request.threadId)),

    claimJoining: (request) =>
      request.threadId === options.localThreadId
        ? local.claimJoining(request)
        : Effect.fail(crossThreadLedgerError("ledger claim joining", request.threadId)),

    renewOwnership: (request) =>
      submissionTarget("ledger renew ownership", request.submissionId).pipe(
        Effect.flatMap((target) =>
          target._tag === "local"
            ? local.renewOwnership(request)
            : Effect.fail(crossThreadLedgerError("ledger renew ownership", target.threadId)),
        ),
      ),

    releaseOwnership: (request) =>
      submissionTarget("ledger release ownership", request.submissionId).pipe(
        Effect.flatMap((target) =>
          target._tag === "local"
            ? local.releaseOwnership(request)
            : Effect.fail(crossThreadLedgerError("ledger release ownership", target.threadId)),
        ),
      ),

    markInputApplied: (request) =>
      submissionTarget("ledger mark input applied", request.submissionId).pipe(
        Effect.flatMap((target) =>
          target._tag === "local"
            ? local.markInputApplied(request)
            : Effect.fail(crossThreadLedgerError("ledger mark input applied", target.threadId)),
        ),
      ),

    reserveSettlement: (request) =>
      submissionTarget("ledger reserve settlement", request.submissionId).pipe(
        Effect.flatMap((target) =>
          target._tag === "local"
            ? local.reserveSettlement(request)
            : Effect.fail(crossThreadLedgerError("ledger reserve settlement", target.threadId)),
        ),
      ),

    finalizeSettlement: (request) =>
      submissionTarget("ledger finalize settlement", request.submissionId).pipe(
        Effect.flatMap((target) =>
          target._tag === "local"
            ? local.finalizeSettlement(request)
            : Effect.fail(crossThreadLedgerError("ledger finalize settlement", target.threadId)),
        ),
      ),

    markJoined: (request) =>
      submissionTarget("ledger mark joined", request.submissionId).pipe(
        Effect.flatMap((target) =>
          target._tag === "local"
            ? local.markJoined(request)
            : Effect.fail(crossThreadLedgerError("ledger mark joined", target.threadId)),
        ),
      ),

    revertJoining: (request) =>
      submissionTarget("ledger revert joining", request.submissionId).pipe(
        Effect.flatMap((target) =>
          target._tag === "local"
            ? local.revertJoining(request)
            : Effect.fail(crossThreadLedgerError("ledger revert joining", target.threadId)),
        ),
      ),

    suspend: (request) =>
      submissionTarget("ledger suspend", request.submissionId).pipe(
        Effect.flatMap((target) =>
          target._tag === "local"
            ? local.suspend(request)
            : Effect.fail(crossThreadLedgerError("ledger suspend", target.threadId)),
        ),
      ),

    recordApprovalDecision: (command) =>
      submissionTarget("ledger record approval decision", command.submissionId).pipe(
        Effect.flatMap((target) =>
          target._tag === "local"
            ? local.recordApprovalDecision(command)
            : Effect.fail(
                crossThreadLedgerError("ledger record approval decision", target.threadId),
              ),
        ),
      ),

    markUnknown: (request) =>
      submissionTarget("ledger mark unknown", request.submissionId).pipe(
        Effect.flatMap((target) =>
          target._tag === "local"
            ? local.markUnknown(request)
            : Effect.fail(crossThreadLedgerError("ledger mark unknown", target.threadId)),
        ),
      ),

    recordUnknownResolution: (command) =>
      submissionTarget("ledger record unknown resolution", command.submissionId).pipe(
        Effect.flatMap((target) =>
          target._tag === "local"
            ? local.recordUnknownResolution(command)
            : Effect.fail(
                crossThreadLedgerError("ledger record unknown resolution", target.threadId),
              ),
        ),
      ),

    reserveChildBudget: (request) =>
      submissionTarget("ledger reserve child budget", request.parentSubmissionId).pipe(
        Effect.flatMap((target) =>
          target._tag === "local"
            ? local.reserveChildBudget(request)
            : Effect.fail(crossThreadLedgerError("ledger reserve child budget", target.threadId)),
        ),
      ),

    // Reservation identities carry no Thread address; the reservation row lives in the
    // parent's own Object and these transitions are parent-lane-local by construction, so
    // they always execute on the local facet (which fails typed for an unknown row).
    attachChildToReservation: local.attachChildToReservation,
    beginChildBudgetRelease: local.beginChildBudgetRelease,
    releaseChildBudget: local.releaseChildBudget,

    // The local scan IS the whole worklist: one Thread per Object (durability §5).
    scanNonterminal: local.scanNonterminal,

    loadRecoverySnapshot: (request) =>
      submissionTarget("ledger load recovery snapshot", request.submissionId).pipe(
        Effect.flatMap((target) =>
          target._tag === "local"
            ? local.loadRecoverySnapshot(request).pipe(Effect.flatMap(enrichChildAttachments))
            : Effect.fail(crossThreadLedgerError("ledger load recovery snapshot", target.threadId)),
        ),
      ),
  });

  return Context.make(SubmissionLedger, routed);
});

const makeRoutedStoreServices = Effect.fn("DoPortRouting.makeRoutedStoreServices")(function* (
  options: RoutedPortOptions,
) {
  const local = yield* ThreadStore;
  const checkpoints = local.checkpoints;
  const transport = yield* ThreadPortTransport;
  const transportCall: TransportCall = makeTransportCall(transport);

  const routeFailure =
    (operation: string, target: string) =>
    (error: PortTransportError | PortProtocolError): ThreadStoreError =>
      ThreadStoreError.make({
        operation,
        message: boundPortDiagnostic(
          `Routed ${operation} to the Thread Object owning ${target} failed: ${error.message}`,
        ),
        cause: error,
      });

  /** The store twin of `foreignLedgerCall` with `ThreadStoreError` as the base error. */
  const foreignStoreCall = <ResultSchema extends Schema.Top, FailureSchema extends Schema.Top>(
    operation: string,
    target: ThreadId,
    call: PortRequest,
    resultSchema: ResultSchema,
    failureSchema: FailureSchema,
  ): Effect.Effect<ResultSchema["Type"], FailureSchema["Type"] | ThreadStoreError> => {
    const isExpectedResult = Schema.is(resultSchema);
    const isExpectedFailure = Schema.is(failureSchema);

    return transportCall(target, call).pipe(
      Effect.mapError(routeFailure(operation, target)),
      Effect.flatMap(
        (
          response,
        ): Effect.Effect<ResultSchema["Type"], FailureSchema["Type"] | ThreadStoreError> => {
          if (response._tag === "PortFailed") {
            const failure = response.failure;

            if (isExpectedFailure(failure)) return Effect.fail(failure);
            if (failure._tag === "ThreadStoreError") return Effect.fail(failure);

            return Effect.fail(
              ThreadStoreError.make({
                operation,
                message: boundPortDiagnostic(
                  `The Thread Object owning ${target} answered ${operation} with the ` +
                    `out-of-contract failure ${failure._tag}: ${failure.message}`,
                ),
                cause: failure,
              }),
            );
          }
          const result = response.result;

          if (!isExpectedResult(result)) {
            return Effect.fail(
              ThreadStoreError.make({
                operation,
                message:
                  `The Thread Object owning ${target} answered ${operation} with the ` +
                  `mismatched result ${result._tag}.`,
              }),
            );
          }

          return Effect.succeed(result);
        },
      ),
      Effect.withSpan("DoPortRouting.foreignStoreCall", {
        attributes: { operation, target },
      }),
    );
  };

  const routed = ThreadStore.of({
    materialize: (request) =>
      request.threadId === options.localThreadId
        ? local.materialize(request)
        : foreignStoreCall(
            "thread materialize",
            request.threadId,
            StoreMaterializeCall.make({ request }),
            StoreMaterializeResult,
            FenceRejected,
          ).pipe(Effect.asVoid),

    append: (request) =>
      request.threadId === options.localThreadId
        ? local.append(request)
        : foreignStoreCall(
            "thread append",
            request.threadId,
            StoreAppendCall.make({ request }),
            StoreAppendResult,
            AppendPortFailure,
          ).pipe(Effect.map((reply) => reply.result)),

    read: (request) =>
      request.threadId === options.localThreadId
        ? local.read(request)
        : Stream.unwrap(
            foreignStoreCall(
              "thread read",
              request.threadId,
              StoreReadPageCall.make({ request }),
              StoreReadPageResult,
              ThreadNotMaterialized,
            ).pipe(Effect.map((reply) => Stream.fromIterable(reply.records))),
          ),

    inspectTail: (request) =>
      request.threadId === options.localThreadId
        ? local.inspectTail(request)
        : foreignStoreCall(
            "thread inspect tail",
            request.threadId,
            StoreInspectTailCall.make({ request }),
            StoreInspectTailResult,
            ThreadNotMaterialized,
          ).pipe(Effect.map((reply) => reply.tail)),

    export: (request) =>
      request.threadId === options.localThreadId
        ? local.export(request)
        : foreignStoreCall(
            "thread export",
            request.threadId,
            StoreExportCall.make({ request }),
            StoreExportResult,
            ThreadNotMaterialized,
          ).pipe(Effect.map((reply) => reply.export)),

    // Observation and checkpoints are lane-local by construction (plan §1.3): the closed
    // route-capable store subset is materialize/append/read/inspectTail/export, and a
    // foreign address on anything else fails fast typed.
    observe: (request) =>
      request.threadId === options.localThreadId
        ? local.observe(request)
        : Stream.unwrap(Effect.fail(crossThreadStoreError("thread observe", request.threadId))),

    ...(checkpoints === undefined
      ? {}
      : {
          checkpoints: {
            save: (request) =>
              request.checkpoint.threadId === options.localThreadId
                ? checkpoints.save(request)
                : Effect.fail(
                    crossThreadStoreError("thread save checkpoint", request.checkpoint.threadId),
                  ),
            load: (request) =>
              request.threadId === options.localThreadId
                ? checkpoints.load(request)
                : Effect.fail(crossThreadStoreError("thread load checkpoint", request.threadId)),
          },
        }),
  });

  return Context.make(ThreadStore, routed);
});

/**
 * Routing decorator over the LOCAL `SubmissionLedger` facet (plan §1.3): a request addressing
 * this Object's Thread executes locally; a route-capable request addressing another
 * Thread is Schema-encoded onto the `ThreadPortTransport` and executed by the
 * owning Object's local facet; any other foreign request fails fast typed. Provide the WP1
 * local facet (`submissionLedgerLayer`/`ledgerLayer`) and a transport to close it.
 */
export const routedSubmissionLedgerLayer = (
  options: RoutedPortOptions,
): Layer.Layer<SubmissionLedger, never, SubmissionLedger | ThreadPortTransport> =>
  Layer.effectContext(makeRoutedLedgerServices(options));

/**
 * Routing decorator over the LOCAL `ThreadStore` facet (plan §1.3): this-thread
 * requests execute locally; foreign materialize/append/read/inspectTail/export travel the
 * transport; foreign observation and checkpoints fail fast typed.
 */
export const routedThreadStoreLayer = (
  options: RoutedPortOptions,
): Layer.Layer<ThreadStore, never, ThreadStore | ThreadPortTransport> =>
  Layer.effectContext(makeRoutedStoreServices(options));

// ---------------------------------------------------------------------------
// Owner-side execution
// ---------------------------------------------------------------------------

/** Fold one port operation's typed failures into the uniform response envelope. */
const capture = <Failure extends PortFailure>(
  effect: Effect.Effect<PortResult, Failure>,
): Effect.Effect<PortResponse> =>
  effect.pipe(
    Effect.map((result): PortResponse => PortSucceeded.make({ result })),
    Effect.catch((failure) => Effect.succeed<PortResponse>(PortFailed.make({ failure }))),
  );

/**
 * Execute one decoded port request against THIS Object's LOCAL facets — the owner-side half
 * of the routed ports (plan §1.3). Callers must provide the WP1 local facets, never the
 * routed decorators: the routing layer already established that this Object owns the
 * addressed Thread, and re-routing here could bounce a request between Objects.
 * Failures never escape — every typed port failure becomes a `PortFailed` envelope that
 * re-decodes on the caller side.
 */
export const executePortRequest = Effect.fn("DoPortRouting.executePortRequest")(function* (
  request: PortRequest,
): Effect.fn.Return<PortResponse, never, SubmissionLedger | ThreadStore> {
  switch (request._tag) {
    case "LedgerAdmit": {
      const ledger = yield* SubmissionLedger;

      return yield* capture(
        ledger
          .admit(request.request)
          .pipe(Effect.map((result) => LedgerAdmitResult.make({ result }))),
      );
    }
    case "LedgerMarkReady": {
      const ledger = yield* SubmissionLedger;

      return yield* capture(
        ledger.markReady(request.request).pipe(Effect.map(() => LedgerMarkReadyResult.make({}))),
      );
    }
    case "LedgerLookup": {
      const ledger = yield* SubmissionLedger;

      return yield* capture(
        ledger
          .lookup(request.request)
          .pipe(
            Effect.map((submission) =>
              Option.isSome(submission)
                ? LedgerLookupResult.make({ submission: submission.value })
                : LedgerLookupResult.make({}),
            ),
          ),
      );
    }
    case "LedgerResolveAdmission": {
      const ledger = yield* SubmissionLedger;

      return yield* capture(
        ledger
          .resolveAdmission(request.request)
          .pipe(Effect.map((resolution) => LedgerResolveAdmissionResult.make({ resolution }))),
      );
    }
    case "LedgerRequestAbort": {
      const ledger = yield* SubmissionLedger;

      return yield* capture(
        ledger
          .requestAbort(request.request)
          .pipe(Effect.map((intent) => LedgerRequestAbortResult.make({ intent }))),
      );
    }
    case "LedgerRecordChildSettled": {
      const ledger = yield* SubmissionLedger;

      return yield* capture(
        ledger
          .recordChildSettled(request.request)
          .pipe(Effect.map((outcome) => LedgerRecordChildSettledResult.make({ outcome }))),
      );
    }
    case "StoreMaterialize": {
      const store = yield* ThreadStore;

      return yield* capture(
        store.materialize(request.request).pipe(Effect.map(() => StoreMaterializeResult.make({}))),
      );
    }
    case "StoreAppend": {
      const store = yield* ThreadStore;

      return yield* capture(
        store
          .append(request.request)
          .pipe(Effect.map((result) => StoreAppendResult.make({ result }))),
      );
    }
    case "StoreReadPage": {
      const store = yield* ThreadStore;

      return yield* capture(
        store.read(request.request).pipe(
          Stream.runCollect,
          Effect.map((records) => StoreReadPageResult.make({ records: [...records] })),
        ),
      );
    }
    case "StoreInspectTail": {
      const store = yield* ThreadStore;

      return yield* capture(
        store
          .inspectTail(request.request)
          .pipe(Effect.map((tail) => StoreInspectTailResult.make({ tail }))),
      );
    }
    case "StoreExport": {
      const store = yield* ThreadStore;

      return yield* capture(
        store
          .export(request.request)
          .pipe(Effect.map((threadExport) => StoreExportResult.make({ export: threadExport }))),
      );
    }
  }
});

/**
 * The last-resort wire fallback when even encoding a response fails: the literal encoded
 * form of `PortFailed(PortProtocolError)` — tagged classes of bounded strings encode to
 * exactly this shape, so no Schema round trip is needed to produce it.
 */
const encodedProtocolFailure = (message: string): unknown => ({
  _tag: "PortFailed",
  failure: { _tag: "PortProtocolError", message: boundPortDiagnostic(message) },
});

/**
 * The complete owner-side endpoint body for `portCall` (D-P6-3): decode the wire request,
 * execute it against this Object's LOCAL facets, and answer with the encoded response
 * envelope. Total by construction — a request that cannot be decoded, or a response that
 * cannot be encoded, answers `PortFailed(PortProtocolError)` instead of throwing, so the
 * transport never has to interpret exceptions as protocol answers.
 */
export const handleEncodedPortRequest = Effect.fn("DoPortRouting.handleEncodedPortRequest")(
  function* (encoded: unknown): Effect.fn.Return<unknown, never, SubmissionLedger | ThreadStore> {
    const response = yield* decodePortRequest(encoded).pipe(
      Effect.flatMap(executePortRequest),
      Effect.catch((error) =>
        Effect.succeed<PortResponse>(
          PortFailed.make({
            failure: PortProtocolError.make({
              message: boundPortDiagnostic(
                `The port request could not be decoded: ${error.message}`,
              ),
            }),
          }),
        ),
      ),
    );

    return yield* encodePortResponse(response).pipe(
      Effect.catch((error) =>
        Effect.succeed(
          encodedProtocolFailure(`The port response could not be encoded: ${error.message}`),
        ),
      ),
    );
  },
);

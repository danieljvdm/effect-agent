import {
  AdmissionIndeterminate,
  AppendConflict,
  ChildAttachmentSnapshot,
  ConversationMaterialization,
  ConversationNotMaterialized,
  ConversationStore,
  ConversationStoreError,
  FenceRejected,
  JoinedToHost,
  LedgerError,
  RecoverySnapshot,
  SettlementConflict,
  SubmissionLedger,
  SubmissionLookupById,
  type AdmissionConflict,
  type SubmissionLookupByKey,
  type SubmissionSnapshot,
} from "@effect-agent/session";
import { Context, Effect, Layer, Option, Schema, Stream } from "effect";

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
} from "./port-protocol.ts";

type ConversationId = ConversationMaterialization["conversationId"];
type SubmissionId = SubmissionSnapshot["submissionId"];

const ConversationIdSchema = ConversationMaterialization.fields.conversationId;
const decodeConversationId = Schema.decodeUnknownEffect(ConversationIdSchema);

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
 * Conversation's Durable Object. `retryable` carries the platform's own stub signal when one
 * exists. This error never crosses the wire — it is the CALLER-side evidence that the
 * authority was unreachable, which is exactly the case `AdmissionIndeterminate` was
 * specified for (SUB-031).
 */
export class PortTransportError extends Schema.TaggedErrorClass<PortTransportError>()(
  "PortTransportError",
  {
    target: Schema.String,
    message: Schema.String,
    retryable: Schema.optionalKey(Schema.Boolean),
    cause: Schema.optionalKey(Schema.Defect()),
  },
) {}

/**
 * Build a `PortTransportError` from an arbitrary thrown transport cause, preserving the
 * platform stub's own `retryable` signal when present.
 */
export const portTransportFailure = (target: string, cause: unknown): PortTransportError => {
  const message = cause instanceof Error ? cause.message : String(cause);
  let retryable: boolean | undefined;
  if (typeof cause === "object" && cause !== null && "retryable" in cause) {
    const signal = cause.retryable;
    if (typeof signal === "boolean") retryable = signal;
  }
  return PortTransportError.make({
    target,
    message: boundPortDiagnostic(message),
    ...(retryable === undefined ? {} : { retryable }),
    cause,
  });
};

/**
 * Delivery of Schema-encoded port envelopes to the Durable Object that owns a FOREIGN
 * Conversation (plan §1.3, D-P6-3). The shipped implementation (platform-cloudflare, WP3)
 * calls the owner's `portCall` over native Durable Object JS RPC via
 * `namespace.idFromName(conversationId)`; the protocol is transport-agnostic and any carrier
 * that moves the encoded envelopes verbatim satisfies this service. Implementations MUST
 * surface every delivery problem as `PortTransportError` and must never fabricate an answer.
 */
export class ConversationPortTransport extends Context.Service<
  ConversationPortTransport,
  {
    readonly call: (
      conversationId: ConversationId,
      request: PortRequestEnvelope,
    ) => Effect.Effect<unknown, PortTransportError>;
  }
>()("@effect-agent/storage-cloudflare/ConversationPortTransport") {}

/** Construction options shared by both routed port Layers. */
export interface RoutedPortOptions {
  /**
   * The Conversation this Durable Object owns (the Object identity rule is
   * `namespace.idFromName(conversationId)`). Requests addressed here execute on the local
   * facet; requests addressed anywhere else route through the transport or fail fast typed.
   */
  readonly localConversationId: ConversationId;
}

/** Where one port request must execute. */
type RouteTarget =
  | { readonly _tag: "local" }
  | { readonly _tag: "foreign"; readonly conversationId: ConversationId };

const LOCAL: RouteTarget = { _tag: "local" };

/**
 * Parse a DC-minted routable Submission identity — `{uuidv7}:{conversationId}`, split at the
 * FIRST `:` because the Conversation tail may itself contain colons (D-P6-5). This adapter
 * minted the format at admission and is the ONLY component that parses it; identities that do
 * not carry the minted shape (no separator, non-UUID head, empty tail) fall back to the local
 * facet, which is the only authority this Object can consult without inventing an owner.
 * Identities beyond the ledger's 1,024-character row bound fail typed: the minting side
 * refused them at admission, so they cannot name any stored row anywhere.
 */
const routableSubmissionTarget = (
  localConversationId: ConversationId,
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
    if (tail === localConversationId) return LOCAL;
    return yield* decodeConversationId(tail).pipe(
      Effect.map((conversationId): RouteTarget => ({ _tag: "foreign", conversationId })),
      Effect.orElseSucceed(() => LOCAL),
    );
  });

const isResultTag =
  <Tag extends PortResult["_tag"]>(tag: Tag) =>
  (result: PortResult): result is Extract<PortResult, { readonly _tag: Tag }> =>
    result._tag === tag;

const isAdmitConflict = (failure: PortFailure): failure is AdmissionConflict =>
  failure._tag === "AdmissionConflict";
const isAbortConflict = (failure: PortFailure): failure is SettlementConflict | JoinedToHost =>
  failure._tag === "SettlementConflict" || failure._tag === "JoinedToHost";
const noExtraFailure = (_failure: PortFailure): _failure is never => false;
const isFenceRejected = (failure: PortFailure): failure is FenceRejected =>
  failure._tag === "FenceRejected";
const isAppendFailure = (
  failure: PortFailure,
): failure is ConversationNotMaterialized | AppendConflict | FenceRejected =>
  failure._tag === "ConversationNotMaterialized" ||
  failure._tag === "AppendConflict" ||
  failure._tag === "FenceRejected";
const isNotMaterialized = (failure: PortFailure): failure is ConversationNotMaterialized =>
  failure._tag === "ConversationNotMaterialized";

/**
 * The fail-fast refusal for any foreign operation OUTSIDE the closed route-capable subset
 * (plan §1.3): honesty over accidental distribution.
 */
const crossConversationLedgerError = (operation: string, target: string): LedgerError =>
  LedgerError.make({
    operation,
    message:
      `${operation} addressed to foreign Conversation ${target} is not route-capable; the ` +
      "closed cross-Object subset is admit, markReady, lookup, resolveAdmission, " +
      "requestAbort, and recordChildSettled. Every other ledger operation is lane-local by " +
      "construction and must execute inside the owning Conversation's Durable Object.",
  });

const crossConversationStoreError = (operation: string, target: string): ConversationStoreError =>
  ConversationStoreError.make({
    operation,
    message:
      `${operation} addressed to foreign Conversation ${target} is not route-capable; the ` +
      "closed cross-Object subset is materialize, append, read (paged), inspectTail, and " +
      "export. Observation and checkpoints are lane-local by construction and must execute " +
      "inside the owning Conversation's Durable Object.",
  });

const makeTransportCall = (transport: ConversationPortTransport["Service"]) =>
  Effect.fn("DoPortRouting.transportCall")(function* (target: ConversationId, call: PortRequest) {
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
  const transport = yield* ConversationPortTransport;
  const transportCall: TransportCall = makeTransportCall(transport);
  const submissionTarget = routableSubmissionTarget(options.localConversationId);

  const routeFailure =
    (operation: string, target: string) =>
    (error: PortTransportError | PortProtocolError): LedgerError =>
      LedgerError.make({
        operation,
        message: boundPortDiagnostic(
          `Routed ${operation} to the Conversation Object owning ${target} failed: ${error.message}`,
        ),
        cause: error,
      });

  /**
   * One routed ledger call: encode, deliver, decode, then narrow the uniform envelope to the
   * operation's own result and failure surface. A foreign `LedgerError` is always in-channel;
   * any failure outside the operation's declared surface — including protocol anomalies — is
   * folded into a `LedgerError` naming the anomaly instead of being erased or re-thrown raw.
   */
  const foreignLedgerCall = <Tag extends PortResult["_tag"], ExpectedFailure extends PortFailure>(
    operation: string,
    target: ConversationId,
    call: PortRequest,
    resultTag: Tag,
    isExpectedFailure: (failure: PortFailure) => failure is ExpectedFailure,
  ): Effect.Effect<Extract<PortResult, { readonly _tag: Tag }>, ExpectedFailure | LedgerError> =>
    transportCall(target, call).pipe(
      Effect.mapError(routeFailure(operation, target)),
      Effect.flatMap(
        (
          response,
        ): Effect.Effect<
          Extract<PortResult, { readonly _tag: Tag }>,
          ExpectedFailure | LedgerError
        > => {
          if (response._tag === "PortFailed") {
            const failure = response.failure;
            if (isExpectedFailure(failure)) return Effect.fail(failure);
            if (failure._tag === "LedgerError") return Effect.fail(failure);
            return Effect.fail(
              LedgerError.make({
                operation,
                message: boundPortDiagnostic(
                  `The Conversation Object owning ${target} answered ${operation} with the ` +
                    `out-of-contract failure ${failure._tag}: ${failure.message}`,
                ),
                cause: failure,
              }),
            );
          }
          const result = response.result;
          if (!isResultTag(resultTag)(result)) {
            return Effect.fail(
              LedgerError.make({
                operation,
                message:
                  `The Conversation Object owning ${target} answered ${operation} with the ` +
                  `mismatched result ${result._tag}; expected ${resultTag}.`,
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

  /**
   * Routed `resolveAdmission` — where the S2 tri-state becomes real (plan §1.3): when the
   * owning Object cannot be reached, or its answer cannot be understood, the routed adapter
   * answers `AdmissionIndeterminate{reason}` and NEVER `NotAdmitted` — an unreachable
   * authority proves nothing, and only `NotAdmitted` permits an admission attempt (SUB-031).
   * A typed `LedgerError` answered BY the authority still fails typed: the authority was
   * reached and reported its own storage failure.
   */
  const resolveForeignAdmission = (
    target: ConversationId,
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
                `The Conversation Object owning ${target} answered resolveAdmission with the ` +
                  `out-of-contract failure ${response.failure._tag}: ${response.failure.message}`,
              ),
            }),
          );
        }
        if (response.result._tag !== "LedgerResolveAdmissionResult") {
          return Effect.succeed(
            AdmissionIndeterminate.make({
              reason: boundPortDiagnostic(
                `The Conversation Object owning ${target} answered resolveAdmission with the ` +
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
                `The Conversation Object owning ${target} is unreachable: ${error.message}`,
              ),
            }),
          ),
        PortProtocolError: (error) =>
          Effect.succeed(
            AdmissionIndeterminate.make({
              reason: boundPortDiagnostic(
                `The answer of the Conversation Object owning ${target} could not be ` +
                  `understood: ${error.message}`,
              ),
            }),
          ),
      }),
      Effect.withSpan("DoPortRouting.resolveForeignAdmission", { attributes: { target } }),
    );

  const foreignLookupById = (
    operation: string,
    target: ConversationId,
    submissionId: SubmissionId,
  ): Effect.Effect<Option.Option<SubmissionSnapshot>, LedgerError> =>
    foreignLedgerCall(
      operation,
      target,
      LedgerLookupCall.make({ request: SubmissionLookupById.make({ submissionId }) }),
      "LedgerLookupResult",
      noExtraFailure,
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
      const child = yield* foreignLookupById(operation, target.conversationId, childSubmissionId);
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
    return RecoverySnapshot.make({
      submission: snapshot.submission,
      joins: snapshot.joins,
      approvalDecisions: snapshot.approvalDecisions,
      unknownResolutions: snapshot.unknownResolutions,
      childReservations: snapshot.childReservations,
      childAttachments: ordered,
      ...(snapshot.ownership === undefined ? {} : { ownership: snapshot.ownership }),
      ...(snapshot.inputApplied === undefined ? {} : { inputApplied: snapshot.inputApplied }),
      ...(snapshot.reservation === undefined ? {} : { reservation: snapshot.reservation }),
      ...(snapshot.abortIntent === undefined ? {} : { abortIntent: snapshot.abortIntent }),
      ...(snapshot.hostSubmissionId === undefined
        ? {}
        : { hostSubmissionId: snapshot.hostSubmissionId }),
      ...(snapshot.suspension === undefined ? {} : { suspension: snapshot.suspension }),
      ...(snapshot.parentLinkage === undefined ? {} : { parentLinkage: snapshot.parentLinkage }),
    });
  });

  const routed = SubmissionLedger.of({
    capabilities: local.capabilities,

    admit: (request) =>
      request.conversationId === options.localConversationId
        ? local.admit(request)
        : foreignLedgerCall(
            "ledger admit",
            request.conversationId,
            LedgerAdmitCall.make({ request }),
            "LedgerAdmitResult",
            isAdmitConflict,
          ).pipe(Effect.map((reply) => reply.result)),

    markReady: (request) =>
      submissionTarget("ledger mark ready", request.submissionId).pipe(
        Effect.flatMap((target) =>
          target._tag === "local"
            ? local.markReady(request)
            : foreignLedgerCall(
                "ledger mark ready",
                target.conversationId,
                LedgerMarkReadyCall.make({ request }),
                "LedgerMarkReadyResult",
                noExtraFailure,
              ).pipe(Effect.asVoid),
        ),
      ),

    lookup: (request) =>
      request._tag === "SubmissionLookupById"
        ? submissionTarget("ledger lookup", request.submissionId).pipe(
            Effect.flatMap((target) =>
              target._tag === "local"
                ? local.lookup(request)
                : foreignLookupById("ledger lookup", target.conversationId, request.submissionId),
            ),
          )
        : request.conversationId === options.localConversationId
          ? local.lookup(request)
          : foreignLedgerCall(
              "ledger lookup",
              request.conversationId,
              LedgerLookupCall.make({ request }),
              "LedgerLookupResult",
              noExtraFailure,
            ).pipe(
              Effect.map((result) =>
                result.submission === undefined ? Option.none() : Option.some(result.submission),
              ),
            ),

    resolveAdmission: (request) =>
      request.conversationId === options.localConversationId
        ? local.resolveAdmission(request)
        : resolveForeignAdmission(request.conversationId, request),

    requestAbort: (request) =>
      submissionTarget("ledger request abort", request.submissionId).pipe(
        Effect.flatMap((target) =>
          target._tag === "local"
            ? local.requestAbort(request)
            : foreignLedgerCall(
                "ledger request abort",
                target.conversationId,
                LedgerRequestAbortCall.make({ request }),
                "LedgerRequestAbortResult",
                isAbortConflict,
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
                target.conversationId,
                LedgerRecordChildSettledCall.make({ request }),
                "LedgerRecordChildSettledResult",
                noExtraFailure,
              ).pipe(Effect.map((reply) => reply.outcome)),
        ),
      ),

    // Every operation below is lane-local by construction (plan §1.3): a foreign address is
    // an out-of-contract call and fails fast typed instead of being quietly distributed.
    claim: (request) =>
      request.conversationId === options.localConversationId
        ? local.claim(request)
        : Effect.fail(crossConversationLedgerError("ledger claim", request.conversationId)),

    claimJoining: (request) =>
      request.conversationId === options.localConversationId
        ? local.claimJoining(request)
        : Effect.fail(crossConversationLedgerError("ledger claim joining", request.conversationId)),

    renewOwnership: (request) =>
      submissionTarget("ledger renew ownership", request.submissionId).pipe(
        Effect.flatMap((target) =>
          target._tag === "local"
            ? local.renewOwnership(request)
            : Effect.fail(
                crossConversationLedgerError("ledger renew ownership", target.conversationId),
              ),
        ),
      ),

    releaseOwnership: (request) =>
      submissionTarget("ledger release ownership", request.submissionId).pipe(
        Effect.flatMap((target) =>
          target._tag === "local"
            ? local.releaseOwnership(request)
            : Effect.fail(
                crossConversationLedgerError("ledger release ownership", target.conversationId),
              ),
        ),
      ),

    markInputApplied: (request) =>
      submissionTarget("ledger mark input applied", request.submissionId).pipe(
        Effect.flatMap((target) =>
          target._tag === "local"
            ? local.markInputApplied(request)
            : Effect.fail(
                crossConversationLedgerError("ledger mark input applied", target.conversationId),
              ),
        ),
      ),

    reserveSettlement: (request) =>
      submissionTarget("ledger reserve settlement", request.submissionId).pipe(
        Effect.flatMap((target) =>
          target._tag === "local"
            ? local.reserveSettlement(request)
            : Effect.fail(
                crossConversationLedgerError("ledger reserve settlement", target.conversationId),
              ),
        ),
      ),

    finalizeSettlement: (request) =>
      submissionTarget("ledger finalize settlement", request.submissionId).pipe(
        Effect.flatMap((target) =>
          target._tag === "local"
            ? local.finalizeSettlement(request)
            : Effect.fail(
                crossConversationLedgerError("ledger finalize settlement", target.conversationId),
              ),
        ),
      ),

    markJoined: (request) =>
      submissionTarget("ledger mark joined", request.submissionId).pipe(
        Effect.flatMap((target) =>
          target._tag === "local"
            ? local.markJoined(request)
            : Effect.fail(
                crossConversationLedgerError("ledger mark joined", target.conversationId),
              ),
        ),
      ),

    revertJoining: (request) =>
      submissionTarget("ledger revert joining", request.submissionId).pipe(
        Effect.flatMap((target) =>
          target._tag === "local"
            ? local.revertJoining(request)
            : Effect.fail(
                crossConversationLedgerError("ledger revert joining", target.conversationId),
              ),
        ),
      ),

    suspend: (request) =>
      submissionTarget("ledger suspend", request.submissionId).pipe(
        Effect.flatMap((target) =>
          target._tag === "local"
            ? local.suspend(request)
            : Effect.fail(crossConversationLedgerError("ledger suspend", target.conversationId)),
        ),
      ),

    recordApprovalDecision: (command) =>
      submissionTarget("ledger record approval decision", command.submissionId).pipe(
        Effect.flatMap((target) =>
          target._tag === "local"
            ? local.recordApprovalDecision(command)
            : Effect.fail(
                crossConversationLedgerError(
                  "ledger record approval decision",
                  target.conversationId,
                ),
              ),
        ),
      ),

    markUnknown: (request) =>
      submissionTarget("ledger mark unknown", request.submissionId).pipe(
        Effect.flatMap((target) =>
          target._tag === "local"
            ? local.markUnknown(request)
            : Effect.fail(
                crossConversationLedgerError("ledger mark unknown", target.conversationId),
              ),
        ),
      ),

    recordUnknownResolution: (command) =>
      submissionTarget("ledger record unknown resolution", command.submissionId).pipe(
        Effect.flatMap((target) =>
          target._tag === "local"
            ? local.recordUnknownResolution(command)
            : Effect.fail(
                crossConversationLedgerError(
                  "ledger record unknown resolution",
                  target.conversationId,
                ),
              ),
        ),
      ),

    reserveChildBudget: (request) =>
      submissionTarget("ledger reserve child budget", request.parentSubmissionId).pipe(
        Effect.flatMap((target) =>
          target._tag === "local"
            ? local.reserveChildBudget(request)
            : Effect.fail(
                crossConversationLedgerError("ledger reserve child budget", target.conversationId),
              ),
        ),
      ),

    // Reservation identities carry no Conversation address; the reservation row lives in the
    // parent's own Object and these transitions are parent-lane-local by construction, so
    // they always execute on the local facet (which fails typed for an unknown row).
    attachChildToReservation: local.attachChildToReservation,
    beginChildBudgetRelease: local.beginChildBudgetRelease,
    releaseChildBudget: local.releaseChildBudget,

    // The local scan IS the whole worklist: one Conversation per Object (durability §5).
    scanNonterminal: local.scanNonterminal,

    loadRecoverySnapshot: (request) =>
      submissionTarget("ledger load recovery snapshot", request.submissionId).pipe(
        Effect.flatMap((target) =>
          target._tag === "local"
            ? local.loadRecoverySnapshot(request).pipe(Effect.flatMap(enrichChildAttachments))
            : Effect.fail(
                crossConversationLedgerError(
                  "ledger load recovery snapshot",
                  target.conversationId,
                ),
              ),
        ),
      ),
  });

  return Context.make(SubmissionLedger, routed);
});

const makeRoutedStoreServices = Effect.fn("DoPortRouting.makeRoutedStoreServices")(function* (
  options: RoutedPortOptions,
) {
  const local = yield* ConversationStore;
  const transport = yield* ConversationPortTransport;
  const transportCall: TransportCall = makeTransportCall(transport);

  const routeFailure =
    (operation: string, target: string) =>
    (error: PortTransportError | PortProtocolError): ConversationStoreError =>
      ConversationStoreError.make({
        operation,
        message: boundPortDiagnostic(
          `Routed ${operation} to the Conversation Object owning ${target} failed: ${error.message}`,
        ),
        cause: error,
      });

  /** The store twin of `foreignLedgerCall` with `ConversationStoreError` as the base error. */
  const foreignStoreCall = <Tag extends PortResult["_tag"], ExpectedFailure extends PortFailure>(
    operation: string,
    target: ConversationId,
    call: PortRequest,
    resultTag: Tag,
    isExpectedFailure: (failure: PortFailure) => failure is ExpectedFailure,
  ): Effect.Effect<
    Extract<PortResult, { readonly _tag: Tag }>,
    ExpectedFailure | ConversationStoreError
  > =>
    transportCall(target, call).pipe(
      Effect.mapError(routeFailure(operation, target)),
      Effect.flatMap(
        (
          response,
        ): Effect.Effect<
          Extract<PortResult, { readonly _tag: Tag }>,
          ExpectedFailure | ConversationStoreError
        > => {
          if (response._tag === "PortFailed") {
            const failure = response.failure;
            if (isExpectedFailure(failure)) return Effect.fail(failure);
            if (failure._tag === "ConversationStoreError") return Effect.fail(failure);
            return Effect.fail(
              ConversationStoreError.make({
                operation,
                message: boundPortDiagnostic(
                  `The Conversation Object owning ${target} answered ${operation} with the ` +
                    `out-of-contract failure ${failure._tag}: ${failure.message}`,
                ),
                cause: failure,
              }),
            );
          }
          const result = response.result;
          if (!isResultTag(resultTag)(result)) {
            return Effect.fail(
              ConversationStoreError.make({
                operation,
                message:
                  `The Conversation Object owning ${target} answered ${operation} with the ` +
                  `mismatched result ${result._tag}; expected ${resultTag}.`,
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

  const routed = ConversationStore.of({
    materialize: (request) =>
      request.conversationId === options.localConversationId
        ? local.materialize(request)
        : foreignStoreCall(
            "conversation materialize",
            request.conversationId,
            StoreMaterializeCall.make({ request }),
            "StoreMaterializeResult",
            isFenceRejected,
          ).pipe(Effect.asVoid),

    append: (request) =>
      request.conversationId === options.localConversationId
        ? local.append(request)
        : foreignStoreCall(
            "conversation append",
            request.conversationId,
            StoreAppendCall.make({ request }),
            "StoreAppendResult",
            isAppendFailure,
          ).pipe(Effect.map((reply) => reply.result)),

    read: (request) =>
      request.conversationId === options.localConversationId
        ? local.read(request)
        : Stream.unwrap(
            foreignStoreCall(
              "conversation read",
              request.conversationId,
              StoreReadPageCall.make({ request }),
              "StoreReadPageResult",
              isNotMaterialized,
            ).pipe(Effect.map((reply) => Stream.fromIterable(reply.records))),
          ),

    inspectTail: (request) =>
      request.conversationId === options.localConversationId
        ? local.inspectTail(request)
        : foreignStoreCall(
            "conversation inspect tail",
            request.conversationId,
            StoreInspectTailCall.make({ request }),
            "StoreInspectTailResult",
            isNotMaterialized,
          ).pipe(Effect.map((reply) => reply.tail)),

    export: (request) =>
      request.conversationId === options.localConversationId
        ? local.export(request)
        : foreignStoreCall(
            "conversation export",
            request.conversationId,
            StoreExportCall.make({ request }),
            "StoreExportResult",
            isNotMaterialized,
          ).pipe(Effect.map((reply) => reply.export)),

    // Observation and checkpoints are lane-local by construction (plan §1.3): the closed
    // route-capable store subset is materialize/append/read/inspectTail/export, and a
    // foreign address on anything else fails fast typed.
    observe: (request) =>
      request.conversationId === options.localConversationId
        ? local.observe(request)
        : Stream.unwrap(
            Effect.fail(
              crossConversationStoreError("conversation observe", request.conversationId),
            ),
          ),

    saveCheckpoint: (request) =>
      request.checkpoint.conversationId === options.localConversationId
        ? local.saveCheckpoint(request)
        : Effect.fail(
            crossConversationStoreError(
              "conversation save checkpoint",
              request.checkpoint.conversationId,
            ),
          ),

    loadCheckpoint: (request) =>
      request.conversationId === options.localConversationId
        ? local.loadCheckpoint(request)
        : Effect.fail(
            crossConversationStoreError("conversation load checkpoint", request.conversationId),
          ),
  });

  return Context.make(ConversationStore, routed);
});

/**
 * Routing decorator over the LOCAL `SubmissionLedger` facet (plan §1.3): a request addressing
 * this Object's Conversation executes locally; a route-capable request addressing another
 * Conversation is Schema-encoded onto the `ConversationPortTransport` and executed by the
 * owning Object's local facet; any other foreign request fails fast typed. Provide the WP1
 * local facet (`submissionLedgerLayer`/`ledgerLayer`) and a transport to close it.
 */
export const routedSubmissionLedgerLayer = (
  options: RoutedPortOptions,
): Layer.Layer<SubmissionLedger, never, SubmissionLedger | ConversationPortTransport> =>
  Layer.effectContext(makeRoutedLedgerServices(options));

/**
 * Routing decorator over the LOCAL `ConversationStore` facet (plan §1.3): this-conversation
 * requests execute locally; foreign materialize/append/read/inspectTail/export travel the
 * transport; foreign observation and checkpoints fail fast typed.
 */
export const routedConversationStoreLayer = (
  options: RoutedPortOptions,
): Layer.Layer<ConversationStore, never, ConversationStore | ConversationPortTransport> =>
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
 * addressed Conversation, and re-routing here could bounce a request between Objects.
 * Failures never escape — every typed port failure becomes a `PortFailed` envelope that
 * re-decodes on the caller side.
 */
export const executePortRequest = Effect.fn("DoPortRouting.executePortRequest")(function* (
  request: PortRequest,
): Effect.fn.Return<PortResponse, never, SubmissionLedger | ConversationStore> {
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
      const store = yield* ConversationStore;
      return yield* capture(
        store.materialize(request.request).pipe(Effect.map(() => StoreMaterializeResult.make({}))),
      );
    }
    case "StoreAppend": {
      const store = yield* ConversationStore;
      return yield* capture(
        store
          .append(request.request)
          .pipe(Effect.map((result) => StoreAppendResult.make({ result }))),
      );
    }
    case "StoreReadPage": {
      const store = yield* ConversationStore;
      return yield* capture(
        store.read(request.request).pipe(
          Stream.runCollect,
          Effect.map((records) => StoreReadPageResult.make({ records: [...records] })),
        ),
      );
    }
    case "StoreInspectTail": {
      const store = yield* ConversationStore;
      return yield* capture(
        store
          .inspectTail(request.request)
          .pipe(Effect.map((tail) => StoreInspectTailResult.make({ tail }))),
      );
    }
    case "StoreExport": {
      const store = yield* ConversationStore;
      return yield* capture(
        store
          .export(request.request)
          .pipe(
            Effect.map((conversationExport) =>
              StoreExportResult.make({ export: conversationExport }),
            ),
          ),
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
  function* (
    encoded: unknown,
  ): Effect.fn.Return<unknown, never, SubmissionLedger | ConversationStore> {
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

import {
  AbortCommand,
  AbortIntent,
  AdmissionConflict,
  AdmissionRequest,
  AdmissionResolution,
  AdmissionResult,
  AppendConflict,
  AppendResult,
  CanonicalSettlementRepair,
  CanonicalRecordEnvelope,
  ChildSettledNotification,
  ChildSettledOutcome,
  ConversationExport,
  ConversationExportRequest,
  ConversationMaterialization,
  ConversationNotMaterialized,
  ConversationRead,
  ConversationStoreError,
  ConversationTail,
  ConversationTailRequest,
  FenceRejected,
  FencedAppendRequest,
  JoinedToHost,
  LedgerError,
  MarkReadyRequest,
  ResumeSuspensionRequest,
  Settlement,
  SettlementConflict,
  SubmissionLookup,
  SubmissionLookupByKey,
  SubmissionSnapshot,
  SuspensionResumeOutcome,
} from "@effect-agent/session";
import { Effect, Schema } from "effect";

/**
 * The cross-Durable-Object port protocol (plan §1.3, D-P6-3): Schema request/response/error
 * envelopes for the CLOSED route-capable subset of the session ports. One Conversation's
 * Durable Object executes another Conversation's request against its OWN local facets; the
 * envelopes here are the only values that cross the Object boundary, and they are
 * transport-agnostic — native Durable Object JS RPC is the shipped carrier, fetch-with-JSON
 * the documented fallback, and both move the same Schema-encoded JSON.
 *
 * The closed subset is exactly the set of operations the durable coordinator performs against
 * a FOREIGN Conversation (parent/child establishment, status checks, abort propagation,
 * child-settlement notification, and the child-conversation store operations used by
 * establishment, `verifySettledChild`, and result projection):
 *
 * - ledger: `admit`, `markReady`, `lookup`, `resolveAdmission`, `requestAbort`,
 *   `recordChildSettled`, `repairSettlementFromCanonical`, `resumeSuspension`;
 * - store: `materialize`, `append`, `read` (one page), `inspectTail`, `export`.
 *
 * Every other port operation is lane-local by construction and is NOT given an envelope:
 * honesty over accidental distribution — the routing layer fails such calls fast and typed
 * instead of quietly widening the distributed surface.
 *
 * Failures cross the boundary as the `PortFailure` union and re-decode on the caller side to
 * the SAME tagged error types the local facet would have produced, so routed calls keep
 * error-tag fidelity. `cause` chains inside `LedgerError`/`ConversationStoreError` travel as
 * Schema defects and do not claim instance fidelity across Objects (plan §2.8).
 */

/** Ceiling for protocol diagnostic strings; matches `AdmissionIndeterminate.reason`. */
export const MAX_PORT_DIAGNOSTIC_LENGTH = 4_096;

const BoundedDiagnostic = Schema.String.check(Schema.isMaxLength(MAX_PORT_DIAGNOSTIC_LENGTH));

/** Truncate a diagnostic string to the protocol's bounded diagnostic length. */
export const boundPortDiagnostic = (value: string): string =>
  value.length > MAX_PORT_DIAGNOSTIC_LENGTH
    ? `${value.slice(0, MAX_PORT_DIAGNOSTIC_LENGTH - 3)}...`
    : value;

/**
 * The envelope itself could not be honored: the receiving Object could not decode the
 * request, or a response could not be encoded/decoded. It never carries port semantics —
 * callers fold it into the operation's base error (`LedgerError`/`ConversationStoreError`),
 * except `resolveAdmission`, which folds it into `AdmissionIndeterminate` because a
 * non-answer is never proof of absence (SUB-031).
 */
export class PortProtocolError extends Schema.TaggedError<PortProtocolError>()(
  "PortProtocolError",
  {
    message: BoundedDiagnostic,
  },
) {}

// ---------------------------------------------------------------------------
// Requests
// ---------------------------------------------------------------------------

/** Routed `SubmissionLedger.admit` — child establishment admits INTO the owning Object. */
export class LedgerAdmitCall extends Schema.TaggedClass<LedgerAdmitCall>(
  "@effect-agent/storage-cloudflare/LedgerAdmitCall",
)("LedgerAdmit", {
  request: AdmissionRequest,
}) {}

/** Routed `SubmissionLedger.markReady` for a Submission owned by another Object. */
export class LedgerMarkReadyCall extends Schema.TaggedClass<LedgerMarkReadyCall>(
  "@effect-agent/storage-cloudflare/LedgerMarkReadyCall",
)("LedgerMarkReady", {
  request: MarkReadyRequest,
}) {}

/** Routed `SubmissionLedger.lookup` (by identity or scoped idempotency key). */
export class LedgerLookupCall extends Schema.TaggedClass<LedgerLookupCall>(
  "@effect-agent/storage-cloudflare/LedgerLookupCall",
)("LedgerLookup", {
  request: SubmissionLookup,
}) {}

/** Routed `SubmissionLedger.resolveAdmission` — the SUB-031 tri-state authority call. */
export class LedgerResolveAdmissionCall extends Schema.TaggedClass<LedgerResolveAdmissionCall>(
  "@effect-agent/storage-cloudflare/LedgerResolveAdmissionCall",
)("LedgerResolveAdmission", {
  request: SubmissionLookupByKey,
}) {}

/** Routed `SubmissionLedger.requestAbort` — abort propagation across Objects. */
export class LedgerRequestAbortCall extends Schema.TaggedClass<LedgerRequestAbortCall>(
  "@effect-agent/storage-cloudflare/LedgerRequestAbortCall",
)("LedgerRequestAbort", {
  request: AbortCommand,
}) {}

/** Routed `SubmissionLedger.recordChildSettled` — the child→parent durable notification. */
export class LedgerRecordChildSettledCall extends Schema.TaggedClass<LedgerRecordChildSettledCall>(
  "@effect-agent/storage-cloudflare/LedgerRecordChildSettledCall",
)("LedgerRecordChildSettled", {
  request: ChildSettledNotification,
}) {}

/** Routed canonical repair when recovery targets a Submission owned by another Object. */
export class LedgerRepairSettlementCall extends Schema.TaggedClass<LedgerRepairSettlementCall>(
  "@effect-agent/storage-cloudflare/LedgerRepairSettlementCall",
)("LedgerRepairSettlement", {
  request: CanonicalSettlementRepair,
}) {}

/** Routed canonical-evidence-authorized suspension wake. */
export class LedgerResumeSuspensionCall extends Schema.TaggedClass<LedgerResumeSuspensionCall>(
  "@effect-agent/storage-cloudflare/LedgerResumeSuspensionCall",
)("LedgerResumeSuspension", {
  request: ResumeSuspensionRequest,
}) {}

/** Routed `ConversationStore.materialize` against the owning Object. */
export class StoreMaterializeCall extends Schema.TaggedClass<StoreMaterializeCall>(
  "@effect-agent/storage-cloudflare/StoreMaterializeCall",
)("StoreMaterialize", {
  request: ConversationMaterialization,
}) {}

/** Routed `ConversationStore.append` against the owning Object. */
export class StoreAppendCall extends Schema.TaggedClass<StoreAppendCall>(
  "@effect-agent/storage-cloudflare/StoreAppendCall",
)("StoreAppend", {
  request: FencedAppendRequest,
}) {}

/** Routed one-page `ConversationStore.read`; the page bound is the request's own `limit`. */
export class StoreReadPageCall extends Schema.TaggedClass<StoreReadPageCall>(
  "@effect-agent/storage-cloudflare/StoreReadPageCall",
)("StoreReadPage", {
  request: ConversationRead,
}) {}

/** Routed `ConversationStore.inspectTail` against the owning Object. */
export class StoreInspectTailCall extends Schema.TaggedClass<StoreInspectTailCall>(
  "@effect-agent/storage-cloudflare/StoreInspectTailCall",
)("StoreInspectTail", {
  request: ConversationTailRequest,
}) {}

/** Routed `ConversationStore.export` against the owning Object. */
export class StoreExportCall extends Schema.TaggedClass<StoreExportCall>(
  "@effect-agent/storage-cloudflare/StoreExportCall",
)("StoreExport", {
  request: ConversationExportRequest,
}) {}

/** Every request that may cross a Durable Object boundary — the CLOSED route-capable subset. */
export const PortRequest = Schema.Union([
  LedgerAdmitCall,
  LedgerMarkReadyCall,
  LedgerLookupCall,
  LedgerResolveAdmissionCall,
  LedgerRequestAbortCall,
  LedgerRecordChildSettledCall,
  LedgerRepairSettlementCall,
  LedgerResumeSuspensionCall,
  StoreMaterializeCall,
  StoreAppendCall,
  StoreReadPageCall,
  StoreInspectTailCall,
  StoreExportCall,
]);
export type PortRequest = typeof PortRequest.Type;

/** The wire form of one port request (what a transport actually carries). */
export type PortRequestEnvelope = typeof PortRequest.Encoded;

/**
 * Whether executing a decoded request durably mutates the owning Object. The exhaustive switch
 * keeps the alarm pre-arm classification coupled to the closed protocol union: adding an
 * operation without classifying it is a compile error in this protocol owner.
 */
export const portRequestMutates = (request: PortRequest): boolean => {
  switch (request._tag) {
    case "LedgerAdmit":
    case "LedgerMarkReady":
    case "LedgerRequestAbort":
    case "LedgerRecordChildSettled":
    case "LedgerRepairSettlement":
    case "LedgerResumeSuspension":
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
};

// ---------------------------------------------------------------------------
// Results
// ---------------------------------------------------------------------------

export class LedgerAdmitResult extends Schema.TaggedClass<LedgerAdmitResult>(
  "@effect-agent/storage-cloudflare/LedgerAdmitResult",
)("LedgerAdmitResult", {
  result: AdmissionResult,
}) {}

export class LedgerMarkReadyResult extends Schema.TaggedClass<LedgerMarkReadyResult>(
  "@effect-agent/storage-cloudflare/LedgerMarkReadyResult",
)("LedgerMarkReadyResult", {}) {}

/** `submission` is absent exactly when the lookup answered `Option.none`. */
export class LedgerLookupResult extends Schema.TaggedClass<LedgerLookupResult>(
  "@effect-agent/storage-cloudflare/LedgerLookupResult",
)("LedgerLookupResult", {
  submission: Schema.optionalKey(SubmissionSnapshot),
}) {}

export class LedgerResolveAdmissionResult extends Schema.TaggedClass<LedgerResolveAdmissionResult>(
  "@effect-agent/storage-cloudflare/LedgerResolveAdmissionResult",
)("LedgerResolveAdmissionResult", {
  resolution: AdmissionResolution,
}) {}

export class LedgerRequestAbortResult extends Schema.TaggedClass<LedgerRequestAbortResult>(
  "@effect-agent/storage-cloudflare/LedgerRequestAbortResult",
)("LedgerRequestAbortResult", {
  intent: AbortIntent,
}) {}

export class LedgerRecordChildSettledResult extends Schema.TaggedClass<LedgerRecordChildSettledResult>(
  "@effect-agent/storage-cloudflare/LedgerRecordChildSettledResult",
)("LedgerRecordChildSettledResult", {
  outcome: ChildSettledOutcome,
}) {}

export class LedgerRepairSettlementResult extends Schema.TaggedClass<LedgerRepairSettlementResult>(
  "@effect-agent/storage-cloudflare/LedgerRepairSettlementResult",
)("LedgerRepairSettlementResult", {
  settlement: Settlement,
}) {}

export class LedgerResumeSuspensionResult extends Schema.TaggedClass<LedgerResumeSuspensionResult>(
  "@effect-agent/storage-cloudflare/LedgerResumeSuspensionResult",
)("LedgerResumeSuspensionResult", {
  outcome: SuspensionResumeOutcome,
}) {}

export class StoreMaterializeResult extends Schema.TaggedClass<StoreMaterializeResult>(
  "@effect-agent/storage-cloudflare/StoreMaterializeResult",
)("StoreMaterializeResult", {}) {}

export class StoreAppendResult extends Schema.TaggedClass<StoreAppendResult>(
  "@effect-agent/storage-cloudflare/StoreAppendResult",
)("StoreAppendResult", {
  result: AppendResult,
}) {}

/** One page of canonical records, bounded by the request's `limit` (≤ 1,024). */
export class StoreReadPageResult extends Schema.TaggedClass<StoreReadPageResult>(
  "@effect-agent/storage-cloudflare/StoreReadPageResult",
)("StoreReadPageResult", {
  records: Schema.Array(CanonicalRecordEnvelope).check(Schema.isMaxLength(1_024)),
}) {}

export class StoreInspectTailResult extends Schema.TaggedClass<StoreInspectTailResult>(
  "@effect-agent/storage-cloudflare/StoreInspectTailResult",
)("StoreInspectTailResult", {
  tail: ConversationTail,
}) {}

export class StoreExportResult extends Schema.TaggedClass<StoreExportResult>(
  "@effect-agent/storage-cloudflare/StoreExportResult",
)("StoreExportResult", {
  export: ConversationExport,
}) {}

/** Every successful routed result. Callers narrow by the tag their request implies. */
export const PortResult = Schema.Union([
  LedgerAdmitResult,
  LedgerMarkReadyResult,
  LedgerLookupResult,
  LedgerResolveAdmissionResult,
  LedgerRequestAbortResult,
  LedgerRecordChildSettledResult,
  LedgerRepairSettlementResult,
  LedgerResumeSuspensionResult,
  StoreMaterializeResult,
  StoreAppendResult,
  StoreReadPageResult,
  StoreInspectTailResult,
  StoreExportResult,
]);
export type PortResult = typeof PortResult.Type;

// ---------------------------------------------------------------------------
// Failures and the response envelope
// ---------------------------------------------------------------------------

/**
 * Every typed failure a route-capable operation can produce on its owning Object, plus the
 * protocol's own `PortProtocolError`. Members re-decode to the SAME tagged classes the
 * session ports declare, so a routed caller observes identical error tags and fields.
 */
export const PortFailure = Schema.Union([
  AdmissionConflict,
  SettlementConflict,
  JoinedToHost,
  LedgerError,
  ConversationStoreError,
  ConversationNotMaterialized,
  AppendConflict,
  FenceRejected,
  PortProtocolError,
]);
export type PortFailure = typeof PortFailure.Type;

/** The routed operation succeeded on its owning Object. */
export class PortSucceeded extends Schema.TaggedClass<PortSucceeded>(
  "@effect-agent/storage-cloudflare/PortSucceeded",
)("PortSucceeded", {
  result: PortResult,
}) {}

/** The routed operation failed TYPED on its owning Object; the failure re-decodes verbatim. */
export class PortFailed extends Schema.TaggedClass<PortFailed>(
  "@effect-agent/storage-cloudflare/PortFailed",
)("PortFailed", {
  failure: PortFailure,
}) {}

/** The uniform answer of one `portCall`: op-specific success or a re-decodable typed failure. */
export const PortResponse = Schema.Union([PortSucceeded, PortFailed]);
export type PortResponse = typeof PortResponse.Type;

/** The wire form of one port response (what a transport actually carries). */
export type PortResponseEnvelope = typeof PortResponse.Encoded;

// ---------------------------------------------------------------------------
// Codecs
// ---------------------------------------------------------------------------

export const encodePortRequest = Schema.encodeEffect(PortRequest);
export const decodePortRequest = Schema.decodeUnknownEffect(PortRequest);
export const decodePortRequestMutation = Effect.fn("DoPortProtocol.decodePortRequestMutation")(
  (encoded: unknown) => decodePortRequest(encoded).pipe(Effect.map(portRequestMutates)),
);
export const encodePortResponse = Schema.encodeEffect(PortResponse);
export const decodePortResponse = Schema.decodeUnknownEffect(PortResponse);

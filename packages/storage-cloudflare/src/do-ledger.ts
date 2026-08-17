import {
  AbortCommand,
  AbortIntent,
  AdmissionAdmitted,
  AdmissionConflict,
  AdmissionNotAdmitted,
  AdmissionRequest,
  AdmissionResult,
  ApprovalConflict,
  ApprovalDecision,
  ApprovalDecisionCommand,
  ApprovalDecisionIntent,
  AttachChildToReservationRequest,
  BeginChildBudgetReleaseRequest,
  CanonicalSettlementRepair,
  CanonicalSequence,
  ChildAttachmentSnapshot,
  ChildBudgetReservationRequest,
  ChildBudgetReservationSnapshot,
  ChildReservationConflict,
  ChildReservationStatus,
  ChildSettledNotification,
  Claim,
  ClaimJoiningRequest,
  ClaimRequest,
  DefinitionDigests,
  Digest,
  EMPTY_TAIL_DIGEST,
  InputAppliedMarker,
  JoinSnapshot,
  JoinedToHost,
  JoiningClaim,
  LedgerCapabilities,
  LedgerError,
  MarkInputAppliedRequest,
  MarkJoinedRequest,
  MarkReadyRequest,
  MarkUnknownRequest,
  OwnershipLost,
  OwnershipRenewal,
  OwnershipSnapshot,
  ParentLinkage,
  PersistedJson,
  ProducerEpoch,
  QueueSequence,
  RecordEnvelope,
  RecoverySnapshot,
  RecoverySnapshotRequest,
  ScanConversationNonterminalRequest,
  ReleaseChildBudgetRequest,
  ReleaseOwnershipRequest,
  ResumeSuspensionRequest,
  RenewOwnershipRequest,
  ReservedChildBudget,
  ReservedSettlement,
  RevertJoiningRequest,
  Settlement,
  SettlementConflict,
  SettlementFinalization,
  SettlementOutcome,
  SettlementReservation,
  SubmissionLedger,
  SubmissionLookup,
  SubmissionLookupByKey,
  SubmissionSnapshot,
  SubmissionState,
  SettlementReservationSnapshot,
  SuspendRequest,
  SuspensionReason,
  SuspensionSnapshot,
  UnknownResolution,
  UnknownResolutionCommand,
  UnknownResolutionConflict,
  UnknownResolutionIntent,
  validateCanonicalSettlementRepair,
  isCanonicalSettlementRepairOperationalFailure,
  submissionAbortRecordId,
  type ChildSettledOutcome,
  type SuspensionOutcome,
  type SuspensionResumeOutcome,
} from "@effect-agent/session";
import { BrowserCrypto } from "@effect/platform-browser";
import { SqliteClient } from "@effect/sql-sqlite-do";
import { Clock, Context, Crypto, DateTime, Effect, Layer, Option, Schema, Stream } from "effect";
import * as SqlClientService from "effect/unstable/sql/SqlClient";
import type { SqlError } from "effect/unstable/sql/SqlError";

import {
  storageConfigLayer,
  storageFailpointLayer,
  type DoStorageInitializationError,
  type DoStorageOptions,
} from "./do-conversation-store.ts";
import { decodeRows, initializeDoJournal } from "./do-journal.ts";
import { DoStorageConfig } from "./do-storage-config.ts";
import { DoStorageFailpoint } from "./do-storage-failpoint.ts";
import {
  DoLedgerError,
  DoStorageCorruptionError,
  DoStorageError,
  type DoStorageFailpointLocation,
} from "./errors.ts";

type SubmissionId = SubmissionSnapshot["submissionId"];

/**
 * Static decode-side ceiling; writes are bounded in bytes by the configured
 * `maxStoredValueBytes` (see do-journal.ts).
 */
const BoundedStoredText = Schema.String.check(Schema.isMaxLength(2_000_000));
const BoundedIdentifier = Schema.NonEmptyString.check(Schema.isMaxLength(1024));
const BoundedTimestamp = Schema.NonEmptyString.check(Schema.isMaxLength(128));

const SCAN_PAGE_SIZE = 256;
const EPOCH_ZERO = Schema.decodeSync(ProducerEpoch)(0);
const MAX_IDENTIFIER_LENGTH = 1_024;

class SubmissionRow extends Schema.Class<SubmissionRow>("SubmissionRow")({
  submission_id: BoundedIdentifier,
  conversation_id: BoundedIdentifier,
  queue_sequence: QueueSequence,
  principal: BoundedIdentifier,
  idempotency_key: BoundedIdentifier,
  agent_id: BoundedIdentifier,
  agent_digests_json: BoundedStoredText,
  deployment_id: BoundedIdentifier,
  input_json: BoundedStoredText,
  input_digest: Digest,
  receipt_id: BoundedIdentifier,
  state: SubmissionState,
  settled_outcome: Schema.NullOr(SettlementOutcome),
  created_at: BoundedTimestamp,
  ready_at: Schema.NullOr(BoundedTimestamp),
  input_applied_record_id: Schema.NullOr(BoundedIdentifier),
  input_applied_sequence: Schema.NullOr(CanonicalSequence),
  joined_host_submission_id: Schema.NullOr(BoundedIdentifier),
  suspended_reason_json: Schema.NullOr(BoundedStoredText),
  suspended_at: Schema.NullOr(BoundedTimestamp),
  unknown_reason: Schema.NullOr(BoundedStoredText),
  unknown_tool_call_ids_json: Schema.NullOr(BoundedStoredText),
  parent_submission_id: Schema.NullOr(BoundedIdentifier),
  parent_tool_call_id: Schema.NullOr(BoundedIdentifier),
}) {}

const ValidSubmissionRow = SubmissionRow.pipe(
  Schema.refine(
    (row): row is SubmissionRow =>
      (row.input_applied_record_id === null) === (row.input_applied_sequence === null) &&
      (row.suspended_reason_json === null) === (row.suspended_at === null),
    {
      expected:
        "input marker record/sequence and suspension reason/timestamp must each be both present or both absent",
    },
  ),
);

class ChildReservationRow extends Schema.Class<ChildReservationRow>("ChildReservationRow")({
  reservation_id: BoundedIdentifier,
  parent_submission_id: BoundedIdentifier,
  parent_tool_call_id: BoundedIdentifier,
  child_submission_id: Schema.NullOr(BoundedIdentifier),
  status: ChildReservationStatus,
  allocation_json: BoundedStoredText,
  allocation_digest: Digest,
  accounting_json: Schema.NullOr(BoundedStoredText),
  reserved_at: BoundedTimestamp,
  release_began_at: Schema.NullOr(BoundedTimestamp),
  released_at: Schema.NullOr(BoundedTimestamp),
}) {}

class ChildSettlementMarkerRow extends Schema.Class<ChildSettlementMarkerRow>(
  "ChildSettlementMarkerRow",
)({
  parent_submission_id: BoundedIdentifier,
  child_submission_id: BoundedIdentifier,
  child_outcome: Schema.NullOr(SettlementOutcome),
  recorded_at: BoundedTimestamp,
}) {}

class ApprovalDecisionRow extends Schema.Class<ApprovalDecisionRow>("ApprovalDecisionRow")({
  submission_id: BoundedIdentifier,
  tool_call_id: BoundedIdentifier,
  decision: ApprovalDecision,
  resolver: BoundedIdentifier,
  reason: BoundedStoredText,
  decided_at: BoundedTimestamp,
}) {}

class UnknownResolutionRow extends Schema.Class<UnknownResolutionRow>("UnknownResolutionRow")({
  submission_id: BoundedIdentifier,
  tool_call_id: BoundedIdentifier,
  author: BoundedIdentifier,
  reason: BoundedStoredText,
  resolution_json: BoundedStoredText,
  resolved_at: BoundedTimestamp,
}) {}

class OwnershipRow extends Schema.Class<OwnershipRow>("OwnershipRow")({
  submission_id: BoundedIdentifier,
  attempt_id: BoundedIdentifier,
  ownership_token: BoundedIdentifier,
  producer_epoch: ProducerEpoch,
  owner_producer_id: BoundedIdentifier,
  lease_expires_at: BoundedTimestamp,
}) {}

class ReservationRow extends Schema.Class<ReservationRow>("ReservationRow")({
  submission_id: BoundedIdentifier,
  settlement_id: BoundedIdentifier,
  outcome: SettlementOutcome,
  record_id: BoundedIdentifier,
  record_json: BoundedStoredText,
  record_digest: Digest,
  reserved_at: BoundedTimestamp,
  finalized_at: Schema.NullOr(BoundedTimestamp),
}) {}

class AbortIntentRow extends Schema.Class<AbortIntentRow>("AbortIntentRow")({
  submission_id: BoundedIdentifier,
  author: BoundedIdentifier,
  reason: BoundedStoredText,
  requested_at: BoundedTimestamp,
  canonical_record_id: Schema.NullOr(BoundedIdentifier),
}) {}

class MaxQueueSequenceRow extends Schema.Class<MaxQueueSequenceRow>("MaxQueueSequenceRow")({
  max_queue_sequence: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
}) {}

class CanonicalRecordIdRow extends Schema.Class<CanonicalRecordIdRow>("CanonicalRecordIdRow")({
  record_id: BoundedIdentifier,
}) {}

const SUBMISSION_COLUMNS = `
  submission_id,
  conversation_id,
  queue_sequence,
  principal,
  idempotency_key,
  agent_id,
  agent_digests_json,
  deployment_id,
  input_json,
  input_digest,
  receipt_id,
  state,
  settled_outcome,
  created_at,
  ready_at,
  input_applied_record_id,
  input_applied_sequence,
  joined_host_submission_id,
  suspended_reason_json,
  suspended_at,
  unknown_reason,
  unknown_tool_call_ids_json,
  parent_submission_id,
  parent_tool_call_id
`;

const CHILD_RESERVATION_COLUMNS = `
  reservation_id,
  parent_submission_id,
  parent_tool_call_id,
  child_submission_id,
  status,
  allocation_json,
  allocation_digest,
  accounting_json,
  reserved_at,
  release_began_at,
  released_at
`;

/** The branded ToolCallId schema, reached through the session port so no core import is needed. */
const ToolCallIdSchema = ApprovalDecisionCommand.fields.toolCallId;
const ToolCallIdList = Schema.Array(ToolCallIdSchema);

const encodePersistedJsonText = Schema.encodeEffect(Schema.fromJsonString(PersistedJson));
const encodeDefinitionDigestsText = Schema.encodeEffect(Schema.fromJsonString(DefinitionDigests));
const encodeRecordEnvelopeText = Schema.encodeEffect(Schema.fromJsonString(RecordEnvelope));
const decodeRecordEnvelopeText = Schema.decodeEffect(Schema.fromJsonString(RecordEnvelope));
const encodeSuspensionReasonText = Schema.encodeEffect(Schema.fromJsonString(SuspensionReason));
const encodeUnknownResolutionText = Schema.encodeEffect(Schema.fromJsonString(UnknownResolution));
const encodeToolCallIdsText = Schema.encodeEffect(Schema.fromJsonString(ToolCallIdList));
const decodeToolCallIdsText = Schema.decodeEffect(Schema.fromJsonString(ToolCallIdList));
const parseStoredJsonText = Schema.decodeEffect(Schema.fromJsonString(Schema.Json));
const decodeAdmissionResult = Schema.decodeUnknownEffect(AdmissionResult);
const decodeClaim = Schema.decodeUnknownEffect(Claim);
const decodeOwnershipRenewal = Schema.decodeUnknownEffect(OwnershipRenewal);
const decodeSettlement = Schema.decodeUnknownEffect(Settlement);
const decodeAbortIntent = Schema.decodeUnknownEffect(AbortIntent);
const decodeOwnershipSnapshot = Schema.decodeUnknownEffect(OwnershipSnapshot);
const decodeInputAppliedMarker = Schema.decodeUnknownEffect(InputAppliedMarker);
const decodeSubmissionSnapshotUnknown = Schema.decodeUnknownEffect(SubmissionSnapshot);
const decodeSubmissionId = Schema.decodeUnknownEffect(SubmissionSnapshot.fields.submissionId);
const decodeReceiptId = Schema.decodeUnknownEffect(SubmissionSnapshot.fields.receiptId);
const decodeQueueSequence = Schema.decodeUnknownEffect(QueueSequence);
const decodeUtcInstant = Schema.decodeUnknownEffect(Schema.DateTimeUtcFromString);
const decodeJoiningClaim = Schema.decodeUnknownEffect(JoiningClaim);
const decodeJoinSnapshot = Schema.decodeUnknownEffect(JoinSnapshot);
const decodeSuspensionSnapshot = Schema.decodeUnknownEffect(SuspensionSnapshot);
const decodeApprovalDecisionIntent = Schema.decodeUnknownEffect(ApprovalDecisionIntent);
const decodeUnknownResolutionIntent = Schema.decodeUnknownEffect(UnknownResolutionIntent);
const decodeParentLinkage = Schema.decodeUnknownEffect(ParentLinkage);
const decodeChildReservationSnapshotUnknown = Schema.decodeUnknownEffect(
  ChildBudgetReservationSnapshot,
);
const decodeChildAttachmentSnapshot = Schema.decodeUnknownEffect(ChildAttachmentSnapshot);

/** Wrap an adapter-internal failure into the port's LedgerError without erasing its tag. */
const internalFailure =
  (operation: string) =>
  (error: { readonly message: string }): LedgerError =>
    LedgerError.make({ operation, message: error.message, cause: error });

/**
 * Classify raw SQL failures. Within one Durable Object there is exactly one writer, so the
 * Node adapter's retryable `SqliteWriteContention` classification has no analogue: every raw
 * failure is a `DoLedgerError` preserved as the LedgerError's cause.
 */
const sqlFailure =
  (operation: string) =>
  (error: SqlError): LedgerError =>
    internalFailure(operation)(
      DoLedgerError.make({
        cause: error,
        operation,
        message: error.message,
      }),
    );

const corruptionFailure = (operation: string, table: string, rowKey: string, message: string) =>
  internalFailure(operation)(DoStorageCorruptionError.make({ table, rowKey, message }));

const makeServices = Effect.fn("DoSubmissionLedger.makeServices")(function* () {
  const config = yield* DoStorageConfig;
  const failpoint = yield* DoStorageFailpoint;
  const sql = yield* SqlClientService.SqlClient;
  const crypto = yield* Crypto.Crypto;
  // Close Crypto once at adapter construction; business operations call this boundary function
  // without providing hidden services themselves.
  const validateCanonicalSettlement = (
    request: unknown,
    receiptId: SubmissionSnapshot["receiptId"],
  ) =>
    validateCanonicalSettlementRepair(request, receiptId).pipe(
      Effect.provideService(Crypto.Crypto, crypto),
    );
  const journal = yield* initializeDoJournal(sql, failpoint.hit, config.maxStoredValueBytes);

  const hitFailpoint = (
    location: DoStorageFailpointLocation,
    operation: string,
  ): Effect.Effect<void, LedgerError> =>
    failpoint.hit(location).pipe(Effect.mapError((error) => internalFailure(operation)(error)));

  /**
   * Run one ledger mutation under the journal's Durable Object storage-backed transaction so
   * ownership-token and epoch checks are atomic with their writes (DUR-006). Transaction
   * failures surface as LedgerError carrying the typed `DoStorageError` as cause.
   */
  const inWriteTransaction = <
    A,
    E extends
      | AdmissionConflict
      | ApprovalConflict
      | ChildReservationConflict
      | JoinedToHost
      | OwnershipLost
      | SettlementConflict
      | UnknownResolutionConflict
      | LedgerError,
  >(
    operation: string,
    effect: Effect.Effect<A, E>,
  ): Effect.Effect<A, E | LedgerError> =>
    journal
      .withWriteTransaction(operation)(effect)
      .pipe(
        Effect.mapError((error) =>
          error instanceof DoStorageError ? internalFailure(operation)(error) : error,
        ),
      );

  const mintUuid = (operation: string): Effect.Effect<string, LedgerError> =>
    crypto.randomUUIDv7.pipe(Effect.mapError((error) => internalFailure(operation)(error)));

  const currentInstant = Effect.map(Clock.currentTimeMillis, (millis) => ({
    millis,
    iso: new Date(millis).toISOString(),
  }));

  const timestampMillis = (operation: string, rowKey: string) => (timestamp: string) =>
    decodeUtcInstant(timestamp).pipe(
      Effect.map(DateTime.toEpochMillis),
      Effect.mapError((error) =>
        corruptionFailure(operation, "effect_agent_submission_ownership", rowKey, error.message),
      ),
    );

  const decodeSubmissionRows = (operation: string, rowKey: string, rows: unknown) =>
    decodeRows(Schema.Array(ValidSubmissionRow), "effect_agent_submissions", rowKey, rows).pipe(
      Effect.mapError(internalFailure(operation)),
    );

  const readSubmission = Effect.fn("DoSubmissionLedger.readSubmission")(function* (
    operation: string,
    submissionId: string,
  ): Effect.fn.Return<Option.Option<SubmissionRow>, LedgerError> {
    const rows = yield* sql<Record<string, unknown>>`
      SELECT ${sql.literal(SUBMISSION_COLUMNS)}
      FROM effect_agent_submissions
      WHERE submission_id = ${submissionId}
    `.pipe(Effect.mapError(sqlFailure(operation)));
    const decoded = yield* decodeSubmissionRows(operation, submissionId, rows);
    if (decoded.length > 1) {
      return yield* corruptionFailure(
        operation,
        "effect_agent_submissions",
        submissionId,
        "A submission primary key returned more than one row.",
      );
    }
    return decoded.length === 0 ? Option.none() : Option.some(decoded[0]);
  });

  const requireSubmission = Effect.fn("DoSubmissionLedger.requireSubmission")(function* (
    operation: string,
    submissionId: string,
  ): Effect.fn.Return<SubmissionRow, LedgerError> {
    const submission = yield* readSubmission(operation, submissionId);
    if (Option.isNone(submission)) {
      return yield* LedgerError.make({
        operation,
        message: `Unknown submission ${submissionId}.`,
      });
    }
    return submission.value;
  });

  const readOwnership = Effect.fn("DoSubmissionLedger.readOwnership")(function* (
    operation: string,
    submissionId: string,
  ): Effect.fn.Return<Option.Option<OwnershipRow>, LedgerError> {
    const rows = yield* sql<Record<string, unknown>>`
      SELECT
        submission_id,
        attempt_id,
        ownership_token,
        producer_epoch,
        owner_producer_id,
        lease_expires_at
      FROM effect_agent_submission_ownership
      WHERE submission_id = ${submissionId}
    `.pipe(Effect.mapError(sqlFailure(operation)));
    const decoded = yield* decodeRows(
      Schema.Array(OwnershipRow),
      "effect_agent_submission_ownership",
      submissionId,
      rows,
    ).pipe(Effect.mapError(internalFailure(operation)));
    if (decoded.length > 1) {
      return yield* corruptionFailure(
        operation,
        "effect_agent_submission_ownership",
        submissionId,
        "An ownership primary key returned more than one row.",
      );
    }
    return decoded.length === 0 ? Option.none() : Option.some(decoded[0]);
  });

  const conversationEpoch = Effect.fn("DoSubmissionLedger.conversationEpoch")(function* (
    operation: string,
    conversationId: string,
  ): Effect.fn.Return<ProducerEpoch, LedgerError> {
    const conversations = yield* journal
      .getConversation(conversationId)
      .pipe(Effect.mapError(internalFailure(operation)));
    return conversations.length === 0 ? EPOCH_ZERO : conversations[0].producer_epoch;
  });

  /**
   * Verify inside the surrounding write transaction that the presented token still owns the
   * Submission's lane; a superseded or missing token fails with OwnershipLost carrying the
   * Conversation's current producer epoch (DUR-006).
   */
  const requireOwnership = Effect.fn("DoSubmissionLedger.requireOwnership")(function* (
    operation: string,
    submission: SubmissionRow,
    ownershipToken: string,
  ): Effect.fn.Return<OwnershipRow, OwnershipLost | LedgerError> {
    const ownership = yield* readOwnership(operation, submission.submission_id);
    if (Option.isNone(ownership) || ownership.value.ownership_token !== ownershipToken) {
      const actualEpoch = yield* conversationEpoch(operation, submission.conversation_id);
      const submissionId = yield* Schema.decodeUnknownEffect(
        SubmissionSnapshot.fields.submissionId,
      )(submission.submission_id).pipe(Effect.mapError(internalFailure(operation)));
      return yield* OwnershipLost.make({ submissionId, actualEpoch });
    }
    return ownership.value;
  });

  const decodeSubmissionSnapshot = Effect.fn("DoSubmissionLedger.decodeSubmissionSnapshot")(
    function* (
      operation: string,
      row: SubmissionRow,
    ): Effect.fn.Return<SubmissionSnapshot, LedgerError> {
      const agentDigests = yield* parseStoredJsonText(row.agent_digests_json).pipe(
        Effect.mapError((error) =>
          corruptionFailure(
            operation,
            "effect_agent_submissions",
            row.submission_id,
            error.message,
          ),
        ),
      );
      const inputPayload = yield* parseStoredJsonText(row.input_json).pipe(
        Effect.mapError((error) =>
          corruptionFailure(
            operation,
            "effect_agent_submissions",
            row.submission_id,
            error.message,
          ),
        ),
      );
      if ((row.parent_submission_id === null) !== (row.parent_tool_call_id === null)) {
        return yield* corruptionFailure(
          operation,
          "effect_agent_submissions",
          row.submission_id,
          "A parent linkage must record both the parent Submission and the parent Tool Call.",
        );
      }
      return yield* decodeSubmissionSnapshotUnknown({
        submissionId: row.submission_id,
        conversationId: row.conversation_id,
        queueSequence: row.queue_sequence,
        principal: row.principal,
        idempotencyKey: row.idempotency_key,
        agentId: row.agent_id,
        agentDigests,
        deploymentId: row.deployment_id,
        inputPayload,
        inputDigest: row.input_digest,
        receiptId: row.receipt_id,
        state: row.state,
        createdAt: row.created_at,
        ...(row.settled_outcome === null ? {} : { settledOutcome: row.settled_outcome }),
        ...(row.ready_at === null ? {} : { readyAt: row.ready_at }),
        ...(row.parent_submission_id === null || row.parent_tool_call_id === null
          ? {}
          : {
              parentLinkage: {
                parentSubmissionId: row.parent_submission_id,
                parentToolCallId: row.parent_tool_call_id,
              },
            }),
      }).pipe(
        Effect.mapError((error) =>
          corruptionFailure(
            operation,
            "effect_agent_submissions",
            row.submission_id,
            error.message,
          ),
        ),
      );
    },
  );

  const readReservation = Effect.fn("DoSubmissionLedger.readReservation")(function* (
    operation: string,
    submissionId: string,
  ): Effect.fn.Return<Option.Option<ReservationRow>, LedgerError> {
    const rows = yield* sql<Record<string, unknown>>`
      SELECT
        submission_id,
        settlement_id,
        outcome,
        record_id,
        record_json,
        record_digest,
        reserved_at,
        finalized_at
      FROM effect_agent_settlement_reservations
      WHERE submission_id = ${submissionId}
    `.pipe(Effect.mapError(sqlFailure(operation)));
    const decoded = yield* decodeRows(
      Schema.Array(ReservationRow),
      "effect_agent_settlement_reservations",
      submissionId,
      rows,
    ).pipe(Effect.mapError(internalFailure(operation)));
    if (decoded.length > 1) {
      return yield* corruptionFailure(
        operation,
        "effect_agent_settlement_reservations",
        submissionId,
        "A settlement reservation primary key returned more than one row.",
      );
    }
    return decoded.length === 0 ? Option.none() : Option.some(decoded[0]);
  });

  const validateStoredReservationSelfIntegrity = Effect.fn(
    "DoSubmissionLedger.validateStoredReservationSelfIntegrity",
  )(function* (operation: string, submission: SubmissionRow, row: ReservationRow) {
    const rowFailure = (message: string) =>
      corruptionFailure(
        operation,
        "effect_agent_settlement_reservations",
        submission.submission_id,
        message,
      );
    const record = yield* decodeRecordEnvelopeText(row.record_json).pipe(
      Effect.mapError((error) => rowFailure(error.message)),
    );
    yield* decodeUtcInstant(row.reserved_at).pipe(
      Effect.mapError((error) => rowFailure(error.message)),
    );
    if (row.finalized_at !== null) {
      yield* decodeUtcInstant(row.finalized_at).pipe(
        Effect.mapError((error) => rowFailure(error.message)),
      );
    }
    const submissionId = yield* decodeSubmissionId(submission.submission_id).pipe(
      Effect.mapError((error) => rowFailure(error.message)),
    );
    const receiptId = yield* decodeReceiptId(submission.receipt_id).pipe(
      Effect.mapError((error) => rowFailure(error.message)),
    );
    const canonical = yield* validateCanonicalSettlement(
      CanonicalSettlementRepair.make({
        submissionId,
        record,
        recordDigest: row.record_digest,
      }),
      receiptId,
    ).pipe(
      Effect.mapError((error) =>
        isCanonicalSettlementRepairOperationalFailure(error) ? error : rowFailure(error.message),
      ),
    );
    if (
      row.submission_id !== canonical.submissionId ||
      row.settlement_id !== canonical.settlementId ||
      row.outcome !== canonical.outcome ||
      row.record_id !== canonical.record.recordId
    ) {
      return yield* rowFailure(
        "Settlement reservation columns disagree with the exact canonical record.",
      );
    }
    return canonical;
  });

  const validateStoredReservation = Effect.fn("DoSubmissionLedger.validateStoredReservation")(
    function* (operation: string, submission: SubmissionRow, row: ReservationRow) {
      const canonical = yield* validateStoredReservationSelfIntegrity(operation, submission, row);
      if (
        (submission.state === "settled") !== (row.finalized_at !== null) ||
        (submission.state === "settled"
          ? submission.settled_outcome !== canonical.outcome
          : submission.settled_outcome !== null)
      ) {
        return yield* corruptionFailure(
          operation,
          "effect_agent_settlement_reservations",
          submission.submission_id,
          "Settlement reservation columns disagree with the Submission lifecycle projection.",
        );
      }
      return canonical;
    },
  );

  /** Recovery preserves repairability while classifying reservation self-integrity explicitly. */
  const readRecoveryReservation = Effect.fn("DoSubmissionLedger.readRecoveryReservation")(
    function* (operation: string, submission: SubmissionRow) {
      const rows = yield* sql<Record<string, unknown>>`
        SELECT
          submission_id,
          settlement_id,
          outcome,
          record_id,
          record_json,
          record_digest,
          reserved_at,
          finalized_at
        FROM effect_agent_settlement_reservations
        WHERE submission_id = ${submission.submission_id}
      `.pipe(Effect.mapError(sqlFailure(operation)));
      if (rows.length === 0) {
        return { reservationIntegrity: "absent" as const };
      }
      const decoded = yield* decodeRows(
        Schema.Array(ReservationRow),
        "effect_agent_settlement_reservations",
        submission.submission_id,
        rows,
      ).pipe(Effect.option);
      if (Option.isNone(decoded) || decoded.value.length !== 1) {
        return { reservationIntegrity: "invalid" as const };
      }
      const row = decoded.value[0];
      const canonical = yield* validateStoredReservationSelfIntegrity(
        operation,
        submission,
        row,
      ).pipe(
        Effect.map(Option.some),
        Effect.catch((error) =>
          isCanonicalSettlementRepairOperationalFailure(error)
            ? Effect.fail(
                LedgerError.make({ operation, message: error.message, cause: error.cause }),
              )
            : Effect.succeed(Option.none()),
        ),
      );
      if (Option.isNone(canonical)) {
        return { reservationIntegrity: "invalid" as const };
      }
      return {
        reservationIntegrity: "verified" as const,
        reservation: SettlementReservationSnapshot.make({
          settlementId: canonical.value.settlementId,
          outcome: canonical.value.outcome,
          record: canonical.value.record,
          recordDigest: canonical.value.recordDigest,
          finalized: row.finalized_at !== null,
        }),
      };
    },
  );

  const validateRequestedSettlement = Effect.fn("DoSubmissionLedger.validateRequestedSettlement")(
    function* (operation: string, submission: SubmissionRow, request: SettlementReservation) {
      const receiptId = yield* decodeReceiptId(submission.receipt_id).pipe(
        Effect.mapError(internalFailure(operation)),
      );
      const canonical = yield* validateCanonicalSettlement(
        CanonicalSettlementRepair.make({
          submissionId: request.submissionId,
          record: request.record,
          recordDigest: request.recordDigest,
        }),
        receiptId,
      );
      if (
        canonical.settlementId !== request.settlementId ||
        canonical.outcome !== request.outcome
      ) {
        return yield* LedgerError.make({
          operation,
          message: "Settlement reservation fields disagree with the exact canonical record.",
        });
      }
      return canonical;
    },
  );

  const readAbortIntent = Effect.fn("DoSubmissionLedger.readAbortIntent")(function* (
    operation: string,
    submissionId: string,
  ): Effect.fn.Return<Option.Option<AbortIntentRow>, LedgerError> {
    const rows = yield* sql<Record<string, unknown>>`
      SELECT
        submission_id,
        author,
        reason,
        requested_at,
        canonical_record_id
      FROM effect_agent_abort_intents
      WHERE submission_id = ${submissionId}
    `.pipe(Effect.mapError(sqlFailure(operation)));
    const decoded = yield* decodeRows(
      Schema.Array(AbortIntentRow),
      "effect_agent_abort_intents",
      submissionId,
      rows,
    ).pipe(Effect.mapError(internalFailure(operation)));
    if (decoded.length > 1) {
      return yield* corruptionFailure(
        operation,
        "effect_agent_abort_intents",
        submissionId,
        "An abort intent primary key returned more than one row.",
      );
    }
    return decoded.length === 0 ? Option.none() : Option.some(decoded[0]);
  });

  /**
   * Read the durable cross-store child-settlement markers recorded against one parent
   * Submission (the DC realization of the port's "cross-store adapters record a durable
   * notification marker" contract).
   */
  const readChildSettlementMarkers = Effect.fn("DoSubmissionLedger.readChildSettlementMarkers")(
    function* (
      operation: string,
      parentSubmissionId: string,
    ): Effect.fn.Return<ReadonlyArray<ChildSettlementMarkerRow>, LedgerError> {
      const rows = yield* sql<Record<string, unknown>>`
        SELECT
          parent_submission_id,
          child_submission_id,
          child_outcome,
          recorded_at
        FROM effect_agent_child_settlements
        WHERE parent_submission_id = ${parentSubmissionId}
        ORDER BY child_submission_id ASC
      `.pipe(Effect.mapError(sqlFailure(operation)));
      return yield* decodeRows(
        Schema.Array(ChildSettlementMarkerRow),
        "effect_agent_child_settlements",
        parentSubmissionId,
        rows,
      ).pipe(Effect.mapError(internalFailure(operation)));
    },
  );

  /**
   * Whether one listed child is provably settled from THIS store: either its own row lives
   * here and is settled (single-store evidence, identical to the Node adapter), or a durable
   * cross-store notification marker was recorded for it (the child's row lives in another
   * Durable Object and its owner reported the settlement through `recordChildSettled`).
   */
  const childProvablySettled = Effect.fn("DoSubmissionLedger.childProvablySettled")(function* (
    operation: string,
    markerChildren: ReadonlySet<string>,
    childSubmissionId: string,
  ): Effect.fn.Return<boolean, LedgerError> {
    const childRow = yield* readSubmission(operation, childSubmissionId);
    if (Option.isNone(childRow)) return markerChildren.has(childSubmissionId);
    if (childRow.value.state !== "settled") return false;
    const reservation = yield* readReservation(operation, childSubmissionId);
    if (Option.isNone(reservation)) {
      return yield* corruptionFailure(
        operation,
        "effect_agent_settlement_reservations",
        childSubmissionId,
        "A settled child Submission has no exact settlement reservation.",
      );
    }
    yield* validateStoredReservation(operation, childRow.value, reservation.value);
    return true;
  });

  /** Repair preflight: invalid local projection evidence is a mutation-free deferral. */
  const localChildSettlementNotificationCovered = Effect.fn(
    "DoSubmissionLedger.localChildSettlementNotificationCovered",
  )(function* (operation: string, child: SubmissionRow): Effect.fn.Return<boolean, LedgerError> {
    if (child.state !== "settled") return false;
    const evidence = yield* readRecoveryReservation(operation, child);
    if (evidence.reservationIntegrity !== "verified" || evidence.reservation === undefined) {
      return false;
    }
    return evidence.reservation.finalized && child.settled_outcome === evidence.reservation.outcome;
  });

  const decodeChildReservationRows = (operation: string, rowKey: string, rows: unknown) =>
    decodeRows(
      Schema.Array(ChildReservationRow),
      "effect_agent_child_reservations",
      rowKey,
      rows,
    ).pipe(Effect.mapError(internalFailure(operation)));

  const readChildReservation = Effect.fn("DoSubmissionLedger.readChildReservation")(function* (
    operation: string,
    reservationId: string,
  ): Effect.fn.Return<Option.Option<ChildReservationRow>, LedgerError> {
    const rows = yield* sql<Record<string, unknown>>`
      SELECT ${sql.literal(CHILD_RESERVATION_COLUMNS)}
      FROM effect_agent_child_reservations
      WHERE reservation_id = ${reservationId}
    `.pipe(Effect.mapError(sqlFailure(operation)));
    const decoded = yield* decodeChildReservationRows(operation, reservationId, rows);
    if (decoded.length > 1) {
      return yield* corruptionFailure(
        operation,
        "effect_agent_child_reservations",
        reservationId,
        "A child reservation primary key returned more than one row.",
      );
    }
    return decoded.length === 0 ? Option.none() : Option.some(decoded[0]);
  });

  const readChildReservationForCall = Effect.fn("DoSubmissionLedger.readChildReservationForCall")(
    function* (
      operation: string,
      parentSubmissionId: string,
      parentToolCallId: string,
    ): Effect.fn.Return<Option.Option<ChildReservationRow>, LedgerError> {
      const rows = yield* sql<Record<string, unknown>>`
        SELECT ${sql.literal(CHILD_RESERVATION_COLUMNS)}
        FROM effect_agent_child_reservations
        WHERE parent_submission_id = ${parentSubmissionId}
          AND parent_tool_call_id = ${parentToolCallId}
      `.pipe(Effect.mapError(sqlFailure(operation)));
      const decoded = yield* decodeChildReservationRows(
        operation,
        `${parentSubmissionId}/${parentToolCallId}`,
        rows,
      );
      if (decoded.length > 1) {
        return yield* corruptionFailure(
          operation,
          "effect_agent_child_reservations",
          `${parentSubmissionId}/${parentToolCallId}`,
          "A parent Tool Call returned more than one child reservation.",
        );
      }
      return decoded.length === 0 ? Option.none() : Option.some(decoded[0]);
    },
  );

  const childReservationSnapshotFromRow = Effect.fn(
    "DoSubmissionLedger.childReservationSnapshotFromRow",
  )(function* (
    operation: string,
    row: ChildReservationRow,
  ): Effect.fn.Return<ChildBudgetReservationSnapshot, LedgerError> {
    const rowFailure = (error: { readonly message: string }) =>
      corruptionFailure(
        operation,
        "effect_agent_child_reservations",
        row.reservation_id,
        error.message,
      );
    const allocation = yield* parseStoredJsonText(row.allocation_json).pipe(
      Effect.mapError(rowFailure),
    );
    const accounting =
      row.accounting_json === null
        ? undefined
        : yield* parseStoredJsonText(row.accounting_json).pipe(Effect.mapError(rowFailure));
    return yield* decodeChildReservationSnapshotUnknown({
      reservationId: row.reservation_id,
      parentSubmissionId: row.parent_submission_id,
      parentToolCallId: row.parent_tool_call_id,
      status: row.status,
      allocation,
      allocationDigest: row.allocation_digest,
      reservedAt: row.reserved_at,
      ...(row.child_submission_id === null ? {} : { childSubmissionId: row.child_submission_id }),
      ...(accounting === undefined ? {} : { accounting }),
      ...(row.release_began_at === null ? {} : { releaseBeganAt: row.release_began_at }),
      ...(row.released_at === null ? {} : { releasedAt: row.released_at }),
    }).pipe(Effect.mapError(rowFailure));
  });

  const readApprovalDecisions = Effect.fn("DoSubmissionLedger.readApprovalDecisions")(function* (
    operation: string,
    submissionId: string,
  ): Effect.fn.Return<ReadonlyArray<ApprovalDecisionRow>, LedgerError> {
    const rows = yield* sql<Record<string, unknown>>`
      SELECT
        submission_id,
        tool_call_id,
        decision,
        resolver,
        reason,
        decided_at
      FROM effect_agent_approval_decisions
      WHERE submission_id = ${submissionId}
      ORDER BY tool_call_id ASC
    `.pipe(Effect.mapError(sqlFailure(operation)));
    return yield* decodeRows(
      Schema.Array(ApprovalDecisionRow),
      "effect_agent_approval_decisions",
      submissionId,
      rows,
    ).pipe(Effect.mapError(internalFailure(operation)));
  });

  const approvalIntentFromRow = Effect.fn("DoSubmissionLedger.approvalIntentFromRow")(function* (
    operation: string,
    row: ApprovalDecisionRow,
  ): Effect.fn.Return<ApprovalDecisionIntent, LedgerError> {
    return yield* decodeApprovalDecisionIntent({
      submissionId: row.submission_id,
      toolCallId: row.tool_call_id,
      decision: row.decision,
      resolver: row.resolver,
      reason: row.reason,
      decidedAt: row.decided_at,
    }).pipe(
      Effect.mapError((error) =>
        corruptionFailure(
          operation,
          "effect_agent_approval_decisions",
          `${row.submission_id}/${row.tool_call_id}`,
          error.message,
        ),
      ),
    );
  });

  const readUnknownResolutions = Effect.fn("DoSubmissionLedger.readUnknownResolutions")(function* (
    operation: string,
    submissionId: string,
  ): Effect.fn.Return<ReadonlyArray<UnknownResolutionRow>, LedgerError> {
    const rows = yield* sql<Record<string, unknown>>`
      SELECT
        submission_id,
        tool_call_id,
        author,
        reason,
        resolution_json,
        resolved_at
      FROM effect_agent_unknown_resolutions
      WHERE submission_id = ${submissionId}
      ORDER BY tool_call_id ASC
    `.pipe(Effect.mapError(sqlFailure(operation)));
    return yield* decodeRows(
      Schema.Array(UnknownResolutionRow),
      "effect_agent_unknown_resolutions",
      submissionId,
      rows,
    ).pipe(Effect.mapError(internalFailure(operation)));
  });

  const unknownResolutionIntentFromRow = Effect.fn(
    "DoSubmissionLedger.unknownResolutionIntentFromRow",
  )(function* (
    operation: string,
    row: UnknownResolutionRow,
  ): Effect.fn.Return<UnknownResolutionIntent, LedgerError> {
    const resolution = yield* parseStoredJsonText(row.resolution_json).pipe(
      Effect.mapError((error) =>
        corruptionFailure(
          operation,
          "effect_agent_unknown_resolutions",
          `${row.submission_id}/${row.tool_call_id}`,
          error.message,
        ),
      ),
    );
    return yield* decodeUnknownResolutionIntent({
      submissionId: row.submission_id,
      toolCallId: row.tool_call_id,
      author: row.author,
      reason: row.reason,
      resolution,
      resolvedAt: row.resolved_at,
    }).pipe(
      Effect.mapError((error) =>
        corruptionFailure(
          operation,
          "effect_agent_unknown_resolutions",
          `${row.submission_id}/${row.tool_call_id}`,
          error.message,
        ),
      ),
    );
  });

  /** The Submission's marked-unknown open Tool Call identities, empty when never marked. */
  const storedUnknownToolCallIds = Effect.fn("DoSubmissionLedger.storedUnknownToolCallIds")(
    function* (
      operation: string,
      submission: SubmissionRow,
    ): Effect.fn.Return<ReadonlyArray<typeof ToolCallIdSchema.Type>, LedgerError> {
      if (submission.unknown_tool_call_ids_json === null) return [];
      return yield* decodeToolCallIdsText(submission.unknown_tool_call_ids_json).pipe(
        Effect.mapError((error) =>
          corruptionFailure(
            operation,
            "effect_agent_submissions",
            submission.submission_id,
            error.message,
          ),
        ),
      );
    },
  );

  /**
   * Canonical history is the abort authority (DUR-015): the intent's canonicalRecordId is
   * derived from the shared canonical-records table using the deterministic abort record
   * identity, never from a cached ledger marker.
   */
  const canonicalAbortRecordId = Effect.fn("DoSubmissionLedger.canonicalAbortRecordId")(function* (
    operation: string,
    conversationId: string,
    submissionId: SubmissionId,
  ): Effect.fn.Return<string | undefined, LedgerError> {
    const recordId = submissionAbortRecordId(submissionId);
    const rows = yield* sql<Record<string, unknown>>`
      SELECT record_id
      FROM effect_agent_canonical_records
      WHERE conversation_id = ${conversationId}
        AND record_id = ${recordId}
    `.pipe(Effect.mapError(sqlFailure(operation)));
    const decoded = yield* decodeRows(
      Schema.Array(CanonicalRecordIdRow),
      "effect_agent_canonical_records",
      `${conversationId}/${recordId}`,
      rows,
    ).pipe(Effect.mapError(internalFailure(operation)));
    return decoded.length === 0 ? undefined : recordId;
  });

  const abortIntentFromRow = Effect.fn("DoSubmissionLedger.abortIntentFromRow")(function* (
    operation: string,
    submission: SubmissionRow,
    submissionId: SubmissionId,
    row: AbortIntentRow,
  ): Effect.fn.Return<AbortIntent, LedgerError> {
    const canonicalRecordId = yield* canonicalAbortRecordId(
      operation,
      submission.conversation_id,
      submissionId,
    );
    return yield* decodeAbortIntent({
      submissionId: row.submission_id,
      author: row.author,
      reason: row.reason,
      requestedAt: row.requested_at,
      ...(canonicalRecordId === undefined ? {} : { canonicalRecordId }),
    }).pipe(
      Effect.mapError((error) =>
        corruptionFailure(
          operation,
          "effect_agent_abort_intents",
          row.submission_id,
          error.message,
        ),
      ),
    );
  });

  // Durable Object storage is the single serialized owner: writes confirm through output
  // gates before any response is observable, which is exactly the single-owner crash
  // durability this adapter claims — under its own honest label (P7 WP0).
  const capabilities = Effect.succeed(
    LedgerCapabilities.make({ durability: "durable-cloudflare" }),
  );

  const admit: SubmissionLedger["Service"]["admit"] = Effect.fn("DoSubmissionLedger.admit")(
    function* (request: AdmissionRequest) {
      const operation = "ledger admit";
      const validated = yield* Schema.decodeUnknownEffect(Schema.toType(AdmissionRequest))(
        request,
      ).pipe(Effect.mapError(internalFailure(operation)));
      const inputJson = yield* encodePersistedJsonText(validated.inputPayload).pipe(
        Effect.mapError(internalFailure(operation)),
      );
      // The platform's ~2 MB per-value bound, refused typed BEFORE any durable mutation
      // (resource-limits gate; oversized payloads are the designed R2 overflow path).
      yield* journal
        .checkValueBound(operation, inputJson)
        .pipe(Effect.mapError(internalFailure(operation)));
      const agentDigestsJson = yield* encodeDefinitionDigestsText(validated.agentDigests).pipe(
        Effect.mapError(internalFailure(operation)),
      );
      // Routable Submission identity (D-P6-5): `{uuidv7}:{conversationId}`. The cross-DO
      // routing layer parses ITS OWN minted format (split at the first ":") to address
      // submissionId-only operations to the owning Conversation Object; the id stays opaque
      // to every other component, exactly like DN's `submission-{uuid}` prefix.
      const mintedSubmissionId = `${yield* mintUuid(operation)}:${validated.conversationId}`;
      if (mintedSubmissionId.length > MAX_IDENTIFIER_LENGTH) {
        return yield* LedgerError.make({
          operation,
          message:
            `A routable Submission identity of ${mintedSubmissionId.length} characters exceeds ` +
            `the ${MAX_IDENTIFIER_LENGTH}-character ledger row bound; shorten the Conversation identity.`,
        });
      }
      const mintedReceiptId = `receipt-${yield* mintUuid(operation)}`;
      yield* hitFailpoint("ledger:admit:before", operation);
      const result = yield* inWriteTransaction(
        operation,
        Effect.gen(function* () {
          const keyRowKey = `${validated.conversationId}/${validated.principal}/${validated.idempotencyKey}`;
          const existingRows = yield* sql<Record<string, unknown>>`
            SELECT ${sql.literal(SUBMISSION_COLUMNS)}
            FROM effect_agent_submissions
            WHERE conversation_id = ${validated.conversationId}
              AND principal = ${validated.principal}
              AND idempotency_key = ${validated.idempotencyKey}
          `.pipe(Effect.mapError(sqlFailure(operation)));
          const existing = yield* decodeSubmissionRows(operation, keyRowKey, existingRows);
          if (existing.length > 1) {
            return yield* corruptionFailure(
              operation,
              "effect_agent_submissions",
              keyRowKey,
              "An admission idempotency key returned more than one row.",
            );
          }
          if (existing.length === 1) {
            // A replay must repeat the exact canonical input AND the exact parent linkage (or
            // its absence): linkage is immutable child lineage (spec §12 step 5, SUB-016).
            const sameLinkage =
              validated.parentLinkage === undefined
                ? existing[0].parent_submission_id === null &&
                  existing[0].parent_tool_call_id === null
                : existing[0].parent_submission_id === validated.parentLinkage.parentSubmissionId &&
                  existing[0].parent_tool_call_id === validated.parentLinkage.parentToolCallId;
            if (existing[0].input_digest !== validated.inputDigest || !sameLinkage) {
              return yield* AdmissionConflict.make({
                conversationId: validated.conversationId,
                principal: validated.principal,
                idempotencyKey: validated.idempotencyKey,
                existingInputDigest: existing[0].input_digest,
                attemptedInputDigest: validated.inputDigest,
              });
            }
            return yield* decodeAdmissionResult({
              submissionId: existing[0].submission_id,
              receiptId: existing[0].receipt_id,
              queueSequence: existing[0].queue_sequence,
              state: existing[0].state,
              replayed: true,
            }).pipe(Effect.mapError(internalFailure(operation)));
          }

          const maxRows = yield* sql<Record<string, unknown>>`
            SELECT COALESCE(MAX(queue_sequence), 0) AS max_queue_sequence
            FROM effect_agent_submissions
            WHERE conversation_id = ${validated.conversationId}
          `.pipe(Effect.mapError(sqlFailure(operation)));
          const decodedMax = yield* decodeRows(
            Schema.Array(MaxQueueSequenceRow),
            "effect_agent_submissions",
            validated.conversationId,
            maxRows,
          ).pipe(Effect.mapError(internalFailure(operation)));
          const queueSequence = yield* decodeQueueSequence(
            (decodedMax[0]?.max_queue_sequence ?? 0) + 1,
          ).pipe(Effect.mapError(internalFailure(operation)));
          const now = yield* currentInstant;

          yield* sql`
            INSERT INTO effect_agent_submissions (
              submission_id,
              conversation_id,
              queue_sequence,
              principal,
              idempotency_key,
              agent_id,
              agent_digests_json,
              deployment_id,
              input_json,
              input_digest,
              receipt_id,
              state,
              created_at,
              parent_submission_id,
              parent_tool_call_id
            ) VALUES (
              ${mintedSubmissionId},
              ${validated.conversationId},
              ${queueSequence},
              ${validated.principal},
              ${validated.idempotencyKey},
              ${validated.agentId},
              ${agentDigestsJson},
              ${validated.deploymentId},
              ${inputJson},
              ${validated.inputDigest},
              ${mintedReceiptId},
              'admitted',
              ${now.iso},
              ${validated.parentLinkage?.parentSubmissionId ?? null},
              ${validated.parentLinkage?.parentToolCallId ?? null}
            )
          `.pipe(Effect.mapError(sqlFailure(operation)));

          return yield* decodeAdmissionResult({
            submissionId: mintedSubmissionId,
            receiptId: mintedReceiptId,
            queueSequence,
            state: "admitted",
            replayed: false,
          }).pipe(Effect.mapError(internalFailure(operation)));
        }),
      );
      yield* hitFailpoint("ledger:admit:after", operation);
      return result;
    },
  );

  const markReady: SubmissionLedger["Service"]["markReady"] = Effect.fn(
    "DoSubmissionLedger.markReady",
  )(function* (request: MarkReadyRequest) {
    const operation = "ledger mark ready";
    const validated = yield* Schema.decodeUnknownEffect(Schema.toType(MarkReadyRequest))(
      request,
    ).pipe(Effect.mapError(internalFailure(operation)));
    yield* hitFailpoint("ledger:mark-ready:before", operation);
    yield* inWriteTransaction(
      operation,
      Effect.gen(function* () {
        const submission = yield* requireSubmission(operation, validated.submissionId);
        if (submission.state !== "admitted") return;
        const now = yield* currentInstant;
        yield* sql`
          UPDATE effect_agent_submissions
          SET state = 'ready', ready_at = ${now.iso}
          WHERE submission_id = ${validated.submissionId}
        `.pipe(Effect.mapError(sqlFailure(operation)));
      }),
    );
    yield* hitFailpoint("ledger:mark-ready:after", operation);
  });

  const lookup: SubmissionLedger["Service"]["lookup"] = Effect.fn("DoSubmissionLedger.lookup")(
    function* (request: SubmissionLookup) {
      const operation = "ledger lookup";
      const validated = yield* Schema.decodeUnknownEffect(Schema.toType(SubmissionLookup))(
        request,
      ).pipe(Effect.mapError(internalFailure(operation)));
      if (validated._tag === "SubmissionLookupById") {
        const row = yield* readSubmission(operation, validated.submissionId);
        if (Option.isNone(row)) return Option.none();
        return Option.some(yield* decodeSubmissionSnapshot(operation, row.value));
      }
      const rows = yield* sql<Record<string, unknown>>`
      SELECT ${sql.literal(SUBMISSION_COLUMNS)}
      FROM effect_agent_submissions
      WHERE conversation_id = ${validated.conversationId}
        AND principal = ${validated.principal}
        AND idempotency_key = ${validated.idempotencyKey}
    `.pipe(Effect.mapError(sqlFailure(operation)));
      const decoded = yield* decodeSubmissionRows(
        operation,
        `${validated.conversationId}/${validated.principal}/${validated.idempotencyKey}`,
        rows,
      );
      if (decoded.length > 1) {
        return yield* corruptionFailure(
          operation,
          "effect_agent_submissions",
          `${validated.conversationId}/${validated.principal}/${validated.idempotencyKey}`,
          "An admission idempotency key returned more than one row.",
        );
      }
      if (decoded.length === 0) return Option.none();
      return Option.some(yield* decodeSubmissionSnapshot(operation, decoded[0]));
    },
  );

  // This LOCAL facet is the authoritative owner of every Conversation stored in this Durable
  // Object, so the key-scoped read IS the admission truth and the tri-state degenerates to
  // NotAdmitted or Admitted (SUB-031). `AdmissionIndeterminate` becomes real one layer out:
  // the WP2 routed decorator answers it when the OWNING Durable Object is unreachable.
  const resolveAdmission: SubmissionLedger["Service"]["resolveAdmission"] = Effect.fn(
    "DoSubmissionLedger.resolveAdmission",
  )(function* (request: SubmissionLookupByKey) {
    const operation = "ledger resolve admission";
    const validated = yield* Schema.decodeUnknownEffect(Schema.toType(SubmissionLookupByKey))(
      request,
    ).pipe(Effect.mapError(internalFailure(operation)));
    const rows = yield* sql<Record<string, unknown>>`
      SELECT ${sql.literal(SUBMISSION_COLUMNS)}
      FROM effect_agent_submissions
      WHERE conversation_id = ${validated.conversationId}
        AND principal = ${validated.principal}
        AND idempotency_key = ${validated.idempotencyKey}
    `.pipe(Effect.mapError(sqlFailure(operation)));
    const decoded = yield* decodeSubmissionRows(
      operation,
      `${validated.conversationId}/${validated.principal}/${validated.idempotencyKey}`,
      rows,
    );
    if (decoded.length > 1) {
      return yield* corruptionFailure(
        operation,
        "effect_agent_submissions",
        `${validated.conversationId}/${validated.principal}/${validated.idempotencyKey}`,
        "An admission idempotency key returned more than one row.",
      );
    }
    if (decoded.length === 0) return AdmissionNotAdmitted.make();
    return AdmissionAdmitted.make({
      submission: yield* decodeSubmissionSnapshot(operation, decoded[0]),
    });
  });

  const claim: SubmissionLedger["Service"]["claim"] = Effect.fn("DoSubmissionLedger.claim")(
    function* (request: ClaimRequest) {
      const operation = "ledger claim";
      const validated = yield* Schema.decodeUnknownEffect(Schema.toType(ClaimRequest))(
        request,
      ).pipe(Effect.mapError(internalFailure(operation)));
      const attemptId = `attempt-${yield* mintUuid(operation)}`;
      const ownershipToken = `owner-${yield* mintUuid(operation)}`;
      yield* hitFailpoint("ledger:claim:before", operation);
      const claimed = yield* inWriteTransaction(
        operation,
        Effect.gen(function* () {
          const headRows = yield* sql<Record<string, unknown>>`
            SELECT ${sql.literal(SUBMISSION_COLUMNS)}
            FROM effect_agent_submissions
            WHERE conversation_id = ${validated.conversationId}
              AND state <> 'settled'
            ORDER BY queue_sequence ASC
            LIMIT 1
          `.pipe(Effect.mapError(sqlFailure(operation)));
          const heads = yield* decodeSubmissionRows(operation, validated.conversationId, headRows);
          if (heads.length === 0) return Option.none<Claim>();
          const head = heads[0];

          // A joining/joined head is host-owned and a suspended/unknown head is durably
          // blocked (DUR-017); the lane produces no claim and later ready work is never
          // skipped past the blocked head (DUR-004).
          if (
            head.state === "joining" ||
            head.state === "joined" ||
            head.state === "suspended" ||
            head.state === "unknown"
          ) {
            return Option.none<Claim>();
          }

          const now = yield* currentInstant;
          const ownership = yield* readOwnership(operation, head.submission_id);
          if (Option.isSome(ownership)) {
            const expiresAt = yield* timestampMillis(
              operation,
              head.submission_id,
            )(ownership.value.lease_expires_at);
            // A live lease blocks every new claim; expiry alone only revokes the liveness
            // assumption — correctness stays with producer-epoch fencing (D5). In DC a live
            // lease under another token can only come from an evicted incarnation.
            if (expiresAt > now.millis) return Option.none<Claim>();
          }

          // Bump the Conversation's producer epoch atomically with the claim so every stale
          // Attempt is fenced out of canonical appends (DUR-006). A Conversation that was
          // never materialized (eviction between admission and materialization) is created
          // here so recovery can claim first and re-materialize idempotently at this epoch.
          const conversations = yield* journal
            .getConversation(head.conversation_id)
            .pipe(Effect.mapError(internalFailure(operation)));
          let producerEpoch: number;
          if (conversations.length === 0) {
            producerEpoch = 1;
            yield* sql`
              INSERT INTO effect_agent_conversations (
                conversation_id,
                created_at,
                tail_sequence,
                tail_digest,
                producer_epoch
              ) VALUES (
                ${head.conversation_id},
                ${now.iso},
                0,
                ${EMPTY_TAIL_DIGEST},
                ${producerEpoch}
              )
            `.pipe(Effect.mapError(sqlFailure(operation)));
          } else {
            producerEpoch = conversations[0].producer_epoch + 1;
            yield* sql`
              UPDATE effect_agent_conversations
              SET producer_epoch = ${producerEpoch}
              WHERE conversation_id = ${head.conversation_id}
            `.pipe(Effect.mapError(sqlFailure(operation)));
          }

          const leaseExpiresAt = new Date(now.millis + config.ownershipLeaseDuration).toISOString();
          yield* sql`
            INSERT INTO effect_agent_submission_ownership (
              submission_id,
              attempt_id,
              ownership_token,
              producer_epoch,
              owner_producer_id,
              lease_expires_at
            ) VALUES (
              ${head.submission_id},
              ${attemptId},
              ${ownershipToken},
              ${producerEpoch},
              ${validated.producerId},
              ${leaseExpiresAt}
            )
            ON CONFLICT (submission_id) DO UPDATE SET
              attempt_id = excluded.attempt_id,
              ownership_token = excluded.ownership_token,
              producer_epoch = excluded.producer_epoch,
              owner_producer_id = excluded.owner_producer_id,
              lease_expires_at = excluded.lease_expires_at
          `.pipe(Effect.mapError(sqlFailure(operation)));

          yield* sql`
            INSERT INTO effect_agent_attempts (
              attempt_id,
              submission_id,
              conversation_id,
              owner_producer_id,
              producer_epoch,
              claimed_at
            ) VALUES (
              ${attemptId},
              ${head.submission_id},
              ${head.conversation_id},
              ${validated.producerId},
              ${producerEpoch},
              ${now.iso}
            )
          `.pipe(Effect.mapError(sqlFailure(operation)));

          if (head.state === "ready") {
            yield* sql`
              UPDATE effect_agent_submissions
              SET state = 'running'
              WHERE submission_id = ${head.submission_id}
            `.pipe(Effect.mapError(sqlFailure(operation)));
          }

          const inputPayload = yield* parseStoredJsonText(head.input_json).pipe(
            Effect.mapError((error) =>
              corruptionFailure(
                operation,
                "effect_agent_submissions",
                head.submission_id,
                error.message,
              ),
            ),
          );
          return Option.some(
            yield* decodeClaim({
              submissionId: head.submission_id,
              attemptId,
              ownershipToken,
              producerEpoch,
              leaseExpiresAt,
              inputPayload,
            }).pipe(Effect.mapError(internalFailure(operation))),
          );
        }),
      );
      yield* hitFailpoint("ledger:claim:after", operation);
      return claimed;
    },
  );

  const renewOwnership: SubmissionLedger["Service"]["renewOwnership"] = Effect.fn(
    "DoSubmissionLedger.renewOwnership",
  )(function* (request: RenewOwnershipRequest) {
    const operation = "ledger renew ownership";
    const validated = yield* Schema.decodeUnknownEffect(Schema.toType(RenewOwnershipRequest))(
      request,
    ).pipe(Effect.mapError(internalFailure(operation)));
    yield* hitFailpoint("ledger:renew:before", operation);
    const renewal = yield* inWriteTransaction(
      operation,
      Effect.gen(function* () {
        const submission = yield* requireSubmission(operation, validated.submissionId);
        yield* requireOwnership(operation, submission, validated.ownershipToken);
        const now = yield* currentInstant;
        const leaseExpiresAt = new Date(now.millis + config.ownershipLeaseDuration).toISOString();
        yield* sql`
          UPDATE effect_agent_submission_ownership
          SET lease_expires_at = ${leaseExpiresAt}
          WHERE submission_id = ${validated.submissionId}
        `.pipe(Effect.mapError(sqlFailure(operation)));
        return yield* decodeOwnershipRenewal({
          ownershipToken: validated.ownershipToken,
          leaseExpiresAt,
        }).pipe(Effect.mapError(internalFailure(operation)));
      }),
    );
    yield* hitFailpoint("ledger:renew:after", operation);
    return renewal;
  });

  const releaseOwnership: SubmissionLedger["Service"]["releaseOwnership"] = Effect.fn(
    "DoSubmissionLedger.releaseOwnership",
  )(function* (request: ReleaseOwnershipRequest) {
    const operation = "ledger release ownership";
    const validated = yield* Schema.decodeUnknownEffect(Schema.toType(ReleaseOwnershipRequest))(
      request,
    ).pipe(Effect.mapError(internalFailure(operation)));
    yield* hitFailpoint("ledger:release:before", operation);
    yield* inWriteTransaction(
      operation,
      Effect.gen(function* () {
        const submission = yield* requireSubmission(operation, validated.submissionId);
        yield* requireOwnership(operation, submission, validated.ownershipToken);
        yield* sql`
          DELETE FROM effect_agent_submission_ownership
          WHERE submission_id = ${validated.submissionId}
        `.pipe(Effect.mapError(sqlFailure(operation)));
        if (submission.state === "running") {
          yield* sql`
            UPDATE effect_agent_submissions
            SET state = 'ready'
            WHERE submission_id = ${validated.submissionId}
          `.pipe(Effect.mapError(sqlFailure(operation)));
        }
      }),
    );
    yield* hitFailpoint("ledger:release:after", operation);
  });

  const markInputApplied: SubmissionLedger["Service"]["markInputApplied"] = Effect.fn(
    "DoSubmissionLedger.markInputApplied",
  )(function* (request: MarkInputAppliedRequest) {
    const operation = "ledger mark input applied";
    const validated = yield* Schema.decodeUnknownEffect(Schema.toType(MarkInputAppliedRequest))(
      request,
    ).pipe(Effect.mapError(internalFailure(operation)));
    yield* hitFailpoint("ledger:mark-input-applied:before", operation);
    yield* inWriteTransaction(
      operation,
      Effect.gen(function* () {
        const submission = yield* requireSubmission(operation, validated.submissionId);
        yield* requireOwnership(operation, submission, validated.ownershipToken);
        if (submission.input_applied_record_id !== null) {
          if (
            submission.input_applied_record_id === validated.recordId &&
            submission.input_applied_sequence === validated.sequence
          ) {
            return;
          }
          return yield* corruptionFailure(
            operation,
            "effect_agent_submissions",
            validated.submissionId,
            "A different canonical input marker is already recorded for this Submission.",
          );
        }
        yield* sql`
          UPDATE effect_agent_submissions
          SET
            input_applied_record_id = ${validated.recordId},
            input_applied_sequence = ${validated.sequence},
            state = CASE
              WHEN state IN ('admitted', 'ready', 'running') THEN 'input-applied'
              ELSE state
            END
          WHERE submission_id = ${validated.submissionId}
        `.pipe(Effect.mapError(sqlFailure(operation)));
      }),
    );
    yield* hitFailpoint("ledger:mark-input-applied:after", operation);
  });

  const reserveSettlement: SubmissionLedger["Service"]["reserveSettlement"] = Effect.fn(
    "DoSubmissionLedger.reserveSettlement",
  )(function* (request: SettlementReservation) {
    const operation = "ledger reserve settlement";
    const validated = yield* Schema.decodeUnknownEffect(Schema.toType(SettlementReservation))(
      request,
    ).pipe(Effect.mapError(internalFailure(operation)));
    const recordJson = yield* encodeRecordEnvelopeText(validated.record).pipe(
      Effect.mapError(internalFailure(operation)),
    );
    // The reserved record is appended canonically later; refuse over-bound payloads typed
    // before the reservation row exists.
    yield* journal
      .checkValueBound(operation, recordJson)
      .pipe(Effect.mapError(internalFailure(operation)));
    yield* hitFailpoint("ledger:reserve-settlement:before", operation);
    const reserved = yield* inWriteTransaction(
      operation,
      Effect.gen(function* () {
        const submission = yield* requireSubmission(operation, validated.submissionId);
        const canonical = yield* validateRequestedSettlement(operation, submission, validated);
        const existing = yield* readReservation(operation, validated.submissionId);
        if (Option.isSome(existing)) {
          const storedCanonical = yield* validateStoredReservation(
            operation,
            submission,
            existing.value,
          );
          const identical =
            storedCanonical.settlementId === canonical.settlementId &&
            storedCanonical.outcome === canonical.outcome &&
            storedCanonical.recordDigest === canonical.recordDigest &&
            existing.value.record_json === recordJson;
          if (!identical) {
            return yield* SettlementConflict.make({
              submissionId: canonical.submissionId,
              existingOutcome: storedCanonical.outcome,
            });
          }
          return ReservedSettlement.make({
            submissionId: canonical.submissionId,
            settlementId: canonical.settlementId,
            outcome: canonical.outcome,
            record: canonical.record,
            recordDigest: canonical.recordDigest,
            replayed: true,
          });
        }

        if (submission.state === "settled") {
          if (submission.settled_outcome === null) {
            return yield* corruptionFailure(
              operation,
              "effect_agent_submissions",
              validated.submissionId,
              "A settled Submission carries no terminal outcome.",
            );
          }
          return yield* SettlementConflict.make({
            submissionId: validated.submissionId,
            existingOutcome: submission.settled_outcome,
          });
        }
        // A `joined` Submission settles WITH its host (plan §2.5) and its lane is never
        // worker-claimable, so no ownership token can exist for it: the recorded host linkage
        // authorizes the reservation and the presented token is not consulted.
        if (!(submission.state === "joined" && submission.joined_host_submission_id !== null)) {
          // P7 §7(c): an aborted, never-claimed, still-queued Submission likewise has no live
          // ownership to fence against — its durable abort intent authorizes exactly its
          // ABORTED settlement (`terminalizing` is the same pass's crash replay). Every other
          // reservation stays fenced by the target lane's live ownership.
          let queuedAbortSettlement = false;
          if (
            validated.outcome === "aborted" &&
            (submission.state === "ready" || submission.state === "terminalizing")
          ) {
            const abortIntent = yield* readAbortIntent(operation, validated.submissionId);
            if (Option.isSome(abortIntent)) {
              const ownership = yield* readOwnership(operation, validated.submissionId);
              queuedAbortSettlement = Option.isNone(ownership);
            }
          }
          if (!queuedAbortSettlement) {
            yield* requireOwnership(operation, submission, validated.ownershipToken);
          }
        }
        const now = yield* currentInstant;
        yield* sql`
          INSERT INTO effect_agent_settlement_reservations (
            submission_id,
            settlement_id,
            outcome,
            record_id,
            record_json,
            record_digest,
            reserved_at
          ) VALUES (
            ${canonical.submissionId},
            ${canonical.settlementId},
            ${canonical.outcome},
            ${canonical.record.recordId},
            ${recordJson},
            ${canonical.recordDigest},
            ${now.iso}
          )
        `.pipe(Effect.mapError(sqlFailure(operation)));
        yield* sql`
          UPDATE effect_agent_submissions
          SET state = 'terminalizing'
          WHERE submission_id = ${validated.submissionId}
        `.pipe(Effect.mapError(sqlFailure(operation)));
        return ReservedSettlement.make({
          submissionId: canonical.submissionId,
          settlementId: canonical.settlementId,
          outcome: canonical.outcome,
          record: canonical.record,
          recordDigest: canonical.recordDigest,
          replayed: false,
        });
      }),
    );
    yield* hitFailpoint("ledger:reserve-settlement:after", operation);
    return reserved;
  });

  const finalizeSettlement: SubmissionLedger["Service"]["finalizeSettlement"] = Effect.fn(
    "DoSubmissionLedger.finalizeSettlement",
  )(function* (request: SettlementFinalization) {
    const operation = "ledger finalize settlement";
    const validated = yield* Schema.decodeUnknownEffect(Schema.toType(SettlementFinalization))(
      request,
    ).pipe(Effect.mapError(internalFailure(operation)));
    yield* hitFailpoint("ledger:finalize-settlement:before", operation);
    const settlement = yield* inWriteTransaction(
      operation,
      Effect.gen(function* () {
        const submission = yield* requireSubmission(operation, validated.submissionId);
        const reservation = yield* readReservation(operation, validated.submissionId);
        if (Option.isNone(reservation)) {
          return yield* LedgerError.make({
            operation,
            message: `No settlement reservation exists for submission ${validated.submissionId}.`,
          });
        }
        const canonical = yield* validateStoredReservation(
          operation,
          submission,
          reservation.value,
        );
        if (canonical.settlementId !== validated.settlementId) {
          return yield* SettlementConflict.make({
            submissionId: validated.submissionId,
            existingOutcome: canonical.outcome,
          });
        }
        if (submission.state === "settled") {
          if (reservation.value.finalized_at === null) {
            return yield* corruptionFailure(
              operation,
              "effect_agent_settlement_reservations",
              validated.submissionId,
              "A settled Submission's reservation carries no finalization timestamp.",
            );
          }
          return yield* decodeSettlement({
            submissionId: validated.submissionId,
            settlementId: validated.settlementId,
            receiptId: submission.receipt_id,
            outcome: canonical.outcome,
            settledAt: reservation.value.finalized_at,
          }).pipe(Effect.mapError(internalFailure(operation)));
        }
        const now = yield* currentInstant;
        yield* sql`
          UPDATE effect_agent_submissions
          SET state = 'settled', settled_outcome = ${canonical.outcome}
          WHERE submission_id = ${validated.submissionId}
        `.pipe(Effect.mapError(sqlFailure(operation)));
        yield* sql`
          UPDATE effect_agent_settlement_reservations
          SET finalized_at = ${now.iso}
          WHERE submission_id = ${validated.submissionId}
        `.pipe(Effect.mapError(sqlFailure(operation)));
        yield* sql`
          DELETE FROM effect_agent_submission_ownership
          WHERE submission_id = ${validated.submissionId}
        `.pipe(Effect.mapError(sqlFailure(operation)));
        return yield* decodeSettlement({
          submissionId: validated.submissionId,
          settlementId: validated.settlementId,
          receiptId: submission.receipt_id,
          outcome: canonical.outcome,
          settledAt: now.iso,
        }).pipe(Effect.mapError(internalFailure(operation)));
      }),
    );
    yield* hitFailpoint("ledger:finalize-settlement:after", operation);
    return settlement;
  });

  const repairSettlementFromCanonical: SubmissionLedger["Service"]["repairSettlementFromCanonical"] =
    Effect.fn("DoSubmissionLedger.repairSettlementFromCanonical")(function* (
      request: CanonicalSettlementRepair,
    ) {
      const operation = "ledger repair settlement from canonical";
      const validated = yield* Schema.decodeUnknownEffect(Schema.toType(CanonicalSettlementRepair))(
        request,
      ).pipe(Effect.mapError(internalFailure(operation)));
      yield* hitFailpoint("ledger:repair-settlement:before", operation);
      const settlement = yield* inWriteTransaction(
        operation,
        Effect.gen(function* () {
          const submission = yield* requireSubmission(operation, validated.submissionId);
          const receiptId = yield* decodeReceiptId(submission.receipt_id).pipe(
            Effect.mapError(internalFailure(operation)),
          );
          const canonical = yield* validateCanonicalSettlement(validated, receiptId);
          const recordJson = yield* encodeRecordEnvelopeText(canonical.record).pipe(
            Effect.mapError(internalFailure(operation)),
          );
          yield* journal
            .checkValueBound(operation, recordJson)
            .pipe(Effect.mapError(internalFailure(operation)));
          const existingRows = yield* sql<Record<string, unknown>>`
            SELECT reserved_at, finalized_at
            FROM effect_agent_settlement_reservations
            WHERE submission_id = ${canonical.submissionId}
          `.pipe(Effect.mapError(sqlFailure(operation)));
          const now = yield* currentInstant;
          let reservedAt = now.iso;
          let settledAt = now.iso;
          const existingReservedAt = existingRows[0]?.reserved_at;
          if (typeof existingReservedAt === "string") {
            const persistedReservedAt = yield* decodeUtcInstant(existingReservedAt).pipe(
              Effect.option,
            );
            if (Option.isSome(persistedReservedAt)) reservedAt = existingReservedAt;
          }
          const existingFinalizedAt = existingRows[0]?.finalized_at;
          if (submission.state === "settled" && typeof existingFinalizedAt === "string") {
            const persistedSettledAt = yield* decodeUtcInstant(existingFinalizedAt).pipe(
              Effect.option,
            );
            if (Option.isSome(persistedSettledAt)) settledAt = existingFinalizedAt;
          }
          yield* sql`
            INSERT INTO effect_agent_settlement_reservations (
              submission_id,
              settlement_id,
              outcome,
              record_id,
              record_json,
              record_digest,
              reserved_at,
              finalized_at
            ) VALUES (
              ${canonical.submissionId},
              ${canonical.settlementId},
              ${canonical.outcome},
              ${canonical.record.recordId},
              ${recordJson},
              ${canonical.recordDigest},
              ${reservedAt},
              ${settledAt}
            )
            ON CONFLICT(submission_id) DO UPDATE SET
              settlement_id = excluded.settlement_id,
              outcome = excluded.outcome,
              record_id = excluded.record_id,
              record_json = excluded.record_json,
              record_digest = excluded.record_digest,
              reserved_at = excluded.reserved_at,
              finalized_at = excluded.finalized_at
          `.pipe(Effect.mapError(sqlFailure(operation)));
          yield* sql`
            UPDATE effect_agent_submissions
            SET
              state = 'settled',
              settled_outcome = ${canonical.outcome},
              suspended_reason_json = NULL,
              suspended_at = NULL
            WHERE submission_id = ${canonical.submissionId}
          `.pipe(Effect.mapError(sqlFailure(operation)));
          yield* sql`
            DELETE FROM effect_agent_submission_ownership
            WHERE submission_id = ${canonical.submissionId}
          `.pipe(Effect.mapError(sqlFailure(operation)));
          return yield* decodeSettlement({
            submissionId: canonical.submissionId,
            settlementId: canonical.settlementId,
            receiptId: canonical.receiptId,
            outcome: canonical.outcome,
            settledAt,
          }).pipe(Effect.mapError(internalFailure(operation)));
        }),
      );
      yield* hitFailpoint("ledger:repair-settlement:after", operation);
      return settlement;
    });

  const requestAbort: SubmissionLedger["Service"]["requestAbort"] = Effect.fn(
    "DoSubmissionLedger.requestAbort",
  )(function* (request: AbortCommand) {
    const operation = "ledger request abort";
    const validated = yield* Schema.decodeUnknownEffect(Schema.toType(AbortCommand))(request).pipe(
      Effect.mapError(internalFailure(operation)),
    );
    yield* hitFailpoint("ledger:request-abort:before", operation);
    const intent = yield* inWriteTransaction(
      operation,
      Effect.gen(function* () {
        const submission = yield* requireSubmission(operation, validated.submissionId);
        if (submission.state === "settled") {
          if (submission.settled_outcome === null) {
            return yield* corruptionFailure(
              operation,
              "effect_agent_submissions",
              validated.submissionId,
              "A settled Submission carries no terminal outcome.",
            );
          }
          return yield* SettlementConflict.make({
            submissionId: validated.submissionId,
            existingOutcome: submission.settled_outcome,
          });
        }
        // A joined Submission settles WITH its host; the abort target is the host (plan
        // §2.5). A joining Submission still records the intent: it is honored only if the
        // host has not consumed the input (revert-then-abort).
        if (submission.state === "joined") {
          if (submission.joined_host_submission_id === null) {
            return yield* corruptionFailure(
              operation,
              "effect_agent_submissions",
              validated.submissionId,
              "A joined Submission carries no host linkage.",
            );
          }
          const hostSubmissionId = yield* decodeSubmissionId(
            submission.joined_host_submission_id,
          ).pipe(Effect.mapError(internalFailure(operation)));
          return yield* JoinedToHost.make({
            submissionId: validated.submissionId,
            hostSubmissionId,
          });
        }
        const existing = yield* readAbortIntent(operation, validated.submissionId);
        if (Option.isSome(existing)) {
          return yield* abortIntentFromRow(
            operation,
            submission,
            validated.submissionId,
            existing.value,
          );
        }
        const now = yield* currentInstant;
        yield* sql`
          INSERT INTO effect_agent_abort_intents (
            submission_id,
            author,
            reason,
            requested_at
          ) VALUES (
            ${validated.submissionId},
            ${validated.author},
            ${validated.reason},
            ${now.iso}
          )
        `.pipe(Effect.mapError(sqlFailure(operation)));
        const canonicalRecordId = yield* canonicalAbortRecordId(
          operation,
          submission.conversation_id,
          validated.submissionId,
        );
        return yield* decodeAbortIntent({
          submissionId: validated.submissionId,
          author: validated.author,
          reason: validated.reason,
          requestedAt: now.iso,
          ...(canonicalRecordId === undefined ? {} : { canonicalRecordId }),
        }).pipe(Effect.mapError(internalFailure(operation)));
      }),
    );
    yield* hitFailpoint("ledger:request-abort:after", operation);
    return intent;
  });

  const claimJoining: SubmissionLedger["Service"]["claimJoining"] = Effect.fn(
    "DoSubmissionLedger.claimJoining",
  )(function* (request: ClaimJoiningRequest) {
    const operation = "ledger claim joining";
    const validated = yield* Schema.decodeUnknownEffect(Schema.toType(ClaimJoiningRequest))(
      request,
    ).pipe(Effect.mapError(internalFailure(operation)));
    yield* hitFailpoint("ledger:claim-joining:before", operation);
    const claims = yield* inWriteTransaction(
      operation,
      Effect.gen(function* () {
        const host = yield* requireSubmission(operation, validated.hostSubmissionId);
        if (host.conversation_id !== validated.conversationId) {
          return yield* LedgerError.make({
            operation,
            message: `Host submission ${validated.hostSubmissionId} does not belong to conversation ${validated.conversationId}.`,
          });
        }
        // The host Attempt already owns the lane; no epoch bump happens here (plan §2.5).
        yield* requireOwnership(operation, host, validated.ownershipToken);
        const laterRows = yield* sql<Record<string, unknown>>`
          SELECT ${sql.literal(SUBMISSION_COLUMNS)}
          FROM effect_agent_submissions
          WHERE conversation_id = ${validated.conversationId}
            AND queue_sequence > ${host.queue_sequence}
          ORDER BY queue_sequence ASC
        `.pipe(Effect.mapError(sqlFailure(operation)));
        const later = yield* decodeSubmissionRows(operation, validated.conversationId, laterRows);
        const claimed: Array<JoiningClaim> = [];
        for (const row of later) {
          if (claimed.length >= validated.maxCount) break;
          // Rows already claimed by THIS host extend its contiguous prefix and are skipped;
          // the coordinator re-delivers already-joined input through the coverage rule.
          if (
            (row.state === "joining" || row.state === "joined") &&
            row.joined_host_submission_id === validated.hostSubmissionId
          ) {
            continue;
          }
          // P7 §7(c): an aborted-settled row is a CLOSED obligation, not a gap — recovery
          // settles aborted never-claimed queued work immediately, and settlement order of
          // never-run work is not execution order (DUR-004 bounds execution).
          if (row.state === "settled" && row.settled_outcome === "aborted") continue;
          // Any other non-ready row — an admitted-not-ready gap in particular — breaks the
          // contiguous ready prefix (plan §2.5); later ready work stays queued (DUR-004).
          if (row.state !== "ready") break;
          yield* sql`
            UPDATE effect_agent_submissions
            SET state = 'joining', joined_host_submission_id = ${validated.hostSubmissionId}
            WHERE submission_id = ${row.submission_id}
          `.pipe(Effect.mapError(sqlFailure(operation)));
          const inputPayload = yield* parseStoredJsonText(row.input_json).pipe(
            Effect.mapError((error) =>
              corruptionFailure(
                operation,
                "effect_agent_submissions",
                row.submission_id,
                error.message,
              ),
            ),
          );
          claimed.push(
            yield* decodeJoiningClaim({
              submissionId: row.submission_id,
              queueSequence: row.queue_sequence,
              inputPayload,
            }).pipe(Effect.mapError(internalFailure(operation))),
          );
        }
        return claimed as ReadonlyArray<JoiningClaim>;
      }),
    );
    yield* hitFailpoint("ledger:claim-joining:after", operation);
    return claims;
  });

  const markJoined: SubmissionLedger["Service"]["markJoined"] = Effect.fn(
    "DoSubmissionLedger.markJoined",
  )(function* (request: MarkJoinedRequest) {
    const operation = "ledger mark joined";
    const validated = yield* Schema.decodeUnknownEffect(Schema.toType(MarkJoinedRequest))(
      request,
    ).pipe(Effect.mapError(internalFailure(operation)));
    yield* hitFailpoint("ledger:mark-joined:before", operation);
    yield* inWriteTransaction(
      operation,
      Effect.gen(function* () {
        const submission = yield* requireSubmission(operation, validated.submissionId);
        if (submission.joined_host_submission_id === null) {
          return yield* LedgerError.make({
            operation,
            message: `Submission ${validated.submissionId} was never claimed for joining.`,
          });
        }
        const host = yield* requireSubmission(operation, submission.joined_host_submission_id);
        // The lane is host-owned: the presented token must own the HOST's ownership period,
        // which also lets a later host Attempt repair a lost marker from history (DUR-016).
        yield* requireOwnership(operation, host, validated.ownershipToken);
        if (submission.input_applied_record_id !== null) {
          if (
            submission.input_applied_record_id === validated.recordId &&
            submission.input_applied_sequence === validated.sequence
          ) {
            return;
          }
          return yield* corruptionFailure(
            operation,
            "effect_agent_submissions",
            validated.submissionId,
            "A different join marker is already recorded for this Submission.",
          );
        }
        if (submission.state !== "joining" && submission.state !== "joined") {
          return yield* LedgerError.make({
            operation,
            message: `Cannot mark submission ${validated.submissionId} joined from state ${submission.state}.`,
          });
        }
        yield* sql`
          UPDATE effect_agent_submissions
          SET
            input_applied_record_id = ${validated.recordId},
            input_applied_sequence = ${validated.sequence},
            state = 'joined'
          WHERE submission_id = ${validated.submissionId}
        `.pipe(Effect.mapError(sqlFailure(operation)));
      }),
    );
    yield* hitFailpoint("ledger:mark-joined:after", operation);
  });

  const revertJoining: SubmissionLedger["Service"]["revertJoining"] = Effect.fn(
    "DoSubmissionLedger.revertJoining",
  )(function* (request: RevertJoiningRequest) {
    const operation = "ledger revert joining";
    const validated = yield* Schema.decodeUnknownEffect(Schema.toType(RevertJoiningRequest))(
      request,
    ).pipe(Effect.mapError(internalFailure(operation)));
    yield* hitFailpoint("ledger:revert-joining:before", operation);
    yield* inWriteTransaction(
      operation,
      Effect.gen(function* () {
        const submission = yield* requireSubmission(operation, validated.submissionId);
        // Idempotent and recovery-only: only a still-`joining` Submission reverts; an
        // already-joined (or already-reverted) Submission is a no-op (DUR-016).
        if (submission.state !== "joining") return;
        yield* sql`
          UPDATE effect_agent_submissions
          SET state = 'ready', joined_host_submission_id = NULL
          WHERE submission_id = ${validated.submissionId}
        `.pipe(Effect.mapError(sqlFailure(operation)));
      }),
    );
    yield* hitFailpoint("ledger:revert-joining:after", operation);
  });

  const suspend: SubmissionLedger["Service"]["suspend"] = Effect.fn("DoSubmissionLedger.suspend")(
    function* (request: SuspendRequest) {
      const operation = "ledger suspend";
      const validated = yield* Schema.decodeUnknownEffect(Schema.toType(SuspendRequest))(
        request,
      ).pipe(Effect.mapError(internalFailure(operation)));
      const reasonJson = yield* encodeSuspensionReasonText(validated.reason).pipe(
        Effect.mapError(internalFailure(operation)),
      );
      yield* hitFailpoint("ledger:suspend:before", operation);
      const outcome = yield* inWriteTransaction(
        operation,
        Effect.gen(function* () {
          const submission = yield* requireSubmission(operation, validated.submissionId);
          if (submission.state === "settled") {
            if (submission.settled_outcome === null) {
              return yield* corruptionFailure(
                operation,
                "effect_agent_submissions",
                validated.submissionId,
                "A settled Submission carries no terminal outcome.",
              );
            }
            return yield* SettlementConflict.make({
              submissionId: validated.submissionId,
              existingOutcome: submission.settled_outcome,
            });
          }
          // An exact terminal outcome is already reserved (DUR-011); suspension would
          // contradict it, so the reservation wins.
          const reservation = yield* readReservation(operation, validated.submissionId);
          if (Option.isSome(reservation)) {
            const canonical = yield* validateStoredReservation(
              operation,
              submission,
              reservation.value,
            );
            return yield* SettlementConflict.make({
              submissionId: validated.submissionId,
              existingOutcome: canonical.outcome,
            });
          }
          yield* requireOwnership(operation, submission, validated.ownershipToken);
          // A covering event that raced ahead of the suspend transaction resumes the caller
          // immediately WITHOUT releasing the lane (plan §2.6, §12). For WaitingForChild the
          // covering evidence is EITHER a locally settled child row OR a durable cross-store
          // notification marker: parent and child Conversations live in different Durable
          // Objects, and the port contract requires that a child settlement reported (via
          // `recordChildSettled` → marker) before this suspend commits is observed here.
          if (validated.reason._tag === "ApprovalPending") {
            const decisions = yield* readApprovalDecisions(operation, validated.submissionId);
            const decided = new Set(decisions.map((row) => row.tool_call_id));
            if (validated.reason.toolCallIds.every((toolCallId) => decided.has(toolCallId))) {
              return "resume-immediately" as SuspensionOutcome;
            }
          } else {
            const markers = yield* readChildSettlementMarkers(operation, validated.submissionId);
            const markerChildren = new Set(markers.map((row) => row.child_submission_id));
            let allSettled = true;
            for (const child of validated.reason.children) {
              const settled = yield* childProvablySettled(
                operation,
                markerChildren,
                child.childSubmissionId,
              );
              if (!settled) {
                allSettled = false;
                break;
              }
            }
            if (allSettled) {
              return "resume-immediately" as SuspensionOutcome;
            }
          }
          const now = yield* currentInstant;
          yield* sql`
            UPDATE effect_agent_submissions
            SET
              state = 'suspended',
              suspended_reason_json = ${reasonJson},
              suspended_at = ${now.iso}
            WHERE submission_id = ${validated.submissionId}
          `.pipe(Effect.mapError(sqlFailure(operation)));
          // Suspension ends the ownership period WITHOUT settling: the accepted-work
          // obligation stays owed while the lane consumes no worker permit (plan §2.6).
          yield* sql`
            DELETE FROM effect_agent_submission_ownership
            WHERE submission_id = ${validated.submissionId}
          `.pipe(Effect.mapError(sqlFailure(operation)));
          return "suspended" as SuspensionOutcome;
        }),
      );
      yield* hitFailpoint("ledger:suspend:after", operation);
      return outcome;
    },
  );

  const resumeSuspension: SubmissionLedger["Service"]["resumeSuspension"] = Effect.fn(
    "DoSubmissionLedger.resumeSuspension",
  )(function* (request: ResumeSuspensionRequest) {
    const operation = "ledger resume suspension";
    const validated = yield* Schema.decodeUnknownEffect(Schema.toType(ResumeSuspensionRequest))(
      request,
    ).pipe(Effect.mapError(internalFailure(operation)));
    const expectedReasonJson = yield* encodeSuspensionReasonText(validated.expectedReason).pipe(
      Effect.mapError(internalFailure(operation)),
    );
    yield* hitFailpoint("ledger:resume-suspension:before", operation);
    const outcome = yield* inWriteTransaction(
      operation,
      Effect.gen(function* () {
        const submission = yield* requireSubmission(operation, validated.submissionId);
        if (submission.state !== "suspended") {
          return "not-suspended" as SuspensionResumeOutcome;
        }
        if (submission.suspended_reason_json === null || submission.suspended_at === null) {
          return yield* corruptionFailure(
            operation,
            "effect_agent_submissions",
            validated.submissionId,
            "A suspended Submission is missing its reason or timestamp.",
          );
        }
        if (submission.suspended_reason_json !== expectedReasonJson) {
          return yield* LedgerError.make({
            operation,
            message: `Stored suspension reason for submission ${validated.submissionId} does not match canonical evidence.`,
          });
        }
        if (validated.expectedReason._tag === "ApprovalPending") {
          const decisions = yield* readApprovalDecisions(operation, validated.submissionId);
          const decided = new Set(decisions.map((row) => row.tool_call_id));
          if (
            !validated.expectedReason.toolCallIds.every((toolCallId) => decided.has(toolCallId))
          ) {
            return "not-covered" as SuspensionResumeOutcome;
          }
        } else {
          const markers = yield* readChildSettlementMarkers(operation, validated.submissionId);
          const markerChildren = new Set(markers.map((row) => row.child_submission_id));
          for (const child of validated.expectedReason.children) {
            const settled = yield* childProvablySettled(
              operation,
              markerChildren,
              child.childSubmissionId,
            );
            if (!settled) return "not-covered" as SuspensionResumeOutcome;
          }
        }
        yield* sql`
          UPDATE effect_agent_submissions
          SET
            state = 'input-applied',
            suspended_reason_json = NULL,
            suspended_at = NULL
          WHERE submission_id = ${validated.submissionId}
        `.pipe(Effect.mapError(sqlFailure(operation)));
        return "resumed" as SuspensionResumeOutcome;
      }),
    );
    yield* hitFailpoint("ledger:resume-suspension:after", operation);
    return outcome;
  });

  const recordApprovalDecision: SubmissionLedger["Service"]["recordApprovalDecision"] = Effect.fn(
    "DoSubmissionLedger.recordApprovalDecision",
  )(function* (command: ApprovalDecisionCommand) {
    const operation = "ledger record approval decision";
    const validated = yield* Schema.decodeUnknownEffect(Schema.toType(ApprovalDecisionCommand))(
      command,
    ).pipe(Effect.mapError(internalFailure(operation)));
    yield* hitFailpoint("ledger:approval-decision:before", operation);
    const intent = yield* inWriteTransaction(
      operation,
      Effect.gen(function* () {
        const submission = yield* requireSubmission(operation, validated.submissionId);
        if (submission.state === "settled") {
          if (submission.settled_outcome === null) {
            return yield* corruptionFailure(
              operation,
              "effect_agent_submissions",
              validated.submissionId,
              "A settled Submission carries no terminal outcome.",
            );
          }
          return yield* SettlementConflict.make({
            submissionId: validated.submissionId,
            existingOutcome: submission.settled_outcome,
          });
        }
        const decisions = yield* readApprovalDecisions(operation, validated.submissionId);
        const existing = decisions.find((row) => row.tool_call_id === validated.toolCallId);
        if (existing !== undefined) {
          // Idempotent per (submissionId, toolCallId): repeating the SAME decision replays
          // the recorded intent unchanged; a divergent re-decision conflicts.
          if (existing.decision !== validated.decision) {
            return yield* ApprovalConflict.make({
              submissionId: validated.submissionId,
              toolCallId: validated.toolCallId,
              existingDecision: existing.decision,
            });
          }
          return yield* approvalIntentFromRow(operation, existing);
        }
        const now = yield* currentInstant;
        yield* sql`
          INSERT INTO effect_agent_approval_decisions (
            submission_id,
            tool_call_id,
            decision,
            resolver,
            reason,
            decided_at
          ) VALUES (
            ${validated.submissionId},
            ${validated.toolCallId},
            ${validated.decision},
            ${validated.resolver},
            ${validated.reason},
            ${now.iso}
          )
        `.pipe(Effect.mapError(sqlFailure(operation)));
        return yield* decodeApprovalDecisionIntent({
          submissionId: validated.submissionId,
          toolCallId: validated.toolCallId,
          decision: validated.decision,
          resolver: validated.resolver,
          reason: validated.reason,
          decidedAt: now.iso,
        }).pipe(Effect.mapError(internalFailure(operation)));
      }),
    );
    yield* hitFailpoint("ledger:approval-decision:after", operation);
    return intent;
  });

  const markUnknown: SubmissionLedger["Service"]["markUnknown"] = Effect.fn(
    "DoSubmissionLedger.markUnknown",
  )(function* (request: MarkUnknownRequest) {
    const operation = "ledger mark unknown";
    const validated = yield* Schema.decodeUnknownEffect(Schema.toType(MarkUnknownRequest))(
      request,
    ).pipe(Effect.mapError(internalFailure(operation)));
    yield* hitFailpoint("ledger:mark-unknown:before", operation);
    yield* inWriteTransaction(
      operation,
      Effect.gen(function* () {
        const submission = yield* requireSubmission(operation, validated.submissionId);
        if (submission.state === "settled") {
          if (submission.settled_outcome === null) {
            return yield* corruptionFailure(
              operation,
              "effect_agent_submissions",
              validated.submissionId,
              "A settled Submission carries no terminal outcome.",
            );
          }
          return yield* SettlementConflict.make({
            submissionId: validated.submissionId,
            existingOutcome: submission.settled_outcome,
          });
        }
        // A reserved exact outcome wins over a late Unknown marking (DUR-011); the recovery
        // classifier orders reservation ahead of MarkUnknown for the same reason.
        const reservation = yield* readReservation(operation, validated.submissionId);
        if (Option.isSome(reservation)) {
          const canonical = yield* validateStoredReservation(
            operation,
            submission,
            reservation.value,
          );
          return yield* SettlementConflict.make({
            submissionId: validated.submissionId,
            existingOutcome: canonical.outcome,
          });
        }
        // Idempotent merge: repeating is a no-op; additional open calls extend the marked
        // set while the first recorded reason is kept.
        const existingIds = yield* storedUnknownToolCallIds(operation, submission);
        const known = new Set(existingIds);
        const merged = [
          ...existingIds,
          ...validated.toolCallIds.filter((toolCallId) => !known.has(toolCallId)),
        ];
        const idsJson = yield* encodeToolCallIdsText(merged).pipe(
          Effect.mapError(internalFailure(operation)),
        );
        yield* sql`
          UPDATE effect_agent_submissions
          SET
            state = 'unknown',
            unknown_reason = ${submission.unknown_reason ?? validated.reason},
            unknown_tool_call_ids_json = ${idsJson}
          WHERE submission_id = ${validated.submissionId}
        `.pipe(Effect.mapError(sqlFailure(operation)));
      }),
    );
    yield* hitFailpoint("ledger:mark-unknown:after", operation);
  });

  const recordUnknownResolution: SubmissionLedger["Service"]["recordUnknownResolution"] = Effect.fn(
    "DoSubmissionLedger.recordUnknownResolution",
  )(function* (command: UnknownResolutionCommand) {
    const operation = "ledger record unknown resolution";
    const validated = yield* Schema.decodeUnknownEffect(Schema.toType(UnknownResolutionCommand))(
      command,
    ).pipe(Effect.mapError(internalFailure(operation)));
    const resolutionJson = yield* encodeUnknownResolutionText(validated.resolution).pipe(
      Effect.mapError(internalFailure(operation)),
    );
    yield* hitFailpoint("ledger:unknown-resolution:before", operation);
    const intent = yield* inWriteTransaction(
      operation,
      Effect.gen(function* () {
        const submission = yield* requireSubmission(operation, validated.submissionId);
        if (submission.state === "settled") {
          if (submission.settled_outcome === null) {
            return yield* corruptionFailure(
              operation,
              "effect_agent_submissions",
              validated.submissionId,
              "A settled Submission carries no terminal outcome.",
            );
          }
          return yield* SettlementConflict.make({
            submissionId: validated.submissionId,
            existingOutcome: submission.settled_outcome,
          });
        }
        const resolutions = yield* readUnknownResolutions(operation, validated.submissionId);
        const existing = resolutions.find((row) => row.tool_call_id === validated.toolCallId);
        if (existing !== undefined && existing.resolution_json !== resolutionJson) {
          return yield* UnknownResolutionConflict.make({
            submissionId: validated.submissionId,
            toolCallId: validated.toolCallId,
          });
        }
        let resolved: UnknownResolutionIntent;
        if (existing !== undefined) {
          // Idempotent replay of the recorded intent (author/reason may differ; the stored
          // audit fields win, exactly like requestAbort).
          resolved = yield* unknownResolutionIntentFromRow(operation, existing);
        } else {
          const now = yield* currentInstant;
          yield* sql`
            INSERT INTO effect_agent_unknown_resolutions (
              submission_id,
              tool_call_id,
              author,
              reason,
              resolution_json,
              resolved_at
            ) VALUES (
              ${validated.submissionId},
              ${validated.toolCallId},
              ${validated.author},
              ${validated.reason},
              ${resolutionJson},
              ${now.iso}
            )
          `.pipe(Effect.mapError(sqlFailure(operation)));
          const resolution = yield* parseStoredJsonText(resolutionJson).pipe(
            Effect.mapError(internalFailure(operation)),
          );
          resolved = yield* decodeUnknownResolutionIntent({
            submissionId: validated.submissionId,
            toolCallId: validated.toolCallId,
            author: validated.author,
            reason: validated.reason,
            resolution,
            resolvedAt: now.iso,
          }).pipe(Effect.mapError(internalFailure(operation)));
        }
        // The lane reopens only when EVERY marked open call has a durable resolution intent:
        // unknown → input-applied (DUR-017). Replays re-run the coverage check so a
        // recovering caller can wake the lane idempotently.
        if (submission.state === "unknown" && submission.unknown_tool_call_ids_json !== null) {
          const markedIds = yield* storedUnknownToolCallIds(operation, submission);
          const covering = yield* readUnknownResolutions(operation, validated.submissionId);
          const coveredIds = new Set(covering.map((row) => row.tool_call_id));
          if (markedIds.every((toolCallId) => coveredIds.has(toolCallId))) {
            yield* sql`
              UPDATE effect_agent_submissions
              SET
                state = 'input-applied',
                unknown_reason = NULL,
                unknown_tool_call_ids_json = NULL
              WHERE submission_id = ${validated.submissionId}
            `.pipe(Effect.mapError(sqlFailure(operation)));
          }
        }
        return resolved;
      }),
    );
    yield* hitFailpoint("ledger:unknown-resolution:after", operation);
    return intent;
  });

  const recordChildSettled: SubmissionLedger["Service"]["recordChildSettled"] = Effect.fn(
    "DoSubmissionLedger.recordChildSettled",
  )(function* (request: ChildSettledNotification) {
    const operation = "ledger record child settled";
    const validated = yield* Schema.decodeUnknownEffect(Schema.toType(ChildSettledNotification))(
      request,
    ).pipe(Effect.mapError(internalFailure(operation)));
    yield* hitFailpoint("ledger:child-settled:before", operation);
    const outcome = yield* inWriteTransaction(
      operation,
      Effect.gen(function* () {
        const parent = yield* requireSubmission(operation, validated.parentSubmissionId);
        // The child's canonical Settlement is the authority for this wake. When the child's row
        // lives in THIS store (single-store latitude, and every conformance lane), only a settled
        // row with its exact finalized reservation admits the notification; `terminalizing`
        // remains a disposable prefix and defers to canonical repair. When the row does not live
        // here — the normal cross-DO case — the routed notification from the child's owning
        // Durable Object is the settlement evidence this store records durably.
        const child = yield* readSubmission(operation, validated.childSubmissionId);
        if (
          Option.isSome(child) &&
          !(yield* localChildSettlementNotificationCovered(operation, child.value))
        ) {
          // A canonical settlement may exist while this local projection is still preterminal.
          // Admit repair without recording parent coverage; the post-repair replay observes the
          // repaired terminal child and records the notification atomically.
          return "child-not-terminal" as ChildSettledOutcome;
        }
        const childReservation = yield* readReservation(operation, validated.childSubmissionId);
        // Record the durable notification marker FIRST and unconditionally (idempotent):
        // the port's cross-store race guarantee requires that a notification committed
        // before the parent's suspend transaction is observed by that suspend's covering
        // check, even when the parent is not (or not yet) suspended.
        const now = yield* currentInstant;
        yield* sql`
          INSERT INTO effect_agent_child_settlements (
            parent_submission_id,
            child_submission_id,
            child_outcome,
            recorded_at
          ) VALUES (
            ${validated.parentSubmissionId},
            ${validated.childSubmissionId},
            ${
              Option.isSome(child) && child.value.state === "settled"
                ? child.value.settled_outcome
                : Option.isSome(childReservation)
                  ? childReservation.value.outcome
                  : null
            },
            ${now.iso}
          )
          ON CONFLICT (parent_submission_id, child_submission_id) DO NOTHING
        `.pipe(Effect.mapError(sqlFailure(operation)));

        if (parent.state !== "suspended" || parent.suspended_reason_json === null) {
          return "not-waiting" as ChildSettledOutcome;
        }
        const reason = yield* Schema.decodeEffect(Schema.fromJsonString(SuspensionReason))(
          parent.suspended_reason_json,
        ).pipe(
          Effect.mapError((error) =>
            corruptionFailure(
              operation,
              "effect_agent_submissions",
              parent.submission_id,
              error.message,
            ),
          ),
        );
        if (reason._tag !== "WaitingForChild") {
          return "not-waiting" as ChildSettledOutcome;
        }
        if (
          !reason.children.some((entry) => entry.childSubmissionId === validated.childSubmissionId)
        ) {
          return "not-waiting" as ChildSettledOutcome;
        }
        return "still-waiting" as ChildSettledOutcome;
      }),
    );
    yield* hitFailpoint("ledger:child-settled:after", operation);
    return outcome;
  });

  const reserveChildBudget: SubmissionLedger["Service"]["reserveChildBudget"] = Effect.fn(
    "DoSubmissionLedger.reserveChildBudget",
  )(function* (request: ChildBudgetReservationRequest) {
    const operation = "ledger reserve child budget";
    const validated = yield* Schema.decodeUnknownEffect(
      Schema.toType(ChildBudgetReservationRequest),
    )(request).pipe(Effect.mapError(internalFailure(operation)));
    const allocationJson = yield* encodePersistedJsonText(validated.allocation).pipe(
      Effect.mapError(internalFailure(operation)),
    );
    yield* hitFailpoint("ledger:child-reservation:before", operation);
    const reserved = yield* inWriteTransaction(
      operation,
      Effect.gen(function* () {
        const existing = yield* readChildReservation(operation, validated.reservationId);
        if (Option.isSome(existing)) {
          // Identical replays short-circuit before the fence, mirroring reserveSettlement: a
          // replay creates nothing, so a recovering caller resumes rather than duplicates.
          const identical =
            existing.value.parent_submission_id === validated.parentSubmissionId &&
            existing.value.parent_tool_call_id === validated.parentToolCallId &&
            existing.value.allocation_digest === validated.allocationDigest &&
            existing.value.allocation_json === allocationJson;
          if (!identical) {
            return yield* ChildReservationConflict.make({
              reservationId: validated.reservationId,
              status: existing.value.status,
              message:
                "A reservation with this identity exists with a different parent Tool Call or allocation.",
            });
          }
          return ReservedChildBudget.make({
            reservation: yield* childReservationSnapshotFromRow(operation, existing.value),
            replayed: true,
          });
        }
        const collision = yield* readChildReservationForCall(
          operation,
          validated.parentSubmissionId,
          validated.parentToolCallId,
        );
        if (Option.isSome(collision)) {
          return yield* ChildReservationConflict.make({
            reservationId: validated.reservationId,
            status: collision.value.status,
            message: `Parent Tool Call ${validated.parentToolCallId} already owns reservation ${collision.value.reservation_id}.`,
          });
        }
        const parent = yield* requireSubmission(operation, validated.parentSubmissionId);
        // Creation is fenced by the parent lane's live ownership (spec §12 step 2): a stale
        // parent Attempt can never create new reservation state.
        yield* requireOwnership(operation, parent, validated.ownershipToken);
        const now = yield* currentInstant;
        yield* sql`
          INSERT INTO effect_agent_child_reservations (
            reservation_id,
            parent_submission_id,
            parent_tool_call_id,
            status,
            allocation_json,
            allocation_digest,
            reserved_at
          ) VALUES (
            ${validated.reservationId},
            ${validated.parentSubmissionId},
            ${validated.parentToolCallId},
            'reserved',
            ${allocationJson},
            ${validated.allocationDigest},
            ${now.iso}
          )
        `.pipe(Effect.mapError(sqlFailure(operation)));
        const inserted = yield* readChildReservation(operation, validated.reservationId);
        if (Option.isNone(inserted)) {
          return yield* corruptionFailure(
            operation,
            "effect_agent_child_reservations",
            validated.reservationId,
            "An inserted child reservation row is missing inside its own transaction.",
          );
        }
        return ReservedChildBudget.make({
          reservation: yield* childReservationSnapshotFromRow(operation, inserted.value),
          replayed: false,
        });
      }),
    );
    yield* hitFailpoint("ledger:child-reservation:after", operation);
    return reserved;
  });

  const attachChildToReservation: SubmissionLedger["Service"]["attachChildToReservation"] =
    Effect.fn("DoSubmissionLedger.attachChildToReservation")(function* (
      request: AttachChildToReservationRequest,
    ) {
      const operation = "ledger attach child to reservation";
      const validated = yield* Schema.decodeUnknownEffect(
        Schema.toType(AttachChildToReservationRequest),
      )(request).pipe(Effect.mapError(internalFailure(operation)));
      yield* hitFailpoint("ledger:child-attach:before", operation);
      const attached = yield* inWriteTransaction(
        operation,
        Effect.gen(function* () {
          const existing = yield* readChildReservation(operation, validated.reservationId);
          if (Option.isNone(existing)) {
            return yield* LedgerError.make({
              operation,
              message: `Unknown child reservation ${validated.reservationId}.`,
            });
          }
          if (existing.value.child_submission_id !== null) {
            // Idempotent replay of the recorded attachment (unfenced — it mutates nothing).
            if (existing.value.child_submission_id === validated.childSubmissionId) {
              return yield* childReservationSnapshotFromRow(operation, existing.value);
            }
            return yield* ChildReservationConflict.make({
              reservationId: validated.reservationId,
              status: existing.value.status,
              message: `Reservation ${validated.reservationId} already records child ${existing.value.child_submission_id}.`,
            });
          }
          const parent = yield* requireSubmission(operation, existing.value.parent_submission_id);
          yield* requireOwnership(operation, parent, validated.ownershipToken);
          if (existing.value.status !== "reserved") {
            return yield* ChildReservationConflict.make({
              reservationId: validated.reservationId,
              status: existing.value.status,
              message: `Cannot attach a child to a ${existing.value.status} reservation.`,
            });
          }
          // Unlike the single-store Node adapter, the admitted child's row lives in ANOTHER
          // Durable Object, so no local existence check is possible here. The canonical
          // `SubagentStarted` record remains the attachment's repair authority (DUR-015),
          // and the coordinator only attaches after the child's admission committed.
          yield* sql`
            UPDATE effect_agent_child_reservations
            SET child_submission_id = ${validated.childSubmissionId}
            WHERE reservation_id = ${validated.reservationId}
          `.pipe(Effect.mapError(sqlFailure(operation)));
          const updated = yield* readChildReservation(operation, validated.reservationId);
          if (Option.isNone(updated)) {
            return yield* corruptionFailure(
              operation,
              "effect_agent_child_reservations",
              validated.reservationId,
              "An updated child reservation row is missing inside its own transaction.",
            );
          }
          return yield* childReservationSnapshotFromRow(operation, updated.value);
        }),
      );
      yield* hitFailpoint("ledger:child-attach:after", operation);
      return attached;
    });

  const beginChildBudgetRelease: SubmissionLedger["Service"]["beginChildBudgetRelease"] = Effect.fn(
    "DoSubmissionLedger.beginChildBudgetRelease",
  )(function* (request: BeginChildBudgetReleaseRequest) {
    const operation = "ledger begin child budget release";
    const validated = yield* Schema.decodeUnknownEffect(
      Schema.toType(BeginChildBudgetReleaseRequest),
    )(request).pipe(Effect.mapError(internalFailure(operation)));
    const accountingJson = yield* encodePersistedJsonText(validated.accounting).pipe(
      Effect.mapError(internalFailure(operation)),
    );
    yield* hitFailpoint("ledger:child-release-pending:before", operation);
    const frozen = yield* inWriteTransaction(
      operation,
      Effect.gen(function* () {
        const existing = yield* readChildReservation(operation, validated.reservationId);
        if (Option.isNone(existing)) {
          return yield* LedgerError.make({
            operation,
            message: `Unknown child reservation ${validated.reservationId}.`,
          });
        }
        if (existing.value.status !== "reserved") {
          // The accounting decision was already frozen exactly once; an identical replay is a
          // no-op and a divergent decision conflicts (spec §12 join step 6).
          if (existing.value.accounting_json === accountingJson) {
            return yield* childReservationSnapshotFromRow(operation, existing.value);
          }
          return yield* ChildReservationConflict.make({
            reservationId: validated.reservationId,
            status: existing.value.status,
            message: "A different accounting decision is already frozen for this reservation.",
          });
        }
        const now = yield* currentInstant;
        yield* sql`
          UPDATE effect_agent_child_reservations
          SET
            status = 'releasePending',
            accounting_json = ${accountingJson},
            release_began_at = ${now.iso}
          WHERE reservation_id = ${validated.reservationId}
        `.pipe(Effect.mapError(sqlFailure(operation)));
        const updated = yield* readChildReservation(operation, validated.reservationId);
        if (Option.isNone(updated)) {
          return yield* corruptionFailure(
            operation,
            "effect_agent_child_reservations",
            validated.reservationId,
            "An updated child reservation row is missing inside its own transaction.",
          );
        }
        return yield* childReservationSnapshotFromRow(operation, updated.value);
      }),
    );
    yield* hitFailpoint("ledger:child-release-pending:after", operation);
    return frozen;
  });

  const releaseChildBudget: SubmissionLedger["Service"]["releaseChildBudget"] = Effect.fn(
    "DoSubmissionLedger.releaseChildBudget",
  )(function* (request: ReleaseChildBudgetRequest) {
    const operation = "ledger release child budget";
    const validated = yield* Schema.decodeUnknownEffect(Schema.toType(ReleaseChildBudgetRequest))(
      request,
    ).pipe(Effect.mapError(internalFailure(operation)));
    yield* hitFailpoint("ledger:child-release:before", operation);
    const released = yield* inWriteTransaction(
      operation,
      Effect.gen(function* () {
        const existing = yield* readChildReservation(operation, validated.reservationId);
        if (Option.isNone(existing)) {
          return yield* LedgerError.make({
            operation,
            message: `Unknown child reservation ${validated.reservationId}.`,
          });
        }
        // Applied exactly once: replaying a released reservation returns the stored row
        // unchanged (spec §12: "never available twice").
        if (existing.value.status === "released") {
          return yield* childReservationSnapshotFromRow(operation, existing.value);
        }
        if (existing.value.status !== "releasePending") {
          return yield* ChildReservationConflict.make({
            reservationId: validated.reservationId,
            status: existing.value.status,
            message: "Cannot release a reservation whose accounting decision is not frozen.",
          });
        }
        const now = yield* currentInstant;
        yield* sql`
          UPDATE effect_agent_child_reservations
          SET status = 'released', released_at = ${now.iso}
          WHERE reservation_id = ${validated.reservationId}
        `.pipe(Effect.mapError(sqlFailure(operation)));
        const updated = yield* readChildReservation(operation, validated.reservationId);
        if (Option.isNone(updated)) {
          return yield* corruptionFailure(
            operation,
            "effect_agent_child_reservations",
            validated.reservationId,
            "An updated child reservation row is missing inside its own transaction.",
          );
        }
        return yield* childReservationSnapshotFromRow(operation, updated.value);
      }),
    );
    yield* hitFailpoint("ledger:child-release:after", operation);
    return released;
  });

  interface ScanCursor {
    readonly conversationId: string;
    readonly queueSequence: number;
  }

  const scanPage = Effect.fn("DoSubmissionLedger.scanPage")(function* (
    cursor: ScanCursor | undefined,
  ): Effect.fn.Return<
    readonly [ReadonlyArray<SubmissionSnapshot>, Option.Option<ScanCursor | undefined>],
    LedgerError
  > {
    const operation = "ledger scan nonterminal";
    const rows = yield* (
      cursor === undefined
        ? sql<Record<string, unknown>>`
          SELECT ${sql.literal(SUBMISSION_COLUMNS)}
          FROM effect_agent_submissions
          WHERE state <> 'settled'
          ORDER BY conversation_id ASC, queue_sequence ASC
          LIMIT ${SCAN_PAGE_SIZE}
        `
        : sql<Record<string, unknown>>`
          SELECT ${sql.literal(SUBMISSION_COLUMNS)}
          FROM effect_agent_submissions
          WHERE state <> 'settled'
            AND (
              conversation_id > ${cursor.conversationId}
              OR (
                conversation_id = ${cursor.conversationId}
                AND queue_sequence > ${cursor.queueSequence}
              )
            )
          ORDER BY conversation_id ASC, queue_sequence ASC
          LIMIT ${SCAN_PAGE_SIZE}
        `
    ).pipe(Effect.mapError(sqlFailure(operation)));
    const decoded = yield* decodeSubmissionRows(operation, "nonterminal_scan", rows);
    const snapshots = yield* Effect.forEach(decoded, (row) =>
      decodeSubmissionSnapshot(operation, row),
    );
    const last = decoded[decoded.length - 1];
    const next: Option.Option<ScanCursor | undefined> =
      last === undefined || decoded.length < SCAN_PAGE_SIZE
        ? Option.none()
        : Option.some({
            conversationId: last.conversation_id,
            queueSequence: last.queue_sequence,
          });
    return [snapshots, next] as const;
  });

  const scanNonterminal: Stream.Stream<SubmissionSnapshot, LedgerError> = Stream.paginate(
    undefined as ScanCursor | undefined,
    scanPage,
  );

  const scanConversationNonterminal: SubmissionLedger["Service"]["scanConversationNonterminal"] = (
    request,
  ) =>
    Stream.unwrap(
      Schema.decodeUnknownEffect(Schema.toType(ScanConversationNonterminalRequest))(request).pipe(
        Effect.mapError(internalFailure("ledger scan conversation nonterminal")),
        Effect.map((validated) =>
          Stream.paginate(validated.afterQueueSequence, (cursor) =>
            Effect.gen(function* () {
              const operation = "ledger scan conversation nonterminal";
              const rows = yield* (
                cursor === undefined
                  ? sql<Record<string, unknown>>`
                      SELECT ${sql.literal(SUBMISSION_COLUMNS)}
                      FROM effect_agent_submissions
                      WHERE conversation_id = ${validated.conversationId}
                        AND state <> 'settled'
                      ORDER BY queue_sequence ASC
                      LIMIT ${SCAN_PAGE_SIZE}
                    `
                  : sql<Record<string, unknown>>`
                      SELECT ${sql.literal(SUBMISSION_COLUMNS)}
                      FROM effect_agent_submissions
                      WHERE conversation_id = ${validated.conversationId}
                        AND state <> 'settled'
                        AND queue_sequence > ${cursor}
                      ORDER BY queue_sequence ASC
                      LIMIT ${SCAN_PAGE_SIZE}
                    `
              ).pipe(Effect.mapError(sqlFailure(operation)));
              const decoded = yield* decodeSubmissionRows(
                operation,
                validated.conversationId,
                rows,
              );
              const snapshots = yield* Effect.forEach(decoded, (row) =>
                decodeSubmissionSnapshot(operation, row),
              );
              const last = decoded[decoded.length - 1];
              const next: Option.Option<SubmissionSnapshot["queueSequence"] | undefined> =
                last === undefined || decoded.length < SCAN_PAGE_SIZE
                  ? Option.none()
                  : Option.some(last.queue_sequence);
              return [snapshots, next] as const;
            }),
          ),
        ),
      ),
    );

  const loadRecoverySnapshot: SubmissionLedger["Service"]["loadRecoverySnapshot"] = Effect.fn(
    "DoSubmissionLedger.loadRecoverySnapshot",
  )(function* (request: RecoverySnapshotRequest) {
    const operation = "ledger load recovery snapshot";
    const validated = yield* Schema.decodeUnknownEffect(Schema.toType(RecoverySnapshotRequest))(
      request,
    ).pipe(Effect.mapError(internalFailure(operation)));
    return yield* sql
      .withTransaction(
        Effect.gen(function* () {
          const submissionRow = yield* requireSubmission(operation, validated.submissionId);
          const submission = yield* decodeSubmissionSnapshot(operation, submissionRow);

          let ownership: OwnershipSnapshot | undefined;
          const ownershipRow = yield* readOwnership(operation, validated.submissionId);
          if (Option.isSome(ownershipRow)) {
            ownership = yield* decodeOwnershipSnapshot({
              attemptId: ownershipRow.value.attempt_id,
              ownerProducerId: ownershipRow.value.owner_producer_id,
              producerEpoch: ownershipRow.value.producer_epoch,
              leaseExpiresAt: ownershipRow.value.lease_expires_at,
            }).pipe(Effect.mapError(internalFailure(operation)));
          }

          let inputApplied: InputAppliedMarker | undefined;
          if (
            submissionRow.input_applied_record_id !== null &&
            submissionRow.input_applied_sequence !== null
          ) {
            inputApplied = yield* decodeInputAppliedMarker({
              recordId: submissionRow.input_applied_record_id,
              sequence: submissionRow.input_applied_sequence,
            }).pipe(Effect.mapError(internalFailure(operation)));
          }

          const recoveryReservation = yield* readRecoveryReservation(operation, submissionRow);

          let abortIntent: AbortIntent | undefined;
          const abortRow = yield* readAbortIntent(operation, validated.submissionId);
          if (Option.isSome(abortRow)) {
            abortIntent = yield* abortIntentFromRow(
              operation,
              submissionRow,
              validated.submissionId,
              abortRow.value,
            );
          }

          // Host-side view: every Submission whose host linkage points here, in queue order
          // (the terminalize loop settles them with the host outcome, DUR-002).
          const joinRows = yield* sql<Record<string, unknown>>`
            SELECT ${sql.literal(SUBMISSION_COLUMNS)}
            FROM effect_agent_submissions
            WHERE joined_host_submission_id = ${validated.submissionId}
            ORDER BY queue_sequence ASC
          `.pipe(Effect.mapError(sqlFailure(operation)));
          const joinSubmissions = yield* decodeSubmissionRows(
            operation,
            validated.submissionId,
            joinRows,
          );
          const joins = yield* Effect.forEach(joinSubmissions, (row) =>
            decodeJoinSnapshot({
              submissionId: row.submission_id,
              state: row.state,
              hostSubmissionId: validated.submissionId,
            }).pipe(Effect.mapError(internalFailure(operation))),
          );

          let hostSubmissionId: RecoverySnapshot["hostSubmissionId"];
          if (submissionRow.joined_host_submission_id !== null) {
            hostSubmissionId = yield* decodeSubmissionId(
              submissionRow.joined_host_submission_id,
            ).pipe(Effect.mapError(internalFailure(operation)));
          }

          let suspension: SuspensionSnapshot | undefined;
          if (submissionRow.suspended_reason_json !== null && submissionRow.suspended_at !== null) {
            const reason = yield* parseStoredJsonText(submissionRow.suspended_reason_json).pipe(
              Effect.mapError((error) =>
                corruptionFailure(
                  operation,
                  "effect_agent_submissions",
                  validated.submissionId,
                  error.message,
                ),
              ),
            );
            suspension = yield* decodeSuspensionSnapshot({
              reason,
              suspendedAt: submissionRow.suspended_at,
            }).pipe(
              Effect.mapError((error) =>
                corruptionFailure(
                  operation,
                  "effect_agent_submissions",
                  validated.submissionId,
                  error.message,
                ),
              ),
            );
          }

          const decisionRows = yield* readApprovalDecisions(operation, validated.submissionId);
          const approvalDecisions = yield* Effect.forEach(decisionRows, (row) =>
            approvalIntentFromRow(operation, row),
          );

          const resolutionRows = yield* readUnknownResolutions(operation, validated.submissionId);
          const unknownResolutions = yield* Effect.forEach(resolutionRows, (row) =>
            unknownResolutionIntentFromRow(operation, row),
          );

          // Parent-side subagent view: this Submission's child budget reservations in parent
          // Tool Call order, plus each attached child's current lane state (a disposable
          // derived view; canonical records stay the recovery truth, DUR-015). The child's
          // state comes from its local row when this store holds it, and otherwise from the
          // durable cross-store settlement marker; a child that is neither local nor marked
          // settled is enriched by the routed per-child lookup one layer out (plan §1.3).
          const childReservationRows = yield* sql<Record<string, unknown>>`
            SELECT ${sql.literal(CHILD_RESERVATION_COLUMNS)}
            FROM effect_agent_child_reservations
            WHERE parent_submission_id = ${validated.submissionId}
            ORDER BY parent_tool_call_id ASC
          `.pipe(Effect.mapError(sqlFailure(operation)));
          const decodedChildReservations = yield* decodeChildReservationRows(
            operation,
            validated.submissionId,
            childReservationRows,
          );
          const childReservations = yield* Effect.forEach(decodedChildReservations, (row) =>
            childReservationSnapshotFromRow(operation, row),
          );
          const markers = yield* readChildSettlementMarkers(operation, validated.submissionId);
          const markersByChild = new Map(markers.map((row) => [row.child_submission_id, row]));
          const childAttachments: Array<ChildAttachmentSnapshot> = [];
          for (const row of decodedChildReservations) {
            if (row.child_submission_id === null) continue;
            const child = yield* readSubmission(operation, row.child_submission_id);
            if (Option.isSome(child)) {
              childAttachments.push(
                yield* decodeChildAttachmentSnapshot({
                  toolCallId: row.parent_tool_call_id,
                  childSubmissionId: row.child_submission_id,
                  childState: child.value.state,
                  ...(child.value.settled_outcome === null
                    ? {}
                    : { childOutcome: child.value.settled_outcome }),
                }).pipe(Effect.mapError(internalFailure(operation))),
              );
              continue;
            }
            const marker = markersByChild.get(row.child_submission_id);
            if (marker === undefined) continue;
            childAttachments.push(
              yield* decodeChildAttachmentSnapshot({
                toolCallId: row.parent_tool_call_id,
                childSubmissionId: row.child_submission_id,
                childState: "settled",
                ...(marker.child_outcome === null ? {} : { childOutcome: marker.child_outcome }),
              }).pipe(Effect.mapError(internalFailure(operation))),
            );
          }

          let parentLinkage: ParentLinkage | undefined;
          if (
            submissionRow.parent_submission_id !== null &&
            submissionRow.parent_tool_call_id !== null
          ) {
            parentLinkage = yield* decodeParentLinkage({
              parentSubmissionId: submissionRow.parent_submission_id,
              parentToolCallId: submissionRow.parent_tool_call_id,
            }).pipe(Effect.mapError(internalFailure(operation)));
          }

          return RecoverySnapshot.make({
            submission,
            ...recoveryReservation,
            joins,
            approvalDecisions,
            unknownResolutions,
            childReservations,
            childAttachments,
            ...(parentLinkage === undefined ? {} : { parentLinkage }),
            ...(hostSubmissionId === undefined ? {} : { hostSubmissionId }),
            ...(suspension === undefined ? {} : { suspension }),
            ...(ownership === undefined ? {} : { ownership }),
            ...(inputApplied === undefined ? {} : { inputApplied }),
            ...(abortIntent === undefined ? {} : { abortIntent }),
          });
        }),
      )
      .pipe(Effect.catchTag("SqlError", (error) => Effect.fail(sqlFailure(operation)(error))));
  });

  return Context.make(
    SubmissionLedger,
    SubmissionLedger.of({
      capabilities,
      admit,
      markReady,
      lookup,
      resolveAdmission,
      claim,
      renewOwnership,
      releaseOwnership,
      markInputApplied,
      reserveSettlement,
      finalizeSettlement,
      repairSettlementFromCanonical,
      requestAbort,
      claimJoining,
      markJoined,
      revertJoining,
      suspend,
      resumeSuspension,
      recordApprovalDecision,
      markUnknown,
      recordUnknownResolution,
      recordChildSettled,
      reserveChildBudget,
      attachChildToReservation,
      beginChildBudgetRelease,
      releaseChildBudget,
      scanNonterminal,
      scanConversationNonterminal,
      loadRecoverySnapshot,
    }),
  );
});

/**
 * Durable Object SubmissionLedger implementation sharing the journal's private SQLite
 * database, storage-backed transaction discipline, and producer-epoch fencing substrate.
 * Configuration, failpoint, SQL, and Crypto authority stay visible in the input channel.
 */
export const submissionLedgerLayer: Layer.Layer<
  SubmissionLedger,
  DoStorageInitializationError,
  DoStorageConfig | DoStorageFailpoint | SqlClientService.SqlClient | Crypto.Crypto
> = Layer.effectContext(makeServices());

/**
 * A composition-root convenience Layer for the durable Submission Ledger. Point it at the
 * same `ctx.storage` as the ConversationStore so claims fence the same producer epochs.
 */
export const ledgerLayer = (
  options: DoStorageOptions,
): Layer.Layer<SubmissionLedger, DoStorageInitializationError> =>
  submissionLedgerLayer.pipe(
    Layer.provide(
      Layer.mergeAll(
        storageConfigLayer(options),
        storageFailpointLayer(options),
        SqliteClient.layer({ storage: options.storage }),
        BrowserCrypto.layer,
      ),
    ),
  );

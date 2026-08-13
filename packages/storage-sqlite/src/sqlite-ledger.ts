import { NodeCrypto } from "@effect/platform-node";
import { SqliteClient } from "@effect/sql-sqlite-node";
import {
  AbortCommand,
  AbortIntent,
  AdmissionConflict,
  AdmissionRequest,
  AdmissionResult,
  CanonicalSequence,
  Claim,
  ClaimRequest,
  DefinitionDigests,
  Digest,
  EMPTY_TAIL_DIGEST,
  InputAppliedMarker,
  LedgerCapabilities,
  LedgerError,
  MarkInputAppliedRequest,
  MarkReadyRequest,
  OwnershipLost,
  OwnershipRenewal,
  OwnershipSnapshot,
  PersistedJson,
  ProducerEpoch,
  QueueSequence,
  RecordEnvelope,
  RecoverySnapshot,
  RecoverySnapshotRequest,
  ReleaseOwnershipRequest,
  RenewOwnershipRequest,
  ReservedSettlement,
  Settlement,
  SettlementConflict,
  SettlementFinalization,
  SettlementOutcome,
  SettlementReservation,
  SubmissionLedger,
  SubmissionLookup,
  SubmissionSnapshot,
  SubmissionState,
  SettlementReservationSnapshot,
  submissionAbortRecordId,
} from "@effect-agent/session";
import { Clock, Context, Crypto, DateTime, Effect, Layer, Option, Schema, Stream } from "effect";
import * as SqlClientService from "effect/unstable/sql/SqlClient";
import type { SqlError } from "effect/unstable/sql/SqlError";
import {
  SqliteLedgerError,
  SqliteStorageCorruptionError,
  SqliteStorageError,
  SqliteWriteContention,
  type SqliteStorageFailpointLocation,
} from "./errors.ts";
import {
  storageConfigLayer,
  storageFailpointLayer,
  type SqliteStorageInitializationError,
  type SqliteStorageOptions,
} from "./sqlite-conversation-store.ts";
import { decodeRows, initializeSqliteJournal } from "./sqlite-journal.ts";
import { SqliteStorageConfig } from "./sqlite-storage-config.ts";
import { SqliteStorageFailpoint } from "./sqlite-storage-failpoint.ts";

type SubmissionId = SubmissionSnapshot["submissionId"];

const BoundedStoredText = Schema.String.check(Schema.isMaxLength(16 * 1024 * 1024));
const BoundedIdentifier = Schema.NonEmptyString.check(Schema.isMaxLength(1024));
const BoundedTimestamp = Schema.NonEmptyString.check(Schema.isMaxLength(128));

const SCAN_PAGE_SIZE = 256;
const EPOCH_ZERO = Schema.decodeSync(ProducerEpoch)(0);

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
  input_applied_sequence
`;

const encodePersistedJsonText = Schema.encodeEffect(Schema.fromJsonString(PersistedJson));
const encodeDefinitionDigestsText = Schema.encodeEffect(Schema.fromJsonString(DefinitionDigests));
const encodeRecordEnvelopeText = Schema.encodeEffect(Schema.fromJsonString(RecordEnvelope));
const decodeRecordEnvelopeText = Schema.decodeEffect(Schema.fromJsonString(RecordEnvelope));
const parseStoredJsonText = Schema.decodeEffect(Schema.fromJsonString(Schema.Json));
const decodeAdmissionResult = Schema.decodeUnknownEffect(AdmissionResult);
const decodeClaim = Schema.decodeUnknownEffect(Claim);
const decodeOwnershipRenewal = Schema.decodeUnknownEffect(OwnershipRenewal);
const decodeSettlement = Schema.decodeUnknownEffect(Settlement);
const decodeAbortIntent = Schema.decodeUnknownEffect(AbortIntent);
const decodeOwnershipSnapshot = Schema.decodeUnknownEffect(OwnershipSnapshot);
const decodeInputAppliedMarker = Schema.decodeUnknownEffect(InputAppliedMarker);
const decodeSubmissionSnapshotUnknown = Schema.decodeUnknownEffect(SubmissionSnapshot);
const decodeQueueSequence = Schema.decodeUnknownEffect(QueueSequence);
const decodeUtcInstant = Schema.decodeUnknownEffect(Schema.DateTimeUtcFromString);

/** Wrap an adapter-internal failure into the port's LedgerError without erasing its tag. */
const internalFailure =
  (operation: string) =>
  (error: { readonly message: string }): LedgerError =>
    LedgerError.make({ operation, message: error.message, cause: error });

/** Classify raw SQL failures: write-lock timeouts stay retryable typed contention. */
const sqlFailure =
  (operation: string) =>
  (error: SqlError): LedgerError => {
    const internal =
      error.reason._tag === "LockTimeoutError"
        ? SqliteWriteContention.make({
            cause: error,
            operation,
            message: `Another producer holds the SQLite write lock; ${operation} is safe to retry.`,
          })
        : SqliteLedgerError.make({
            cause: error,
            operation,
            message: error.message,
          });
    return internalFailure(operation)(internal);
  };

const corruptionFailure = (operation: string, table: string, rowKey: string, message: string) =>
  internalFailure(operation)(SqliteStorageCorruptionError.make({ table, rowKey, message }));

const makeServices = Effect.fn("SqliteSubmissionLedger.makeServices")(function* () {
  const config = yield* SqliteStorageConfig;
  const failpoint = yield* SqliteStorageFailpoint;
  const sql = yield* SqlClientService.SqlClient;
  const crypto = yield* Crypto.Crypto;
  const journal = yield* initializeSqliteJournal(sql, failpoint.hit, config.busyTimeout);

  const hitFailpoint = (
    location: SqliteStorageFailpointLocation,
    operation: string,
  ): Effect.Effect<void, LedgerError> =>
    failpoint.hit(location).pipe(Effect.mapError((error) => internalFailure(operation)(error)));

  /**
   * Run one ledger mutation under the journal's `BEGIN IMMEDIATE` write transaction so
   * ownership-token and epoch checks are atomic with their writes (DUR-006). Transaction
   * acquisition failures surface as LedgerError carrying the typed retryable
   * SqliteWriteContention (or SqliteStorageError) as cause.
   */
  const inWriteTransaction = <
    A,
    E extends AdmissionConflict | OwnershipLost | SettlementConflict | LedgerError,
  >(
    operation: string,
    effect: Effect.Effect<A, E>,
  ): Effect.Effect<A, E | LedgerError> =>
    journal
      .withWriteTransaction(operation)(effect)
      .pipe(
        Effect.mapError((error) =>
          error instanceof SqliteStorageError || error instanceof SqliteWriteContention
            ? internalFailure(operation)(error)
            : error,
        ),
      );

  const mintIdentifier = (prefix: string, operation: string): Effect.Effect<string, LedgerError> =>
    crypto.randomUUIDv7.pipe(
      Effect.map((uuid) => `${prefix}-${uuid}`),
      Effect.mapError((error) => internalFailure(operation)(error)),
    );

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
    decodeRows(Schema.Array(SubmissionRow), "effect_agent_submissions", rowKey, rows).pipe(
      Effect.mapError(internalFailure(operation)),
    );

  const readSubmission = Effect.fn("SqliteSubmissionLedger.readSubmission")(function* (
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

  const requireSubmission = Effect.fn("SqliteSubmissionLedger.requireSubmission")(function* (
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

  const readOwnership = Effect.fn("SqliteSubmissionLedger.readOwnership")(function* (
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

  const conversationEpoch = Effect.fn("SqliteSubmissionLedger.conversationEpoch")(function* (
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
  const requireOwnership = Effect.fn("SqliteSubmissionLedger.requireOwnership")(function* (
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

  const decodeSubmissionSnapshot = Effect.fn("SqliteSubmissionLedger.decodeSubmissionSnapshot")(
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

  const readReservation = Effect.fn("SqliteSubmissionLedger.readReservation")(function* (
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

  const readAbortIntent = Effect.fn("SqliteSubmissionLedger.readAbortIntent")(function* (
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
   * Canonical history is the abort authority (DUR-015): the intent's canonicalRecordId is
   * derived from the shared canonical-records table using the deterministic abort record
   * identity, never from a cached ledger marker.
   */
  const canonicalAbortRecordId = Effect.fn("SqliteSubmissionLedger.canonicalAbortRecordId")(
    function* (
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
    },
  );

  const abortIntentFromRow = Effect.fn("SqliteSubmissionLedger.abortIntentFromRow")(function* (
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

  const capabilities = Effect.succeed(LedgerCapabilities.make({ durability: "durable-node" }));

  const admit: SubmissionLedger["Service"]["admit"] = Effect.fn("SqliteSubmissionLedger.admit")(
    function* (request: AdmissionRequest) {
      const operation = "ledger admit";
      const validated = yield* Schema.decodeUnknownEffect(Schema.toType(AdmissionRequest))(
        request,
      ).pipe(Effect.mapError(internalFailure(operation)));
      const inputJson = yield* encodePersistedJsonText(validated.inputPayload).pipe(
        Effect.mapError(internalFailure(operation)),
      );
      const agentDigestsJson = yield* encodeDefinitionDigestsText(validated.agentDigests).pipe(
        Effect.mapError(internalFailure(operation)),
      );
      const mintedSubmissionId = yield* mintIdentifier("submission", operation);
      const mintedReceiptId = yield* mintIdentifier("receipt", operation);
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
            if (existing[0].input_digest !== validated.inputDigest) {
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
              created_at
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
              ${now.iso}
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
    "SqliteSubmissionLedger.markReady",
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

  const lookup: SubmissionLedger["Service"]["lookup"] = Effect.fn("SqliteSubmissionLedger.lookup")(
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

  const claim: SubmissionLedger["Service"]["claim"] = Effect.fn("SqliteSubmissionLedger.claim")(
    function* (request: ClaimRequest) {
      const operation = "ledger claim";
      const validated = yield* Schema.decodeUnknownEffect(Schema.toType(ClaimRequest))(
        request,
      ).pipe(Effect.mapError(internalFailure(operation)));
      const attemptId = yield* mintIdentifier("attempt", operation);
      const ownershipToken = yield* mintIdentifier("owner", operation);
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

          const now = yield* currentInstant;
          const ownership = yield* readOwnership(operation, head.submission_id);
          if (Option.isSome(ownership)) {
            const expiresAt = yield* timestampMillis(
              operation,
              head.submission_id,
            )(ownership.value.lease_expires_at);
            // A live lease blocks every new claim; expiry alone only revokes the liveness
            // assumption — correctness stays with producer-epoch fencing (D5).
            if (expiresAt > now.millis) return Option.none<Claim>();
          }

          // Bump the Conversation's producer epoch atomically with the claim so every stale
          // Attempt is fenced out of canonical appends (DUR-006). A Conversation that was
          // never materialized (crash between admission and materialization) is created here
          // so recovery can claim first and re-materialize idempotently at this epoch.
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
    "SqliteSubmissionLedger.renewOwnership",
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
    "SqliteSubmissionLedger.releaseOwnership",
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
    "SqliteSubmissionLedger.markInputApplied",
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
    "SqliteSubmissionLedger.reserveSettlement",
  )(function* (request: SettlementReservation) {
    const operation = "ledger reserve settlement";
    const validated = yield* Schema.decodeUnknownEffect(Schema.toType(SettlementReservation))(
      request,
    ).pipe(Effect.mapError(internalFailure(operation)));
    const recordJson = yield* encodeRecordEnvelopeText(validated.record).pipe(
      Effect.mapError(internalFailure(operation)),
    );
    yield* hitFailpoint("ledger:reserve-settlement:before", operation);
    const reserved = yield* inWriteTransaction(
      operation,
      Effect.gen(function* () {
        const existing = yield* readReservation(operation, validated.submissionId);
        if (Option.isSome(existing)) {
          const identical =
            existing.value.settlement_id === validated.settlementId &&
            existing.value.outcome === validated.outcome &&
            existing.value.record_digest === validated.recordDigest &&
            existing.value.record_json === recordJson;
          if (!identical) {
            return yield* SettlementConflict.make({
              submissionId: validated.submissionId,
              existingOutcome: existing.value.outcome,
            });
          }
          const record = yield* decodeRecordEnvelopeText(existing.value.record_json).pipe(
            Effect.mapError((error) =>
              corruptionFailure(
                operation,
                "effect_agent_settlement_reservations",
                validated.submissionId,
                error.message,
              ),
            ),
          );
          return ReservedSettlement.make({
            submissionId: validated.submissionId,
            settlementId: validated.settlementId,
            outcome: validated.outcome,
            record,
            recordDigest: validated.recordDigest,
            replayed: true,
          });
        }

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
        yield* requireOwnership(operation, submission, validated.ownershipToken);
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
            ${validated.submissionId},
            ${validated.settlementId},
            ${validated.outcome},
            ${validated.record.recordId},
            ${recordJson},
            ${validated.recordDigest},
            ${now.iso}
          )
        `.pipe(Effect.mapError(sqlFailure(operation)));
        yield* sql`
          UPDATE effect_agent_submissions
          SET state = 'terminalizing'
          WHERE submission_id = ${validated.submissionId}
        `.pipe(Effect.mapError(sqlFailure(operation)));
        return ReservedSettlement.make({
          submissionId: validated.submissionId,
          settlementId: validated.settlementId,
          outcome: validated.outcome,
          record: validated.record,
          recordDigest: validated.recordDigest,
          replayed: false,
        });
      }),
    );
    yield* hitFailpoint("ledger:reserve-settlement:after", operation);
    return reserved;
  });

  const finalizeSettlement: SubmissionLedger["Service"]["finalizeSettlement"] = Effect.fn(
    "SqliteSubmissionLedger.finalizeSettlement",
  )(function* (request: SettlementFinalization) {
    const operation = "ledger finalize settlement";
    const validated = yield* Schema.decodeUnknownEffect(Schema.toType(SettlementFinalization))(
      request,
    ).pipe(Effect.mapError(internalFailure(operation)));
    yield* hitFailpoint("ledger:finalize-settlement:before", operation);
    const settlement = yield* inWriteTransaction(
      operation,
      Effect.gen(function* () {
        const reservation = yield* readReservation(operation, validated.submissionId);
        if (Option.isNone(reservation)) {
          return yield* LedgerError.make({
            operation,
            message: `No settlement reservation exists for submission ${validated.submissionId}.`,
          });
        }
        if (reservation.value.settlement_id !== validated.settlementId) {
          return yield* SettlementConflict.make({
            submissionId: validated.submissionId,
            existingOutcome: reservation.value.outcome,
          });
        }
        const submission = yield* requireSubmission(operation, validated.submissionId);
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
            outcome: reservation.value.outcome,
            settledAt: reservation.value.finalized_at,
          }).pipe(Effect.mapError(internalFailure(operation)));
        }
        const now = yield* currentInstant;
        yield* sql`
          UPDATE effect_agent_submissions
          SET state = 'settled', settled_outcome = ${reservation.value.outcome}
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
          outcome: reservation.value.outcome,
          settledAt: now.iso,
        }).pipe(Effect.mapError(internalFailure(operation)));
      }),
    );
    yield* hitFailpoint("ledger:finalize-settlement:after", operation);
    return settlement;
  });

  const requestAbort: SubmissionLedger["Service"]["requestAbort"] = Effect.fn(
    "SqliteSubmissionLedger.requestAbort",
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

  interface ScanCursor {
    readonly conversationId: string;
    readonly queueSequence: number;
  }

  const scanPage = Effect.fn("SqliteSubmissionLedger.scanPage")(function* (
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

  const loadRecoverySnapshot: SubmissionLedger["Service"]["loadRecoverySnapshot"] = Effect.fn(
    "SqliteSubmissionLedger.loadRecoverySnapshot",
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

          let reservation: SettlementReservationSnapshot | undefined;
          const reservationRow = yield* readReservation(operation, validated.submissionId);
          if (Option.isSome(reservationRow)) {
            const record = yield* decodeRecordEnvelopeText(reservationRow.value.record_json).pipe(
              Effect.mapError((error) =>
                corruptionFailure(
                  operation,
                  "effect_agent_settlement_reservations",
                  validated.submissionId,
                  error.message,
                ),
              ),
            );
            const settlementId = yield* Schema.decodeUnknownEffect(
              SettlementReservationSnapshot.fields.settlementId,
            )(reservationRow.value.settlement_id).pipe(Effect.mapError(internalFailure(operation)));
            reservation = SettlementReservationSnapshot.make({
              settlementId,
              outcome: reservationRow.value.outcome,
              record,
              recordDigest: reservationRow.value.record_digest,
              finalized: reservationRow.value.finalized_at !== null,
            });
          }

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

          return RecoverySnapshot.make({
            submission,
            ...(ownership === undefined ? {} : { ownership }),
            ...(inputApplied === undefined ? {} : { inputApplied }),
            ...(reservation === undefined ? {} : { reservation }),
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
      claim,
      renewOwnership,
      releaseOwnership,
      markInputApplied,
      reserveSettlement,
      finalizeSettlement,
      requestAbort,
      scanNonterminal,
      loadRecoverySnapshot,
    }),
  );
});

/**
 * SQLite SubmissionLedger implementation sharing the journal's database file, write
 * transaction discipline, and producer-epoch fencing substrate. Configuration, failpoint,
 * SQL, and Crypto authority stay visible in the input channel.
 */
export const submissionLedgerLayer: Layer.Layer<
  SubmissionLedger,
  SqliteStorageInitializationError,
  SqliteStorageConfig | SqliteStorageFailpoint | SqlClientService.SqlClient | Crypto.Crypto
> = Layer.effectContext(makeServices());

/**
 * A composition-root convenience Layer for the durable Submission Ledger. Point it at the
 * same database file as the ConversationStore so claims fence the same producer epochs.
 */
export const ledgerLayer = (
  options: SqliteStorageOptions,
): Layer.Layer<SubmissionLedger, SqliteStorageInitializationError> =>
  submissionLedgerLayer.pipe(
    Layer.provide(
      Layer.mergeAll(
        storageConfigLayer(options),
        storageFailpointLayer(options),
        SqliteClient.layer({ filename: options.filename }),
        NodeCrypto.layer,
      ),
    ),
  );

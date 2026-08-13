import {
  AttemptId,
  ReceiptId,
  SubmissionId,
  ToolCallId,
  type AgentId,
  type ConversationId,
  type SettlementId,
} from "@effect-agent/core";
import { Clock, DateTime, Duration, Effect, Layer, Option, Ref, Schema, Stream } from "effect";

import {
  AbortCommand,
  AdmissionConflict,
  AdmissionRequest,
  AdmissionResult,
  ApprovalConflict,
  ApprovalDecisionCommand,
  ApprovalDecisionIntent,
  Claim,
  ClaimJoiningRequest,
  ClaimRequest,
  DEFAULT_OWNERSHIP_LEASE_DURATION,
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
  OwnershipToken,
  ProducerEpoch,
  QueueSequence,
  RecoverySnapshot,
  RecoverySnapshotRequest,
  ReleaseOwnershipRequest,
  RenewOwnershipRequest,
  ReservedSettlement,
  RevertJoiningRequest,
  Settlement,
  SettlementConflict,
  SettlementFinalization,
  SettlementReservation,
  SettlementReservationSnapshot,
  AbortIntent,
  SubmissionLedger,
  SubmissionLookup,
  SubmissionSnapshot,
  SuspendRequest,
  SuspensionSnapshot,
  UnknownResolution,
  UnknownResolutionCommand,
  UnknownResolutionConflict,
  UnknownResolutionIntent,
  type DefinitionDigests,
  type DeploymentId,
  type Digest,
  type IdempotencyKey,
  type PersistedJson,
  type Principal,
  type ProducerId,
  type RecordEnvelope,
  type SettlementOutcome,
  type SubmissionState,
  type SuspensionOutcome,
  type SuspensionReason,
} from "@effect-agent/session";

const MAX_SUBMISSIONS = 65_536;

/**
 * Lifecycle ordering used to advance-but-never-regress the operational state marker: a reclaimed
 * Attempt must not erase progress markers (input-applied, terminalizing) that an earlier Attempt
 * already committed.
 */
const STATE_RANK: Record<SubmissionState, number> = {
  admitted: 0,
  ready: 1,
  joining: 2,
  joined: 3,
  running: 4,
  "input-applied": 5,
  suspended: 6,
  unknown: 7,
  terminalizing: 8,
  settled: 9,
};

interface SubmissionRow {
  readonly submissionId: SubmissionId;
  readonly conversationId: ConversationId;
  readonly queueSequence: QueueSequence;
  readonly principal: Principal;
  readonly idempotencyKey: IdempotencyKey;
  readonly agentId: AgentId;
  readonly agentDigests: DefinitionDigests;
  readonly deploymentId: DeploymentId;
  readonly inputPayload: PersistedJson;
  readonly inputDigest: Digest;
  readonly receiptId: ReceiptId;
  readonly state: SubmissionState;
  readonly settledOutcome: SettlementOutcome | undefined;
  readonly createdAtMillis: number;
  readonly readyAtMillis: number | undefined;
}

interface StoredOwnership {
  readonly attemptId: AttemptId;
  readonly ownershipToken: OwnershipToken;
  readonly producerEpoch: ProducerEpoch;
  readonly ownerProducerId: ProducerId;
  readonly leaseExpiresAtMillis: number;
}

interface StoredReservation {
  readonly settlementId: SettlementId;
  readonly outcome: SettlementOutcome;
  readonly record: RecordEnvelope;
  readonly recordDigest: Digest;
  readonly finalizedAtMillis: number | undefined;
}

interface StoredSuspension {
  readonly reason: SuspensionReason;
  readonly suspendedAtMillis: number;
}

interface StoredUnknownMark {
  readonly reason: MarkUnknownRequest["reason"];
  readonly toolCallIds: ReadonlyArray<ToolCallId>;
}

interface StoredUnknownResolution {
  readonly intent: UnknownResolutionIntent;
  /** Canonical JSON of the Schema-encoded resolution, for divergent re-resolution detection. */
  readonly resolutionJson: string;
}

interface StoredSubmission {
  readonly row: SubmissionRow;
  readonly ownership: StoredOwnership | undefined;
  readonly inputApplied: InputAppliedMarker | undefined;
  readonly reservation: StoredReservation | undefined;
  readonly abortIntent: AbortIntent | undefined;
  /** Host linkage recorded at `claimJoining` time; cleared by `revertJoining` (DUR-016). */
  readonly joinedHostSubmissionId: SubmissionId | undefined;
  readonly suspension: StoredSuspension | undefined;
  readonly unknownMark: StoredUnknownMark | undefined;
  readonly approvalDecisions: ReadonlyMap<ToolCallId, ApprovalDecisionIntent>;
  readonly unknownResolutions: ReadonlyMap<ToolCallId, StoredUnknownResolution>;
}

/**
 * States in which `claim` never grants the head: the lane is host-owned (`joining`/`joined`)
 * or durably blocked (`suspended`/`unknown`) rather than worker-claimable.
 */
const BLOCKED_HEAD_STATES: ReadonlySet<SubmissionState> = new Set([
  "joining",
  "joined",
  "suspended",
  "unknown",
]);

interface LaneState {
  readonly nextQueueSequence: number;
  readonly producerEpoch: number;
}

interface LedgerState {
  readonly submissions: ReadonlyMap<SubmissionId, StoredSubmission>;
  readonly admissionIndex: ReadonlyMap<string, SubmissionId>;
  readonly lanes: ReadonlyMap<ConversationId, LaneState>;
  readonly mintCounter: number;
}

type Decision<A, E> =
  | { readonly _tag: "failure"; readonly error: E }
  | { readonly _tag: "success"; readonly value: A };

const failure = <E>(error: E): Decision<never, E> => ({ _tag: "failure", error });
const success = <A>(value: A): Decision<A, never> => ({ _tag: "success", value });

const ledgerError = (operation: string, message: string, cause?: unknown): LedgerError =>
  cause === undefined
    ? LedgerError.make({ operation, message })
    : LedgerError.make({ operation, message, cause });

const validate = Effect.fn("MemorySubmissionLedger.validate")(
  <A, I>(
    schema: Schema.Codec<A, I>,
    operation: string,
    value: unknown,
  ): Effect.Effect<A, LedgerError> =>
    Schema.encodeUnknownEffect(schema)(value).pipe(
      Effect.flatMap(Schema.decodeUnknownEffect(schema)),
      Effect.mapError((error) => ledgerError(operation, `Invalid ${operation} request`, error)),
    ),
);

const decodeSubmissionId = Schema.decodeSync(SubmissionId);
const decodeReceiptId = Schema.decodeSync(ReceiptId);
const decodeAttemptId = Schema.decodeSync(AttemptId);
const decodeOwnershipToken = Schema.decodeSync(OwnershipToken);
const decodeQueueSequence = Schema.decodeSync(QueueSequence);
const decodeProducerEpoch = Schema.decodeSync(ProducerEpoch);

const utc = (millis: number): DateTime.Utc => DateTime.toUtc(DateTime.makeUnsafe(millis));

const admissionKey = (
  conversationId: ConversationId,
  principal: Principal,
  idempotencyKey: IdempotencyKey,
): string => `${conversationId}\u001f${principal}\u001f${idempotencyKey}`;

const toSnapshot = (row: SubmissionRow): SubmissionSnapshot =>
  SubmissionSnapshot.make({
    submissionId: row.submissionId,
    conversationId: row.conversationId,
    queueSequence: row.queueSequence,
    principal: row.principal,
    idempotencyKey: row.idempotencyKey,
    agentId: row.agentId,
    agentDigests: row.agentDigests,
    deploymentId: row.deploymentId,
    inputPayload: row.inputPayload,
    inputDigest: row.inputDigest,
    receiptId: row.receiptId,
    state: row.state,
    createdAt: utc(row.createdAtMillis),
    ...(row.settledOutcome === undefined ? {} : { settledOutcome: row.settledOutcome }),
    ...(row.readyAtMillis === undefined ? {} : { readyAt: utc(row.readyAtMillis) }),
  });

const laneEpoch = (state: LedgerState, conversationId: ConversationId): number =>
  state.lanes.get(conversationId)?.producerEpoch ?? 0;

const ownershipLost = (state: LedgerState, stored: StoredSubmission): OwnershipLost =>
  OwnershipLost.make({
    submissionId: stored.row.submissionId,
    actualEpoch: decodeProducerEpoch(laneEpoch(state, stored.row.conversationId)),
  });

/** The presented token owns the lane only while it matches the live ownership record. */
const ownsLane = (stored: StoredSubmission, ownershipToken: OwnershipToken): boolean =>
  stored.ownership !== undefined && stored.ownership.ownershipToken === ownershipToken;

const withSubmission = (state: LedgerState, stored: StoredSubmission): LedgerState => ({
  ...state,
  submissions: new Map(state.submissions).set(stored.row.submissionId, stored),
});

const findHead = (
  state: LedgerState,
  conversationId: ConversationId,
): StoredSubmission | undefined => {
  let head: StoredSubmission | undefined;
  for (const stored of state.submissions.values()) {
    if (stored.row.conversationId !== conversationId || stored.row.state === "settled") continue;
    if (head === undefined || stored.row.queueSequence < head.row.queueSequence) head = stored;
  }
  return head;
};

/**
 * Reference in-memory SubmissionLedger. It implements the full port contract — atomic idempotent
 * admission, FIFO-head claims, producer-epoch fencing, Clock-driven ownership leases, idempotent
 * settlement reservation/finalization, and durable abort intent — with every transition applied
 * as one atomic `Ref.modify`, but its state does not survive the process (`non-durable`).
 *
 * Adapter-specific semantics within the port's latitude:
 *
 * - Time comes exclusively from the Effect `Clock` service, so `TestClock` drives lease expiry
 *   deterministically; no wall clock is consulted.
 * - The ownership lease is pinned to `DEFAULT_OWNERSHIP_LEASE_DURATION` (D5); durable adapters
 *   own the configuration seam.
 * - A live lease blocks claims from other producers only: the same `producerId` may reclaim its
 *   own live lease (restart recovery), which supersedes and fences the prior Attempt's token.
 * - Claiming advances `ready` to `running` and otherwise preserves the recorded state, so
 *   progress markers from an earlier Attempt survive a reclaim.
 * - `renewOwnership` keeps the token stable (the port allows rotation); a replayed admission
 *   reports the Submission's current state alongside the original identities.
 * - `claimJoining` walks the strictly-later queue: rows already `joining`/`joined` to the
 *   SAME host extend the claimed prefix and are skipped; any other non-`ready` row (an
 *   `admitted` gap, a settled row, foreign-host linkage) breaks the prefix conservatively.
 * - `markJoined` verifies the token against the HOST's live ownership (the lane is
 *   host-owned), so a later host Attempt can repair a lost marker from history (DUR-016). The
 *   join marker reuses the input-applied marker: the joined input IS `input:{sid}`.
 * - `suspend` and `markUnknown` refuse when an exact settlement is already reserved
 *   (`SettlementConflict` with the reserved outcome) — DUR-011's reservation wins.
 */
const makeSubmissionLedger = Effect.gen(function* () {
  const state = yield* Ref.make<LedgerState>({
    submissions: new Map(),
    admissionIndex: new Map(),
    lanes: new Map(),
    mintCounter: 0,
  });
  const leaseMillis = Duration.toMillis(DEFAULT_OWNERSHIP_LEASE_DURATION);

  const capabilities = Effect.succeed(LedgerCapabilities.make({ durability: "non-durable" }));

  const admit: SubmissionLedger["Service"]["admit"] = Effect.fn("MemorySubmissionLedger.admit")(
    (unvalidated) =>
      Effect.gen(function* () {
        const request = yield* validate(AdmissionRequest, "admit", unvalidated);
        const nowMillis = yield* Clock.currentTimeMillis;
        const decision = yield* Ref.modify(
          state,
          (
            current,
          ): readonly [Decision<AdmissionResult, AdmissionConflict | LedgerError>, LedgerState] => {
            const key = admissionKey(
              request.conversationId,
              request.principal,
              request.idempotencyKey,
            );
            const existingId = current.admissionIndex.get(key);
            if (existingId !== undefined) {
              const existing = current.submissions.get(existingId);
              if (existing === undefined) {
                return [
                  failure(ledgerError("admit", "Admission index references a missing Submission")),
                  current,
                ];
              }
              if (existing.row.inputDigest !== request.inputDigest) {
                return [
                  failure(
                    AdmissionConflict.make({
                      conversationId: request.conversationId,
                      principal: request.principal,
                      idempotencyKey: request.idempotencyKey,
                      existingInputDigest: existing.row.inputDigest,
                      attemptedInputDigest: request.inputDigest,
                    }),
                  ),
                  current,
                ];
              }
              return [
                success(
                  AdmissionResult.make({
                    submissionId: existing.row.submissionId,
                    receiptId: existing.row.receiptId,
                    queueSequence: existing.row.queueSequence,
                    state: existing.row.state,
                    replayed: true,
                  }),
                ),
                current,
              ];
            }
            if (current.submissions.size >= MAX_SUBMISSIONS) {
              return [
                failure(
                  ledgerError("admit", `In-memory submission limit ${MAX_SUBMISSIONS} exceeded`),
                ),
                current,
              ];
            }
            const lane = current.lanes.get(request.conversationId) ?? {
              nextQueueSequence: 1,
              producerEpoch: 0,
            };
            const mintCounter = current.mintCounter + 1;
            const row: SubmissionRow = {
              submissionId: decodeSubmissionId(`submission-memory-${mintCounter}`),
              conversationId: request.conversationId,
              queueSequence: decodeQueueSequence(lane.nextQueueSequence),
              principal: request.principal,
              idempotencyKey: request.idempotencyKey,
              agentId: request.agentId,
              agentDigests: request.agentDigests,
              deploymentId: request.deploymentId,
              inputPayload: request.inputPayload,
              inputDigest: request.inputDigest,
              receiptId: decodeReceiptId(`receipt-memory-${mintCounter}`),
              state: "admitted",
              settledOutcome: undefined,
              createdAtMillis: nowMillis,
              readyAtMillis: undefined,
            };
            const submissions = new Map(current.submissions).set(row.submissionId, {
              row,
              ownership: undefined,
              inputApplied: undefined,
              reservation: undefined,
              abortIntent: undefined,
              joinedHostSubmissionId: undefined,
              suspension: undefined,
              unknownMark: undefined,
              approvalDecisions: new Map<ToolCallId, ApprovalDecisionIntent>(),
              unknownResolutions: new Map<ToolCallId, StoredUnknownResolution>(),
            });
            const admissionIndex = new Map(current.admissionIndex).set(key, row.submissionId);
            const lanes = new Map(current.lanes).set(request.conversationId, {
              nextQueueSequence: lane.nextQueueSequence + 1,
              producerEpoch: lane.producerEpoch,
            });
            return [
              success(
                AdmissionResult.make({
                  submissionId: row.submissionId,
                  receiptId: row.receiptId,
                  queueSequence: row.queueSequence,
                  state: row.state,
                  replayed: false,
                }),
              ),
              { submissions, admissionIndex, lanes, mintCounter },
            ];
          },
        );
        if (decision._tag === "failure") return yield* decision.error;
        return decision.value;
      }),
  );

  const markReady: SubmissionLedger["Service"]["markReady"] = Effect.fn(
    "MemorySubmissionLedger.markReady",
  )((unvalidated) =>
    Effect.gen(function* () {
      const request = yield* validate(MarkReadyRequest, "markReady", unvalidated);
      const nowMillis = yield* Clock.currentTimeMillis;
      const decision = yield* Ref.modify(
        state,
        (current): readonly [Decision<void, LedgerError>, LedgerState] => {
          const stored = current.submissions.get(request.submissionId);
          if (stored === undefined) {
            return [
              failure(ledgerError("markReady", `Unknown Submission ${request.submissionId}`)),
              current,
            ];
          }
          if (stored.row.state !== "admitted") return [success(undefined), current];
          return [
            success(undefined),
            withSubmission(current, {
              ...stored,
              row: { ...stored.row, state: "ready", readyAtMillis: nowMillis },
            }),
          ];
        },
      );
      if (decision._tag === "failure") return yield* decision.error;
    }),
  );

  const lookup: SubmissionLedger["Service"]["lookup"] = Effect.fn("MemorySubmissionLedger.lookup")(
    (unvalidated) =>
      Effect.gen(function* () {
        const request = yield* validate(SubmissionLookup, "lookup", unvalidated);
        const current = yield* Ref.get(state);
        const submissionId =
          request._tag === "SubmissionLookupById"
            ? request.submissionId
            : current.admissionIndex.get(
                admissionKey(request.conversationId, request.principal, request.idempotencyKey),
              );
        const stored =
          submissionId === undefined ? undefined : current.submissions.get(submissionId);
        return stored === undefined ? Option.none() : Option.some(toSnapshot(stored.row));
      }),
  );

  const claim: SubmissionLedger["Service"]["claim"] = Effect.fn("MemorySubmissionLedger.claim")(
    (unvalidated) =>
      Effect.gen(function* () {
        const request = yield* validate(ClaimRequest, "claim", unvalidated);
        const nowMillis = yield* Clock.currentTimeMillis;
        const decision = yield* Ref.modify(
          state,
          (current): readonly [Decision<Option.Option<Claim>, LedgerError>, LedgerState] => {
            const head = findHead(current, request.conversationId);
            if (head === undefined) return [success(Option.none()), current];
            // A joining/joined head is host-owned and a suspended/unknown head is durably
            // blocked; the lane produces no claim and later ready work is never skipped.
            if (BLOCKED_HEAD_STATES.has(head.row.state)) return [success(Option.none()), current];
            if (
              head.ownership !== undefined &&
              head.ownership.leaseExpiresAtMillis > nowMillis &&
              head.ownership.ownerProducerId !== request.producerId
            ) {
              return [success(Option.none()), current];
            }
            const lane = current.lanes.get(request.conversationId);
            if (lane === undefined) {
              return [
                failure(ledgerError("claim", "Claimable head without a Conversation lane")),
                current,
              ];
            }
            const producerEpoch = decodeProducerEpoch(lane.producerEpoch + 1);
            const mintCounter = current.mintCounter + 1;
            const ownership: StoredOwnership = {
              attemptId: decodeAttemptId(`attempt-memory-${mintCounter}`),
              ownershipToken: decodeOwnershipToken(`ownership-memory-${mintCounter}`),
              producerEpoch,
              ownerProducerId: request.producerId,
              leaseExpiresAtMillis: nowMillis + leaseMillis,
            };
            const row: SubmissionRow =
              head.row.state === "ready" ? { ...head.row, state: "running" } : head.row;
            const next = withSubmission(current, { ...head, row, ownership });
            const lanes = new Map(next.lanes).set(request.conversationId, {
              nextQueueSequence: lane.nextQueueSequence,
              producerEpoch: lane.producerEpoch + 1,
            });
            return [
              success(
                Option.some(
                  Claim.make({
                    submissionId: row.submissionId,
                    attemptId: ownership.attemptId,
                    ownershipToken: ownership.ownershipToken,
                    producerEpoch,
                    leaseExpiresAt: utc(ownership.leaseExpiresAtMillis),
                    inputPayload: row.inputPayload,
                  }),
                ),
              ),
              { ...next, lanes, mintCounter },
            ];
          },
        );
        if (decision._tag === "failure") return yield* decision.error;
        return decision.value;
      }),
  );

  const renewOwnership: SubmissionLedger["Service"]["renewOwnership"] = Effect.fn(
    "MemorySubmissionLedger.renewOwnership",
  )((unvalidated) =>
    Effect.gen(function* () {
      const request = yield* validate(RenewOwnershipRequest, "renewOwnership", unvalidated);
      const nowMillis = yield* Clock.currentTimeMillis;
      const decision = yield* Ref.modify(
        state,
        (
          current,
        ): readonly [Decision<OwnershipRenewal, OwnershipLost | LedgerError>, LedgerState] => {
          const stored = current.submissions.get(request.submissionId);
          if (stored === undefined) {
            return [
              failure(ledgerError("renewOwnership", `Unknown Submission ${request.submissionId}`)),
              current,
            ];
          }
          if (stored.ownership === undefined || !ownsLane(stored, request.ownershipToken)) {
            return [failure(ownershipLost(current, stored)), current];
          }
          const ownership: StoredOwnership = {
            ...stored.ownership,
            leaseExpiresAtMillis: nowMillis + leaseMillis,
          };
          return [
            success(
              OwnershipRenewal.make({
                ownershipToken: ownership.ownershipToken,
                leaseExpiresAt: utc(ownership.leaseExpiresAtMillis),
              }),
            ),
            withSubmission(current, { ...stored, ownership }),
          ];
        },
      );
      if (decision._tag === "failure") return yield* decision.error;
      return decision.value;
    }),
  );

  const releaseOwnership: SubmissionLedger["Service"]["releaseOwnership"] = Effect.fn(
    "MemorySubmissionLedger.releaseOwnership",
  )((unvalidated) =>
    Effect.gen(function* () {
      const request = yield* validate(ReleaseOwnershipRequest, "releaseOwnership", unvalidated);
      const decision = yield* Ref.modify(
        state,
        (current): readonly [Decision<void, OwnershipLost | LedgerError>, LedgerState] => {
          const stored = current.submissions.get(request.submissionId);
          if (stored === undefined) {
            return [
              failure(
                ledgerError("releaseOwnership", `Unknown Submission ${request.submissionId}`),
              ),
              current,
            ];
          }
          if (!ownsLane(stored, request.ownershipToken)) {
            return [failure(ownershipLost(current, stored)), current];
          }
          return [success(undefined), withSubmission(current, { ...stored, ownership: undefined })];
        },
      );
      if (decision._tag === "failure") return yield* decision.error;
    }),
  );

  const markInputApplied: SubmissionLedger["Service"]["markInputApplied"] = Effect.fn(
    "MemorySubmissionLedger.markInputApplied",
  )((unvalidated) =>
    Effect.gen(function* () {
      const request = yield* validate(MarkInputAppliedRequest, "markInputApplied", unvalidated);
      const decision = yield* Ref.modify(
        state,
        (current): readonly [Decision<void, OwnershipLost | LedgerError>, LedgerState] => {
          const stored = current.submissions.get(request.submissionId);
          if (stored === undefined) {
            return [
              failure(
                ledgerError("markInputApplied", `Unknown Submission ${request.submissionId}`),
              ),
              current,
            ];
          }
          if (!ownsLane(stored, request.ownershipToken)) {
            return [failure(ownershipLost(current, stored)), current];
          }
          const marker = InputAppliedMarker.make({
            recordId: request.recordId,
            sequence: request.sequence,
          });
          const row: SubmissionRow =
            STATE_RANK[stored.row.state] < STATE_RANK["input-applied"]
              ? { ...stored.row, state: "input-applied" }
              : stored.row;
          return [
            success(undefined),
            withSubmission(current, { ...stored, row, inputApplied: marker }),
          ];
        },
      );
      if (decision._tag === "failure") return yield* decision.error;
    }),
  );

  const reserveSettlement: SubmissionLedger["Service"]["reserveSettlement"] = Effect.fn(
    "MemorySubmissionLedger.reserveSettlement",
  )((unvalidated) =>
    Effect.gen(function* () {
      const request = yield* validate(SettlementReservation, "reserveSettlement", unvalidated);
      const decision = yield* Ref.modify(
        state,
        (
          current,
        ): readonly [
          Decision<ReservedSettlement, SettlementConflict | OwnershipLost | LedgerError>,
          LedgerState,
        ] => {
          const stored = current.submissions.get(request.submissionId);
          if (stored === undefined) {
            return [
              failure(
                ledgerError("reserveSettlement", `Unknown Submission ${request.submissionId}`),
              ),
              current,
            ];
          }
          // A `joined` Submission settles WITH its host (plan §2.5) and its lane is never
          // worker-claimable, so no ownership token can exist for it: the recorded host
          // linkage authorizes the reservation and the presented token is not consulted.
          // Every other reservation stays fenced by the target lane's live ownership.
          const joinedSettlement =
            stored.row.state === "joined" && stored.joinedHostSubmissionId !== undefined;
          if (!joinedSettlement && !ownsLane(stored, request.ownershipToken)) {
            return [failure(ownershipLost(current, stored)), current];
          }
          const existing = stored.reservation;
          if (existing !== undefined) {
            if (
              existing.settlementId !== request.settlementId ||
              existing.outcome !== request.outcome ||
              existing.recordDigest !== request.recordDigest
            ) {
              return [
                failure(
                  SettlementConflict.make({
                    submissionId: request.submissionId,
                    existingOutcome: existing.outcome,
                  }),
                ),
                current,
              ];
            }
            return [
              success(
                ReservedSettlement.make({
                  submissionId: request.submissionId,
                  settlementId: existing.settlementId,
                  outcome: existing.outcome,
                  record: existing.record,
                  recordDigest: existing.recordDigest,
                  replayed: true,
                }),
              ),
              current,
            ];
          }
          const reservation: StoredReservation = {
            settlementId: request.settlementId,
            outcome: request.outcome,
            record: request.record,
            recordDigest: request.recordDigest,
            finalizedAtMillis: undefined,
          };
          const row: SubmissionRow =
            STATE_RANK[stored.row.state] < STATE_RANK.terminalizing
              ? { ...stored.row, state: "terminalizing" }
              : stored.row;
          return [
            success(
              ReservedSettlement.make({
                submissionId: request.submissionId,
                settlementId: reservation.settlementId,
                outcome: reservation.outcome,
                record: reservation.record,
                recordDigest: reservation.recordDigest,
                replayed: false,
              }),
            ),
            withSubmission(current, { ...stored, row, reservation }),
          ];
        },
      );
      if (decision._tag === "failure") return yield* decision.error;
      return decision.value;
    }),
  );

  const finalizeSettlement: SubmissionLedger["Service"]["finalizeSettlement"] = Effect.fn(
    "MemorySubmissionLedger.finalizeSettlement",
  )((unvalidated) =>
    Effect.gen(function* () {
      const request = yield* validate(SettlementFinalization, "finalizeSettlement", unvalidated);
      const nowMillis = yield* Clock.currentTimeMillis;
      const decision = yield* Ref.modify(
        state,
        (
          current,
        ): readonly [Decision<Settlement, SettlementConflict | LedgerError>, LedgerState] => {
          const stored = current.submissions.get(request.submissionId);
          if (stored === undefined) {
            return [
              failure(
                ledgerError("finalizeSettlement", `Unknown Submission ${request.submissionId}`),
              ),
              current,
            ];
          }
          const reservation = stored.reservation;
          if (reservation === undefined) {
            return [
              failure(
                ledgerError(
                  "finalizeSettlement",
                  `No settlement reservation for Submission ${request.submissionId}`,
                ),
              ),
              current,
            ];
          }
          if (reservation.settlementId !== request.settlementId) {
            return [
              failure(
                SettlementConflict.make({
                  submissionId: request.submissionId,
                  existingOutcome: reservation.outcome,
                }),
              ),
              current,
            ];
          }
          if (reservation.finalizedAtMillis !== undefined) {
            return [
              success(
                Settlement.make({
                  submissionId: stored.row.submissionId,
                  settlementId: reservation.settlementId,
                  receiptId: stored.row.receiptId,
                  outcome: reservation.outcome,
                  settledAt: utc(reservation.finalizedAtMillis),
                }),
              ),
              current,
            ];
          }
          const next = withSubmission(current, {
            ...stored,
            row: { ...stored.row, state: "settled", settledOutcome: reservation.outcome },
            ownership: undefined,
            reservation: { ...reservation, finalizedAtMillis: nowMillis },
          });
          return [
            success(
              Settlement.make({
                submissionId: stored.row.submissionId,
                settlementId: reservation.settlementId,
                receiptId: stored.row.receiptId,
                outcome: reservation.outcome,
                settledAt: utc(nowMillis),
              }),
            ),
            next,
          ];
        },
      );
      if (decision._tag === "failure") return yield* decision.error;
      return decision.value;
    }),
  );

  const requestAbort: SubmissionLedger["Service"]["requestAbort"] = Effect.fn(
    "MemorySubmissionLedger.requestAbort",
  )((unvalidated) =>
    Effect.gen(function* () {
      const request = yield* validate(AbortCommand, "requestAbort", unvalidated);
      const nowMillis = yield* Clock.currentTimeMillis;
      const decision = yield* Ref.modify(
        state,
        (
          current,
        ): readonly [
          Decision<AbortIntent, SettlementConflict | JoinedToHost | LedgerError>,
          LedgerState,
        ] => {
          const stored = current.submissions.get(request.submissionId);
          if (stored === undefined) {
            return [
              failure(ledgerError("requestAbort", `Unknown Submission ${request.submissionId}`)),
              current,
            ];
          }
          // A joined Submission settles WITH its host; the abort target is the host (plan
          // §2.5). A joining Submission still records the intent: it is honored only if the
          // host has not consumed the input (revert-then-abort).
          if (stored.row.state === "joined") {
            if (stored.joinedHostSubmissionId === undefined) {
              return [
                failure(
                  ledgerError(
                    "requestAbort",
                    `Joined Submission ${request.submissionId} is missing its host linkage`,
                  ),
                ),
                current,
              ];
            }
            return [
              failure(
                JoinedToHost.make({
                  submissionId: request.submissionId,
                  hostSubmissionId: stored.joinedHostSubmissionId,
                }),
              ),
              current,
            ];
          }
          if (stored.row.state === "settled") {
            if (stored.row.settledOutcome === undefined) {
              return [
                failure(
                  ledgerError(
                    "requestAbort",
                    `Settled Submission ${request.submissionId} is missing its outcome`,
                  ),
                ),
                current,
              ];
            }
            return [
              failure(
                SettlementConflict.make({
                  submissionId: request.submissionId,
                  existingOutcome: stored.row.settledOutcome,
                }),
              ),
              current,
            ];
          }
          if (stored.abortIntent !== undefined) return [success(stored.abortIntent), current];
          const intent = AbortIntent.make({
            submissionId: request.submissionId,
            author: request.author,
            reason: request.reason,
            requestedAt: utc(nowMillis),
          });
          return [success(intent), withSubmission(current, { ...stored, abortIntent: intent })];
        },
      );
      if (decision._tag === "failure") return yield* decision.error;
      return decision.value;
    }),
  );

  const claimJoining: SubmissionLedger["Service"]["claimJoining"] = Effect.fn(
    "MemorySubmissionLedger.claimJoining",
  )((unvalidated) =>
    Effect.gen(function* () {
      const request = yield* validate(ClaimJoiningRequest, "claimJoining", unvalidated);
      const decision = yield* Ref.modify(
        state,
        (
          current,
        ): readonly [
          Decision<ReadonlyArray<JoiningClaim>, OwnershipLost | LedgerError>,
          LedgerState,
        ] => {
          const host = current.submissions.get(request.hostSubmissionId);
          if (host === undefined) {
            return [
              failure(
                ledgerError("claimJoining", `Unknown Submission ${request.hostSubmissionId}`),
              ),
              current,
            ];
          }
          if (host.row.conversationId !== request.conversationId) {
            return [
              failure(
                ledgerError(
                  "claimJoining",
                  `Host Submission ${request.hostSubmissionId} does not belong to Conversation ${request.conversationId}`,
                ),
              ),
              current,
            ];
          }
          if (!ownsLane(host, request.ownershipToken)) {
            return [failure(ownershipLost(current, host)), current];
          }
          const later = [...current.submissions.values()]
            .filter(
              (stored) =>
                stored.row.conversationId === request.conversationId &&
                stored.row.queueSequence > host.row.queueSequence,
            )
            .sort((left, right) => left.row.queueSequence - right.row.queueSequence);
          const claims: Array<JoiningClaim> = [];
          const submissions = new Map(current.submissions);
          for (const stored of later) {
            if (claims.length >= request.maxCount) break;
            // Rows already claimed by THIS host extend its contiguous prefix and are skipped;
            // the coordinator re-delivers already-joined input through the coverage rule.
            if (
              (stored.row.state === "joining" || stored.row.state === "joined") &&
              stored.joinedHostSubmissionId === request.hostSubmissionId
            ) {
              continue;
            }
            // Any other non-ready row — an admitted-not-ready gap in particular — breaks the
            // contiguous ready prefix (plan §2.5); later ready work stays queued (DUR-004).
            if (stored.row.state !== "ready") break;
            submissions.set(stored.row.submissionId, {
              ...stored,
              row: { ...stored.row, state: "joining" },
              joinedHostSubmissionId: request.hostSubmissionId,
            });
            claims.push(
              JoiningClaim.make({
                submissionId: stored.row.submissionId,
                queueSequence: stored.row.queueSequence,
                inputPayload: stored.row.inputPayload,
              }),
            );
          }
          return [success(claims), { ...current, submissions }];
        },
      );
      if (decision._tag === "failure") return yield* decision.error;
      return decision.value;
    }),
  );

  const markJoined: SubmissionLedger["Service"]["markJoined"] = Effect.fn(
    "MemorySubmissionLedger.markJoined",
  )((unvalidated) =>
    Effect.gen(function* () {
      const request = yield* validate(MarkJoinedRequest, "markJoined", unvalidated);
      const decision = yield* Ref.modify(
        state,
        (current): readonly [Decision<void, OwnershipLost | LedgerError>, LedgerState] => {
          const stored = current.submissions.get(request.submissionId);
          if (stored === undefined) {
            return [
              failure(ledgerError("markJoined", `Unknown Submission ${request.submissionId}`)),
              current,
            ];
          }
          if (stored.joinedHostSubmissionId === undefined) {
            return [
              failure(
                ledgerError(
                  "markJoined",
                  `Submission ${request.submissionId} was never claimed for joining`,
                ),
              ),
              current,
            ];
          }
          const host = current.submissions.get(stored.joinedHostSubmissionId);
          if (host === undefined) {
            return [
              failure(
                ledgerError(
                  "markJoined",
                  `Host Submission ${stored.joinedHostSubmissionId} is missing`,
                ),
              ),
              current,
            ];
          }
          // The lane is host-owned: the presented token must own the HOST's ownership period,
          // which also lets a later host Attempt repair a lost marker from history (DUR-016).
          if (!ownsLane(host, request.ownershipToken)) {
            return [failure(ownershipLost(current, host)), current];
          }
          if (stored.inputApplied !== undefined) {
            if (
              stored.inputApplied.recordId === request.recordId &&
              stored.inputApplied.sequence === request.sequence
            ) {
              return [success(undefined), current];
            }
            return [
              failure(
                ledgerError(
                  "markJoined",
                  `A different join marker is already recorded for Submission ${request.submissionId}`,
                ),
              ),
              current,
            ];
          }
          if (stored.row.state !== "joining" && stored.row.state !== "joined") {
            return [
              failure(
                ledgerError(
                  "markJoined",
                  `Cannot mark Submission ${request.submissionId} joined from state ${stored.row.state}`,
                ),
              ),
              current,
            ];
          }
          const marker = InputAppliedMarker.make({
            recordId: request.recordId,
            sequence: request.sequence,
          });
          return [
            success(undefined),
            withSubmission(current, {
              ...stored,
              row: { ...stored.row, state: "joined" },
              inputApplied: marker,
            }),
          ];
        },
      );
      if (decision._tag === "failure") return yield* decision.error;
    }),
  );

  const revertJoining: SubmissionLedger["Service"]["revertJoining"] = Effect.fn(
    "MemorySubmissionLedger.revertJoining",
  )((unvalidated) =>
    Effect.gen(function* () {
      const request = yield* validate(RevertJoiningRequest, "revertJoining", unvalidated);
      const decision = yield* Ref.modify(
        state,
        (current): readonly [Decision<void, LedgerError>, LedgerState] => {
          const stored = current.submissions.get(request.submissionId);
          if (stored === undefined) {
            return [
              failure(ledgerError("revertJoining", `Unknown Submission ${request.submissionId}`)),
              current,
            ];
          }
          // Idempotent and recovery-only: only a still-`joining` Submission reverts; an
          // already-joined (or already-reverted) Submission is a no-op (DUR-016).
          if (stored.row.state !== "joining") return [success(undefined), current];
          return [
            success(undefined),
            withSubmission(current, {
              ...stored,
              row: { ...stored.row, state: "ready" },
              joinedHostSubmissionId: undefined,
            }),
          ];
        },
      );
      if (decision._tag === "failure") return yield* decision.error;
    }),
  );

  const suspend: SubmissionLedger["Service"]["suspend"] = Effect.fn(
    "MemorySubmissionLedger.suspend",
  )((unvalidated) =>
    Effect.gen(function* () {
      const request = yield* validate(SuspendRequest, "suspend", unvalidated);
      const nowMillis = yield* Clock.currentTimeMillis;
      const decision = yield* Ref.modify(
        state,
        (
          current,
        ): readonly [
          Decision<SuspensionOutcome, OwnershipLost | SettlementConflict | LedgerError>,
          LedgerState,
        ] => {
          const stored = current.submissions.get(request.submissionId);
          if (stored === undefined) {
            return [
              failure(ledgerError("suspend", `Unknown Submission ${request.submissionId}`)),
              current,
            ];
          }
          if (stored.row.state === "settled") {
            if (stored.row.settledOutcome === undefined) {
              return [
                failure(
                  ledgerError(
                    "suspend",
                    `Settled Submission ${request.submissionId} is missing its outcome`,
                  ),
                ),
                current,
              ];
            }
            return [
              failure(
                SettlementConflict.make({
                  submissionId: request.submissionId,
                  existingOutcome: stored.row.settledOutcome,
                }),
              ),
              current,
            ];
          }
          // An exact terminal outcome is already reserved (DUR-011); suspension would
          // contradict it, so the reservation wins.
          if (stored.reservation !== undefined) {
            return [
              failure(
                SettlementConflict.make({
                  submissionId: request.submissionId,
                  existingOutcome: stored.reservation.outcome,
                }),
              ),
              current,
            ];
          }
          if (!ownsLane(stored, request.ownershipToken)) {
            return [failure(ownershipLost(current, stored)), current];
          }
          // A decision that raced ahead of the suspend transaction already covers the reason:
          // the caller resumes immediately WITHOUT releasing the lane (plan §2.6).
          if (
            request.reason.toolCallIds.every((toolCallId) =>
              stored.approvalDecisions.has(toolCallId),
            )
          ) {
            return [success("resume-immediately" as const), current];
          }
          return [
            success("suspended" as const),
            withSubmission(current, {
              ...stored,
              row: { ...stored.row, state: "suspended" },
              ownership: undefined,
              suspension: { reason: request.reason, suspendedAtMillis: nowMillis },
            }),
          ];
        },
      );
      if (decision._tag === "failure") return yield* decision.error;
      return decision.value;
    }),
  );

  const recordApprovalDecision: SubmissionLedger["Service"]["recordApprovalDecision"] = Effect.fn(
    "MemorySubmissionLedger.recordApprovalDecision",
  )((unvalidated) =>
    Effect.gen(function* () {
      const command = yield* validate(
        ApprovalDecisionCommand,
        "recordApprovalDecision",
        unvalidated,
      );
      const nowMillis = yield* Clock.currentTimeMillis;
      const decision = yield* Ref.modify(
        state,
        (
          current,
        ): readonly [
          Decision<ApprovalDecisionIntent, ApprovalConflict | SettlementConflict | LedgerError>,
          LedgerState,
        ] => {
          const stored = current.submissions.get(command.submissionId);
          if (stored === undefined) {
            return [
              failure(
                ledgerError("recordApprovalDecision", `Unknown Submission ${command.submissionId}`),
              ),
              current,
            ];
          }
          if (stored.row.state === "settled") {
            if (stored.row.settledOutcome === undefined) {
              return [
                failure(
                  ledgerError(
                    "recordApprovalDecision",
                    `Settled Submission ${command.submissionId} is missing its outcome`,
                  ),
                ),
                current,
              ];
            }
            return [
              failure(
                SettlementConflict.make({
                  submissionId: command.submissionId,
                  existingOutcome: stored.row.settledOutcome,
                }),
              ),
              current,
            ];
          }
          const existing = stored.approvalDecisions.get(command.toolCallId);
          if (existing !== undefined) {
            if (existing.decision !== command.decision) {
              return [
                failure(
                  ApprovalConflict.make({
                    submissionId: command.submissionId,
                    toolCallId: command.toolCallId,
                    existingDecision: existing.decision,
                  }),
                ),
                current,
              ];
            }
            return [success(existing), current];
          }
          const intent = ApprovalDecisionIntent.make({
            submissionId: command.submissionId,
            toolCallId: command.toolCallId,
            decision: command.decision,
            resolver: command.resolver,
            reason: command.reason,
            decidedAt: utc(nowMillis),
          });
          const approvalDecisions = new Map(stored.approvalDecisions).set(
            command.toolCallId,
            intent,
          );
          // Once every pending call of the suspension reason is decided, the lane wakes:
          // suspended → input-applied (plan §2.6).
          const wakes =
            stored.row.state === "suspended" &&
            stored.suspension !== undefined &&
            stored.suspension.reason.toolCallIds.every((toolCallId) =>
              approvalDecisions.has(toolCallId),
            );
          return [
            success(intent),
            withSubmission(current, {
              ...stored,
              row: wakes ? { ...stored.row, state: "input-applied" } : stored.row,
              suspension: wakes ? undefined : stored.suspension,
              approvalDecisions,
            }),
          ];
        },
      );
      if (decision._tag === "failure") return yield* decision.error;
      return decision.value;
    }),
  );

  const markUnknown: SubmissionLedger["Service"]["markUnknown"] = Effect.fn(
    "MemorySubmissionLedger.markUnknown",
  )((unvalidated) =>
    Effect.gen(function* () {
      const request = yield* validate(MarkUnknownRequest, "markUnknown", unvalidated);
      const decision = yield* Ref.modify(
        state,
        (current): readonly [Decision<void, SettlementConflict | LedgerError>, LedgerState] => {
          const stored = current.submissions.get(request.submissionId);
          if (stored === undefined) {
            return [
              failure(ledgerError("markUnknown", `Unknown Submission ${request.submissionId}`)),
              current,
            ];
          }
          if (stored.row.state === "settled") {
            if (stored.row.settledOutcome === undefined) {
              return [
                failure(
                  ledgerError(
                    "markUnknown",
                    `Settled Submission ${request.submissionId} is missing its outcome`,
                  ),
                ),
                current,
              ];
            }
            return [
              failure(
                SettlementConflict.make({
                  submissionId: request.submissionId,
                  existingOutcome: stored.row.settledOutcome,
                }),
              ),
              current,
            ];
          }
          // A reserved exact outcome wins over a late Unknown marking (DUR-011); the recovery
          // classifier orders reservation ahead of MarkUnknown for the same reason.
          if (stored.reservation !== undefined) {
            return [
              failure(
                SettlementConflict.make({
                  submissionId: request.submissionId,
                  existingOutcome: stored.reservation.outcome,
                }),
              ),
              current,
            ];
          }
          // Idempotent merge: repeating is a no-op; additional open calls extend the marked
          // set while the first recorded reason is kept.
          const existing = stored.unknownMark;
          const known = new Set(existing?.toolCallIds ?? []);
          const merged = [
            ...(existing?.toolCallIds ?? []),
            ...request.toolCallIds.filter((toolCallId) => !known.has(toolCallId)),
          ];
          return [
            success(undefined),
            withSubmission(current, {
              ...stored,
              row:
                stored.row.state === "unknown" ? stored.row : { ...stored.row, state: "unknown" },
              unknownMark: { reason: existing?.reason ?? request.reason, toolCallIds: merged },
            }),
          ];
        },
      );
      if (decision._tag === "failure") return yield* decision.error;
    }),
  );

  const recordUnknownResolution: SubmissionLedger["Service"]["recordUnknownResolution"] = Effect.fn(
    "MemorySubmissionLedger.recordUnknownResolution",
  )((unvalidated) =>
    Effect.gen(function* () {
      const command = yield* validate(
        UnknownResolutionCommand,
        "recordUnknownResolution",
        unvalidated,
      );
      const nowMillis = yield* Clock.currentTimeMillis;
      const encodedResolution = yield* Schema.encodeUnknownEffect(UnknownResolution)(
        command.resolution,
      ).pipe(
        Effect.mapError((error) =>
          ledgerError("recordUnknownResolution", "Invalid unknown-outcome resolution", error),
        ),
      );
      const resolutionJson = JSON.stringify(encodedResolution);
      const decision = yield* Ref.modify(
        state,
        (
          current,
        ): readonly [
          Decision<
            UnknownResolutionIntent,
            UnknownResolutionConflict | SettlementConflict | LedgerError
          >,
          LedgerState,
        ] => {
          const stored = current.submissions.get(command.submissionId);
          if (stored === undefined) {
            return [
              failure(
                ledgerError(
                  "recordUnknownResolution",
                  `Unknown Submission ${command.submissionId}`,
                ),
              ),
              current,
            ];
          }
          if (stored.row.state === "settled") {
            if (stored.row.settledOutcome === undefined) {
              return [
                failure(
                  ledgerError(
                    "recordUnknownResolution",
                    `Settled Submission ${command.submissionId} is missing its outcome`,
                  ),
                ),
                current,
              ];
            }
            return [
              failure(
                SettlementConflict.make({
                  submissionId: command.submissionId,
                  existingOutcome: stored.row.settledOutcome,
                }),
              ),
              current,
            ];
          }
          const existing = stored.unknownResolutions.get(command.toolCallId);
          if (existing !== undefined && existing.resolutionJson !== resolutionJson) {
            return [
              failure(
                UnknownResolutionConflict.make({
                  submissionId: command.submissionId,
                  toolCallId: command.toolCallId,
                }),
              ),
              current,
            ];
          }
          const intent =
            existing?.intent ??
            UnknownResolutionIntent.make({
              submissionId: command.submissionId,
              toolCallId: command.toolCallId,
              author: command.author,
              reason: command.reason,
              resolution: command.resolution,
              resolvedAt: utc(nowMillis),
            });
          const unknownResolutions =
            existing !== undefined
              ? stored.unknownResolutions
              : new Map(stored.unknownResolutions).set(command.toolCallId, {
                  intent,
                  resolutionJson,
                });
          // The lane reopens only when EVERY marked open call has a durable resolution
          // intent: unknown → input-applied (DUR-017). Replays re-run the coverage check so
          // a recovering caller can wake the lane idempotently.
          const wakes =
            stored.row.state === "unknown" &&
            stored.unknownMark !== undefined &&
            stored.unknownMark.toolCallIds.every((toolCallId) =>
              unknownResolutions.has(toolCallId),
            );
          return [
            success(intent),
            withSubmission(current, {
              ...stored,
              row: wakes ? { ...stored.row, state: "input-applied" } : stored.row,
              unknownMark: wakes ? undefined : stored.unknownMark,
              unknownResolutions,
            }),
          ];
        },
      );
      if (decision._tag === "failure") return yield* decision.error;
      return decision.value;
    }),
  );

  const scanNonterminal: SubmissionLedger["Service"]["scanNonterminal"] = Stream.unwrap(
    Ref.get(state).pipe(
      Effect.map((current) => {
        const snapshots = [...current.submissions.values()]
          .filter((stored) => stored.row.state !== "settled")
          .sort((left, right) =>
            left.row.conversationId < right.row.conversationId
              ? -1
              : left.row.conversationId > right.row.conversationId
                ? 1
                : left.row.queueSequence - right.row.queueSequence,
          )
          .map((stored) => toSnapshot(stored.row));
        return Stream.fromIterable(snapshots);
      }),
    ),
  );

  const loadRecoverySnapshot: SubmissionLedger["Service"]["loadRecoverySnapshot"] = Effect.fn(
    "MemorySubmissionLedger.loadRecoverySnapshot",
  )((unvalidated) =>
    Effect.gen(function* () {
      const request = yield* validate(RecoverySnapshotRequest, "loadRecoverySnapshot", unvalidated);
      const current = yield* Ref.get(state);
      const stored = current.submissions.get(request.submissionId);
      if (stored === undefined) {
        return yield* ledgerError(
          "loadRecoverySnapshot",
          `Unknown Submission ${request.submissionId}`,
        );
      }
      const joins = [...current.submissions.values()]
        .filter((candidate) => candidate.joinedHostSubmissionId === request.submissionId)
        .sort((left, right) => left.row.queueSequence - right.row.queueSequence)
        .map((candidate) =>
          JoinSnapshot.make({
            submissionId: candidate.row.submissionId,
            state: candidate.row.state,
            hostSubmissionId: request.submissionId,
          }),
        );
      const byToolCallId = <A extends { readonly toolCallId: ToolCallId }>(
        left: A,
        right: A,
      ): number =>
        left.toolCallId < right.toolCallId ? -1 : left.toolCallId > right.toolCallId ? 1 : 0;
      return RecoverySnapshot.make({
        submission: toSnapshot(stored.row),
        joins,
        approvalDecisions: [...stored.approvalDecisions.values()].sort(byToolCallId),
        unknownResolutions: [...stored.unknownResolutions.values()]
          .map((resolution) => resolution.intent)
          .sort(byToolCallId),
        ...(stored.joinedHostSubmissionId === undefined
          ? {}
          : { hostSubmissionId: stored.joinedHostSubmissionId }),
        ...(stored.suspension === undefined
          ? {}
          : {
              suspension: SuspensionSnapshot.make({
                reason: stored.suspension.reason,
                suspendedAt: utc(stored.suspension.suspendedAtMillis),
              }),
            }),
        ...(stored.ownership === undefined
          ? {}
          : {
              ownership: OwnershipSnapshot.make({
                attemptId: stored.ownership.attemptId,
                ownerProducerId: stored.ownership.ownerProducerId,
                producerEpoch: stored.ownership.producerEpoch,
                leaseExpiresAt: utc(stored.ownership.leaseExpiresAtMillis),
              }),
            }),
        ...(stored.inputApplied === undefined ? {} : { inputApplied: stored.inputApplied }),
        ...(stored.reservation === undefined
          ? {}
          : {
              reservation: SettlementReservationSnapshot.make({
                settlementId: stored.reservation.settlementId,
                outcome: stored.reservation.outcome,
                record: stored.reservation.record,
                recordDigest: stored.reservation.recordDigest,
                finalized: stored.reservation.finalizedAtMillis !== undefined,
              }),
            }),
        ...(stored.abortIntent === undefined ? {} : { abortIntent: stored.abortIntent }),
      });
    }),
  );

  return SubmissionLedger.of({
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
    claimJoining,
    markJoined,
    revertJoining,
    suspend,
    recordApprovalDecision,
    markUnknown,
    recordUnknownResolution,
    scanNonterminal,
    loadRecoverySnapshot,
  });
});

/**
 * In-memory reference SubmissionLedger Layer (durability `non-durable`). All state lives in one
 * `Ref` owned by the Layer's Scope; no daemon fibers are spawned and no wall clock is consulted.
 */
export const MemorySubmissionLedgerLive: Layer.Layer<SubmissionLedger> = Layer.effect(
  SubmissionLedger,
  makeSubmissionLedger,
);

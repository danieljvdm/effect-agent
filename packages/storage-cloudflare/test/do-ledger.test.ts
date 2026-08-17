import {
  CanonicalSettlementRepair,
  ChildSettledNotification,
  ClaimRequest,
  DigestError,
  LedgerError,
  MarkReadyRequest,
  RecoverySnapshotRequest,
  ResumeSuspensionRequest,
  SettlementFinalization,
  SubmissionLedger,
  SubmissionLookupByKey,
  SuspendRequest,
  WaitingChild,
  WaitingForChildSuspension,
  submissionLedgerConformanceCases,
  IdempotencyKey,
} from "@effect-agent/session";
import { BrowserCrypto } from "@effect/platform-browser";
import { SqliteClient } from "@effect/sql-sqlite-do";
import { runInDurableObject } from "cloudflare:test";
import { Cause, Crypto, Effect, Exit, Layer, Option, PlatformError, Stream } from "effect";
import * as SqlClientService from "effect/unstable/sql/SqlClient";
import { describe, expect, it } from "vite-plus/test";

import {
  DoStorageCorruptionError,
  DoStorageConfig,
  DoStorageFailpoint,
  DoValueBoundExceeded,
  evictionFailpointHandler,
  ledgerLayer,
  storageConfigLayer,
  submissionLedgerLayer,
  type DoStorageInitializationError,
} from "../src/index.ts";
import {
  admission,
  conversation,
  conversationStub,
  id,
  settlementReservation,
  TEST_PRINCIPAL,
  TEST_PRODUCER,
  toolCall,
  withConversationStorage,
} from "./harness.ts";

type Equal<Left, Right> =
  (<Value>() => Value extends Left ? 1 : 2) extends <Value>() => Value extends Right ? 1 : 2
    ? (<Value>() => Value extends Right ? 1 : 2) extends <Value>() => Value extends Left ? 1 : 2
      ? true
      : false
    : false;
type Assert<Value extends true> = Value;
type SubmissionLedgerLayerRequirementsProof = Assert<
  Equal<
    Layer.Services<typeof submissionLedgerLayer>,
    DoStorageConfig | DoStorageFailpoint | SqlClientService.SqlClient | Crypto.Crypto
  >
>;
type SubmissionLedgerLayerErrorProof = Assert<
  Equal<Layer.Error<typeof submissionLedgerLayer>, DoStorageInitializationError>
>;

const failingDigestCryptoLayer = Layer.succeed(Crypto.Crypto)(
  Crypto.make({
    randomBytes: (size) => new Uint8Array(size),
    digest: () =>
      Effect.fail(
        PlatformError.systemError({
          _tag: "Unknown",
          module: "DoLedgerTestCrypto",
          method: "digest",
          description: "injected operational failure",
        }),
      ),
  }),
);

describe("DoSubmissionLedger", () => {
  // The SAME adapter-neutral contract suite the Node/SQLite and in-memory adapters run —
  // all cases, including lease expiry via TestClock, producer fencing, joined input,
  // suspensions, unknown outcomes, and the S2 subagent operations — executed in-workerd
  // against a real SQLite-backed Durable Object's storage. One Durable Object per case: the
  // 0.21.x pool shares storage across tests within a run.
  describe("shared SubmissionLedger conformance", () => {
    for (const conformanceCase of submissionLedgerConformanceCases) {
      it(conformanceCase.name, () =>
        withConversationStorage(`wp1-ledger:${conformanceCase.name}`, (storage) =>
          conformanceCase.run.pipe(Effect.provide([ledgerLayer({ storage }), BrowserCrypto.layer])),
        ),
      );
    }
  });

  it("keeps configuration, failpoint, SQL, and Crypto authority in the named Layer input", () => {
    const requirementsProof: SubmissionLedgerLayerRequirementsProof = true;
    const errorProof: SubmissionLedgerLayerErrorProof = true;

    expect(requirementsProof).toBe(true);
    expect(errorProof).toBe(true);
  });

  for (const corruption of ["input-marker", "suspension"] as const) {
    it(`rejects a one-sided persisted ${corruption} pair`, () =>
      withConversationStorage(`wp1-ledger-corrupt-${corruption}`, (storage) =>
        Effect.gen(function* () {
          const ledger = yield* SubmissionLedger;
          const sql = yield* SqlClientService.SqlClient;
          const admitted = yield* ledger.admit(
            yield* admission(`conversation-corrupt-${corruption}`, `corrupt-${corruption}`, {
              corruption,
            }),
          );
          if (corruption === "input-marker") {
            yield* sql`
              UPDATE effect_agent_submissions
              SET input_applied_sequence = 1
              WHERE submission_id = ${admitted.submissionId}
            `;
          } else {
            yield* sql`
              UPDATE effect_agent_submissions
              SET suspended_at = '1970-01-01T00:00:00.000Z'
              WHERE submission_id = ${admitted.submissionId}
            `;
          }

          const loaded = yield* ledger
            .loadRecoverySnapshot(
              RecoverySnapshotRequest.make({ submissionId: admitted.submissionId }),
            )
            .pipe(Effect.exit);
          expect(Exit.isFailure(loaded)).toBe(true);
          if (Exit.isFailure(loaded)) {
            const error = Cause.squash(loaded.cause);
            expect(error).toBeInstanceOf(LedgerError);
            if (error instanceof LedgerError) {
              expect(error.cause).toBeInstanceOf(DoStorageCorruptionError);
            }
          }
        }).pipe(
          Effect.provide(
            submissionLedgerLayer.pipe(
              Layer.provideMerge(
                Layer.mergeAll(
                  storageConfigLayer({ storage }),
                  DoStorageFailpoint.layer,
                  SqliteClient.layer({ storage }),
                  BrowserCrypto.layer,
                ),
              ),
            ),
          ),
        ),
      ));
  }

  for (const corruption of [
    "settlement-id",
    "settlement-id-schema",
    "outcome",
    "record-id",
    "record-json",
    "record-digest",
    "reserved-at",
    "submission-outcome",
  ] as const) {
    it(`classifies a settlement reservation with a tampered ${corruption} for repair`, () =>
      withConversationStorage(`wp1-ledger-reservation-corrupt-${corruption}`, (storage) =>
        Effect.gen(function* () {
          const ledger = yield* SubmissionLedger;
          const sql = yield* SqlClientService.SqlClient;
          const lane = `conversation-reservation-corrupt-${corruption}`;
          const admitted = yield* ledger.admit(
            yield* admission(lane, `reservation-corrupt-${corruption}`, { corruption }),
          );
          yield* ledger.markReady(MarkReadyRequest.make({ submissionId: admitted.submissionId }));
          const claim = yield* ledger.claim(
            ClaimRequest.make({
              conversationId: conversation(lane),
              producerId: TEST_PRODUCER,
            }),
          );
          if (Option.isNone(claim)) return yield* Effect.die("missing settlement claim");
          const reservation = yield* settlementReservation(
            admitted,
            claim.value.ownershipToken,
            "completed",
          );
          yield* ledger.reserveSettlement(reservation);
          const originalSettlement = yield* ledger.finalizeSettlement(
            SettlementFinalization.make({
              submissionId: admitted.submissionId,
              settlementId: reservation.settlementId,
            }),
          );

          switch (corruption) {
            case "settlement-id":
              yield* sql`
                UPDATE effect_agent_settlement_reservations
                SET settlement_id = 'settlement-corrupt'
                WHERE submission_id = ${admitted.submissionId}
              `;
              break;
            case "settlement-id-schema":
              yield* sql`
                UPDATE effect_agent_settlement_reservations
                SET settlement_id = ''
                WHERE submission_id = ${admitted.submissionId}
              `;
              break;
            case "outcome":
              yield* sql`
                UPDATE effect_agent_settlement_reservations
                SET outcome = 'failed'
                WHERE submission_id = ${admitted.submissionId}
              `;
              break;
            case "record-id":
              yield* sql`
                UPDATE effect_agent_settlement_reservations
                SET record_id = 'record-corrupt'
                WHERE submission_id = ${admitted.submissionId}
              `;
              break;
            case "record-json":
              yield* sql`
                UPDATE effect_agent_settlement_reservations
                SET record_json = '{'
                WHERE submission_id = ${admitted.submissionId}
              `;
              break;
            case "record-digest":
              yield* sql`
                UPDATE effect_agent_settlement_reservations
                SET record_digest = ${"b".repeat(64)}
                WHERE submission_id = ${admitted.submissionId}
              `;
              break;
            case "reserved-at":
              yield* sql`
                UPDATE effect_agent_settlement_reservations
                SET reserved_at = 'not-a-timestamp'
                WHERE submission_id = ${admitted.submissionId}
              `;
              break;
            case "submission-outcome":
              yield* sql`
                UPDATE effect_agent_submissions
                SET settled_outcome = 'failed'
                WHERE submission_id = ${admitted.submissionId}
              `;
              break;
          }

          const loaded = yield* ledger.loadRecoverySnapshot(
            RecoverySnapshotRequest.make({ submissionId: admitted.submissionId }),
          );
          expect(loaded.reservationIntegrity).toBe(
            corruption === "submission-outcome" ? "verified" : "invalid",
          );
          expect(loaded.reservation === undefined).toBe(corruption !== "submission-outcome");
          const settlement = yield* ledger.repairSettlementFromCanonical(
            CanonicalSettlementRepair.make({
              submissionId: admitted.submissionId,
              record: reservation.record,
              recordDigest: reservation.recordDigest,
            }),
          );
          const repaired = yield* ledger.loadRecoverySnapshot(
            RecoverySnapshotRequest.make({ submissionId: admitted.submissionId }),
          );
          expect(settlement.outcome).toBe("completed");
          expect(settlement.settledAt).toEqual(originalSettlement.settledAt);
          expect(repaired.submission.state).toBe("settled");
          expect(repaired.reservationIntegrity).toBe("verified");
          expect(repaired.reservation?.recordDigest).toBe(reservation.recordDigest);
        }).pipe(
          Effect.provide(
            submissionLedgerLayer.pipe(
              Layer.provideMerge(
                Layer.mergeAll(
                  storageConfigLayer({ storage }),
                  DoStorageFailpoint.layer,
                  SqliteClient.layer({ storage }),
                  BrowserCrypto.layer,
                ),
              ),
            ),
          ),
        ),
      ));
  }

  it("propagates operational Crypto failure while checking recovery integrity", () =>
    withConversationStorage("wp1-ledger-recovery-crypto-failure", (storage) =>
      Effect.gen(function* () {
        const admitted = yield* Effect.gen(function* () {
          const ledger = yield* SubmissionLedger;
          const admitted = yield* ledger.admit(
            yield* admission("recovery-crypto-failure", "recovery-crypto-failure", {
              work: "digest",
            }),
          );
          yield* ledger.markReady(MarkReadyRequest.make({ submissionId: admitted.submissionId }));
          const claim = yield* ledger.claim(
            ClaimRequest.make({
              conversationId: conversation("recovery-crypto-failure"),
              producerId: TEST_PRODUCER,
            }),
          );
          if (Option.isNone(claim)) return yield* Effect.die("missing settlement claim");
          const reservation = yield* settlementReservation(
            admitted,
            claim.value.ownershipToken,
            "completed",
          );
          yield* ledger.reserveSettlement(reservation);
          return admitted;
        }).pipe(Effect.provide([ledgerLayer({ storage }), BrowserCrypto.layer]));
        if (admitted === undefined) return;

        const failingLedgerLayer = submissionLedgerLayer.pipe(
          Layer.provide(
            Layer.mergeAll(
              storageConfigLayer({ storage }),
              DoStorageFailpoint.layer,
              SqliteClient.layer({ storage }),
              failingDigestCryptoLayer,
            ),
          ),
        );
        const loaded = yield* Effect.gen(function* () {
          const ledger = yield* SubmissionLedger;
          return yield* ledger.loadRecoverySnapshot(
            RecoverySnapshotRequest.make({ submissionId: admitted.submissionId }),
          );
        }).pipe(Effect.provide(failingLedgerLayer), Effect.exit);
        expect(Exit.isFailure(loaded)).toBe(true);
        if (Exit.isFailure(loaded)) {
          const error = Cause.squash(loaded.cause);
          expect(error).toBeInstanceOf(LedgerError);
          if (error instanceof LedgerError) {
            expect(error.operation).toBe("ledger load recovery snapshot");
            expect(error.cause).toBeInstanceOf(DigestError);
          }
        }
      }),
    ));

  for (const corruption of ["missing", "malformed", "invalid-digest"] as const) {
    it(`defers a settled child notification with a ${corruption} reservation without waking`, () =>
      withConversationStorage(`wp1-child-notify-${corruption}`, (storage) =>
        Effect.gen(function* () {
          const ledger = yield* SubmissionLedger;
          const sql = yield* SqlClientService.SqlClient;
          const parentLane = `parent-child-notify-${corruption}`;
          const childLane = `child-child-notify-${corruption}`;
          const parent = yield* ledger.admit(
            yield* admission(parentLane, `parent-child-notify-${corruption}`, { parent: true }),
          );
          yield* ledger.markReady(MarkReadyRequest.make({ submissionId: parent.submissionId }));
          const parentClaim = yield* ledger.claim(
            ClaimRequest.make({
              conversationId: conversation(parentLane),
              producerId: TEST_PRODUCER,
            }),
          );
          if (Option.isNone(parentClaim)) return yield* Effect.die("missing parent claim");

          const child = yield* ledger.admit(
            yield* admission(childLane, `child-child-notify-${corruption}`, { child: true }),
          );
          yield* ledger.markReady(MarkReadyRequest.make({ submissionId: child.submissionId }));
          const reason = WaitingForChildSuspension.make({
            children: [
              WaitingChild.make({
                toolCallId: toolCall(`call-child-notify-${corruption}`),
                childSubmissionId: child.submissionId,
              }),
            ],
          });
          yield* ledger.suspend(
            SuspendRequest.make({
              submissionId: parent.submissionId,
              ownershipToken: parentClaim.value.ownershipToken,
              reason,
            }),
          );
          const childClaim = yield* ledger.claim(
            ClaimRequest.make({
              conversationId: conversation(childLane),
              producerId: TEST_PRODUCER,
            }),
          );
          if (Option.isNone(childClaim)) return yield* Effect.die("missing child claim");
          const reservation = yield* settlementReservation(
            child,
            childClaim.value.ownershipToken,
            "completed",
          );
          yield* ledger.reserveSettlement(reservation);
          yield* ledger.finalizeSettlement(
            SettlementFinalization.make({
              submissionId: child.submissionId,
              settlementId: reservation.settlementId,
            }),
          );
          if (corruption === "missing") {
            yield* sql`
              DELETE FROM effect_agent_settlement_reservations
              WHERE submission_id = ${child.submissionId}
            `;
          } else if (corruption === "malformed") {
            yield* sql`
              UPDATE effect_agent_settlement_reservations
              SET settlement_id = ''
              WHERE submission_id = ${child.submissionId}
            `;
          } else {
            yield* sql`
              UPDATE effect_agent_settlement_reservations
              SET record_digest = ${"b".repeat(64)}
              WHERE submission_id = ${child.submissionId}
            `;
          }

          expect(
            yield* ledger.recordChildSettled(
              ChildSettledNotification.make({
                parentSubmissionId: parent.submissionId,
                childSubmissionId: child.submissionId,
              }),
            ),
          ).toBe("child-not-terminal");
          const resume = yield* ledger
            .resumeSuspension(
              ResumeSuspensionRequest.make({
                submissionId: parent.submissionId,
                expectedReason: reason,
              }),
            )
            .pipe(Effect.exit);
          expect(Exit.isFailure(resume)).toBe(true);
          if (Exit.isFailure(resume)) {
            const error = Cause.squash(resume.cause);
            expect(error).toBeInstanceOf(LedgerError);
            if (error instanceof LedgerError) {
              expect(error.operation).toBe("ledger resume suspension");
              expect(error.cause).toBeInstanceOf(DoStorageCorruptionError);
            }
          }
          const parentSnapshot = yield* ledger.loadRecoverySnapshot(
            RecoverySnapshotRequest.make({ submissionId: parent.submissionId }),
          );
          expect(parentSnapshot.submission.state).toBe("suspended");
          expect(parentSnapshot.suspension?.reason).toEqual(reason);
        }).pipe(
          Effect.provide(
            submissionLedgerLayer.pipe(
              Layer.provideMerge(
                Layer.mergeAll(
                  storageConfigLayer({ storage }),
                  DoStorageFailpoint.layer,
                  SqliteClient.layer({ storage }),
                  BrowserCrypto.layer,
                ),
              ),
            ),
          ),
        ),
      ));
  }

  it("does not trust a parseable finalized timestamp on a nonterminal reservation", () =>
    withConversationStorage("wp1-ledger-nonterminal-finalized-at", (storage) =>
      Effect.gen(function* () {
        const ledger = yield* SubmissionLedger;
        const sql = yield* SqlClientService.SqlClient;
        const staleFinalizedAt = "2099-01-01T00:00:00.000Z";
        const lane = "conversation-nonterminal-finalized-at";
        const admitted = yield* ledger.admit(
          yield* admission(lane, "nonterminal-finalized-at", { work: "repair" }),
        );
        yield* ledger.markReady(MarkReadyRequest.make({ submissionId: admitted.submissionId }));
        const claim = yield* ledger.claim(
          ClaimRequest.make({
            conversationId: conversation(lane),
            producerId: TEST_PRODUCER,
          }),
        );
        if (Option.isNone(claim)) return yield* Effect.die("missing settlement claim");
        const reservation = yield* settlementReservation(
          admitted,
          claim.value.ownershipToken,
          "completed",
        );
        yield* ledger.reserveSettlement(reservation);
        yield* sql`
          UPDATE effect_agent_settlement_reservations
          SET finalized_at = ${staleFinalizedAt}
          WHERE submission_id = ${admitted.submissionId}
        `;
        yield* ledger.repairSettlementFromCanonical(
          CanonicalSettlementRepair.make({
            submissionId: admitted.submissionId,
            record: reservation.record,
            recordDigest: reservation.recordDigest,
          }),
        );
        const rows = yield* sql<{ finalized_at: string }>`
          SELECT finalized_at
          FROM effect_agent_settlement_reservations
          WHERE submission_id = ${admitted.submissionId}
        `;
        expect(rows[0]?.finalized_at).not.toBe(staleFinalizedAt);
      }).pipe(
        Effect.provide(
          submissionLedgerLayer.pipe(
            Layer.provideMerge(
              Layer.mergeAll(
                storageConfigLayer({ storage }),
                DoStorageFailpoint.layer,
                SqliteClient.layer({ storage }),
                BrowserCrypto.layer,
              ),
            ),
          ),
        ),
      ),
    ));

  // The DC realization of "persists admissions durably across process-style reopen": the
  // Durable Object is evicted mid-flight through the failpoint's `ctx.abort()` mode — the
  // platform's real failure shape — and a FRESH instance over the same storage proves the
  // committed admission is the recovery truth, with no in-memory field involved.
  it("persists admissions across Durable Object re-instantiation (ctx.abort eviction + reread)", async () => {
    const objectName = "wp1-ledger-eviction-reread";
    const lane = "conversation-eviction-reread";

    const first = conversationStub(objectName);
    const outcome = await runInDurableObject(first, (_instance, state) =>
      Effect.runPromise(
        Effect.gen(function* () {
          const ledger = yield* SubmissionLedger;
          return yield* ledger.admit(yield* admission(lane, "eviction-key", { city: "Kyoto" }));
        }).pipe(
          Effect.provide([
            ledgerLayer({
              storage: state.storage,
              failpoint: evictionFailpointHandler({
                isArmed: (location) => Effect.succeed(location === "ledger:admit:after"),
                evict: () => state.abort("wp1 injected eviction at ledger:admit:after"),
              }),
            }),
            BrowserCrypto.layer,
          ]),
        ),
      ),
    ).then(
      () => "returned" as const,
      () => "evicted" as const,
    );
    // The armed hit fired AFTER the admission transaction committed and killed the
    // incarnation before the caller could observe the result.
    expect(outcome).toBe("evicted");

    // A fresh incarnation over the SAME storage: the committed admission survives, the
    // client retry replays the identical identities (DUR-001), and the nonterminal scan —
    // recovery's admission-independent worklist — sees the accepted obligation.
    const second = conversationStub(objectName);
    const reread = await runInDurableObject(second, (_instance, state) =>
      Effect.runPromise(
        Effect.gen(function* () {
          const ledger = yield* SubmissionLedger;
          const capabilities = yield* ledger.capabilities;
          const replayed = yield* ledger.admit(
            yield* admission(lane, "eviction-key", { city: "Kyoto" }),
          );
          const byKey = yield* ledger.lookup(
            SubmissionLookupByKey.make({
              conversationId: conversation(lane),
              principal: TEST_PRINCIPAL,
              idempotencyKey: id(IdempotencyKey, "eviction-key"),
            }),
          );
          const nonterminal = yield* ledger.scanNonterminal.pipe(Stream.runCollect);
          return { capabilities, replayed, byKey, nonterminal: [...nonterminal] };
        }).pipe(Effect.provide([ledgerLayer({ storage: state.storage }), BrowserCrypto.layer])),
      ),
    );
    expect(reread.capabilities.durability).toBe("durable-cloudflare");
    expect(reread.replayed.replayed).toBe(true);
    expect(Option.isSome(reread.byKey)).toBe(true);
    if (Option.isSome(reread.byKey)) {
      expect(reread.byKey.value.submissionId).toBe(reread.replayed.submissionId);
      expect(reread.byKey.value.receiptId).toBe(reread.replayed.receiptId);
      expect(reread.byKey.value.state).toBe("admitted");
    }
    expect(reread.nonterminal.map((snapshot) => snapshot.submissionId)).toEqual([
      reread.replayed.submissionId,
    ]);
  });

  it("mints routable Submission identities that carry the Conversation identity", () =>
    withConversationStorage("wp1-ledger-routable-ids", (storage) =>
      Effect.gen(function* () {
        const ledger = yield* SubmissionLedger;
        const admitted = yield* ledger.admit(
          yield* admission("conversation-routable", "routable-key", { work: "route" }),
        );
        // D-P6-5: `{uuidv7}:{conversationId}`, split at the FIRST ":" — the tail is the
        // owning Conversation, which may itself contain colons. Opaque to every consumer;
        // parsed only by this adapter's routing layer (WP2).
        const separator = admitted.submissionId.indexOf(":");
        expect(separator).toBeGreaterThan(0);
        expect(admitted.submissionId.slice(separator + 1)).toBe("conversation-routable");
      }).pipe(Effect.provide([ledgerLayer({ storage }), BrowserCrypto.layer])),
    ));

  it("refuses an over-bound admission input payload typed before any ledger row exists", () =>
    withConversationStorage("wp1-ledger-value-bound", (storage) =>
      Effect.gen(function* () {
        const ledger = yield* SubmissionLedger;
        const sql = yield* SqlClientService.SqlClient;

        const exit = yield* ledger
          .admit(
            yield* admission("conversation-ledger-bound", "bound-key", {
              blob: "x".repeat(2_048),
            }),
          )
          .pipe(Effect.exit);
        expect(Exit.isFailure(exit)).toBe(true);
        if (Exit.isFailure(exit)) {
          const error = Cause.squash(exit.cause);
          expect(error).toBeInstanceOf(LedgerError);
          if (error instanceof LedgerError) {
            expect(error.cause).toBeInstanceOf(DoValueBoundExceeded);
            if (error.cause instanceof DoValueBoundExceeded) {
              expect(error.cause.maxBytes).toBe(1_024);
              expect(error.cause.actualBytes).toBeGreaterThan(1_024);
              expect(error.cause.message).toContain("R2");
            }
          }
        }

        // The refusal happened BEFORE any durable mutation: no admission row exists.
        const rows = yield* sql<Record<string, unknown>>`
          SELECT submission_id FROM effect_agent_submissions
        `;
        expect(rows).toEqual([]);
      }).pipe(
        Effect.provide(
          submissionLedgerLayer.pipe(
            Layer.provideMerge(
              Layer.mergeAll(
                storageConfigLayer({ storage, maxStoredValueBytes: 1_024 }),
                DoStorageFailpoint.layer,
                SqliteClient.layer({ storage }),
                BrowserCrypto.layer,
              ),
            ),
          ),
        ),
      ),
    ));
});

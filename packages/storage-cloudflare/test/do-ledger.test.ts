import { DoValueBoundExceeded } from "@effect-agent/storage-cloudflare/do-storage-error";
import { DoStorageFailpoint } from "@effect-agent/storage-cloudflare/do-storage-failpoint";
import {
  ledgerLayer,
  submissionLedgerLayer,
} from "@effect-agent/storage-cloudflare/do-submission-ledger";
import { storageConfigLayer } from "@effect-agent/storage-cloudflare/do-thread-store";
import { evictionFailpointHandler } from "@effect-agent/storage-cloudflare/testing/do-storage-failpoint-testing";
import { BrowserCrypto } from "@effect/platform-browser";
import { SqliteClient } from "@effect/sql-sqlite-do";
import { runInDurableObject } from "cloudflare:test";
import { Cause, Effect, Exit, Layer, Option, Schema, Stream } from "effect";
import {
  AdmissionRequest,
  AdmissionPolicyError,
  SubmissionAdmissionFence,
  ClaimRequest,
  LedgerError,
  MarkReadyRequest,
  MarkUnknownRequest,
  RecoverySnapshotRequest,
  ReleaseOwnershipRequest,
  RenewOwnershipRequest,
  ResolutionNeverHappened,
  SubmissionLedger,
  SubmissionLookupByKey,
  IdempotencyKey,
  UnknownResolutionCommand,
} from "effect-agent/submission-ledger";
import { submissionLedgerConformanceCases } from "effect-agent/testing/submission-ledger-conformance";
import * as SqlClientService from "effect/unstable/sql/SqlClient";
import { describe, expect, it } from "vite-plus/test";

import {
  admission,
  thread,
  threadStub,
  id,
  TEST_PRINCIPAL,
  TEST_PRODUCER,
  toolCall,
  withThreadStorage,
} from "./harness.ts";

const isLedgerError = Schema.is(LedgerError);

const isDoValueBoundExceeded = Schema.is(DoValueBoundExceeded);

describe("DoSubmissionLedger", () => {
  for (const conformanceCase of submissionLedgerConformanceCases) {
    // oxlint-disable-next-line vitest/valid-title, vitest/expect-expect -- shared contracts own names and assertions
    it(conformanceCase.name, () =>
      withThreadStorage(`ledger-conformance:${conformanceCase.name}`, (storage) =>
        conformanceCase.run.pipe(Effect.provide([ledgerLayer({ storage }), BrowserCrypto.layer])),
      ),
    );
  }

  it("fences local policy in the admission transaction and replays before mutable checks", () =>
    withThreadStorage("policy-transaction", (storage) =>
      Effect.gen(function* () {
        const sql = yield* SqlClientService.SqlClient;

        yield* sql`CREATE TABLE host_admission_policy (revision TEXT NOT NULL, observations INTEGER NOT NULL)`;
        yield* sql`INSERT INTO host_admission_policy VALUES ('1', 0)`;
        let unavailable = false;

        const fence = Layer.succeed(SubmissionAdmissionFence)({
          check: (request) =>
            Effect.gen(function* () {
              if (unavailable)
                return yield* AdmissionPolicyError.make({
                  reason: "unavailable",
                  code: "host-policy",
                });
              yield* sql`UPDATE host_admission_policy SET observations=observations+1`.pipe(
                Effect.mapError(() =>
                  AdmissionPolicyError.make({ reason: "unavailable", code: "host-policy" }),
                ),
              );

              const rows = yield* sql<{
                revision: string;
              }>`SELECT revision FROM host_admission_policy`.pipe(
                Effect.mapError(() =>
                  AdmissionPolicyError.make({ reason: "unavailable", code: "host-policy" }),
                ),
              );

              if (request.admissionFence?.revision !== rows[0]?.revision)
                return yield* AdmissionPolicyError.make({
                  reason: "refused",
                  code: "stale-revision",
                });
            }),
        });

        yield* Effect.gen(function* () {
          const ledger = yield* SubmissionLedger;

          const original = AdmissionRequest.make({
            ...(yield* admission("policy-transaction", "first", {})),
            admissionGroup: "entity",
            admissionFence: { policyId: "host", key: "entity", revision: "1" },
          });

          const first = yield* ledger.admit(original);

          yield* sql`UPDATE host_admission_policy SET revision='2'`;
          expect((yield* ledger.admit(original)).submissionId).toBe(first.submissionId);

          const fresh = AdmissionRequest.make({
            ...original,
            idempotencyKey: id(IdempotencyKey, "fresh"),
          });

          expect(yield* ledger.admit(fresh).pipe(Effect.flip)).toMatchObject({
            reason: "refused",
            code: "stale-revision",
          });
          // The policy callback's SQL and the admission have one rollback boundary.
          expect(yield* sql`SELECT observations FROM host_admission_policy`).toEqual([
            { observations: 1 },
          ]);
          expect((yield* ledger.resolveAdmission(SubmissionLookupByKey.make(fresh)))._tag).toBe(
            "NotAdmitted",
          );
          unavailable = true;
          expect(yield* ledger.admit(fresh).pipe(Effect.flip)).toMatchObject({
            reason: "unavailable",
          });
          expect((yield* ledger.admit(original)).replayed).toBe(true);
        }).pipe(
          Effect.provide(
            submissionLedgerLayer.pipe(
              Layer.provide(
                Layer.mergeAll(
                  fence,
                  Layer.succeed(SqlClientService.SqlClient)(sql),
                  storageConfigLayer({ storage }),
                  DoStorageFailpoint.layer,
                  BrowserCrypto.layer,
                ),
              ),
            ),
          ),
        );
      }).pipe(Effect.provide([SqliteClient.layer({ storage }), BrowserCrypto.layer])),
    ));

  // The DC realization of "persists admissions durably across process-style reopen": the
  // Durable Object is evicted mid-flight through the failpoint's `ctx.abort()` mode — the
  // platform's real failure shape — and a FRESH instance over the same storage proves the
  // committed admission is the recovery truth, with no in-memory field involved.
  it("persists admissions across Durable Object re-instantiation (ctx.abort eviction + reread)", async () => {
    const objectName = "wp1-ledger-eviction-reread";
    const lane = "thread-eviction-reread";

    const first = threadStub(objectName);

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
    const second = threadStub(objectName);

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
              threadId: thread(lane),
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

  it("preserves a later writer across eviction after an unknown-resolution wake", async () => {
    const objectName = "unknown-writer-resolution-eviction";
    const lane = "thread-unknown-writer-resolution";
    const request = ClaimRequest.make({ threadId: thread(lane), producerId: TEST_PRODUCER });

    const before = await withThreadStorage(objectName, (storage) =>
      Effect.gen(function* () {
        const ledger = yield* SubmissionLedger;
        const older = yield* ledger.admit(yield* admission(lane, "older", { work: "older" }));
        const later = yield* ledger.admit(yield* admission(lane, "later", { work: "later" }));

        yield* ledger.markReady(MarkReadyRequest.make({ submissionId: older.submissionId }));
        yield* ledger.markReady(MarkReadyRequest.make({ submissionId: later.submissionId }));
        const first = yield* ledger.claim(request);

        if (Option.isNone(first)) return yield* Effect.die("missing older claim");
        yield* ledger.markUnknown(
          MarkUnknownRequest.make({
            submissionId: older.submissionId,
            toolCallIds: [toolCall("unknown-eviction-call")],
            reason: "ordinary Tool outcome is uncertain",
          }),
        );
        yield* ledger.releaseOwnership(
          ReleaseOwnershipRequest.make({
            submissionId: older.submissionId,
            ownershipToken: first.value.ownershipToken,
          }),
        );
        const second = yield* ledger.claim(request);

        if (Option.isNone(second)) return yield* Effect.die("missing later claim");
        expect(second.value.submissionId).toBe(later.submissionId);

        return { older, second: second.value };
      }).pipe(Effect.provide([ledgerLayer({ storage }), BrowserCrypto.layer])),
    );

    const outcome = await withThreadStorage(objectName, (storage, state) =>
      Effect.gen(function* () {
        const ledger = yield* SubmissionLedger;

        yield* ledger.recordUnknownResolution(
          UnknownResolutionCommand.make({
            submissionId: before.older.submissionId,
            toolCallId: toolCall("unknown-eviction-call"),
            author: "operator",
            reason: "external service confirmed no effect",
            resolution: ResolutionNeverHappened.make(),
          }),
        );
      }).pipe(
        Effect.provide([
          ledgerLayer({
            storage,
            failpoint: evictionFailpointHandler({
              isArmed: (location) => Effect.succeed(location === "ledger:unknown-resolution:after"),
              evict: () => state.abort("eviction after unknown resolution committed"),
            }),
          }),
          BrowserCrypto.layer,
        ]),
      ),
    ).then(
      () => "returned" as const,
      () => "evicted" as const,
    );

    expect(outcome).toBe("evicted");

    await withThreadStorage(objectName, (storage) =>
      Effect.gen(function* () {
        const ledger = yield* SubmissionLedger;

        const older = yield* ledger.loadRecoverySnapshot(
          RecoverySnapshotRequest.make({ submissionId: before.older.submissionId }),
        );

        const later = yield* ledger.loadRecoverySnapshot(
          RecoverySnapshotRequest.make({ submissionId: before.second.submissionId }),
        );

        expect(older.submission.state).toBe("input-applied");
        expect(later.ownership?.producerEpoch).toBe(before.second.producerEpoch);
        expect(Option.isNone(yield* ledger.claim(request))).toBe(true);

        const renewal = yield* ledger.renewOwnership(
          RenewOwnershipRequest.make({
            submissionId: before.second.submissionId,
            ownershipToken: before.second.ownershipToken,
          }),
        );

        expect(renewal.ownershipToken).toBe(before.second.ownershipToken);
        yield* ledger.releaseOwnership(
          ReleaseOwnershipRequest.make({
            submissionId: before.second.submissionId,
            ownershipToken: renewal.ownershipToken,
          }),
        );
        const resumed = yield* ledger.claim(request);

        expect(Option.isSome(resumed)).toBe(true);
        if (Option.isSome(resumed)) {
          expect(resumed.value.submissionId).toBe(before.older.submissionId);
          expect(resumed.value.producerEpoch).toBeGreaterThan(before.second.producerEpoch);
        }
      }).pipe(Effect.provide([ledgerLayer({ storage }), BrowserCrypto.layer])),
    );
  });

  it("refuses an over-bound admission input payload typed before any ledger row exists", () =>
    withThreadStorage("wp1-ledger-value-bound", (storage) =>
      Effect.gen(function* () {
        const ledger = yield* SubmissionLedger;
        const sql = yield* SqlClientService.SqlClient;

        const exit = yield* ledger
          .admit(
            yield* admission("thread-ledger-bound", "bound-key", {
              blob: "x".repeat(2_048),
            }),
          )
          .pipe(Effect.exit);

        expect(Exit.isFailure(exit)).toBe(true);
        if (Exit.isFailure(exit)) {
          const error = Cause.squash(exit.cause);

          expect(error).toBeInstanceOf(LedgerError);
          if (isLedgerError(error)) {
            expect(error.cause).toBeInstanceOf(DoValueBoundExceeded);
            if (isDoValueBoundExceeded(error.cause)) {
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

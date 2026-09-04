import { type DoStorageConfig } from "@effect-agent/storage-cloudflare/DoStorageConfig";
import {
  DoStorageError,
  DoValueBoundExceeded,
} from "@effect-agent/storage-cloudflare/DoStorageError";
import { DoStorageFailpoint } from "@effect-agent/storage-cloudflare/DoStorageFailpoint";
import {
  ledgerLayer,
  submissionLedgerLayer,
} from "@effect-agent/storage-cloudflare/DoSubmissionLedger";
import {
  storageConfigLayer,
  type DoStorageInitializationError,
} from "@effect-agent/storage-cloudflare/DoThreadStore";
import { evictionFailpointHandler } from "@effect-agent/storage-cloudflare/testing/DoStorageFailpointTesting";
import { digestJson } from "@effect-agent/thread/Digest";
import {
  AbortCommand,
  AbortIntentRequest,
  BeginChildBudgetReleaseRequest,
  ChildBudgetReservationRequest,
  ChildReservationId,
  ClaimRequest,
  LedgerError,
  MarkReadyRequest,
  MarkUnknownRequest,
  ResolutionCompletedWithResult,
  SubmissionLedger,
  SubmissionLookupByKey,
  IdempotencyKey,
  UnknownResolutionCommand,
} from "@effect-agent/thread/SubmissionLedger";
import { submissionLedgerConformanceCases } from "@effect-agent/thread/testing/SubmissionLedgerConformance";
import { BrowserCrypto } from "@effect/platform-browser";
import { SqliteClient } from "@effect/sql-sqlite-do";
import { runInDurableObject } from "cloudflare:test";
import type { Crypto } from "effect";
import { Cause, Effect, Exit, Layer, Option, Ref, Schema, Stream } from "effect";
import * as SqlClientService from "effect/unstable/sql/SqlClient";
import { CurrentTransformer } from "effect/unstable/sql/Statement";
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
const isLedgerError = Schema.is(LedgerError);
const isDoStorageError = Schema.is(DoStorageError);
const isDoValueBoundExceeded = Schema.is(DoValueBoundExceeded);

describe("DoSubmissionLedger", () => {
  it("reads an abort intent with one query regardless of other admitted inputs", () =>
    withThreadStorage("wp1-ledger-abort-poll", (storage) =>
      Effect.gen(function* () {
        const ledger = yield* SubmissionLedger;
        const admitted = yield* ledger.admit(yield* admission("abort-poll", "target", {}));
        const request = AbortIntentRequest.make({ submissionId: admitted.submissionId });
        const queries = yield* Ref.make(0);

        const read = ledger
          .readAbortIntent(request)
          .pipe(
            Effect.provideService(CurrentTransformer, (statement) =>
              Ref.update(queries, (count) => count + 1).pipe(Effect.as(statement)),
            ),
          );

        expect(yield* read).toBeUndefined();
        for (let index = 0; index < 5; index++) {
          yield* ledger.admit(
            yield* admission("abort-poll", `other-${index}`, { text: "x".repeat(1024) }),
          );
        }
        expect(yield* read).toBeUndefined();
        yield* ledger.requestAbort(
          AbortCommand.make({
            submissionId: admitted.submissionId,
            author: "operator",
            reason: "stop",
          }),
        );
        expect(yield* read).toMatchObject({ reason: "stop" });
        expect(yield* Ref.get(queries)).toBe(3);
      }).pipe(Effect.provide([ledgerLayer({ storage }), BrowserCrypto.layer])),
    ));

  // The SAME adapter-neutral contract suite the Node/SQLite and in-memory adapters run —
  // all cases, including lease expiry via TestClock, producer fencing, joined input,
  // suspensions, unknown outcomes, and the S2 subagent operations — executed in-workerd
  // against a real SQLite-backed Durable Object's storage. One Durable Object per case: the
  // 0.21.x pool shares storage across tests within a run.
  describe("shared SubmissionLedger conformance", () => {
    for (const conformanceCase of submissionLedgerConformanceCases) {
      it(conformanceCase.name, () =>
        withThreadStorage(`wp1-ledger:${conformanceCase.name}`, (storage) =>
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

  it("validates convenience-layer configuration before initializing storage", () =>
    withThreadStorage("wp1-ledger-invalid-config", (storage) =>
      Effect.gen(function* () {
        const opened = yield* SubmissionLedger.pipe(
          Effect.provide(ledgerLayer({ storage, observationPollInterval: -1 })),
          Effect.exit,
        );

        expect(Exit.isFailure(opened)).toBe(true);
        if (Exit.isFailure(opened)) {
          const failure = Cause.findErrorOption(opened.cause);

          expect(Option.isSome(failure)).toBe(true);
          if (Option.isSome(failure)) {
            expect(isDoStorageError(failure.value)).toBe(true);
            if (isDoStorageError(failure.value)) {
              expect(failure.value.operation).toBe("configure Durable Object storage");
            }
          }
        }

        const tables = storage.sql
          .exec<{ name: string }>(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'effect_agent_%'",
          )
          .toArray();

        expect(tables).toEqual([]);
      }),
    ));

  it("treats reordered persisted JSON as an idempotent replay", () =>
    withThreadStorage("wp1-ledger-semantic-json", (storage) =>
      Effect.gen(function* () {
        const ledger = yield* SubmissionLedger;
        const lane = thread("thread-do-semantic-json");

        const admitted = yield* ledger.admit(
          yield* admission("thread-do-semantic-json", "semantic-json-key", {
            work: "semantic JSON",
          }),
        );

        yield* ledger.markReady(MarkReadyRequest.make({ submissionId: admitted.submissionId }));

        const claim = yield* ledger.claim(
          ClaimRequest.make({ threadId: lane, producerId: TEST_PRODUCER }),
        );

        if (Option.isNone(claim)) return yield* Effect.die("missing semantic JSON claim");

        const allocation = { turns: 4, toolCalls: 8 };
        const reorderedAllocation = { toolCalls: 8, turns: 4 };
        const allocationDigest = yield* digestJson(allocation);

        expect(yield* digestJson(reorderedAllocation)).toBe(allocationDigest);
        const reservationId = id(ChildReservationId, "child-reservation:do-semantic-json");

        const reservationFields = {
          reservationId,
          parentSubmissionId: admitted.submissionId,
          parentToolCallId: toolCall("call-do-semantic-json"),
          ownershipToken: claim.value.ownershipToken,
          allocationDigest,
        };

        yield* ledger.reserveChildBudget(
          ChildBudgetReservationRequest.make({ ...reservationFields, allocation }),
        );

        const replayed = yield* ledger.reserveChildBudget(
          ChildBudgetReservationRequest.make({
            ...reservationFields,
            allocation: reorderedAllocation,
          }),
        );

        expect(replayed.replayed).toBe(true);

        const accounting = {
          consumed: { turns: 1, toolCalls: 2 },
          released: { turns: 3, toolCalls: 6 },
        };

        yield* ledger.beginChildBudgetRelease(
          BeginChildBudgetReleaseRequest.make({ reservationId, accounting }),
        );

        const replayedFreeze = yield* ledger.beginChildBudgetRelease(
          BeginChildBudgetReleaseRequest.make({
            reservationId,
            accounting: {
              released: { toolCalls: 6, turns: 3 },
              consumed: { toolCalls: 2, turns: 1 },
            },
          }),
        );

        expect(replayedFreeze.status).toBe("releasePending");

        const resolutionCall = toolCall("call-do-semantic-resolution");

        yield* ledger.markUnknown(
          MarkUnknownRequest.make({
            submissionId: admitted.submissionId,
            toolCallIds: [resolutionCall],
            reason: "semantic JSON replay",
          }),
        );

        const firstResolution = yield* ledger.recordUnknownResolution(
          UnknownResolutionCommand.make({
            submissionId: admitted.submissionId,
            toolCallId: resolutionCall,
            author: "do-ledger-test",
            reason: "supplier answered",
            resolution: ResolutionCompletedWithResult.make({
              result: { bookingRef: "booking-1", details: { city: "Kyoto", nights: 2 } },
              isFailure: false,
            }),
          }),
        );

        const replayedResolution = yield* ledger.recordUnknownResolution(
          UnknownResolutionCommand.make({
            submissionId: admitted.submissionId,
            toolCallId: resolutionCall,
            author: "do-ledger-test-replay",
            reason: "same supplier answer",
            resolution: ResolutionCompletedWithResult.make({
              isFailure: false,
              result: { details: { nights: 2, city: "Kyoto" }, bookingRef: "booking-1" },
            }),
          }),
        );

        expect(replayedResolution.resolvedAt).toEqual(firstResolution.resolvedAt);
      }).pipe(Effect.provide([ledgerLayer({ storage }), BrowserCrypto.layer])),
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

  it("mints routable Submission identities that carry the Thread identity", () =>
    withThreadStorage("wp1-ledger-routable-ids", (storage) =>
      Effect.gen(function* () {
        const ledger = yield* SubmissionLedger;

        const admitted = yield* ledger.admit(
          yield* admission("thread-routable", "routable-key", { work: "route" }),
        );

        // D-P6-5: `{uuidv7}:{threadId}`, split at the FIRST ":" — the tail is the
        // owning Thread, which may itself contain colons. Opaque to every consumer;
        // parsed only by this adapter's routing layer (WP2).
        const separator = admitted.submissionId.indexOf(":");

        expect(separator).toBeGreaterThan(0);
        expect(admitted.submissionId.slice(separator + 1)).toBe("thread-routable");
      }).pipe(Effect.provide([ledgerLayer({ storage }), BrowserCrypto.layer])),
    ));

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

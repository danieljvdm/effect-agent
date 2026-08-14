import {
  LedgerError,
  SubmissionLedger,
  SubmissionLookupByKey,
  submissionLedgerConformanceCases,
  IdempotencyKey,
} from "@effect-agent/session";
import { BrowserCrypto } from "@effect/platform-browser";
import { SqliteClient } from "@effect/sql-sqlite-do";
import { runInDurableObject } from "cloudflare:test";
import { Cause, Crypto, Effect, Exit, Layer, Option, Stream } from "effect";
import * as SqlClientService from "effect/unstable/sql/SqlClient";
import { describe, expect, it } from "vite-plus/test";

import {
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
  TEST_PRINCIPAL,
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

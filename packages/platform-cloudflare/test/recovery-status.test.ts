import { DoStorageFailpoint } from "@effect-agent/storage-cloudflare/do-storage-failpoint";
import { submissionLedgerLayer } from "@effect-agent/storage-cloudflare/do-submission-ledger";
import {
  storageConfigLayer,
  threadStoreLayer,
} from "@effect-agent/storage-cloudflare/do-thread-store";
import { runInDurableObject } from "cloudflare:test";
import { Cause, Clock, Effect, Exit, Layer, Option, Stream } from "effect";
import { DurableAgentRuntime } from "effect-agent/durable-agent-runtime";
import { OperationAuthorizer, OperationDenied } from "effect-agent/operation-authorizer";
import {
  RecoverySnapshotRequest,
  SubmissionLedger,
  SubmissionLookupById,
} from "effect-agent/submission-ledger";
import { ThreadStore } from "effect-agent/thread-store";
import { WakeScheduler } from "effect-agent/wake-scheduler";
import { DurableObject } from "effect-cf";
import { TestClock } from "effect/testing";
import { describe, expect, it } from "vite-plus/test";

import {
  ThreadMaintenance,
  ThreadMaintenanceFailpoint,
  type ThreadMaintenanceFailpointHandler,
  type ThreadMaintenanceFailpointLocation,
} from "../src/Alarm.ts";
import { CloudflareDurableRuntimeConfig } from "../src/CloudflareConfig.ts";
import {
  bookDefinition,
  decodeThreadId,
  lostBookReplies,
  maintenanceClocks,
  makeTestBindings,
  plannerDefinition,
  submitOptions,
  supplierCountsFor,
} from "./fixtures.ts";
import { scheduledAlarm, stubFor } from "./harness.ts";

// Reconstruct the real runtime over one physical SQLite owner, retaining its mutation gate.
// The read probe observes the public port; it never substitutes canonical data or decisions.
const localRun =
  (owner: string, reads: Array<string>, hit?: ThreadMaintenanceFailpointHandler) =>
  <A, E>(
    body: Effect.Effect<
      A,
      E,
      ThreadMaintenance | DurableAgentRuntime | SubmissionLedger | ThreadStore
    >,
  ) =>
    Effect.promise(() =>
      runInDurableObject(stubFor(owner), (instance, state) =>
        instance[DurableObject.RunSymbol](
          Effect.gen(function* () {
            const bindings = yield* makeTestBindings;
            const config = yield* CloudflareDurableRuntimeConfig;

            const observedStore = Layer.effect(
              ThreadStore,
              Effect.map(ThreadStore, (store) =>
                ThreadStore.of({
                  ...store,
                  read: (request) =>
                    Stream.suspend(() => {
                      reads.push(request.threadId);

                      return store.read(request);
                    }),
                }),
              ),
            ).pipe(Layer.provide(threadStoreLayer));

            const ports = Layer.mergeAll(observedStore, submissionLedgerLayer).pipe(
              Layer.provide(
                storageConfigLayer({
                  storage: state.storage,
                  ownershipLeaseDuration: config.ownershipLeaseDuration,
                }),
              ),
              Layer.provide(DoStorageFailpoint.layer),
            );

            const services = Layer.fresh(ThreadMaintenance.layer).pipe(
              Layer.provideMerge(DurableAgentRuntime.layerWithBindings(bindings)),
              Layer.provideMerge(ports),
              Layer.provide(WakeScheduler.layerNoop),
            );

            return yield* body.pipe(
              Effect.provide(services),
              Effect.provideService(ThreadMaintenanceFailpoint, {
                hit: hit ?? (() => Effect.void),
              }),
            );
          }),
        ),
      ),
    );

const storage = <A>(owner: string, body: (state: DurableObjectState) => A | Promise<A>) =>
  Effect.promise(() => runInDurableObject(stubFor(owner), (_instance, state) => body(state)));

const evict = (owner: string) =>
  storage(owner, (state) => state.abort("recovery status restart")).pipe(
    Effect.catchCause(() => Effect.void),
  );

const status = (thread: string) =>
  Effect.flatMap(ThreadMaintenance, (maintenance) =>
    maintenance.recoveryStatus(decodeThreadId(thread)),
  );

const pass = Effect.flatMap(ThreadMaintenance, (maintenance) => maintenance.pass);
const ensure = Effect.flatMap(ThreadMaintenance, (maintenance) => maintenance.ensureAlarm);

const submit = (thread: string, key: string) =>
  Effect.flatMap(ThreadMaintenance, (maintenance) =>
    maintenance.withMutation(
      DurableAgentRuntime.use((runtime) =>
        runtime.submitRegistered(
          { definition: plannerDefinition },
          { question: "accepted input", ref: thread },
          submitOptions(thread, key),
        ),
      ),
    ),
  );

const corruptHistory = (owner: string, thread: string, sequence = 1) =>
  storage(owner, (state) => {
    const row = state.storage.sql
      .exec<{ record_json: string }>(
        "SELECT record_json FROM effect_agent_canonical_records WHERE thread_id = ? AND sequence = ?",
        thread,
        sequence,
      )
      .one();

    state.storage.sql.exec(
      "UPDATE effect_agent_canonical_records SET record_json = ? WHERE thread_id = ? AND sequence = ?",
      '{"private_fixture_payload":"never expose this value"}',
      thread,
      sequence,
    );

    return row.record_json;
  });

const restoreHistory = (owner: string, thread: string, original: string, sequence = 1) =>
  storage(owner, (state) => {
    state.storage.sql.exec(
      "UPDATE effect_agent_canonical_records SET record_json = ? WHERE thread_id = ? AND sequence = ?",
      original,
      thread,
      sequence,
    );
  });

describe("recovery faults independent of execution history", () => {
  it("keeps accepted work visible across eviction, serves healthy lanes, and never replays an uncertain action", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const owner = `recovery-status-${crypto.randomUUID()}`;
        const poisoned = `${owner}-a`;
        const healthy = `${owner}-z`;
        const reads: Array<string> = [];
        const run = localRun(owner, reads);

        yield* TestClock.setTime(Date.now() + 86_400_000);
        maintenanceClocks.set(owner, yield* Clock.Clock);
        yield* Effect.addFinalizer(() => Effect.sync(() => maintenanceClocks.delete(owner)));

        const original = yield* run(
          ThreadMaintenance.use((maintenance) =>
            maintenance.withMutation(
              DurableAgentRuntime.use((runtime) =>
                runtime.submitRegistered(
                  { definition: bookDefinition },
                  { question: "one simulated action", ref: poisoned },
                  submitOptions(poisoned, "original"),
                ),
              ),
            ),
          ),
        );

        lostBookReplies.add(poisoned);
        yield* run(
          DurableAgentRuntime.use((runtime) =>
            runtime.processThreadHead(decodeThreadId(poisoned)),
          ).pipe(Effect.exit),
        );
        expect(supplierCountsFor(poisoned)).toEqual({ book: 1 });
        // The Thread's creation and tail remain readable. A retained interior record fails
        // only when recovery traverses it, after a new input has already been accepted.
        const bytes = yield* corruptHistory(owner, poisoned, 2);
        const pending = yield* run(submit(poisoned, "later-input"));
        const other = yield* run(submit(healthy, "other-thread"));

        const before = yield* run(
          SubmissionLedger.use((ledger) =>
            ledger.loadRecoverySnapshot(
              RecoverySnapshotRequest.make({ submissionId: original.submissionId }),
            ),
          ),
        );

        expect(before.ownership).toBeUndefined();

        const report = yield* run(pass);

        expect(report.settled).toBe(1);
        expect(report.alarm).toBe("rearmed");
        expect(
          yield* run(DurableAgentRuntime.use((runtime) => runtime.submissionStatus(other))),
        ).toMatchObject({ _tag: "settled" });
        expect(
          yield* run(DurableAgentRuntime.use((runtime) => runtime.submissionStatus(pending))),
        ).toEqual({ _tag: "pending" });
        const fault = yield* run(status(poisoned));

        expect(Option.isSome(fault)).toBe(true);
        if (Option.isNone(fault)) return;
        expect(fault.value).toMatchObject({
          threadId: poisoned,
          attempts: 1,
          failure: {
            phase: "history",
            reason: "failure",
            errorTag: "ThreadStoreError",
            operation: "decode canonical record",
            diagnostic: { causeTag: "SchemaError", decoder: "CanonicalRecord", sequence: 2 },
          },
        });
        expect(JSON.stringify(fault)).not.toContain("private_fixture_payload");
        expect(fault.value.retryAt - fault.value.lastFailedAt).toBe(5_000);

        yield* evict(owner);
        reads.length = 0;
        yield* run(ensure);
        expect(yield* run(status(poisoned))).toEqual(fault);
        expect(reads).toEqual([]);
        // An exact retry returns the original Receipt even with unreadable retained history.
        expect(yield* run(submit(poisoned, "later-input"))).toEqual(pending);
        const duringFault = yield* run(submit(poisoned, "during-fault"));

        expect(yield* run(submit(poisoned, "during-fault"))).toEqual(duringFault);
        const fresh = yield* run(submit(`${healthy}-new`, "new-admission"));

        reads.length = 0;
        expect((yield* run(pass)).settled).toBe(1);
        expect(reads).not.toContain(poisoned);
        expect(yield* run(status(poisoned))).toEqual(fault);
        expect(
          yield* run(DurableAgentRuntime.use((runtime) => runtime.submissionStatus(fresh))),
        ).toMatchObject({ _tag: "settled" });
        expect(
          yield* run(
            SubmissionLedger.use((ledger) =>
              ledger.loadRecoverySnapshot(
                RecoverySnapshotRequest.make({ submissionId: original.submissionId }),
              ),
            ),
          ),
        ).toEqual(before);
        expect(supplierCountsFor(poisoned)).toEqual({ book: 1 });

        // Authorization precedes even status decoding.
        const key = `effect-agent:thread-recovery-fault:v1:${poisoned}`;
        const saved = yield* storage(owner, (state) => state.storage.get(key));

        yield* storage(owner, (state) => state.storage.put(key, "unreadable status"));

        const denied = yield* run(
          status(poisoned).pipe(
            Effect.provideService(OperationAuthorizer, {
              authorize: (request) => OperationDenied.make({ ...request, reason: "denied" }),
            }),
            Effect.exit,
          ),
        );

        expect(Exit.isFailure(denied) ? Cause.squash(denied.cause) : undefined).toMatchObject({
          _tag: "OperationDenied",
          operation: "explain",
        });
        yield* storage(owner, (state) => state.storage.put(key, saved));

        let latest = fault.value;

        for (const delay of [10_000, 20_000, 40_000, 60_000, 60_000]) {
          yield* TestClock.adjust(latest.retryAt - (yield* Clock.currentTimeMillis));
          yield* run(pass);
          const retried = yield* run(status(poisoned));

          expect(Option.isSome(retried)).toBe(true);
          if (Option.isNone(retried)) return;
          expect(retried.value.firstFailedAt).toBe(fault.value.firstFailedAt);
          expect(retried.value.attempts).toBe(latest.attempts + 1);
          expect(retried.value.retryAt - retried.value.lastFailedAt).toBe(delay);
          latest = retried.value;
        }

        // Restore only the fixture's deliberately altered bytes. Ordinary recovery must reuse
        // the original unknown action and receipt; the later accepted input can then progress.
        yield* restoreHistory(owner, poisoned, bytes, 2);
        yield* TestClock.adjust(latest.retryAt - (yield* Clock.currentTimeMillis));
        yield* run(pass);
        expect(yield* run(status(poisoned))).toEqual(Option.none());
        expect(
          yield* run(DurableAgentRuntime.use((runtime) => runtime.submissionStatus(pending))),
        ).toMatchObject({ _tag: "settled" });
        yield* run(pass);
        expect(
          yield* run(DurableAgentRuntime.use((runtime) => runtime.submissionStatus(duringFault))),
        ).toMatchObject({ _tag: "settled" });
        expect(supplierCountsFor(poisoned)).toEqual({ book: 1 });

        const retained = yield* run(
          SubmissionLedger.use((ledger) =>
            ledger.lookup(SubmissionLookupById.make({ submissionId: original.submissionId })),
          ),
        );

        expect(Option.isSome(retained) ? retained.value.state : undefined).toBe("unknown");
        expect(Option.isSome(retained) ? retained.value.receiptId : undefined).toBe(
          original.receiptId,
        );
      }).pipe(Effect.scoped, Effect.provide(TestClock.layer())),
    ));

  for (const transition of ["record", "clear"] as const) {
    for (const location of [
      "maintenance:recovery-status:before",
      "maintenance:recovery-status:after",
    ] as const) {
      it(`retains the alarm and exact receipt when ${transition} crashes at ${location}`, () =>
        Effect.runPromise(
          Effect.gen(function* () {
            const owner = `recovery-status-crash-${crypto.randomUUID()}`;
            const reads: Array<string> = [];
            let crashAt: ThreadMaintenanceFailpointLocation | undefined;

            const run = localRun(owner, reads, (at) =>
              Effect.gen(function* () {
                if (crashAt !== at) return;
                crashAt = undefined;

                return yield* Effect.die("simulated recovery status commit crash");
              }),
            );

            yield* TestClock.setTime(Date.now() + 86_400_000);
            maintenanceClocks.set(owner, yield* Clock.Clock);
            yield* Effect.addFinalizer(() => Effect.sync(() => maintenanceClocks.delete(owner)));
            const receipt = yield* run(submit(owner, "exact-retry"));
            const bytes = yield* corruptHistory(owner, owner);

            if (transition === "clear") {
              yield* run(pass);
              yield* restoreHistory(owner, owner, bytes);
              yield* TestClock.adjust(5_000);
            }
            crashAt = location;
            expect(Exit.isFailure(yield* run(pass.pipe(Effect.exit)))).toBe(true);
            expect(crashAt).toBeUndefined();
            expect(yield* Effect.promise(() => scheduledAlarm(owner))).not.toBeNull();
            yield* evict(owner);
            yield* run(ensure);
            expect(yield* run(submit(owner, "exact-retry"))).toEqual(receipt);
            if (transition === "record") {
              yield* TestClock.adjust(60_000);
              yield* run(pass);
              expect(Option.isSome(yield* run(status(owner)))).toBe(true);
              yield* restoreHistory(owner, owner, bytes);
            }
            yield* TestClock.adjust(60_000);
            yield* run(pass);
            expect(yield* run(status(owner))).toEqual(Option.none());
            expect(
              yield* run(DurableAgentRuntime.use((runtime) => runtime.submissionStatus(receipt))),
            ).toMatchObject({ _tag: "settled" });
          }).pipe(Effect.scoped, Effect.provide(TestClock.layer())),
        ));
    }
  }
});

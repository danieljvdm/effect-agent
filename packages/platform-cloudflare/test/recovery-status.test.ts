import { DoStorageFailpoint } from "@effect-agent/storage-cloudflare/do-storage-failpoint";
import { submissionLedgerLayer } from "@effect-agent/storage-cloudflare/do-submission-ledger";
import {
  storageConfigLayer,
  threadStoreLayer,
} from "@effect-agent/storage-cloudflare/do-thread-store";
import { evictDurableObject, runDurableObjectAlarm, runInDurableObject } from "cloudflare:test";
import { Cause, Clock, Deferred, Effect, Exit, Fiber, Layer, Logger, Option, Stream } from "effect";
import { DurableAgentRuntime } from "effect-agent/durable-agent-runtime";
import {
  OperationDenied,
  type OperationAuthorizerService,
  operationAuthorizerLayer,
  possessionOperationAuthorizer,
} from "effect-agent/operation-authorizer";
import {
  AbortCommand,
  AbortIntentRequest,
  RecoverySnapshotRequest,
  SubmissionLedger,
  SubmissionLookupById,
  SubmissionLookupByKey,
} from "effect-agent/submission-ledger";
import { ThreadRead, ThreadStore } from "effect-agent/thread-store";
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
import { recoveryReadHolds, recoveryReplies } from "./recovery-fixture.ts";
import type { TestThreadObject } from "./worker.ts";

// Reconstruct the real runtime over one physical SQLite owner, retaining its mutation gate.
// The read probe observes the public port; it never substitutes canonical data or decisions.
const localRun =
  (
    owner: string,
    reads: Array<string>,
    options: {
      readonly hit?: ThreadMaintenanceFailpointHandler;
      readonly authorizer?: OperationAuthorizerService;
      readonly readFailureDefect?: Error;
    } = {},
  ) =>
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

                      return store
                        .read(request)
                        .pipe(
                          Stream.catchCause((cause) =>
                            Stream.failCause(
                              options.readFailureDefect === undefined
                                ? cause
                                : Cause.combine(cause, Cause.die(options.readFailureDefect)),
                            ),
                          ),
                        );
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
              Layer.provide(
                operationAuthorizerLayer(options.authorizer ?? possessionOperationAuthorizer),
              ),
            );

            return yield* body.pipe(
              Effect.provide(services),
              Effect.provideService(ThreadMaintenanceFailpoint, {
                hit: options.hit ?? (() => Effect.void),
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

const reconstructAlarmOwner = Effect.fnUntraced(function* (owner: string) {
  const previous = yield* Effect.promise(() =>
    (stubFor(owner) as DurableObjectStub<TestThreadObject>).progressIncarnation(),
  );

  yield* Effect.promise(() => evictDurableObject(stubFor(owner)));

  const next = yield* Effect.promise(() =>
    (stubFor(owner) as DurableObjectStub<TestThreadObject>).progressIncarnation(),
  );

  expect(next).not.toBe(previous);
});

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
        const errors: Array<{ cause: Cause.Cause<unknown>; message: unknown }> = [];

        const observedPass = pass.pipe(
          Effect.provide(
            Logger.layer([
              Logger.make(({ cause, message, logLevel }) => {
                if (logLevel === "Error") errors.push({ cause, message });
              }),
            ]),
          ),
        );

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

        const report = yield* run(observedPass);

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
        expect(errors.map(({ cause }) => Cause.findErrorOption(cause))).toEqual([
          Option.some(fault.value.failure),
        ]);
        expect(errors.flatMap(({ cause }) => Cause.prettyErrors(cause))).toHaveLength(1);
        expect(JSON.stringify(errors)).not.toContain("private_fixture_payload");
        expect(JSON.stringify(errors)).not.toContain("never expose this value");
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
        expect((yield* run(observedPass)).settled).toBe(1);
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

        // The construction-only policy must survive into later service calls, before decoding.
        const key = `effect-agent:thread-recovery-fault:v1:${poisoned}`;
        const saved = yield* storage(owner, (state) => state.storage.get(key));

        yield* storage(owner, (state) => state.storage.put(key, "unreadable status"));

        const deniedRun = localRun(owner, reads, {
          authorizer: {
            authorize: (request) => OperationDenied.make({ ...request, reason: "denied" }),
          },
        });

        const denied = yield* deniedRun(status(poisoned).pipe(Effect.exit));

        expect(Exit.isFailure(denied) ? Cause.squash(denied.cause) : undefined).toMatchObject({
          _tag: "OperationDenied",
          operation: "explain",
          threadId: poisoned,
        });
        yield* storage(owner, (state) => state.storage.put(key, saved));

        let latest = fault.value;

        for (const delay of [10_000, 20_000, 40_000, 60_000, 60_000]) {
          yield* TestClock.adjust(latest.retryAt - (yield* Clock.currentTimeMillis));
          yield* run(observedPass);
          const retried = yield* run(status(poisoned));

          expect(Option.isSome(retried)).toBe(true);
          if (Option.isNone(retried)) return;
          expect(retried.value.firstFailedAt).toBe(fault.value.firstFailedAt);
          expect(retried.value.attempts).toBe(latest.attempts + 1);
          expect(retried.value.retryAt - retried.value.lastFailedAt).toBe(delay);
          latest = retried.value;
        }
        expect(errors).toHaveLength(1);

        // Restore only the fixture's deliberately altered bytes. Ordinary recovery must reuse
        // the original unknown action and receipt; the later accepted input can then progress.
        yield* restoreHistory(owner, poisoned, bytes, 2);
        yield* TestClock.adjust(latest.retryAt - (yield* Clock.currentTimeMillis));
        yield* run(observedPass);
        expect(yield* run(status(poisoned))).toEqual(Option.none());
        expect(
          yield* run(DurableAgentRuntime.use((runtime) => runtime.submissionStatus(pending))),
        ).toMatchObject({ _tag: "settled" });
        yield* run(observedPass);
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

        // A later outage with both the storage failure and a cleanup defect stays a defect
        // at the reporter, retaining the original bounded storage provenance.
        yield* run(submit(poisoned, "mixed-cause"));
        yield* corruptHistory(owner, poisoned, 2);

        const defectiveRun = localRun(owner, reads, {
          readFailureDefect: new Error("private cleanup defect"),
        });

        yield* defectiveRun(observedPass);
        const mixed = yield* run(status(poisoned));

        expect(Option.isSome(mixed) ? mixed.value.failure : undefined).toMatchObject({
          reason: "defect",
          errorTag: "ThreadStoreError",
          causes: expect.arrayContaining([{ errorTag: "Error" }]),
          diagnostic: { causeTag: "SchemaError", decoder: "CanonicalRecord", sequence: 2 },
        });
        expect(errors.map(({ cause }) => Cause.hasDies(cause))).toEqual([false, true]);
        expect(errors.flatMap(({ cause }) => Cause.prettyErrors(cause))).toHaveLength(2);
        expect(JSON.stringify(errors)).not.toContain("private cleanup defect");
        expect(JSON.stringify(errors)).not.toContain("private_fixture_payload");
        yield* restoreHistory(owner, poisoned, bytes, 2);
        yield* TestClock.adjust(5_000);
        yield* run(observedPass);
        expect(supplierCountsFor(poisoned)).toEqual({ book: 1 });
      }).pipe(Effect.scoped, Effect.provide(TestClock.layer())),
    ));

  it("a real alarm publishes a fresh Thread reply while old recovery is stalled and cleanup survives eviction", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const owner = `recovery-alarm-${crypto.randomUUID()}`;
        const old = `${owner}-old`;
        const fresh = `${owner}-fresh`;
        const stoppedReady = `${owner}-ready`;
        const entered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const releaseReady = yield* Deferred.make<void>();
        let activeReads = 0;

        const run = <A, E>(
          body: Effect.Effect<
            A,
            E,
            ThreadMaintenance | DurableAgentRuntime | SubmissionLedger | ThreadStore
          >,
        ) =>
          Effect.promise(() =>
            runInDurableObject(stubFor(owner), (instance) =>
              instance[DurableObject.RunSymbol](body),
            ),
          );

        yield* TestClock.setTime(Date.now() + 86_400_000);
        maintenanceClocks.set(owner, yield* Clock.Clock);

        const reply = {
          lookup: SubmissionLookupByKey.make(submitOptions(fresh, "after-clear")),
          published: [],
        };

        recoveryReplies.set(owner, reply);
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            recoveryReadHolds.delete(old);
            recoveryReadHolds.delete(stoppedReady);
            recoveryReplies.delete(owner);
            maintenanceClocks.delete(owner);
          }),
        );

        const original = yield* run(
          ThreadMaintenance.use((maintenance) =>
            maintenance.withMutation(
              DurableAgentRuntime.use((runtime) =>
                runtime.submitRegistered(
                  { definition: bookDefinition },
                  { question: "one simulated action", ref: old },
                  submitOptions(old, "original"),
                ),
              ),
            ),
          ),
        );

        lostBookReplies.add(old);
        yield* run(
          DurableAgentRuntime.use((runtime) => runtime.processThreadHead(decodeThreadId(old))).pipe(
            Effect.exit,
          ),
        );
        expect(supplierCountsFor(old)).toEqual({ book: 1 });

        const command = AbortCommand.make({
          submissionId: original.submissionId,
          author: "fixture-owner",
          reason: "retire the old session",
        });

        const intent = yield* run(
          ThreadMaintenance.use((maintenance) =>
            maintenance.withMutation(DurableAgentRuntime.use((runtime) => runtime.abort(command))),
          ),
        );

        const before = yield* run(
          SubmissionLedger.use((ledger) =>
            ledger.lookup(SubmissionLookupById.make({ submissionId: original.submissionId })),
          ),
        );

        yield* run(submit(stoppedReady, "prior-context"));
        yield* run(
          DurableAgentRuntime.use((runtime) =>
            runtime.processThreadHead(decodeThreadId(stoppedReady)),
          ),
        );
        const queued = yield* run(submit(stoppedReady, "retired-before-claim"));

        const queuedIntent = yield* run(
          ThreadMaintenance.use((maintenance) =>
            maintenance.withMutation(
              DurableAgentRuntime.use((runtime) =>
                runtime.abort(
                  AbortCommand.make({
                    submissionId: queued.submissionId,
                    author: "fixture-owner",
                    reason: "retire queued input too",
                  }),
                ),
              ),
            ),
          ),
        );

        const queuedBefore = yield* run(
          SubmissionLedger.use((ledger) =>
            ledger.lookup(SubmissionLookupById.make({ submissionId: queued.submissionId })),
          ),
        );

        yield* corruptHistory(owner, stoppedReady, 2);
        recoveryReadHolds.set(stoppedReady, Deferred.await(releaseReady));
        yield* corruptHistory(owner, old, 2);
        recoveryReadHolds.set(
          old,
          Effect.acquireUseRelease(
            Effect.sync(() => {
              activeReads++;
            }).pipe(Effect.andThen(Deferred.succeed(entered, undefined))),
            () => Deferred.await(release),
            () =>
              Effect.sync(() => {
                activeReads--;
              }),
          ),
        );

        const alarm = yield* Effect.forkChild(
          Effect.promise(() => runDurableObjectAlarm(stubFor(owner))),
        );

        yield* Effect.addFinalizer(() =>
          Deferred.succeed(release, undefined).pipe(
            Effect.andThen(Deferred.succeed(releaseReady, undefined)),
          ),
        );
        yield* Deferred.await(entered);
        const freshReceipt = yield* run(submit(fresh, "after-clear"));

        // Exercise the existing 100ms scan too; promptness does not depend on a wake hint.
        for (let elapsed = 0; elapsed < 1_000; elapsed += 100) {
          yield* TestClock.adjust(100);
          if (
            (yield* run(
              DurableAgentRuntime.use((runtime) => runtime.submissionStatus(freshReceipt)),
            ))._tag === "settled" &&
            reply.published.length > 0
          )
            break;
        }
        expect(
          yield* run(DurableAgentRuntime.use((runtime) => runtime.submissionStatus(freshReceipt))),
        ).toMatchObject({
          _tag: "settled",
          settlement: { outcome: "completed" },
        });

        const records = yield* run(
          ThreadStore.use((store) =>
            Stream.runCollect(
              store.read(ThreadRead.make({ threadId: decodeThreadId(fresh), limit: 100 })),
            ),
          ),
        );

        expect(records.map((record) => record.record.payload._tag)).toContain(
          "ModelResponseRecorded",
        );
        expect(
          records.find((record) => record.record.payload._tag === "RunCompleted")?.record.payload,
        ).toMatchObject({ output: { answer: "done" } });
        expect(reply.published).toEqual([{ answer: "done" }]);
        expect(activeReads).toBe(1);
        expect(alarm.pollUnsafe()).toBeUndefined();
        expect(
          yield* run(
            SubmissionLedger.use((ledger) =>
              ledger.lookup(SubmissionLookupById.make({ submissionId: original.submissionId })),
            ),
          ),
        ).toEqual(before);
        expect(
          yield* run(
            SubmissionLedger.use((ledger) =>
              ledger.readAbortIntent(
                AbortIntentRequest.make({ submissionId: original.submissionId }),
              ),
            ),
          ),
        ).toEqual(intent);
        expect(supplierCountsFor(old)).toEqual({ book: 1 });
        expect(
          yield* run(
            SubmissionLedger.use((ledger) =>
              ledger.lookup(SubmissionLookupById.make({ submissionId: queued.submissionId })),
            ),
          ),
        ).toEqual(queuedBefore);
        // Let the second old read expose its poisoned bytes when the bounded old wave reaches it.
        yield* Deferred.succeed(releaseReady, undefined);

        // The cooperative read timeout closes the old scope and retains an inspectable fault;
        // it does not pretend cleanup settled or retry the uncertain action.
        yield* TestClock.adjust(30_000);
        expect(yield* Fiber.join(alarm)).toBe(true);
        expect(activeReads).toBe(0);
        expect(yield* run(status(old))).toMatchObject({
          _tag: "Some",
          value: { failure: { reason: "timeout" } },
        });
        recoveryReadHolds.delete(old);
        yield* reconstructAlarmOwner(owner);
        yield* run(ensure);
        expect(
          yield* run(
            SubmissionLedger.use((ledger) =>
              ledger.readAbortIntent(
                AbortIntentRequest.make({ submissionId: original.submissionId }),
              ),
            ),
          ),
        ).toEqual(intent);
        expect(
          yield* run(
            SubmissionLedger.use((ledger) =>
              ledger.readAbortIntent(
                AbortIntentRequest.make({ submissionId: queued.submissionId }),
              ),
            ),
          ),
        ).toEqual(queuedIntent);
        expect(yield* run(submit(stoppedReady, "retired-before-claim"))).toEqual(queued);
        expect(yield* run(submit(fresh, "after-clear"))).toEqual(freshReceipt);
        const afterEviction = yield* run(submit(`${fresh}-next`, "after-eviction"));

        yield* TestClock.adjust(5_000);
        expect(yield* Effect.promise(() => runDurableObjectAlarm(stubFor(owner)))).toBe(true);
        expect(yield* run(status(old))).toMatchObject({
          _tag: "Some",
          value: {
            failure: { reason: "failure", diagnostic: { decoder: "CanonicalRecord", sequence: 2 } },
          },
        });
        expect(
          yield* run(DurableAgentRuntime.use((runtime) => runtime.submissionStatus(afterEviction))),
        ).toMatchObject({ _tag: "settled", settlement: { outcome: "completed" } });
        expect(
          yield* run(
            SubmissionLedger.use((ledger) =>
              ledger.lookup(SubmissionLookupById.make({ submissionId: original.submissionId })),
            ),
          ),
        ).toEqual(before);
        expect(supplierCountsFor(old)).toEqual({ book: 1 });
        expect(yield* Effect.promise(() => scheduledAlarm(owner))).not.toBeNull();
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

            const run = localRun(owner, reads, {
              hit: (at) =>
                Effect.gen(function* () {
                  if (crashAt !== at) return;
                  crashAt = undefined;

                  return yield* Effect.die("simulated recovery status commit crash");
                }),
            });

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

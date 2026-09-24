import { DoStorageFailpoint } from "@effect-agent/storage-cloudflare/do-storage-failpoint";
import { submissionLedgerLayer } from "@effect-agent/storage-cloudflare/do-submission-ledger";
import {
  storageConfigLayer,
  threadStoreLayer,
} from "@effect-agent/storage-cloudflare/do-thread-store";
import { evictDurableObject, runDurableObjectAlarm, runInDurableObject } from "cloudflare:test";
import { Cause, Clock, Deferred, Effect, Fiber, Layer, Logger, Option, Stream } from "effect";
import { DurableAgentRuntime } from "effect-agent/durable-agent-runtime";
import {
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
} from "../src/Alarm.ts";
import { CloudflareDurableRuntimeConfig } from "../src/CloudflareConfig.ts";
import type { submit as admitToThread } from "../src/ThreadObject.ts";
import {
  bookDefinition,
  decodeThreadId,
  lostBookReplies,
  maintenanceClocks,
  makeTestBindings,
  modelRequestHolds,
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
      readonly withoutBinding?: string;
      readonly authorizer?: OperationAuthorizerService;
      readonly readFailureDefect?: Error;
      readonly readAbortIntent?: (
        read: SubmissionLedger["Service"]["readAbortIntent"],
      ) => SubmissionLedger["Service"]["readAbortIntent"];
    } = {},
  ) =>
  <A, E>(
    body: Effect.Effect<
      A,
      E,
      | ThreadMaintenance
      | DurableAgentRuntime
      | SubmissionLedger
      | ThreadStore
      | Effect.Services<ReturnType<typeof admitToThread>>
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

            const observedLedger = Layer.effect(
              SubmissionLedger,
              Effect.map(SubmissionLedger, (ledger) =>
                SubmissionLedger.of({
                  ...ledger,
                  readAbortIntent:
                    options.readAbortIntent?.(ledger.readAbortIntent) ?? ledger.readAbortIntent,
                }),
              ),
            ).pipe(Layer.provide(submissionLedgerLayer));

            const ports = Layer.mergeAll(observedStore, observedLedger).pipe(
              Layer.provide(
                storageConfigLayer({
                  storage: state.storage,
                  ownershipLeaseDuration: config.ownershipLeaseDuration,
                }),
              ),
              Layer.provide(DoStorageFailpoint.layer),
            );

            const services = Layer.fresh(ThreadMaintenance.layer).pipe(
              Layer.provideMerge(
                DurableAgentRuntime.layerWithBindings(
                  bindings.filter((binding) => binding.agentId !== options.withoutBinding),
                ),
              ),
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

// Regression: https://github.com/danieljvdm/effect-agent/commit/e6407479ae233527685928bead040dbfe5153a22
it(
  "starts a later independent Thread with two slots and retains same-Thread FIFO work",
  () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const owner = `native-concurrent-${crypto.randomUUID()}`;
        const first = `${owner}-a`;
        const second = `${owner}-b`;
        const third = `${owner}-c`;

        yield* TestClock.setTime(Date.now() + 86_400_000);
        maintenanceClocks.set(owner, yield* Clock.Clock);
        const clock = yield* TestClock.testClockWith(Effect.succeed);

        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            maintenanceClocks.delete(owner);
            for (const thread of [first, second, third]) modelRequestHolds.delete(thread);
          }),
        );
        const run = localRun(owner, []);

        yield* run(
          Effect.gen(function* () {
            const firstEntered = yield* Deferred.make<void>();
            const secondEntered = yield* Deferred.make<void>();
            const thirdEntered = yield* Deferred.make<void>();
            const releaseFirst = yield* Deferred.make<void>();
            const releaseSecond = yield* Deferred.make<void>();

            modelRequestHolds.set(
              first,
              Deferred.succeed(firstEntered, undefined).pipe(
                Effect.andThen(Deferred.await(releaseFirst)),
              ),
            );
            modelRequestHolds.set(
              second,
              Deferred.succeed(secondEntered, undefined).pipe(
                Effect.andThen(Deferred.await(releaseSecond)),
              ),
            );
            modelRequestHolds.set(
              third,
              Deferred.succeed(thirdEntered, undefined).pipe(Effect.asVoid),
            );
            const firstReceipt = yield* submit(first, "first");
            const running = yield* pass.pipe(Effect.forkChild);

            yield* Deferred.await(firstEntered);
            yield* clock.adjust(60);
            const later = yield* submit(first, "follower");
            const admissionEntered = yield* Deferred.make<void>();
            const releaseAdmission = yield* Deferred.make<void>();

            const preparing = yield* ThreadMaintenance.use((maintenance) =>
              maintenance.withMutation(
                Deferred.succeed(admissionEntered, undefined).pipe(
                  Effect.andThen(Deferred.await(releaseAdmission)),
                  Effect.andThen(
                    DurableAgentRuntime.use((runtime) =>
                      runtime.submitRegistered(
                        { definition: plannerDefinition },
                        { question: "late ready input", ref: second },
                        submitOptions(second, "later-independent"),
                      ),
                    ),
                  ),
                ),
              ),
            ).pipe(Effect.forkChild);

            yield* Deferred.await(admissionEntered);
            // The generation is already dirty while B is still absent from the ledger.
            yield* clock.adjust(1_000);
            yield* Deferred.succeed(releaseAdmission, undefined);
            const secondReceipt = yield* Fiber.join(preparing);

            yield* clock.adjust(1_000);
            const secondStarted = yield* Deferred.isDone(secondEntered);

            yield* submit(third, "third-independent");
            yield* clock.adjust(1_000);
            const thirdStartedAtCapacity = yield* Deferred.isDone(thirdEntered);

            yield* Deferred.succeed(releaseSecond, undefined);
            yield* clock.adjust(1_000);
            const thirdStartedAfterSlotRelease = yield* Deferred.isDone(thirdEntered);
            const ledger = yield* SubmissionLedger;

            const queued = yield* ledger.loadRecoverySnapshot(
              RecoverySnapshotRequest.make({ submissionId: later.submissionId }),
            );

            expect(queued.submission.state).toBe("ready");
            expect(queued.ownership).toBeUndefined();
            yield* Deferred.succeed(releaseFirst, undefined);
            yield* Fiber.join(running);
            expect(secondStarted).toBe(true);
            expect(thirdStartedAtCapacity).toBe(false);
            expect(thirdStartedAfterSlotRelease).toBe(true);

            const secondSnapshot = yield* ledger.loadRecoverySnapshot(
              RecoverySnapshotRequest.make({ submissionId: secondReceipt.submissionId }),
            );

            const followerSnapshot = yield* ledger.loadRecoverySnapshot(
              RecoverySnapshotRequest.make({ submissionId: later.submissionId }),
            );

            expect(secondSnapshot.submission.state).toBe("settled");
            expect(followerSnapshot.submission.state).toBe("settled");
            expect(followerSnapshot.ownership).toBeUndefined();

            const records = yield* ThreadStore.use((store) =>
              Stream.runCollect(
                store.read(ThreadRead.make({ threadId: decodeThreadId(first), limit: 100 })),
              ),
            );

            expect(
              records.flatMap((record) =>
                record.record.payload._tag === "UserInputRecorded"
                  ? [record.record.payload.submissionId]
                  : [],
              ),
            ).toEqual([firstReceipt.submissionId, later.submissionId]);
          }),
        );
      }).pipe(Effect.scoped, Effect.provide(TestClock.layer())),
    ),
  20_000,
);

// Regression: https://github.com/danieljvdm/effect-agent/commit/e6407479ae233527685928bead040dbfe5153a22
it(
  "releases both native claims on interruption and resumes their original receipts",
  () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const owner = `native-interrupt-${crypto.randomUUID()}`;
        const threads = [`${owner}-a`, `${owner}-b`];

        yield* TestClock.setTime(Date.now() + 86_400_000);
        maintenanceClocks.set(owner, yield* Clock.Clock);
        const clock = yield* TestClock.testClockWith(Effect.succeed);

        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            maintenanceClocks.delete(owner);
            for (const thread of threads) modelRequestHolds.delete(thread);
          }),
        );
        const run = localRun(owner, []);

        const receipts = yield* run(
          Effect.gen(function* () {
            const entered = yield* Effect.forEach(threads, () => Deferred.make<void>());
            const finalized = yield* Effect.forEach(threads, () => Deferred.make<void>());

            for (let index = 0; index < threads.length; index++) {
              modelRequestHolds.set(
                threads[index]!,
                Effect.acquireUseRelease(
                  Deferred.succeed(entered[index]!, undefined),
                  () => Effect.never,
                  () => Deferred.succeed(finalized[index]!, undefined),
                ),
              );
            }
            const accepted = yield* Effect.forEach(threads, (thread) => submit(thread, thread));
            const running = yield* pass.pipe(Effect.forkChild);

            yield* Deferred.await(entered[0]!);
            yield* clock.adjust(1_000);
            expect(yield* Deferred.isDone(entered[1]!)).toBe(true);
            yield* Fiber.interrupt(running);
            const ledger = yield* SubmissionLedger;

            for (let index = 0; index < accepted.length; index++) {
              expect(yield* Deferred.isDone(finalized[index]!)).toBe(true);

              const snapshot = yield* ledger.loadRecoverySnapshot(
                RecoverySnapshotRequest.make({ submissionId: accepted[index]!.submissionId }),
              );

              expect(snapshot.ownership).toBeUndefined();
              expect(snapshot.submission.state).not.toBe("settled");
            }

            return accepted;
          }),
        );

        expect(yield* storage(owner, (state) => state.storage.getAlarm())).not.toBeNull();
        for (const thread of threads) modelRequestHolds.delete(thread);
        yield* clock.adjust(1_000);
        for (let event = 0; event < threads.length; event++) {
          yield* clock.adjust(1_000);
          yield* run(pass);
        }
        yield* run(
          Effect.gen(function* () {
            const ledger = yield* SubmissionLedger;
            const store = yield* ThreadStore;

            for (const receipt of receipts) {
              const snapshot = yield* ledger.loadRecoverySnapshot(
                RecoverySnapshotRequest.make({ submissionId: receipt.submissionId }),
              );

              expect(snapshot.submission.state).toBe("settled");

              const records = yield* Stream.runCollect(
                store.read(
                  ThreadRead.make({
                    threadId: snapshot.submission.threadId,
                    limit: 100,
                  }),
                ),
              );

              expect(
                records.filter((record) => record.record.payload._tag === "UserInputRecorded"),
              ).toHaveLength(1);
              expect(
                records.filter((record) => record.record.payload._tag === "SubmissionSettled"),
              ).toHaveLength(1);
            }
          }),
        );
      }).pipe(Effect.scoped, Effect.provide(TestClock.layer())),
    ),
  20_000,
);

describe("recovery faults independent of execution history", () => {
  // https://reve-r6.sentry.io/issues/KOMMUNIKASIE-API-AA
  it("does not attach an old recovery deadline to an admission still becoming ready", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const owner = `recovery-admission-${crypto.randomUUID()}`;
        const old = `${owner}-old`;
        const fresh = `${owner}-fresh`;
        const reads: Array<string> = [];
        const run = localRun(owner, reads);

        yield* TestClock.setTime(Date.now() + 86_400_000);
        maintenanceClocks.set(owner, yield* Clock.Clock);
        yield* Effect.addFinalizer(() => Effect.sync(() => maintenanceClocks.delete(owner)));

        const book = DurableAgentRuntime.use((runtime) =>
          runtime.submitRegistered(
            { definition: bookDefinition },
            { question: "one simulated action", ref: old },
            submitOptions(old, "original"),
          ),
        );

        const original = yield* run(
          ThreadMaintenance.use((maintenance) => maintenance.withMutation(book)),
        );

        lostBookReplies.add(old);
        yield* run(
          DurableAgentRuntime.use((runtime) => runtime.processThreadHead(decodeThreadId(old))).pipe(
            Effect.exit,
          ),
        );
        expect(supplierCountsFor(old)).toEqual({ book: 1 });
        yield* corruptHistory(owner, old, 2);
        yield* run(pass);
        const fault = yield* run(status(old));

        expect(Option.isSome(fault)).toBe(true);
        if (Option.isNone(fault)) return;

        const originalRow = yield* run(
          SubmissionLedger.use((ledger) =>
            ledger.lookup(SubmissionLookupById.make({ submissionId: original.submissionId })),
          ),
        );

        reads.length = 0;

        const observed = yield* run(
          Effect.gen(function* () {
            const maintenance = yield* ThreadMaintenance;
            const runtime = yield* DurableAgentRuntime;
            const enrolled = yield* Deferred.make<void>();
            const release = yield* Deferred.make<void>();

            const admission = yield* Effect.forkChild(
              maintenance.withMutation(
                Deferred.succeed(enrolled, undefined).pipe(
                  Effect.andThen(Deferred.await(release)),
                  Effect.andThen(
                    runtime.submitRegistered(
                      { definition: plannerDefinition },
                      { question: "accepted input", ref: fresh },
                      submitOptions(fresh, "fresh"),
                    ),
                  ),
                ),
              ),
            );

            yield* Deferred.await(enrolled);
            // Checkpoint the retained fault after enrollment, before this admission can
            // become ready. Completing the same mutation does not enroll another generation.
            expect((yield* maintenance.pass).settled).toBe(0);
            yield* Deferred.succeed(release, undefined);
            const receipt = yield* Fiber.join(admission);
            const report = yield* maintenance.pass;
            const result = yield* runtime.submissionStatus(receipt);

            return { receipt, report, result };
          }),
        );

        expect(yield* Clock.currentTimeMillis).toBeLessThan(fault.value.retryAt);
        expect(reads).not.toContain(old);
        expect(yield* run(status(old))).toEqual(fault);
        // The deadline is a recovery retry for the old Thread, never a prerequisite for
        // the fresh one. Also observe its expiry to distinguish postponement from lost work.
        yield* TestClock.adjust(fault.value.retryAt - (yield* Clock.currentTimeMillis));
        yield* run(pass);
        expect(
          yield* run(
            DurableAgentRuntime.use((runtime) => runtime.submissionStatus(observed.receipt)),
          ),
        ).toMatchObject({ _tag: "settled" });
        expect(yield* run(submit(fresh, "fresh"))).toEqual(observed.receipt);
        expect(yield* run(book)).toEqual(original);
        expect(
          yield* run(
            SubmissionLedger.use((ledger) =>
              ledger.lookup(SubmissionLookupById.make({ submissionId: original.submissionId })),
            ),
          ),
        ).toEqual(originalRow);
        expect(supplierCountsFor(old)).toEqual({ book: 1 });
        expect(observed.report.settled).toBe(1);
        expect(observed.result).toMatchObject({ _tag: "settled" });
      }).pipe(Effect.scoped, Effect.provide(TestClock.layer())),
    ));

  it("a real alarm publishes a fresh Thread reply while old recovery is stalled and cleanup survives eviction", async ({
    signal,
  }) => {
    const owner = `recovery-alarm-${crypto.randomUUID()}`;
    const old = `${owner}-old`;
    const fresh = `${owner}-fresh`;
    const stoppedReady = `${owner}-ready`;

    // Observe fixture state from the Runner without cross-request Promise callbacks that
    // retain the Object's I/O context through the restart assertion.
    const waitFor = async (predicate: () => boolean) => {
      while (!predicate()) {
        signal.throwIfAborted();
        await new Promise<void>((resolve) => setTimeout(resolve, 10));
      }
    };

    let epochOffset = 86_400_000;
    const nowMillis = () => Date.now() + epochOffset;
    const nowNanos = () => BigInt(nowMillis()) * 1_000_000n;
    const liveClock = Effect.runSync(Clock.Clock);

    // Future epoch time suppresses automatic alarms; live sleep keeps the native fallback
    // scan and the bounded recovery timeout in the Object's own I/O context.
    const clock: Clock.Clock = {
      currentTimeMillisUnsafe: nowMillis,
      currentTimeMillis: Effect.sync(nowMillis),
      currentTimeNanosUnsafe: nowNanos,
      currentTimeNanos: Effect.sync(nowNanos),
      monotonicTimeNanosUnsafe: () => liveClock.monotonicTimeNanosUnsafe(),
      monotonicTimeNanos: liveClock.monotonicTimeNanos,
      sleep: (duration) => liveClock.sleep(duration),
    };

    let activeReads = 0;

    const run = <A, E>(
      body: Effect.Effect<
        A,
        E,
        ThreadMaintenance | DurableAgentRuntime | SubmissionLedger | ThreadStore
      >,
    ) => runInDurableObject(stubFor(owner), (instance) => instance[DurableObject.RunSymbol](body));

    try {
      maintenanceClocks.set(owner, clock);

      const reply = {
        lookup: SubmissionLookupByKey.make(submitOptions(fresh, "after-clear")),
        published: [],
      };

      recoveryReplies.set(owner, reply);

      // Keep eviction in the runner's async call chain, outside Object-resumed Effects.
      const original = await run(
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
      await run(
        DurableAgentRuntime.use((runtime) => runtime.processThreadHead(decodeThreadId(old))).pipe(
          Effect.exit,
        ),
      );
      expect(supplierCountsFor(old)).toEqual({ book: 1 });

      const intent = await run(
        ThreadMaintenance.use((maintenance) =>
          maintenance.withMutation(
            DurableAgentRuntime.use((runtime) =>
              runtime.abort(
                AbortCommand.make({
                  submissionId: original.submissionId,
                  author: "fixture-owner",
                  reason: "retire the old session",
                }),
              ),
            ),
          ),
        ),
      );

      const oldLookup = SubmissionLookupById.make({ submissionId: original.submissionId });
      const before = await run(SubmissionLedger.use((ledger) => ledger.lookup(oldLookup)));

      await run(submit(stoppedReady, "prior-context"));
      await run(
        DurableAgentRuntime.use((runtime) =>
          runtime.processThreadHead(decodeThreadId(stoppedReady)),
        ),
      );
      const queued = await run(submit(stoppedReady, "retired-before-claim"));

      const queuedIntent = await run(
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

      const queuedLookup = SubmissionLookupById.make({ submissionId: queued.submissionId });
      const queuedBefore = await run(SubmissionLedger.use((ledger) => ledger.lookup(queuedLookup)));

      await Effect.runPromise(corruptHistory(owner, stoppedReady, 2));
      await Effect.runPromise(corruptHistory(owner, old, 2));
      recoveryReadHolds.set(
        old,
        Effect.acquireUseRelease(
          Effect.sync(() => {
            activeReads++;
          }),
          () => Effect.never,
          () =>
            Effect.sync(() => {
              activeReads--;
            }),
        ),
      );
      let alarmFinished = false;

      const alarm = runDurableObjectAlarm(stubFor(owner)).finally(() => {
        alarmFinished = true;
      });

      try {
        await waitFor(() => activeReads === 1);
        const freshReceipt = await run(submit(fresh, "after-clear"));

        await waitFor(() => reply.published.length > 0 || alarmFinished);
        expect(
          await run(DurableAgentRuntime.use((runtime) => runtime.submissionStatus(freshReceipt))),
        ).toMatchObject({
          _tag: "settled",
          settlement: { outcome: "completed" },
        });

        const freshRecords = await run(
          ThreadStore.use((store) =>
            Stream.runCollect(
              store.read(ThreadRead.make({ threadId: decodeThreadId(fresh), limit: 100 })),
            ).pipe(Effect.map((records) => records.map(({ record }) => record.payload._tag))),
          ),
        );

        expect(freshRecords).toContain("ModelResponseRecorded");
        expect(reply.published).toEqual([{ answer: "done" }]);
        expect(activeReads).toBe(1);
        expect(alarmFinished).toBe(false);
        expect(await run(SubmissionLedger.use((ledger) => ledger.lookup(oldLookup)))).toEqual(
          before,
        );
        expect(await run(SubmissionLedger.use((ledger) => ledger.lookup(queuedLookup)))).toEqual(
          queuedBefore,
        );
        expect(
          await run(
            SubmissionLedger.use((ledger) =>
              ledger.readAbortIntent(
                AbortIntentRequest.make({ submissionId: original.submissionId }),
              ),
            ),
          ),
        ).toEqual(intent);
        expect(supplierCountsFor(old)).toEqual({ book: 1 });

        // Timeout closes the read resource without settling or replaying the old work.
        expect(await alarm).toBe(true);
        expect(activeReads).toBe(0);
        const retainedFault = await run(status(old));

        expect(retainedFault).toMatchObject({
          _tag: "Some",
          value: { failure: { reason: "timeout" } },
        });
        recoveryReadHolds.delete(old);
        expect(await scheduledAlarm(owner)).toBeGreaterThan(Date.now() + 80_000_000);

        const incarnation = () =>
          runInDurableObject(stubFor(owner) as DurableObjectStub<TestThreadObject>, (instance) =>
            instance.progressIncarnation(),
          );

        const previous = await incarnation();

        await evictDurableObject(stubFor(owner));
        expect(await incarnation()).not.toBe(previous);
        await run(ensure);
        expect(await run(status(old))).toEqual(retainedFault);
        expect(
          await run(
            SubmissionLedger.use((ledger) =>
              ledger.readAbortIntent(
                AbortIntentRequest.make({ submissionId: original.submissionId }),
              ),
            ),
          ),
        ).toEqual(intent);
        expect(
          await run(
            SubmissionLedger.use((ledger) =>
              ledger.readAbortIntent(
                AbortIntentRequest.make({ submissionId: queued.submissionId }),
              ),
            ),
          ),
        ).toEqual(queuedIntent);
        expect(await run(submit(stoppedReady, "retired-before-claim"))).toEqual(queued);
        expect(await run(submit(fresh, "after-clear"))).toEqual(freshReceipt);
        expect(
          await run(
            DurableAgentRuntime.use((runtime) =>
              runtime.submitRegistered(
                { definition: bookDefinition },
                { question: "one simulated action", ref: old },
                submitOptions(old, "original"),
              ),
            ),
          ),
        ).toEqual(original);
        const afterEviction = await run(submit(`${fresh}-next`, "after-eviction"));

        epochOffset += 5_000;
        expect(await runDurableObjectAlarm(stubFor(owner))).toBe(true);
        expect(await run(status(old))).toMatchObject({
          _tag: "Some",
          value: {
            failure: {
              reason: "failure",
              diagnostic: { decoder: "CanonicalRecord", sequence: 2 },
            },
          },
        });
        expect(
          await run(DurableAgentRuntime.use((runtime) => runtime.submissionStatus(afterEviction))),
        ).toMatchObject({
          _tag: "settled",
          settlement: { outcome: "completed" },
        });
        expect(await run(SubmissionLedger.use((ledger) => ledger.lookup(oldLookup)))).toEqual(
          before,
        );
        expect(await run(SubmissionLedger.use((ledger) => ledger.lookup(queuedLookup)))).toEqual(
          queuedBefore,
        );
        expect(supplierCountsFor(old)).toEqual({ book: 1 });
        expect(await scheduledAlarm(owner)).not.toBeNull();
      } finally {
        await alarm.catch(() => undefined);
      }
    } finally {
      recoveryReadHolds.delete(old);
      recoveryReplies.delete(owner);
      maintenanceClocks.delete(owner);
    }
  });

  for (const faultKind of ["timeout"] as const) {
    it(`isolates abort control ${faultKind}, skips its backoff, and reports the retained fault once`, () =>
      Effect.runPromise(
        Effect.gen(function* () {
          const owner = `recovery-control-${crypto.randomUUID()}`;
          const old = `${owner}-a`;
          const fresh = `${owner}-z`;
          const reads: Array<string> = [];
          const errors: Array<Cause.Cause<unknown>> = [];
          const entered = yield* Deferred.make<void>();
          let retainedId: string | undefined;
          let abortReads = 0;
          let active = 0;

          const run = localRun(owner, reads, {
            readAbortIntent: (read) => (request) =>
              Effect.suspend(() => {
                if (request.submissionId !== retainedId) return read(request);
                abortReads++;

                return Effect.acquireUseRelease(
                  Effect.sync(() => {
                    active++;
                  }),
                  () => Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)),
                  () =>
                    Effect.sync(() => {
                      active--;
                    }),
                );
              }),
          });

          const observedPass = pass.pipe(
            Effect.provide(
              Logger.layer([
                Logger.make(({ cause, logLevel }) => {
                  if (logLevel === "Error") errors.push(cause);
                }),
              ]),
            ),
          );

          yield* TestClock.setTime(Date.now() + 86_400_000);
          maintenanceClocks.set(owner, yield* Clock.Clock);
          yield* Effect.addFinalizer(() => Effect.sync(() => maintenanceClocks.delete(owner)));
          const receipt = yield* run(submit(old, "retained"));

          retainedId = receipt.submissionId;
          yield* run(
            ThreadMaintenance.use((maintenance) =>
              maintenance.withMutation(
                DurableAgentRuntime.use((runtime) =>
                  runtime.abort(
                    AbortCommand.make({
                      submissionId: receipt.submissionId,
                      author: "fixture-owner",
                      reason: "retire retained input",
                    }),
                  ),
                ),
              ),
            ),
          );

          const retained = yield* run(
            SubmissionLedger.use((ledger) =>
              ledger.lookup(SubmissionLookupById.make({ submissionId: receipt.submissionId })),
            ),
          );

          const accepted = yield* run(submit(fresh, "fresh"));
          const running = yield* run(observedPass).pipe(Effect.forkChild);

          {
            yield* Deferred.await(entered);
            expect(active).toBe(1);
            yield* TestClock.adjust(30_000);
          }
          yield* Fiber.join(running);
          expect(active).toBe(0);
          expect(abortReads).toBe(1);
          expect(reads).not.toContain(old);
          const fault = yield* run(status(old));

          expect(fault).toMatchObject({
            _tag: "Some",
            value: {
              attempts: 1,
              failure: {
                phase: "recovery",
                reason: "timeout",
                operation: "read abort intent",
              },
            },
          });
          expect(
            yield* run(DurableAgentRuntime.use((runtime) => runtime.submissionStatus(accepted))),
          ).toMatchObject({
            _tag: "settled",
            settlement: { outcome: "completed" },
          });

          // Fresh ingress does not bypass the blocked Thread's deadline or reread its row.
          const later = yield* run(submit(fresh, "later"));

          yield* run(observedPass);
          expect(
            yield* run(DurableAgentRuntime.use((runtime) => runtime.submissionStatus(later))),
          ).toMatchObject({
            _tag: "settled",
            settlement: { outcome: "completed" },
          });
          expect(abortReads).toBe(1);
          expect(yield* run(status(old))).toEqual(fault);
          expect(yield* run(submit(old, "retained"))).toEqual(receipt);
          expect(
            yield* run(
              SubmissionLedger.use((ledger) =>
                ledger.lookup(SubmissionLookupById.make({ submissionId: receipt.submissionId })),
              ),
            ),
          ).toEqual(retained);
          expect(errors).toHaveLength(1);
          expect(errors.flatMap((cause) => Cause.prettyErrors(cause))).toHaveLength(1);
          expect(JSON.stringify(errors)).not.toContain("private_control_fixture");
          expect(yield* Effect.promise(() => scheduledAlarm(owner))).not.toBeNull();
        }).pipe(Effect.scoped, Effect.provide(TestClock.layer())),
      ));
  }
});

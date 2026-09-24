import { DoStorageFailpoint } from "@effect-agent/storage-cloudflare/do-storage-failpoint";
import { submissionLedgerLayer } from "@effect-agent/storage-cloudflare/do-submission-ledger";
import {
  storageConfigLayer,
  threadStoreLayer,
} from "@effect-agent/storage-cloudflare/do-thread-store";
import { runInDurableObject } from "cloudflare:test";
import { Cause, Clock, Deferred, Effect, Exit, Fiber, Layer, Option, Schema, Stream } from "effect";
import { DurableAgentRuntime } from "effect-agent/durable-agent-runtime";
import { ProducerId } from "effect-agent/records";
import {
  ClaimRequest,
  RecoverySnapshotRequest,
  ReleaseOwnershipRequest,
  SubmissionLedger,
  SubmissionLookupById,
} from "effect-agent/submission-ledger";
import { ThreadStore, ThreadTailRequest } from "effect-agent/thread-store";
import { WakeScheduler } from "effect-agent/wake-scheduler";
import { DurableObject } from "effect-cf";
import { TestClock } from "effect/testing";
import { describe, expect, it } from "vite-plus/test";

import {
  DurableAlarmError,
  ThreadHostMaintenance,
  ThreadMaintenance,
  ThreadMutationGate,
  ThreadMaintenanceFailpoint,
} from "../src/Alarm.ts";
import { CloudflareDurableRuntimeConfig } from "../src/CloudflareConfig.ts";
import { CloudflareThreadClient } from "../src/CloudflareThreadClient.ts";
import {
  maintenanceClocks,
  decodeThreadId,
  makeTestBindings,
  plannerDefinition,
  submitOptions,
} from "./fixtures.ts";
import { laneRows, readCanonical, runClient, scheduledAlarm, stubFor } from "./harness.ts";

const Generation = Schema.Struct({
  dirty: Schema.BigIntFromString,
  processed: Schema.BigIntFromString,
});

describe("maintenance retry deadlines", () => {
  // Regression: https://reve-r6.sentry.io/issues/KOMMUNIKASIE-API-C9
  it.each(["typed", "timeout"] as const)(
    "keeps admission and native dispatch available after an independent %s failure",
    (failure) =>
      Effect.runPromise(
        Effect.gen(function* () {
          const thread = `maintenance-isolation-${crypto.randomUUID()}`;

          yield* TestClock.setTime(Date.now() + 86_400_000);
          maintenanceClocks.set(thread, yield* Clock.Clock);
          yield* Effect.addFinalizer(() => Effect.sync(() => maintenanceClocks.delete(thread)));
          yield* Effect.promise(() =>
            runInDurableObject(stubFor(thread), (instance, state) =>
              instance[DurableObject.RunSymbol](
                Effect.gen(function* () {
                  yield* TestClock.setTime(Date.now() + 86_400_000);
                  const clock = yield* TestClock.testClockWith(Effect.succeed);
                  const bindings = yield* makeTestBindings;
                  const config = yield* CloudflareDurableRuntimeConfig;
                  const entered = yield* Deferred.make<void>();
                  const admit = yield* Deferred.make<void>();
                  const finish = yield* Deferred.make<void>();
                  let completed = false;
                  let active = false;
                  let failedAttempts = 0;

                  const ports = Layer.mergeAll(threadStoreLayer, submissionLedgerLayer).pipe(
                    Layer.provide(
                      storageConfigLayer({
                        storage: state.storage,
                        ownershipLeaseDuration: config.ownershipLeaseDuration,
                      }),
                    ),
                    Layer.provide(DoStorageFailpoint.layer),
                  );

                  const services = DurableAgentRuntime.layerWithBindings(bindings).pipe(
                    Layer.provideMerge(ports),
                    Layer.provide(WakeScheduler.layerNoop),
                  );

                  yield* Effect.gen(function* () {
                    const runtime = yield* DurableAgentRuntime;
                    const ledger = yield* SubmissionLedger;
                    const gate = yield* ThreadMutationGate;

                    const submitted =
                      yield* Deferred.make<
                        Effect.Success<ReturnType<typeof runtime.submitRegistered>>
                      >();

                    const host = ThreadHostMaintenance.of({
                      lanes: [
                        {
                          dispatchTimeoutMillis: 1_000,
                          pendingDeadline: Effect.succeed(Option.some(0)),
                          run: Effect.gen(function* () {
                            failedAttempts++;
                            yield* Deferred.await(entered);
                            switch (failure) {
                              case "typed":
                                return yield* DurableAlarmError.make({
                                  operation: "browser cleanup",
                                  message: "Cleanup remains pending",
                                });
                              case "timeout":
                                return yield* Effect.never;
                            }
                          }),
                        },
                        {
                          dispatchTimeoutMillis: 5_000,
                          pendingDeadline: Effect.sync(() =>
                            completed ? Option.none() : Option.some(0),
                          ),
                          run: Effect.gen(function* () {
                            yield* Effect.acquireRelease(
                              Effect.sync(() => {
                                active = true;
                              }),
                              () =>
                                Effect.sync(() => {
                                  active = false;
                                }),
                            );
                            yield* Deferred.succeed(entered, undefined);
                            yield* Deferred.await(admit);

                            const receipt = yield* gate
                              .withMutation(
                                runtime.submitRegistered(
                                  { definition: plannerDefinition },
                                  { question: "admitted after cleanup failed", ref: thread },
                                  submitOptions(thread, thread),
                                ),
                              )
                              .pipe(Effect.orDie);

                            yield* Deferred.succeed(submitted, receipt);
                            yield* Deferred.await(finish);
                            completed = true;
                          }),
                        },
                      ],
                    });

                    yield* Effect.gen(function* () {
                      const maintenance = yield* ThreadMaintenance;
                      const running = yield* Effect.forkChild(maintenance.pass);

                      yield* Deferred.await(entered);
                      yield* clock.adjust(1_001);
                      expect(running.pollUnsafe()).toBeUndefined();
                      expect(active).toBe(true);
                      yield* Deferred.succeed(admit, undefined);
                      const receipt = yield* Deferred.await(submitted);

                      for (let elapsed = 0; elapsed < 500; elapsed += 100) {
                        yield* clock.adjust(100);
                        if ((yield* Stream.runCollect(ledger.scanNonterminal)).length === 0) break;
                      }

                      const row = yield* ledger.lookup(
                        SubmissionLookupById.make({ submissionId: receipt.submissionId }),
                      );

                      expect(Option.isSome(row) ? row.value.state : "missing").toBe("settled");
                      yield* Deferred.succeed(finish, undefined);
                      expect(Exit.isFailure(yield* Fiber.await(running))).toBe(true);
                      expect({ completed, active, failedAttempts }).toEqual({
                        completed: true,
                        active: false,
                        failedAttempts: 1,
                      });
                      expect(yield* Effect.promise(() => state.storage.getAlarm())).not.toBeNull();
                    }).pipe(
                      Effect.provide(Layer.fresh(ThreadMaintenance.layer)),
                      Effect.provideService(ThreadHostMaintenance, host),
                    );
                  }).pipe(Effect.provide(services));
                }).pipe(Effect.scoped, Effect.provide(TestClock.layer())),
              ),
            ),
          );
        }).pipe(Effect.scoped, Effect.provide(TestClock.layer())),
      ),
  );

  // A missing root agent must not repeatedly claim and release the same receipt behind a
  // 50ms pre-arm, even when auxiliary work fails or the Object is evicted.
  // Native SQLite and Object eviction use a wall-clock budget; deadlines use TestClock.
  it("retains root binding backoff across auxiliary failures, ensureAlarm and eviction", ({
    signal,
  }) => {
    return Effect.runPromise(
      Effect.gen(function* () {
        const thread = `maintenance-retry-${crypto.randomUUID()}`;

        yield* TestClock.setTime(Date.now() + 86_400_000);
        maintenanceClocks.set(thread, yield* Clock.Clock);
        yield* Effect.addFinalizer(() => Effect.sync(() => maintenanceClocks.delete(thread)));

        const receipt = yield* Effect.promise(() =>
          runClient(
            CloudflareThreadClient.use((client) =>
              client.submit(
                { definition: plannerDefinition },
                { question: "retry the original contract", ref: thread },
                submitOptions(thread, thread),
              ),
            ),
          ),
        );

        const canonicalBefore = yield* Effect.promise(() => readCanonical(thread));
        let available = false;
        let hostDeadline: number | undefined;
        let hostDrains = 0;
        let hostFailure = false;
        let activeHostResources = 0;

        // Rebuild real runtime/maintenance services over this Object's SQLite adapters.
        // Local ports let this physical owner exercise multiple logical Threads, rather
        // than the standard fixture's one-Thread-per-Object transport. Keep its native
        // SQL connection, clock and mutation gate; never rewrite stored submissions.
        const run = <A, E>(body: Effect.Effect<A, E, ThreadMaintenance | DurableAgentRuntime>) =>
          Effect.promise(() =>
            runInDurableObject(stubFor(thread), (instance, state) =>
              instance[DurableObject.RunSymbol](
                Effect.gen(function* () {
                  const bindings = yield* makeTestBindings;
                  const config = yield* CloudflareDurableRuntimeConfig;

                  const deployed = bindings.filter(
                    (binding) => available || binding.agentId !== plannerDefinition.id,
                  );

                  const ports = Layer.mergeAll(threadStoreLayer, submissionLedgerLayer).pipe(
                    Layer.provide(
                      storageConfigLayer({
                        storage: state.storage,
                        ownershipLeaseDuration: config.ownershipLeaseDuration,
                      }),
                    ),
                    Layer.provide(DoStorageFailpoint.layer),
                  );

                  const maintenance = Layer.fresh(ThreadMaintenance.layer).pipe(
                    Layer.provideMerge(DurableAgentRuntime.layerWithBindings(deployed)),
                    Layer.provide(ports),
                  );

                  return yield* body.pipe(
                    Effect.provide(maintenance),
                    Effect.provideService(ThreadMaintenanceFailpoint, {
                      hit: (location) =>
                        Effect.sync(() => {
                          // #500 owns hook resources until event retirement. Backoff follows cleanup.
                          if (location === "maintenance:retry:before")
                            expect(activeHostResources).toBe(0);
                        }),
                    }),
                    Effect.provideService(CloudflareDurableRuntimeConfig, {
                      ...config,
                      alarmBackoffBase: 100,
                      alarmBackoffCap: 100,
                      wakeScanInterval: 1_000,
                    }),
                    Effect.provideService(ThreadHostMaintenance, {
                      lanes: [
                        {
                          dispatchTimeoutMillis: 1_000,
                          pendingDeadline: Effect.sync(() => Option.fromUndefinedOr(hostDeadline)),
                          run: Effect.gen(function* () {
                            yield* Effect.acquireRelease(
                              Effect.sync(() => {
                                activeHostResources++;
                              }),
                              () =>
                                Effect.sync(() => {
                                  activeHostResources--;
                                }),
                            );
                            if (hostFailure)
                              return yield* DurableAlarmError.make({
                                operation: "test host failure",
                                message: "host delivery remains pending",
                              });
                            if (hostDeadline !== undefined) {
                              hostDrains++;
                              hostDeadline = undefined;
                            }
                          }),
                        },
                      ],
                    }),
                    Effect.exit,
                  );
                }),
              ),
            ),
          );

        const pass = ThreadMaintenance.use((maintenance) => maintenance.pass);
        const ensure = ThreadMaintenance.use((maintenance) => maintenance.ensureAlarm);

        const snapshot = (submissionId = receipt.submissionId) =>
          Effect.promise(() =>
            runInDurableObject(stubFor(thread), (instance) =>
              instance[DurableObject.RunSymbol](
                Effect.gen(function* () {
                  const ledger = yield* SubmissionLedger;
                  const store = yield* ThreadStore;

                  const snapshot = yield* ledger.loadRecoverySnapshot(
                    RecoverySnapshotRequest.make({ submissionId }),
                  );

                  const tail = yield* store.inspectTail(
                    ThreadTailRequest.make({ threadId: decodeThreadId(thread) }),
                  );

                  return { ...snapshot, producerEpoch: tail.producerEpoch };
                }),
              ),
            ),
          );

        const evict = () =>
          Effect.promise(() =>
            runInDurableObject(stubFor(thread), (_instance, state) => {
              state.abort("maintenance retry restart");
            }).catch(() => undefined),
          );

        const refused = yield* run(
          DurableAgentRuntime.use((runtime) => runtime.processThreadHead(decodeThreadId(thread))),
        );

        expect(
          Exit.isFailure(refused) ? Cause.pretty(refused.cause) : "unexpected execution",
        ).toContain("BindingUnavailable");
        expect((yield* snapshot()).ownership).toBeUndefined();

        // Constructor repair and auxiliary failures must preserve the earned binding wait.
        const delays = [5_000];

        for (const [attempt, delay] of delays.entries()) {
          const before = yield* Clock.currentTimeMillis;

          hostFailure = true;
          const failed = yield* run(pass);

          expect(Exit.isFailure(failed) ? Cause.pretty(failed.cause) : "success").toContain(
            "host delivery remains pending",
          );
          const afterBindingFailure = yield* snapshot();

          expect(afterBindingFailure.ownership).toBeUndefined();
          const genericRetry = yield* Effect.promise(() => scheduledAlarm(thread));

          // An auxiliary error must retain the longer binding wait already earned by the
          // selected head. A second failed event at the generic deadline cannot claim it again.
          expect(genericRetry).toBeGreaterThanOrEqual(before + 50);
          expect(genericRetry).toBeLessThanOrEqual(before + 100);
          yield* evict();
          yield* run(ensure);
          expect(yield* Effect.promise(() => scheduledAlarm(thread))).toBe(genericRetry);
          yield* TestClock.adjust(genericRetry! - before);
          expect(Exit.isFailure(yield* run(pass))).toBe(true);
          expect(yield* snapshot()).toEqual(afterBindingFailure);
          hostFailure = false;
          const nextRetry = yield* Effect.promise(() => scheduledAlarm(thread));

          yield* TestClock.adjust(nextRetry! - (yield* Clock.currentTimeMillis));
          expect(Exit.isSuccess(yield* run(pass))).toBe(true);
          expect(yield* snapshot()).toEqual(afterBindingFailure);
          expect(yield* Effect.promise(() => scheduledAlarm(thread))).toBe(before + delay);
          yield* run(ensure);
          yield* evict();
          yield* run(ensure);
          expect(yield* Effect.promise(() => scheduledAlarm(thread))).toBe(before + delay);
          const afterFailure = yield* snapshot();

          // A host deadline and a forced redelivery do not run native recovery early.
          const hostNow = yield* Clock.currentTimeMillis;

          hostDeadline = attempt === 0 ? before : before + 1_000;
          yield* run(ensure);
          expect(yield* Effect.promise(() => scheduledAlarm(thread))).toBe(
            attempt === 0 ? hostNow + 50 : hostDeadline,
          );
          yield* TestClock.adjust(before + 1_000 - (yield* Clock.currentTimeMillis));
          expect(Exit.isSuccess(yield* run(pass))).toBe(true);
          expect(yield* snapshot()).toEqual(afterFailure);
          expect(yield* Effect.promise(() => scheduledAlarm(thread))).toBe(before + delay);
          expect(Exit.isSuccess(yield* run(pass))).toBe(true);
          expect(yield* snapshot()).toEqual(afterFailure);
          yield* TestClock.adjust(before + delay - (yield* Clock.currentTimeMillis));
        }
        expect(hostDrains).toBe(delays.length);
        expect(yield* Effect.promise(() => readCanonical(thread))).toEqual(canonicalBefore);
        expect((yield* Effect.promise(() => laneRows(thread)))[0]?.state).not.toBe("settled");

        const generation = yield* Effect.promise(() =>
          runInDurableObject(stubFor(thread), async (_instance, state) =>
            Schema.decodeUnknownSync(Generation)(
              await state.storage.get("effect-agent:thread-maintenance:v1"),
            ),
          ),
        );

        expect(generation.dirty).toBeGreaterThan(generation.processed);

        hostFailure = true;

        available = true;
        const resumed = yield* run(pass);

        expect(Exit.isFailure(resumed) ? Cause.pretty(resumed.cause) : "success").toContain(
          "host delivery remains pending",
        );

        const retainedRetries = yield* Effect.promise(() =>
          runInDurableObject(stubFor(thread), async (_instance, state) =>
            Schema.decodeUnknownSync(
              Schema.Struct({
                bindingRetries: Schema.Array(Schema.Struct({ submissionId: Schema.String })),
              }),
            )(await state.storage.get("effect-agent:thread-maintenance:v1")),
          ),
        );

        // An available agent clears its prior binding wait even if the host join fails.
        expect(retainedRetries.bindingRetries.map((retry) => retry.submissionId)).not.toContain(
          receipt.submissionId,
        );

        const settlement = yield* Effect.promise(() =>
          runClient(CloudflareThreadClient.use((client) => client.awaitSettlement(receipt))),
        );

        expect(settlement.submissionId).toBe(receipt.submissionId);
        expect(settlement.outcome).toBe("completed");
        hostFailure = false;
        const finalRetry = yield* Effect.promise(() => scheduledAlarm(thread));

        yield* TestClock.adjust(finalRetry! - (yield* Clock.currentTimeMillis));
        expect(Exit.isSuccess(yield* run(pass))).toBe(true);
        expect(yield* Effect.promise(() => scheduledAlarm(thread))).toBeNull();
      }).pipe(Effect.scoped, Effect.provide(TestClock.layer())),
      { signal },
    );
  }, 20_000);

  it("lets no-progress backoff exceed the scan cadence while preserving a live claim", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const thread = `maintenance-owned-${crypto.randomUUID()}`;

        yield* TestClock.setTime(Date.now() + 86_400_000);
        maintenanceClocks.set(thread, yield* Clock.Clock);
        yield* Effect.addFinalizer(() => Effect.sync(() => maintenanceClocks.delete(thread)));
        yield* Effect.promise(() =>
          runClient(
            CloudflareThreadClient.use((client) =>
              client.submit(
                { definition: plannerDefinition },
                { question: "live owner", ref: thread },
                submitOptions(thread, thread),
              ),
            ),
          ),
        );
        const clock = yield* TestClock.testClockWith(Effect.succeed);

        yield* Effect.promise(() =>
          runInDurableObject(stubFor(thread), (instance, state) =>
            instance[DurableObject.RunSymbol](
              Effect.gen(function* () {
                const ledger = yield* SubmissionLedger;
                const config = yield* CloudflareDurableRuntimeConfig;

                const claim = yield* ledger.claim(
                  ClaimRequest.make({
                    threadId: decodeThreadId(thread),
                    producerId: ProducerId.make("other-live-owner"),
                  }),
                );

                expect(Option.isSome(claim)).toBe(true);
                if (Option.isNone(claim)) return;
                yield* Effect.gen(function* () {
                  const maintenance = yield* ThreadMaintenance;

                  // Two backoff stages fit inside the fixture's 250ms lease even at their
                  // maximum delays (10 + 20 + 40 + 40), so no pass may steal ownership.
                  for (let stage = 0; stage < 2; stage++) {
                    const now = yield* Clock.currentTimeMillis;

                    const report = yield* ThreadMaintenance.use((fresh) => fresh.pass).pipe(
                      Effect.provide(Layer.fresh(ThreadMaintenance.layer)),
                    );

                    expect(report.settled).toBe(0);
                    const deadline = yield* Effect.promise(() => state.storage.getAlarm());

                    expect(deadline).toBeGreaterThan(now + 1);
                    expect(deadline).toBeGreaterThanOrEqual(now + [5, 10][stage]!);
                    expect(deadline).toBeLessThanOrEqual(now + 40);
                    yield* maintenance.ensureAlarm;
                    expect(yield* Effect.promise(() => state.storage.getAlarm())).toBe(deadline);
                    if (stage < 1) yield* clock.adjust(deadline! - now);
                  }

                  const snapshot = yield* ledger.loadRecoverySnapshot(
                    RecoverySnapshotRequest.make({ submissionId: claim.value.submissionId }),
                  );

                  expect(snapshot.ownership?.attemptId).toBe(claim.value.attemptId);
                  // An accepted durable mutation supersedes the retry, before its deadline.
                  yield* maintenance.withMutation(
                    ledger.releaseOwnership(
                      ReleaseOwnershipRequest.make({
                        submissionId: claim.value.submissionId,
                        ownershipToken: claim.value.ownershipToken,
                      }),
                    ),
                  );
                  expect((yield* maintenance.pass).settled).toBe(1);
                }).pipe(
                  Effect.provide(Layer.fresh(ThreadMaintenance.layer)),
                  Effect.provideService(CloudflareDurableRuntimeConfig, {
                    ...config,
                    alarmBackoffBase: 10,
                    alarmBackoffCap: 40,
                    wakeScanInterval: 1,
                  }),
                );
              }),
            ),
          ),
        );
      }).pipe(Effect.scoped, Effect.provide(TestClock.layer())),
    ));
});

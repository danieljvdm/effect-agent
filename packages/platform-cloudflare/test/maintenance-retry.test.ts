import { DoStorageFailpoint } from "@effect-agent/storage-cloudflare/do-storage-failpoint";
import { submissionLedgerLayer } from "@effect-agent/storage-cloudflare/do-submission-ledger";
import {
  storageConfigLayer,
  threadStoreLayer,
} from "@effect-agent/storage-cloudflare/do-thread-store";
import { runInDurableObject } from "cloudflare:test";
import { Cause, Clock, Effect, Exit, Layer, Option, Schema } from "effect";
import { DurableAgentRuntime } from "effect-agent/durable-agent-runtime";
import { DefinitionDigests, Digest, ProducerId } from "effect-agent/records";
import {
  ClaimRequest,
  RecoverySnapshotRequest,
  ReleaseOwnershipRequest,
  SubmissionLedger,
} from "effect-agent/submission-ledger";
import { ThreadStore, ThreadTailRequest } from "effect-agent/thread-store";
import { DurableObject } from "effect-cf";
import { TestClock } from "effect/testing";
import { describe, expect, it } from "vite-plus/test";

import {
  DurableAlarmError,
  ThreadHostMaintenance,
  ThreadMaintenance,
  ThreadMaintenanceFailpoint,
  type ThreadMaintenanceFailpointLocation,
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
  // Provenance: September 2026 Sentry incident — a root digest mismatch repeatedly claimed
  // and released the same receipt behind a 50ms pre-arm. Private customer identifiers omitted.
  it("retains root binding backoff across auxiliary failures, ensureAlarm and eviction", () =>
    Effect.runPromise(
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
        let compatible = false;
        let hostDeadline: number | undefined;
        let hostDrains = 0;
        let hostFailure = false;
        let activeHostResources = 0;
        let crashAt: ThreadMaintenanceFailpointLocation | undefined;

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
                  const failpoint = yield* ThreadMaintenanceFailpoint;

                  const changed = bindings.map((binding) => ({
                    ...binding,
                    digests: compatible
                      ? binding.digests
                      : DefinitionDigests.make({
                          ...binding.digests,
                          agent: Schema.decodeSync(Digest)("b".repeat(64)),
                        }),
                  }));

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
                    Layer.provideMerge(DurableAgentRuntime.layerWithBindings(changed)),
                    Layer.provide(ports),
                  );

                  return yield* body.pipe(
                    Effect.provide(maintenance),
                    Effect.provideService(ThreadMaintenanceFailpoint, {
                      hit: (location) =>
                        Effect.gen(function* () {
                          // #500 owns hook resources until event retirement. Failure backoff
                          // must observe cleanup already complete, even if its commit crashes.
                          if (location === "maintenance:retry:before")
                            expect(activeHostResources).toBe(0);
                          if (crashAt !== location) return yield* failpoint.hit(location);
                          crashAt = undefined;

                          yield* Effect.sync(() => state.abort("maintenance retry commit crash"));
                        }),
                    }),
                    Effect.provideService(CloudflareDurableRuntimeConfig, {
                      ...config,
                      alarmBackoffBase: 100,
                      alarmBackoffCap: 100,
                      wakeScanInterval: 1_000,
                    }),
                    Effect.provideService(ThreadHostMaintenance, {
                      dispatchTimeoutMillis: 1_000,
                      pendingDeadline: Effect.sync(() => Option.fromUndefinedOr(hostDeadline)),
                      drainUntil: () =>
                        Effect.gen(function* () {
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
        ).toContain("BindingDigestMismatch");
        expect((yield* snapshot()).ownership).toBeUndefined();

        // Exercise every doubling and two deliveries at the one-minute cap. Constructor
        // repair and unrelated host deadlines must not restart or bypass that schedule.
        const delays = [5_000, 10_000, 20_000, 40_000, 60_000, 60_000];

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

          if (attempt === 0) {
            // Admit a new request against the currently deployed contract in the same Object.
            // Its durable mutation must bypass only the obsolete Object-wide wait; the old
            // incompatible head keeps its own deadline and never blocks this eligible lane.
            const fresh = yield* run(
              ThreadMaintenance.use((maintenance) =>
                maintenance.withMutation(
                  DurableAgentRuntime.use((runtime) =>
                    runtime.submitRegistered(
                      { definition: plannerDefinition },
                      { question: "new compatible request", ref: thread },
                      submitOptions(`${thread}-new`, `${thread}-new`),
                    ),
                  ),
                ),
              ),
            );

            expect(Exit.isFailure(fresh) ? Cause.pretty(fresh.cause) : "admitted").toBe("admitted");
            const otherLane = yield* run(pass);

            expect(Exit.isSuccess(otherLane) ? otherLane.value.settled : "failed").toBe(1);
            expect(yield* snapshot()).toEqual(afterFailure);
            // The following pass restores the old head's deadline after the healthy lane settles.
            expect(Exit.isSuccess(yield* run(pass))).toBe(true);
            expect(yield* Effect.promise(() => scheduledAlarm(thread))).toBe(before + delay);
            expect(yield* snapshot()).toEqual(afterFailure);
          }
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
        for (const location of [
          "maintenance:binding-retry:before",
          "maintenance:binding-retry:after",
          "maintenance:retry:before",
          "maintenance:retry:after",
        ] as const) {
          crashAt = location;
          yield* run(pass).pipe(Effect.exit);
          expect(crashAt).toBeUndefined();
          const afterCrash = yield* snapshot();

          expect(afterCrash.ownership).toBeUndefined();
          // A crash leaves the pre-arm or the committed event retry intact. Only a crash
          // BEFORE saving the binding outcome can repeat that Claim at the short deadline.
          expect(yield* Effect.promise(() => scheduledAlarm(thread))).not.toBeNull();
          yield* run(ensure);
          yield* TestClock.adjust(100);
          expect(Exit.isFailure(yield* run(pass))).toBe(true);
          expect((yield* snapshot()).producerEpoch).toBe(
            afterCrash.producerEpoch + (location === "maintenance:binding-retry:before" ? 1 : 0),
          );
          yield* TestClock.adjust(60_000);
        }

        compatible = true;
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

        // A compatible attempt clears its prior binding wait even if the host join fails.
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

        // Clearing a binding wait is also a durable boundary: the completed receipt must
        // survive either side of that write, and a stale retry must not keep the Object awake.
        for (const location of [
          "maintenance:binding-retry:before",
          "maintenance:binding-retry:after",
        ] as const) {
          const nextReceipt = yield* Effect.promise(() =>
            runClient(
              CloudflareThreadClient.use((client) =>
                client.submit(
                  { definition: plannerDefinition },
                  { question: "resume after retry clear crash", ref: thread },
                  submitOptions(thread, `${thread}-${location}`),
                ),
              ),
            ),
          );

          compatible = false;
          expect(Exit.isSuccess(yield* run(pass))).toBe(true);
          yield* TestClock.adjust(5_000);
          compatible = true;
          crashAt = location;
          yield* run(pass).pipe(Effect.exit);
          expect(crashAt).toBeUndefined();
          const afterCrash = yield* snapshot(nextReceipt.submissionId);

          expect(afterCrash.ownership).toBeUndefined();

          const completed = yield* Effect.promise(() =>
            runClient(CloudflareThreadClient.use((client) => client.awaitSettlement(nextReceipt))),
          );

          expect(completed.submissionId).toBe(nextReceipt.submissionId);
          expect(completed.outcome).toBe("completed");
          yield* run(ensure);
          yield* TestClock.adjust(100);
          expect(Exit.isSuccess(yield* run(pass))).toBe(true);
          expect(yield* snapshot(nextReceipt.submissionId)).toEqual(afterCrash);
          expect(yield* Effect.promise(() => scheduledAlarm(thread))).toBeNull();
        }
      }).pipe(Effect.scoped, Effect.provide(TestClock.layer())),
    ));

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

                  // Four backoff stages fit inside the fixture's 250ms lease even at their
                  // maximum delays (10 + 20 + 40 + 40), so no pass may steal ownership.
                  for (let stage = 0; stage < 4; stage++) {
                    const now = yield* Clock.currentTimeMillis;

                    const report = yield* ThreadMaintenance.use((fresh) => fresh.pass).pipe(
                      Effect.provide(Layer.fresh(ThreadMaintenance.layer)),
                    );

                    expect(report.settled).toBe(0);
                    const deadline = yield* Effect.promise(() => state.storage.getAlarm());

                    expect(deadline).toBeGreaterThan(now + 1);
                    expect(deadline).toBeGreaterThanOrEqual(now + [5, 10, 20, 20][stage]!);
                    expect(deadline).toBeLessThanOrEqual(now + 40);
                    yield* maintenance.ensureAlarm;
                    expect(yield* Effect.promise(() => state.storage.getAlarm())).toBe(deadline);
                    if (stage < 3) yield* clock.adjust(deadline! - now);
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

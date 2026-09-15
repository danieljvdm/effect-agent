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
import { DurableObject } from "effect-cf";
import { TestClock } from "effect/testing";
import { describe, expect, it } from "vite-plus/test";

import {
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
  it("backs off a root digest mismatch across ensureAlarm and eviction without losing the receipt", () =>
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
        let crashAt: ThreadMaintenanceFailpointLocation | undefined;

        // Rebuild real runtime/maintenance services over the native ports on every call.
        // Only the deployed registration changes; stored submissions are never rewritten.
        const run = <A, E>(body: Effect.Effect<A, E, ThreadMaintenance>) =>
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

                  const maintenance = Layer.fresh(ThreadMaintenance.layer).pipe(
                    Layer.provide(DurableAgentRuntime.layerWithBindings(changed)),
                  );

                  return yield* body.pipe(
                    Effect.provide(maintenance),
                    Effect.provideService(ThreadMaintenanceFailpoint, {
                      hit: (location) => {
                        if (crashAt !== location) return failpoint.hit(location);
                        crashAt = undefined;

                        return Effect.sync(() => state.abort("maintenance retry commit crash"));
                      },
                    }),
                    Effect.provideService(CloudflareDurableRuntimeConfig, {
                      ...config,
                      alarmBackoffBase: 100,
                      alarmBackoffCap: 5_000,
                      wakeScanInterval: 1_000,
                    }),
                    Effect.provideService(ThreadHostMaintenance, {
                      pendingDeadline: Effect.sync(() => Option.fromUndefinedOr(hostDeadline)),
                      drainUntil: () =>
                        Effect.sync(() => {
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

        const snapshot = () =>
          Effect.promise(() =>
            runInDurableObject(stubFor(thread), (instance) =>
              instance[DurableObject.RunSymbol](
                SubmissionLedger.use((ledger) =>
                  ledger.loadRecoverySnapshot(
                    RecoverySnapshotRequest.make({ submissionId: receipt.submissionId }),
                  ),
                ),
              ),
            ),
          );

        const evict = () =>
          Effect.promise(() =>
            runInDurableObject(stubFor(thread), (_instance, state) => {
              state.abort("maintenance retry restart");
            }).catch(() => undefined),
          );

        // Three failing deliveries cross recreations; each must retain the full default
        // five-second retry even though constructor repair normally scans every second.
        for (let attempt = 0; attempt < 3; attempt++) {
          const before = yield* Clock.currentTimeMillis;
          const failed = yield* run(pass);

          expect(Exit.isFailure(failed) ? Cause.pretty(failed.cause) : "success").toContain(
            "BindingDigestMismatch",
          );
          expect((yield* snapshot()).ownership).toBeUndefined();
          expect(yield* Effect.promise(() => scheduledAlarm(thread))).toBe(before + 5_000);
          yield* run(ensure);
          yield* evict();
          yield* run(ensure);
          expect(yield* Effect.promise(() => scheduledAlarm(thread))).toBe(before + 5_000);
          const afterFailure = yield* snapshot();

          // A host deadline and a forced redelivery do not run native recovery early.
          hostDeadline = attempt === 0 ? before : before + 1_000;
          yield* run(ensure);
          expect(yield* Effect.promise(() => scheduledAlarm(thread))).toBe(
            attempt === 0 ? before + 50 : hostDeadline,
          );
          yield* TestClock.adjust(1_000);
          expect(Exit.isSuccess(yield* run(pass))).toBe(true);
          expect(yield* snapshot()).toEqual(afterFailure);
          expect(yield* Effect.promise(() => scheduledAlarm(thread))).toBe(before + 5_000);
          expect(Exit.isSuccess(yield* run(pass))).toBe(true);
          expect(yield* snapshot()).toEqual(afterFailure);
          yield* TestClock.adjust(4_000);
        }
        expect(hostDrains).toBe(3);
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

        for (const location of ["maintenance:retry:before", "maintenance:retry:after"] as const) {
          crashAt = location;
          yield* run(pass).pipe(Effect.exit);
          expect(crashAt).toBeUndefined();
          // The old pre-arm or the committed retry survives the kill. After the bounded
          // deadline, the original failure can still be retried and ownership released.
          expect(yield* Effect.promise(() => scheduledAlarm(thread))).not.toBeNull();
          yield* TestClock.adjust(5_000);
        }

        compatible = true;
        const resumed = yield* run(pass);

        expect(Exit.isFailure(resumed) ? Cause.pretty(resumed.cause) : "completed").toBe(
          "completed",
        );

        const settlement = yield* Effect.promise(() =>
          runClient(CloudflareThreadClient.use((client) => client.awaitSettlement(receipt))),
        );

        expect(settlement.submissionId).toBe(receipt.submissionId);
        expect(settlement.outcome).toBe("completed");
        expect(yield* Effect.promise(() => scheduledAlarm(thread))).toBeNull();
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

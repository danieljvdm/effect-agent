import { DoStorageFailpoint } from "@effect-agent/storage-cloudflare/do-storage-failpoint";
import { submissionLedgerLayer } from "@effect-agent/storage-cloudflare/do-submission-ledger";
import {
  storageConfigLayer,
  threadStoreLayer,
} from "@effect-agent/storage-cloudflare/do-thread-store";
import { runInDurableObject } from "cloudflare:test";
import { Cause, Clock, Deferred, Effect, Exit, Fiber, Layer, Logger, Option, Stream } from "effect";
import { CurrentBindingSelection, type BindingSelection } from "effect-agent/agent-registration";
import { DurableAgentRuntime } from "effect-agent/durable-agent-runtime";
import { ProducerId } from "effect-agent/records";
import {
  ClaimRequest,
  RecoverySnapshotRequest,
  ReleaseOwnershipRequest,
  SubmissionLedger,
  SubmissionLookupById,
  LedgerError,
} from "effect-agent/submission-ledger";
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
  maintenanceBindings,
  decodeThreadId,
  makeTestBindings,
  plannerDefinition,
  submitOptions,
} from "./fixtures.ts";
import { readCanonical, runClient, scheduledAlarm, stubFor } from "./harness.ts";

describe("maintenance retry deadlines", () => {
  // Regression: https://github.com/danieljvdm/effect-agent/commit/e1c3ce677e82589a4b133840e640464472ec2c3f
  it("gives each due post-native lane a turn across full passes and coordinator rebuilds", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const thread = `maintenance-after-native-fairness-${crypto.randomUUID()}`;

        yield* TestClock.setTime(Date.now() + 86_400_000);
        maintenanceClocks.set(thread, yield* Clock.Clock);
        yield* Effect.addFinalizer(() => Effect.sync(() => maintenanceClocks.delete(thread)));
        yield* Effect.promise(() =>
          runInDurableObject(stubFor(thread), (instance) =>
            instance[DurableObject.RunSymbol](
              Effect.gen(function* () {
                yield* TestClock.setTime(Date.now() + 86_400_000);
                const gate = yield* ThreadMutationGate;
                const selected: Array<string> = [];
                let active = 0;
                let maximum = 0;
                let began = yield* Deferred.make<void>();
                let paired = yield* Deferred.make<void>();
                const ids = ["test:a", "test:b", "test:c"];

                const host = ThreadHostMaintenance.of({
                  lanes: ids.map((id) => ({
                    id,
                    phase: "after-native",
                    dispatchTimeoutMillis: 180_001,
                    run: Effect.acquireRelease(
                      Effect.gen(function* () {
                        selected.push(id);
                        active++;
                        maximum = Math.max(maximum, active);
                        if (active === 2) yield* Deferred.succeed(paired, undefined);
                      }),
                      () =>
                        Effect.sync(() => {
                          active--;
                        }),
                    ).pipe(Effect.andThen(Effect.sleep("3 minutes")), Effect.as(Option.some(0))),
                  })),
                });

                for (const id of ids) yield* gate.schedule(id, 0);
                for (let pass = 0; pass < 2; pass++) {
                  began = yield* Deferred.make<void>();
                  paired = yield* Deferred.make<void>();
                  yield* Effect.gen(function* () {
                    const maintenance = yield* ThreadMaintenance;
                    const running = yield* Effect.forkChild(maintenance.pass);

                    yield* Deferred.await(began);
                    yield* TestClock.adjust("10 minutes");
                    yield* Deferred.await(paired);
                    yield* TestClock.adjust("3 minutes");
                    yield* Fiber.join(running);
                  }).pipe(
                    Effect.provide(Layer.fresh(ThreadMaintenance.layer)),
                    Effect.provideService(ThreadHostMaintenance, host),
                    Effect.provideService(ThreadMaintenanceFailpoint, {
                      hit: (location) =>
                        location === "maintenance:begin:before"
                          ? Deferred.succeed(began, undefined).pipe(
                              Effect.andThen(Effect.sleep("10 minutes")),
                            )
                          : Effect.void,
                    }),
                  );
                }
                expect(selected).toEqual(["test:a", "test:b", "test:c", "test:a"]);
                expect(maximum).toBe(2);
                expect(active).toBe(0);
              }).pipe(Effect.scoped, Effect.provide(TestClock.layer())),
            ),
          ),
        );
      }).pipe(Effect.scoped, Effect.provide(TestClock.layer())),
    ));

  // Regression: https://github.com/danieljvdm/effect-agent/commit/0e83011e
  it.each([false, true])("runs one post-native wave after attempt cleanup (failed: %s)", (failed) =>
    Effect.runPromise(
      Effect.gen(function* () {
        const thread = `maintenance-after-native-${crypto.randomUUID()}`;

        yield* TestClock.setTime(Date.now() + 86_400_000);
        maintenanceClocks.set(thread, yield* Clock.Clock);
        yield* Effect.addFinalizer(() => Effect.sync(() => maintenanceClocks.delete(thread)));
        yield* Effect.promise(() =>
          runClient(
            CloudflareThreadClient.use((client) =>
              client.submit(
                { definition: plannerDefinition },
                { question: "post-native", ref: thread },
                submitOptions(thread, thread),
              ),
            ),
          ),
        );
        yield* Effect.promise(() =>
          runInDurableObject(stubFor(thread), (instance, state) =>
            instance[DurableObject.RunSymbol](
              Effect.gen(function* () {
                yield* TestClock.setTime(Date.now() + 86_400_000);
                const runtime = yield* DurableAgentRuntime;
                const gate = yield* ThreadMutationGate;
                const failpoint = yield* ThreadMaintenanceFailpoint;
                const timeoutEntered = yield* Deferred.make<void>();
                const postEntered = yield* Deferred.make<void>();
                const finishPost = yield* Deferred.make<void>();
                const observed: Array<boolean> = [];
                let finalized = false;
                let postActive = false;

                const nativeFailure = LedgerError.make({
                  operation: "native fixture",
                  message: "failed",
                });

                const defect = new Error("native defect");

                yield* Effect.gen(function* () {
                  const maintenance = yield* ThreadMaintenance;

                  yield* gate.schedule("test:after-native", 0);
                  if (failed) yield* gate.schedule("test:after-timeout", 0);
                  if (failed) yield* Deferred.succeed(finishPost, undefined);
                  const running = yield* Effect.forkChild(maintenance.pass);

                  if (failed) {
                    yield* Deferred.await(timeoutEntered);
                    yield* TestClock.adjust(1_001);
                  } else {
                    yield* Deferred.await(postEntered);
                    const next = yield* Effect.forkChild(maintenance.pass);

                    yield* TestClock.adjust(1);
                    expect(next.pollUnsafe()).toBeUndefined();
                    yield* Deferred.succeed(finishPost, undefined);
                    yield* Fiber.join(next);
                  }
                  const exit = yield* Fiber.await(running);

                  expect(observed).toEqual([true]);
                  expect(
                    state.storage.sql
                      .exec<{ dueAt: number | null }>(
                        "SELECT dueAt FROM platform_cloudflare_due_queue WHERE id = 'test:after-native'",
                      )
                      .one().dueAt,
                  ).toBeNull();
                  expect(Exit.isFailure(exit)).toBe(failed);
                  if (Exit.isFailure(exit)) {
                    expect(Cause.hasDies(exit.cause)).toBe(true);
                    expect(Cause.pretty(exit.cause)).toContain("LedgerError: failed");
                    expect(Cause.pretty(exit.cause)).toContain(
                      "The admitted host wave exceeded its allowance",
                    );
                    expect(yield* Effect.promise(() => state.storage.getAlarm())).not.toBeNull();
                  }
                }).pipe(
                  Effect.provide(Layer.fresh(ThreadMaintenance.layer)),
                  Effect.provideService(ThreadMaintenanceFailpoint, {
                    hit: (location) =>
                      failpoint.hit(location).pipe(
                        Effect.tap(() =>
                          Effect.sync(() => {
                            if (location === "maintenance:begin:before")
                              expect(postActive).toBe(false);
                          }),
                        ),
                      ),
                  }),
                  Effect.provideService(DurableAgentRuntime, {
                    ...runtime,
                    processThreadHead: (...args) =>
                      runtime.processThreadHead(...args).pipe(
                        Effect.filterOrElse(
                          () => !failed,
                          () =>
                            Effect.failCause(
                              Cause.combine(Cause.fail(nativeFailure), Cause.die(defect)),
                            ),
                        ),
                        Effect.ensuring(
                          gate.schedule("test:after-native", 0).pipe(
                            Effect.orDie,
                            Effect.andThen(
                              Effect.sync(() => {
                                finalized = true;
                              }),
                            ),
                          ),
                        ),
                      ),
                  }),
                  Effect.provideService(ThreadHostMaintenance, {
                    lanes: [
                      {
                        id: "test:after-native",
                        phase: "after-native",
                        dispatchTimeoutMillis: 1_000,
                        run: Effect.gen(function* () {
                          observed.push(finalized);
                          postActive = true;
                          yield* Deferred.succeed(postEntered, undefined);
                          yield* Deferred.await(finishPost);
                          postActive = false;

                          return Option.none<number>();
                        }),
                      },
                      {
                        id: "test:after-timeout",
                        phase: "after-native",
                        dispatchTimeoutMillis: 1_000,
                        run: Deferred.succeed(timeoutEntered, undefined).pipe(
                          Effect.andThen(Effect.never),
                        ),
                      },
                    ],
                  }),
                );
              }).pipe(Effect.scoped, Effect.provide(TestClock.layer())),
            ),
          ),
        );
      }).pipe(Effect.scoped, Effect.provide(TestClock.layer())),
    ),
  );

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
                          id: "test:failure",
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
                          id: "test:admission",
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

                            return Option.none<number>();
                          }),
                        },
                      ],
                    });

                    yield* Effect.gen(function* () {
                      const maintenance = yield* ThreadMaintenance;

                      yield* gate.schedule("test:failure", 0);
                      yield* gate.schedule("test:admission", 0);
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

  // Human-requested regression: zero and duplicate bindings must park accepted work across
  // Object eviction and auxiliary failure, report once, and resume when registration changes.
  for (const unavailable of ["missing", "duplicate"] as const) {
    it(`parks a ${unavailable} binding until the registry changes`, async ({ signal }) => {
      await Effect.runPromise(
        Effect.gen(function* () {
          const thread = `maintenance-binding-${crypto.randomUUID()}`;

          yield* TestClock.setTime(Date.now() + 86_400_000);
          maintenanceClocks.set(thread, yield* Clock.Clock);
          yield* Effect.addFinalizer(() => Effect.sync(() => maintenanceClocks.delete(thread)));

          const receipt = yield* Effect.promise(() =>
            runClient(
              CloudflareThreadClient.use((client) =>
                client.submit(
                  { definition: plannerDefinition },
                  { question: "preserve the accepted contract", ref: thread },
                  submitOptions(thread, thread),
                ),
              ),
            ),
          );

          const canonicalBefore = yield* Effect.promise(() => readCanonical(thread));

          yield* Effect.addFinalizer(() => Effect.sync(() => maintenanceBindings.delete(thread)));
          const otherDefinition = { ...plannerDefinition };
          let available = false;
          let reorderMetadata = false;
          let interruptParking = unavailable === "missing";
          let hostFailure = true;
          let hostDrains = 0;
          const errors: string[] = [];

          const logger = Logger.make((options) => {
            if (options.logLevel === "Error") errors.push(String(options.message));
          });

          const run = <A, E>(
            body: Effect.Effect<A, E, ThreadMaintenance | DurableAgentRuntime | ThreadMutationGate>,
          ) =>
            Effect.promise(() =>
              runInDurableObject(stubFor(thread), (instance, state) =>
                instance[DurableObject.RunSymbol](
                  Effect.gen(function* () {
                    const bindings = (yield* makeTestBindings).map((binding) =>
                      reorderMetadata
                        ? {
                            ...binding,
                            digests: {
                              ...(binding.digests.replay === undefined
                                ? {}
                                : { replay: binding.digests.replay }),
                              tools: binding.digests.tools,
                              model: binding.digests.model,
                              agent: binding.digests.agent,
                            },
                          }
                        : binding,
                    );

                    const config = yield* CloudflareDurableRuntimeConfig;

                    const planner = bindings.find(
                      (binding) => binding.agentId === plannerDefinition.id,
                    )!;

                    const deployed =
                      unavailable === "duplicate"
                        ? [
                            ...bindings,
                            {
                              ...planner,
                              definition: otherDefinition,
                              attempt: () => Effect.die("The wrong executable was selected"),
                            },
                          ]
                        : available
                          ? bindings
                          : bindings.filter((binding) => binding.agentId !== plannerDefinition.id);

                    const selection: BindingSelection | undefined =
                      available && unavailable === "duplicate"
                        ? {
                            key: "planner-input-v1",
                            select: (submission) =>
                              Effect.succeed(
                                submission.agentId === plannerDefinition.id &&
                                  typeof submission.inputPayload === "object" &&
                                  submission.inputPayload !== null &&
                                  "question" in submission.inputPayload
                                  ? plannerDefinition
                                  : undefined,
                              ),
                          }
                        : undefined;

                    maintenanceBindings.set(thread, { bindings: deployed, selection });

                    const ports = Layer.mergeAll(threadStoreLayer, submissionLedgerLayer).pipe(
                      Layer.provide(
                        storageConfigLayer({
                          storage: state.storage,
                          ownershipLeaseDuration: config.ownershipLeaseDuration,
                        }),
                      ),
                      Layer.provide(DoStorageFailpoint.layer),
                    );

                    return yield* body.pipe(
                      Effect.provide(
                        Layer.fresh(ThreadMaintenance.layer).pipe(
                          Layer.provideMerge(DurableAgentRuntime.layerWithBindings(deployed)),
                          Layer.provide(Layer.succeed(CurrentBindingSelection, selection)),
                          Layer.provide(ports),
                        ),
                      ),
                      Effect.provideService(CloudflareDurableRuntimeConfig, {
                        ...config,
                        alarmBackoffBase: 100,
                        alarmBackoffCap: 100,
                      }),
                      Effect.provideService(ThreadHostMaintenance, {
                        lanes: [
                          {
                            dispatchTimeoutMillis: 1_000,
                            id: "test:host",
                            run: Effect.gen(function* () {
                              hostDrains++;
                              if (hostFailure)
                                return yield* DurableAlarmError.make({
                                  operation: "test host failure",
                                  message: "host delivery remains pending",
                                });

                              return Option.none();
                            }),
                          },
                        ],
                      }),
                      Effect.provide(Logger.layer([logger])),
                      Effect.provideService(ThreadMaintenanceFailpoint, {
                        hit: (location) => {
                          if (interruptParking && location === "maintenance:binding-retry:after") {
                            interruptParking = false;

                            return Effect.interrupt;
                          }

                          return Effect.void;
                        },
                      }),
                      Effect.exit,
                    );
                  }),
                ),
              ),
            );

          const pass = ThreadMaintenance.use((maintenance) => maintenance.pass);
          const ensure = ThreadMaintenance.use((maintenance) => maintenance.ensureAlarm);

          const snapshot = Effect.promise(() =>
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

          const evict = Effect.promise(() =>
            runInDurableObject(stubFor(thread), (_instance, state) => {
              state.abort("binding registry restart");
            }).catch(() => undefined),
          );

          yield* run(ThreadMutationGate.use((gate) => gate.schedule("test:host", 0)));
          expect(Exit.isFailure(yield* run(pass))).toBe(true);
          const parked = yield* snapshot;

          expect(parked.ownership).toBeUndefined();
          expect(parked.submission.state).toBe("ready");
          expect(errors.filter((message) => message.includes("binding"))).toHaveLength(1);

          hostFailure = false;
          yield* TestClock.adjust("1 second");
          expect(Exit.isSuccess(yield* run(pass))).toBe(true);
          expect(yield* Effect.promise(() => scheduledAlarm(thread))).toBeNull();
          expect(yield* snapshot).toEqual(parked);
          // Regression: https://github.com/danieljvdm/effect-agent/commit/35b5e858
          // Equivalent metadata ordering must not wake or report parked work again.
          reorderMetadata = true;
          yield* run(ensure);
          expect(yield* Effect.promise(() => scheduledAlarm(thread))).toBeNull();
          yield* evict;
          yield* run(ensure);
          expect(yield* Effect.promise(() => scheduledAlarm(thread))).toBeNull();

          // Unrelated host delivery and a redelivered alarm cannot retry the same registry.
          yield* TestClock.adjust("1 hour");
          yield* run(ThreadMutationGate.use((gate) => gate.schedule("test:host", 0)));
          expect(Exit.isSuccess(yield* run(pass))).toBe(true);
          expect(Exit.isSuccess(yield* run(pass))).toBe(true);
          expect(yield* snapshot).toEqual(parked);
          expect(yield* Effect.promise(() => readCanonical(thread))).toEqual(canonicalBefore);
          expect(errors.filter((message) => message.includes("binding"))).toHaveLength(1);
          expect(hostDrains).toBeGreaterThan(1);
          expect(yield* Effect.promise(() => scheduledAlarm(thread))).toBeNull();

          available = true;
          yield* evict;
          yield* run(ensure);
          expect(yield* Effect.promise(() => scheduledAlarm(thread))).not.toBeNull();

          // Exact-definition admission must replay the original receipt despite another
          // definition sharing its stable Agent ID. Selection is not a digest migration.
          const replay = yield* run(
            DurableAgentRuntime.use((runtime) =>
              runtime.submitRegistered(
                { definition: plannerDefinition },
                { question: "preserve the accepted contract", ref: thread },
                submitOptions(thread, thread),
              ),
            ),
          );

          expect(Exit.isSuccess(replay) && replay.value).toEqual(receipt);
          yield* TestClock.adjust("1 second");
          expect(Exit.isSuccess(yield* run(pass))).toBe(true);

          const settlement = yield* Effect.promise(() =>
            runClient(CloudflareThreadClient.use((client) => client.awaitSettlement(receipt))),
          );

          expect(settlement.submissionId).toBe(receipt.submissionId);
          expect(settlement.outcome).toBe("completed");
          expect((yield* snapshot).submission.agentDigests).toEqual(parked.submission.agentDigests);
          expect((yield* snapshot).submission.receiptId).toBe(parked.submission.receiptId);
          expect(yield* Effect.promise(() => scheduledAlarm(thread))).toBeNull();
        }).pipe(Effect.scoped, Effect.provide(TestClock.layer())),
        { signal },
      );
    }, 20_000);
  }

  it("retains exponential no-progress backoff while preserving a live claim", () =>
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
                  }),
                );
              }),
            ),
          ),
        );
      }).pipe(Effect.scoped, Effect.provide(TestClock.layer())),
    ));
});

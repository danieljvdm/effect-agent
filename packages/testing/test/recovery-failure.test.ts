import { MemorySubmissionLedgerLive } from "@effect-agent/storage-memory/memory-submission-ledger";
import { MemoryThreadStoreLive } from "@effect-agent/storage-memory/memory-thread-store";
import { NodeCrypto } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import { Cause, Deferred, Duration, Effect, Exit, Fiber, Layer, Schema, Stream } from "effect";
import { DurableAgentRuntime, DurableRuntimeConfig } from "effect-agent/durable-agent-runtime";
import { DurableRuntimeFailpoint } from "effect-agent/durable-failpoint";
import { AgentId, ThreadId } from "effect-agent/identifiers";
import { DefinitionDigests, DeploymentId, Digest, ProducerId } from "effect-agent/records";
import {
  IdempotencyKey,
  LedgerError,
  Principal,
  SubmissionLedger,
} from "effect-agent/submission-ledger";
import { ThreadStore, ThreadStoreDiagnostic, ThreadStoreError } from "effect-agent/thread-store";
import { ToolReconciler } from "effect-agent/tool-reconciler";
import { WakeScheduler } from "effect-agent/wake-scheduler";
import { TestClock } from "effect/testing";

const failingThread = ThreadId.make("a-recovery-failure");
const healthyThread = ThreadId.make("z-recovery-healthy");
const digest = Digest.make("a".repeat(64));
const definitions = DefinitionDigests.make({ agent: digest, model: digest, tools: digest });

const config = DurableRuntimeConfig.layer({
  deploymentId: DeploymentId.make("recovery-test"),
  producerId: ProducerId.make("recovery-worker"),
  recoveryTimeout: Duration.seconds(1),
});

describe("bounded recovery failure isolation", () => {
  it.effect(
    "isolates loadRecoverySnapshot faults, retains content-free causes, and propagates scan failures",
    () =>
      Effect.gen(function* () {
        class ForeignSnapshotFailure extends Schema.TaggedError<ForeignSnapshotFailure>()(
          "private_fixture_payload_tag",
          {
            operation: Schema.String,
            message: Schema.String,
            diagnostic: ThreadStoreDiagnostic,
          },
        ) {}

        const diagnostic = ThreadStoreDiagnostic.make({
          causeTag: "SchemaError",
          operation: "decode worker admission",
          decoder: "WorkerAdmission",
          issueTag: "Filter",
        });

        const failure = LedgerError.make({
          operation: "loadRecoverySnapshot",
          message: "private snapshot failure",
          cause: ForeignSnapshotFailure.make({
            operation: "private_fixture_payload_operation",
            message: "private foreign payload",
            diagnostic,
          }),
        });

        let scanFails = false;
        let mixedCause = false;

        const observedLedger = Layer.effect(
          SubmissionLedger,
          Effect.map(SubmissionLedger, (ledger) =>
            SubmissionLedger.of({
              ...ledger,
              scanNonterminal: Stream.suspend(() =>
                scanFails ? Stream.fail(failure) : ledger.scanNonterminal,
              ),
              loadRecoverySnapshot: (request) =>
                Effect.gen(function* () {
                  const snapshot = yield* ledger.loadRecoverySnapshot(request);

                  if (snapshot.submission.threadId !== failingThread) return snapshot;

                  return yield* Effect.failCause(
                    mixedCause
                      ? Cause.combine(
                          Cause.fail(failure),
                          Cause.die(
                            Object.assign(new Error("private cleanup defect"), {
                              name: "private_fixture_payload_name",
                            }),
                          ),
                        )
                      : Cause.fail(failure),
                  );
                }),
            }),
          ),
        ).pipe(Layer.provide(MemorySubmissionLedgerLive));

        const services = DurableAgentRuntime.layer.pipe(
          Layer.provideMerge(
            Layer.mergeAll(
              observedLedger,
              MemoryThreadStoreLive,
              config,
              DurableRuntimeFailpoint.layer,
              WakeScheduler.layerNoop,
              ToolReconciler.uncertain,
            ).pipe(Layer.provideMerge(NodeCrypto.layer)),
          ),
        );

        yield* Effect.gen(function* () {
          const runtime = yield* DurableAgentRuntime;
          const ledger = yield* SubmissionLedger;

          for (const threadId of [failingThread, healthyThread]) {
            yield* runtime.submit(
              { definition: { id: AgentId.make("recovery-agent"), input: Schema.String } },
              "accepted",
              {
                threadId,
                principal: Principal.make("recovery-principal"),
                idempotencyKey: IdempotencyKey.make(threadId),
                definitions,
              },
            );
          }
          const reports = yield* runtime.runRecovery();

          expect(JSON.stringify(reports)).not.toContain("private");
          expect(reports).toMatchObject({
            blocked: [
              {
                threadId: failingThread,
                failure: {
                  phase: "recovery",
                  reason: "failure",
                  errorTag: "LedgerError",
                  operation: "loadRecoverySnapshot",
                  causes: [{ errorTag: "LedgerError" }, { errorTag: "ForeignError" }],
                  diagnostic,
                },
              },
            ],
            reports: [
              {
                threadId: healthyThread,
                disposition: "deferred",
                decision: { _tag: "ApplyInput" },
              },
            ],
          });
          mixedCause = true;
          const mixedReports = yield* runtime.runRecovery();

          expect(JSON.stringify(mixedReports)).not.toContain("private");
          expect(mixedReports.blocked).toMatchObject([
            {
              failure: {
                reason: "defect",
                errorTag: "LedgerError",
                causes: [
                  { errorTag: "LedgerError" },
                  { errorTag: "ForeignError" },
                  { errorTag: "ForeignError" },
                ],
                diagnostic,
              },
            },
          ]);
          // A global scan has not identified a Thread: preserve its original typed cause.
          const accepted = yield* Stream.runCollect(ledger.scanNonterminal);

          scanFails = true;
          expect(yield* runtime.runRecovery().pipe(Effect.flip)).toBe(failure);
          scanFails = false;
          expect(yield* Stream.runCollect(ledger.scanNonterminal)).toEqual(accepted);
        }).pipe(Effect.provide(services));
      }),
  );

  for (const mode of ["failure", "defect", "timeout", "interruption"] as const) {
    it.effect(`isolates ${mode} without granting execution authority or leaking resources`, () =>
      Effect.gen(function* () {
        let armed = false;
        let active = 0;
        const reads: Array<ThreadId> = [];
        const entered = yield* Deferred.make<void>();

        const observedStore = Layer.effect(
          ThreadStore,
          Effect.map(ThreadStore, (store) =>
            ThreadStore.of({
              ...store,
              read: (request) =>
                Stream.suspend(() => {
                  if (!armed) return store.read(request);
                  reads.push(request.threadId);
                  if (request.threadId !== failingThread) return store.read(request);

                  return Stream.fromEffect(
                    Effect.acquireUseRelease(
                      Effect.sync(() => {
                        active++;
                      }),
                      () =>
                        Effect.gen(function* () {
                          yield* Deferred.succeed(entered, undefined);
                          switch (mode) {
                            case "failure":
                              return yield* ThreadStoreError.make({
                                operation: "fixture read",
                                message: "private payload must not be copied",
                              });
                            case "defect":
                              return yield* Effect.die("private defect payload must not be copied");
                            case "timeout":
                              return yield* Effect.never;
                            case "interruption":
                              return yield* Effect.interrupt;
                          }
                        }),
                      () =>
                        Effect.sync(() => {
                          active--;
                        }),
                    ),
                  );
                }),
            }),
          ),
        ).pipe(Layer.provide(MemoryThreadStoreLive));

        const services = DurableAgentRuntime.layer.pipe(
          Layer.provideMerge(
            Layer.mergeAll(
              observedStore,
              MemorySubmissionLedgerLive,
              config,
              DurableRuntimeFailpoint.layer,
              WakeScheduler.layerNoop,
              ToolReconciler.uncertain,
            ).pipe(Layer.provideMerge(NodeCrypto.layer)),
          ),
        );

        yield* Effect.gen(function* () {
          const runtime = yield* DurableAgentRuntime;

          for (const threadId of [failingThread, healthyThread]) {
            yield* runtime.submit(
              { definition: { id: AgentId.make("recovery-agent"), input: Schema.String } },
              "accepted",
              {
                threadId,
                principal: Principal.make("recovery-principal"),
                idempotencyKey: IdempotencyKey.make(threadId),
                definitions,
              },
            );
          }
          armed = true;
          const fiber = yield* Effect.forkChild(runtime.runRecovery());

          yield* Deferred.await(entered);
          if (mode === "timeout") yield* TestClock.adjust(1_000);
          const outcome = yield* Fiber.await(fiber);

          expect(active).toBe(0);

          if (mode === "interruption") {
            expect(Exit.isFailure(outcome) && Cause.hasInterrupts(outcome.cause)).toBe(true);
            expect(reads).not.toContain(healthyThread);

            return;
          }
          expect(Exit.isSuccess(outcome)).toBe(true);
          if (Exit.isFailure(outcome)) return;
          expect(outcome.value).toMatchObject({
            blocked: [
              {
                threadId: failingThread,
                failure: {
                  phase: "history",
                  reason: mode,
                  errorTag:
                    mode === "failure"
                      ? "ThreadStoreError"
                      : mode === "defect"
                        ? "Defect"
                        : "RecoveryTimeout",
                },
              },
            ],
            reports: [
              {
                threadId: healthyThread,
                disposition: "deferred",
                decision: { _tag: "ApplyInput" },
              },
            ],
          });
          expect(JSON.stringify(outcome.value)).not.toContain("private");
          reads.length = 0;
          const due = yield* runtime.runRecovery({ threadId: healthyThread });

          expect(due.reports.map((report) => report.threadId)).toEqual([healthyThread]);
          expect(reads).not.toContain(failingThread);
        }).pipe(Effect.provide(services));
      }),
    );
  }
});

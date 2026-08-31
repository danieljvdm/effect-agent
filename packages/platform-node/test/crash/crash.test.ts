import { Agent } from "@effect-agent/core";
import {
  AbortCommand,
  ClaimRequest,
  DurableAgentRuntime,
  ProducerId,
  RecoverySnapshotRequest,
  ReleaseOwnershipRequest,
  SubmissionLedger,
  modelResponseInterruptedRecordId,
  modelResponseRecordId,
  recoveryRepairRecordId,
  runIdForSubmission,
  submissionInputRecordId,
  submissionSettlementRecordId,
  toolApprovalDecisionRecordId,
  toolApprovalRequestRecordId,
  toolCallPreparedRecordId,
  toolCallResolvedRecordId,
  toolCallSettledRecordId,
  toolCallUnknownRecordId,
  toolStepSettledRecordId,
  type CanonicalRecordEnvelope,
} from "@effect-agent/thread";
import { NodeFileSystem } from "@effect/platform-node";
import { expect, layer } from "@effect/vitest";
import { Effect, Option, Schema, Stream } from "effect";
import { Prompt } from "effect/unstable/ai";

import { NodeDurableHost } from "../../src/index.ts";
import {
  BOOK_CALL_ID,
  BOOK_REF,
  CHILD_ANSWER,
  CRASH_QUESTION,
  FENCED_EXIT_CODE,
  FRESH_ANSWER,
  HOST_PRODUCER_ID,
  ITINERARY_CALL_ID,
  JOIN_QUESTION,
  STEP_REF,
  approvalDefinition,
  approvalTools,
  bookDefinition,
  bookIdempotentDefinition,
  bookIdempotentTools,
  bookTools,
  crashSubmitOptions,
  decodeThreadId,
  decodeToolCallId,
  finalParts,
  itineraryDefinition,
  makeBookToolLayer,
  makeItineraryToolLayer,
  makeScriptedModel,
  plannerDefinition,
  searchDefinition,
  searchToolLayer,
  supplierCount,
  supplierReconcilerLayer,
} from "./fixtures.ts";
import {
  CHILD_LEASE_MS,
  assertConvergence,
  childMessages,
  expectKilled,
  failureTag,
  lookupByKey,
  lookupState,
  logTags,
  readLog,
  runWorkerToExit,
  startWorker,
  touchFile,
  waitForFile,
  waitOutChildLease,
  withCrashSite,
  withHost,
  withRuntime,
  type ChildOptions,
  type CrashSite,
} from "./harness.ts";

/**
 * Process-kill crash harness (plan §Crash matrix + §4.3, durability §15). Every test spawns
 * `worker-entry.ts` as a REAL child process over a temp SQLite file, kills it at an armed
 * failpoint (`process.exit(137)` — no finalizers, no ownership drain) or with SIGKILL, then
 * restarts against the same file and asserts the required durable outcome, ending with the
 * convergence property: no accepted Submission disappears, no Submission has two terminal
 * outcomes, FIFO order holds, and stale producer epochs are fenced.
 *
 * The P5 rows add a file-backed external supplier store shared with the children: recorded Tool
 * results must exist in that store (never fabricate, durability §10) and every row's per-call
 * invocation counts are asserted exactly — at-least-once duplicates are OBSERVABLE, recovered
 * results stay at one call, and unproven outcomes stop at Unknown until the authorized
 * `resolveUnknown`/`resolveApproval` drivers decide them from a second real process.
 *
 * These tests use real clocks, real files, and real processes by necessity, so the suite runs
 * with `excludeTestServices` like the sandbox-local process tests.
 */

const decodeProducerId = Schema.decodeSync(ProducerId);

const resubmit = (thread: string, key: string) =>
  Effect.gen(function* () {
    const runtime = yield* DurableAgentRuntime;
    return yield* runtime.submit(
      { definition: plannerDefinition },
      { question: CRASH_QUESTION },
      crashSubmitOptions(thread, key),
    );
  });

const drainPlanner = (thread: string, answer: string) =>
  Effect.gen(function* () {
    const runtime = yield* DurableAgentRuntime;
    const model = yield* makeScriptedModel(() => finalParts(answer));
    const agent = Agent.withModel(plannerDefinition, model);
    return yield* runtime.processThread(agent, decodeThreadId(thread));
  });

const drainSearch = (thread: string, answer: string) =>
  Effect.gen(function* () {
    const runtime = yield* DurableAgentRuntime;
    const model = yield* makeScriptedModel(() => finalParts(answer));
    const agent = Agent.withModel(searchDefinition, model);
    return yield* runtime
      .processThread(agent, decodeThreadId(thread))
      .pipe(Effect.provide(searchToolLayer));
  });

const drainUncertainBook = (site: CrashSite, thread: string, answer: string) =>
  Effect.gen(function* () {
    const runtime = yield* DurableAgentRuntime;
    const model = yield* makeScriptedModel(() => finalParts(answer));
    const agent = Agent.withModel(bookDefinition, model);
    return yield* runtime
      .processThread(agent, decodeThreadId(thread))
      .pipe(Effect.provide(makeBookToolLayer(site.supplier, bookTools)));
  });

const drainIdempotentBook = (site: CrashSite, thread: string, answer: string) =>
  Effect.gen(function* () {
    const runtime = yield* DurableAgentRuntime;
    const model = yield* makeScriptedModel(() => finalParts(answer));
    const agent = Agent.withModel(bookIdempotentDefinition, model);
    return yield* runtime
      .processThread(agent, decodeThreadId(thread))
      .pipe(Effect.provide(makeBookToolLayer(site.supplier, bookIdempotentTools)));
  });

const drainApprovalBook = (site: CrashSite, thread: string, answer: string) =>
  Effect.gen(function* () {
    const runtime = yield* DurableAgentRuntime;
    const model = yield* makeScriptedModel(() => finalParts(answer));
    const agent = Agent.withModel(approvalDefinition, model);
    return yield* runtime
      .processThread(agent, decodeThreadId(thread))
      .pipe(Effect.provide(makeBookToolLayer(site.supplier, approvalTools)));
  });

const drainItinerary = (site: CrashSite, thread: string, answer: string) =>
  Effect.gen(function* () {
    const runtime = yield* DurableAgentRuntime;
    const model = yield* makeScriptedModel(() => finalParts(answer));
    const agent = Agent.withModel(itineraryDefinition, model);
    return yield* runtime
      .processThread(agent, decodeThreadId(thread))
      .pipe(Effect.provide(makeItineraryToolLayer(site.supplier)));
  });

/** Spawn a second-process resolution driver against the shared file and require success. */
const runResolver = (options: ChildOptions) =>
  Effect.gen(function* () {
    const result = yield* runWorkerToExit(options);
    expect(result.exit.code, `resolver stderr: ${result.stderr}`).toBe(0);
    const resolved = childMessages(result.stdout).find((message) => message.kind === "resolved");
    expect(resolved).toBeDefined();
    return resolved;
  });

/** Occurrences of `needle` inside the committed model-response messages (coverage evidence). */
const responseOccurrences = (
  records: ReadonlyArray<CanonicalRecordEnvelope>,
  needle: string,
): number =>
  records
    .filter((envelope) => envelope.record.payload._tag === "ModelResponseRecorded")
    .map((envelope) => JSON.stringify(envelope.record.payload).split(needle).length - 1)
    .reduce((sum, count) => sum + count, 0);

/** The supplier-store honesty claims of one crash row (plan §4.3, durability §10). */
layer(NodeFileSystem.layer, { excludeTestServices: true })(
  "DN crash matrix (real process kills)",
  (it) => {
    it.effect(
      "kill at submit:after-admit: recovery completes materialization; the same key resumes",
      () =>
        withCrashSite((site) =>
          Effect.gen(function* () {
            const thread = "thread-kill-admit";
            const key = "kill-admit-1";
            const result = yield* runWorkerToExit({
              db: site.db,
              scenario: "submit",
              thread,
              key,
              killAt: "submit:after-admit",
            });
            expectKilled(result);

            yield* withHost(
              site.db,
              Effect.gen(function* () {
                const host = yield* NodeDurableHost;
                const snapshot = yield* lookupByKey(thread, key);
                const report = host.startupRecovery.find(
                  (entry) => entry.submissionId === snapshot.submissionId,
                );
                expect(report?.decision._tag).toBe("CompleteMaterialization");
                expect(report?.disposition).toBe("repaired");
                expect(yield* lookupState(snapshot.submissionId)).toBe("ready");
                const records = yield* readLog(thread);
                expect(records[0]?.record.recordId).toBe(`thread-created:${thread}`);

                // Reattachment: the identical payload under the same key resumes the Receipt.
                const receipt = yield* resubmit(thread, key);
                expect(receipt.receiptId).toBe(snapshot.receiptId);
                expect(receipt.submissionId).toBe(snapshot.submissionId);
                expect(Number(receipt.queueSequence)).toBe(Number(snapshot.queueSequence));

                // The same key with DIFFERENT content is refused, never silently replaced.
                const runtime = yield* DurableAgentRuntime;
                const conflict = yield* Effect.exit(
                  runtime.submit(
                    { definition: plannerDefinition },
                    { question: "a conflicting question" },
                    crashSubmitOptions(thread, key),
                  ),
                );
                expect(failureTag(conflict)).toBe("AdmissionConflict");

                const settlements = yield* drainPlanner(thread, CHILD_ANSWER);
                expect(settlements).toHaveLength(1);
                expect(settlements[0]?.outcome).toBe("completed");
                const settlement = yield* host.awaitSettlement(receipt);
                expect(settlement.outcome).toBe("completed");
                yield* assertConvergence(thread, [snapshot.submissionId]);
              }),
            );
          }),
        ),
      30_000,
    );

    it.effect(
      "kill at ledger:mark-ready:after: the same key returns the original Receipt",
      () =>
        withCrashSite((site) =>
          Effect.gen(function* () {
            const thread = "thread-kill-ready";
            const key = "kill-ready-1";
            const result = yield* runWorkerToExit({
              db: site.db,
              scenario: "submit",
              thread,
              key,
              killAtStorage: "ledger:mark-ready:after",
            });
            expectKilled(result);

            // Client-only restart (no recovery pass): the replay alone returns the Receipt.
            const submissionId = yield* withRuntime(
              site.db,
              Effect.gen(function* () {
                const snapshot = yield* lookupByKey(thread, key);
                expect(snapshot.state).toBe("ready");
                const receipt = yield* resubmit(thread, key);
                expect(receipt.receiptId).toBe(snapshot.receiptId);
                expect(receipt.submissionId).toBe(snapshot.submissionId);
                return snapshot.submissionId;
              }),
            );

            yield* withHost(
              site.db,
              Effect.gen(function* () {
                const host = yield* NodeDurableHost;
                const report = host.startupRecovery.find(
                  (entry) => entry.submissionId === submissionId,
                );
                expect(report?.decision._tag).toBe("ApplyInput");
                expect(report?.disposition).toBe("repaired");
                const settlements = yield* drainPlanner(thread, CHILD_ANSWER);
                expect(settlements[0]?.outcome).toBe("completed");
                const inputRecords = (yield* readLog(thread)).filter(
                  (envelope) => envelope.record.recordId === submissionInputRecordId(submissionId),
                );
                expect(inputRecords).toHaveLength(1);
                yield* assertConvergence(thread, [submissionId]);
              }),
            );
          }),
        ),
      30_000,
    );

    it.effect(
      "kill at claim:after-claim: recovery re-applies the input exactly once",
      () =>
        withCrashSite((site) =>
          Effect.gen(function* () {
            const thread = "thread-kill-claim";
            const key = "kill-claim-1";
            const result = yield* runWorkerToExit({
              db: site.db,
              scenario: "run",
              thread,
              key,
              killAt: "claim:after-claim",
              leaseMillis: CHILD_LEASE_MS,
            });
            expectKilled(result);
            yield* waitOutChildLease;

            yield* withHost(
              site.db,
              Effect.gen(function* () {
                const host = yield* NodeDurableHost;
                const ledger = yield* SubmissionLedger;
                const snapshot = yield* lookupByKey(thread, key);
                const report = host.startupRecovery.find(
                  (entry) => entry.submissionId === snapshot.submissionId,
                );
                expect(report?.decision._tag).toBe("ApplyInput");
                expect(report?.disposition).toBe("repaired");
                const recovered = yield* ledger.loadRecoverySnapshot(
                  RecoverySnapshotRequest.make({ submissionId: snapshot.submissionId }),
                );
                expect(recovered.inputApplied?.recordId).toBe(
                  submissionInputRecordId(snapshot.submissionId),
                );

                const settlements = yield* drainPlanner(thread, CHILD_ANSWER);
                expect(settlements[0]?.outcome).toBe("completed");
                const inputRecords = (yield* readLog(thread)).filter(
                  (envelope) =>
                    envelope.record.recordId === submissionInputRecordId(snapshot.submissionId),
                );
                expect(inputRecords).toHaveLength(1);
                yield* assertConvergence(thread, [snapshot.submissionId]);
              }),
            );
          }),
        ),
      30_000,
    );

    it.effect(
      "RUN-030: process loss on either side of Run-start append preserves one canonical clock",
      () =>
        Effect.forEach(
          ["run:before-start-append", "run:after-start-append"] as const,
          (killAt) =>
            withCrashSite((site) =>
              Effect.gen(function* () {
                const thread = `thread-${killAt}`;
                const key = "run-clock";
                expectKilled(
                  yield* runWorkerToExit({
                    db: site.db,
                    scenario: "run",
                    thread,
                    key,
                    killAt,
                    leaseMillis: CHILD_LEASE_MS,
                  }),
                );
                yield* waitOutChildLease;
                yield* withHost(
                  site.db,
                  Effect.gen(function* () {
                    const snapshot = yield* lookupByKey(thread, key);
                    const before = (yield* readLog(thread)).filter(
                      ({ record }) => record.payload._tag === "RunStarted",
                    );
                    expect(before).toHaveLength(killAt === "run:after-start-append" ? 1 : 0);
                    expect(
                      (yield* drainPlanner(thread, CHILD_ANSWER)).map((entry) => entry.outcome),
                    ).toEqual(["completed"]);
                    const after = (yield* readLog(thread)).filter(
                      ({ record }) => record.payload._tag === "RunStarted",
                    );
                    expect(after).toHaveLength(1);
                    if (before.length > 0) expect(after).toEqual(before);
                    yield* assertConvergence(thread, [snapshot.submissionId]);
                  }),
                );
              }),
            ),
          { discard: true },
        ),
      30_000,
    );

    it.effect(
      "kill at input:after-canonical-append: the marker is repaired and FIFO holds for the queued Submission",
      () =>
        withCrashSite((site) =>
          Effect.gen(function* () {
            const thread = "thread-kill-input";
            const key = "kill-input";
            const result = yield* runWorkerToExit({
              db: site.db,
              scenario: "run-two",
              thread,
              key,
              killAt: "input:after-canonical-append",
              leaseMillis: CHILD_LEASE_MS,
            });
            expectKilled(result);
            yield* waitOutChildLease;

            yield* withHost(
              site.db,
              Effect.gen(function* () {
                const host = yield* NodeDurableHost;
                const ledger = yield* SubmissionLedger;
                const head = yield* lookupByKey(thread, `${key}-1`);
                const queued = yield* lookupByKey(thread, `${key}-2`);

                // The killed head's canonical input exists; only its ledger marker is repaired.
                const headReport = host.startupRecovery.find(
                  (entry) => entry.submissionId === head.submissionId,
                );
                expect(headReport?.decision._tag).toBe("RepairInputMarker");
                expect(headReport?.disposition).toBe("repaired");
                // The queued Submission stays deferred: it is not claimable behind the head.
                const queuedReport = host.startupRecovery.find(
                  (entry) => entry.submissionId === queued.submissionId,
                );
                expect(queuedReport?.disposition).toBe("deferred");

                // FIFO: a direct claim grants ONLY the unsettled lane head.
                const claimed = yield* ledger.claim(
                  ClaimRequest.make({
                    threadId: decodeThreadId(thread),
                    producerId: decodeProducerId(HOST_PRODUCER_ID),
                  }),
                );
                expect(Option.isSome(claimed)).toBe(true);
                if (Option.isNone(claimed)) throw new Error("Expected a claimable lane head");
                expect(claimed.value.submissionId).toBe(head.submissionId);
                yield* ledger.releaseOwnership(
                  ReleaseOwnershipRequest.make({
                    submissionId: claimed.value.submissionId,
                    ownershipToken: claimed.value.ownershipToken,
                  }),
                );

                const settlements = yield* drainPlanner(thread, CHILD_ANSWER);
                // P5 (plan §2.5): the queued Submission joins the resumed head Run at its
                // first Turn seam and settles WITH it — one head settlement, both settled in
                // admitted FIFO order (assertConvergence pins the canonical ordering).
                expect(settlements.map((settlement) => settlement.submissionId)).toEqual([
                  head.submissionId,
                ]);
                const records = yield* readLog(thread);
                expect(
                  records.filter(
                    (envelope) =>
                      envelope.record.recordId === submissionInputRecordId(head.submissionId),
                  ),
                ).toHaveLength(1);
                yield* assertConvergence(thread, [head.submissionId, queued.submissionId]);
              }),
            );
          }),
        ),
      30_000,
    );

    it.effect(
      "SIGKILL mid model Turn: a new Attempt resumes from the committed Turn boundary and the client reattaches",
      () =>
        withCrashSite((site) =>
          Effect.gen(function* () {
            const thread = "thread-kill-midturn";
            const key = "kill-midturn-1";
            const exit = yield* Effect.scoped(
              Effect.gen(function* () {
                const handle = yield* startWorker({
                  db: site.db,
                  scenario: "run-blocked",
                  thread,
                  key,
                  leaseMillis: CHILD_LEASE_MS,
                  markerFile: site.marker,
                  releaseFile: site.release,
                });
                // Turn 1 (tool call) is canonical once the marker exists; Turn 2 is mid-stream.
                yield* waitForFile(site.marker);
                handle.kill();
                return yield* handle.awaitExit;
              }),
            );
            expect(exit.signal).toBe("SIGKILL");
            yield* waitOutChildLease;

            yield* withHost(
              site.db,
              Effect.gen(function* () {
                const host = yield* NodeDurableHost;
                const runtime = yield* DurableAgentRuntime;
                const snapshot = yield* lookupByKey(thread, key);
                const runId = runIdForSubmission(snapshot.submissionId);

                // Exactly Turn 1 survived; the incomplete Turn 2 left no canonical trace.
                const committed = yield* readLog(thread);
                expect(logTags(committed)).toEqual([
                  "ThreadCreated",
                  "UserInputRecorded",
                  "RunStarted",
                  "ModelResponseRecorded",
                  "ToolCallSettled",
                ]);
                const report = host.startupRecovery.find(
                  (entry) => entry.submissionId === snapshot.submissionId,
                );
                expect(report?.decision._tag).toBe("ResumeFromTurnBoundary");
                expect(report?.disposition).toBe("deferred");

                // Client restart: same-key resubmission reattaches to the original Receipt.
                const receipt = yield* resubmit(thread, key);
                expect(receipt.receiptId).toBe(snapshot.receiptId);
                expect(receipt.submissionId).toBe(snapshot.submissionId);

                // Observation resumes from a stored offset instead of replaying from scratch.
                const observedHead = yield* Stream.runCollect(
                  Stream.take(runtime.observe(receipt), 2),
                );
                expect(observedHead).toHaveLength(2);
                const storedOffset = observedHead[0]?.offset;
                if (storedOffset === undefined) throw new Error("Expected a stored offset");
                const observedResume = yield* Stream.runCollect(
                  Stream.take(runtime.observe(receipt, { after: storedOffset }), 1),
                );
                expect(Number(observedResume[0]?.sequence)).toBe(Number(observedHead[1]?.sequence));

                const settlements = yield* drainSearch(thread, FRESH_ANSWER);
                expect(settlements).toHaveLength(1);
                expect(settlements[0]?.outcome).toBe("completed");

                const records = yield* readLog(thread);
                expect(
                  records.filter(
                    (envelope) => envelope.record.recordId === modelResponseRecordId(runId, 1),
                  ),
                ).toHaveLength(1);
                expect(records.map((envelope) => envelope.record.recordId)).toContain(
                  modelResponseRecordId(runId, 2),
                );
                const settlement = yield* host.awaitSettlement(receipt);
                expect(settlement.outcome).toBe("completed");
                yield* assertConvergence(thread, [snapshot.submissionId]);
              }),
            );
          }),
        ),
      30_000,
    );

    it.effect(
      "a stale Attempt in a live process is fenced out of canonical history",
      () =>
        withCrashSite((site) =>
          Effect.gen(function* () {
            const thread = "thread-fenced";
            const key = "fenced-1";
            yield* Effect.scoped(
              Effect.gen(function* () {
                const handle = yield* startWorker({
                  db: site.db,
                  scenario: "run-blocked",
                  thread,
                  key,
                  leaseMillis: CHILD_LEASE_MS,
                  markerFile: site.marker,
                  releaseFile: site.release,
                });
                yield* waitForFile(site.marker);
                // The blocked child's short lease lapses while it is still alive mid-Turn.
                yield* waitOutChildLease;

                // A restarted host claims a HIGHER epoch and completes the accepted work.
                const submissionId = yield* withHost(
                  site.db,
                  Effect.gen(function* () {
                    const snapshot = yield* lookupByKey(thread, key);
                    const settlements = yield* drainSearch(thread, FRESH_ANSWER);
                    expect(settlements).toHaveLength(1);
                    expect(settlements[0]?.outcome).toBe("completed");
                    return snapshot.submissionId;
                  }),
                );

                // Unblock the stale child: its Turn commit must be fenced, never committed.
                yield* touchFile(site.release);
                const exit = yield* handle.awaitExit;
                expect(exit.code).toBe(FENCED_EXIT_CODE);
                const failure = childMessages(handle.stdoutText()).find(
                  (message) => message.kind === "worker-failure",
                );
                expect(["FenceRejected", "OwnershipLost"]).toContain(
                  failure?.kind === "worker-failure" && failure.tag,
                );

                yield* withRuntime(
                  site.db,
                  Effect.gen(function* () {
                    const records = yield* readLog(thread);
                    const runId = runIdForSubmission(submissionId);
                    const turnTwo = records.filter(
                      (envelope) => envelope.record.recordId === modelResponseRecordId(runId, 2),
                    );
                    expect(turnTwo).toHaveLength(1);
                    const payload = turnTwo[0]?.record.payload;
                    expect(payload?._tag).toBe("ModelResponseRecorded");
                    if (payload?._tag === "ModelResponseRecorded") {
                      const messages = yield* Schema.decodeUnknownEffect(Prompt.Prompt)(
                        payload.messages,
                      );
                      const text = messages.content
                        .filter((message) => message.role === "assistant")
                        .flatMap((message) => message.content)
                        .filter((part) => part.type === "text")
                        .map((part) => part.text)
                        .join("");
                      // The surviving Turn 2 is the fresh Attempt's, not the fenced stale one.
                      expect(text).toBe(FRESH_ANSWER);
                    }
                    yield* assertConvergence(thread, [submissionId]);
                  }),
                );
              }),
            );
          }),
        ),
      30_000,
    );

    it.effect(
      "kill before settlement reservation: canonical completion avoids another model call and settles once",
      () =>
        withCrashSite((site) =>
          Effect.gen(function* () {
            const thread = "thread-kill-prereserve";
            const key = "kill-prereserve-1";
            const result = yield* runWorkerToExit({
              db: site.db,
              scenario: "run",
              thread,
              key,
              killAtStorage: "ledger:reserve-settlement:before",
              leaseMillis: CHILD_LEASE_MS,
            });
            expectKilled(result);
            yield* waitOutChildLease;

            yield* withHost(
              site.db,
              Effect.gen(function* () {
                const host = yield* NodeDurableHost;
                const snapshot = yield* lookupByKey(thread, key);
                const report = host.startupRecovery.find(
                  (entry) => entry.submissionId === snapshot.submissionId,
                );
                expect(report?.decision._tag).toBe("ResumeFromTurnBoundary");
                expect(report?.disposition).toBe("deferred");

                const settlements = yield* drainPlanner(thread, FRESH_ANSWER);
                expect(settlements).toHaveLength(1);
                expect(settlements[0]?.outcome).toBe("completed");

                // The response and Run completion committed atomically before
                // reservation, so recovery terminalizes without another model call.
                const records = yield* readLog(thread);
                expect(
                  logTags(records).filter((tag) => tag === "ModelResponseRecorded"),
                ).toHaveLength(1);
                expect(logTags(records).filter((tag) => tag === "RunCompleted")).toHaveLength(1);
                expect(logTags(records).filter((tag) => tag === "SubmissionSettled")).toHaveLength(
                  1,
                );
                yield* assertConvergence(thread, [snapshot.submissionId]);
              }),
            );
          }),
        ),
      30_000,
    );

    it.effect(
      "kill at terminalize:after-reserve: recovery appends the EXACT reserved record",
      () =>
        withCrashSite((site) =>
          Effect.gen(function* () {
            const thread = "thread-kill-reserved";
            const key = "kill-reserved-1";
            const result = yield* runWorkerToExit({
              db: site.db,
              scenario: "run",
              thread,
              key,
              killAt: "terminalize:after-reserve",
              leaseMillis: CHILD_LEASE_MS,
            });
            expectKilled(result);
            yield* waitOutChildLease;

            yield* withHost(
              site.db,
              Effect.gen(function* () {
                const host = yield* NodeDurableHost;
                const ledger = yield* SubmissionLedger;
                const snapshot = yield* lookupByKey(thread, key);
                const report = host.startupRecovery.find(
                  (entry) => entry.submissionId === snapshot.submissionId,
                );
                expect(report?.decision._tag).toBe("AppendReservedSettlement");
                expect(report?.disposition).toBe("repaired");
                expect(yield* lookupState(snapshot.submissionId)).toBe("settled");

                // The appended canonical settlement IS the reserved record, byte for byte.
                const recovered = yield* ledger.loadRecoverySnapshot(
                  RecoverySnapshotRequest.make({ submissionId: snapshot.submissionId }),
                );
                const records = yield* readLog(thread);
                const settled = records.filter(
                  (envelope) => envelope.record.payload._tag === "SubmissionSettled",
                );
                expect(settled).toHaveLength(1);
                expect(settled[0]?.record).toEqual(recovered.reservation?.record);
                expect(records.map((envelope) => envelope.record.recordId)).toContain(
                  recoveryRepairRecordId(snapshot.submissionId, "AppendReservedSettlement"),
                );

                // awaitSettlement after restart returns the recorded Settlement.
                const receipt = yield* resubmit(thread, key);
                const settlement = yield* host.awaitSettlement(receipt);
                expect(settlement.outcome).toBe("completed");
                expect(settlement.receiptId).toBe(snapshot.receiptId);
                yield* assertConvergence(thread, [snapshot.submissionId]);
              }),
            );
          }),
        ),
      30_000,
    );

    it.effect(
      "kill at terminalize:after-canonical-append: the ledger is finalized from history, the record never rewritten",
      () =>
        withCrashSite((site) =>
          Effect.gen(function* () {
            const thread = "thread-kill-finalize";
            const key = "kill-finalize-1";
            const result = yield* runWorkerToExit({
              db: site.db,
              scenario: "run",
              thread,
              key,
              killAt: "terminalize:after-canonical-append",
              leaseMillis: CHILD_LEASE_MS,
            });
            expectKilled(result);

            // Before any recovery: the canonical outcome exists, the ledger row is nonterminal.
            const before = yield* withRuntime(
              site.db,
              Effect.gen(function* () {
                const snapshot = yield* lookupByKey(thread, key);
                expect(snapshot.state).not.toBe("settled");
                const records = yield* readLog(thread);
                const settled = records.filter(
                  (envelope) => envelope.record.payload._tag === "SubmissionSettled",
                );
                expect(settled).toHaveLength(1);
                const envelope = settled[0];
                if (envelope === undefined) throw new Error("Expected a settled record");
                return { submissionId: snapshot.submissionId, envelope };
              }),
            );

            yield* withHost(
              site.db,
              Effect.gen(function* () {
                const host = yield* NodeDurableHost;
                const report = host.startupRecovery.find(
                  (entry) => entry.submissionId === before.submissionId,
                );
                expect(report?.decision._tag).toBe("FinalizeLedgerFromHistory");
                expect(report?.disposition).toBe("repaired");
                expect(yield* lookupState(before.submissionId)).toBe("settled");

                const records = yield* readLog(thread);
                const settled = records.filter(
                  (envelope) => envelope.record.payload._tag === "SubmissionSettled",
                );
                expect(settled).toHaveLength(1);
                expect(settled[0]).toEqual(before.envelope);

                const receipt = yield* resubmit(thread, key);
                const settlement = yield* host.awaitSettlement(receipt);
                expect(settlement.outcome).toBe("completed");
                yield* assertConvergence(thread, [before.submissionId]);
              }),
            );
          }),
        ),
      30_000,
    );

    it.effect(
      "kill at abort:after-intent: ready work settles aborted without an Attempt",
      () =>
        withCrashSite((site) =>
          Effect.gen(function* () {
            const thread = "thread-abort-ready";
            const key = "abort-ready-1";
            const result = yield* runWorkerToExit({
              db: site.db,
              scenario: "abort-ready",
              thread,
              key,
              killAt: "abort:after-intent",
            });
            expectKilled(result);

            yield* withHost(
              site.db,
              Effect.gen(function* () {
                const host = yield* NodeDurableHost;
                const runtime = yield* DurableAgentRuntime;
                const snapshot = yield* lookupByKey(thread, key);
                const report = host.startupRecovery.find(
                  (entry) => entry.submissionId === snapshot.submissionId,
                );
                expect(report?.decision._tag).toBe("SettleAborted");
                expect(report?.disposition).toBe("repaired");

                const receipt = yield* resubmit(thread, key);
                const settlement = yield* host.awaitSettlement(receipt);
                expect(settlement.outcome).toBe("aborted");

                const records = yield* readLog(thread);
                const tags = logTags(records);
                expect(tags.indexOf("AbortRequested")).toBeGreaterThanOrEqual(0);
                expect(tags.indexOf("AbortRequested")).toBeLessThan(
                  tags.indexOf("SubmissionSettled"),
                );
                // Never claimed: no model ran, no canonical input was applied.
                expect(tags).not.toContain("ModelResponseRecorded");
                expect(tags).not.toContain("UserInputRecorded");
                expect(records.map((envelope) => envelope.record.recordId)).toContain(
                  recoveryRepairRecordId(snapshot.submissionId, "SettleAborted"),
                );

                // A settled Submission can never be aborted into a different outcome (DUR-012).
                const conflict = yield* Effect.exit(
                  runtime.abort(
                    AbortCommand.make({
                      submissionId: snapshot.submissionId,
                      author: "operator",
                      reason: "second abort",
                    }),
                  ),
                );
                expect(failureTag(conflict)).toBe("SettlementConflict");
                yield* assertConvergence(thread, [snapshot.submissionId]);
              }),
            );
          }),
        ),
      30_000,
    );

    it.effect(
      "kill at abort:after-intent on a QUEUED submission: the aborted non-head settles without waiting for the head",
      () =>
        withCrashSite((site) =>
          Effect.gen(function* () {
            // P7 §7(c): settlement order of never-run work is not execution order — restart
            // recovery settles the aborted, never-claimed, non-head `ready` Submission
            // immediately, while the head is still unsettled (DUR-004 bounds execution;
            // DUR-012 permits settling inactive accepted work without an Attempt).
            const thread = "thread-abort-queued";
            const key = "abort-queued-1";
            const result = yield* runWorkerToExit({
              db: site.db,
              scenario: "abort-queued",
              thread,
              key,
              killAt: "abort:after-intent",
            });
            expectKilled(result);

            yield* withHost(
              site.db,
              Effect.gen(function* () {
                const host = yield* NodeDurableHost;
                const runtime = yield* DurableAgentRuntime;
                const head = yield* lookupByKey(thread, `${key}-head`);
                const queued = yield* lookupByKey(thread, `${key}-queued`);
                const report = host.startupRecovery.find(
                  (entry) => entry.submissionId === queued.submissionId,
                );
                expect(report?.decision._tag).toBe("SettleAborted");
                expect(report?.disposition).toBe("repaired");

                // The aborted non-head is settled while the head is still nonterminal.
                expect(queued.state).toBe("settled");
                expect(queued.settledOutcome).toBe("aborted");
                expect(head.state).not.toBe("settled");

                const midRecords = yield* readLog(thread);
                const midIds = midRecords.map((envelope) => envelope.record.recordId);
                // Never claimed: no canonical input, no model call; abort precedes settlement.
                expect(midIds).not.toContain(submissionInputRecordId(queued.submissionId));
                expect(midIds).toContain(
                  recoveryRepairRecordId(queued.submissionId, "SettleAborted"),
                );

                // The head then completes normally behind the already-settled non-head.
                const settlements = yield* drainPlanner(thread, '{"answer":"post-abort"}');
                expect(settlements.map((settlement) => settlement.submissionId)).toEqual([
                  head.submissionId,
                ]);
                expect(settlements[0]?.outcome).toBe("completed");

                const records = yield* readLog(thread);
                const recordIds = records.map((envelope) => envelope.record.recordId);
                expect(
                  recordIds.indexOf(submissionSettlementRecordId(queued.submissionId)),
                ).toBeLessThan(recordIds.indexOf(submissionSettlementRecordId(head.submissionId)));

                // A settled Submission can never be aborted into a different outcome (DUR-012).
                const conflict = yield* Effect.exit(
                  runtime.abort(
                    AbortCommand.make({
                      submissionId: queued.submissionId,
                      author: "operator",
                      reason: "second abort",
                    }),
                  ),
                );
                expect(failureTag(conflict)).toBe("SettlementConflict");
                yield* assertConvergence(thread, [head.submissionId, queued.submissionId]);
              }),
            );
          }),
        ),
      30_000,
    );

    it.effect(
      "abort of an active Run in another process: canonical AbortRequested precedes interruption",
      () =>
        withCrashSite((site) =>
          Effect.gen(function* () {
            const thread = "thread-abort-active";
            const key = "abort-active-1";
            yield* Effect.scoped(
              Effect.gen(function* () {
                const handle = yield* startWorker({
                  db: site.db,
                  scenario: "abort-active",
                  thread,
                  key,
                  markerFile: site.marker,
                });
                // The child's Attempt is live and blocked inside its first model call.
                yield* waitForFile(site.marker);

                // Abort through the SHARED durable ledger from this "operator" process.
                yield* withRuntime(
                  site.db,
                  Effect.gen(function* () {
                    const runtime = yield* DurableAgentRuntime;
                    const snapshot = yield* lookupByKey(thread, key);
                    yield* runtime.abort(
                      AbortCommand.make({
                        submissionId: snapshot.submissionId,
                        author: "operator",
                        reason: "stop the run",
                      }),
                    );
                  }),
                );

                const exit = yield* handle.awaitExit;
                expect(exit.code, `stderr: ${handle.stderrText()}`).toBe(0);
                const settled = childMessages(handle.stdoutText()).find(
                  (message) => message.kind === "settlements",
                );
                expect(settled?.kind === "settlements" && settled.settlements.length).toBe(1);
                if (settled?.kind === "settlements") {
                  expect(settled.settlements[0]?.outcome).toBe("aborted");
                }
              }),
            );

            yield* withHost(
              site.db,
              Effect.gen(function* () {
                const host = yield* NodeDurableHost;
                const runtime = yield* DurableAgentRuntime;
                const snapshot = yield* lookupByKey(thread, key);
                expect(snapshot.state).toBe("settled");

                const records = yield* readLog(thread);
                const tags = logTags(records);
                // Durable §13: the abort became canonical BEFORE the Run fiber died — and the
                // interrupted model produced no committed Turn at all.
                expect(tags.indexOf("AbortRequested")).toBeGreaterThanOrEqual(0);
                expect(tags.indexOf("AbortRequested")).toBeLessThan(
                  tags.indexOf("SubmissionSettled"),
                );
                expect(tags).not.toContain("ModelResponseRecorded");
                expect(tags).toContain("UserInputRecorded");

                const receipt = yield* resubmit(thread, key);
                const settlement = yield* host.awaitSettlement(receipt);
                expect(settlement.outcome).toBe("aborted");
                const conflict = yield* Effect.exit(
                  runtime.abort(
                    AbortCommand.make({
                      submissionId: snapshot.submissionId,
                      author: "operator",
                      reason: "stop the run",
                    }),
                  ),
                );
                expect(failureTag(conflict)).toBe("SettlementConflict");
                yield* assertConvergence(thread, [snapshot.submissionId]);
              }),
            );
          }),
        ),
      30_000,
    );

    // ------------------------------------------------------------------------------------------
    // P5 rows (plan §4.3): durable ordinary Tools, Durable Steps, approval suspension, joining.
    // ------------------------------------------------------------------------------------------

    it.effect(
      "kill at turn:after-response-append: the declared batch resumes without model re-invocation",
      () =>
        withCrashSite((site) =>
          Effect.gen(function* () {
            const thread = "thread-kill-response";
            const key = "kill-response-1";
            const result = yield* runWorkerToExit({
              db: site.db,
              scenario: "run-uncertain",
              thread,
              key,
              killAt: "turn:after-response-append",
              leaseMillis: CHILD_LEASE_MS,
              supplierDir: site.supplier,
            });
            expectKilled(result);
            yield* waitOutChildLease;

            yield* withHost(
              site.db,
              Effect.gen(function* () {
                const host = yield* NodeDurableHost;
                const snapshot = yield* lookupByKey(thread, key);
                const runId = runIdForSubmission(snapshot.submissionId);

                // The provably-safe window (durability §15): the response is canonical, nothing
                // is prepared, and the external supplier was never called.
                const committed = yield* readLog(thread);
                expect(logTags(committed)).toEqual([
                  "ThreadCreated",
                  "UserInputRecorded",
                  "RunStarted",
                  "ModelResponseRecorded",
                ]);
                expect(supplierCount(site.supplier, "book", BOOK_REF)).toBe(0);
                const report = host.startupRecovery.find(
                  (entry) => entry.submissionId === snapshot.submissionId,
                );
                expect(report?.decision._tag).toBe("ResumePendingToolBatch");
                expect(report?.disposition).toBe("deferred");

                const settlements = yield* drainUncertainBook(site, thread, FRESH_ANSWER);
                expect(settlements).toHaveLength(1);
                expect(settlements[0]?.outcome).toBe("completed");
                expect(supplierCount(site.supplier, "book", BOOK_REF)).toBe(1);

                // No model re-invocation for the declared Turn: exactly one ModelResponseRecorded
                // for Turn 1 and no interruption audit — the resumed batch replayed the canonical
                // declaration instead of asking the model again.
                const records = yield* readLog(thread);
                const ids = records.map((envelope) => envelope.record.recordId);
                expect(
                  records.filter(
                    (envelope) => envelope.record.recordId === modelResponseRecordId(runId, 1),
                  ),
                ).toHaveLength(1);
                expect(
                  logTags(records).filter((tag) => tag === "ModelResponseRecorded"),
                ).toHaveLength(2);
                expect(ids).toContain(
                  toolCallPreparedRecordId(runId, 1, decodeToolCallId(BOOK_CALL_ID)),
                );
                expect(ids).toContain(
                  toolCallSettledRecordId(runId, 1, decodeToolCallId(BOOK_CALL_ID)),
                );
                expect(ids).not.toContain(modelResponseInterruptedRecordId(runId, 1));
                yield* assertConvergence(thread, [snapshot.submissionId], {
                  site,
                  counts: { [`book:${BOOK_REF}`]: 1 },
                });
              }),
            );
          }),
        ),
      30_000,
    );

    it.effect(
      "kill at tools:after-prepared-append with NeverStarted proof: the deferred resume executes exactly once",
      () =>
        withCrashSite((site) =>
          Effect.gen(function* () {
            const thread = "thread-kill-prepared-proof";
            const key = "kill-prepared-proof-1";
            const result = yield* runWorkerToExit({
              db: site.db,
              scenario: "run-uncertain",
              thread,
              key,
              killAt: "tools:after-prepared-append",
              leaseMillis: CHILD_LEASE_MS,
              supplierDir: site.supplier,
            });
            expectKilled(result);
            yield* waitOutChildLease;

            yield* withHost(
              site.db,
              Effect.gen(function* () {
                const host = yield* NodeDurableHost;
                const snapshot = yield* lookupByKey(thread, key);
                const runId = runIdForSubmission(snapshot.submissionId);

                // The empty supplier store IS the marker: the handler provably never started.
                expect(supplierCount(site.supplier, "book", BOOK_REF)).toBe(0);
                const report = host.startupRecovery.find(
                  (entry) => entry.submissionId === snapshot.submissionId,
                );
                expect(report?.decision._tag).toBe("MarkUnknown");
                expect(report?.disposition).toBe("deferred");
                expect(yield* lookupState(snapshot.submissionId)).not.toBe("unknown");

                const settlements = yield* drainUncertainBook(site, thread, FRESH_ANSWER);
                expect(settlements).toHaveLength(1);
                expect(settlements[0]?.outcome).toBe("completed");
                expect(supplierCount(site.supplier, "book", BOOK_REF)).toBe(1);

                // A point-in-time proof records nothing: no Unknown Outcome, no resolution audit,
                // exactly one canonical settled result.
                const records = yield* readLog(thread);
                const tags = logTags(records);
                expect(tags).not.toContain("ToolCallUnknown");
                expect(tags).not.toContain("ToolCallResolved");
                expect(
                  records.filter(
                    (envelope) =>
                      envelope.record.recordId ===
                      toolCallSettledRecordId(runId, 1, decodeToolCallId(BOOK_CALL_ID)),
                  ),
                ).toHaveLength(1);
                yield* assertConvergence(thread, [snapshot.submissionId], {
                  site,
                  counts: { [`book:${BOOK_REF}`]: 1 },
                });
              }),
              { toolReconciler: supplierReconcilerLayer(site.supplier) },
            );
          }),
        ),
      30_000,
    );

    it.effect(
      "kill at tools:after-prepared-append under the default reconciler: Unknown blocks the lane until resolveUnknown from a second process",
      () =>
        withCrashSite((site) =>
          Effect.gen(function* () {
            const thread = "thread-kill-prepared-unknown";
            const key = "kill-prepared-unknown-1";
            const result = yield* runWorkerToExit({
              db: site.db,
              scenario: "run-uncertain",
              thread,
              key,
              killAt: "tools:after-prepared-append",
              leaseMillis: CHILD_LEASE_MS,
              supplierDir: site.supplier,
            });
            expectKilled(result);
            yield* waitOutChildLease;

            yield* withHost(
              site.db,
              Effect.gen(function* () {
                const host = yield* NodeDurableHost;
                const snapshot = yield* lookupByKey(thread, key);
                const runId = runIdForSubmission(snapshot.submissionId);
                const report = host.startupRecovery.find(
                  (entry) => entry.submissionId === snapshot.submissionId,
                );
                // Fail-closed default (AGENTS rule 11): no registered policy proves anything, so
                // the open call becomes a durable Unknown Outcome and the lane blocks.
                expect(report?.decision._tag).toBe("MarkUnknown");
                expect(report?.disposition).toBe("unknown");
                expect(yield* lookupState(snapshot.submissionId)).toBe("unknown");
                expect(
                  (yield* readLog(thread)).map((envelope) => envelope.record.recordId),
                ).toContain(toolCallUnknownRecordId(runId, 1, decodeToolCallId(BOOK_CALL_ID)));

                // The unresolved ordinary call is never auto-replayed: draining grants nothing.
                const blocked = yield* drainUncertainBook(site, thread, FRESH_ANSWER);
                expect(blocked).toEqual([]);
                expect(supplierCount(site.supplier, "book", BOOK_REF)).toBe(0);

                // Authorized DUR-017 resolution arrives from a SECOND real process through the
                // shared durable ledger.
                yield* runResolver({
                  db: site.db,
                  scenario: "resolve-unknown",
                  thread,
                  key,
                });
                expect(yield* lookupState(snapshot.submissionId)).toBe("input-applied");

                const settlements = yield* drainUncertainBook(site, thread, FRESH_ANSWER);
                expect(settlements).toHaveLength(1);
                expect(settlements[0]?.outcome).toBe("completed");
                expect(supplierCount(site.supplier, "book", BOOK_REF)).toBe(1);

                const records = yield* readLog(thread);
                const resolved = records.find(
                  (envelope) =>
                    envelope.record.recordId ===
                    toolCallResolvedRecordId(runId, 1, decodeToolCallId(BOOK_CALL_ID)),
                )?.record.payload;
                expect(resolved?._tag).toBe("ToolCallResolved");
                if (resolved?._tag === "ToolCallResolved") {
                  expect(resolved.resolution).toBe("never-started");
                  expect(resolved.author).toBe("operator");
                }
                expect(
                  records.filter(
                    (envelope) =>
                      envelope.record.recordId ===
                      toolCallSettledRecordId(runId, 1, decodeToolCallId(BOOK_CALL_ID)),
                  ),
                ).toHaveLength(1);
                yield* assertConvergence(thread, [snapshot.submissionId], {
                  site,
                  counts: { [`book:${BOOK_REF}`]: 1 },
                });
              }),
            );
          }),
        ),
      30_000,
    );

    it.effect(
      "SIGKILL mid-handler: the reconciler recovers the supplier booking without a second call",
      () =>
        withCrashSite((site) =>
          Effect.gen(function* () {
            const thread = "thread-kill-midhandler";
            const key = "kill-midhandler-1";
            const exit = yield* Effect.scoped(
              Effect.gen(function* () {
                const handle = yield* startWorker({
                  db: site.db,
                  scenario: "run-uncertain",
                  thread,
                  key,
                  leaseMillis: CHILD_LEASE_MS,
                  supplierDir: site.supplier,
                  markerFile: site.marker,
                });
                // The handler has performed the EXTERNAL booking (marker proves it) but will
                // never return: kill the process while the call is prepared-but-unsettled.
                yield* waitForFile(site.marker);
                handle.kill();
                return yield* handle.awaitExit;
              }),
            );
            expect(exit.signal).toBe("SIGKILL");
            yield* waitOutChildLease;
            expect(supplierCount(site.supplier, "book", BOOK_REF)).toBe(1);

            yield* withHost(
              site.db,
              Effect.gen(function* () {
                const host = yield* NodeDurableHost;
                const snapshot = yield* lookupByKey(thread, key);
                const runId = runIdForSubmission(snapshot.submissionId);
                const report = host.startupRecovery.find(
                  (entry) => entry.submissionId === snapshot.submissionId,
                );
                // The supplier store shows the booking: recovery settles the recovered result
                // canonically WITHOUT executing anything (never fabricate, durability §10).
                expect(report?.decision._tag).toBe("MarkUnknown");
                expect(report?.disposition).toBe("repaired");
                expect(supplierCount(site.supplier, "book", BOOK_REF)).toBe(1);

                const records = yield* readLog(thread);
                const settled = records.find(
                  (envelope) =>
                    envelope.record.recordId ===
                    toolCallSettledRecordId(runId, 1, decodeToolCallId(BOOK_CALL_ID)),
                )?.record.payload;
                expect(settled?._tag).toBe("ToolCallSettled");
                if (settled?._tag === "ToolCallSettled") {
                  expect(settled.result).toEqual({ confirmation: `confirmed-${BOOK_REF}` });
                  expect(settled.isFailure).toBe(false);
                }
                const resolved = records.find(
                  (envelope) =>
                    envelope.record.recordId ===
                    toolCallResolvedRecordId(runId, 1, decodeToolCallId(BOOK_CALL_ID)),
                )?.record.payload;
                expect(resolved?._tag).toBe("ToolCallResolved");
                if (resolved?._tag === "ToolCallResolved") {
                  expect(resolved.resolution).toBe("completed-with-result");
                  expect(resolved.author).toBe("reconciler");
                }

                const settlements = yield* drainUncertainBook(site, thread, FRESH_ANSWER);
                expect(settlements).toHaveLength(1);
                expect(settlements[0]?.outcome).toBe("completed");
                // The supplier call count stays 1: the recovered outcome was never re-executed.
                expect(supplierCount(site.supplier, "book", BOOK_REF)).toBe(1);
                yield* assertConvergence(thread, [snapshot.submissionId], {
                  site,
                  counts: { [`book:${BOOK_REF}`]: 1 },
                });
              }),
              { toolReconciler: supplierReconcilerLayer(site.supplier) },
            );
          }),
        ),
      30_000,
    );

    it.effect(
      "kill after the handler returns, before the results append: the idempotent contract re-executes honestly",
      () =>
        withCrashSite((site) =>
          Effect.gen(function* () {
            const thread = "thread-kill-preresults";
            const key = "kill-preresults-1";
            // The supplier gate arms `append:before` to fire only once the booking exists, so the
            // kill lands exactly between the handler's return and the Turn's results append.
            const result = yield* runWorkerToExit({
              db: site.db,
              scenario: "run-idempotent",
              thread,
              key,
              killAtStorage: "append:before",
              killRequiresSupplier: `book:${BOOK_REF}`,
              leaseMillis: CHILD_LEASE_MS,
              supplierDir: site.supplier,
            });
            expectKilled(result);
            yield* waitOutChildLease;
            expect(supplierCount(site.supplier, "book", BOOK_REF)).toBe(1);

            // Deliberately NO recovery pass: a recovery pass has no Agent Binding, so the
            // annotation is invisible there and the default reconciler would fail closed to
            // Unknown. The WORKER resume owns the declared idempotency contract.
            yield* withRuntime(
              site.db,
              Effect.gen(function* () {
                const snapshot = yield* lookupByKey(thread, key);
                const runId = runIdForSubmission(snapshot.submissionId);
                expect(snapshot.state).not.toBe("settled");
                const before = (yield* readLog(thread)).map((envelope) => envelope.record.recordId);
                // The result was lost in memory: prepared is canonical, settled is not.
                expect(before).toContain(
                  toolCallPreparedRecordId(runId, 1, decodeToolCallId(BOOK_CALL_ID)),
                );
                expect(before).not.toContain(
                  toolCallSettledRecordId(runId, 1, decodeToolCallId(BOOK_CALL_ID)),
                );

                const settlements = yield* drainIdempotentBook(site, thread, FRESH_ANSWER);
                expect(settlements).toHaveLength(1);
                expect(settlements[0]?.outcome).toBe("completed");

                // Honest at-least-once: the declared contract re-executed the external call and
                // the duplicate is OBSERVABLE — supplier count 2, never hidden (rule 8).
                expect(supplierCount(site.supplier, "book", BOOK_REF)).toBe(2);
                const records = yield* readLog(thread);
                expect(logTags(records)).not.toContain("ToolCallUnknown");
                expect(
                  records.filter(
                    (envelope) =>
                      envelope.record.recordId ===
                      toolCallSettledRecordId(runId, 1, decodeToolCallId(BOOK_CALL_ID)),
                  ),
                ).toHaveLength(1);
                yield* assertConvergence(thread, [snapshot.submissionId], {
                  site,
                  counts: { [`book:${BOOK_REF}`]: 2 },
                });
              }),
            );
          }),
        ),
      30_000,
    );

    it.effect(
      "kill at step:after-step-append: step 1 replays from its record while step 2 executes once",
      () =>
        withCrashSite((site) =>
          Effect.gen(function* () {
            const thread = "thread-kill-step";
            const key = "kill-step-1";
            const result = yield* runWorkerToExit({
              db: site.db,
              scenario: "run-steps",
              thread,
              key,
              killAt: "step:after-step-append",
              leaseMillis: CHILD_LEASE_MS,
              supplierDir: site.supplier,
            });
            expectKilled(result);
            yield* waitOutChildLease;
            // Step 1 executed and committed; step 2 never ran; the handler entered once.
            expect(supplierCount(site.supplier, "itinerary-enter", STEP_REF)).toBe(1);
            expect(supplierCount(site.supplier, "reserve-flight", STEP_REF)).toBe(1);
            expect(supplierCount(site.supplier, "reserve-lodging", STEP_REF)).toBe(0);

            yield* withHost(
              site.db,
              Effect.gen(function* () {
                const host = yield* NodeDurableHost;
                const snapshot = yield* lookupByKey(thread, key);
                const runId = runIdForSubmission(snapshot.submissionId);
                const callId = decodeToolCallId(ITINERARY_CALL_ID);
                const committed = yield* readLog(thread);
                expect(committed.map((envelope) => envelope.record.recordId)).toContain(
                  toolStepSettledRecordId(runId, callId, "reserve-flight"),
                );
                const report = host.startupRecovery.find(
                  (entry) => entry.submissionId === snapshot.submissionId,
                );
                // The Durable Tool is re-enterable (SafeToRetry proof): the worker resumes it.
                expect(report?.decision._tag).toBe("MarkUnknown");
                expect(report?.disposition).toBe("deferred");

                const settlements = yield* drainItinerary(site, thread, FRESH_ANSWER);
                expect(settlements).toHaveLength(1);
                expect(settlements[0]?.outcome).toBe("completed");

                // The handler re-entered honestly (at-least-once), step 1 replayed its recorded
                // result WITHOUT executing (supplier count stays 1), step 2 executed exactly once.
                expect(supplierCount(site.supplier, "itinerary-enter", STEP_REF)).toBe(2);
                expect(supplierCount(site.supplier, "reserve-flight", STEP_REF)).toBe(1);
                expect(supplierCount(site.supplier, "reserve-lodging", STEP_REF)).toBe(1);

                const records = yield* readLog(thread);
                expect(
                  records.filter(
                    (envelope) =>
                      envelope.record.recordId ===
                      toolStepSettledRecordId(runId, callId, "reserve-flight"),
                  ),
                ).toHaveLength(1);
                expect(records.map((envelope) => envelope.record.recordId)).toContain(
                  toolStepSettledRecordId(runId, callId, "reserve-lodging"),
                );
                const settled = records.find(
                  (envelope) =>
                    envelope.record.recordId === toolCallSettledRecordId(runId, 1, callId),
                )?.record.payload;
                expect(settled?._tag).toBe("ToolCallSettled");
                if (settled?._tag === "ToolCallSettled") {
                  expect(settled.result).toEqual({
                    state: `flight-${STEP_REF}+lodging-${STEP_REF}`,
                  });
                }
                yield* assertConvergence(thread, [snapshot.submissionId], {
                  site,
                  counts: {
                    [`itinerary-enter:${STEP_REF}`]: 2,
                    [`reserve-flight:${STEP_REF}`]: 1,
                    [`reserve-lodging:${STEP_REF}`]: 1,
                  },
                });
              }),
              { toolReconciler: supplierReconcilerLayer(site.supplier) },
            );
          }),
        ),
      30_000,
    );

    it.effect(
      "kill at approval:after-request-append: recovery repairs the suspension from history and nothing executes",
      () =>
        withCrashSite((site) =>
          Effect.gen(function* () {
            const thread = "thread-kill-approval-request";
            const key = "kill-approval-request-1";
            const result = yield* runWorkerToExit({
              db: site.db,
              scenario: "suspend-approval",
              thread,
              key,
              killAt: "approval:after-request-append",
              leaseMillis: CHILD_LEASE_MS,
              supplierDir: site.supplier,
            });
            expectKilled(result);
            yield* waitOutChildLease;

            // Before recovery: the request is canonical, the ledger never suspended.
            yield* withRuntime(
              site.db,
              Effect.gen(function* () {
                const snapshot = yield* lookupByKey(thread, key);
                const runId = runIdForSubmission(snapshot.submissionId);
                expect(snapshot.state).toBe("input-applied");
                expect(
                  (yield* readLog(thread)).map((envelope) => envelope.record.recordId),
                ).toContain(toolApprovalRequestRecordId(runId, 1, decodeToolCallId(BOOK_CALL_ID)));
              }),
            );

            yield* withHost(
              site.db,
              Effect.gen(function* () {
                const host = yield* NodeDurableHost;
                const snapshot = yield* lookupByKey(thread, key);
                const runId = runIdForSubmission(snapshot.submissionId);
                const report = host.startupRecovery.find(
                  (entry) => entry.submissionId === snapshot.submissionId,
                );
                // The lost suspension is repaired from the canonical request (durability §8);
                // no execution, no settlement.
                expect(report?.decision._tag).toBe("AwaitApprovalDecision");
                expect(report?.disposition).toBe("repaired");
                expect(yield* lookupState(snapshot.submissionId)).toBe("suspended");
                expect(supplierCount(site.supplier, "book", BOOK_REF)).toBe(0);

                yield* runResolver({
                  db: site.db,
                  scenario: "resolve-approval",
                  thread,
                  key,
                  decision: "approved",
                });
                expect(yield* lookupState(snapshot.submissionId)).toBe("input-applied");

                const settlements = yield* drainApprovalBook(site, thread, FRESH_ANSWER);
                expect(settlements).toHaveLength(1);
                expect(settlements[0]?.outcome).toBe("completed");
                expect(supplierCount(site.supplier, "book", BOOK_REF)).toBe(1);

                // The request was appended exactly once across the crash and both Attempts.
                const records = yield* readLog(thread);
                expect(
                  records.filter(
                    (envelope) => envelope.record.payload._tag === "ToolApprovalRequested",
                  ),
                ).toHaveLength(1);
                const decision = records.find(
                  (envelope) =>
                    envelope.record.recordId ===
                    toolApprovalDecisionRecordId(runId, 1, decodeToolCallId(BOOK_CALL_ID)),
                )?.record.payload;
                expect(decision?._tag).toBe("ToolApprovalDecided");
                if (decision?._tag === "ToolApprovalDecided") {
                  expect(decision.decision).toBe("approved");
                  expect(decision.resolver).toBe("operator");
                }
                yield* assertConvergence(thread, [snapshot.submissionId], {
                  site,
                  counts: { [`book:${BOOK_REF}`]: 1 },
                });
              }),
            );
          }),
        ),
      30_000,
    );

    it.effect(
      "kill at approval:after-suspend: resolveApproval(approved) from a second process resumes the declared batch",
      () =>
        withCrashSite((site) =>
          Effect.gen(function* () {
            const thread = "thread-kill-approval-suspend";
            const key = "kill-approval-suspend-1";
            const result = yield* runWorkerToExit({
              db: site.db,
              scenario: "suspend-approval",
              thread,
              key,
              killAt: "approval:after-suspend",
              supplierDir: site.supplier,
            });
            expectKilled(result);

            yield* withHost(
              site.db,
              Effect.gen(function* () {
                const host = yield* NodeDurableHost;
                const snapshot = yield* lookupByKey(thread, key);
                const runId = runIdForSubmission(snapshot.submissionId);
                // The suspend transaction committed before the crash: the lane is durably
                // suspended, permit-free, with nothing for recovery to repair.
                expect(snapshot.state).toBe("suspended");
                const report = host.startupRecovery.find(
                  (entry) => entry.submissionId === snapshot.submissionId,
                );
                expect(report?.decision._tag).toBe("AwaitApprovalDecision");
                expect(report?.disposition).toBe("deferred");

                yield* runResolver({
                  db: site.db,
                  scenario: "resolve-approval",
                  thread,
                  key,
                  decision: "approved",
                });
                expect(yield* lookupState(snapshot.submissionId)).toBe("input-applied");

                const settlements = yield* drainApprovalBook(site, thread, FRESH_ANSWER);
                expect(settlements).toHaveLength(1);
                expect(settlements[0]?.outcome).toBe("completed");
                expect(supplierCount(site.supplier, "book", BOOK_REF)).toBe(1);

                // Batch resume, not model re-invocation: exactly one ModelResponseRecorded for
                // the declaring Turn; the gated call entered the ordinary uncertainty protocol.
                const records = yield* readLog(thread);
                const ids = records.map((envelope) => envelope.record.recordId);
                expect(
                  records.filter(
                    (envelope) => envelope.record.recordId === modelResponseRecordId(runId, 1),
                  ),
                ).toHaveLength(1);
                expect(ids).toContain(
                  toolCallPreparedRecordId(runId, 1, decodeToolCallId(BOOK_CALL_ID)),
                );
                expect(ids).toContain(
                  toolCallSettledRecordId(runId, 1, decodeToolCallId(BOOK_CALL_ID)),
                );
                yield* assertConvergence(thread, [snapshot.submissionId], {
                  site,
                  counts: { [`book:${BOOK_REF}`]: 1 },
                });
              }),
            );
          }),
        ),
      30_000,
    );

    it.effect(
      "kill at approval:after-suspend: a denial from a second process settles failed with the canonical decision",
      () =>
        withCrashSite((site) =>
          Effect.gen(function* () {
            const thread = "thread-kill-approval-deny";
            const key = "kill-approval-deny-1";
            const result = yield* runWorkerToExit({
              db: site.db,
              scenario: "suspend-approval",
              thread,
              key,
              killAt: "approval:after-suspend",
              supplierDir: site.supplier,
            });
            expectKilled(result);

            yield* withHost(
              site.db,
              Effect.gen(function* () {
                const snapshot = yield* lookupByKey(thread, key);
                const runId = runIdForSubmission(snapshot.submissionId);
                expect(snapshot.state).toBe("suspended");

                yield* runResolver({
                  db: site.db,
                  scenario: "resolve-approval",
                  thread,
                  key,
                  decision: "denied",
                });

                const settlements = yield* drainApprovalBook(site, thread, FRESH_ANSWER);
                expect(settlements).toHaveLength(1);
                // Denial-terminal (P2 default): the Run fails with the denial canonical and the
                // handler NEVER started — the supplier store stays empty.
                expect(settlements[0]?.outcome).toBe("failed");
                expect(supplierCount(site.supplier, "book", BOOK_REF)).toBe(0);

                const records = yield* readLog(thread);
                const ids = records.map((envelope) => envelope.record.recordId);
                const decision = records.find(
                  (envelope) =>
                    envelope.record.recordId ===
                    toolApprovalDecisionRecordId(runId, 1, decodeToolCallId(BOOK_CALL_ID)),
                )?.record.payload;
                expect(decision?._tag).toBe("ToolApprovalDecided");
                if (decision?._tag === "ToolApprovalDecided") {
                  expect(decision.decision).toBe("denied");
                }
                expect(ids).not.toContain(
                  toolCallPreparedRecordId(runId, 1, decodeToolCallId(BOOK_CALL_ID)),
                );
                expect(ids).not.toContain(
                  toolCallSettledRecordId(runId, 1, decodeToolCallId(BOOK_CALL_ID)),
                );
                yield* assertConvergence(thread, [snapshot.submissionId], {
                  site,
                  counts: {},
                });
              }),
            );
          }),
        ),
      30_000,
    );

    it.effect(
      "kill at join:after-claim: RevertJoining returns the queued Submission and it joins exactly once",
      () =>
        withCrashSite((site) =>
          Effect.gen(function* () {
            const thread = "thread-kill-join-claim";
            const key = "kill-join-claim";
            const result = yield* runWorkerToExit({
              db: site.db,
              scenario: "run-join",
              thread,
              key,
              killAt: "join:after-claim",
              leaseMillis: CHILD_LEASE_MS,
            });
            expectKilled(result);
            yield* waitOutChildLease;

            // Before recovery: the claim is durable, the canonical input never committed.
            const queuedId = yield* withRuntime(
              site.db,
              Effect.gen(function* () {
                const queued = yield* lookupByKey(thread, `${key}-2`);
                expect(queued.state).toBe("joining");
                expect(
                  (yield* readLog(thread)).map((envelope) => envelope.record.recordId),
                ).not.toContain(submissionInputRecordId(queued.submissionId));
                return queued.submissionId;
              }),
            );

            yield* withHost(
              site.db,
              Effect.gen(function* () {
                const host = yield* NodeDurableHost;
                const headSnapshot = yield* lookupByKey(thread, `${key}-1`);
                const queuedReport = host.startupRecovery.find(
                  (entry) => entry.submissionId === queuedId,
                );
                // DUR-016: joining without canonical input reverts to ready.
                expect(queuedReport?.decision._tag).toBe("RevertJoining");
                expect(queuedReport?.disposition).toBe("repaired");
                expect(yield* lookupState(queuedId)).toBe("ready");

                const settlements = yield* drainPlanner(thread, FRESH_ANSWER);
                // One HEAD settlement: the reverted Submission re-joined the resumed host Run.
                expect(settlements.map((settlement) => settlement.submissionId)).toEqual([
                  headSnapshot.submissionId,
                ]);
                expect(yield* lookupState(queuedId)).toBe("settled");

                // Delivered exactly once: one canonical input record ever, and the queued text
                // entered exactly one committed model response (the coverage rule's evidence).
                const records = yield* readLog(thread);
                expect(
                  records.filter(
                    (envelope) => envelope.record.recordId === submissionInputRecordId(queuedId),
                  ),
                ).toHaveLength(1);
                expect(responseOccurrences(records, JOIN_QUESTION)).toBe(1);
                yield* assertConvergence(thread, [headSnapshot.submissionId, queuedId]);
              }),
            );
          }),
        ),
      30_000,
    );

    it.effect(
      "kill at join:after-canonical-append: RepairJoinMarker reattaches the input without duplication",
      () =>
        withCrashSite((site) =>
          Effect.gen(function* () {
            const thread = "thread-kill-join-append";
            const key = "kill-join-append";
            const result = yield* runWorkerToExit({
              db: site.db,
              scenario: "run-join",
              thread,
              key,
              killAt: "join:after-canonical-append",
              leaseMillis: CHILD_LEASE_MS,
            });
            expectKilled(result);
            yield* waitOutChildLease;

            // Before recovery: the input is canonical, only the joined marker was lost.
            const queuedId = yield* withRuntime(
              site.db,
              Effect.gen(function* () {
                const queued = yield* lookupByKey(thread, `${key}-2`);
                expect(queued.state).toBe("joining");
                expect(
                  (yield* readLog(thread)).map((envelope) => envelope.record.recordId),
                ).toContain(submissionInputRecordId(queued.submissionId));
                return queued.submissionId;
              }),
            );

            yield* withHost(
              site.db,
              Effect.gen(function* () {
                const host = yield* NodeDurableHost;
                const headSnapshot = yield* lookupByKey(thread, `${key}-1`);
                const queuedReport = host.startupRecovery.find(
                  (entry) => entry.submissionId === queuedId,
                );
                expect(queuedReport?.decision._tag).toBe("RepairJoinMarker");
                expect(queuedReport?.disposition).toBe("repaired");
                expect(yield* lookupState(queuedId)).toBe("joined");

                const settlements = yield* drainPlanner(thread, FRESH_ANSWER);
                expect(settlements.map((settlement) => settlement.submissionId)).toEqual([
                  headSnapshot.submissionId,
                ]);
                expect(yield* lookupState(queuedId)).toBe("settled");

                // Reattached, never duplicated: one canonical record, one prompt coverage.
                const records = yield* readLog(thread);
                expect(
                  records.filter(
                    (envelope) => envelope.record.recordId === submissionInputRecordId(queuedId),
                  ),
                ).toHaveLength(1);
                expect(responseOccurrences(records, JOIN_QUESTION)).toBe(1);
                yield* assertConvergence(thread, [headSnapshot.submissionId, queuedId]);
              }),
            );
          }),
        ),
      30_000,
    );

    it.effect(
      "SIGKILL of the host after the join: the resumed host covers the joined input once and both settle",
      () =>
        withCrashSite((site) =>
          Effect.gen(function* () {
            const thread = "thread-kill-joined-host";
            const key = "kill-joined-host";
            const exit = yield* Effect.scoped(
              Effect.gen(function* () {
                const handle = yield* startWorker({
                  db: site.db,
                  scenario: "run-join",
                  thread,
                  key,
                  leaseMillis: CHILD_LEASE_MS,
                  markerFile: site.marker,
                });
                // The marker is written by Turn 1's model stream, which begins only after the
                // pre-Turn join drain claimed, appended, and marked the queued input joined.
                yield* waitForFile(site.marker);
                handle.kill();
                return yield* handle.awaitExit;
              }),
            );
            expect(exit.signal).toBe("SIGKILL");
            yield* waitOutChildLease;

            // Durable state: `joined` with a nonterminal host and an uncovered canonical input.
            const queuedId = yield* withRuntime(
              site.db,
              Effect.gen(function* () {
                const queued = yield* lookupByKey(thread, `${key}-2`);
                expect(queued.state).toBe("joined");
                const records = yield* readLog(thread);
                expect(records.map((envelope) => envelope.record.recordId)).toContain(
                  submissionInputRecordId(queued.submissionId),
                );
                expect(logTags(records)).not.toContain("ModelResponseRecorded");
                return queued.submissionId;
              }),
            );

            yield* withHost(
              site.db,
              Effect.gen(function* () {
                const host = yield* NodeDurableHost;
                const headSnapshot = yield* lookupByKey(thread, `${key}-1`);
                const queuedReport = host.startupRecovery.find(
                  (entry) => entry.submissionId === queuedId,
                );
                // A live host owns its joined Submissions: recovery defers to the host's resume.
                expect(queuedReport?.decision._tag).toBe("AwaitHostSettlement");
                expect(queuedReport?.disposition).toBe("deferred");

                const settlements = yield* drainPlanner(thread, FRESH_ANSWER);
                expect(settlements.map((settlement) => settlement.submissionId)).toEqual([
                  headSnapshot.submissionId,
                ]);
                expect(settlements[0]?.outcome).toBe("completed");
                expect(yield* lookupState(queuedId)).toBe("settled");

                // The coverage rule across process death: the reattached input entered exactly
                // one committed model response, and the joined settlement rides the host Run.
                const records = yield* readLog(thread);
                expect(
                  records.filter(
                    (envelope) => envelope.record.recordId === submissionInputRecordId(queuedId),
                  ),
                ).toHaveLength(1);
                expect(responseOccurrences(records, JOIN_QUESTION)).toBe(1);
                const joinedSettlement = records.find(
                  (envelope) => envelope.record.recordId === submissionSettlementRecordId(queuedId),
                )?.record.payload;
                expect(joinedSettlement?._tag).toBe("SubmissionSettled");
                if (joinedSettlement?._tag === "SubmissionSettled") {
                  expect(joinedSettlement.outcome).toBe("completed");
                  expect(joinedSettlement.runId).toBe(
                    runIdForSubmission(headSnapshot.submissionId),
                  );
                }
                yield* assertConvergence(thread, [headSnapshot.submissionId, queuedId]);
              }),
            );
          }),
        ),
      30_000,
    );

    it.effect(
      "kill between host finalization and joined settlement: SettleJoinedWithHost completes the obligation",
      () =>
        withCrashSite((site) =>
          Effect.gen(function* () {
            const thread = "thread-kill-joined-settle";
            const key = "kill-joined-settle";
            // The FIRST ledger finalization of the run is the host's: the kill lands after the
            // host is fully settled but before the joined settlement loop starts.
            const result = yield* runWorkerToExit({
              db: site.db,
              scenario: "run-join",
              thread,
              key,
              killAtStorage: "ledger:finalize-settlement:after",
              leaseMillis: CHILD_LEASE_MS,
            });
            expectKilled(result);

            const ids = yield* withRuntime(
              site.db,
              Effect.gen(function* () {
                const head = yield* lookupByKey(thread, `${key}-1`);
                const queued = yield* lookupByKey(thread, `${key}-2`);
                expect(head.state).toBe("settled");
                expect(queued.state).toBe("joined");
                const recordIds = (yield* readLog(thread)).map(
                  (envelope) => envelope.record.recordId,
                );
                expect(recordIds).toContain(submissionSettlementRecordId(head.submissionId));
                expect(recordIds).not.toContain(submissionSettlementRecordId(queued.submissionId));
                return { head: head.submissionId, queued: queued.submissionId };
              }),
            );

            yield* withHost(
              site.db,
              Effect.gen(function* () {
                const host = yield* NodeDurableHost;
                const queuedReport = host.startupRecovery.find(
                  (entry) => entry.submissionId === ids.queued,
                );
                // The canonical host settlement authorizes the joined settlement (DUR-015).
                expect(queuedReport?.decision._tag).toBe("SettleJoinedWithHost");
                expect(queuedReport?.disposition).toBe("repaired");
                expect(yield* lookupState(ids.queued)).toBe("settled");

                const records = yield* readLog(thread);
                const joinedSettlement = records.find(
                  (envelope) =>
                    envelope.record.recordId === submissionSettlementRecordId(ids.queued),
                )?.record.payload;
                expect(joinedSettlement?._tag).toBe("SubmissionSettled");
                if (joinedSettlement?._tag === "SubmissionSettled") {
                  expect(joinedSettlement.outcome).toBe("completed");
                  expect(joinedSettlement.runId).toBe(runIdForSubmission(ids.head));
                }
                yield* assertConvergence(thread, [ids.head, ids.queued]);
              }),
            );
          }),
        ),
      30_000,
    );
  },
);

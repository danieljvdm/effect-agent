import type { SubmissionId } from "@effect-agent/core";
import {
  AbortCommand,
  AgentBindingResolver,
  ConversationStore,
  ConversationTailRequest,
  DurableAgentRuntime,
  RecoverySnapshotRequest,
  SubmissionLedger,
  SubmissionLookupById,
  SubmissionLookupByKey,
  childConversationIdFor,
  childIdempotencyKeyFor,
  runIdForSubmission,
  toolCallSettledRecordId,
  type DurableRuntimeFailpointLocation,
  type ResolvedBinding,
} from "@effect-agent/session";
import { NodeFileSystem } from "@effect/platform-node";
import { expect, layer } from "@effect/vitest";
import { Effect, Option } from "effect";

import { NodeDurableHost } from "../../src/index.ts";
import {
  CHILD_MODEL_OP,
  CRASH_CALLER,
  CRASH_PRINCIPAL,
  CRASH_QUESTION,
  DELEGATE_CALL_ID,
  FENCED_EXIT_CODE,
  PROJECTED_SUMMARY,
  RESEARCH_TOPIC,
  childModelInvocations,
  coordinatorSubmitSlice,
  crashSubmitOptions,
  decodeConversationId,
  decodeToolCallId,
  makeCrashSubagentBindings,
} from "./fixtures.ts";
import {
  CHILD_LEASE_MS,
  assertConvergence,
  childMessages,
  expectKilled,
  lookupByKey,
  payloadsOf,
  readLog,
  runWorkerToExit,
  startWorker,
  touchFile,
  waitForFile,
  waitOutChildLease,
  withCrashSite,
  withHost,
  withRuntime,
  type CrashSite,
} from "./harness.ts";

/**
 * S2 durable attached Subagents — process-kill crash matrix (spec/subagents.md §14, plan §4.4).
 * Every row spawns `worker-entry.ts` as a REAL child process over a temp SQLite file, kills it
 * at an armed coordinator/storage failpoint (or SIGKILLs it while blocked mid-Attempt), restarts
 * a host against the same file, and asserts the required durable outcome. The scripted child
 * LanguageModel counts its invocations through the FILE-BACKED supplier store, so "a completed
 * child is never re-executed" (§16.4) is asserted from external truth that survives the kill —
 * never assumed. Real clocks, files, and processes, hence `excludeTestServices`.
 */

const DELEGATE_CALL = decodeToolCallId(DELEGATE_CALL_ID);
const CHILD_MODEL_COUNT_KEY = `${CHILD_MODEL_OP}:${RESEARCH_TOPIC}`;

const submitCoordinator = (conversation: string, key: string) =>
  Effect.gen(function* () {
    const runtime = yield* DurableAgentRuntime;
    return yield* runtime.submit(
      coordinatorSubmitSlice,
      { mission: CRASH_QUESTION },
      crashSubmitOptions(conversation, key),
    );
  });

/** Drive one Conversation lane through the S2 multi-binding worker path (SUB-023). */
const driveWith = (bindings: ReadonlyArray<ResolvedBinding>) => (conversation: string) =>
  Effect.gen(function* () {
    const runtime = yield* DurableAgentRuntime;
    return yield* runtime
      .processConversationResolved(decodeConversationId(conversation))
      .pipe(
        Effect.provideService(AgentBindingResolver, AgentBindingResolver.fromBindings(bindings)),
      );
  });

/** Restart-drive bindings: the parent model always answers final (batch resume, P5 precedent). */
const restartBindings = (site: CrashSite) =>
  makeCrashSubagentBindings({ supplierDir: site.supplier });

const reservationStatuses = (submissionId: SubmissionId) =>
  Effect.gen(function* () {
    const ledger = yield* SubmissionLedger;
    const snapshot = yield* ledger.loadRecoverySnapshot(
      RecoverySnapshotRequest.make({ submissionId }),
    );
    return snapshot.childReservations.map((row) => row.status);
  });

const submissionSnapshot = (submissionId: SubmissionId) =>
  Effect.gen(function* () {
    const ledger = yield* SubmissionLedger;
    const snapshot = yield* ledger.lookup(SubmissionLookupById.make({ submissionId }));
    expect(Option.isSome(snapshot)).toBe(true);
    if (Option.isNone(snapshot)) throw new Error("Expected the Submission to exist");
    return snapshot.value;
  });

const startedPayloadOf = (conversation: string) =>
  Effect.gen(function* () {
    const log = yield* readLog(conversation);
    const payload = payloadsOf(log, "SubagentStarted")[0]?.record.payload;
    if (payload?._tag !== "SubagentStarted") throw new Error("Expected SubagentStarted");
    return payload;
  });

const readTail = (conversation: string) =>
  Effect.gen(function* () {
    const store = yield* ConversationStore;
    return yield* store.inspectTail(
      ConversationTailRequest.make({ conversationId: decodeConversationId(conversation) }),
    );
  });

const recordIdsOf = (conversation: string) =>
  Effect.map(readLog(conversation), (records) =>
    records.map((envelope) => envelope.record.recordId as string),
  );

/** The delegation call settled exactly once with the given payload expectations. */
const assertDelegationSettled = (
  conversation: string,
  parentSubmissionId: SubmissionId,
  expected: { readonly isFailure: boolean; readonly result: unknown },
) =>
  Effect.gen(function* () {
    const runId = runIdForSubmission(parentSubmissionId);
    const log = yield* readLog(conversation);
    const settled = log.filter(
      (envelope) => envelope.record.recordId === toolCallSettledRecordId(runId, 1, DELEGATE_CALL),
    );
    expect(settled).toHaveLength(1);
    const payload = settled[0]?.record.payload;
    expect(payload?._tag).toBe("ToolCallSettled");
    if (payload?._tag === "ToolCallSettled") {
      expect(payload.isFailure).toBe(expected.isFailure);
      expect(payload.result).toEqual(expected.result);
    }
  });

/**
 * The one-child identity claims common to every converged establishment (SUB-016/SUB-017):
 * exactly one requested/started/joined record, one child Conversation with one lineage record,
 * and the recorded Receipt naming the one admitted child.
 */
const assertOneEstablishedChild = (conversation: string, childConversation: string) =>
  Effect.gen(function* () {
    const log = yield* readLog(conversation);
    expect(payloadsOf(log, "SubagentRequested")).toHaveLength(1);
    expect(payloadsOf(log, "SubagentStarted")).toHaveLength(1);
    expect(payloadsOf(log, "SubagentJoined")).toHaveLength(1);
    const childLog = yield* readLog(childConversation);
    expect(payloadsOf(childLog, "ConversationCreated")).toHaveLength(1);
    expect(payloadsOf(childLog, "SubagentLineageRecorded")).toHaveLength(1);
    const started = yield* startedPayloadOf(conversation);
    const child = yield* submissionSnapshot(started.childSubmissionId);
    expect(child.receiptId).toBe(started.childReceiptId);
    expect(child.state).toBe("settled");
    return started;
  });

layer(NodeFileSystem.layer, { excludeTestServices: true })(
  "S2 durable Subagent crash matrix (real process kills)",
  (it) => {
    it.effect(
      "every establishment failpoint kill converges on one child Receipt, Conversation, and join",
      () =>
        Effect.gen(function* () {
          const rows: ReadonlyArray<{
            readonly location: DurableRuntimeFailpointLocation;
            readonly decision: string;
            readonly disposition: string;
          }> = [
            // Reservation exists, request absent, live parent: the declared batch re-executes
            // and the idempotent handler appends the fixed request exactly once (spec §13).
            {
              location: "subagent:after-reserve",
              decision: "ResumePendingToolBatch",
              disposition: "deferred",
            },
            // Requested + notAdmitted: binding-free recovery admits the ONE intended child from
            // the canonical SubagentRequested payload (SUB-016/SUB-031).
            {
              location: "subagent:after-request-append",
              decision: "CompleteChildAdmission",
              disposition: "repaired",
            },
            // Admitted before readiness: recovery completes materialization/lineage/readiness
            // idempotently and appends the exact start link for the same Receipt.
            {
              location: "subagent:after-admit",
              decision: "RepairSubagentStartLink",
              disposition: "repaired",
            },
            {
              location: "subagent:after-child-ready",
              decision: "RepairSubagentStartLink",
              disposition: "repaired",
            },
            // Started before the waitingForChild checkpoint: recovery restores the suspension;
            // the parent lane holds no worker permit while the child proceeds (SUB-030).
            {
              location: "subagent:after-start-append",
              decision: "EnsureWaitingForChild",
              disposition: "repaired",
            },
          ];
          for (const row of rows) {
            yield* withCrashSite((site) =>
              Effect.gen(function* () {
                const conversation = `conversation-${row.location.replaceAll(":", "-")}`;
                const key = `key-${row.location}`;
                const result = yield* runWorkerToExit({
                  db: site.db,
                  scenario: "subagent-run",
                  conversation,
                  key,
                  killAt: row.location,
                  leaseMillis: CHILD_LEASE_MS,
                  supplierDir: site.supplier,
                });
                expectKilled(result);
                yield* waitOutChildLease;

                const bindings = yield* restartBindings(site);
                const drive = driveWith(bindings);
                yield* withHost(
                  site.db,
                  Effect.gen(function* () {
                    const host = yield* NodeDurableHost;
                    const parent = yield* lookupByKey(conversation, key);
                    const report = host.startupRecovery.find(
                      (entry) => entry.submissionId === parent.submissionId,
                    );
                    expect(report?.decision._tag, row.location).toBe(row.decision);
                    expect(report?.disposition, row.location).toBe(row.disposition);

                    const childConversation = childConversationIdFor(
                      parent.submissionId,
                      DELEGATE_CALL,
                    );
                    // Worker convergence: the parent (re)establishes and suspends without
                    // settling, the child lane settles, the woken parent joins.
                    expect(yield* drive(conversation), row.location).toHaveLength(0);
                    const childSettlements = yield* drive(childConversation);
                    expect(
                      childSettlements.map((settlement) => settlement.outcome),
                      row.location,
                    ).toEqual(["completed"]);
                    const settlements = yield* drive(conversation);
                    expect(
                      settlements.map((settlement) => settlement.outcome),
                      row.location,
                    ).toEqual(["completed"]);

                    const started = yield* assertOneEstablishedChild(
                      conversation,
                      childConversation,
                    );
                    yield* assertDelegationSettled(conversation, parent.submissionId, {
                      isFailure: false,
                      result: { summary: PROJECTED_SUMMARY },
                    });
                    expect(yield* reservationStatuses(parent.submissionId), row.location).toEqual([
                      "released",
                    ]);
                    // The one child model invocation across the kill and every replay.
                    expect(childModelInvocations(site.supplier), row.location).toBe(1);
                    yield* assertConvergence(conversation, [parent.submissionId], {
                      site,
                      counts: { [CHILD_MODEL_COUNT_KEY]: 1 },
                    });
                    yield* assertConvergence(childConversation, [started.childSubmissionId]);
                  }),
                );
              }),
            );
          }
        }),
      150_000,
    );

    it.effect(
      "a reservation orphaned before its request releases exactly once under parent abort",
      () =>
        withCrashSite((site) =>
          Effect.gen(function* () {
            const conversation = "conversation-s2-orphan";
            const key = "s2-orphan-1";
            const result = yield* runWorkerToExit({
              db: site.db,
              scenario: "subagent-run",
              conversation,
              key,
              killAt: "subagent:after-reserve",
              leaseMillis: CHILD_LEASE_MS,
              supplierDir: site.supplier,
            });
            expectKilled(result);
            yield* waitOutChildLease;

            // Abort the parent from a client-only restart before any recovery pass runs.
            const parentId = yield* withRuntime(
              site.db,
              Effect.gen(function* () {
                const runtime = yield* DurableAgentRuntime;
                const parent = yield* lookupByKey(conversation, key);
                yield* runtime.abort(
                  AbortCommand.make({
                    submissionId: parent.submissionId,
                    author: "operator",
                    reason: "abandon before request",
                  }),
                  CRASH_CALLER,
                );
                return parent.submissionId;
              }),
            );

            yield* withHost(
              site.db,
              Effect.gen(function* () {
                const host = yield* NodeDurableHost;
                const runtime = yield* DurableAgentRuntime;
                const ledger = yield* SubmissionLedger;
                const report = host.startupRecovery.find(
                  (entry) => entry.submissionId === parentId,
                );
                // Provably childless reservation under abort: released exactly once, never a
                // child admitted merely to abort it (spec §13/§14).
                expect(report?.decision._tag).toBe("ReleaseOrphanChildReservation");
                expect(report?.disposition).toBe("repaired");
                expect(yield* reservationStatuses(parentId)).toEqual(["released"]);

                const second = yield* runtime.runRecovery;
                const settleReport = second.find((entry) => entry.submissionId === parentId);
                expect(settleReport?.decision._tag).toBe("SettleAborted");
                expect(settleReport?.disposition).toBe("repaired");
                const parent = yield* submissionSnapshot(parentId);
                expect(parent.state).toBe("settled");
                expect(parent.settledOutcome).toBe("aborted");

                // No child was ever admitted for the orphaned reservation (SUB-031 evidence).
                const resolution = yield* ledger.resolveAdmission(
                  SubmissionLookupByKey.make({
                    conversationId: childConversationIdFor(parentId, DELEGATE_CALL),
                    principal: CRASH_PRINCIPAL,
                    idempotencyKey: childIdempotencyKeyFor(
                      runIdForSubmission(parentId),
                      DELEGATE_CALL,
                    ),
                  }),
                );
                expect(resolution._tag).toBe("NotAdmitted");
                const log = yield* readLog(conversation);
                expect(payloadsOf(log, "SubagentRequested")).toHaveLength(0);
                // The delegation call is never marked Unknown (plan §4.3 classifier row 6).
                expect(payloadsOf(log, "ToolCallUnknown")).toHaveLength(0);
                expect(childModelInvocations(site.supplier)).toBe(0);
                yield* assertConvergence(conversation, [parentId], { site, counts: {} });
              }),
            );
          }),
        ),
      30_000,
    );

    it.effect(
      "kill between the child settlement finalize and the parent wake: recovery replays the wake",
      () =>
        withCrashSite((site) =>
          Effect.gen(function* () {
            const conversation = "conversation-s2-wake";
            const key = "s2-wake-1";
            // The FIRST settlement finalization of the scenario is the child's: the kill lands
            // after the child is durably settled but before `recordChildSettled` wakes the
            // suspended parent (spec §14 "after child Settlement, before parent observes it").
            const result = yield* runWorkerToExit({
              db: site.db,
              scenario: "subagent-run",
              conversation,
              key,
              killAtStorage: "ledger:finalize-settlement:after",
              leaseMillis: CHILD_LEASE_MS,
              supplierDir: site.supplier,
            });
            expectKilled(result);
            yield* waitOutChildLease;

            // Before recovery: the child is settled, the parent still durably suspended.
            const ids = yield* withRuntime(
              site.db,
              Effect.gen(function* () {
                const parent = yield* lookupByKey(conversation, key);
                expect(parent.state).toBe("suspended");
                const started = yield* startedPayloadOf(conversation);
                const child = yield* submissionSnapshot(started.childSubmissionId);
                expect(child.state).toBe("settled");
                return { parent: parent.submissionId, child: started.childSubmissionId };
              }),
            );

            const bindings = yield* restartBindings(site);
            const drive = driveWith(bindings);
            yield* withHost(
              site.db,
              Effect.gen(function* () {
                const host = yield* NodeDurableHost;
                const report = host.startupRecovery.find(
                  (entry) => entry.submissionId === ids.parent,
                );
                // Scan-seeded recovery replays the idempotent wake: a dropped wake is never a
                // lost obligation (plan §1.3).
                expect(report?.decision._tag).toBe("ResumeWaitingParent");
                expect(report?.disposition).toBe("repaired");
                expect((yield* submissionSnapshot(ids.parent)).state).toBe("input-applied");

                const settlements = yield* drive(conversation);
                expect(settlements.map((settlement) => settlement.outcome)).toEqual(["completed"]);
                // The settled child was read, verified, and joined — never re-executed.
                expect(childModelInvocations(site.supplier)).toBe(1);
                const childConversation = childConversationIdFor(ids.parent, DELEGATE_CALL);
                yield* assertOneEstablishedChild(conversation, childConversation);
                yield* assertConvergence(conversation, [ids.parent], {
                  site,
                  counts: { [CHILD_MODEL_COUNT_KEY]: 1 },
                });
                yield* assertConvergence(childConversation, [ids.child]);
              }),
            );
          }),
        ),
      30_000,
    );

    it.effect(
      "join and release failpoint kills replay the canonical accounting and never re-execute the child",
      () =>
        Effect.gen(function* () {
          const rows: ReadonlyArray<{
            readonly location: DurableRuntimeFailpointLocation;
            readonly decision: string | undefined;
          }> = [
            // Join canonical, release incomplete: ApplyJoinAccounting replays the decision FROM
            // the canonical SubagentJoined record — never available twice (spec §12 step 6).
            { location: "subagent:after-join-append", decision: "ApplyJoinAccounting" },
            // Frozen decision, release unapplied: the fixed amounts are applied exactly once.
            { location: "subagent:after-release-pending", decision: "ApplyJoinAccounting" },
            // Fully released: replay observes the settled call + released reservation; neither
            // repeats (spec §14 "after release, before parent continues").
            { location: "subagent:after-release", decision: undefined },
          ];
          for (const row of rows) {
            yield* withCrashSite((site) =>
              Effect.gen(function* () {
                const conversation = `conversation-${row.location.replaceAll(":", "-")}`;
                const key = `key-${row.location}`;
                const result = yield* runWorkerToExit({
                  db: site.db,
                  scenario: "subagent-run",
                  conversation,
                  key,
                  killAt: row.location,
                  leaseMillis: CHILD_LEASE_MS,
                  supplierDir: site.supplier,
                });
                expectKilled(result);
                yield* waitOutChildLease;
                // The child ran exactly once BEFORE the kill; everything after is replay.
                expect(childModelInvocations(site.supplier), row.location).toBe(1);

                const bindings = yield* restartBindings(site);
                const drive = driveWith(bindings);
                yield* withHost(
                  site.db,
                  Effect.gen(function* () {
                    const host = yield* NodeDurableHost;
                    const parent = yield* lookupByKey(conversation, key);
                    if (row.decision !== undefined) {
                      const report = host.startupRecovery.find(
                        (entry) => entry.submissionId === parent.submissionId,
                      );
                      expect(report?.decision._tag, row.location).toBe(row.decision);
                      expect(report?.disposition, row.location).toBe("repaired");
                    }
                    expect(yield* reservationStatuses(parent.submissionId), row.location).toEqual([
                      "released",
                    ]);

                    const settlements = yield* drive(conversation);
                    expect(
                      settlements.map((settlement) => settlement.outcome),
                      row.location,
                    ).toEqual(["completed"]);

                    const childConversation = childConversationIdFor(
                      parent.submissionId,
                      DELEGATE_CALL,
                    );
                    const started = yield* assertOneEstablishedChild(
                      conversation,
                      childConversation,
                    );
                    yield* assertDelegationSettled(conversation, parent.submissionId, {
                      isFailure: false,
                      result: { summary: PROJECTED_SUMMARY },
                    });
                    // §16.4: a completed child is never re-executed merely because the parent
                    // join acknowledgment was lost — the file-backed count stays at ONE.
                    expect(childModelInvocations(site.supplier), row.location).toBe(1);
                    yield* assertConvergence(conversation, [parent.submissionId], {
                      site,
                      counts: { [CHILD_MODEL_COUNT_KEY]: 1 },
                    });
                    yield* assertConvergence(childConversation, [started.childSubmissionId]);
                  }),
                );
              }),
            );
          }
        }),
      90_000,
    );

    it.effect(
      "kill at abort:after-intent with a waiting child: one idempotent abort command, joins before the aborted settlement",
      () =>
        withCrashSite((site) =>
          Effect.gen(function* () {
            const conversation = "conversation-s2-abort";
            const key = "s2-abort-1";
            const result = yield* runWorkerToExit({
              db: site.db,
              scenario: "subagent-abort",
              conversation,
              key,
              killAt: "abort:after-intent",
              supplierDir: site.supplier,
            });
            expectKilled(result);

            const bindings = yield* restartBindings(site);
            const drive = driveWith(bindings);
            yield* withHost(
              site.db,
              Effect.gen(function* () {
                const host = yield* NodeDurableHost;
                const runtime = yield* DurableAgentRuntime;
                const parent = yield* lookupByKey(conversation, key);
                const started = yield* startedPayloadOf(conversation);
                // Recovery emits the ONE idempotent child abort command and keeps the parent
                // waiting for the join (spec §13.1, request-abort-and-join).
                const parentReport = host.startupRecovery.find(
                  (entry) => entry.submissionId === parent.submissionId,
                );
                expect(parentReport?.decision._tag).toBe("PropagateChildAbort");
                expect(parentReport?.disposition).toBe("repaired");
                const childReport = host.startupRecovery.find(
                  (entry) => entry.submissionId === started.childSubmissionId,
                );
                expect(childReport?.decision._tag).toBe("SettleAborted");
                const child = yield* submissionSnapshot(started.childSubmissionId);
                expect(child.state).toBe("settled");
                expect(child.settledOutcome).toBe("aborted");
                // The never-started child never invoked its model.
                expect(childModelInvocations(site.supplier)).toBe(0);

                // The settled children classify as a pending join; the parent then settles
                // aborted strictly after the joins (spec §13.1).
                const second = yield* runtime.runRecovery;
                const wakeReport = second.find(
                  (entry) => entry.submissionId === parent.submissionId,
                );
                expect(wakeReport?.decision._tag).toBe("ResumeWaitingParent");
                const settlements = yield* drive(conversation);
                expect(settlements.map((settlement) => settlement.outcome)).toEqual(["aborted"]);

                // Exactly one durable child abort command became canonical (DUR-012).
                const childConversation = childConversationIdFor(
                  parent.submissionId,
                  DELEGATE_CALL,
                );
                const childLog = yield* readLog(childConversation);
                const abortRecords = payloadsOf(childLog, "AbortRequested");
                expect(abortRecords).toHaveLength(1);
                const abortPayload = abortRecords[0]?.record.payload;
                if (abortPayload?._tag === "AbortRequested") {
                  expect(abortPayload.author).toBe("subagent-parent-abort");
                }

                // The join committed with the child's ACTUAL outcome, before the settlement.
                const log = yield* readLog(conversation);
                const joined = payloadsOf(log, "SubagentJoined");
                expect(joined).toHaveLength(1);
                const joinedPayload = joined[0]?.record.payload;
                if (joinedPayload?._tag !== "SubagentJoined") {
                  throw new Error("Expected SubagentJoined");
                }
                expect(joinedPayload.childOutcome).toBe("aborted");
                const settledEnvelope = payloadsOf(log, "SubmissionSettled")[0];
                expect(settledEnvelope).toBeDefined();
                if (settledEnvelope !== undefined && joined[0] !== undefined) {
                  expect(Number(joined[0].sequence)).toBeLessThan(Number(settledEnvelope.sequence));
                }
                yield* assertDelegationSettled(conversation, parent.submissionId, {
                  isFailure: true,
                  result: {
                    errorTag: "SubagentParentAborted",
                    message: `The parent Submission aborted; attached child ${started.childSubmissionId} settled aborted`,
                  },
                });
                expect(yield* reservationStatuses(parent.submissionId)).toEqual(["released"]);
                yield* assertConvergence(conversation, [parent.submissionId], {
                  site,
                  counts: {},
                });
                yield* assertConvergence(childConversation, [started.childSubmissionId]);
              }),
            );
          }),
        ),
      30_000,
    );

    it.effect(
      "kill at subagent:after-child-abort-intent: the replayed propagation is a no-op, never a second command",
      () =>
        withCrashSite((site) =>
          Effect.gen(function* () {
            const conversation = "conversation-s2-abort-marker";
            const key = "s2-abort-marker-1";
            const bindings = yield* makeCrashSubagentBindings({
              supplierDir: site.supplier,
              parentScript: "delegate-then-final",
            });
            const drive = driveWith(bindings);

            // Arrange in-process: established waiting parent + durable parent abort intent,
            // with NO propagation yet (abort of a suspended lane only records the intent).
            const parentId = yield* withRuntime(
              site.db,
              Effect.gen(function* () {
                const runtime = yield* DurableAgentRuntime;
                yield* submitCoordinator(conversation, key);
                expect(yield* drive(conversation)).toHaveLength(0);
                const parent = yield* lookupByKey(conversation, key);
                expect(parent.state).toBe("suspended");
                yield* runtime.abort(
                  AbortCommand.make({
                    submissionId: parent.submissionId,
                    author: "operator",
                    reason: "abort the waiting parent",
                  }),
                  CRASH_CALLER,
                );
                return parent.submissionId;
              }),
            );

            // A recovery pass in a REAL process dies right after the child abort intent
            // commits — the recorded intent IS the propagation marker (spec §14).
            const result = yield* runWorkerToExit({
              db: site.db,
              scenario: "subagent-recover",
              conversation,
              key,
              killAt: "subagent:after-child-abort-intent",
            });
            expectKilled(result);

            yield* withHost(
              site.db,
              Effect.gen(function* () {
                const host = yield* NodeDurableHost;
                const runtime = yield* DurableAgentRuntime;
                const started = yield* startedPayloadOf(conversation);
                // The replayed idempotent command returns the recorded intent unchanged
                // (DUR-012); recovery repairs the marker without a second command.
                const parentReport = host.startupRecovery.find(
                  (entry) => entry.submissionId === parentId,
                );
                expect(parentReport?.decision._tag).toBe("PropagateChildAbort");
                expect(parentReport?.disposition).toBe("repaired");
                const child = yield* submissionSnapshot(started.childSubmissionId);
                expect(child.state).toBe("settled");
                expect(child.settledOutcome).toBe("aborted");

                const second = yield* runtime.runRecovery;
                const wakeReport = second.find((entry) => entry.submissionId === parentId);
                expect(wakeReport?.decision._tag).toBe("ResumeWaitingParent");
                const settlements = yield* drive(conversation);
                expect(settlements.map((settlement) => settlement.outcome)).toEqual(["aborted"]);

                // One canonical child abort command across the killed pass and every replay.
                const childConversation = childConversationIdFor(parentId, DELEGATE_CALL);
                const childLog = yield* readLog(childConversation);
                const abortRecords = payloadsOf(childLog, "AbortRequested");
                expect(abortRecords).toHaveLength(1);
                const abortPayload = abortRecords[0]?.record.payload;
                if (abortPayload?._tag === "AbortRequested") {
                  expect(abortPayload.author).toBe("subagent-parent-abort");
                }
                const joined = payloadsOf(yield* readLog(conversation), "SubagentJoined");
                expect(joined).toHaveLength(1);
                expect(childModelInvocations(site.supplier)).toBe(0);
                yield* assertConvergence(conversation, [parentId], { site, counts: {} });
                yield* assertConvergence(childConversation, [started.childSubmissionId]);
              }),
            );
          }),
        ),
      30_000,
    );

    it.effect(
      "simultaneous SIGKILL of the parent and child workers converges on one link, one Settlement, one join",
      () =>
        withCrashSite((site) =>
          Effect.gen(function* () {
            const conversation = "conversation-s2-simultaneous";
            const key = "s2-simultaneous-1";
            const childMarker = `${site.marker}-child`;
            yield* Effect.scoped(
              Effect.gen(function* () {
                // Worker P blocks mid-establishment, AFTER the start link, BEFORE the
                // waitingForChild checkpoint — still holding the parent ownership lease.
                const parentWorker = yield* startWorker({
                  db: site.db,
                  scenario: "subagent-run",
                  conversation,
                  key,
                  blockAt: "subagent:after-start-append",
                  markerFile: site.marker,
                  leaseMillis: CHILD_LEASE_MS,
                  supplierDir: site.supplier,
                });
                yield* waitForFile(site.marker);
                // Worker C claims the ready child lane and blocks mid-child-model-Turn.
                const childWorker = yield* startWorker({
                  db: site.db,
                  scenario: "subagent-child",
                  conversation,
                  key,
                  childBlockFile: childMarker,
                  leaseMillis: CHILD_LEASE_MS,
                  supplierDir: site.supplier,
                });
                yield* waitForFile(childMarker);
                parentWorker.kill();
                childWorker.kill();
                const parentExit = yield* parentWorker.awaitExit;
                const childExit = yield* childWorker.awaitExit;
                expect(parentExit.signal).toBe("SIGKILL");
                expect(childExit.signal).toBe("SIGKILL");
              }),
            );
            yield* waitOutChildLease;

            const bindings = yield* restartBindings(site);
            const drive = driveWith(bindings);
            yield* withHost(
              site.db,
              Effect.gen(function* () {
                const host = yield* NodeDurableHost;
                const parent = yield* lookupByKey(conversation, key);
                const started = yield* startedPayloadOf(conversation);
                // Independent fenced recovery: the parent restores its waiting checkpoint
                // under its own fence, the child resumes from its own Turn boundary.
                const parentReport = host.startupRecovery.find(
                  (entry) => entry.submissionId === parent.submissionId,
                );
                expect(parentReport?.decision._tag).toBe("EnsureWaitingForChild");
                expect(parentReport?.disposition).toBe("repaired");
                const childReport = host.startupRecovery.find(
                  (entry) => entry.submissionId === started.childSubmissionId,
                );
                expect(childReport?.decision._tag).toBe("ResumeFromTurnBoundary");
                expect(childReport?.disposition).toBe("deferred");
                expect((yield* submissionSnapshot(parent.submissionId)).state).toBe("suspended");

                const childConversation = childConversationIdFor(
                  parent.submissionId,
                  DELEGATE_CALL,
                );
                const childSettlements = yield* drive(childConversation);
                expect(childSettlements.map((settlement) => settlement.outcome)).toEqual([
                  "completed",
                ]);
                const settlements = yield* drive(conversation);
                expect(settlements.map((settlement) => settlement.outcome)).toEqual(["completed"]);

                // One link, one child Settlement, one join (spec §14 simultaneous row); the
                // child's interrupted model invocation is honestly visible as count 2 with
                // exactly ONE committed child response.
                yield* assertOneEstablishedChild(conversation, childConversation);
                const childLog = yield* readLog(childConversation);
                expect(payloadsOf(childLog, "ModelResponseRecorded")).toHaveLength(1);
                expect(payloadsOf(childLog, "SubmissionSettled")).toHaveLength(1);
                expect(childModelInvocations(site.supplier)).toBe(2);
                yield* assertConvergence(conversation, [parent.submissionId], {
                  site,
                  counts: { [CHILD_MODEL_COUNT_KEY]: 2 },
                });
                yield* assertConvergence(childConversation, [started.childSubmissionId]);
              }),
            );
          }),
        ),
      30_000,
    );

    it.effect(
      "a stale parent resumed past its replacement is fenced out of the join and the child stays untouched",
      () =>
        withCrashSite((site) =>
          Effect.gen(function* () {
            const conversation = "conversation-s2-stale-join";
            const key = "s2-stale-join-1";
            yield* Effect.scoped(
              Effect.gen(function* () {
                // The stale worker establishes, runs the child, then blocks inside
                // `projectResult` — after verifying the settled child, BEFORE the join append —
                // still holding the parent ownership lease.
                const staleWorker = yield* startWorker({
                  db: site.db,
                  scenario: "subagent-run",
                  conversation,
                  key,
                  projectMarkerFile: site.marker,
                  projectReleaseFile: site.release,
                  leaseMillis: CHILD_LEASE_MS,
                  supplierDir: site.supplier,
                });
                yield* waitForFile(site.marker);
                yield* waitOutChildLease;
                expect(childModelInvocations(site.supplier)).toBe(1);

                // The replacement claims the parent lane at a higher epoch and completes the
                // join by recomputing the projection from canonical child output (spec §14).
                const bindings = yield* restartBindings(site);
                const drive = driveWith(bindings);
                const ids = yield* withHost(
                  site.db,
                  Effect.gen(function* () {
                    const parent = yield* lookupByKey(conversation, key);
                    const settlements = yield* drive(conversation);
                    expect(settlements.map((settlement) => settlement.outcome)).toEqual([
                      "completed",
                    ]);
                    const started = yield* startedPayloadOf(conversation);
                    return {
                      parent: parent.submissionId,
                      child: started.childSubmissionId,
                    };
                  }),
                );
                const childConversation = childConversationIdFor(ids.parent, DELEGATE_CALL);
                const childTailBefore = yield* withRuntime(site.db, readTail(childConversation));

                // Unblock the stale parent: its pending join append MUST be fenced.
                yield* touchFile(site.release);
                const exit = yield* staleWorker.awaitExit;
                expect(exit.code, `stderr: ${staleWorker.stderrText()}`).toBe(FENCED_EXIT_CODE);
                const failure = childMessages(staleWorker.stdoutText()).find(
                  (message) => message.kind === "worker-failure",
                );
                expect(
                  failure?.kind === "worker-failure" &&
                    ["FenceRejected", "OwnershipLost"].includes(failure.tag),
                  `expected a fenced stale join, got ${JSON.stringify(failure)}`,
                ).toBe(true);

                yield* withRuntime(
                  site.db,
                  Effect.gen(function* () {
                    // Vice-versa independent fencing: fencing the stale PARENT Attempt left
                    // the CHILD lane byte-identical — same tail digest, same epoch.
                    const childTailAfter = yield* readTail(childConversation);
                    expect(childTailAfter.tailDigest).toBe(childTailBefore.tailDigest);
                    expect(Number(childTailAfter.producerEpoch)).toBe(
                      Number(childTailBefore.producerEpoch),
                    );
                    // Exactly one join/result/settlement exists; the recomputed projection
                    // never re-executed the completed child.
                    yield* assertOneEstablishedChild(conversation, childConversation);
                    yield* assertDelegationSettled(conversation, ids.parent, {
                      isFailure: false,
                      result: { summary: PROJECTED_SUMMARY },
                    });
                    expect(yield* reservationStatuses(ids.parent)).toEqual(["released"]);
                    expect(childModelInvocations(site.supplier)).toBe(1);
                    yield* assertConvergence(conversation, [ids.parent], {
                      site,
                      counts: { [CHILD_MODEL_COUNT_KEY]: 1 },
                    });
                    yield* assertConvergence(childConversation, [ids.child]);
                  }),
                );
              }),
            );
          }),
        ),
      30_000,
    );

    it.effect(
      "fencing the child's stale Attempt leaves the waiting parent's lane and epoch untouched",
      () =>
        withCrashSite((site) =>
          Effect.gen(function* () {
            const conversation = "conversation-s2-child-fence";
            const key = "s2-child-fence-1";
            const bindings = yield* makeCrashSubagentBindings({
              supplierDir: site.supplier,
              parentScript: "delegate-then-final",
            });
            const drive = driveWith(bindings);

            // Establish in-process: the parent suspends waitingForChild.
            const parentId = yield* withRuntime(
              site.db,
              Effect.gen(function* () {
                yield* submitCoordinator(conversation, key);
                expect(yield* drive(conversation)).toHaveLength(0);
                const parent = yield* lookupByKey(conversation, key);
                expect(parent.state).toBe("suspended");
                return parent.submissionId;
              }),
            );
            const childConversation = childConversationIdFor(parentId, DELEGATE_CALL);
            const parentTailBefore = yield* withRuntime(site.db, readTail(conversation));
            const parentIdsBefore = yield* withRuntime(site.db, recordIdsOf(conversation));

            yield* Effect.scoped(
              Effect.gen(function* () {
                // The stale child worker blocks mid-child-Turn holding the child lease.
                const childWorker = yield* startWorker({
                  db: site.db,
                  scenario: "subagent-child",
                  conversation,
                  key,
                  childBlockFile: site.marker,
                  childReleaseFile: site.release,
                  leaseMillis: CHILD_LEASE_MS,
                  supplierDir: site.supplier,
                });
                yield* waitForFile(site.marker);
                yield* waitOutChildLease;

                // A fresh owner completes the child lane at a HIGHER child epoch.
                const childId = yield* withRuntime(
                  site.db,
                  Effect.gen(function* () {
                    const settlements = yield* drive(childConversation);
                    expect(settlements.map((settlement) => settlement.outcome)).toEqual([
                      "completed",
                    ]);
                    const started = yield* startedPayloadOf(conversation);
                    return started.childSubmissionId;
                  }),
                );
                expect(childModelInvocations(site.supplier)).toBe(2);

                // Unblock the stale child: its Turn commit is fenced by the child's OWN epoch;
                // it emits the STALE answer which must never become canonical.
                yield* touchFile(site.release);
                const exit = yield* childWorker.awaitExit;
                expect(exit.code, `stderr: ${childWorker.stderrText()}`).toBe(FENCED_EXIT_CODE);
                const failure = childMessages(childWorker.stdoutText()).find(
                  (message) => message.kind === "worker-failure",
                );
                expect(failure?.kind === "worker-failure" && failure.tag).toBe("FenceRejected");

                yield* withRuntime(
                  site.db,
                  Effect.gen(function* () {
                    // Independent fencing (spec §7 platform row): the child replacement never
                    // touched the parent's log or epoch — the parent's only transition is its
                    // own durable wake (suspended → input-applied).
                    const parentTailAfter = yield* readTail(conversation);
                    expect(parentTailAfter.tailDigest).toBe(parentTailBefore.tailDigest);
                    expect(Number(parentTailAfter.producerEpoch)).toBe(
                      Number(parentTailBefore.producerEpoch),
                    );
                    expect(yield* recordIdsOf(conversation)).toEqual(parentIdsBefore);
                    expect((yield* submissionSnapshot(parentId)).state).toBe("input-applied");
                    // Exactly one committed child response — the fresh Attempt's.
                    const childLog = yield* readLog(childConversation);
                    expect(payloadsOf(childLog, "ModelResponseRecorded")).toHaveLength(1);

                    // The woken parent joins the FRESH child answer, not the fenced stale one.
                    const settlements = yield* drive(conversation);
                    expect(settlements.map((settlement) => settlement.outcome)).toEqual([
                      "completed",
                    ]);
                    yield* assertDelegationSettled(conversation, parentId, {
                      isFailure: false,
                      result: { summary: PROJECTED_SUMMARY },
                    });
                    yield* assertConvergence(conversation, [parentId], {
                      site,
                      counts: { [CHILD_MODEL_COUNT_KEY]: 2 },
                    });
                    yield* assertConvergence(childConversation, [childId]);
                  }),
                );
              }),
            );
          }),
        ),
      30_000,
    );

    it.effect(
      "workerConcurrency=1: the suspension frees the single worker, the child runs, and the woken parent joins",
      () =>
        withCrashSite((site) =>
          Effect.gen(function* () {
            const conversation = "conversation-s2-smallest-pool";
            const key = "s2-smallest-pool-1";
            const bindings = yield* makeCrashSubagentBindings({
              supplierDir: site.supplier,
              parentScript: "delegate-then-final",
            });
            yield* withHost(
              site.db,
              Effect.gen(function* () {
                const host = yield* NodeDurableHost;
                const receipt = yield* host.submit(
                  coordinatorSubmitSlice,
                  { mission: CRASH_QUESTION },
                  crashSubmitOptions(conversation, key),
                );
                // The §12 smallest-pool proof: ONE resolved worker loop serves the parent AND
                // the child lane. Completion is only possible because waitingForChild released
                // the single worker permit (SUB-030) — a held permit would deadlock here.
                const settlement = yield* Effect.raceFirst(
                  host.awaitSettlement(receipt, CRASH_CALLER),
                  host.runResolvedWorkers.pipe(
                    Effect.andThen(
                      Effect.die(new Error("the resolved worker loop ended unexpectedly")),
                    ),
                  ),
                );
                expect(settlement.outcome).toBe("completed");

                const childConversation = childConversationIdFor(
                  receipt.submissionId,
                  DELEGATE_CALL,
                );
                const started = yield* assertOneEstablishedChild(conversation, childConversation);
                yield* assertDelegationSettled(conversation, receipt.submissionId, {
                  isFailure: false,
                  result: { summary: PROJECTED_SUMMARY },
                });
                expect(yield* reservationStatuses(receipt.submissionId)).toEqual(["released"]);
                expect(childModelInvocations(site.supplier)).toBe(1);
                yield* assertConvergence(conversation, [receipt.submissionId], {
                  site,
                  counts: { [CHILD_MODEL_COUNT_KEY]: 1 },
                });
                yield* assertConvergence(childConversation, [started.childSubmissionId]);
              }),
              {
                workerConcurrency: 1,
                bindings: [...bindings],
                wakeScanInterval: 200,
                settlementPollInterval: 50,
              },
            );
          }),
        ),
      30_000,
    );
  },
);

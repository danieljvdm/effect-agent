import * as fs from "node:fs";

import { Agent } from "@effect-agent/core";
import {
  ClaimRequest,
  DurableAgentRuntime,
  ObligationThresholds,
  ProducerId,
  ReleaseOwnershipRequest,
  RenewOwnershipRequest,
  ResolutionCompletedWithResult,
  SubmissionLedger,
  SubmissionLookupByKey,
  UnknownResolutionCommand,
  runIdForSubmission,
  toolCallPreparedRecordId,
  toolCallSettledRecordId,
} from "@effect-agent/thread";
import { NodeFileSystem } from "@effect/platform-node";
import { expect, layer } from "@effect/vitest";
import { Effect, Option, Schema } from "effect";

import { NodeDurableHost } from "../src/index.ts";
import {
  BOOK_CALL_ID,
  BOOK_REF,
  CHILD_ANSWER,
  FRESH_ANSWER,
  bookDefinition,
  bookTools,
  crashSubmitOptions,
  decodeThreadId,
  decodeToolCallId,
  finalParts,
  makeBookToolLayer,
  makeScriptedModel,
  plannerDefinition,
  supplierCount,
} from "./crash/fixtures.ts";
import {
  CHILD_LEASE_MS,
  expectKilled,
  failureTag,
  lookupByKey,
  readLog,
  runWorkerToExit,
  waitOutChildLease,
  withCrashSite,
  withHost,
  withRuntime,
  type CrashSite,
} from "./crash/harness.ts";

const POST_BACKUP_PRODUCER = Schema.decodeSync(ProducerId)("producer-post-backup");

/**
 * P7 WP4 restore drill (plan §5; DEPLOY-009's executable DN half): snapshot the SQLite file
 * MID-RUN (a Submission's external effect happened, its outcome never committed), keep the
 * original timeline running to settlement, then restore the snapshot into a fresh host and
 * assert the restore semantics the operations runbook documents:
 *
 * 1. work settled BEFORE the backup survives intact (integrity verified);
 * 2. ownership epochs minted AFTER the backup on the original timeline are FENCED by the
 *    restored store — a divergent-timeline token can never mutate restored state;
 * 3. external effects whose outcomes were only recorded after the backup surface through the
 *    Unknown/reconciliation regime — never assumed rolled back, never silently re-executed;
 * 4. admissions accepted after the backup do NOT exist in the restored store — the runbook's
 *    post-restore reconciliation duty, stated honestly rather than papered over.
 *
 * The DC (Durable Object) half of DEPLOY-009 is point-in-time recovery through hosted
 * Cloudflare APIs that Miniflare does not expose; it remains a documented manual runbook in
 * docs/guides/operations.md — recorded as scoped, never silently claimed.
 */

const SETTLED_LANE = "thread-restore-settled";
const SETTLED_KEY = "restore-settled-1";
const UNCERTAIN_LANE = "thread-restore-uncertain";
const UNCERTAIN_KEY = "restore-uncertain-1";
const POST_BACKUP_LANE = "thread-restore-post-backup";
const POST_BACKUP_KEY = "restore-post-backup-1";

const SQLITE_SUFFIXES = ["", "-wal", "-shm"] as const;

const copyDatabase = (from: string, to: string): void => {
  for (const suffix of SQLITE_SUFFIXES) {
    if (fs.existsSync(`${from}${suffix}`)) {
      fs.cpSync(`${from}${suffix}`, `${to}${suffix}`, { force: true });
    } else {
      fs.rmSync(`${to}${suffix}`, { force: true });
    }
  }
};

const drainPlanner = (thread: string) =>
  Effect.gen(function* () {
    const runtime = yield* DurableAgentRuntime;
    const model = yield* makeScriptedModel(() => finalParts(CHILD_ANSWER));
    const agent = Agent.withModel(plannerDefinition, model);
    return yield* runtime.processThread(agent, decodeThreadId(thread));
  });

const drainBook = (site: CrashSite, thread: string) =>
  Effect.gen(function* () {
    const runtime = yield* DurableAgentRuntime;
    const model = yield* makeScriptedModel(() => finalParts(FRESH_ANSWER));
    const agent = Agent.withModel(bookDefinition, model);
    return yield* runtime
      .processThread(agent, decodeThreadId(thread))
      .pipe(Effect.provide(makeBookToolLayer(site.supplier, bookTools)));
  });

layer(NodeFileSystem.layer, { excludeTestServices: true })(
  "DEPLOY-009/DUR-017 P7 restore drill (snapshot mid-run, restore into a fresh host)",
  (it) => {
    it.effect(
      "a restored snapshot fences post-backup epochs and surfaces post-backup external effects through the Unknown regime",
      () =>
        withCrashSite((site) =>
          Effect.gen(function* () {
            // ---------------------------------------------------------------
            // Phase 1 — pre-backup history: one lane settles completely.
            // ---------------------------------------------------------------
            yield* withHost(
              site.db,
              Effect.gen(function* () {
                const runtime = yield* DurableAgentRuntime;
                const model = yield* makeScriptedModel(() => finalParts(CHILD_ANSWER));
                const agent = Agent.withModel(plannerDefinition, model);
                yield* runtime.submit(
                  agent,
                  { question: "settle before the backup" },
                  crashSubmitOptions(SETTLED_LANE, SETTLED_KEY),
                );
                const settlements = yield* drainPlanner(SETTLED_LANE);
                expect(settlements[0]?.outcome).toBe("completed");
              }),
            );

            // ---------------------------------------------------------------
            // Phase 2 — mid-run state: a REAL worker process executes the
            // external booking, then dies at the results append. The external
            // effect exists; no canonical outcome does.
            // ---------------------------------------------------------------
            const killed = yield* runWorkerToExit({
              db: site.db,
              scenario: "run-uncertain",
              thread: UNCERTAIN_LANE,
              key: UNCERTAIN_KEY,
              killAtStorage: "append:before",
              killRequiresSupplier: `book:${BOOK_REF}`,
              leaseMillis: CHILD_LEASE_MS,
              supplierDir: site.supplier,
            });
            expectKilled(killed);
            yield* waitOutChildLease;
            expect(supplierCount(site.supplier, "book", BOOK_REF)).toBe(1);

            // ---------------------------------------------------------------
            // Phase 3 — BACKUP: file-level snapshot while no process holds the
            // database (the drill's stand-in for an online backup bookmark).
            // ---------------------------------------------------------------
            const backupDb = `${site.db}.backup`;
            copyDatabase(site.db, backupDb);

            // ---------------------------------------------------------------
            // Phase 4 — the ORIGINAL timeline keeps running: recovery marks the
            // uncertain call Unknown, the operator resolves it from supplier
            // truth, the lane settles, and a brand-new Submission is admitted
            // and settled — all AFTER the backup point.
            // ---------------------------------------------------------------
            const postBackupToken = yield* withHost(
              site.db,
              Effect.gen(function* () {
                const runtime = yield* DurableAgentRuntime;
                const ledger = yield* SubmissionLedger;
                const host = yield* NodeDurableHost;
                const snapshot = yield* lookupByKey(UNCERTAIN_LANE, UNCERTAIN_KEY);
                const report = host.startupRecovery.find(
                  (entry) => entry.submissionId === snapshot.submissionId,
                );
                expect(report?.decision._tag).toBe("MarkUnknown");
                expect(report?.disposition).toBe("unknown");

                yield* runtime.resolveUnknown(
                  UnknownResolutionCommand.make({
                    submissionId: snapshot.submissionId,
                    toolCallId: decodeToolCallId(BOOK_CALL_ID),
                    author: "operator",
                    reason: "the supplier store shows the booking (original timeline)",
                    resolution: ResolutionCompletedWithResult.make({
                      result: { confirmation: `booked-${BOOK_REF}-1` },
                      isFailure: false,
                    }),
                  }),
                );

                // Mint a POST-BACKUP ownership epoch on this timeline, then release it so the
                // lane still drains; the restored store must fence this exact token later.
                const claim = yield* ledger.claim(
                  ClaimRequest.make({
                    threadId: decodeThreadId(UNCERTAIN_LANE),
                    producerId: POST_BACKUP_PRODUCER,
                  }),
                );
                expect(Option.isSome(claim)).toBe(true);
                if (Option.isNone(claim)) throw new Error("Expected a post-backup claim");
                yield* ledger.releaseOwnership(
                  ReleaseOwnershipRequest.make({
                    submissionId: claim.value.submissionId,
                    ownershipToken: claim.value.ownershipToken,
                  }),
                );

                const settlements = yield* drainBook(site, UNCERTAIN_LANE);
                expect(settlements[0]?.outcome).toBe("completed");
                // The recovered result was applied WITHOUT re-executing the booking.
                expect(supplierCount(site.supplier, "book", BOOK_REF)).toBe(1);

                const postModel = yield* makeScriptedModel(() => finalParts(CHILD_ANSWER));
                const postAgent = Agent.withModel(plannerDefinition, postModel);
                yield* runtime.submit(
                  postAgent,
                  { question: "admitted after the backup" },
                  crashSubmitOptions(POST_BACKUP_LANE, POST_BACKUP_KEY),
                );
                const postSettlements = yield* drainPlanner(POST_BACKUP_LANE);
                expect(postSettlements[0]?.outcome).toBe("completed");

                return claim.value.ownershipToken;
              }),
            );

            // ---------------------------------------------------------------
            // Phase 5 — RESTORE the backup into a fresh host and assert the
            // documented restore semantics.
            // ---------------------------------------------------------------
            const restoredDb = `${site.db}.restored`;
            copyDatabase(backupDb, restoredDb);
            yield* withHost(
              restoredDb,
              Effect.gen(function* () {
                const runtime = yield* DurableAgentRuntime;
                const ledger = yield* SubmissionLedger;
                const host = yield* NodeDurableHost;
                const snapshot = yield* lookupByKey(UNCERTAIN_LANE, UNCERTAIN_KEY);
                const runId = runIdForSubmission(snapshot.submissionId);

                // (1) Pre-backup history survived intact.
                const settledLane = yield* runtime.verify(decodeThreadId(SETTLED_LANE));
                expect(settledLane.ok).toBe(true);

                // (3) The post-backup outcome is GONE; the external effect is not: startup
                // recovery re-enters the Unknown regime instead of assuming rollback or
                // replaying the call (supplier count stays exactly 1).
                const report = host.startupRecovery.find(
                  (entry) => entry.submissionId === snapshot.submissionId,
                );
                expect(report?.decision._tag).toBe("MarkUnknown");
                expect(report?.disposition).toBe("unknown");
                expect(supplierCount(site.supplier, "book", BOOK_REF)).toBe(1);
                const obligations = yield* runtime.scanObligations(
                  ObligationThresholds.make({ agingSeconds: 0, overdueSeconds: 0 }),
                );
                const entry = obligations.entries.find(
                  (candidate) => candidate.submissionId === snapshot.submissionId,
                );
                expect(entry?.blockedOn).toBe("unknown");

                // (2) The post-backup epoch from the ORIGINAL timeline is fenced: its token
                // cannot renew (or mutate) anything in the restored store.
                const renewExit = yield* Effect.exit(
                  ledger.renewOwnership(
                    RenewOwnershipRequest.make({
                      submissionId: snapshot.submissionId,
                      ownershipToken: postBackupToken,
                    }),
                  ),
                );
                expect(["OwnershipLost", "FenceRejected", "LedgerError"]).toContain(
                  failureTag(renewExit),
                );

                // (4) The post-backup admission does not exist here — the runbook's
                // reconciliation duty (Receipts issued after the backup point).
                const postBackupRow = yield* ledger.lookup(
                  SubmissionLookupByKey.make({
                    threadId: decodeThreadId(POST_BACKUP_LANE),
                    principal: crashSubmitOptions(POST_BACKUP_LANE, POST_BACKUP_KEY).principal,
                    idempotencyKey: crashSubmitOptions(POST_BACKUP_LANE, POST_BACKUP_KEY)
                      .idempotencyKey,
                  }),
                );
                expect(Option.isNone(postBackupRow)).toBe(true);

                // The restored lane completes through the SAME authorized DUR-017 path, from
                // supplier truth — still without re-executing the external call.
                yield* runtime.resolveUnknown(
                  UnknownResolutionCommand.make({
                    submissionId: snapshot.submissionId,
                    toolCallId: decodeToolCallId(BOOK_CALL_ID),
                    author: "operator",
                    reason: "the supplier store still shows the booking (restored timeline)",
                    resolution: ResolutionCompletedWithResult.make({
                      result: { confirmation: `booked-${BOOK_REF}-1` },
                      isFailure: false,
                    }),
                  }),
                );
                const settlements = yield* drainBook(site, UNCERTAIN_LANE);
                expect(settlements[0]?.outcome).toBe("completed");
                expect(supplierCount(site.supplier, "book", BOOK_REF)).toBe(1);
                // The restored lane's canonical history carries the full prepared → resolved →
                // settled trail and passes the shared integrity checks.
                const verifyReport = yield* runtime.verify(decodeThreadId(UNCERTAIN_LANE));
                expect(
                  verifyReport.ok,
                  `restored integrity report: ${JSON.stringify(verifyReport.checks)}`,
                ).toBe(true);
                const recordIds = (yield* readLog(UNCERTAIN_LANE)).map(
                  (envelope) => envelope.record.recordId,
                );
                expect(recordIds).toContain(
                  toolCallPreparedRecordId(runId, 1, decodeToolCallId(BOOK_CALL_ID)),
                );
                expect(recordIds).toContain(
                  toolCallSettledRecordId(runId, 1, decodeToolCallId(BOOK_CALL_ID)),
                );
              }),
            );

            // The ORIGINAL timeline is untouched by the drill: its post-backup Submission is
            // still settled on the original file (the two timelines have diverged, which is
            // exactly what the runbook's reconciliation section warns about).
            yield* withRuntime(
              site.db,
              Effect.gen(function* () {
                const original = yield* lookupByKey(POST_BACKUP_LANE, POST_BACKUP_KEY);
                expect(original.state).toBe("settled");
              }),
            );
          }),
        ),
      60_000,
    );
  },
);

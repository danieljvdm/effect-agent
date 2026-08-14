import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import * as v8 from "node:v8";
import * as vm from "node:vm";

import { ToolCallId } from "@effect-agent/core";
import {
  DurableAgentRuntime,
  ObligationThresholds,
  SubmissionLedger,
  SubmissionLookupById,
  childConversationIdFor,
  type Receipt,
} from "@effect-agent/session";
import { NodeFileSystem } from "@effect/platform-node";
import { expect, layer } from "@effect/vitest";
import { Duration, Effect, FileSystem, Option, Schema, Stream, type Scope } from "effect";

import { NodeDurableHost } from "../../src/index.ts";
import { packageRoot, waitUntil, withHost, withRuntime } from "../crash/harness.ts";
import {
  SOAK_DELEGATE_CALL_ID,
  SOAK_KILL_EXIT_CODE,
  SoakEnv,
  decodeConversationId,
  soakCoordinatorSubmitSlice,
  soakPlannerSubmitSlice,
  soakSubmitOptions,
} from "./soak-fixtures.ts";

/**
 * P7 WP4 DN soak (plan §5): 500 Submissions across 20 lanes over ONE SQLite file, drained by
 * real worker processes that the harness SIGKILLs on a seeded schedule and replaces (each
 * incarnation is a new producer identity, so every kill exercises epoch supersession). Plain
 * multi-Submission lanes keep the joining/joined machinery hot; delegation lanes run the S2
 * establish → child → join protocol; the final host asserts convergence through the shared
 * admin surface (`verify`, `scanObligations`) plus the resource claims: heap stability across
 * forced-GC windows, the active-handle count back at baseline, no orphaned child processes,
 * and a clean database close.
 *
 * Lane placement: this suite runs in the same lane as the process-kill crash tests — today
 * that is the ordinary per-package `vp test` gate (`bun run ready`); testing.md §13 assigns
 * crash/soak suites to the release-candidate gate as CI matures. Budget ≤ 5 minutes
 * (`SOAK_BUDGET_MS`); see docs/guides/operations.md.
 */

const PLAIN_LANES = 16;
const PLAIN_PER_LANE = 30; // 480 plain submissions (joins mixed in by queue depth)
const DELEGATION_LANES = 4;
const DELEGATION_PER_LANE = 5; // 20 delegation submissions → 20 child Conversations
const TOTAL_SUBMISSIONS = PLAIN_LANES * PLAIN_PER_LANE + DELEGATION_LANES * DELEGATION_PER_LANE;
const KILLS = 6;
const SOAK_BUDGET_MS = 300_000;
const SOAK_SEED = 20260813;

const soakWorkerEntry = fileURLToPath(new URL("./soak-worker-entry.ts", import.meta.url));
const decodeToolCallId = Schema.decodeSync(ToolCallId);

const mulberry32 = (seed: number): (() => number) => {
  let state = seed | 0;
  return () => {
    state = (state + 0x6d2b79f5) | 0;
    let t = Math.imul(state ^ (state >>> 15), 1 | state);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
};

/** Forced full GC through the documented v8 flag toggle (no --expose-gc launch flag). */
const heapAfterGc = (): number => {
  v8.setFlagsFromString("--expose-gc");
  const gc = vm.runInNewContext("gc") as () => void;
  gc();
  gc();
  v8.setFlagsFromString("--no-expose-gc");
  return process.memoryUsage().heapUsed;
};

interface SoakWorker {
  readonly pid: number | undefined;
  readonly kill: () => void;
  readonly exited: () => { code: number | null; signal: NodeJS.Signals | null } | undefined;
  readonly stderrText: () => string;
}

const spawnedPids: Array<number> = [];

/** Spawn one soak worker over the shared file; the Scope guarantees no child outlives us. */
const startSoakWorker = (
  db: string,
  producer: string,
): Effect.Effect<SoakWorker, never, Scope.Scope> =>
  Effect.acquireRelease(
    Effect.sync(() => {
      const child = spawn(process.execPath, ["--import", "tsx", soakWorkerEntry], {
        cwd: packageRoot,
        env: {
          ...process.env,
          [SoakEnv.database]: db,
          [SoakEnv.producer]: producer,
          [SoakEnv.leaseMillis]: "750",
        },
        stdio: ["ignore", "ignore", "pipe"],
      });
      if (child.pid !== undefined) spawnedPids.push(child.pid);
      let stderr = "";
      let exited: { code: number | null; signal: NodeJS.Signals | null } | undefined;
      child.stderr.setEncoding("utf8");
      child.stderr.on("data", (chunk: string) => {
        stderr += chunk;
      });
      child.once("exit", (code, signal) => {
        exited = { code, signal };
      });
      const worker: SoakWorker = {
        pid: child.pid,
        kill: () => {
          child.kill("SIGKILL");
        },
        exited: () => exited,
        stderrText: () => stderr,
      };
      return worker;
    }),
    (worker) => Effect.sync(() => worker.kill()),
  );

const awaitWorkerExit = (worker: SoakWorker) =>
  waitUntil(
    () => `soak worker exit (stderr: ${worker.stderrText()})`,
    Effect.sync(() => {
      const exit = worker.exited();
      return exit === undefined ? Option.none() : Option.some(exit);
    }),
    20_000,
  );

const plainLane = (lane: number): string => `soak-plain-${lane}`;
const delegationLane = (lane: number): string => `soak-delegation-${lane}`;

/** Submit the full population through the client-only DN stack (no worker in-process). */
const submitAll = (db: string) =>
  withRuntime(
    db,
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const receipts: Array<Receipt> = [];
      const delegationReceipts: Array<Receipt> = [];
      for (let lane = 0; lane < PLAIN_LANES; lane++) {
        for (let member = 0; member < PLAIN_PER_LANE; member++) {
          receipts.push(
            yield* runtime.submit(
              soakPlannerSubmitSlice,
              { question: `soak ${lane}/${member}` },
              soakSubmitOptions(plainLane(lane), `soak-p${lane}-${member}`),
            ),
          );
        }
      }
      for (let lane = 0; lane < DELEGATION_LANES; lane++) {
        for (let member = 0; member < DELEGATION_PER_LANE; member++) {
          const receipt = yield* runtime.submit(
            soakCoordinatorSubmitSlice,
            { mission: `soak ${lane}/${member}` },
            soakSubmitOptions(delegationLane(lane), `soak-d${lane}-${member}`),
          );
          receipts.push(receipt);
          delegationReceipts.push(receipt);
        }
      }
      return { receipts, delegationReceipts };
    }),
  );

/** Real-clock convergence poll through a client-only stack (workers do all the work). */
const awaitConvergence = (db: string, budgetMillis: number) =>
  withRuntime(
    db,
    Effect.gen(function* () {
      const ledger = yield* SubmissionLedger;
      yield* waitUntil(
        () => "soak convergence (all lanes settled)",
        Effect.gen(function* () {
          const nonterminal = yield* Stream.runCollect(ledger.scanNonterminal).pipe(Effect.orDie);
          return Array.from(nonterminal).length === 0 ? Option.some(undefined) : Option.none();
        }),
        budgetMillis,
      );
    }),
  );

layer(NodeFileSystem.layer, { excludeTestServices: true })(
  "DUR-001/DUR-016/SUB-030 P7 DN soak (real worker kills over one SQLite file)",
  (it) => {
    it.effect(
      `SOAK: ${TOTAL_SUBMISSIONS} submissions across ${PLAIN_LANES + DELEGATION_LANES} lanes converge under ${KILLS} seeded worker kills with stable heap, baseline handles, no orphans, and a clean close`,
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const fileSystem = yield* FileSystem.FileSystem;
            const directory = yield* fileSystem.makeTempDirectoryScoped({
              prefix: "effect-agent-soak-",
            });
            const db = `${directory}/soak.sqlite`;
            const random = mulberry32(SOAK_SEED);
            const handleBaseline = process.getActiveResourcesInfo().length;
            const heapBaseline = heapAfterGc();

            // One live worker at a time — the documented DN deployment shape is one process
            // owner per database file (durability §6). Each seeded kill SIGKILLs the current
            // incarnation and starts a fresh producer identity, so every kill exercises real
            // crash recovery plus epoch supersession without inventing a multi-owner topology
            // the deployment does not claim.
            let workerSerial = 0;
            const spawnNext = () => {
              workerSerial += 1;
              return startSoakWorker(db, `producer-soak-w${workerSerial}`);
            };
            let worker = yield* spawnNext();

            const { receipts, delegationReceipts } = yield* submitAll(db);
            expect(receipts).toHaveLength(TOTAL_SUBMISSIONS);

            for (let kill = 0; kill < KILLS; kill++) {
              yield* Effect.sleep(Duration.millis(800 + Math.floor(random() * 1_500)));
              worker.kill();
              const exit = yield* awaitWorkerExit(worker);
              // A SIGKILLed worker never exits cleanly — the kill was a real crash.
              expect(
                exit.signal === "SIGKILL" || exit.code === SOAK_KILL_EXIT_CODE,
                `expected a killed worker, got ${JSON.stringify(exit)}`,
              ).toBe(true);
              worker = yield* spawnNext();
            }

            yield* awaitConvergence(db, SOAK_BUDGET_MS - 60_000);
            const heapAfterRun = heapAfterGc();

            // Stop the surviving worker (SIGKILL — settled work needs no drain), then verify
            // through a FRESH host whose startup recovery sees a fully settled store.
            worker.kill();
            yield* awaitWorkerExit(worker);

            yield* withHost(
              db,
              Effect.gen(function* () {
                const host = yield* NodeDurableHost;
                const ledger = yield* SubmissionLedger;
                // Accepted-work contract: every Receipt settled, none disappeared.
                for (const receipt of receipts) {
                  const row = yield* ledger.lookup(
                    SubmissionLookupById.make({ submissionId: receipt.submissionId }),
                  );
                  expect(Option.isSome(row)).toBe(true);
                  if (Option.isSome(row)) expect(row.value.state).toBe("settled");
                }
                // The shared integrity claims for every parent lane and child Conversation.
                const conversations = [
                  ...Array.from({ length: PLAIN_LANES }, (_, lane) => plainLane(lane)),
                  ...Array.from({ length: DELEGATION_LANES }, (_, lane) => delegationLane(lane)),
                ].map((name) => decodeConversationId(name));
                for (const conversationId of conversations) {
                  const report = yield* host.verify(conversationId);
                  expect(
                    report.ok,
                    `integrity report for ${conversationId}: ${JSON.stringify(report.checks)}`,
                  ).toBe(true);
                }
                // Delegation receipts that were JOINED into a host Run settle with it and never
                // own a child Conversation (DUR-016), so only materialized children are
                // verified — and every delegation lane's host Run established at least one.
                let verifiedChildren = 0;
                for (const receipt of delegationReceipts) {
                  const child = childConversationIdFor(
                    receipt.submissionId,
                    decodeToolCallId(SOAK_DELEGATE_CALL_ID),
                  );
                  const report = yield* host.verify(child).pipe(
                    Effect.map(Option.some),
                    Effect.catchTag("ConversationNotMaterialized", () =>
                      Effect.succeed(Option.none()),
                    ),
                  );
                  if (Option.isSome(report)) {
                    expect(
                      report.value.ok,
                      `integrity report for child ${child}: ${JSON.stringify(report.value.checks)}`,
                    ).toBe(true);
                    verifiedChildren += 1;
                  }
                }
                expect(verifiedChildren).toBeGreaterThanOrEqual(DELEGATION_LANES);
                // DUR-017 surface agrees: nothing is aged, blocked, or invisibly stuck.
                const obligations = yield* host.scanObligations(
                  ObligationThresholds.make({ agingSeconds: 0, overdueSeconds: 0 }),
                );
                expect(obligations.entries).toHaveLength(0);
              }),
            );

            // Clean close: the host scope above released the database; a fresh client stack
            // reopens the same file and still sees a fully settled ledger.
            yield* withRuntime(
              db,
              Effect.gen(function* () {
                const ledger = yield* SubmissionLedger;
                const nonterminal = yield* Stream.runCollect(ledger.scanNonterminal);
                expect(Array.from(nonterminal)).toHaveLength(0);
              }),
            );

            // No orphaned children: every spawned worker pid is gone.
            for (const pid of spawnedPids) {
              const alive = ((): boolean => {
                try {
                  process.kill(pid, 0);
                  return true;
                } catch {
                  return false;
                }
              })();
              expect(alive, `worker pid ${pid} is still alive`).toBe(false);
            }

            // Resource stability after forced GC: the run and its teardown return the process
            // near its baselines (bounds are lenient against allocator noise, strict against
            // leaks proportional to 500 submissions).
            const heapFinal = heapAfterGc();
            expect(heapFinal).toBeLessThanOrEqual(heapBaseline + 48 * 1024 * 1024);
            expect(heapFinal).toBeLessThanOrEqual(heapAfterRun + 16 * 1024 * 1024);
            const handleFinal = process.getActiveResourcesInfo().length;
            expect(handleFinal).toBeLessThanOrEqual(handleBaseline + 8);
          }),
        ),
      SOAK_BUDGET_MS,
    );
  },
);

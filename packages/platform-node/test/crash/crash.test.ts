import { spawn } from "node:child_process";
import * as fs from "node:fs";
import { fileURLToPath } from "node:url";

import type { SubmissionId } from "@effect-agent/core";
import {
  AbortCommand,
  ClaimRequest,
  ConversationRead,
  ConversationStore,
  DurableAgentRuntime,
  ProducerId,
  RecoverySnapshotRequest,
  ReleaseOwnershipRequest,
  SubmissionLedger,
  SubmissionLookupById,
  SubmissionLookupByKey,
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
  type DurableRuntimeFailpointLocation,
  type SubmissionSnapshot,
} from "@effect-agent/session";
import type { SqliteStorageFailpointLocation } from "@effect-agent/storage-sqlite";
import { NodeFileSystem } from "@effect/platform-node";
import { expect, layer } from "@effect/vitest";
import {
  Cause,
  Duration,
  Effect,
  Exit,
  FileSystem,
  Option,
  Schema,
  Stream,
  type Scope,
} from "effect";
import { Prompt } from "effect/unstable/ai";

import { Agent } from "@effect-agent/core";
import {
  NodeDurableHost,
  NodeDurableRuntime,
  type NodeDurableRuntimeOptions,
} from "../../src/index.ts";
import {
  BOOK_CALL_ID,
  BOOK_REF,
  CHILD_ANSWER,
  CRASH_DEPLOYMENT_ID,
  CRASH_QUESTION,
  CrashEnv,
  FENCED_EXIT_CODE,
  FRESH_ANSWER,
  HOST_PRODUCER_ID,
  ITINERARY_CALL_ID,
  JOIN_QUESTION,
  KILL_EXIT_CODE,
  STEP_REF,
  approvalDefinition,
  approvalTools,
  bookDefinition,
  bookIdempotentDefinition,
  bookIdempotentTools,
  bookTools,
  crashSubmitOptions,
  decodeChildMessageOption,
  decodeConversationId,
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
  supplierCounts,
  supplierReconcilerLayer,
  supplierValues,
  type ChildMessage,
  type CrashScenario,
} from "./fixtures.ts";

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

const workerEntry = fileURLToPath(new URL("./worker-entry.ts", import.meta.url));
const packageRoot = fileURLToPath(new URL("../..", import.meta.url));

/** Lease the child claims with in kill scenarios; expired by `waitOutChildLease` after death. */
const CHILD_LEASE_MS = 250;
const LEASE_WAIT_MS = 1_000;
const POLL_INTERVAL_MS = 25;

interface WorkerExit {
  readonly code: number | null;
  readonly signal: NodeJS.Signals | null;
}

interface WorkerHandle {
  readonly stdoutText: () => string;
  readonly stderrText: () => string;
  readonly kill: () => void;
  readonly awaitExit: Effect.Effect<WorkerExit>;
}

/** Bounded real-clock polling; dies with the label (and context) instead of hanging the suite. */
const waitUntil = <A>(
  label: () => string,
  poll: Effect.Effect<Option.Option<A>>,
  timeoutMillis = 15_000,
): Effect.Effect<A> =>
  Effect.gen(function* () {
    const rounds = Math.ceil(timeoutMillis / POLL_INTERVAL_MS);
    for (let round = 0; round < rounds; round++) {
      const result = yield* poll;
      if (Option.isSome(result)) return result.value;
      yield* Effect.sleep(Duration.millis(POLL_INTERVAL_MS));
    }
    return yield* Effect.die(new Error(`Timed out waiting for ${label()}`));
  });

const waitForFile = (path: string): Effect.Effect<void> =>
  waitUntil(
    () => `file ${path}`,
    Effect.sync(() => (fs.existsSync(path) ? Option.some(undefined) : Option.none())),
  );

const touchFile = (path: string): Effect.Effect<void> =>
  Effect.sync(() => {
    fs.writeFileSync(path, "1");
  });

interface ChildOptions {
  readonly db: string;
  readonly scenario: CrashScenario;
  readonly conversation: string;
  readonly key: string;
  readonly killAt?: DurableRuntimeFailpointLocation | undefined;
  readonly killAtStorage?: SqliteStorageFailpointLocation | undefined;
  readonly leaseMillis?: number | undefined;
  readonly markerFile?: string | undefined;
  readonly releaseFile?: string | undefined;
  readonly supplierDir?: string | undefined;
  /** `{op}:{key}` gate: the armed kill fires only once the supplier store holds this call. */
  readonly killRequiresSupplier?: string | undefined;
  readonly decision?: "approved" | "denied" | undefined;
}

const childEnv = (options: ChildOptions): Record<string, string> => {
  const env: Record<string, string> = {
    [CrashEnv.database]: options.db,
    [CrashEnv.scenario]: options.scenario,
    [CrashEnv.conversation]: options.conversation,
    [CrashEnv.idempotencyKey]: options.key,
  };
  if (options.killAt !== undefined) env[CrashEnv.killAt] = options.killAt;
  if (options.killAtStorage !== undefined) env[CrashEnv.killAtStorage] = options.killAtStorage;
  if (options.leaseMillis !== undefined) env[CrashEnv.leaseMillis] = String(options.leaseMillis);
  if (options.markerFile !== undefined) env[CrashEnv.markerFile] = options.markerFile;
  if (options.releaseFile !== undefined) env[CrashEnv.releaseFile] = options.releaseFile;
  if (options.supplierDir !== undefined) env[CrashEnv.supplierDir] = options.supplierDir;
  if (options.killRequiresSupplier !== undefined) {
    env[CrashEnv.killRequiresSupplier] = options.killRequiresSupplier;
  }
  if (options.decision !== undefined) env[CrashEnv.decision] = options.decision;
  return env;
};

/** Spawn one worker child process; the Scope guarantees no child outlives its test. */
const startWorker = (options: ChildOptions): Effect.Effect<WorkerHandle, never, Scope.Scope> =>
  Effect.acquireRelease(
    Effect.sync(() => {
      const child = spawn(process.execPath, ["--import", "tsx", workerEntry], {
        cwd: packageRoot,
        env: { ...process.env, ...childEnv(options) },
        stdio: ["ignore", "pipe", "pipe"],
      });
      let stdout = "";
      let stderr = "";
      let exited: WorkerExit | undefined;
      child.stdout.setEncoding("utf8");
      child.stderr.setEncoding("utf8");
      child.stdout.on("data", (chunk: string) => {
        stdout += chunk;
      });
      child.stderr.on("data", (chunk: string) => {
        stderr += chunk;
      });
      child.once("exit", (code, signal) => {
        exited = { code, signal };
      });
      const handle: WorkerHandle = {
        stdoutText: () => stdout,
        stderrText: () => stderr,
        kill: () => {
          child.kill("SIGKILL");
        },
        awaitExit: waitUntil(
          () => `worker exit (stderr: ${stderr})`,
          Effect.sync(() => (exited === undefined ? Option.none() : Option.some(exited))),
          20_000,
        ),
      };
      return handle;
    }),
    (handle) => Effect.sync(() => handle.kill()),
  );

interface WorkerResult {
  readonly exit: WorkerExit;
  readonly stdout: string;
  readonly stderr: string;
}

const runWorkerToExit = (options: ChildOptions): Effect.Effect<WorkerResult> =>
  Effect.scoped(
    Effect.gen(function* () {
      const handle = yield* startWorker(options);
      const exit = yield* handle.awaitExit;
      return { exit, stdout: handle.stdoutText(), stderr: handle.stderrText() };
    }),
  );

const childMessages = (stdout: string): ReadonlyArray<ChildMessage> =>
  stdout
    .split("\n")
    .filter((line) => line.trim().length > 0)
    .flatMap((line) => {
      try {
        const parsed: unknown = JSON.parse(line);
        return Option.match(decodeChildMessageOption(parsed), {
          onNone: () => [],
          onSome: (message) => [message],
        });
      } catch {
        return [];
      }
    });

const expectKilled = (result: WorkerResult): void => {
  expect(
    result.exit.code === KILL_EXIT_CODE || result.exit.signal === "SIGKILL",
    `expected a killed worker, got ${JSON.stringify(result.exit)}; stderr: ${result.stderr}`,
  ).toBe(true);
};

/** The dead child's short lease must lapse before a restarted owner can claim its lane. */
const waitOutChildLease = Effect.sleep(Duration.millis(LEASE_WAIT_MS));

const runtimeOptions = (
  filename: string,
  overrides?: Partial<NodeDurableRuntimeOptions>,
): NodeDurableRuntimeOptions => ({
  filename,
  deploymentId: CRASH_DEPLOYMENT_ID,
  producerId: HOST_PRODUCER_ID,
  settlementPollInterval: 50,
  abortPollInterval: 50,
  wakeScanInterval: 1_000,
  ...overrides,
});

/** One restarted host "process": startup recovery runs before the effect observes anything. */
const withHost = <A, E, R>(
  db: string,
  effect: Effect.Effect<A, E, R>,
  overrides?: Partial<NodeDurableRuntimeOptions>,
) => Effect.provide(effect, NodeDurableHost.layerStack(runtimeOptions(db, overrides)));

/** A restarted "client-only" process: the DN stack WITHOUT the host recovery gate. */
const withRuntime = <A, E, R>(
  db: string,
  effect: Effect.Effect<A, E, R>,
  overrides?: Partial<NodeDurableRuntimeOptions>,
) => Effect.provide(effect, NodeDurableRuntime.layer(runtimeOptions(db, overrides)));

interface CrashSite {
  readonly db: string;
  readonly marker: string;
  readonly release: string;
  /** File-backed external supplier store shared with the child processes (plan §4.3). */
  readonly supplier: string;
}

const withCrashSite = <A, E, R>(use: (site: CrashSite) => Effect.Effect<A, E, R>) =>
  Effect.scoped(
    Effect.gen(function* () {
      const fileSystem = yield* FileSystem.FileSystem;
      const directory = yield* fileSystem.makeTempDirectoryScoped({
        prefix: "effect-agent-crash-",
      });
      return yield* use({
        db: `${directory}/crash.sqlite`,
        marker: `${directory}/marker`,
        release: `${directory}/release`,
        supplier: `${directory}/supplier`,
      });
    }),
  );

const failureTag = <A, E>(exit: Exit.Exit<A, E>): string => {
  expect(Exit.isFailure(exit)).toBe(true);
  if (Exit.isSuccess(exit)) throw new Error("Expected the Effect to fail");
  const failure = Cause.findErrorOption(exit.cause);
  expect(Option.isSome(failure)).toBe(true);
  if (Option.isNone(failure)) throw new Error("Expected a typed failure");
  const error: unknown = failure.value;
  return typeof error === "object" && error !== null && "_tag" in error
    ? String(error._tag)
    : "unknown";
};

const decodeProducerId = Schema.decodeSync(ProducerId);

const readLog = (conversation: string) =>
  Effect.gen(function* () {
    const store = yield* ConversationStore;
    return yield* Stream.runCollect(
      store.read(
        ConversationRead.make({
          conversationId: decodeConversationId(conversation),
          limit: 1_024,
        }),
      ),
    );
  });

const logTags = (records: ReadonlyArray<CanonicalRecordEnvelope>): ReadonlyArray<string> =>
  records.map((envelope) => envelope.record.payload._tag);

const lookupByKey = (conversation: string, key: string) =>
  Effect.gen(function* () {
    const ledger = yield* SubmissionLedger;
    const snapshot = yield* ledger.lookup(
      SubmissionLookupByKey.make({
        conversationId: decodeConversationId(conversation),
        principal: crashSubmitOptions(conversation, key).principal,
        idempotencyKey: crashSubmitOptions(conversation, key).idempotencyKey,
      }),
    );
    expect(Option.isSome(snapshot)).toBe(true);
    if (Option.isNone(snapshot)) throw new Error(`Expected an accepted Submission for ${key}`);
    return snapshot.value;
  });

const lookupState = (submissionId: SubmissionId) =>
  Effect.gen(function* () {
    const ledger = yield* SubmissionLedger;
    const snapshot = yield* ledger.lookup(SubmissionLookupById.make({ submissionId }));
    expect(Option.isSome(snapshot)).toBe(true);
    if (Option.isNone(snapshot)) throw new Error("Expected the Submission to exist");
    return snapshot.value.state;
  });

/** Same-key client reattachment: resubmitting the identical payload resumes the Receipt. */
const resubmit = (conversation: string, key: string) =>
  Effect.gen(function* () {
    const runtime = yield* DurableAgentRuntime;
    return yield* runtime.submit(
      { definition: plannerDefinition },
      { question: CRASH_QUESTION },
      crashSubmitOptions(conversation, key),
    );
  });

const drainPlanner = (conversation: string, answer: string) =>
  Effect.gen(function* () {
    const runtime = yield* DurableAgentRuntime;
    const model = yield* makeScriptedModel(() => finalParts(answer));
    const agent = Agent.withModel(plannerDefinition, model);
    return yield* runtime.processConversation(agent, decodeConversationId(conversation));
  });

const drainSearch = (conversation: string, answer: string) =>
  Effect.gen(function* () {
    const runtime = yield* DurableAgentRuntime;
    const model = yield* makeScriptedModel(() => finalParts(answer));
    const agent = Agent.withModel(searchDefinition, model);
    return yield* runtime
      .processConversation(agent, decodeConversationId(conversation))
      .pipe(Effect.provide(searchToolLayer));
  });

const drainUncertainBook = (site: CrashSite, conversation: string, answer: string) =>
  Effect.gen(function* () {
    const runtime = yield* DurableAgentRuntime;
    const model = yield* makeScriptedModel(() => finalParts(answer));
    const agent = Agent.withModel(bookDefinition, model);
    return yield* runtime
      .processConversation(agent, decodeConversationId(conversation))
      .pipe(Effect.provide(makeBookToolLayer(site.supplier, bookTools)));
  });

const drainIdempotentBook = (site: CrashSite, conversation: string, answer: string) =>
  Effect.gen(function* () {
    const runtime = yield* DurableAgentRuntime;
    const model = yield* makeScriptedModel(() => finalParts(answer));
    const agent = Agent.withModel(bookIdempotentDefinition, model);
    return yield* runtime
      .processConversation(agent, decodeConversationId(conversation))
      .pipe(Effect.provide(makeBookToolLayer(site.supplier, bookIdempotentTools)));
  });

const drainApprovalBook = (site: CrashSite, conversation: string, answer: string) =>
  Effect.gen(function* () {
    const runtime = yield* DurableAgentRuntime;
    const model = yield* makeScriptedModel(() => finalParts(answer));
    const agent = Agent.withModel(approvalDefinition, model);
    return yield* runtime
      .processConversation(agent, decodeConversationId(conversation))
      .pipe(Effect.provide(makeBookToolLayer(site.supplier, approvalTools)));
  });

const drainItinerary = (site: CrashSite, conversation: string, answer: string) =>
  Effect.gen(function* () {
    const runtime = yield* DurableAgentRuntime;
    const model = yield* makeScriptedModel(() => finalParts(answer));
    const agent = Agent.withModel(itineraryDefinition, model);
    return yield* runtime
      .processConversation(agent, decodeConversationId(conversation))
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
interface SupplierExpectation {
  readonly site: CrashSite;
  /** EXACT per-call invocation counts (`{op}:{key}` → n); `{}` claims no external call ever. */
  readonly counts: Record<string, number>;
}

const BookResult = Schema.Struct({ confirmation: Schema.String });
const ItineraryResult = Schema.Struct({ state: Schema.String });
const decodeBookResult = Schema.decodeUnknownOption(BookResult);
const decodeItineraryResult = Schema.decodeUnknownOption(ItineraryResult);
const decodeStepOutput = Schema.decodeUnknownOption(Schema.String);

/**
 * Never-fabricate (durability §10: "the engine must not manufacture a result and continue"):
 * every canonical success for a supplier-backed Tool must be a value the external store actually
 * produced, and the per-call invocation counts must match the row's honesty claim exactly — an
 * at-least-once duplicate is asserted as 2, never hidden; a recovered result is asserted as 1.
 */
const assertSupplierHonesty = (
  records: ReadonlyArray<CanonicalRecordEnvelope>,
  supplier: SupplierExpectation,
): void => {
  const produced = supplierValues(supplier.site.supplier);
  const requireProduced = (value: string, label: string): void => {
    expect(
      produced.has(value),
      `${label} "${value}" is absent from the supplier store — a fabricated result`,
    ).toBe(true);
  };
  for (const envelope of records) {
    const payload = envelope.record.payload;
    if (payload._tag === "ToolCallSettled" && !payload.isFailure) {
      if (payload.toolName === "book") {
        const result = decodeBookResult(payload.result);
        expect(Option.isSome(result)).toBe(true);
        if (Option.isSome(result)) requireProduced(result.value.confirmation, "book result");
      }
      if (payload.toolName === "itinerary") {
        const result = decodeItineraryResult(payload.result);
        expect(Option.isSome(result)).toBe(true);
        if (Option.isSome(result)) {
          for (const part of result.value.state.split("+")) {
            requireProduced(part, "itinerary step result");
          }
        }
      }
    }
    if (payload._tag === "ToolStepSettled") {
      const output = decodeStepOutput(payload.output);
      expect(Option.isSome(output)).toBe(true);
      if (Option.isSome(output)) requireProduced(output.value, "step output");
    }
  }
  expect(supplierCounts(supplier.site.supplier)).toEqual(supplier.counts);
};

/**
 * The convergence property asserted after every kill (plan §Crash matrix): every accepted
 * Submission still exists and is settled; each has EXACTLY one canonical terminal outcome; no
 * canonical record identity was ever double-appended (stale epochs fenced by DUR-006/DUR-007);
 * canonical inputs + settlements follow the admitted FIFO queue sequence (DUR-004); and, when
 * the row involves the external supplier, no recorded Tool result was fabricated and every
 * invocation count matches the row's honesty claim.
 */
const assertConvergence = (
  conversation: string,
  submissionIds: ReadonlyArray<SubmissionId>,
  supplier?: SupplierExpectation,
) =>
  Effect.gen(function* () {
    const ledger = yield* SubmissionLedger;
    const snapshots: Array<SubmissionSnapshot> = [];
    for (const submissionId of submissionIds) {
      const snapshot = yield* ledger.lookup(SubmissionLookupById.make({ submissionId }));
      // No accepted Submission disappears (Accepted-work Contract).
      expect(Option.isSome(snapshot)).toBe(true);
      if (Option.isNone(snapshot)) throw new Error("Expected the Submission to exist");
      expect(snapshot.value.state).toBe("settled");
      snapshots.push(snapshot.value);
    }
    const ordered = [...snapshots].sort(
      (left, right) => Number(left.queueSequence) - Number(right.queueSequence),
    );
    const records = yield* readLog(conversation);
    const recordIds = records.map((envelope) => envelope.record.recordId);
    expect(new Set(recordIds).size).toBe(recordIds.length);
    for (const snapshot of ordered) {
      const settled = records.filter(
        (envelope) =>
          envelope.record.recordId === submissionSettlementRecordId(snapshot.submissionId),
      );
      expect(settled).toHaveLength(1);
      expect(settled[0]?.record.payload._tag).toBe("SubmissionSettled");
    }
    const expectedInputs = ordered.map((snapshot) =>
      submissionInputRecordId(snapshot.submissionId),
    );
    const inputOrder = recordIds.filter((recordId) =>
      expectedInputs.some((expected) => expected === recordId),
    );
    expect(inputOrder).toEqual(expectedInputs.filter((expected) => inputOrder.includes(expected)));
    const expectedSettlements = ordered.map((snapshot) =>
      submissionSettlementRecordId(snapshot.submissionId),
    );
    const settlementOrder = recordIds.filter((recordId) =>
      expectedSettlements.some((expected) => expected === recordId),
    );
    expect(settlementOrder).toEqual(expectedSettlements);
    if (supplier !== undefined) assertSupplierHonesty(records, supplier);
  });

layer(NodeFileSystem.layer, { excludeTestServices: true })(
  "DN crash matrix (real process kills)",
  (it) => {
    it.effect(
      "kill at submit:after-admit: recovery completes materialization; the same key resumes",
      () =>
        withCrashSite((site) =>
          Effect.gen(function* () {
            const conversation = "conversation-kill-admit";
            const key = "kill-admit-1";
            const result = yield* runWorkerToExit({
              db: site.db,
              scenario: "submit",
              conversation,
              key,
              killAt: "submit:after-admit",
            });
            expectKilled(result);

            yield* withHost(
              site.db,
              Effect.gen(function* () {
                const host = yield* NodeDurableHost;
                const snapshot = yield* lookupByKey(conversation, key);
                const report = host.startupRecovery.find(
                  (entry) => entry.submissionId === snapshot.submissionId,
                );
                expect(report?.decision._tag).toBe("CompleteMaterialization");
                expect(report?.disposition).toBe("repaired");
                expect(yield* lookupState(snapshot.submissionId)).toBe("ready");
                const records = yield* readLog(conversation);
                expect(records[0]?.record.recordId).toBe(`conversation-created:${conversation}`);

                // Reattachment: the identical payload under the same key resumes the Receipt.
                const receipt = yield* resubmit(conversation, key);
                expect(receipt.receiptId).toBe(snapshot.receiptId);
                expect(receipt.submissionId).toBe(snapshot.submissionId);
                expect(Number(receipt.queueSequence)).toBe(Number(snapshot.queueSequence));

                // The same key with DIFFERENT content is refused, never silently replaced.
                const runtime = yield* DurableAgentRuntime;
                const conflict = yield* Effect.exit(
                  runtime.submit(
                    { definition: plannerDefinition },
                    { question: "a conflicting question" },
                    crashSubmitOptions(conversation, key),
                  ),
                );
                expect(failureTag(conflict)).toBe("AdmissionConflict");

                const settlements = yield* drainPlanner(conversation, CHILD_ANSWER);
                expect(settlements).toHaveLength(1);
                expect(settlements[0]?.outcome).toBe("completed");
                const settlement = yield* host.awaitSettlement(receipt);
                expect(settlement.outcome).toBe("completed");
                yield* assertConvergence(conversation, [snapshot.submissionId]);
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
            const conversation = "conversation-kill-ready";
            const key = "kill-ready-1";
            const result = yield* runWorkerToExit({
              db: site.db,
              scenario: "submit",
              conversation,
              key,
              killAtStorage: "ledger:mark-ready:after",
            });
            expectKilled(result);

            // Client-only restart (no recovery pass): the replay alone returns the Receipt.
            const submissionId = yield* withRuntime(
              site.db,
              Effect.gen(function* () {
                const snapshot = yield* lookupByKey(conversation, key);
                expect(snapshot.state).toBe("ready");
                const receipt = yield* resubmit(conversation, key);
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
                const settlements = yield* drainPlanner(conversation, CHILD_ANSWER);
                expect(settlements[0]?.outcome).toBe("completed");
                const inputRecords = (yield* readLog(conversation)).filter(
                  (envelope) => envelope.record.recordId === submissionInputRecordId(submissionId),
                );
                expect(inputRecords).toHaveLength(1);
                yield* assertConvergence(conversation, [submissionId]);
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
            const conversation = "conversation-kill-claim";
            const key = "kill-claim-1";
            const result = yield* runWorkerToExit({
              db: site.db,
              scenario: "run",
              conversation,
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
                const snapshot = yield* lookupByKey(conversation, key);
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

                const settlements = yield* drainPlanner(conversation, CHILD_ANSWER);
                expect(settlements[0]?.outcome).toBe("completed");
                const inputRecords = (yield* readLog(conversation)).filter(
                  (envelope) =>
                    envelope.record.recordId === submissionInputRecordId(snapshot.submissionId),
                );
                expect(inputRecords).toHaveLength(1);
                yield* assertConvergence(conversation, [snapshot.submissionId]);
              }),
            );
          }),
        ),
      30_000,
    );

    it.effect(
      "kill at input:after-canonical-append: the marker is repaired and FIFO holds for the queued Submission",
      () =>
        withCrashSite((site) =>
          Effect.gen(function* () {
            const conversation = "conversation-kill-input";
            const key = "kill-input";
            const result = yield* runWorkerToExit({
              db: site.db,
              scenario: "run-two",
              conversation,
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
                const head = yield* lookupByKey(conversation, `${key}-1`);
                const queued = yield* lookupByKey(conversation, `${key}-2`);

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
                    conversationId: decodeConversationId(conversation),
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

                const settlements = yield* drainPlanner(conversation, CHILD_ANSWER);
                // P5 (plan §2.5): the queued Submission joins the resumed head Run at its
                // first Turn seam and settles WITH it — one head settlement, both settled in
                // admitted FIFO order (assertConvergence pins the canonical ordering).
                expect(settlements.map((settlement) => settlement.submissionId)).toEqual([
                  head.submissionId,
                ]);
                const records = yield* readLog(conversation);
                expect(
                  records.filter(
                    (envelope) =>
                      envelope.record.recordId === submissionInputRecordId(head.submissionId),
                  ),
                ).toHaveLength(1);
                yield* assertConvergence(conversation, [head.submissionId, queued.submissionId]);
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
            const conversation = "conversation-kill-midturn";
            const key = "kill-midturn-1";
            const exit = yield* Effect.scoped(
              Effect.gen(function* () {
                const handle = yield* startWorker({
                  db: site.db,
                  scenario: "run-blocked",
                  conversation,
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
                const snapshot = yield* lookupByKey(conversation, key);
                const runId = runIdForSubmission(snapshot.submissionId);

                // Exactly Turn 1 survived; the incomplete Turn 2 left no canonical trace.
                const committed = yield* readLog(conversation);
                expect(logTags(committed)).toEqual([
                  "ConversationCreated",
                  "UserInputRecorded",
                  "ModelResponseRecorded",
                  "ToolCallSettled",
                ]);
                const report = host.startupRecovery.find(
                  (entry) => entry.submissionId === snapshot.submissionId,
                );
                expect(report?.decision._tag).toBe("ResumeFromTurnBoundary");
                expect(report?.disposition).toBe("deferred");

                // Client restart: same-key resubmission reattaches to the original Receipt.
                const receipt = yield* resubmit(conversation, key);
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

                const settlements = yield* drainSearch(conversation, FRESH_ANSWER);
                expect(settlements).toHaveLength(1);
                expect(settlements[0]?.outcome).toBe("completed");

                const records = yield* readLog(conversation);
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
                yield* assertConvergence(conversation, [snapshot.submissionId]);
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
            const conversation = "conversation-fenced";
            const key = "fenced-1";
            yield* Effect.scoped(
              Effect.gen(function* () {
                const handle = yield* startWorker({
                  db: site.db,
                  scenario: "run-blocked",
                  conversation,
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
                    const snapshot = yield* lookupByKey(conversation, key);
                    const settlements = yield* drainSearch(conversation, FRESH_ANSWER);
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
                expect(failure?.kind === "worker-failure" && failure.tag).toBe("FenceRejected");

                yield* withRuntime(
                  site.db,
                  Effect.gen(function* () {
                    const records = yield* readLog(conversation);
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
                    yield* assertConvergence(conversation, [submissionId]);
                  }),
                );
              }),
            );
          }),
        ),
      30_000,
    );

    it.effect(
      "kill before settlement reservation: the outcome is recomputed and settles exactly once",
      () =>
        withCrashSite((site) =>
          Effect.gen(function* () {
            const conversation = "conversation-kill-prereserve";
            const key = "kill-prereserve-1";
            const result = yield* runWorkerToExit({
              db: site.db,
              scenario: "run",
              conversation,
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
                const snapshot = yield* lookupByKey(conversation, key);
                const report = host.startupRecovery.find(
                  (entry) => entry.submissionId === snapshot.submissionId,
                );
                expect(report?.decision._tag).toBe("ResumeFromTurnBoundary");
                expect(report?.disposition).toBe("deferred");

                const settlements = yield* drainPlanner(conversation, FRESH_ANSWER);
                expect(settlements).toHaveLength(1);
                expect(settlements[0]?.outcome).toBe("completed");

                // D6: the model was re-invoked (a visible duplicate Turn), never re-settled.
                const records = yield* readLog(conversation);
                expect(
                  logTags(records).filter((tag) => tag === "ModelResponseRecorded"),
                ).toHaveLength(2);
                expect(logTags(records).filter((tag) => tag === "SubmissionSettled")).toHaveLength(
                  1,
                );
                yield* assertConvergence(conversation, [snapshot.submissionId]);
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
            const conversation = "conversation-kill-reserved";
            const key = "kill-reserved-1";
            const result = yield* runWorkerToExit({
              db: site.db,
              scenario: "run",
              conversation,
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
                const snapshot = yield* lookupByKey(conversation, key);
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
                const records = yield* readLog(conversation);
                const settled = records.filter(
                  (envelope) => envelope.record.payload._tag === "SubmissionSettled",
                );
                expect(settled).toHaveLength(1);
                expect(settled[0]?.record).toEqual(recovered.reservation?.record);
                expect(records.map((envelope) => envelope.record.recordId)).toContain(
                  recoveryRepairRecordId(snapshot.submissionId, "AppendReservedSettlement"),
                );

                // awaitSettlement after restart returns the recorded Settlement.
                const receipt = yield* resubmit(conversation, key);
                const settlement = yield* host.awaitSettlement(receipt);
                expect(settlement.outcome).toBe("completed");
                expect(settlement.receiptId).toBe(snapshot.receiptId);
                yield* assertConvergence(conversation, [snapshot.submissionId]);
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
            const conversation = "conversation-kill-finalize";
            const key = "kill-finalize-1";
            const result = yield* runWorkerToExit({
              db: site.db,
              scenario: "run",
              conversation,
              key,
              killAt: "terminalize:after-canonical-append",
              leaseMillis: CHILD_LEASE_MS,
            });
            expectKilled(result);

            // Before any recovery: the canonical outcome exists, the ledger row is nonterminal.
            const before = yield* withRuntime(
              site.db,
              Effect.gen(function* () {
                const snapshot = yield* lookupByKey(conversation, key);
                expect(snapshot.state).not.toBe("settled");
                const records = yield* readLog(conversation);
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

                const records = yield* readLog(conversation);
                const settled = records.filter(
                  (envelope) => envelope.record.payload._tag === "SubmissionSettled",
                );
                expect(settled).toHaveLength(1);
                expect(settled[0]).toEqual(before.envelope);

                const receipt = yield* resubmit(conversation, key);
                const settlement = yield* host.awaitSettlement(receipt);
                expect(settlement.outcome).toBe("completed");
                yield* assertConvergence(conversation, [before.submissionId]);
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
            const conversation = "conversation-abort-ready";
            const key = "abort-ready-1";
            const result = yield* runWorkerToExit({
              db: site.db,
              scenario: "abort-ready",
              conversation,
              key,
              killAt: "abort:after-intent",
            });
            expectKilled(result);

            yield* withHost(
              site.db,
              Effect.gen(function* () {
                const host = yield* NodeDurableHost;
                const runtime = yield* DurableAgentRuntime;
                const snapshot = yield* lookupByKey(conversation, key);
                const report = host.startupRecovery.find(
                  (entry) => entry.submissionId === snapshot.submissionId,
                );
                expect(report?.decision._tag).toBe("SettleAborted");
                expect(report?.disposition).toBe("repaired");

                const receipt = yield* resubmit(conversation, key);
                const settlement = yield* host.awaitSettlement(receipt);
                expect(settlement.outcome).toBe("aborted");

                const records = yield* readLog(conversation);
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
                yield* assertConvergence(conversation, [snapshot.submissionId]);
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
            const conversation = "conversation-abort-active";
            const key = "abort-active-1";
            yield* Effect.scoped(
              Effect.gen(function* () {
                const handle = yield* startWorker({
                  db: site.db,
                  scenario: "abort-active",
                  conversation,
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
                    const snapshot = yield* lookupByKey(conversation, key);
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
                const snapshot = yield* lookupByKey(conversation, key);
                expect(snapshot.state).toBe("settled");

                const records = yield* readLog(conversation);
                const tags = logTags(records);
                // Durable §13: the abort became canonical BEFORE the Run fiber died — and the
                // interrupted model produced no committed Turn at all.
                expect(tags.indexOf("AbortRequested")).toBeGreaterThanOrEqual(0);
                expect(tags.indexOf("AbortRequested")).toBeLessThan(
                  tags.indexOf("SubmissionSettled"),
                );
                expect(tags).not.toContain("ModelResponseRecorded");
                expect(tags).toContain("UserInputRecorded");

                const receipt = yield* resubmit(conversation, key);
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
                yield* assertConvergence(conversation, [snapshot.submissionId]);
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
            const conversation = "conversation-kill-response";
            const key = "kill-response-1";
            const result = yield* runWorkerToExit({
              db: site.db,
              scenario: "run-uncertain",
              conversation,
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
                const snapshot = yield* lookupByKey(conversation, key);
                const runId = runIdForSubmission(snapshot.submissionId);

                // The provably-safe window (durability §15): the response is canonical, nothing
                // is prepared, and the external supplier was never called.
                const committed = yield* readLog(conversation);
                expect(logTags(committed)).toEqual([
                  "ConversationCreated",
                  "UserInputRecorded",
                  "ModelResponseRecorded",
                ]);
                expect(supplierCount(site.supplier, "book", BOOK_REF)).toBe(0);
                const report = host.startupRecovery.find(
                  (entry) => entry.submissionId === snapshot.submissionId,
                );
                expect(report?.decision._tag).toBe("ResumePendingToolBatch");
                expect(report?.disposition).toBe("deferred");

                const settlements = yield* drainUncertainBook(site, conversation, FRESH_ANSWER);
                expect(settlements).toHaveLength(1);
                expect(settlements[0]?.outcome).toBe("completed");
                expect(supplierCount(site.supplier, "book", BOOK_REF)).toBe(1);

                // No model re-invocation for the declared Turn: exactly one ModelResponseRecorded
                // for Turn 1 and no interruption audit — the resumed batch replayed the canonical
                // declaration instead of asking the model again.
                const records = yield* readLog(conversation);
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
                yield* assertConvergence(conversation, [snapshot.submissionId], {
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
            const conversation = "conversation-kill-prepared-proof";
            const key = "kill-prepared-proof-1";
            const result = yield* runWorkerToExit({
              db: site.db,
              scenario: "run-uncertain",
              conversation,
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
                const snapshot = yield* lookupByKey(conversation, key);
                const runId = runIdForSubmission(snapshot.submissionId);

                // The empty supplier store IS the marker: the handler provably never started.
                expect(supplierCount(site.supplier, "book", BOOK_REF)).toBe(0);
                const report = host.startupRecovery.find(
                  (entry) => entry.submissionId === snapshot.submissionId,
                );
                expect(report?.decision._tag).toBe("MarkUnknown");
                expect(report?.disposition).toBe("deferred");
                expect(yield* lookupState(snapshot.submissionId)).not.toBe("unknown");

                const settlements = yield* drainUncertainBook(site, conversation, FRESH_ANSWER);
                expect(settlements).toHaveLength(1);
                expect(settlements[0]?.outcome).toBe("completed");
                expect(supplierCount(site.supplier, "book", BOOK_REF)).toBe(1);

                // A point-in-time proof records nothing: no Unknown Outcome, no resolution audit,
                // exactly one canonical settled result.
                const records = yield* readLog(conversation);
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
                yield* assertConvergence(conversation, [snapshot.submissionId], {
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
            const conversation = "conversation-kill-prepared-unknown";
            const key = "kill-prepared-unknown-1";
            const result = yield* runWorkerToExit({
              db: site.db,
              scenario: "run-uncertain",
              conversation,
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
                const snapshot = yield* lookupByKey(conversation, key);
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
                  (yield* readLog(conversation)).map((envelope) => envelope.record.recordId),
                ).toContain(toolCallUnknownRecordId(runId, 1, decodeToolCallId(BOOK_CALL_ID)));

                // The unresolved ordinary call is never auto-replayed: draining grants nothing.
                const blocked = yield* drainUncertainBook(site, conversation, FRESH_ANSWER);
                expect(blocked).toEqual([]);
                expect(supplierCount(site.supplier, "book", BOOK_REF)).toBe(0);

                // Authorized DUR-017 resolution arrives from a SECOND real process through the
                // shared durable ledger.
                yield* runResolver({
                  db: site.db,
                  scenario: "resolve-unknown",
                  conversation,
                  key,
                });
                expect(yield* lookupState(snapshot.submissionId)).toBe("input-applied");

                const settlements = yield* drainUncertainBook(site, conversation, FRESH_ANSWER);
                expect(settlements).toHaveLength(1);
                expect(settlements[0]?.outcome).toBe("completed");
                expect(supplierCount(site.supplier, "book", BOOK_REF)).toBe(1);

                const records = yield* readLog(conversation);
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
                yield* assertConvergence(conversation, [snapshot.submissionId], {
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
            const conversation = "conversation-kill-midhandler";
            const key = "kill-midhandler-1";
            const exit = yield* Effect.scoped(
              Effect.gen(function* () {
                const handle = yield* startWorker({
                  db: site.db,
                  scenario: "run-uncertain",
                  conversation,
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
                const snapshot = yield* lookupByKey(conversation, key);
                const runId = runIdForSubmission(snapshot.submissionId);
                const report = host.startupRecovery.find(
                  (entry) => entry.submissionId === snapshot.submissionId,
                );
                // The supplier store shows the booking: recovery settles the recovered result
                // canonically WITHOUT executing anything (never fabricate, durability §10).
                expect(report?.decision._tag).toBe("MarkUnknown");
                expect(report?.disposition).toBe("repaired");
                expect(supplierCount(site.supplier, "book", BOOK_REF)).toBe(1);

                const records = yield* readLog(conversation);
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

                const settlements = yield* drainUncertainBook(site, conversation, FRESH_ANSWER);
                expect(settlements).toHaveLength(1);
                expect(settlements[0]?.outcome).toBe("completed");
                // The supplier call count stays 1: the recovered outcome was never re-executed.
                expect(supplierCount(site.supplier, "book", BOOK_REF)).toBe(1);
                yield* assertConvergence(conversation, [snapshot.submissionId], {
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
            const conversation = "conversation-kill-preresults";
            const key = "kill-preresults-1";
            // The supplier gate arms `append:before` to fire only once the booking exists, so the
            // kill lands exactly between the handler's return and the Turn's results append.
            const result = yield* runWorkerToExit({
              db: site.db,
              scenario: "run-idempotent",
              conversation,
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
                const snapshot = yield* lookupByKey(conversation, key);
                const runId = runIdForSubmission(snapshot.submissionId);
                expect(snapshot.state).not.toBe("settled");
                const before = (yield* readLog(conversation)).map(
                  (envelope) => envelope.record.recordId,
                );
                // The result was lost in memory: prepared is canonical, settled is not.
                expect(before).toContain(
                  toolCallPreparedRecordId(runId, 1, decodeToolCallId(BOOK_CALL_ID)),
                );
                expect(before).not.toContain(
                  toolCallSettledRecordId(runId, 1, decodeToolCallId(BOOK_CALL_ID)),
                );

                const settlements = yield* drainIdempotentBook(site, conversation, FRESH_ANSWER);
                expect(settlements).toHaveLength(1);
                expect(settlements[0]?.outcome).toBe("completed");

                // Honest at-least-once: the declared contract re-executed the external call and
                // the duplicate is OBSERVABLE — supplier count 2, never hidden (rule 8).
                expect(supplierCount(site.supplier, "book", BOOK_REF)).toBe(2);
                const records = yield* readLog(conversation);
                expect(logTags(records)).not.toContain("ToolCallUnknown");
                expect(
                  records.filter(
                    (envelope) =>
                      envelope.record.recordId ===
                      toolCallSettledRecordId(runId, 1, decodeToolCallId(BOOK_CALL_ID)),
                  ),
                ).toHaveLength(1);
                yield* assertConvergence(conversation, [snapshot.submissionId], {
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
            const conversation = "conversation-kill-step";
            const key = "kill-step-1";
            const result = yield* runWorkerToExit({
              db: site.db,
              scenario: "run-steps",
              conversation,
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
                const snapshot = yield* lookupByKey(conversation, key);
                const runId = runIdForSubmission(snapshot.submissionId);
                const callId = decodeToolCallId(ITINERARY_CALL_ID);
                const committed = yield* readLog(conversation);
                expect(committed.map((envelope) => envelope.record.recordId)).toContain(
                  toolStepSettledRecordId(runId, callId, "reserve-flight"),
                );
                const report = host.startupRecovery.find(
                  (entry) => entry.submissionId === snapshot.submissionId,
                );
                // The Durable Tool is re-enterable (SafeToRetry proof): the worker resumes it.
                expect(report?.decision._tag).toBe("MarkUnknown");
                expect(report?.disposition).toBe("deferred");

                const settlements = yield* drainItinerary(site, conversation, FRESH_ANSWER);
                expect(settlements).toHaveLength(1);
                expect(settlements[0]?.outcome).toBe("completed");

                // The handler re-entered honestly (at-least-once), step 1 replayed its recorded
                // result WITHOUT executing (supplier count stays 1), step 2 executed exactly once.
                expect(supplierCount(site.supplier, "itinerary-enter", STEP_REF)).toBe(2);
                expect(supplierCount(site.supplier, "reserve-flight", STEP_REF)).toBe(1);
                expect(supplierCount(site.supplier, "reserve-lodging", STEP_REF)).toBe(1);

                const records = yield* readLog(conversation);
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
                yield* assertConvergence(conversation, [snapshot.submissionId], {
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
            const conversation = "conversation-kill-approval-request";
            const key = "kill-approval-request-1";
            const result = yield* runWorkerToExit({
              db: site.db,
              scenario: "suspend-approval",
              conversation,
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
                const snapshot = yield* lookupByKey(conversation, key);
                const runId = runIdForSubmission(snapshot.submissionId);
                expect(snapshot.state).toBe("input-applied");
                expect(
                  (yield* readLog(conversation)).map((envelope) => envelope.record.recordId),
                ).toContain(toolApprovalRequestRecordId(runId, 1, decodeToolCallId(BOOK_CALL_ID)));
              }),
            );

            yield* withHost(
              site.db,
              Effect.gen(function* () {
                const host = yield* NodeDurableHost;
                const snapshot = yield* lookupByKey(conversation, key);
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
                  conversation,
                  key,
                  decision: "approved",
                });
                expect(yield* lookupState(snapshot.submissionId)).toBe("input-applied");

                const settlements = yield* drainApprovalBook(site, conversation, FRESH_ANSWER);
                expect(settlements).toHaveLength(1);
                expect(settlements[0]?.outcome).toBe("completed");
                expect(supplierCount(site.supplier, "book", BOOK_REF)).toBe(1);

                // The request was appended exactly once across the crash and both Attempts.
                const records = yield* readLog(conversation);
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
                yield* assertConvergence(conversation, [snapshot.submissionId], {
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
            const conversation = "conversation-kill-approval-suspend";
            const key = "kill-approval-suspend-1";
            const result = yield* runWorkerToExit({
              db: site.db,
              scenario: "suspend-approval",
              conversation,
              key,
              killAt: "approval:after-suspend",
              supplierDir: site.supplier,
            });
            expectKilled(result);

            yield* withHost(
              site.db,
              Effect.gen(function* () {
                const host = yield* NodeDurableHost;
                const snapshot = yield* lookupByKey(conversation, key);
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
                  conversation,
                  key,
                  decision: "approved",
                });
                expect(yield* lookupState(snapshot.submissionId)).toBe("input-applied");

                const settlements = yield* drainApprovalBook(site, conversation, FRESH_ANSWER);
                expect(settlements).toHaveLength(1);
                expect(settlements[0]?.outcome).toBe("completed");
                expect(supplierCount(site.supplier, "book", BOOK_REF)).toBe(1);

                // Batch resume, not model re-invocation: exactly one ModelResponseRecorded for
                // the declaring Turn; the gated call entered the ordinary uncertainty protocol.
                const records = yield* readLog(conversation);
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
                yield* assertConvergence(conversation, [snapshot.submissionId], {
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
            const conversation = "conversation-kill-approval-deny";
            const key = "kill-approval-deny-1";
            const result = yield* runWorkerToExit({
              db: site.db,
              scenario: "suspend-approval",
              conversation,
              key,
              killAt: "approval:after-suspend",
              supplierDir: site.supplier,
            });
            expectKilled(result);

            yield* withHost(
              site.db,
              Effect.gen(function* () {
                const snapshot = yield* lookupByKey(conversation, key);
                const runId = runIdForSubmission(snapshot.submissionId);
                expect(snapshot.state).toBe("suspended");

                yield* runResolver({
                  db: site.db,
                  scenario: "resolve-approval",
                  conversation,
                  key,
                  decision: "denied",
                });

                const settlements = yield* drainApprovalBook(site, conversation, FRESH_ANSWER);
                expect(settlements).toHaveLength(1);
                // Denial-terminal (P2 default): the Run fails with the denial canonical and the
                // handler NEVER started — the supplier store stays empty.
                expect(settlements[0]?.outcome).toBe("failed");
                expect(supplierCount(site.supplier, "book", BOOK_REF)).toBe(0);

                const records = yield* readLog(conversation);
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
                yield* assertConvergence(conversation, [snapshot.submissionId], {
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
            const conversation = "conversation-kill-join-claim";
            const key = "kill-join-claim";
            const result = yield* runWorkerToExit({
              db: site.db,
              scenario: "run-join",
              conversation,
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
                const queued = yield* lookupByKey(conversation, `${key}-2`);
                expect(queued.state).toBe("joining");
                expect(
                  (yield* readLog(conversation)).map((envelope) => envelope.record.recordId),
                ).not.toContain(submissionInputRecordId(queued.submissionId));
                return queued.submissionId;
              }),
            );

            yield* withHost(
              site.db,
              Effect.gen(function* () {
                const host = yield* NodeDurableHost;
                const headSnapshot = yield* lookupByKey(conversation, `${key}-1`);
                const queuedReport = host.startupRecovery.find(
                  (entry) => entry.submissionId === queuedId,
                );
                // DUR-016: joining without canonical input reverts to ready.
                expect(queuedReport?.decision._tag).toBe("RevertJoining");
                expect(queuedReport?.disposition).toBe("repaired");
                expect(yield* lookupState(queuedId)).toBe("ready");

                const settlements = yield* drainPlanner(conversation, FRESH_ANSWER);
                // One HEAD settlement: the reverted Submission re-joined the resumed host Run.
                expect(settlements.map((settlement) => settlement.submissionId)).toEqual([
                  headSnapshot.submissionId,
                ]);
                expect(yield* lookupState(queuedId)).toBe("settled");

                // Delivered exactly once: one canonical input record ever, and the queued text
                // entered exactly one committed model response (the coverage rule's evidence).
                const records = yield* readLog(conversation);
                expect(
                  records.filter(
                    (envelope) => envelope.record.recordId === submissionInputRecordId(queuedId),
                  ),
                ).toHaveLength(1);
                expect(responseOccurrences(records, JOIN_QUESTION)).toBe(1);
                yield* assertConvergence(conversation, [headSnapshot.submissionId, queuedId]);
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
            const conversation = "conversation-kill-join-append";
            const key = "kill-join-append";
            const result = yield* runWorkerToExit({
              db: site.db,
              scenario: "run-join",
              conversation,
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
                const queued = yield* lookupByKey(conversation, `${key}-2`);
                expect(queued.state).toBe("joining");
                expect(
                  (yield* readLog(conversation)).map((envelope) => envelope.record.recordId),
                ).toContain(submissionInputRecordId(queued.submissionId));
                return queued.submissionId;
              }),
            );

            yield* withHost(
              site.db,
              Effect.gen(function* () {
                const host = yield* NodeDurableHost;
                const headSnapshot = yield* lookupByKey(conversation, `${key}-1`);
                const queuedReport = host.startupRecovery.find(
                  (entry) => entry.submissionId === queuedId,
                );
                expect(queuedReport?.decision._tag).toBe("RepairJoinMarker");
                expect(queuedReport?.disposition).toBe("repaired");
                expect(yield* lookupState(queuedId)).toBe("joined");

                const settlements = yield* drainPlanner(conversation, FRESH_ANSWER);
                expect(settlements.map((settlement) => settlement.submissionId)).toEqual([
                  headSnapshot.submissionId,
                ]);
                expect(yield* lookupState(queuedId)).toBe("settled");

                // Reattached, never duplicated: one canonical record, one prompt coverage.
                const records = yield* readLog(conversation);
                expect(
                  records.filter(
                    (envelope) => envelope.record.recordId === submissionInputRecordId(queuedId),
                  ),
                ).toHaveLength(1);
                expect(responseOccurrences(records, JOIN_QUESTION)).toBe(1);
                yield* assertConvergence(conversation, [headSnapshot.submissionId, queuedId]);
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
            const conversation = "conversation-kill-joined-host";
            const key = "kill-joined-host";
            const exit = yield* Effect.scoped(
              Effect.gen(function* () {
                const handle = yield* startWorker({
                  db: site.db,
                  scenario: "run-join",
                  conversation,
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
                const queued = yield* lookupByKey(conversation, `${key}-2`);
                expect(queued.state).toBe("joined");
                const records = yield* readLog(conversation);
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
                const headSnapshot = yield* lookupByKey(conversation, `${key}-1`);
                const queuedReport = host.startupRecovery.find(
                  (entry) => entry.submissionId === queuedId,
                );
                // A live host owns its joined Submissions: recovery defers to the host's resume.
                expect(queuedReport?.decision._tag).toBe("AwaitHostSettlement");
                expect(queuedReport?.disposition).toBe("deferred");

                const settlements = yield* drainPlanner(conversation, FRESH_ANSWER);
                expect(settlements.map((settlement) => settlement.submissionId)).toEqual([
                  headSnapshot.submissionId,
                ]);
                expect(settlements[0]?.outcome).toBe("completed");
                expect(yield* lookupState(queuedId)).toBe("settled");

                // The coverage rule across process death: the reattached input entered exactly
                // one committed model response, and the joined settlement rides the host Run.
                const records = yield* readLog(conversation);
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
                yield* assertConvergence(conversation, [headSnapshot.submissionId, queuedId]);
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
            const conversation = "conversation-kill-joined-settle";
            const key = "kill-joined-settle";
            // The FIRST ledger finalization of the run is the host's: the kill lands after the
            // host is fully settled but before the joined settlement loop starts.
            const result = yield* runWorkerToExit({
              db: site.db,
              scenario: "run-join",
              conversation,
              key,
              killAtStorage: "ledger:finalize-settlement:after",
              leaseMillis: CHILD_LEASE_MS,
            });
            expectKilled(result);

            const ids = yield* withRuntime(
              site.db,
              Effect.gen(function* () {
                const head = yield* lookupByKey(conversation, `${key}-1`);
                const queued = yield* lookupByKey(conversation, `${key}-2`);
                expect(head.state).toBe("settled");
                expect(queued.state).toBe("joined");
                const recordIds = (yield* readLog(conversation)).map(
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

                const records = yield* readLog(conversation);
                const joinedSettlement = records.find(
                  (envelope) =>
                    envelope.record.recordId === submissionSettlementRecordId(ids.queued),
                )?.record.payload;
                expect(joinedSettlement?._tag).toBe("SubmissionSettled");
                if (joinedSettlement?._tag === "SubmissionSettled") {
                  expect(joinedSettlement.outcome).toBe("completed");
                  expect(joinedSettlement.runId).toBe(runIdForSubmission(ids.head));
                }
                yield* assertConvergence(conversation, [ids.head, ids.queued]);
              }),
            );
          }),
        ),
      30_000,
    );
  },
);

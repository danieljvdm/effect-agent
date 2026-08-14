import { spawn } from "node:child_process";
import * as fs from "node:fs";
import { fileURLToPath } from "node:url";

import type { SubmissionId } from "@effect-agent/core";
import {
  ConversationRead,
  ConversationStore,
  SubmissionLedger,
  SubmissionLookupById,
  SubmissionLookupByKey,
  submissionInputRecordId,
  submissionSettlementRecordId,
  type CanonicalRecordEnvelope,
  type DurableRuntimeFailpointLocation,
  type SubmissionSnapshot,
} from "@effect-agent/session";
import type { SqliteStorageFailpointLocation } from "@effect-agent/storage-sqlite";
import { expect } from "@effect/vitest";
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

import {
  NodeDurableHost,
  NodeDurableRuntime,
  type NodeDurableRuntimeOptions,
} from "../../src/index.ts";
import {
  CRASH_DEPLOYMENT_ID,
  CrashEnv,
  HOST_PRODUCER_ID,
  KILL_EXIT_CODE,
  crashSubmitOptions,
  decodeChildMessageOption,
  decodeConversationId,
  supplierCounts,
  supplierValues,
  type ChildMessage,
  type CrashScenario,
} from "./fixtures.ts";

/**
 * Shared process-kill harness machinery (plan §Crash matrix, §4.4): spawning real worker
 * processes over one SQLite file, killing them at armed failpoints, restarting hosts against
 * the same file, and the convergence property asserted after every kill. `crash.test.ts` (P4/P5
 * rows) and `crash-subagents.test.ts` (S2 rows) share these helpers so both suites make the
 * same honesty claims through the same machinery.
 */

export const workerEntry = fileURLToPath(new URL("./worker-entry.ts", import.meta.url));
export const packageRoot = fileURLToPath(new URL("../..", import.meta.url));

/** Lease the child claims with in kill scenarios; expired by `waitOutChildLease` after death. */
export const CHILD_LEASE_MS = 250;
const LEASE_WAIT_MS = 1_000;
const POLL_INTERVAL_MS = 25;

export interface WorkerExit {
  readonly code: number | null;
  readonly signal: NodeJS.Signals | null;
}

export interface WorkerHandle {
  readonly stdoutText: () => string;
  readonly stderrText: () => string;
  readonly kill: () => void;
  readonly awaitExit: Effect.Effect<WorkerExit>;
}

/** Bounded real-clock polling; dies with the label (and context) instead of hanging the suite. */
export const waitUntil = <A>(
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

export const waitForFile = (path: string): Effect.Effect<void> =>
  waitUntil(
    () => `file ${path}`,
    Effect.sync(() => (fs.existsSync(path) ? Option.some(undefined) : Option.none())),
  );

export const touchFile = (path: string): Effect.Effect<void> =>
  Effect.sync(() => {
    fs.writeFileSync(path, "1");
  });

export interface ChildOptions {
  readonly db: string;
  readonly scenario: CrashScenario;
  readonly conversation: string;
  readonly key: string;
  readonly killAt?: DurableRuntimeFailpointLocation | undefined;
  readonly killAtStorage?: SqliteStorageFailpointLocation | undefined;
  /** Coordinator failpoint where the worker BLOCKS (marker + hang) instead of dying. */
  readonly blockAt?: DurableRuntimeFailpointLocation | undefined;
  readonly leaseMillis?: number | undefined;
  readonly markerFile?: string | undefined;
  readonly releaseFile?: string | undefined;
  readonly supplierDir?: string | undefined;
  /** `{op}:{key}` gate: the armed kill fires only once the supplier store holds this call. */
  readonly killRequiresSupplier?: string | undefined;
  readonly decision?: "approved" | "denied" | undefined;
  /** S2: the scripted child model writes this marker and blocks mid-stream. */
  readonly childBlockFile?: string | undefined;
  /** S2: unblocks the blocked child model, which then emits the STALE child answer. */
  readonly childReleaseFile?: string | undefined;
  /** S2: `projectResult` writes this marker and blocks until the release file exists. */
  readonly projectMarkerFile?: string | undefined;
  readonly projectReleaseFile?: string | undefined;
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
  if (options.blockAt !== undefined) env[CrashEnv.blockAt] = options.blockAt;
  if (options.leaseMillis !== undefined) env[CrashEnv.leaseMillis] = String(options.leaseMillis);
  if (options.markerFile !== undefined) env[CrashEnv.markerFile] = options.markerFile;
  if (options.releaseFile !== undefined) env[CrashEnv.releaseFile] = options.releaseFile;
  if (options.supplierDir !== undefined) env[CrashEnv.supplierDir] = options.supplierDir;
  if (options.killRequiresSupplier !== undefined) {
    env[CrashEnv.killRequiresSupplier] = options.killRequiresSupplier;
  }
  if (options.decision !== undefined) env[CrashEnv.decision] = options.decision;
  if (options.childBlockFile !== undefined) env[CrashEnv.childBlockFile] = options.childBlockFile;
  if (options.childReleaseFile !== undefined) {
    env[CrashEnv.childReleaseFile] = options.childReleaseFile;
  }
  if (options.projectMarkerFile !== undefined) {
    env[CrashEnv.projectMarkerFile] = options.projectMarkerFile;
  }
  if (options.projectReleaseFile !== undefined) {
    env[CrashEnv.projectReleaseFile] = options.projectReleaseFile;
  }
  return env;
};

/** Spawn one worker child process; the Scope guarantees no child outlives its test. */
export const startWorker = (
  options: ChildOptions,
): Effect.Effect<WorkerHandle, never, Scope.Scope> =>
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

export interface WorkerResult {
  readonly exit: WorkerExit;
  readonly stdout: string;
  readonly stderr: string;
}

export const runWorkerToExit = (options: ChildOptions): Effect.Effect<WorkerResult> =>
  Effect.scoped(
    Effect.gen(function* () {
      const handle = yield* startWorker(options);
      const exit = yield* handle.awaitExit;
      return { exit, stdout: handle.stdoutText(), stderr: handle.stderrText() };
    }),
  );

export const childMessages = (stdout: string): ReadonlyArray<ChildMessage> =>
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

export const expectKilled = (result: WorkerResult): void => {
  expect(
    result.exit.code === KILL_EXIT_CODE || result.exit.signal === "SIGKILL",
    `expected a killed worker, got ${JSON.stringify(result.exit)}; stderr: ${result.stderr}`,
  ).toBe(true);
};

/** The dead child's short lease must lapse before a restarted owner can claim its lane. */
export const waitOutChildLease = Effect.sleep(Duration.millis(LEASE_WAIT_MS));

export const runtimeOptions = (
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
export const withHost = <A, E, R>(
  db: string,
  effect: Effect.Effect<A, E, R>,
  overrides?: Partial<NodeDurableRuntimeOptions>,
) => Effect.provide(effect, NodeDurableHost.layerStack(runtimeOptions(db, overrides)));

/** A restarted "client-only" process: the DN stack WITHOUT the host recovery gate. */
export const withRuntime = <A, E, R>(
  db: string,
  effect: Effect.Effect<A, E, R>,
  overrides?: Partial<NodeDurableRuntimeOptions>,
) => Effect.provide(effect, NodeDurableRuntime.layer(runtimeOptions(db, overrides)));

export interface CrashSite {
  readonly db: string;
  readonly marker: string;
  readonly release: string;
  /** File-backed external supplier store shared with the child processes (plan §4.3). */
  readonly supplier: string;
}

export const withCrashSite = <A, E, R>(use: (site: CrashSite) => Effect.Effect<A, E, R>) =>
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

export const failureTag = <A, E>(exit: Exit.Exit<A, E>): string => {
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

export const readLog = (conversation: string) =>
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

export const logTags = (records: ReadonlyArray<CanonicalRecordEnvelope>): ReadonlyArray<string> =>
  records.map((envelope) => envelope.record.payload._tag);

export const payloadsOf = (
  records: ReadonlyArray<CanonicalRecordEnvelope>,
  tag: string,
): ReadonlyArray<CanonicalRecordEnvelope> =>
  records.filter((envelope) => envelope.record.payload._tag === tag);

export const lookupByKey = (conversation: string, key: string) =>
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

export const lookupState = (submissionId: SubmissionId) =>
  Effect.gen(function* () {
    const ledger = yield* SubmissionLedger;
    const snapshot = yield* ledger.lookup(SubmissionLookupById.make({ submissionId }));
    expect(Option.isSome(snapshot)).toBe(true);
    if (Option.isNone(snapshot)) throw new Error("Expected the Submission to exist");
    return snapshot.value.state;
  });

/** The supplier-store honesty claims of one crash row (plan §4.3, durability §10). */
export interface SupplierExpectation {
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
export const assertConvergence = (
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
    // P7 §7(c) exemption: an ABORTED settlement for never-run work (no canonical
    // `input:{sid}` record) settles immediately by design — without waiting for the head —
    // so it is excluded from the FIFO settlement comparison. DUR-004 bounds EXECUTION order,
    // which never-run work has none of (mirrors `verifyConversationInvariants`).
    const abortedNeverRan = (snapshot: SubmissionSnapshot): boolean =>
      !recordIds.includes(submissionInputRecordId(snapshot.submissionId)) &&
      records.some(
        (envelope) =>
          envelope.record.recordId === submissionSettlementRecordId(snapshot.submissionId) &&
          envelope.record.payload._tag === "SubmissionSettled" &&
          envelope.record.payload.outcome === "aborted",
      );
    const expectedSettlements = ordered
      .filter((snapshot) => !abortedNeverRan(snapshot))
      .map((snapshot) => submissionSettlementRecordId(snapshot.submissionId));
    const settlementOrder = recordIds.filter((recordId) =>
      expectedSettlements.some((expected) => expected === recordId),
    );
    expect(settlementOrder).toEqual(expectedSettlements);
    if (supplier !== undefined) assertSupplierHonesty(records, supplier);
  });

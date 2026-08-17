import { SubmissionId } from "@effect-agent/core";
import {
  OperationCaller,
  submissionInputRecordId,
  submissionSettlementRecordId,
  type CanonicalRecordEnvelope,
} from "@effect-agent/session";
import { SqliteClient } from "@effect/sql-sqlite-do";
import { env, runDurableObjectAlarm, runInDurableObject } from "cloudflare:test";
import { Effect, Layer, Schema } from "effect";
import * as SqlClientService from "effect/unstable/sql/SqlClient";
import { expect } from "vite-plus/test";

import {
  CloudflareConversationClient,
  ConversationObjectNamespace,
  type ConversationObjectRpc,
} from "../src/index.ts";
import {
  TEST_CALLER,
  decodeConversationId,
  supplierCountsFor,
  supplierValuesFor,
} from "./fixtures.ts";
import type {
  ArrayBindingsConversationObject,
  ChangingAuthorizationConversationObject,
  DeniedConversationObject,
  DynamicBindingsConversationObject,
  EffectBindingsConversationObject,
  LimitedConversationObject,
  RunJournalFailureConversationObject,
  SubagentConversationObject,
  TelemetryConversationObject,
  TestConversationObject,
  TinyDatabaseConversationObject,
} from "./worker.ts";

declare global {
  namespace Cloudflare {
    interface Env {
      CONVERSATIONS: DurableObjectNamespace<TestConversationObject>;
      DENIED: DurableObjectNamespace<DeniedConversationObject>;
      CHANGING_AUTH: DurableObjectNamespace<ChangingAuthorizationConversationObject>;
      RUN_JOURNAL_FAILURE: DurableObjectNamespace<RunJournalFailureConversationObject>;
      LIMITED: DurableObjectNamespace<LimitedConversationObject>;
      TINYDB: DurableObjectNamespace<TinyDatabaseConversationObject>;
      SUBAGENTS: DurableObjectNamespace<SubagentConversationObject>;
      DYNAMIC_BINDINGS: DurableObjectNamespace<DynamicBindingsConversationObject>;
      ARRAY_BINDINGS: DurableObjectNamespace<ArrayBindingsConversationObject>;
      EFFECT_BINDINGS: DurableObjectNamespace<EffectBindingsConversationObject>;
      TELEMETRY: DurableObjectNamespace<TelemetryConversationObject>;
    }
  }
}

/**
 * Shared eviction-harness machinery (plan §3): fresh stubs per call (an aborted incarnation
 * may leave a stale stub disconnected), alarm-only convergence drains, raw-storage state
 * probes through `runInDurableObject`, and the convergence property asserted after every
 * abort — the DC twin of the Node crash harness's helpers.
 */

export type TestNamespace =
  | "CONVERSATIONS"
  | "DENIED"
  | "CHANGING_AUTH"
  | "RUN_JOURNAL_FAILURE"
  | "LIMITED"
  | "TINYDB"
  | "SUBAGENTS"
  | "DYNAMIC_BINDINGS"
  | "ARRAY_BINDINGS"
  | "EFFECT_BINDINGS"
  | "TELEMETRY";

/** A FRESH stub for the named Conversation (never cache stubs across aborts). */
export const stubFor = (conversation: string, namespace: TestNamespace = "CONVERSATIONS") => {
  const binding = env[namespace];
  return binding.get(binding.idFromName(conversation));
};

/** Run one client Effect against a namespace binding through the real Worker-side client. */
export const runClient = <A, E>(
  effect: Effect.Effect<A, E, CloudflareConversationClient>,
  namespace: TestNamespace = "CONVERSATIONS",
): Promise<A> =>
  Effect.runPromise(
    effect.pipe(
      Effect.provide(
        CloudflareConversationClient.layer.pipe(
          Layer.provide(
            ConversationObjectNamespace.layer(
              env[namespace] as unknown as DurableObjectNamespace<ConversationObjectRpc>,
            ),
          ),
        ),
      ),
    ),
  );

/** Exit-capturing variant for rows whose client call is EXPECTED to die mid-eviction. */
export const runClientExit = <A, E>(
  effect: Effect.Effect<A, E, CloudflareConversationClient>,
  namespace: TestNamespace = "CONVERSATIONS",
): Promise<{ readonly ok: boolean; readonly value?: A; readonly error?: unknown }> =>
  runClient(effect.pipe(Effect.exit), namespace).then(
    (exit) =>
      exit._tag === "Success" ? { ok: true, value: exit.value } : { ok: false, error: exit.cause },
    (defect: unknown) => ({ ok: false, error: defect }),
  );

export interface LaneRow {
  readonly submission_id: string;
  readonly state: string;
  readonly queue_sequence: number;
}

const sleep = (millis: number) => new Promise((resolve) => setTimeout(resolve, millis));

/**
 * A probe issued right after `ctx.abort()` can land on the dying incarnation and reject with
 * the abort reason (production behavior: the next event reaches a fresh instance). Retry a
 * few times with fresh stubs — this mirrors what any real caller does after an eviction.
 */
const withAbortedInstanceRetry = async <A>(probe: () => Promise<A>): Promise<A> => {
  let lastError: unknown;
  for (let attempt = 0; attempt < 20; attempt++) {
    try {
      return await probe();
    } catch (error) {
      lastError = error;
      await sleep(10);
    }
  }
  throw lastError;
};

/** Raw ledger rows straight from the Object's own storage (never through an entry point). */
export const laneRows = (
  conversation: string,
  namespace: TestNamespace = "CONVERSATIONS",
): Promise<ReadonlyArray<LaneRow>> =>
  withAbortedInstanceRetry(() =>
    runInDurableObject(stubFor(conversation, namespace), (_instance, state) =>
      Effect.runPromise(
        Effect.gen(function* () {
          const sql = yield* SqlClientService.SqlClient;
          return yield* sql<LaneRow>`
            SELECT submission_id, state, queue_sequence
            FROM effect_agent_submissions
            ORDER BY queue_sequence
          `;
        }).pipe(Effect.provide(SqliteClient.layer({ storage: state.storage }))),
      ),
    ),
  );

/** The persisted alarm deadline (epoch millis) or null. */
export const scheduledAlarm = (
  conversation: string,
  namespace: TestNamespace = "CONVERSATIONS",
): Promise<number | null> =>
  withAbortedInstanceRetry(() =>
    runInDurableObject(stubFor(conversation, namespace), (_instance, state) =>
      state.storage.getAlarm(),
    ),
  );

/**
 * Alarm-only convergence: fire the persisted alarm (at-least-once; an armed eviction or a
 * typed pass failure rejects the delivery and the dirty generation keeps the slot committed)
 * until the predicate holds. NO client entry point is ever called — this is the exit gate's
 * "recovers without an incoming request" in its deterministic form: `runDurableObjectAlarm`
 * is accelerated delivery of the alarm that would fire on its own.
 *
 * Built-in invariant audit: locally actionable states observed WITHOUT a scheduled alarm
 * repeatedly are broken. Stable external waits (`suspended`, `unknown`, `joined`, and an
 * admitted child awaiting parent establishment) are intentionally allowed to quiesce.
 */
export const drainAlarmsUntil = async (
  conversation: string,
  predicate: () => Promise<boolean>,
  options?: { readonly namespace?: TestNamespace; readonly rounds?: number },
): Promise<void> => {
  const namespace = options?.namespace ?? "CONVERSATIONS";
  const rounds = options?.rounds ?? 600;
  let consecutiveIdleWithWork = 0;
  for (let round = 0; round < rounds; round++) {
    if (await predicate()) return;
    let fired = false;
    try {
      fired = await runDurableObjectAlarm(stubFor(conversation, namespace));
    } catch {
      // The pass aborted the Object (armed eviction) or failed typed (workerd would retry);
      // either way the pre-armed alarm survives in storage for the next round.
      fired = true;
    }
    if (!fired) {
      const rows = await laneRows(conversation, namespace);
      const actionable = rows.some((row) =>
        ["ready", "input-applied", "running", "joining", "terminalizing"].includes(row.state),
      );
      if (actionable) {
        consecutiveIdleWithWork += 1;
        // Actionable committed work implies a committed alarm. Allow a few rounds for a
        // concurrently-running pass to re-arm before declaring it broken.
        if (consecutiveIdleWithWork >= 20) {
          throw new Error(
            `Alarm invariant broken for ${conversation}: actionable work with no scheduled alarm`,
          );
        }
      } else {
        consecutiveIdleWithWork = 0;
      }
    } else {
      consecutiveIdleWithWork = 0;
    }
    await sleep(10);
  }
  const rows = await laneRows(conversation, namespace);
  throw new Error(
    `Alarm drain did not converge for ${conversation}; lane: ${JSON.stringify(rows)}`,
  );
};

/** Predicate: every Submission on the lane is settled (and at least one exists). */
export const allSettled =
  (conversation: string, namespace: TestNamespace = "CONVERSATIONS") =>
  async (): Promise<boolean> => {
    const rows = await laneRows(conversation, namespace);
    return rows.length > 0 && rows.every((row) => row.state === "settled");
  };

/** Predicate: some Submission on the lane is durably in `state`. */
export const anyInState =
  (conversation: string, state: string, namespace: TestNamespace = "CONVERSATIONS") =>
  async (): Promise<boolean> => {
    const rows = await laneRows(conversation, namespace);
    return rows.some((row) => row.state === state);
  };

const decodeSubmissionId = Schema.decodeSync(SubmissionId);

/** Every canonical record of the lane, through the real paged observation entry point. */
export const readCanonical = (
  conversation: string,
  namespace: TestNamespace = "CONVERSATIONS",
  caller: OperationCaller = TEST_CALLER,
): Promise<ReadonlyArray<CanonicalRecordEnvelope>> =>
  runClient(
    Effect.gen(function* () {
      const client = yield* CloudflareConversationClient;
      return yield* client.readAll(decodeConversationId(conversation), caller);
    }),
    namespace,
  );

/** The supplier-store honesty claims of one eviction row (durability §10). */
export interface SupplierExpectation {
  readonly ref: string;
  /** EXACT per-operation invocation counts; `{}` claims no external call ever happened. */
  readonly counts: Record<string, number>;
}

/**
 * The convergence property asserted after every abort (the Node crash harness's
 * `assertConvergence`, verbatim claims): every accepted Submission still exists and settled;
 * each has EXACTLY one canonical terminal record; no canonical record identity was ever
 * double-appended (stale epochs fenced, DUR-006/DUR-007); canonical inputs and settlements
 * follow the admitted FIFO order (DUR-004); and, when the row touches the external supplier,
 * no recorded result was fabricated and the invocation counts match the claim exactly.
 */
export const assertConvergence = async (
  conversation: string,
  options?: {
    readonly namespace?: TestNamespace;
    readonly supplier?: SupplierExpectation;
    readonly caller?: OperationCaller;
  },
): Promise<void> => {
  const namespace = options?.namespace ?? "CONVERSATIONS";
  const rows = await laneRows(conversation, namespace);
  expect(rows.length).toBeGreaterThan(0);
  for (const row of rows) {
    expect(row.state, `submission ${row.submission_id}`).toBe("settled");
  }
  const records = await readCanonical(conversation, namespace, options?.caller ?? TEST_CALLER);
  const recordIds = records.map((envelope) => envelope.record.recordId);
  expect(new Set(recordIds).size).toBe(recordIds.length);
  const ordered = [...rows].sort((left, right) => left.queue_sequence - right.queue_sequence);
  for (const row of ordered) {
    const settled = records.filter(
      (envelope) =>
        envelope.record.recordId ===
        submissionSettlementRecordId(decodeSubmissionId(row.submission_id)),
    );
    expect(settled, `settlement record for ${row.submission_id}`).toHaveLength(1);
    expect(settled[0]?.record.payload._tag).toBe("SubmissionSettled");
  }
  const expectedInputs = ordered.map((row) =>
    submissionInputRecordId(decodeSubmissionId(row.submission_id)),
  );
  const inputOrder = recordIds.filter((recordId) =>
    expectedInputs.some((expected) => expected === recordId),
  );
  expect(inputOrder).toEqual(expectedInputs.filter((expected) => inputOrder.includes(expected)));
  // P7 §7(c) exemption: an ABORTED settlement for never-run work (no canonical `input:{sid}`
  // record) settles immediately by design — without waiting for the head — so it is excluded
  // from the FIFO settlement comparison. DUR-004 bounds EXECUTION order, which never-run work
  // has none of (mirrors `verifyConversationInvariants`).
  const abortedNeverRan = (submissionId: string): boolean =>
    !recordIds.includes(submissionInputRecordId(decodeSubmissionId(submissionId))) &&
    records.some(
      (envelope) =>
        envelope.record.recordId ===
          submissionSettlementRecordId(decodeSubmissionId(submissionId)) &&
        envelope.record.payload._tag === "SubmissionSettled" &&
        envelope.record.payload.outcome === "aborted",
    );
  const expectedSettlements = ordered
    .filter((row) => !abortedNeverRan(row.submission_id))
    .map((row) => submissionSettlementRecordId(decodeSubmissionId(row.submission_id)));
  const settlementOrder = recordIds.filter((recordId) =>
    expectedSettlements.some((expected) => expected === recordId),
  );
  expect(settlementOrder).toEqual(expectedSettlements);

  if (options?.supplier !== undefined) {
    const produced = supplierValuesFor(options.supplier.ref);
    for (const envelope of records) {
      const payload = envelope.record.payload;
      if (payload._tag === "ToolCallSettled" && payload.isFailure === false) {
        const result: unknown = payload.result;
        if (
          typeof result === "object" &&
          result !== null &&
          "confirmation" in result &&
          typeof result.confirmation === "string"
        ) {
          expect(
            produced.has(result.confirmation),
            `book result "${result.confirmation}" is absent from the supplier store — fabricated`,
          ).toBe(true);
        }
        if (
          typeof result === "object" &&
          result !== null &&
          "state" in result &&
          typeof result.state === "string" &&
          result.state.includes("+")
        ) {
          for (const part of result.state.split("+")) {
            expect(
              produced.has(part),
              `itinerary step result "${part}" is absent from the supplier store — fabricated`,
            ).toBe(true);
          }
        }
      }
      if (payload._tag === "ToolStepSettled") {
        const output: unknown = payload.output;
        if (typeof output === "string") {
          expect(
            produced.has(output),
            `step output "${output}" is absent from the supplier store — fabricated`,
          ).toBe(true);
        }
      }
    }
    expect(supplierCountsFor(options.supplier.ref)).toEqual(options.supplier.counts);
  }
};

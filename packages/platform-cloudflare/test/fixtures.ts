import { contextCompactorRunContextLayer } from "@effect-agent/capabilities/RunHooks";
import * as Agent from "@effect-agent/core/Agent";
import { AgentPolicy } from "@effect-agent/core/AgentPolicy";
import { ThreadId, ToolCallId } from "@effect-agent/core/Identifiers";
import { estimatePromptTokens } from "@effect-agent/engine/Compaction";
import { CompactionError, ContextCompactor } from "@effect-agent/engine/ContextCompactor";
import {
  DurableStep,
  DurableStepError,
  ToolExecutionClass,
} from "@effect-agent/engine/DurableStep";
import { RunToolAuthorization } from "@effect-agent/engine/RunOptions";
import {
  type ThreadMaintenanceFailpointHandler,
  type ThreadMaintenanceFailpointLocation,
} from "@effect-agent/platform-cloudflare/Alarm";
import { type DoStorageFailpointLocation } from "@effect-agent/storage-cloudflare/DoStorageError";
import { type DoStorageFailpointHandler } from "@effect-agent/storage-cloudflare/DoStorageFailpoint";
import { evictionFailpointHandler } from "@effect-agent/storage-cloudflare/testing/DoStorageFailpointTesting";
import { DurableWorkerBinding, type ResolvedBinding } from "@effect-agent/thread/AgentRegistration";
import { type DurableSubmitOptions } from "@effect-agent/thread/DurableAgentRuntime";
import {
  DurableRuntimeFailpointError,
  type DurableRuntimeFailpointHandler,
  type DurableRuntimeFailpointLocation,
} from "@effect-agent/thread/DurableFailpoint";
import { DefinitionDigestInput, DefinitionDigests, Digest } from "@effect-agent/thread/Records";
import {
  ScheduleFailpointError,
  type ScheduleOwner,
  ScheduleRecord,
  ScheduleStorageError,
} from "@effect-agent/thread/Schedule";
import { scheduleOwnerKey } from "@effect-agent/thread/ScheduleTransition";
import { IdempotencyKey, Principal } from "@effect-agent/thread/SubmissionLedger";
import {
  ReconciliationSafeToRetry,
  ReconciliationUncertain,
  ToolReconciler,
} from "@effect-agent/thread/ToolReconciler";
import { Duration, Effect, Layer, Schema, Stream } from "effect";
import { LanguageModel, Model, Tool, Toolkit, type Response } from "effect/unstable/ai";

import { layerFromBindings } from "../src/internal/layers.ts";

/**
 * Workerd-safe eviction-harness fixtures (plan §3, §4 WP3). The vitest pool runs test files
 * and the worker entry in ONE isolate, so this module's state is shared between the tests
 * and the Durable Object instances: armed failpoints are consumed exactly once by the doomed
 * incarnation (a fresh incarnation starts unarmed and converges), the release gates unblock
 * hanging scripted models, and the in-memory supplier store plays the crash harness's
 * file-backed external-truth role — it survives `ctx.abort()` exactly like a real supplier's
 * ledger survives a process kill, keyed per test by the Thread-unique `ref`.
 */

// ---------------------------------------------------------------------------
// Armed failpoints (consumed exactly once, keyed by Thread name)
// ---------------------------------------------------------------------------

const armedStorageEvictions = new Map<string, Array<DoStorageFailpointLocation>>();
const armedRuntimeEvictions = new Map<string, Array<DurableRuntimeFailpointLocation>>();

/** Typed (thrown, NOT abort) pass failures for the workerd alarm-retry row. */
export const armedRuntimeFailures = new Map<string, DurableRuntimeFailpointLocation>();

/**
 * Arm an ORDERED queue of eviction locations for one Thread. Chained rows (a location
 * only reachable through the recovery of an earlier abort) must arm the whole chain UP
 * FRONT: due alarms auto-fire in the pool, so the recovery pass after an abort can run
 * within milliseconds — long before a test observing the first abort could arm the next
 * location. Each hit consumes exactly its queue head, so the doomed incarnations die in
 * order and the final one converges unarmed.
 */
export const armStorageEviction = (
  thread: string,
  ...locations: ReadonlyArray<DoStorageFailpointLocation>
): void => {
  const queue = armedStorageEvictions.get(thread) ?? [];

  queue.push(...locations);
  armedStorageEvictions.set(thread, queue);
};

export const armRuntimeEviction = (
  thread: string,
  ...locations: ReadonlyArray<DurableRuntimeFailpointLocation>
): void => {
  const queue = armedRuntimeEvictions.get(thread) ?? [];

  queue.push(...locations);
  armedRuntimeEvictions.set(thread, queue);
};

/** Locations still armed (either kind); zero means every armed eviction actually fired. */
export const armedEvictionsRemaining = (thread: string): number =>
  (armedStorageEvictions.get(thread)?.length ?? 0) +
  (armedRuntimeEvictions.get(thread)?.length ?? 0);

/** Storage failpoint factory for `ThreadObject.make`: armed hit → `ctx.abort()`. */
export const storageEvictionFailpoint = (ctx: DurableObjectState): DoStorageFailpointHandler =>
  evictionFailpointHandler({
    isArmed: (location) =>
      Effect.sync(() => {
        const name = ctx.id.name;

        if (name === undefined) return false;
        const queue = armedStorageEvictions.get(name);

        if (queue === undefined || queue[0] !== location) return false;
        // Consume BEFORE the abort: the next incarnation must run past this location.
        queue.shift();
        if (queue.length === 0) armedStorageEvictions.delete(name);

        return true;
      }),
    evict: () => {
      ctx.abort("armed storage eviction failpoint");
    },
  });

/** Coordinator failpoint factory: armed eviction → `ctx.abort()`; armed failure → typed. */
export const runtimeEvictionFailpoint =
  (ctx: DurableObjectState): DurableRuntimeFailpointHandler =>
  (location) =>
    Effect.suspend(() => {
      const name = ctx.id.name;

      if (name === undefined) return Effect.void;
      if (armedRuntimeFailures.get(name) === location) {
        armedRuntimeFailures.delete(name);

        return Effect.fail(DurableRuntimeFailpointError.make({ location }));
      }
      const queue = armedRuntimeEvictions.get(name);

      if (queue !== undefined && queue[0] === location) {
        queue.shift();
        if (queue.length === 0) armedRuntimeEvictions.delete(name);

        return Effect.sync((): never => {
          ctx.abort("armed runtime eviction failpoint");
          // `ctx.abort()` never returns; this defensive throw keeps the guarantee even if a
          // harness runtime lets it return.
          throw new Error(`Durable Object eviction did not interrupt execution at ${location}.`);
        });
      }

      return Effect.void;
    });

// ---------------------------------------------------------------------------
// Thread-maintenance race gate (issue #93)
// ---------------------------------------------------------------------------

const maintenancePauses = new Map<string, Array<ThreadMaintenanceFailpointLocation>>();

interface MaintenancePauseGate {
  readonly reached: Promise<void>;
  readonly released: Promise<void>;
  readonly resolveReached: () => void;
  readonly resolveReleased: () => void;
  reachedFlag: boolean;
}

const maintenancePauseGates = new Map<string, MaintenancePauseGate>();

const maintenancePauseKey = (
  thread: string,
  location: ThreadMaintenanceFailpointLocation,
): string => `${thread}:${location}`;

export const armMaintenancePause = (
  thread: string,
  ...locations: ReadonlyArray<ThreadMaintenanceFailpointLocation>
): void => {
  maintenancePauses.set(thread, [...locations]);
  for (const location of locations) {
    const key = maintenancePauseKey(thread, location);
    let resolveReached!: () => void;
    let resolveReleased!: () => void;

    maintenancePauseGates.set(key, {
      reached: new Promise<void>((resolve) => {
        resolveReached = resolve;
      }),
      released: new Promise<void>((resolve) => {
        resolveReleased = resolve;
      }),
      resolveReached: () => resolveReached(),
      resolveReleased: () => resolveReleased(),
      reachedFlag: false,
    });
  }
};

export const awaitMaintenancePause = (
  thread: string,
  location: ThreadMaintenanceFailpointLocation,
): Promise<void> => {
  const gate = maintenancePauseGates.get(maintenancePauseKey(thread, location));

  if (gate === undefined) {
    return Promise.reject(new Error(`Maintenance pause ${location} is not armed for ${thread}.`));
  }

  return gate.reached;
};

export const releaseMaintenancePause = (
  thread: string,
  location?: ThreadMaintenanceFailpointLocation,
): void => {
  if (location === undefined) {
    for (const [key, gate] of maintenancePauseGates) {
      if (key.startsWith(`${thread}:`) && gate.reachedFlag) gate.resolveReleased();
    }

    return;
  }
  maintenancePauseGates.get(maintenancePauseKey(thread, location))?.resolveReleased();
};

export const maintenanceRaceFailpoint =
  (ctx: DurableObjectState): ThreadMaintenanceFailpointHandler =>
  (location) =>
    Effect.gen(function* () {
      const thread = ctx.id.name;

      if (thread === undefined) return;
      const queue = maintenancePauses.get(thread);

      if (queue?.[0] !== location) return;
      queue.shift();
      if (queue.length === 0) maintenancePauses.delete(thread);
      const key = maintenancePauseKey(thread, location);
      const gate = maintenancePauseGates.get(key);

      if (gate === undefined) return;
      gate.reachedFlag = true;
      gate.resolveReached();
      yield* Effect.promise(() => gate.released);
      maintenancePauseGates.delete(key);
    });

// ---------------------------------------------------------------------------
// Release gates (sticky; hanging scripted models poll them)
// ---------------------------------------------------------------------------

const releasedGates = new Set<string>();

export const releaseGate = (ref: string): void => {
  releasedGates.add(ref);
};

export const resetGate = (ref: string): void => {
  releasedGates.delete(ref);
};

const awaitGate = (ref: string): Effect.Effect<void> =>
  Effect.gen(function* () {
    while (!releasedGates.has(ref)) {
      yield* Effect.sleep(Duration.millis(10));
    }
  });

// ---------------------------------------------------------------------------
// In-memory external supplier store (the never-fabricate reference set)
// ---------------------------------------------------------------------------

export interface SupplierRecord {
  readonly op: string;
  readonly key: string;
  readonly value: string;
}

const supplierLog: Array<SupplierRecord> = [];

export const recordSupplierCall = (op: string, key: string, value: string): void => {
  supplierLog.push({ op, key, value });
};

/** Exact per-operation invocation counts for one `ref` — the honesty-claim currency. */
export const supplierCountsFor = (ref: string): Record<string, number> => {
  const counts: Record<string, number> = {};

  for (const record of supplierLog) {
    if (record.key !== ref) continue;
    counts[record.op] = (counts[record.op] ?? 0) + 1;
  }

  return counts;
};

/** Every value the external supplier ever produced for one `ref`. */
export const supplierValuesFor = (ref: string): ReadonlySet<string> =>
  new Set(supplierLog.filter((record) => record.key === ref).map((record) => record.value));

// ---------------------------------------------------------------------------
// Identities
// ---------------------------------------------------------------------------

export const decodeThreadId = Schema.decodeSync(ThreadId);
export const decodeIdempotencyKey = Schema.decodeSync(IdempotencyKey);

export const TEST_PRINCIPAL = Schema.decodeSync(Principal)("principal-cf-eviction");
const SHA_A = Schema.decodeSync(Digest)("a".repeat(64));

export const TEST_DIGESTS = DefinitionDigests.make({ agent: SHA_A, model: SHA_A, tools: SHA_A });

export const DEPLOYMENT_ID = "cf-test-deployment";
export const PRODUCER_PREFIX = "cf-test-producer";
export const THREADS_BINDING = "THREADS";

// ---------------------------------------------------------------------------
// Schedule Owner recovery controls
// ---------------------------------------------------------------------------

interface SchedulePauseGate {
  readonly reached: Promise<void>;
  readonly signalReached: () => void;
  readonly released: Promise<void>;
  readonly release: () => void;
}

const schedulePauseGates = new Map<string, SchedulePauseGate>();
const scheduleIdleWaiters = new Map<string, () => void>();

/** Observe native alarm completion without racing it with a manually invoked handler. */
export const observeScheduleIdle = (owner: ScheduleOwner): Promise<void> =>
  new Promise((resolve) => scheduleIdleWaiters.set(scheduleOwnerKey(owner), resolve));

export const notifyScheduleAlarmCompleted = (ctx: DurableObjectState): void => {
  const name = ctx.id.name;

  if (name === undefined || !scheduleIdleWaiters.has(name)) return;
  if (
    ctx.storage.sql.exec("SELECT storage_id FROM effect_cf_scheduled_alarms LIMIT 1").toArray()
      .length !== 0
  ) {
    return;
  }
  const resolve = scheduleIdleWaiters.get(name);

  scheduleIdleWaiters.delete(name);
  resolve?.();
};

const scheduleEvictions = new Map<string, string>();

interface ScheduleAuthorizationFailureHold {
  held: boolean;
  failureCount: number;
  readonly waiters: Array<{
    readonly minimum: number;
    readonly resolve: (failureCount: number) => void;
  }>;
}
const scheduleAuthorizationFailureHolds = new Map<string, ScheduleAuthorizationFailureHold>();
const scheduleFailures = new Map<string, Array<string>>();
const schedulePrepareEvictionEvidence = new Set<string>();
const ScheduleRecordJsonRow = Schema.Struct({ record_json: Schema.String });
const ScheduleAlarmCountRow = Schema.Struct({ alarm_count: Schema.Natural });

const makeSchedulePauseGate = (): SchedulePauseGate => {
  let signalReached!: () => void;
  let release!: () => void;

  return {
    reached: new Promise<void>((resolve) => {
      signalReached = resolve;
    }),
    signalReached,
    released: new Promise<void>((resolve) => {
      release = resolve;
    }),
    release,
  };
};

export const armScheduleAdmissionPause = (owner: ScheduleOwner) => {
  const key = scheduleOwnerKey(owner);
  const gate = makeSchedulePauseGate();

  schedulePauseGates.set(key, gate);

  return {
    reached: gate.reached,
    release: () => {
      schedulePauseGates.delete(key);
      gate.release();
    },
  };
};

export const armScheduleAdmissionEviction = (owner: ScheduleOwner): void => {
  scheduleEvictions.set(scheduleOwnerKey(owner), "schedule:admission:after");
};

export const armScheduleEviction = (owner: ScheduleOwner, point: string): void => {
  scheduleEvictions.set(scheduleOwnerKey(owner), point);
};

export const observedCommittedPrepareBeforeEviction = (owner: ScheduleOwner): boolean =>
  schedulePrepareEvictionEvidence.has(scheduleOwnerKey(owner));

export const holdScheduleAuthorizationFailures = (owner: ScheduleOwner) => {
  const key = scheduleOwnerKey(owner);

  const hold: ScheduleAuthorizationFailureHold = {
    held: true,
    failureCount: 0,
    waiters: [],
  };

  scheduleAuthorizationFailureHolds.set(key, hold);

  return {
    reached: (minimum: number): Promise<number> => {
      if (hold.failureCount >= minimum) return Promise.resolve(hold.failureCount);

      return new Promise((resolve) => hold.waiters.push({ minimum, resolve }));
    },
    release: (): void => {
      hold.held = false;
      scheduleAuthorizationFailureHolds.delete(key);
    },
  };
};

export const armScheduleFailure = (owner: ScheduleOwner, point: string): void => {
  const key = scheduleOwnerKey(owner);
  const queue = scheduleFailures.get(key) ?? [];

  queue.push(point);
  scheduleFailures.set(key, queue);
};

export const scheduleFailpoint = (ctx: DurableObjectState) => ({
  hit: (point: string) =>
    Effect.suspend(() => {
      const name = ctx.id.name;

      if (name === undefined) return Effect.void;
      if (scheduleEvictions.get(name) === point) {
        scheduleEvictions.delete(name);

        return Effect.sync((): never => {
          if (point === "schedule:prepare:after") {
            const rows = ctx.storage.sql
              .exec("SELECT record_json FROM effect_agent_schedules")
              .toArray();

            const decodedRows = Schema.decodeUnknownSync(Schema.Array(ScheduleRecordJsonRow))(rows);

            const record = Schema.decodeUnknownSync(Schema.fromJsonString(ScheduleRecord))(
              decodedRows[0]?.record_json,
            );

            const alarmRows = ctx.storage.sql
              .exec("SELECT COUNT(*) AS alarm_count FROM effect_cf_scheduled_alarms")
              .toArray();

            const alarmCount = Schema.decodeUnknownSync(Schema.Array(ScheduleAlarmCountRow))(
              alarmRows,
            )[0]?.alarm_count;

            if (record.pending !== null && alarmCount === 1) {
              schedulePrepareEvictionEvidence.add(name);
            }
          }
          ctx.abort("armed Schedule Owner eviction");
          throw new Error("Schedule Owner eviction did not interrupt the failpoint");
        });
      }
      const failures = scheduleFailures.get(name);

      if (failures?.[0] === point) {
        failures.shift();
        if (failures.length === 0) scheduleFailures.delete(name);

        return Effect.fail(ScheduleFailpointError.make({ point }));
      }
      if (point !== "schedule:admission:after") return Effect.void;
      const gate = schedulePauseGates.get(name);

      if (gate === undefined) return Effect.void;
      gate.signalReached();

      return Effect.promise(() => gate.released);
    }),
});

const schedulePolicyResources = new Map<
  string,
  {
    acquired: number;
    released: number;
    fail: boolean;
  }
>();

export const observeSchedulePolicyResources = (owner: ScheduleOwner) => {
  const probe = { acquired: 0, released: 0, fail: false };

  schedulePolicyResources.set(scheduleOwnerKey(owner), probe);

  return probe;
};

export const scheduleAuthorizer = (owner: ScheduleOwner) => {
  // This cached policy acquires no resources during construction. Each operation owns its scope.
  const scoped = <A, E>(operation: Effect.Effect<A, E>) =>
    Effect.scoped(
      Effect.gen(function* () {
        const probe = schedulePolicyResources.get(scheduleOwnerKey(owner));

        if (probe === undefined) return yield* operation;
        yield* Effect.acquireRelease(
          Effect.sync(() => {
            probe.acquired += 1;
          }),
          () =>
            Effect.sync(() => {
              probe.released += 1;
            }),
        );
        if (probe.fail)
          return yield* ScheduleStorageError.make({
            operation: "test scoped Schedule policy",
            reason: "unavailable",
          });

        return yield* operation;
      }),
    );

  return {
    manage: () => scoped(Effect.void),
    prepare: () =>
      Effect.suspend(() => {
        const key = scheduleOwnerKey(owner);
        const hold = scheduleAuthorizationFailureHolds.get(key);

        if (hold?.held === true) {
          hold.failureCount += 1;
          const pending = hold.waiters.splice(0);

          for (const waiter of pending) {
            if (hold.failureCount >= waiter.minimum) waiter.resolve(hold.failureCount);
            else hold.waiters.push(waiter);
          }

          return Effect.fail(
            ScheduleStorageError.make({
              operation: "test Schedule authorization",
              reason: "unavailable",
            }),
          );
        }

        return Effect.succeed({ policyId: "cf-test-policy", decisionId: "cf-test-allow" });
      }).pipe(scoped),
  };
};

export const submitOptions = (thread: string, idempotencyKey: string): DurableSubmitOptions => ({
  threadId: decodeThreadId(thread),
  principal: TEST_PRINCIPAL,
  idempotencyKey: decodeIdempotencyKey(idempotencyKey),
  definitions: TEST_DIGESTS,
});

// ---------------------------------------------------------------------------
// Host-supplied context preparation (issue #49)
// ---------------------------------------------------------------------------

const COMPACTION_MARKER = "[host-compacted-context]";
const COMPACTION_FAILURE_MARKER = "[host-compactor-failure]";

export interface ContextCompactorProbe {
  readonly acquisitions: number;
  readonly releases: number;
  readonly invocations: number;
  readonly sourceMessageCounts: ReadonlyArray<number>;
}

const contextCompactorProbes = new Map<string, ContextCompactorProbe>();

const updateContextCompactorProbe = (
  threadId: string,
  update: (probe: ContextCompactorProbe) => ContextCompactorProbe,
): void => {
  contextCompactorProbes.set(
    threadId,
    update(
      contextCompactorProbes.get(threadId) ?? {
        acquisitions: 0,
        releases: 0,
        invocations: 0,
        sourceMessageCounts: [],
      },
    ),
  );
};

export const contextCompactorProbe = (threadId: string): ContextCompactorProbe =>
  contextCompactorProbes.get(threadId) ?? {
    acquisitions: 0,
    releases: 0,
    invocations: 0,
    sourceMessageCounts: [],
  };

const contextCompactorLayer = (threadId: string) =>
  Layer.effect(
    ContextCompactor,
    Effect.acquireRelease(
      Effect.gen(function* () {
        yield* Effect.sync(() =>
          updateContextCompactorProbe(threadId, (probe) => ({
            ...probe,
            acquisitions: probe.acquisitions + 1,
          })),
        );

        return ContextCompactor.of({
          estimate: (messages) =>
            messages.some((message) => message.role === "assistant")
              ? 2_001
              : estimatePromptTokens(messages),
          compact: (request) =>
            Stream.fromEffect(
              Effect.gen(function* () {
                yield* Effect.sync(() =>
                  updateContextCompactorProbe(threadId, (probe) => ({
                    ...probe,
                    invocations: probe.invocations + 1,
                    sourceMessageCounts: [
                      ...probe.sourceMessageCounts,
                      request.source.content.length,
                    ],
                  })),
                );
                const sourceText = JSON.stringify(request.source);

                if (sourceText.includes(COMPACTION_FAILURE_MARKER)) {
                  return yield* CompactionError.make({
                    message: "Context compaction refused",
                    cause: "the host compactor refused this context",
                  });
                }

                return {
                  kind: "summarize" as const,
                  through:
                    request.source.content.findLastIndex(
                      (message) => message.role === "assistant",
                    ) + 1,
                  summary: COMPACTION_MARKER,
                };
              }),
            ),
        });
      }),
      () =>
        Effect.sync(() =>
          updateContextCompactorProbe(threadId, (probe) => ({
            ...probe,
            releases: probe.releases + 1,
          })),
        ),
    ),
  );

/** A closed generic run-context Layer; BrowserCrypto remains owned by the platform assembly. */
export const makeContextCompactorRunContextLayer = (threadId: string) =>
  contextCompactorRunContextLayer.pipe(Layer.provide(contextCompactorLayer(threadId)));

const contextAuthorizationProbes = new Map<string, { acquisitions: number; calls: number }>();

export const contextAuthorizationProbe = (threadId: string) =>
  contextAuthorizationProbes.get(threadId);

/** Independent policy: the compactor fixture may prepare prompts but cannot authorize Tools. */
export const makeContextAuthorizationLayer = (threadId: string) =>
  Layer.effect(
    RunToolAuthorization,
    Effect.sync(() => {
      const probe = contextAuthorizationProbes.get(threadId) ?? { acquisitions: 0, calls: 0 };

      probe.acquisitions += 1;
      contextAuthorizationProbes.set(threadId, probe);

      return RunToolAuthorization.of({
        authorize: () =>
          Effect.sync(() => {
            probe.calls += 1;

            return { _tag: "denied" as const, reason: "host denied Tool execution" };
          }),
      });
    }),
  );

// ---------------------------------------------------------------------------
// Scripted prompt-aware models
// ---------------------------------------------------------------------------

const usage = { inputTokens: {}, outputTokens: {} };

export const FINAL_ANSWER = '{"answer":"done"}';

export const finalParts = (text: string): ReadonlyArray<Response.StreamPartEncoded> => [
  { type: "text-start", id: "answer" },
  { type: "text-delta", id: "answer", delta: text },
  { type: "text-end", id: "answer" },
  { type: "finish", reason: "stop", usage },
];

export const SEARCH_CALL_ID = "search-1";
export const BOOK_CALL_ID = "book-1";
export const ITINERARY_CALL_ID = "itinerary-1";
/** The booking Tool Call identity as the branded type the resolution commands carry. */
export const BOOK_TOOL_CALL_ID: ToolCallId = Schema.decodeSync(ToolCallId)(BOOK_CALL_ID);

const searchToolCallParts: ReadonlyArray<Response.StreamPartEncoded> = [
  {
    type: "tool-call",
    id: SEARCH_CALL_ID,
    name: "search",
    params: { query: "sea" },
    providerExecuted: false,
  },
  { type: "finish", reason: "tool-calls", usage },
];

const bookToolCallParts = (ref: string): ReadonlyArray<Response.StreamPartEncoded> => [
  {
    type: "tool-call",
    id: BOOK_CALL_ID,
    name: "book",
    params: { ref },
    providerExecuted: false,
  },
  { type: "finish", reason: "tool-calls", usage },
];

const itineraryToolCallParts = (ref: string): ReadonlyArray<Response.StreamPartEncoded> => [
  {
    type: "tool-call",
    id: ITINERARY_CALL_ID,
    name: "itinerary",
    params: { ref },
    providerExecuted: false,
  },
  { type: "finish", reason: "tool-calls", usage },
];

/** Extract the Thread-unique `ref` the instructions embed as `[ref:...]`. */
const refFromPrompt = (promptJson: string): string => {
  const match = /\[ref:([^\]]+)\]/.exec(promptJson);

  return match?.[1] ?? "unknown-ref";
};

/**
 * A PROMPT-AWARE scripted model: the response is a pure function of the normalized request,
 * never of an in-memory call counter. This is what makes ONE registered Binding correct at
 * every resume point — a fresh incarnation's Attempt sees the committed history in its
 * prompt and the model continues from exactly there, the way a real model would.
 */
const promptAwareModel = (
  name: string,
  decide: (promptJson: string) => Stream.Stream<Response.StreamPartEncoded>,
) =>
  Model.make(
    "scripted",
    name,
    Layer.effect(
      LanguageModel.LanguageModel,
      LanguageModel.make({
        generateText: () => Effect.succeed([]),
        streamText: (options) =>
          Stream.unwrap(Effect.sync(() => decide(JSON.stringify(options.prompt)))),
      }),
    ),
  );

// ---------------------------------------------------------------------------
// Agent definitions (crash-fixture shapes, workerd-safe handlers)
// ---------------------------------------------------------------------------

const FixtureInput = Schema.Struct({ question: Schema.String, ref: Schema.String });
const FixtureOutput = Schema.Struct({ answer: Schema.String });

const fixturePolicy = AgentPolicy.make({
  maxTurns: 3,
  maxToolCalls: 2,
  maxDuration: "30 seconds",
  toolConcurrency: 1,
});

/** No tools: one final model response per Run. */
export const plannerDefinition = Agent.make("cf-planner", {
  input: FixtureInput,
  output: FixtureOutput,
  instructions: ({ question, ref }) => `Answer ${question} as JSON. [ref:${ref}]`,
  toolkit: Toolkit.empty,
  policy: fixturePolicy,
});

/** Issue #49 fixture: its result proves which prompt crossed the real model boundary. */
export const contextCompactorDefinition = Agent.make("cf-context-compactor", {
  input: FixtureInput,
  output: FixtureOutput,
  instructions: ({ question, ref }) => `Answer ${question} as JSON. [ref:${ref}]`,
  toolkit: Toolkit.empty,
  policy: AgentPolicy.make({ ...fixturePolicy, contextTokenLimit: 2_000 }),
});

// `readonly`: no external mutation, so a crash between start and settlement is a free re-run
// (the P4 canonical shape; an unannotated Tool would fail closed to `uncertain`).
const SearchTool = Tool.make("search", {
  parameters: Schema.Struct({ query: Schema.String }),
  success: Schema.Struct({ available: Schema.Boolean }),
}).annotate(ToolExecutionClass, "readonly");

const searchTools = Toolkit.make(SearchTool);

export const searchToolLayer = searchTools.toLayer({
  search: () => Effect.succeed({ available: true }),
});

export const searchDefinition = Agent.make("cf-search", {
  input: FixtureInput,
  output: FixtureOutput,
  instructions: ({ ref }) => `Search before answering. [ref:${ref}]`,
  toolkit: searchTools,
  policy: fixturePolicy,
});

/** UNANNOTATED booking tool: fails closed to `uncertain` (prepared/settled protocol). */
const BookTool = Tool.make("book", {
  parameters: Schema.Struct({ ref: Schema.String }),
  success: Schema.Struct({ confirmation: Schema.String }),
});

export const bookTools = Toolkit.make(BookTool);
/** Lose the RPC reply after the external action, without claiming a safe-to-retry failure. */
export const lostBookReplies = new Set<string>();

export const bookToolLayer = bookTools.toLayer({
  book: ({ ref }) =>
    Effect.gen(function* () {
      const confirmation = `confirmed-${ref}`;

      recordSupplierCall("book", ref, confirmation);
      if (lostBookReplies.delete(ref)) return yield* Effect.die("external reply lost");

      return { confirmation };
    }),
});

export const bookDefinition = Agent.make("cf-book", {
  input: FixtureInput,
  output: FixtureOutput,
  instructions: ({ ref }) => `Book it. [ref:${ref}]`,
  toolkit: bookTools,
  policy: fixturePolicy,
});

/** Approval-gated booking tool; unannotated → fail-closed `uncertain` execution class. */
const BookApprovalTool = Tool.make("book", {
  parameters: Schema.Struct({ ref: Schema.String }),
  success: Schema.Struct({ confirmation: Schema.String }),
  needsApproval: true,
});

const approvalTools = Toolkit.make(BookApprovalTool);

export const approvalToolLayer = approvalTools.toLayer({
  book: ({ ref }) =>
    Effect.sync(() => {
      const confirmation = `confirmed-${ref}`;

      recordSupplierCall("book", ref, confirmation);

      return { confirmation };
    }),
});

export const approvalDefinition = Agent.make("cf-book-approval", {
  input: FixtureInput,
  output: FixtureOutput,
  instructions: ({ ref }) => `Book after approval. [ref:${ref}]`,
  toolkit: approvalTools,
  policy: fixturePolicy,
});

/** Durable Tool: declaring `DurableStep` as a dependency is what makes it durable. */
const ItineraryTool = Tool.make("itinerary", {
  parameters: Schema.Struct({ ref: Schema.String }),
  success: Schema.Struct({ state: Schema.String }),
  failure: DurableStepError,
  dependencies: [DurableStep],
});

const itineraryTools = Toolkit.make(ItineraryTool);

export const itineraryToolLayer = itineraryTools.toLayer({
  itinerary: ({ ref }) =>
    Effect.gen(function* () {
      yield* Effect.sync(() => recordSupplierCall("itinerary-enter", ref, `enter-${ref}`));
      const step = yield* DurableStep;

      const flight = yield* step.do(
        "reserve-flight",
        Schema.String,
        Effect.sync(() => {
          const value = `flight-${ref}`;

          recordSupplierCall("reserve-flight", ref, value);

          return value;
        }),
      );

      const lodging = yield* step.do(
        "reserve-lodging",
        Schema.String,
        Effect.sync(() => {
          const value = `lodging-${ref}`;

          recordSupplierCall("reserve-lodging", ref, value);

          return value;
        }),
      );

      return { state: `${flight}+${lodging}` };
    }),
});

export const itineraryDefinition = Agent.make("cf-itinerary", {
  input: FixtureInput,
  output: FixtureOutput,
  instructions: ({ ref }) => `Reserve the itinerary. [ref:${ref}]`,
  toolkit: itineraryTools,
  policy: fixturePolicy,
});

/** Host agent whose FIRST model request hangs on the release gate (join/renewal rows). */
export const joinDefinition = Agent.make("cf-join-host", {
  input: FixtureInput,
  output: FixtureOutput,
  instructions: ({ ref }) => `Search, wait for the gate, and fold in queued input. [ref:${ref}]`,
  toolkit: searchTools,
  policy: fixturePolicy,
});

// ---------------------------------------------------------------------------
// Models
// ---------------------------------------------------------------------------

export const plannerModel = promptAwareModel("cf-planner", () =>
  Stream.fromIterable(finalParts(FINAL_ANSWER)),
);

const contextCompactorModel = promptAwareModel("cf-context-compactor", (promptJson) =>
  Stream.fromIterable(
    finalParts(
      promptJson.includes(COMPACTION_MARKER)
        ? '{"answer":"compacted"}'
        : '{"answer":"uncompacted"}',
    ),
  ),
);

const searchModel = promptAwareModel("cf-search", (promptJson) =>
  promptJson.includes(SEARCH_CALL_ID)
    ? Stream.fromIterable(finalParts(FINAL_ANSWER))
    : Stream.fromIterable(searchToolCallParts),
);

const bookModel = promptAwareModel("cf-book", (promptJson) =>
  promptJson.includes(BOOK_CALL_ID)
    ? Stream.fromIterable(finalParts(FINAL_ANSWER))
    : Stream.fromIterable(bookToolCallParts(refFromPrompt(promptJson))),
);

const approvalModel = promptAwareModel("cf-book-approval", (promptJson) =>
  promptJson.includes(BOOK_CALL_ID)
    ? Stream.fromIterable(finalParts(FINAL_ANSWER))
    : Stream.fromIterable(bookToolCallParts(refFromPrompt(promptJson))),
);

const itineraryModel = promptAwareModel("cf-itinerary", (promptJson) =>
  promptJson.includes(ITINERARY_CALL_ID)
    ? Stream.fromIterable(finalParts(FINAL_ANSWER))
    : Stream.fromIterable(itineraryToolCallParts(refFromPrompt(promptJson))),
);

/** First request per committed history: hang on the gate, then declare the search call. */
const joinModel = promptAwareModel("cf-join-host", (promptJson) =>
  promptJson.includes(SEARCH_CALL_ID)
    ? Stream.fromIterable(finalParts(FINAL_ANSWER))
    : Stream.fromEffectDrain(awaitGate(refFromPrompt(promptJson))).pipe(
        Stream.concat(Stream.fromIterable(searchToolCallParts)),
      ),
);

/**
 * Reconciliation policy for the fixture toolkits (durability §10): the re-enterable Durable
 * Tool `itinerary` is `SafeToRetry` (its Steps replay from their exactly-once records);
 * every ordinary call keeps the fail-closed default answer — no proof means Uncertain, so
 * the `book` rows still block on a durable Unknown Outcome until `resolveUnknown`.
 */
export const fixtureReconcilerLayer: Layer.Layer<ToolReconciler> = Layer.succeed(ToolReconciler)({
  reconcile: (evidence) =>
    Effect.sync(() =>
      evidence.toolName === "itinerary"
        ? ReconciliationSafeToRetry.make()
        : ReconciliationUncertain.make({
            reason: `No proof exists for ${evidence.toolName}; fail closed`,
          }),
    ),
});

// ---------------------------------------------------------------------------
// Registered worker bindings (S2 shape: one resolver serves every fixture lane)
// ---------------------------------------------------------------------------

/**
 * Every fixture Binding, captured with its tool layers: the
 * Thread Object resolves each claimed head's stored `(agentId, digests)` to exactly
 * one of these before any code runs (SUB-023).
 */
export const makeTestBindings: Effect.Effect<ReadonlyArray<ResolvedBinding>> = Effect.gen(
  function* () {
    const planner: ResolvedBinding = yield* DurableWorkerBinding.make(
      Agent.withModel(plannerDefinition, plannerModel),
      TEST_DIGESTS,
    );

    const contextCompactor: ResolvedBinding = yield* DurableWorkerBinding.make(
      Agent.withModel(contextCompactorDefinition, contextCompactorModel),
      TEST_DIGESTS,
    );

    const search: ResolvedBinding = yield* DurableWorkerBinding.make(
      Agent.withModel(searchDefinition, searchModel),
      TEST_DIGESTS,
    ).pipe(Effect.provide(searchToolLayer));

    const book: ResolvedBinding = yield* DurableWorkerBinding.make(
      Agent.withModel(bookDefinition, bookModel),
      TEST_DIGESTS,
    ).pipe(Effect.provide(bookToolLayer));

    const approval: ResolvedBinding = yield* DurableWorkerBinding.make(
      Agent.withModel(approvalDefinition, approvalModel),
      TEST_DIGESTS,
    ).pipe(Effect.provide(approvalToolLayer));

    const itinerary: ResolvedBinding = yield* DurableWorkerBinding.make(
      Agent.withModel(itineraryDefinition, itineraryModel),
      TEST_DIGESTS,
    ).pipe(Effect.provide(itineraryToolLayer));

    const join: ResolvedBinding = yield* DurableWorkerBinding.make(
      Agent.withModel(joinDefinition, joinModel),
      TEST_DIGESTS,
    ).pipe(Effect.provide(searchToolLayer));

    return [planner, contextCompactor, search, book, approval, itinerary, join];
  },
);

export const testRuntimeLayer = Layer.unwrap(Effect.map(makeTestBindings, layerFromBindings));

export const registrationDefinitions = DefinitionDigestInput.make({
  agent: { id: plannerDefinition.id, revision: 1 },
  model: { provider: "scripted", name: "cf-planner" },
  tools: [],
});

import {
  Cause,
  Clock,
  Context,
  DateTime,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Layer,
  Option,
  Random,
  Ref,
  Result,
  Schema,
  Scope,
  Semaphore,
  Stream,
  Struct,
} from "effect";
import { type DurableBindingFailure } from "effect-agent/agent-registration";
import {
  DurableAgentRuntime,
  DurableRuntimeConfig,
  isolateRecovery,
  RecoveryBlocked,
  RecoveryFailure,
  type DurableWorkerFailure,
  type RecoveryReport,
  RecoverySweepResult,
} from "effect-agent/durable-agent-runtime";
import { ThreadId, SubmissionId } from "effect-agent/identifiers";
import {
  AbortIntentRequest,
  SubmissionLedger,
  type SubmissionWorkItem,
} from "effect-agent/submission-ledger";
import {
  ThreadProjectionMaintenance,
  type ThreadProjectionError,
} from "effect-agent/thread-projection-maintenance";
import { WakeScheduler } from "effect-agent/wake-scheduler";
import { DurableObjectStorage } from "effect-cf";
import { SqlClient } from "effect/unstable/sql/SqlClient";

import { DurableObjectContext } from "./CloudflareBindings.ts";
import { AuxiliaryDispatchMillis, CloudflareDurableRuntimeConfig } from "./CloudflareConfig.ts";
import { safeCauseMessage } from "./internal/boundary.ts";
import * as DueQueue from "./internal/due-queue.ts";

/**
 * The single multiplexed Durable Object alarm (decision D-P6-2). A Durable Object has ONE
 * alarm slot; due work (lease expiry, settlement
 * and abort re-checks, retry backoff) multiplexes into one idempotent maintenance pass, and
 * the slot always holds the EARLIEST deadline any caller asked for.
 *
 * The alarm invariant (plan §1.4): every committed actionable mutation carries a newer durable
 * maintenance generation and a committed alarm. Stable externally-driven waits may be
 * nonterminal without retaining an alarm; their resolving mutation advances the generation and
 * restores the alarm atomically.
 */

/** The Durable Object alarm API failed; surfaces on host entry points as a typed refusal. */
export class DurableAlarmError extends Schema.TaggedError<DurableAlarmError>()(
  "DurableAlarmError",
  {
    operation: Schema.String,
    message: Schema.String,
    cause: Schema.optionalKey(Schema.Defect()),
  },
) {}

const alarmFailure =
  (operation: string) =>
  (cause: unknown): DurableAlarmError =>
    DurableAlarmError.make({
      operation,
      message: safeCauseMessage(cause, "The Cloudflare alarm API failed without a diagnostic"),
      cause,
    });

// SQL and raw KV/alarm operations share one physical SQLite transaction. Reserve its
// connection for each short storage operation, never around a mutation or snapshot body.
const makeStorageEffect = Effect.gen(function* () {
  const sql = yield* SqlClient;
  const { ctx } = yield* DurableObjectContext;
  const invalidate = Effect.sync(() => DueQueue.invalidate(ctx.storage));

  return <A, R>(operation: string, execute: Effect.Effect<A, DurableAlarmError, R>) =>
    Effect.flatMap(Effect.serviceOption(sql.transactionService), (current) => {
      const body = Effect.uninterruptible(execute);

      // An enclosing source transaction can still roll back after a successful schedule.
      // Never retain its speculative queue view beyond this nested storage operation.
      return current._tag === "Some"
        ? body.pipe(Effect.ensuring(invalidate))
        : Effect.scoped(
            Effect.andThen(
              sql.reserve.pipe(Effect.mapError(alarmFailure(operation))),
              body.pipe(Effect.onExit((exit) => (Exit.isFailure(exit) ? invalidate : Effect.void))),
            ),
          );
    });
});

const makeStorageOperation = Effect.map(
  makeStorageEffect,
  (run) =>
    <A>(operation: string, execute: () => Promise<A>) =>
      run(operation, Effect.tryPromise({ try: execute, catch: alarmFailure(operation) })),
);

/** `ctx.storage` alarm slot as an Effect service; storage is truth, never a memory field. */
export class DurableAlarmService extends Context.Service<
  DurableAlarmService,
  {
    /** The scheduled deadline in epoch milliseconds, if any. */
    readonly scheduled: Effect.Effect<Option.Option<number>, DurableAlarmError>;
    /** Replace the slot with this deadline. */
    readonly scheduleAt: (epochMillis: number) => Effect.Effect<void, DurableAlarmError>;
    /** Keep the EARLIER of the existing deadline and this one (the multiplexing rule). */
    readonly ensureScheduledBy: (epochMillis: number) => Effect.Effect<void, DurableAlarmError>;
    /**
     * Arm an immediate alarm (the durable, coalescing local wake) — DEFERRED while a
     * maintenance pass is executing. Workerd cancels an in-flight alarm handler when a new
     * EARLIER deadline is written during its execution (`requestScheduledAlarm`), and the
     * maintenance pass runs INSIDE the alarm handler: an immediate wake landing mid-pass
     * (a routed port mutation, a sibling's `wake()`, the coordinator's own local notify)
     * would kill the running Attempt — manufacturing an ownership loss no real eviction
     * caused, and routing open uncertain-class Tool Calls into spurious Unknown Outcomes.
     * Deferral is contract-safe: wakes are droppable hints, every mutating entry point
     * pre-arms BEFORE its first durable mutation (the alarm invariant never rests on this
     * call). The pass's durable generation check observes any racing mutation, so the
     * in-memory hint does not need to be flushed after a stable wait is acknowledged.
     */
    readonly scheduleNow: Effect.Effect<void, DurableAlarmError>;
    /**
     * Run one maintenance pass with wake deferral (see `scheduleNow`). Calls made while `body`
     * executes are droppable promptness hints; correctness rests on the durable generation.
     */
    readonly withWakesDeferred: <A, E, R>(body: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>;
    /** Clear the slot; correctness-sensitive clears live in maintenance generation transactions. */
    readonly cancel: Effect.Effect<void, DurableAlarmError>;
  }
>()("@effect-agent/platform-cloudflare/DurableAlarmService") {
  static readonly layer: Layer.Layer<DurableAlarmService, never, DurableObjectContext | SqlClient> =
    Layer.effect(DurableAlarmService)(
      Effect.gen(function* () {
        const { ctx } = yield* DurableObjectContext;
        /**
         * In-memory pass bookkeeping — a pure CACHE, never state: a fresh incarnation has no
         * running pass, and a deferred wake lost to eviction was only ever a promptness hint
         * on top of the already-committed pre-armed alarm.
         */
        const runningPasses = yield* Ref.make(0);

        const storageOperation = yield* makeStorageOperation;

        const scheduled = storageOperation("get alarm", () => ctx.storage.getAlarm()).pipe(
          Effect.map((deadline) =>
            deadline === null ? Option.none<number>() : Option.some(deadline),
          ),
        );

        const scheduleAt = (epochMillis: number) =>
          storageOperation("set alarm", () => ctx.storage.setAlarm(epochMillis));

        const ensureScheduledBy = (epochMillis: number) =>
          storageOperation("ensure alarm", () =>
            ctx.storage.transaction(async (transaction) => {
              const existing = await transaction.getAlarm();

              if (existing === null || existing > epochMillis) {
                await transaction.setAlarm(epochMillis);
              }
            }),
          );

        const armNow = Clock.currentTimeMillis.pipe(
          Effect.flatMap((now) => ensureScheduledBy(now)),
        );

        const scheduleNow = Ref.get(runningPasses).pipe(
          Effect.flatMap((passes) => (passes > 0 ? Effect.void : armNow)),
        );

        const withWakesDeferred = <A, E, R>(body: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> =>
          Ref.update(runningPasses, (passes) => passes + 1).pipe(
            Effect.andThen(body),
            Effect.ensuring(Ref.update(runningPasses, (passes) => passes - 1)),
          );

        const cancel = storageOperation("delete alarm", () => ctx.storage.deleteAlarm());

        return DurableAlarmService.of({
          scheduled,
          scheduleAt,
          ensureScheduledBy,
          scheduleNow,
          withWakesDeferred,
          cancel,
        });
      }),
    );
}

/** What one maintenance pass did — auditable evidence mirroring `NodeDurableHost`'s report. */
export class MaintenancePassReport extends Schema.Class<MaintenancePassReport>(
  "@effect-agent/platform-cloudflare/MaintenancePassReport",
)({
  /** `caught-up` ran no runtime work (publication may be pending); `actionable` ran recovery. */
  phase: Schema.Literals(["caught-up", "actionable"]),
  /** Recovery decisions and Thread faults observed during this event. */
  recovered: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  /** Head Attempts settled during the event. Joined input may settle with each head. */
  settled: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  /** Submissions still nonterminal after the pass (suspended/unknown lanes stay honest). */
  nonterminal: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  /** `rearmed` for dirty/autonomous work, `cleared` for stable waits or settlement. */
  alarm: Schema.Literals(["rearmed", "cleared"]),
}) {}

/** Fault boundaries around every maintenance-owned durable mutation. */
export type ThreadMaintenanceFailpointLocation =
  | "maintenance:dirty:before"
  | "maintenance:dirty:after"
  | "maintenance:mutation:armed"
  | "maintenance:mutation:finished"
  | "maintenance:ensure:before"
  | "maintenance:ensure:after"
  | "maintenance:begin:before"
  | "maintenance:begin:after"
  | "maintenance:select:before"
  | "maintenance:select:after"
  | "maintenance:binding-retry:before"
  | "maintenance:binding-retry:after"
  | "maintenance:recovery-status:before"
  | "maintenance:recovery-status:after"
  | "maintenance:retry:before"
  | "maintenance:retry:after"
  | "maintenance:checkpoint:before"
  | "maintenance:checkpoint:after"
  | "maintenance:finish:before"
  | "maintenance:finish:after";

export type ThreadMaintenanceFailpointHandler = (
  location: ThreadMaintenanceFailpointLocation,
) => Effect.Effect<void>;

/** Test-only fault authority; production uses the inert layer. */
export class ThreadMaintenanceFailpoint extends Context.Service<
  ThreadMaintenanceFailpoint,
  {
    readonly hit: ThreadMaintenanceFailpointHandler;
  }
>()("@effect-agent/platform-cloudflare/ThreadMaintenanceFailpoint") {
  static readonly layer = Layer.succeed(this)({ hit: () => Effect.void });
}

/**
 * Durable host publication of canonical records and ledger approval/abort/resolution intents.
 * The host owns schema-versioned cursors, destination idempotency and acknowledgement. Delivery
 * is at least once. Hooks must not write the alarm slot or mutate the supplied raw source ports.
 *
 * `invalidate` and `prepareGeneration` must be bounded local operations.
 * `prepareGeneration` durably invalidates a scan only when its generation changes; repeated
 * calls must preserve partial scan progress. It runs with no source mutation in flight.
 * Use this gate only for publication required before dependent native execution. Independent
 * UI relays and outboxes belong to ThreadHostMaintenance.
 * `drain` performs bounded delivery and returns its next deadline (None when caught up),
 * after persisting receipts and retries. A pending deadline
 * defers runtime recovery/Attempts, allowing committed host publications to drain first.
 * Unexpected hook failures leave the prearmed generation for retry. Hooks acquire per-call
 * resources with Effect.scoped; Layer construction owns incarnation resources (eviction need
 * not run finalizers). Do not hold a local hook behind network I/O or call back into producers.
 */
export interface ThreadPublicationService {
  readonly invalidate: Effect.Effect<void, DurableAlarmError>;
  readonly prepareGeneration: (generation: bigint) => Effect.Effect<void, DurableAlarmError>;
  readonly drain: Effect.Effect<Option.Option<number>, DurableAlarmError>;
}

/** Opt in with `ThreadObject.layer(registrations, { publication: Layer.effect(ThreadPublication)(...) })`. */
export class ThreadPublication extends Context.Service<
  ThreadPublication,
  ThreadPublicationService
>()("@effect-agent/platform-cloudflare/ThreadPublication") {
  static readonly layer = Layer.succeed(this)({
    invalidate: Effect.void,
    prepareGeneration: () => Effect.void,
    drain: Effect.succeed(Option.none()),
  });
}

/**
 * Host-assembled native message recovery. Preparation is a bounded local selection; the
 * driver bounds each actual Claim and persists its timeout/retry before `run` returns.
 * The alarm must not add a timer starting at selection: Claim setup and retry commits belong
 * to the driver. The prepared allowance decides only whether a wave fits this event.
 */
export const ThreadMessageDelivery = Context.Reference<{
  readonly prepare: Effect.Effect<
    {
      readonly timeoutMillis: number;
      readonly run: Effect.Effect<Option.Option<number>, DurableAlarmError>;
    },
    DurableAlarmError
  >;
}>("@effect-agent/platform-cloudflare/ThreadMessageDelivery", {
  defaultValue: () => ({
    prepare: Effect.succeed({ timeoutMillis: 1, run: Effect.succeed(Option.none()) }),
  }),
});

/**
 * One explicitly scheduled application obligation. IDs are stable and unique within the
 * physical Object; the `effect-agent:` prefix is reserved. There is no initial host wave.
 * Producers enroll only affected IDs through ThreadMutationGate.withMutation({ lanes })
 * or schedule(id, dueAt). A lane returns its next epoch-millisecond deadline (None = idle)
 * with its finite wave, after persisting claims, receipts and retries. No deadline callback
 * or polling is used. A racing producer keeps its newer revision due.
 *
 * The 1..300000ms allowance includes selection, dispatch, commits and scoped cleanup.
 * Failed waves retain independent backoff and are not retried in the same event. Delivery
 * remains at least once: persist exact envelopes and deduplicate by domain receipt identity.
 * Hooks never write the alarm slot. Required execution gates belong to ThreadPublication.
 */
export interface ThreadHostMaintenanceLane {
  readonly id: string;
  /** Omission runs concurrently with native work. After-native lanes get one due wave
   * after all admitted native Attempts and their scoped cleanup, even on native failure.
   * They share the event deadline and remain inside the maintenance pass permit.
   * Newly enrolled concurrent work yields this phase after scoped cleanup; unfinished
   * waves retain their due revision and domain receipts for the next alarm.
   */
  readonly phase?: "concurrent" | "after-native";
  readonly dispatchTimeoutMillis: number;
  readonly run: Effect.Effect<Option.Option<number>, DurableAlarmError, Scope.Scope>;
}

/** Independent lanes share the existing alarm; compose hosts by concatenating their lanes. */
export const ThreadHostMaintenance = Context.Reference<{
  readonly lanes: ReadonlyArray<ThreadHostMaintenanceLane>;
}>("@effect-agent/platform-cloudflare/ThreadHostMaintenance", {
  defaultValue: () => ({ lanes: [] }),
});

/** @internal Framework handlers are separate from host registrations and their namespace. */
export const ThreadNativeMaintenance = Context.Reference<{
  readonly lanes: ReadonlyArray<ThreadHostMaintenanceLane>;
}>("@effect-agent/platform-cloudflare/ThreadNativeMaintenance", {
  defaultValue: () => ({ lanes: [] }),
});

/** @internal Required host publication only; native lifecycle facts use a maintenance lane. */
export const publishCommitted = Effect.gen(function* () {
  const publication = yield* ThreadPublication;

  yield* publication.invalidate;
  yield* publication.drain;
}).pipe(
  Effect.catchCause((cause) =>
    Cause.hasInterrupts(cause)
      ? Effect.interrupt
      : Effect.logError("Thread publication deferred after source commit", cause),
  ),
);

const MaintenanceGeneration = Schema.BigIntFromString.check(
  Schema.isGreaterThanOrEqualToBigInt(0n),
);

/** A committed recovery transition for one affected Submission; never a Settlement. */
export class ThreadRecoveryFaultEvent extends Schema.Class<ThreadRecoveryFaultEvent>(
  "@effect-agent/platform-cloudflare/ThreadRecoveryFaultEvent",
)({
  schemaVersion: Schema.Literal(1),
  /** Monotonic within the physical Object; retained unchanged on delivery retries. */
  sequence: MaintenanceGeneration,
  transition: Schema.Literals(["created", "changed", "cleared"]),
  threadId: ThreadId,
  submissionId: SubmissionId,
  occurredAt: Schema.Finite,
  firstFailedAt: Schema.Finite,
  /** The current failure, or the last failure for a cleared event. */
  failure: RecoveryFailure,
}) {}

const recoveryEventKey = (sequence: bigint) => `effect-agent:thread-recovery-event:v1:${sequence}`;
const encodeRecoveryEvent = Schema.encodeSync(ThreadRecoveryFaultEvent);
const decodeRecoveryEvent = Schema.decodeUnknownSync(ThreadRecoveryFaultEvent);

/**
 * Ordered, at-least-once delivery through the existing host maintenance lane. Return only
 * after durably applying the event or retaining it in a host outbox. Deduplicate by physical
 * Object and sequence; callbacks may repeat after interruption. This is a trusted host hook,
 * not pre-authorized UI data: the host owns recipient authorization and safe presentation.
 * Capture services in the Layer. Do not call producers or write the raw alarm slot.
 */
export class ThreadRecoveryEvents extends Context.Service<
  ThreadRecoveryEvents,
  { readonly publish: (event: ThreadRecoveryFaultEvent) => Effect.Effect<void, DurableAlarmError> }
>()("@effect-agent/platform-cloudflare/ThreadRecoveryEvents") {}

class BindingWait extends Schema.Class<BindingWait>("BindingWait")({
  threadId: ThreadId,
  submissionId: SubmissionId,
  reportedAt: Schema.Finite,
}) {}

/**
 * A durable observation of blocked recovery, independent of the execution journal. This is
 * neither a Settlement nor proof that external effects did not happen. A successful recovery
 * sweep clears it; repair must preserve canonical history and the original admission identity.
 */
class ThreadRecoveryFault extends Schema.Class<ThreadRecoveryFault>(
  "@effect-agent/platform-cloudflare/ThreadRecoveryFault",
)({
  schemaVersion: Schema.Literal(1),
  threadId: ThreadId,
  firstFailedAt: Schema.Finite,
  lastFailedAt: Schema.Finite,
  /** Earliest automatic recovery retry; new admissions do not erase this deadline. */
  retryAt: Schema.Finite,
  /** Saturates at 2^31 - 1; one observation per Thread per recovery sweep. */
  attempts: Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(2_147_483_647)),
  failure: RecoveryFailure,
  /** Individually retained recipients survive settlement until their clear commits. */
  recipientCount: Schema.optionalKey(Schema.Natural),
}) {}

const recoveryFaultKey = (threadId: ThreadId) =>
  `effect-agent:thread-recovery-fault:v1:${threadId}`;

const decodeRecoveryFaultValue = Schema.decodeUnknownSync(ThreadRecoveryFault);

const decodeRecoveryFault = (threadId: ThreadId, encoded: unknown) => {
  const fault = decodeRecoveryFaultValue(encoded);

  if (fault.threadId !== threadId) throw new Error("Recovery status does not match its Thread key");

  return fault;
};

const encodeRecoveryFault = Schema.encodeSync(ThreadRecoveryFault);
const sameRecoveryFailure = Schema.toEquivalence(RecoveryFailure);
const decodeRecoveryRecipient = Schema.decodeUnknownSync(SubmissionId);
const encodeRecoveryRecipient = Schema.encodeSync(SubmissionId);

const recoveryRecipientKey = (threadId: ThreadId, index: number) =>
  `effect-agent:recovery-recipient:v1:${threadId.length}:${threadId}:${index}`;

const recoveryRecipientKeys = (fault: ThreadRecoveryFault, start: number) =>
  Array.from({ length: Math.min(128, (fault.recipientCount ?? 0) - start) }, (_, offset) =>
    recoveryRecipientKey(fault.threadId, start + offset),
  );

const readRecoveryRecipients = async (
  storage: Pick<DurableObjectTransaction, "get">,
  fault: ThreadRecoveryFault,
): Promise<Array<SubmissionId>> => {
  const ids: Array<SubmissionId> = [];

  for (let start = 0; start < (fault.recipientCount ?? 0); start += 128) {
    const keys = recoveryRecipientKeys(fault, start);
    const values = await storage.get(keys);

    for (const key of keys) ids.push(decodeRecoveryRecipient(values.get(key)));
  }

  return ids;
};

const appendRecoveryRecipients = async (
  transaction: DurableObjectTransaction,
  threadId: ThreadId,
  count: number,
  added: ReadonlyArray<SubmissionId>,
) => {
  for (let start = 0; start < added.length; start += 128)
    await transaction.put(
      Object.fromEntries(
        added
          .slice(start, start + 128)
          .map((id, offset) => [
            recoveryRecipientKey(threadId, count + start + offset),
            encodeRecoveryRecipient(id),
          ]),
      ),
    );
};

const recoveryEvent = (
  fault: ThreadRecoveryFault,
  submissionId: SubmissionId,
  transition: ThreadRecoveryFaultEvent["transition"],
  occurredAt: number,
): Omit<ThreadRecoveryFaultEvent, "sequence"> => ({
  schemaVersion: 1,
  transition,
  threadId: fault.threadId,
  submissionId,
  occurredAt,
  firstFailedAt: fault.firstFailedAt,
  failure: fault.failure,
});

interface NativePassResult {
  readonly phase: "caught-up" | "actionable";
  readonly nonterminal: number;
  readonly nextAttemptAt: number | undefined;
  readonly dispatched?: boolean;
}

/** Independent Threads share two bounded native slots; one active head per Thread. */
interface NativeDispatch {
  readonly scope: Scope.Scope;
  readonly active: Map<ThreadId, Fiber.Fiber<number, MaintenancePassFailure>>;
  readonly deferred: Set<ThreadId>;
  dispatched: number;
  settled: number;
  progressed: boolean;
  needsCheckpoint: boolean;
  generation?: bigint;
  scanGeneration?: bigint;
}

const nativeDispatchConcurrency = 2;
/** Post-native delivery shares a finite connection budget across independent host lanes. */
const afterNativeDispatchConcurrency = 2;

/** Event-local observations only; durable ingress keeps racing mutations dirty. */
interface NativeRecovery {
  readonly queue: Deferred.Deferred<ReadonlyArray<ThreadId>>;
  readonly pending: Set<ThreadId>;
  readonly loaded: Set<ThreadId>;
  readonly reports: Map<SubmissionId, RecoveryReport>;
  readonly faults: Map<ThreadId, ThreadRecoveryFault>;
  readonly recipients: Map<ThreadId, ReadonlySet<SubmissionId>>;
  observation?: {
    readonly generation: bigint;
    readonly activeAtStart: number;
  };
  started: boolean;
  needsCheckpoint: boolean;
  recovered: number;
  repaired: boolean;
}

interface MaintenanceObservation {
  readonly queue: Map<string, DueQueue.DueLane>;
  /** Initially available or serviced revisions; active producers are only failure observations. */
  readonly available: Map<string, number>;
  generation?: bigint;
  nativeOnly: boolean;
}

class MaintenanceRetry extends Schema.Class<MaintenanceRetry>("MaintenanceRetry")({
  generation: MaintenanceGeneration,
  notBefore: Schema.Finite,
  nativeOnly: Schema.Boolean,
  stalls: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0), Schema.isLessThanOrEqualTo(30)),
}) {}

/** Versioned, platform-private maintenance state stored through Durable Object KV. */
class ThreadMaintenanceState extends Schema.Class<ThreadMaintenanceState>(
  "@effect-agent/platform-cloudflare/ThreadMaintenanceState",
)({
  schemaVersion: Schema.Literal(1),
  dirty: MaintenanceGeneration,
  processed: MaintenanceGeneration,
  nonterminal: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  /** One physical-owner cursor; old single-lane records need no conversion. */
  lastServedThreadId: Schema.optionalKey(ThreadId),
  /** Rotate old recovery independently of dispatch, including after eviction or timeout. */
  lastRecoveredThreadId: Schema.optionalKey(ThreadId),
  /** Rotate bounded post-native admission across full events and Object eviction. */
  lastAfterNativeLaneId: Schema.optionalKey(DueQueue.LaneId),
  bindingRegistryKey: Schema.optionalKey(Schema.String),
  bindingWaits: Schema.optionalKey(Schema.Array(BindingWait)),
  recoveryEventSequence: Schema.optionalKey(MaintenanceGeneration),
  recoveryEventAcknowledged: Schema.optionalKey(MaintenanceGeneration),
  /** Absent on older records. A newer mutation makes this retry obsolete. */
  retry: Schema.optionalKey(MaintenanceRetry),
}) {}

const MAINTENANCE_STATE_KEY = "effect-agent:thread-maintenance:v1";
const decodeMaintenanceState = Schema.decodeUnknownSync(ThreadMaintenanceState);
const encodeMaintenanceState = Schema.encodeSync(ThreadMaintenanceState);

const hasRecoveryEvents = (state: ThreadMaintenanceState) =>
  (state.recoveryEventSequence ?? 0n) > (state.recoveryEventAcknowledged ?? 0n);

const initialMaintenanceState = (): ThreadMaintenanceState =>
  ThreadMaintenanceState.make({
    schemaVersion: 1,
    // Bootstrap Objects created by the pre-generation release without scanning the ledger in
    // the constructor. One useful pass classifies and acknowledges any existing obligation.
    dirty: 1n,
    processed: 0n,
    nonterminal: 0,
  });

const readMaintenanceState = async (
  transaction: Pick<DurableObjectTransaction, "get">,
): Promise<{ readonly state: ThreadMaintenanceState; readonly initialized: boolean }> => {
  const encoded = await transaction.get(MAINTENANCE_STATE_KEY);

  return encoded === undefined
    ? { state: initialMaintenanceState(), initialized: false }
    : { state: decodeMaintenanceState(encoded), initialized: true };
};

const ensureTransactionAlarmBy = async (
  transaction: DurableObjectTransaction,
  deadline: number,
): Promise<void> => {
  const scheduled = await transaction.getAlarm();

  if (scheduled === null || scheduled > deadline) {
    await transaction.setAlarm(deadline);
  }
};

const stableExternalWait = (
  snapshot: SubmissionWorkItem,
  reports: ReadonlyMap<string, RecoveryReport>,
): boolean => {
  const report = reports.get(snapshot.submissionId);
  const decision = report?.decision._tag;

  // An accepted abort still owes cleanup/settlement even if its claim was deferred this pass.
  if (decision === "SettleAborted") return false;
  switch (snapshot.state) {
    case "suspended":
    case "joined":
      return true;
    case "unknown":
      return report?.disposition === "unknown";
    case "admitted":
      return reports.get(snapshot.submissionId)?.decision._tag === "AwaitParentEstablishment";
    case "input-applied":
    case "joining":
    case "ready":
    case "running":
    case "settled":
    case "terminalizing":
      return false;
  }
};

/**
 * Shared prearm/acknowledgement boundary for ingress and runtime-owned producers.
 * `ThreadObject.layer` provides this same instance in its Services. Rebuilt runtime/maintenance
 * Layers must reuse that instance; a second gate cannot observe the native producers' activity.
 */
export class ThreadMutationGate extends Context.Service<
  ThreadMutationGate,
  {
    readonly withMutation: <A, E, R>(
      body: Effect.Effect<A, E, R>,
      /**
       * Native admission, approval, abort and unknown resolution keep the default true.
       * Host producers use false and name only the lanes for which they create work.
       * Receipt-only bookkeeping with no new obligation names no lanes. Enrollment and
       * the alarm commit before the body; a failed body may leave a harmless discovery wave.
       */
      options?: { readonly invalidatesRecovery?: boolean; readonly lanes?: ReadonlyArray<string> },
    ) => Effect.Effect<A, E | DurableAlarmError, R>;
    /** Enroll a known lane and keep its earliest deadline. Call inside the local source
     * transaction. Remote sources retain a scheduling notice with their work and retry its
     * delivery here; prearming alone cannot fence a remote commit after Object eviction. */
    readonly schedule: (id: string, dueAt: number) => Effect.Effect<void, DurableAlarmError>;
    /** In-incarnation notification; the durable due queue is the recovery authority. */
    readonly activeLanes: Effect.Effect<ReadonlySet<string>>;
    readonly revision: Effect.Effect<number>;
    readonly awaitChange: (revision: number) => Effect.Effect<void>;
    readonly withSnapshot: <A, E, R>(
      body: (active: number) => Effect.Effect<A, E, R>,
    ) => Effect.Effect<A, E, R>;
  }
>()("@effect-agent/platform-cloudflare/internal/ThreadMutationGate") {
  static readonly layer = Layer.effect(this)(
    Effect.gen(function* () {
      const { ctx } = yield* DurableObjectContext;
      const dueQueue = DueQueue.make(ctx.storage);
      const config = yield* CloudflareDurableRuntimeConfig;
      const failpoint = yield* ThreadMaintenanceFailpoint;
      // A fresh incarnation has no live mutations; durable generations survive eviction.
      const activeMutations = yield* Ref.make(0);
      const activeLanes = new Map<string, number>();
      const generationGate = yield* Semaphore.make(1);
      let revision = 0;
      let changed = yield* Deferred.make<void>();

      const notify = Effect.gen(function* () {
        const previous = changed;

        changed = yield* Deferred.make<void>();
        revision++;
        yield* Deferred.succeed(previous, undefined);
      });

      const minimumAlarmDelay = Math.max(1, Math.ceil(config.alarmBackoffBase / 2));

      const runTransaction = yield* makeStorageOperation;

      yield* runTransaction("initialize maintenance due queue", async () => {
        dueQueue.initialize();
        dueQueue.register(DueQueue.Native);
      });

      const schedule = Effect.fn("ThreadMutationGate.schedule")(function* (
        id: string,
        dueAt: number,
      ) {
        const now = yield* Clock.currentTimeMillis;

        yield* runTransaction("schedule maintenance lane", () =>
          ctx.storage.transaction(async (transaction) => {
            dueQueue.dirty(id, dueAt);
            await ensureTransactionAlarmBy(transaction, Math.max(dueAt, now + minimumAlarmDelay));
          }),
        );
        yield* notify;
      });

      const beginMutation = Effect.fn("ThreadMaintenance.beginMutation")(function* (
        invalidatesRecovery: boolean,
        lanes: ReadonlyArray<string>,
      ) {
        yield* failpoint.hit("maintenance:dirty:before");
        const now = yield* Clock.currentTimeMillis;

        if (invalidatesRecovery || lanes.length > 0)
          yield* runTransaction("advance maintenance generation", () =>
            ctx.storage.transaction(async (transaction) => {
              const { state, initialized } = await readMaintenanceState(transaction);

              if (invalidatesRecovery) {
                dueQueue.dirty(DueQueue.Native, now);
                dueQueue.dirty(DueQueue.Publication, now);
              }
              for (const id of lanes) dueQueue.dirty(id, now);

              const next = ThreadMaintenanceState.make({
                ...state,
                dirty: state.dirty + (invalidatesRecovery ? 1n : 0n),
              });

              if (invalidatesRecovery || !initialized)
                await transaction.put(MAINTENANCE_STATE_KEY, encodeMaintenanceState(next));
              // The earliest configured retry bounds a newly actionable mutation without relying
              // on its best-effort immediate wake hint.
              await ensureTransactionAlarmBy(transaction, now + minimumAlarmDelay);
            }),
          );
        yield* failpoint.hit("maintenance:dirty:after");
        yield* Ref.update(activeMutations, (active) => active + 1);

        const enrolled = new Set([
          ...lanes,
          ...(invalidatesRecovery ? [DueQueue.Native, DueQueue.Publication] : []),
        ]);

        for (const id of enrolled) activeLanes.set(id, (activeLanes.get(id) ?? 0) + 1);

        return enrolled;
      });

      const releaseLanes = (enrolled: Set<string>, committed = false) =>
        Effect.sync(() => {
          for (const id of enrolled) {
            if (committed && (id === DueQueue.Native || id === DueQueue.Publication)) continue;
            const remaining = (activeLanes.get(id) ?? 1) - 1;

            if (remaining === 0) activeLanes.delete(id);
            else activeLanes.set(id, remaining);
            enrolled.delete(id);
          }
        });

      const endMutation = (enrolled: Set<string>) =>
        generationGate.withPermit(
          releaseLanes(enrolled).pipe(
            Effect.andThen(Ref.update(activeMutations, (active) => Math.max(0, active - 1))),
          ),
        );

      const withMutation = <A, E, R>(
        body: Effect.Effect<A, E, R>,
        options?: {
          readonly invalidatesRecovery?: boolean;
          readonly lanes?: ReadonlyArray<string>;
        },
      ): Effect.Effect<A, E | DurableAlarmError, R> =>
        Effect.acquireUseRelease(
          generationGate.withPermit(
            beginMutation(options?.invalidatesRecovery ?? true, options?.lanes ?? []),
          ),
          (enrolled) =>
            failpoint.hit("maintenance:mutation:armed").pipe(
              Effect.andThen(body),
              // The committed outbox may be claimed by the alarm even while the caller
              // is suspended after commit. This synchronous decrement does not acquire the
              // native snapshot gate: a snapshot may conservatively retain its active count.
              // Native certification keeps its broader guard until the mutation is released.
              Effect.tap(() => releaseLanes(enrolled, true).pipe(Effect.andThen(notify))),
              Effect.tap(() => failpoint.hit("maintenance:mutation:finished")),
            ),
          (enrolled) => endMutation(enrolled).pipe(Effect.andThen(notify)),
        );

      return ThreadMutationGate.of({
        withMutation,
        schedule,
        activeLanes: Effect.sync(() => new Set(activeLanes.keys())),
        revision: Effect.sync(() => revision),
        awaitChange: (observed) =>
          Effect.suspend(() => (observed === revision ? Deferred.await(changed) : Effect.void)),
        withSnapshot: (body) =>
          generationGate.withPermit(Effect.flatMap(Ref.get(activeMutations), body)),
      });
    }),
  );
}

export type MaintenancePassFailure =
  | DurableWorkerFailure
  | DurableBindingFailure
  | DurableAlarmError
  | ThreadProjectionError;

/**
 * Incremental, quiescent maintenance over a durable dirty/processed generation (issue #93).
 *
 * One physical event owns native scheduling, auxiliary delivery and final alarm rearming.
 *
 * 1. Prearm before any work. A caught-up native step uses the O(1) generation record without
 *    recovery, ledger scans or canonical-history reads.
 * 2. Reconcile before each head Attempt, then checkpoint only the observed generation. A racing
 *    producer keeps its generation dirty and immediately eligible. Quiescent native retries
 *    retain their durable backoff.
 * 3. Admit at most two independent Thread Attempts, one active head per Thread.
 *    Keep native and delivery admission open together while finite waves remain active, so
 *    fresh replies and abort controls can progress during unrelated cleanup. Close atomically
 *    at quiescence, or at the original ten-minute yield deadline, then join admitted waves.
 * 4. Native message delivery retains its driver-owned Claim deadline. Host/backfill waves are
 *    bounded independently; incoming native work never restarts or cancels their attempts.
 *    Auxiliary failures are reported after the current native opportunity.
 * 5. Close every event resource before the final gated deadline snapshot and alarm decision.
 *    The whole event retains one fourteen-minute cooperative timeout.
 */
export class ThreadMaintenance extends Context.Service<
  ThreadMaintenance,
  {
    /** One idempotent pass; failures propagate after durably scheduling bounded recovery. */
    readonly pass: Effect.Effect<MaintenancePassReport, MaintenancePassFailure>;
    /**
     * Constructor gate: initialize/inspect only the O(1) maintenance record and ensure a dirty
     * generation has an alarm. It never scans the ledger or canonical history.
     */
    readonly ensureAlarm: Effect.Effect<void, MaintenancePassFailure>;
    /**
     * Serialize the pre-arm boundary with pass acknowledgement, advance the durable dirty
     * generation and arm the alarm in one transaction BEFORE running the caller's mutation.
     * A pass cannot acknowledge while that mutation remains in flight.
     */
    readonly withMutation: <A, E, R>(
      body: Effect.Effect<A, E, R>,
    ) => Effect.Effect<A, E | DurableAlarmError, R>;
  }
>()("@effect-agent/platform-cloudflare/ThreadMaintenance") {
  static readonly layer: Layer.Layer<
    ThreadMaintenance,
    DurableAlarmError,
    | ThreadMutationGate
    | ThreadPublication
    | ThreadProjectionMaintenance
    | DurableAgentRuntime
    | DurableRuntimeConfig
    | SubmissionLedger
    | WakeScheduler
    | DurableAlarmService
    | ThreadMaintenanceFailpoint
    | CloudflareDurableRuntimeConfig
    | DurableObjectContext
    | SqlClient
  > = Layer.effect(ThreadMaintenance)(
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const recoveryConfig = yield* DurableRuntimeConfig;
      const ledger = yield* SubmissionLedger;
      const wakes = yield* WakeScheduler;
      const alarm = yield* DurableAlarmService;
      const config = yield* CloudflareDurableRuntimeConfig;
      const { ctx } = yield* DurableObjectContext;
      const dueQueue = DueQueue.make(ctx.storage);
      const storage = DurableObjectStorage.fromDurableObjectStorage(ctx.storage);
      const runStorage = yield* makeStorageEffect;
      const failpoint = yield* ThreadMaintenanceFailpoint;

      const mutations = yield* ThreadMutationGate;
      const publication = yield* ThreadPublication;

      const publicationContext = yield* Effect.context<
        ThreadPublication | DurableObjectContext | SqlClient
      >();

      const projection = yield* ThreadProjectionMaintenance;
      const messages = yield* ThreadMessageDelivery;
      const host = yield* ThreadHostMaintenance;
      const framework = yield* ThreadNativeMaintenance;

      for (const lane of host.lanes)
        yield* Schema.decodeEffect(DueQueue.HostLaneId)(lane.id).pipe(
          Effect.mapError(alarmFailure("maintenance lane ID")),
        );

      const recoveryEvents = Option.getOrElse(
        yield* Effect.serviceOption(ThreadRecoveryEvents),
        () => ThreadRecoveryEvents.of({ publish: () => Effect.void }),
      );

      // Hydrated by the existing maintenance-record reads, never by a status scan. All
      // queue mutations update this hint under the same storage reservation. Eviction
      // reconstructs it in ensureAlarm/beginPass; the durable queue remains authoritative.
      let recoveryEventsPending = false;

      const maintenancePassGate = yield* Semaphore.make(1);
      const minimumAlarmDelay = Math.max(1, Math.ceil(config.alarmBackoffBase / 2));
      const runTransaction = yield* makeStorageOperation;

      const queueSnapshot = runTransaction("read maintenance due queue", async () =>
        dueQueue.read(),
      );

      yield* runTransaction("register native maintenance lanes", async () => {
        for (const id of [
          DueQueue.Publication,
          DueQueue.Projection,
          DueQueue.Messages,
          ...framework.lanes.map((lane) => lane.id),
        ])
          dueQueue.register(id);
      });

      const runQueued = <E, R>(
        row: DueQueue.DueLane,
        work: Effect.Effect<Option.Option<number>, E, R>,
        observed: MaintenanceObservation,
        yielded?: () => boolean,
      ) =>
        Effect.suspend(() => {
          // Include mid-pass enrollments in failure rearming if their checkpoint is
          // interrupted. CAS still protects completed waves and racing producers.
          observed.queue.set(row.id, row);
          observed.available.set(row.id, row.revision);

          return work;
        }).pipe(
          Effect.onExit((exit) => {
            if (
              yielded?.() === true &&
              Exit.isFailure(exit) &&
              Cause.hasInterruptsOnly(exit.cause)
            ) {
              // A promptness yield is neither completion nor a failed delivery. Keep the
              // exact due revision; pass-level failure rearming must not back off this wave.
              observed.queue.delete(row.id);

              return Effect.void;
            }

            return mutations
              .withSnapshot(() =>
                Effect.gen(function* () {
                  const active = (yield* mutations.activeLanes).has(row.id);
                  const now = yield* Clock.currentTimeMillis;
                  const failed = Exit.isFailure(exit);

                  const next = failed
                    ? now + backoffDelay(row.stalls, yield* Random.next)
                    : Option.getOrNull(exit.value);

                  const checkpointed = yield* runTransaction("checkpoint maintenance lane", () =>
                    ctx.storage.transaction(async () => {
                      // A producer can be between enrollment and its source commit. Never acknowledge
                      // that observation; its completion will notify the event or retain the alarm.
                      if (active) return;
                      const current = dueQueue.read().find((lane) => lane.id === row.id);

                      if (current?.revision !== row.revision) return;
                      dueQueue.complete(row, next, failed);

                      return row.revision + 1;
                    }),
                  );

                  // This pass's own checkpoint is not fresh producer enrollment. Record it
                  // only after commit, while the snapshot gate still excludes a new producer.
                  if (checkpointed !== undefined) observed.available.set(row.id, checkpointed);
                }),
              )
              .pipe(
                // Waiting for a producer or the SQL connection belongs to the wave budget.
                // An interrupted checkpoint leaves its revision due for event rearming;
                // the short storage transaction itself remains atomic.
                Effect.interruptible,
              );
          }),
        );

      const appendRecoveryEvents = async (
        transaction: DurableObjectTransaction,
        events: ReadonlyArray<Omit<ThreadRecoveryFaultEvent, "sequence">>,
      ) => {
        if (events.length === 0) return;
        const { state } = await readMaintenanceState(transaction);
        let sequence = state.recoveryEventSequence ?? 0n;

        for (const event of events) {
          sequence++;
          await transaction.put(
            recoveryEventKey(sequence),
            encodeRecoveryEvent(ThreadRecoveryFaultEvent.make({ ...event, sequence })),
          );
        }
        await transaction.put(
          MAINTENANCE_STATE_KEY,
          encodeMaintenanceState(
            ThreadMaintenanceState.make({
              ...state,
              recoveryEventSequence: sequence,
            }),
          ),
        );
        dueQueue.dirty(DueQueue.RecoveryEvents, 0);
        recoveryEventsPending = true;
      };

      const recoveryEventLane: ThreadHostMaintenanceLane = {
        id: DueQueue.RecoveryEvents,
        dispatchTimeoutMillis: 30_000,
        run: Effect.gen(function* () {
          if (!recoveryEventsPending) return Option.none<number>();

          const range = yield* runTransaction("select recovery events", () =>
            ctx.storage.transaction(async (transaction) => {
              const { state } = await readMaintenanceState(transaction);

              return {
                next: (state.recoveryEventAcknowledged ?? 0n) + 1n,
                through: state.recoveryEventSequence ?? 0n,
              };
            }),
          );

          for (let sequence = range.next; sequence <= range.through; sequence++) {
            const event = yield* runTransaction("read recovery event", async () => {
              const event = decodeRecoveryEvent(await ctx.storage.get(recoveryEventKey(sequence)));

              if (event.sequence !== sequence)
                throw new Error("Recovery event does not match its sequence key");

              return event;
            });

            // No storage reservation or source mutation gate is held during host delivery.
            yield* recoveryEvents.publish(event);
            yield* runTransaction("acknowledge recovery event", () =>
              ctx.storage.transaction(async (transaction) => {
                const { state } = await readMaintenanceState(transaction);

                const next = ThreadMaintenanceState.make({
                  ...state,
                  recoveryEventAcknowledged: sequence,
                });

                await transaction.delete(recoveryEventKey(sequence));
                await transaction.put(MAINTENANCE_STATE_KEY, encodeMaintenanceState(next));
                recoveryEventsPending = hasRecoveryEvents(next);
              }),
            );
          }

          return recoveryEventsPending ? Option.some(0) : Option.none<number>();
        }),
      };

      const recordRecoveryFaults = Effect.fn("ThreadMaintenance.recordRecoveryFaults")(function* (
        result: RecoverySweepResult,
        recovery: NativeRecovery,
        current?: ReadonlyArray<SubmissionWorkItem>,
      ) {
        const threads = new Map<ThreadId, RecoveryFailure | undefined>();

        for (const report of result.reports) threads.set(report.threadId, undefined);
        for (const blocked of result.blocked) threads.set(blocked.threadId, blocked.failure);
        if (threads.size === 0) return new Map<ThreadId, ThreadRecoveryFault>();
        const now = yield* Clock.currentTimeMillis;

        // Fault-only discovery uses control state even when retained payloads cannot decode.
        const submissions =
          current ??
          (result.blocked.length === 0 ? [] : yield* Stream.runCollect(ledger.scanNonterminal));

        yield* failpoint.hit("maintenance:recovery-status:before");

        const retained = yield* runTransaction("record Thread recovery faults", () =>
          ctx.storage.transaction(async (transaction) => {
            const events: Array<Omit<ThreadRecoveryFaultEvent, "sequence">> = [];
            const newlyBlocked: Array<ThreadRecoveryFault> = [];
            const faults = new Map<ThreadId, ThreadRecoveryFault>();
            const recipients = new Map<ThreadId, ReadonlySet<SubmissionId>>();

            for (const [threadId, failure] of threads) {
              const key = recoveryFaultKey(threadId);
              const encoded = await transaction.get(key);

              const previous =
                encoded === undefined ? undefined : decodeRecoveryFault(threadId, encoded);

              const previousIds =
                previous === undefined ? [] : await readRecoveryRecipients(transaction, previous);

              const known = new Set(previousIds);

              recipients.set(threadId, known);

              if (failure === undefined) {
                if (previous !== undefined) {
                  await transaction.delete(key);
                  for (let start = 0; start < (previous.recipientCount ?? 0); start += 128)
                    await transaction.delete(recoveryRecipientKeys(previous, start));
                  for (const id of previous.recipientCount === undefined
                    ? result.reports
                        .filter((report) => report.threadId === threadId)
                        .map((report) => report.submissionId)
                    : previousIds)
                    events.push(recoveryEvent(previous, id, "cleared", now));
                }
                continue;
              }

              const added = submissions
                .filter((row) => row.threadId === threadId && !known.has(row.submissionId))
                .map((row) => row.submissionId);

              await appendRecoveryRecipients(transaction, threadId, previousIds.length, added);

              const fault = ThreadRecoveryFault.make({
                schemaVersion: 1,
                threadId,
                firstFailedAt: previous?.firstFailedAt ?? now,
                lastFailedAt: now,
                attempts: Math.min(2_147_483_647, (previous?.attempts ?? 0) + 1),
                retryAt: now + Math.min(60_000, 5_000 * 2 ** Math.min(30, previous?.attempts ?? 0)),
                failure,
                recipientCount: previousIds.length + added.length,
              });

              await transaction.put(key, encodeRecoveryFault(fault));
              if (previous !== undefined && !sameRecoveryFailure(previous.failure, failure))
                for (const id of previousIds) events.push(recoveryEvent(fault, id, "changed", now));
              for (const id of added) {
                known.add(id);
                events.push(recoveryEvent(fault, id, "created", now));
              }
              faults.set(threadId, fault);
              if (previous === undefined) newlyBlocked.push(fault);
            }

            await appendRecoveryEvents(transaction, events);

            return {
              changed: new Set(events.map((event) => event.threadId)),
              newlyBlocked,
              faults,
              recipients,
            };
          }),
        );

        yield* failpoint.hit("maintenance:recovery-status:after");
        for (const [threadId, ids] of retained.recipients) recovery.recipients.set(threadId, ids);
        // A healthy unchanged head must not wake its own maintenance loop.
        for (const threadId of retained.changed) yield* wakes.notify(threadId);
        for (const fault of retained.newlyBlocked)
          yield* Effect.logError(
            "Native Thread recovery blocked; accepted work remains pending",
            fault.failure.reason === "defect"
              ? Cause.die(fault.failure)
              : Cause.fail(fault.failure),
          ).pipe(Effect.annotateLogs({ threadId: fault.threadId }));

        return retained.faults;
      });

      const recoverThread = Effect.fn("ThreadMaintenance.recoverThread")(function* (
        threadId: ThreadId,
        recovery: NativeRecovery,
      ) {
        const result = yield* runtime.runRecovery({ threadId });
        // Visibility is committed before a claim or any fallible auxiliary join.
        const faults = yield* recordRecoveryFaults(result, recovery);

        for (const report of result.reports) recovery.reports.set(report.submissionId, report);
        recovery.faults.delete(threadId);
        for (const [id, fault] of faults) recovery.faults.set(id, fault);
        recovery.recovered += result.reports.length + result.blocked.length;
        recovery.repaired ||= result.reports.some((report) => report.disposition === "repaired");
      });

      // Registry changes, including upgrades from timed binding retries, create one native
      // opportunity. Unchanged registries leave parked work dormant across Object eviction.
      const readCurrentMaintenanceState = async (transaction: DurableObjectTransaction) => {
        const current = await readMaintenanceState(transaction);

        if (current.state.bindingRegistryKey === runtime.bindingRegistryKey) return current;

        const state = ThreadMaintenanceState.make({
          ...Struct.omit(current.state, ["retry"]),
          bindingRegistryKey: runtime.bindingRegistryKey,
          bindingWaits: [],
          dirty: current.state.dirty + 1n,
        });

        await transaction.put(MAINTENANCE_STATE_KEY, encodeMaintenanceState(state));
        dueQueue.dirty(DueQueue.Native, 0);

        return { state, initialized: true };
      };

      const ensureAlarm = Effect.fn("ThreadMaintenance.ensureAlarm")(function* () {
        yield* failpoint.hit("maintenance:ensure:before");
        const now = yield* Clock.currentTimeMillis;

        yield* runTransaction("ensure maintenance alarm", () =>
          ctx.storage.transaction(async (transaction) => {
            const { state, initialized } = await readCurrentMaintenanceState(transaction);

            recoveryEventsPending = hasRecoveryEvents(state);
            const rows = dueQueue.read();

            if (recoveryEventsPending && !rows.some((row) => row.id === DueQueue.RecoveryEvents))
              dueQueue.dirty(DueQueue.RecoveryEvents, now);
            if (!initialized)
              await transaction.put(MAINTENANCE_STATE_KEY, encodeMaintenanceState(state));

            const native =
              state.dirty > state.processed
                ? state.retry?.generation === state.dirty
                  ? state.retry.notBefore
                  : (rows.find((row) => row.id === DueQueue.Native)?.dueAt ??
                    now + minimumAlarmDelay)
                : null;

            dueQueue.checkpointNative(native);
            const next = DueQueue.next(dueQueue.read());

            if (Number.isFinite(next))
              await ensureTransactionAlarmBy(transaction, Math.max(now + minimumAlarmDelay, next));
          }),
        );
        yield* failpoint.hit("maintenance:ensure:after");
      });

      const beginPass = Effect.fn("ThreadMaintenance.beginPass")(function* (observed: {
        generation?: bigint;
      }) {
        yield* failpoint.hit("maintenance:begin:before");
        const now = yield* Clock.currentTimeMillis;

        const result = yield* runTransaction("begin maintenance pass", () =>
          ctx.storage.transaction(async (transaction) => {
            const { state, initialized } = await readCurrentMaintenanceState(transaction);

            recoveryEventsPending = hasRecoveryEvents(state);

            observed.generation = state.dirty;
            if (!initialized) {
              await transaction.put(MAINTENANCE_STATE_KEY, encodeMaintenanceState(state));
            }
            const retryAt = state.retry?.generation === state.dirty ? state.retry.notBefore : 0;

            // Prearm before recovery or any host hook, including publication-only passes.
            // A completed pass may move this crash fallback later to its bounded retry.
            await ensureTransactionAlarmBy(transaction, now + minimumAlarmDelay);
            if (state.processed >= state.dirty || retryAt > now) {
              return {
                _tag: "CaughtUp" as const,
                nonterminal: state.nonterminal,
              };
            }

            return {
              _tag: "Actionable" as const,
              generation: state.dirty,
              nonterminal: state.nonterminal,
              stalls: state.retry?.generation === state.dirty ? state.retry.stalls : 0,
            };
          }),
        );

        yield* failpoint.hit("maintenance:begin:after");

        return result;
      });

      const backoffDelay = (priorStalls: number, jitter: number) => {
        const backoff = Math.min(
          config.alarmBackoffCap,
          config.alarmBackoffBase * 2 ** Math.min(priorStalls, 30),
        );

        // Jitter over [backoff/2, backoff] spreads retries without exceeding the cap.
        return Math.ceil(backoff / 2 + (backoff / 2) * jitter);
      };

      const rearmDelay = Effect.fn("ThreadMaintenance.rearmDelay")(function* (
        progressed: boolean,
        priorStalls: number,
      ) {
        return progressed ? config.alarmBackoffBase : backoffDelay(priorStalls, yield* Random.next);
      });

      const rearmFailure = Effect.fn("ThreadMaintenance.rearmFailure")(function* (
        observed: MaintenanceObservation,
      ) {
        const { generation, nativeOnly } = observed;

        if (generation === undefined) return;
        yield* failpoint.hit("maintenance:retry:before");
        yield* mutations.withSnapshot((active) =>
          Effect.gen(function* () {
            const now = yield* Clock.currentTimeMillis;

            const jitter = yield* Random.next;

            yield* runTransaction("back off failed maintenance", () =>
              ctx.storage.transaction(async (transaction) => {
                const { state } = await readMaintenanceState(transaction);

                const previous =
                  state.retry?.generation === generation && state.retry.nativeOnly === nativeOnly
                    ? state.retry
                    : undefined;

                const retry =
                  state.retry?.generation === generation && state.retry.nativeOnly
                    ? state.retry
                    : MaintenanceRetry.make({
                        generation,
                        notBefore: Math.max(
                          previous?.notBefore ?? 0,
                          now + backoffDelay(previous?.stalls ?? 0, jitter),
                        ),
                        nativeOnly,
                        stalls: Math.min(30, (previous?.stalls ?? 0) + 1),
                      });

                await transaction.put(
                  MAINTENANCE_STATE_KEY,
                  encodeMaintenanceState(ThreadMaintenanceState.make({ ...state, retry })),
                );

                // Never postpone a producer that raced the failed observation. Host work
                // retains its own deadline; an early delivery skips native recovery below.
                const nativeDeadline =
                  active > 0 || state.dirty !== generation
                    ? now + minimumAlarmDelay
                    : retry.notBefore;

                dueQueue.checkpointNative(
                  state.dirty > state.processed || active > 0 ? nativeDeadline : null,
                );
                // Defer only the unacknowledged observations from this failed event. A
                // completed wave or racing enrollment has a different revision.
                for (const row of observed.queue.values()) {
                  if (row.id !== DueQueue.Native && row.dueAt !== null && row.dueAt <= now)
                    dueQueue.complete(row, now + backoffDelay(row.stalls, jitter), true);
                }
                const next = DueQueue.next(dueQueue.read());

                await transaction.setAlarm(
                  Math.max(now + minimumAlarmDelay, Number.isFinite(next) ? next : retry.notBefore),
                );
              }),
            );
          }),
        );
        yield* failpoint.hit("maintenance:retry:after");
      });

      const beginNative = Effect.fn("ThreadMaintenance.beginNative")(function* (
        observed: MaintenanceObservation,
      ) {
        return yield* mutations.withSnapshot((activeAtStart) =>
          Effect.gen(function* () {
            const generation = yield* beginPass(observed);

            if (activeAtStart === 0) {
              const native = (yield* queueSnapshot).find((row) => row.id === DueQueue.Native);

              if (native !== undefined) observed.available.set(native.id, native.revision);
            }
            if (generation._tag === "Actionable" && activeAtStart === 0) {
              // The gate excludes a producer starting between the snapshot and certification.
              yield* publication.prepareGeneration(generation.generation);
            }

            return { ...generation, activeAtStart };
          }),
        );
      });

      const attemptSelected = Effect.fn("ThreadMaintenance.attemptSelected")(function* (
        selected: SubmissionWorkItem,
        previousWaits: ReadonlyArray<BindingWait>,
        yieldAfter: DateTime.Utc,
        recovery: NativeRecovery,
      ): Effect.fn.Return<number, MaintenancePassFailure> {
        yield* recoverThread(selected.threadId, recovery);

        let bindingFailure: DurableBindingFailure | undefined;

        // A missing binding releases its slot after retaining this exact Submission
        // and committing its registry wait below.
        const settlement = recovery.faults.has(selected.threadId)
          ? Option.none()
          : yield* runtime.processThreadHead(selected.threadId, { yieldAfter }).pipe(
              Effect.catchTag("BindingUnavailable", (failure) => {
                bindingFailure = failure;

                return Effect.succeed(Option.none());
              }),
            );

        const previous = previousWaits.find((wait) => wait.submissionId === selected.submissionId);

        const wait =
          bindingFailure === undefined
            ? undefined
            : BindingWait.make({
                threadId: selected.threadId,
                submissionId: selected.submissionId,
                reportedAt: previous?.reportedAt ?? (yield* Clock.currentTimeMillis),
              });

        // Report before recording reportedAt. Interruption before the write may repeat a
        // report, but a durable wait must never suppress a report that was not delivered.
        if (bindingFailure !== undefined && previous === undefined) {
          yield* Effect.logError(
            "Thread parked until its agent binding registry changes; original work remains pending",
            Cause.fail(bindingFailure),
          ).pipe(
            Effect.annotateLogs({
              threadId: selected.threadId,
              submissionId: selected.submissionId,
            }),
          );
        }

        if (wait !== undefined || previous !== undefined) {
          // The Attempt released its Claim. Commit its binding wait (or clear) once,
          // before joining fallible auxiliary work. This local fact neither acknowledges
          // a generation nor changes the shared alarm.
          yield* failpoint.hit("maintenance:binding-retry:before");
          yield* runStorage(
            "record submission binding wait",
            storage
              .transaction((transaction) =>
                Effect.gen(function* () {
                  const encoded = yield* transaction.get(MAINTENANCE_STATE_KEY);

                  const state =
                    encoded === undefined
                      ? initialMaintenanceState()
                      : yield* Schema.decodeUnknownEffect(ThreadMaintenanceState)(encoded);

                  const bindingWaits = [
                    ...(state.bindingWaits ?? []).filter(
                      (entry) => entry.submissionId !== selected.submissionId,
                    ),
                    ...(wait === undefined ? [] : [wait]),
                  ];

                  yield* transaction.put(
                    MAINTENANCE_STATE_KEY,
                    yield* Schema.encodeEffect(ThreadMaintenanceState)(
                      ThreadMaintenanceState.make({ ...state, bindingWaits }),
                    ),
                  );
                }),
              )
              .pipe(Effect.mapError(alarmFailure("record submission binding wait"))),
          );
          yield* failpoint.hit("maintenance:binding-retry:after");
        }

        return Option.isSome(settlement) ? 1 : 0;
      });

      const advance = Effect.fn("ThreadMaintenance.advance")(function* (
        started: Effect.Success<ReturnType<typeof beginNative>>,
        yieldAfter: DateTime.Utc,
        observed: MaintenanceObservation,
        recovery: NativeRecovery,
        native: NativeDispatch,
        dispatch = true,
        reserved: ReadonlySet<ThreadId> = new Set(native.active.keys()),
      ): Effect.fn.Return<NativePassResult, MaintenancePassFailure> {
        let publicationRow = (yield* queueSnapshot).find((row) => row.id === DueQueue.Publication);

        if (
          publicationRow !== undefined &&
          publicationRow.dueAt !== null &&
          publicationRow.dueAt <= (yield* Clock.currentTimeMillis)
        ) {
          yield* runQueued(publicationRow, publication.drain, observed);
          publicationRow = (yield* queueSnapshot).find((row) => row.id === DueQueue.Publication);
        }
        const pending = Option.fromNullishOr(publicationRow?.dueAt);

        if (started._tag === "CaughtUp" || Option.isSome(pending)) {
          const nextAttemptAt = yield* mutations.withSnapshot((active) =>
            Effect.gen(function* () {
              const now = yield* Clock.currentTimeMillis;

              const { state } = yield* runTransaction("read native maintenance deadline", () =>
                ctx.storage.transaction((transaction) => readMaintenanceState(transaction)),
              );

              const native =
                active > 0 || state.dirty > state.processed
                  ? state.retry?.generation === state.dirty && active === 0
                    ? state.retry.notBefore
                    : now + minimumAlarmDelay
                  : Infinity;

              const next = Option.isSome(pending) ? pending.value : native;

              return Number.isFinite(next) ? Math.max(now + minimumAlarmDelay, next) : undefined;
            }),
          );

          return {
            phase: "caught-up",
            nonterminal: started.nonterminal,
            nextAttemptAt,
          };
        }
        if (native.generation !== started.generation) {
          native.deferred.clear();
          native.generation = started.generation;
        }
        // A stable generation with no freed slot needs no second ledger/history scan.
        // Enrollment precedes admission: never cache a scan overlapping a producer body,
        // which may make a head ready later without another generation increment.
        if (native.active.size > 0 && native.scanGeneration === started.generation) {
          return {
            phase: "actionable",
            nonterminal: started.nonterminal,
            nextAttemptAt: undefined,
          };
        }
        native.scanGeneration = started.activeAtStart === 0 ? started.generation : undefined;

        // Select from control state before reading execution history. A recovering or faulted
        // Thread cannot enter dispatch; old cleanup has its own scoped opportunity below.
        observed.nativeOnly = true;

        // Every checkpoint keeps the producer overlap that belongs to this recovery wave.
        const observation = (recovery.observation ??= {
          generation: started.generation,
          activeAtStart: started.activeAtStart,
        });

        const current = yield* Stream.runCollect(ledger.scanNonterminal);
        const selectionTime = yield* Clock.currentTimeMillis;
        const submissionsByThread = new Map<ThreadId, Array<SubmissionId>>();

        for (const row of current) {
          const ids = submissionsByThread.get(row.threadId) ?? [];

          ids.push(row.submissionId);
          submissionsByThread.set(row.threadId, ids);
        }

        yield* runTransaction("read Thread recovery deadlines", async () => {
          for (const [threadId, ids] of submissionsByThread) {
            if (!recovery.loaded.has(threadId)) {
              const encoded = await ctx.storage.get(recoveryFaultKey(threadId));

              if (encoded !== undefined) {
                const fault = decodeRecoveryFault(threadId, encoded);

                recovery.faults.set(threadId, fault);
                recovery.recipients.set(
                  threadId,
                  new Set(await readRecoveryRecipients(ctx.storage, fault)),
                );
              }
              recovery.loaded.add(threadId);
            }
            const fault = recovery.faults.get(threadId);

            if (
              fault === undefined ||
              ids.every((id) => recovery.recipients.get(threadId)?.has(id))
            )
              continue;

            // Admissions during backoff need visibility without accelerating recovery.
            // Preserve recipients even if recovery later settles them before clearing.
            await ctx.storage.transaction(async (transaction) => {
              const encoded = await transaction.get(recoveryFaultKey(threadId));

              if (encoded === undefined) return;
              const previous = decodeRecoveryFault(threadId, encoded);
              const previousIds = await readRecoveryRecipients(transaction, previous);
              const known = new Set(previousIds);
              const added = ids.filter((id) => !known.has(id));

              if (added.length === 0) return;

              const updated = ThreadRecoveryFault.make({
                ...previous,
                recipientCount: previousIds.length + added.length,
              });

              await appendRecoveryRecipients(transaction, threadId, previousIds.length, added);
              await transaction.put(recoveryFaultKey(threadId), encodeRecoveryFault(updated));
              await appendRecoveryEvents(
                transaction,
                added.map((id) => recoveryEvent(updated, id, "created", selectionTime)),
              );
              recovery.faults.set(threadId, updated);
              recovery.recipients.set(threadId, new Set([...previousIds, ...added]));
            });
          }
        });
        const reports = recovery.reports;
        const recoveryFaults = recovery.faults;

        const bindingWaits = yield* runTransaction(
          "read binding waits",
          async () => (await readMaintenanceState(ctx.storage)).state.bindingWaits ?? [],
        );

        const parked = new Set(bindingWaits.map((wait) => wait.submissionId));

        const waiting = (row: SubmissionWorkItem) =>
          !recovery.pending.has(row.threadId) &&
          !recoveryFaults.has(row.threadId) &&
          (parked.has(row.submissionId) || stableExternalWait(row, reports));

        const heads = new Map<ThreadId, SubmissionWorkItem>();

        for (const row of current) {
          // Only recovered uncertainty may release later input; accepted aborts retain FIFO.
          if (row.state === "unknown" && waiting(row)) continue;
          if (!heads.has(row.threadId)) heads.set(row.threadId, row);
        }
        const stopping = new Set<ThreadId>();

        for (const head of heads.values()) {
          if (
            head.state !== "ready" ||
            native.active.has(head.threadId) ||
            recovery.pending.has(head.threadId) ||
            recoveryFaults.has(head.threadId)
          )
            continue;

          // An accepted abort is cleanup even when its input was never claimed. This
          // control-only read must not decode the execution journal or a recovery snapshot.
          const intent = yield* isolateRecovery(
            ledger.readAbortIntent(AbortIntentRequest.make({ submissionId: head.submissionId })),
            {
              timeout: recoveryConfig.recoveryTimeout,
              phase: () => "recovery",
              operation: "read abort intent",
            },
          );

          if (Result.isSuccess(intent)) {
            if (intent.success !== undefined) stopping.add(head.threadId);
          } else {
            const faults = yield* recordRecoveryFaults(
              RecoverySweepResult.make({
                reports: [],
                blocked: [
                  RecoveryBlocked.make({ threadId: head.threadId, failure: intent.failure }),
                ],
              }),
              recovery,
              current,
            );

            for (const [threadId, fault] of faults) recoveryFaults.set(threadId, fault);
            recovery.recovered++;
          }
        }

        const eligible = [...heads.values()]
          .filter(
            (head) =>
              !recovery.pending.has(head.threadId) &&
              !stopping.has(head.threadId) &&
              !recoveryFaults.has(head.threadId) &&
              !waiting(head) &&
              (head.state === "ready" || reports.has(head.submissionId)),
          )
          .map((head) => head.threadId)
          .sort();

        yield* failpoint.hit("maintenance:select:before");

        const selection = yield* runTransaction("select maintenance lane", () =>
          ctx.storage.transaction(async (transaction) => {
            const { state } = await readMaintenanceState(transaction);

            const waits = (state.bindingWaits ?? []).filter(
              (retry) => heads.get(retry.threadId)?.submissionId === retry.submissionId,
            );

            const runnable = eligible.filter(
              (threadId) =>
                !reserved.has(threadId) &&
                !native.active.has(threadId) &&
                !native.deferred.has(threadId) &&
                !waits.some((wait) => wait.threadId === threadId),
            );

            const pivot = Math.max(
              0,
              runnable.findIndex(
                (threadId) =>
                  state.lastServedThreadId === undefined || threadId > state.lastServedThreadId,
              ),
            );

            const selected = dispatch
              ? [...runnable.slice(pivot), ...runnable.slice(0, pivot)].slice(
                  0,
                  nativeDispatchConcurrency - reserved.size,
                )
              : [];

            const next = selected.at(-1);

            if (next !== undefined) {
              await transaction.put(
                MAINTENANCE_STATE_KEY,
                encodeMaintenanceState(
                  ThreadMaintenanceState.make({ ...state, lastServedThreadId: next }),
                ),
              );
            }

            const backlog = recovery.started
              ? []
              : [...heads.keys()].filter((threadId) => {
                  const fault = recoveryFaults.get(threadId);

                  return (
                    !eligible.includes(threadId) &&
                    (fault === undefined || fault.retryAt <= selectionTime)
                  );
                });

            // One finite wave, one Thread at a time, with the runtime's per-Thread deadline.
            // This cursor ensures an event deadline/eviction cannot always restart at the front.
            const after = backlog.filter(
              (threadId) =>
                state.lastRecoveredThreadId === undefined || threadId > state.lastRecoveredThreadId,
            );

            const before = backlog.filter(
              (threadId) =>
                state.lastRecoveredThreadId !== undefined &&
                threadId <= state.lastRecoveredThreadId,
            );

            return { selected, waits, backlog: [...after, ...before] };
          }),
        );

        yield* failpoint.hit("maintenance:select:after");
        if (!recovery.started) {
          recovery.started = true;
          recovery.needsCheckpoint = selection.backlog.length > 0;
          for (const threadId of selection.backlog) recovery.pending.add(threadId);
          yield* Deferred.succeed(recovery.queue, selection.backlog);
        }

        for (const threadId of selection.selected) {
          const selected = heads.get(threadId);

          if (selected === undefined) continue;
          // Reserve before recovery or claim acquisition. The durable Claim remains the
          // authority; this event-local reservation only prevents duplicate dispatch.
          native.dispatched++;
          native.active.set(
            threadId,
            yield* Effect.forkIn(
              attemptSelected(selected, selection.waits, yieldAfter, recovery),
              native.scope,
            ),
          );
        }

        if (native.active.size > 0) {
          // Active Attempts own their claims and may still append/settle. Only acknowledge
          // a generation after joining every admitted native Attempt.
          return {
            phase: "actionable",
            nonterminal: current.length,
            nextAttemptAt: undefined,
            dispatched: selection.selected.length > 0,
          };
        }

        const remaining = yield* Stream.runCollect(ledger.scanNonterminal);
        const waitingHeads = new Map<ThreadId, boolean>();

        const autonomous = remaining.some((snapshot) => {
          if (snapshot.state === "unknown" && waiting(snapshot)) return false;
          const headWaiting = waitingHeads.get(snapshot.threadId);

          if (headWaiting === undefined) waitingHeads.set(snapshot.threadId, waiting(snapshot));
          // FIFO followers cannot execute through a stable external wait. Only plain queued
          // input is dormant here; admission repairs and accepted aborts still need a pass.
          if (
            headWaiting === true &&
            snapshot.state === "ready" &&
            reports.get(snapshot.submissionId)?.decision._tag === "ApplyInput"
          )
            return false;

          return !waiting(snapshot);
        });

        const progressed = native.progressed || recovery.repaired;

        const now = yield* Clock.currentTimeMillis;
        const ordinaryDelay = autonomous ? yield* rearmDelay(progressed, started.stalls) : 0;

        const nextEligible = [...recoveryFaults.values()]
          .map((fault) => fault.retryAt)
          .concat(
            eligible
              .filter((threadId) => !selection.waits.some((wait) => wait.threadId === threadId))
              .map(() => now),
          );

        const retryDelay =
          nextEligible.length === 0 ? 0 : Math.max(0, Math.min(...nextEligible) - now);

        const delay = Math.max(ordinaryDelay, retryDelay);

        // Checkpoint native progress without changing the physical alarm. Auxiliary
        // delivery remains live; later mutations still advance the shared generation.
        yield* failpoint.hit("maintenance:checkpoint:before");

        const nextAttemptAt = yield* mutations.withSnapshot((active) =>
          runTransaction("checkpoint native maintenance", () =>
            ctx.storage.transaction(async (transaction) => {
              const { state } = await readMaintenanceState(transaction);

              const mutationOverlap =
                observation.activeAtStart > 0 || started.activeAtStart > 0 || active > 0;

              // An empty control scan needs no older recovery report. Certify its fresh
              // snapshot only when no producer overlapped it or advanced the generation.
              const generation =
                remaining.length === 0 && !mutationOverlap && state.dirty === started.generation
                  ? started.generation
                  : observation.generation;

              const processed =
                autonomous || mutationOverlap
                  ? state.processed
                  : state.processed > generation
                    ? state.processed
                    : generation;

              // Enrollment precedes the mutation body. A retry from an overlapping snapshot
              // must not defer work that becomes ready later in that same generation.
              const canBackoff = !mutationOverlap && state.dirty === started.generation;

              const next = ThreadMaintenanceState.make({
                ...Struct.omit(state, ["retry"]),
                processed,
                nonterminal: remaining.length,
                bindingWaits: (state.bindingWaits ?? []).filter((retry) =>
                  remaining.some((row) => row.submissionId === retry.submissionId),
                ),
                ...(autonomous && !progressed && canBackoff
                  ? {
                      retry: MaintenanceRetry.make({
                        generation: started.generation,
                        notBefore: now + delay,
                        nativeOnly: true,
                        stalls: Math.min(30, started.stalls + 1),
                      }),
                    }
                  : {}),
              });

              await transaction.put(MAINTENANCE_STATE_KEY, encodeMaintenanceState(next));

              const dueAt = autonomous
                ? now + (canBackoff ? delay : minimumAlarmDelay)
                : mutationOverlap || next.dirty > next.processed
                  ? now + minimumAlarmDelay
                  : null;

              dueQueue.checkpointNative(dueAt);

              return dueAt ?? undefined;
            }),
          ),
        );

        yield* failpoint.hit("maintenance:checkpoint:after");
        // Once this empty recovery wave is checkpointed, its producer overlap must not
        // prevent a later fresh snapshot from acknowledging native quiescence.
        if (remaining.length === 0 && recovery.pending.size === 0) delete recovery.observation;
        native.deferred.clear();
        native.progressed = false;
        native.needsCheckpoint = false;

        return {
          phase: "actionable",
          nonterminal: remaining.length,
          nextAttemptAt,
        };
      });

      const validateLanes = Effect.fn("ThreadMaintenance.validateLanes")(function* (
        lanes: ReadonlyArray<ThreadHostMaintenanceLane>,
        queued: ReadonlyArray<DueQueue.DueLane>,
      ) {
        const ids = new Set([
          DueQueue.Native,
          DueQueue.Publication,
          DueQueue.Projection,
          DueQueue.Messages,
        ]);

        for (const lane of lanes) {
          yield* Schema.decodeEffect(DueQueue.LaneId)(lane.id).pipe(
            Effect.mapError(alarmFailure("maintenance lane ID")),
          );
          if (ids.has(lane.id))
            return yield* DurableAlarmError.make({
              operation: "maintenance lane ID",
              message: `Duplicate maintenance lane: ${lane.id}`,
            });
          ids.add(lane.id);
        }
        for (const row of queued) {
          if (row.dueAt !== null && !ids.has(row.id))
            return yield* DurableAlarmError.make({
              operation: "maintenance lane ID",
              message: `No handler registered for scheduled lane: ${row.id}`,
            });
        }
      });

      const dispatch = Effect.fn("ThreadMaintenance.dispatch")(function* (
        yieldAfter: DateTime.Utc,
        dispatchUntil: DateTime.Utc,
        observed: MaintenanceObservation,
      ) {
        // Subscribe before the first snapshot. Durable enrollment recovers eviction;
        // producer completion and wake hints make work visible during this incarnation.
        const notified = (yield* Stream.toPull(wakes.wakes)).pipe(
          Effect.asVoid,
          Effect.catch(() => Effect.never),
        );

        // A wake may observe temporary backoff while this event's recovery is still running.
        // Its completion must retain the original actionable observation for acknowledgement.
        const started = yield* beginNative(observed);

        yield* mutations.withSnapshot(() =>
          Effect.gen(function* () {
            const queued = yield* queueSnapshot;
            const producing = yield* mutations.activeLanes;

            for (const row of queued) {
              observed.queue.set(row.id, row);
              if (!producing.has(row.id)) observed.available.set(row.id, row.revision);
            }
          }),
        );

        // This scope owns every admitted finite wave, including native advancement.
        // Close it before final alarm rearming, including on failure or event interruption.
        const auxiliaryScope = yield* Effect.acquireRelease(Scope.make("parallel"), (scope, exit) =>
          Scope.close(scope, exit),
        );

        const fork = <A, E>(work: Effect.Effect<A, E>) => Effect.forkIn(work, auxiliaryScope);

        // Each lane has at most one finite wave. A completion is retained until the single
        // scheduling loop observes it; a busy sibling cannot consume another lane's hint.
        const lanes = [recoveryEventLane, ...framework.lanes, ...host.lanes].map((lane) => ({
          ...lane,
          exhausted: false,
          fiber: undefined as Fiber.Fiber<Option.Option<number>, DurableAlarmError> | undefined,
        }));

        let messageExhausted = false;
        let delivery: Fiber.Fiber<void, DurableAlarmError> | undefined;
        let failure: Cause.Cause<MaintenancePassFailure> | undefined;

        const recovery: NativeRecovery = {
          queue: yield* Deferred.make<ReadonlyArray<ThreadId>>(),
          pending: new Set(),
          loaded: new Set(),
          reports: new Map(),
          faults: new Map(),
          recipients: new Map(),
          started: false,
          needsCheckpoint: false,
          recovered: 0,
          repaired: false,
        };

        const recoveryFiber = yield* fork(
          Effect.gen(function* () {
            for (const threadId of yield* Deferred.await(recovery.queue)) {
              yield* failpoint.hit("maintenance:select:before");
              yield* runTransaction("select old recovery lane", () =>
                ctx.storage.transaction(async (transaction) => {
                  const { state } = await readMaintenanceState(transaction);

                  await transaction.put(
                    MAINTENANCE_STATE_KEY,
                    encodeMaintenanceState(
                      ThreadMaintenanceState.make({
                        ...state,
                        lastRecoveredThreadId: threadId,
                      }),
                    ),
                  );
                }),
              );
              yield* failpoint.hit("maintenance:select:after");
              yield* recoverThread(threadId, recovery);
              recovery.pending.delete(threadId);
              if (!recovery.faults.has(threadId)) yield* wakes.notify(threadId);
            }
          }),
        );

        // Backfill is one disposable wave; native and host work keep their own opportunities.
        const backfill = yield* fork(
          Effect.gen(function* () {
            if ((yield* mutations.activeLanes).has(DueQueue.Projection)) return;
            const row = (yield* queueSnapshot).find((row) => row.id === DueQueue.Projection);

            if (
              row?.dueAt === null ||
              row === undefined ||
              row.dueAt > (yield* Clock.currentTimeMillis)
            )
              return;

            return yield* runQueued(
              row,
              projection.drain.pipe(Effect.andThen(projection.pendingDeadline)),
              observed,
            ).pipe(Effect.timeoutOption(config.projectionDispatchTimeoutMillis));
          }),
        );

        let backfillObserved = false;
        let recoveryObserved = false;

        const dispatch: NativeDispatch = {
          scope: auxiliaryScope,
          active: new Map(),
          deferred: new Set(),
          dispatched: 0,
          settled: 0,
          progressed: false,
          needsCheckpoint: false,
        };

        let native: Fiber.Fiber<NativePassResult, MaintenancePassFailure> | undefined = yield* fork(
          advance(started, yieldAfter, observed, recovery, dispatch),
        );

        let result: NativePassResult = {
          phase: "caught-up",
          nonterminal: started.nonterminal,
          nextAttemptAt: undefined,
        };

        let phase = result.phase;
        let nativeCheck = false;
        const until = DateTime.toEpochMillis(yieldAfter);
        const dispatchEnd = DateTime.toEpochMillis(dispatchUntil);

        while (true) {
          if (native?.pollUnsafe() !== undefined) {
            result = yield* Fiber.join(native);
            native = undefined;
            if (result.phase === "actionable") phase = "actionable";
            observed.nativeOnly = dispatch.active.size > 0;
            if (result.dispatched === true && dispatch.active.size < nativeDispatchConcurrency) {
              dispatch.scanGeneration = undefined;
              nativeCheck = true;
            }
            // An empty initial scan still opens exactly one old-recovery opportunity.
            recovery.started = true;
            yield* Deferred.succeed(recovery.queue, []);
          }
          for (const [threadId, attempt] of dispatch.active) {
            const exit = attempt.pollUnsafe();

            if (exit === undefined) continue;
            dispatch.active.delete(threadId);
            dispatch.scanGeneration = undefined;
            if (Exit.isFailure(exit)) return yield* Effect.failCause(exit.cause);
            dispatch.settled += exit.value;
            dispatch.progressed ||= exit.value > 0;
            dispatch.needsCheckpoint = true;
            if (exit.value === 0) dispatch.deferred.add(threadId);
            nativeCheck = true;
          }
          if (!recoveryObserved && recoveryFiber.pollUnsafe() !== undefined) {
            yield* Fiber.join(recoveryFiber);
            recoveryObserved = true;
            dispatch.scanGeneration = undefined;
            nativeCheck ||= recovery.needsCheckpoint;
          }
          if (!backfillObserved) {
            const exit = backfill.pollUnsafe();

            if (exit !== undefined) {
              backfillObserved = true;
              if (Exit.isFailure(exit)) failure ??= exit.cause;
            }
          }
          for (const lane of lanes) {
            const exit = lane.fiber?.pollUnsafe();

            if (exit === undefined) continue;
            lane.fiber = undefined;
            if (Exit.isFailure(exit)) {
              failure ??= exit.cause;
              lane.exhausted = true;
            }
          }
          const deliveryExit = delivery?.pollUnsafe();

          if (deliveryExit !== undefined) {
            delivery = undefined;
            if (Exit.isFailure(deliveryExit)) {
              failure ??= deliveryExit.cause;
              messageExhausted = true;
            }
          }

          // A failed auxiliary lane is exhausted for this event. Its error must not
          // retire healthy admission, delivery or native work that still has an allowance.
          const now = yield* Clock.currentTimeMillis;

          if (now >= until) break;

          const queueRevision = yield* mutations.revision;
          const queued = yield* queueSnapshot;
          const producing = yield* mutations.activeLanes;

          yield* validateLanes(lanes, queued);
          for (const lane of lanes) {
            if (
              lane.phase === "after-native" ||
              lane.fiber !== undefined ||
              lane.exhausted ||
              producing.has(lane.id)
            )
              continue;
            const row = queued.find((row) => row.id === lane.id);

            if (row === undefined || row.dueAt === null || row.dueAt > now) continue;
            const dueAt = row.dueAt;

            lane.fiber = yield* fork(
              Effect.gen(function* () {
                yield* Schema.decodeEffect(AuxiliaryDispatchMillis)(
                  lane.dispatchTimeoutMillis,
                ).pipe(Effect.mapError(alarmFailure("host dispatch allowance")));
                const selectedAt = yield* Clock.currentTimeMillis;

                if (selectedAt >= until || selectedAt + lane.dispatchTimeoutMillis > dispatchEnd) {
                  lane.exhausted = true;

                  return Option.some(dueAt);
                }

                return yield* runQueued(row, lane.run.pipe(Effect.scoped), observed).pipe(
                  Effect.timeoutOrElse({
                    duration: lane.dispatchTimeoutMillis,
                    orElse: () =>
                      DurableAlarmError.make({
                        operation: "host dispatch allowance",
                        message:
                          "The admitted host wave exceeded its allowance; durable work remains pending",
                      }),
                  }),
                );
              }),
            );
          }
          if (delivery === undefined && !messageExhausted && !producing.has(DueQueue.Messages)) {
            // Selection is bounded local work. Fork only an actual due wave, so an empty
            // deadline check cannot extend retirement or consume another scheduling turn.
            const selected = yield* Effect.gen(function* () {
              const row = queued.find((row) => row.id === DueQueue.Messages);

              return row !== undefined &&
                row.dueAt !== null &&
                row.dueAt <= (yield* Clock.currentTimeMillis)
                ? Option.some({ row, wave: yield* messages.prepare })
                : Option.none();
            }).pipe(Effect.exit);

            if (Exit.isFailure(selected)) {
              failure ??= selected.cause;
              messageExhausted = true;
            } else if (Option.isSome(selected.value)) {
              const { row, wave } = selected.value.value;
              const selectedAt = yield* Clock.currentTimeMillis;

              if (selectedAt >= until || selectedAt + wave.timeoutMillis > dispatchEnd) {
                messageExhausted = true;
              } else {
                // No second timeout: the driver owes the Claim's timeout/retry commit.
                delivery = yield* fork(runQueued(row, wave.run, observed).pipe(Effect.asVoid));
              }
            }
          }

          // Retire a quiet event before consuming hints queued during its native Attempt.
          // Recovery still gets its checkpoint and, if needed, initial dispatch opportunity.
          if (
            native === undefined &&
            dispatch.active.size === 0 &&
            recoveryObserved &&
            !recovery.needsCheckpoint &&
            backfillObserved &&
            delivery === undefined &&
            lanes.every((lane) => lane.fiber === undefined)
          )
            break;

          if (
            native === undefined &&
            nativeCheck &&
            dispatch.active.size < nativeDispatchConcurrency
          ) {
            nativeCheck = false;
            const nativeCheckpoint = dispatch.needsCheckpoint && dispatch.active.size === 0;
            const checkpoint = (recoveryObserved && recovery.needsCheckpoint) || nativeCheckpoint;

            if (checkpoint) recovery.needsCheckpoint = false;
            // Keep this opportunity's reservations across beginNative's storage awaits.
            // A finishing Attempt cannot turn a refill into another same-Thread Attempt.
            const reserved = new Set(dispatch.active.keys());

            native = yield* fork(
              Effect.gen(function* () {
                const awakened = checkpoint ? started : yield* beginNative(observed);

                return yield* advance(
                  awakened,
                  yieldAfter,
                  observed,
                  recovery,
                  dispatch,
                  !nativeCheckpoint && (!checkpoint || dispatch.settled === 0),
                  reserved,
                );
              }),
            );
          }

          const next = Math.min(
            native === undefined ? (result.nextAttemptAt ?? Infinity) : Infinity,
            ...queued.flatMap((row) =>
              row.id !== DueQueue.Native && row.dueAt !== null && row.dueAt > now
                ? [row.dueAt]
                : [],
            ),
            until,
          );

          const completing = [
            ...(native === undefined ? [] : [native]),
            ...dispatch.active.values(),
            ...(recoveryObserved ? [] : [recoveryFiber]),
            ...(backfillObserved ? [] : [backfill]),
            ...(delivery === undefined ? [] : [delivery]),
            ...lanes.flatMap((lane) => (lane.fiber === undefined ? [] : [lane.fiber])),
          ];

          const ready = yield* Effect.raceAllFirst([
            ...completing.map((fiber) =>
              Fiber.await<unknown, MaintenancePassFailure>(fiber).pipe(
                Effect.as("completed" as const),
              ),
            ),
            Effect.raceAllFirst([
              notified,
              mutations.awaitChange(queueRevision),
              Effect.sleep(Math.max(0, next - now)),
            ]).pipe(Effect.as("scan" as const)),
          ]);

          if (ready === "scan") {
            nativeCheck = true;
          }
        }
        // Dispatch is closed. Join each admitted wave without renewing its allowance.
        // Message Claims retain their driver's own timer and local retry commit.
        if (native !== undefined) {
          result = yield* Fiber.join(native);
          if (result.phase === "actionable") phase = "actionable";
          observed.nativeOnly = dispatch.active.size > 0;
          recovery.started = true;
          yield* Deferred.succeed(recovery.queue, []);
        }
        for (const attempt of dispatch.active.values()) {
          const settled = yield* Fiber.join(attempt);

          dispatch.settled += settled;
          dispatch.progressed ||= settled > 0;
        }
        dispatch.active.clear();
        observed.nativeOnly = false;
        for (const fiber of [
          recoveryFiber,
          backfill,
          ...(delivery === undefined ? [] : [delivery]),
          ...lanes.flatMap((lane) => (lane.fiber === undefined ? [] : [lane.fiber])),
        ]) {
          // Await exits without failing fast: every admitted wave owns its original
          // bounded allowance and cleanup even if another wave has already failed.
          const exit = yield* Fiber.await<unknown, MaintenancePassFailure>(fiber);

          if (Exit.isFailure(exit)) failure ??= exit.cause;
        }
        if (failure !== undefined) return yield* Effect.failCause(failure);
        if (recovery.needsCheckpoint || dispatch.dispatched > 0) {
          result = yield* advance(started, yieldAfter, observed, recovery, dispatch, false);
          if (result.phase === "actionable") phase = "actionable";
        }
        yield* Scope.close(auxiliaryScope, Exit.void);

        return { result, phase, recovered: recovery.recovered, settled: dispatch.settled };
      });

      const pass = Effect.fn("ThreadMaintenance.pass")(function* (
        yieldAfter: DateTime.Utc,
        dispatchUntil: DateTime.Utc,
        observed: MaintenanceObservation,
      ): Effect.fn.Return<MaintenancePassReport, MaintenancePassFailure, Scope.Scope> {
        const concurrent = yield* Effect.exit(
          Effect.scoped(dispatch(yieldAfter, dispatchUntil, observed)),
        );

        // Closing the concurrent scope first releases every native Attempt and its resources.
        // Select fresh revisions: finalizers may have retained the last host obligation.
        const afterNative = yield* Effect.gen(function* () {
          const lanes = [recoveryEventLane, ...framework.lanes, ...host.lanes];
          const selected = lanes.filter((lane) => lane.phase === "after-native");

          if (selected.length === 0) return [];
          const queued = yield* queueSnapshot;
          const producing = yield* mutations.activeLanes;
          const dispatchEnd = DateTime.toEpochMillis(dispatchUntil);
          const afterNativeIds = new Set(selected.map((lane) => lane.id));
          const completed: Array<Exit.Exit<void, DurableAlarmError>> = [];
          let yielded = false;

          // Subscribe by revision before reading the queue, so an enrollment racing the
          // snapshot cannot be lost. Only producer notifications rescan scheduling state;
          // there is no timer or application deadline read. New arrivals renew no budget.
          const yieldToConcurrent = Effect.gen(function* () {
            while (true) {
              const revision = yield* mutations.revision;
              const now = yield* Clock.currentTimeMillis;

              const current = yield* queueSnapshot;
              const active = yield* mutations.activeLanes;

              if (
                current.some(
                  (row) =>
                    !afterNativeIds.has(row.id) &&
                    !active.has(row.id) &&
                    row.dueAt !== null &&
                    row.dueAt <= now &&
                    row.revision !== observed.available.get(row.id),
                )
              ) {
                yielded = true;

                return completed;
              }
              yield* mutations.awaitChange(revision);
            }
          });

          yield* validateLanes(lanes, queued);

          const cursor = yield* mutations.withSnapshot(() =>
            runTransaction("read post-native dispatch cursor", () =>
              ctx.storage.transaction(
                async (transaction) =>
                  (await readMaintenanceState(transaction)).state.lastAfterNativeLaneId,
              ),
            ),
          );

          const ordered = selected.toSorted((left, right) =>
            left.id < right.id ? -1 : left.id > right.id ? 1 : 0,
          );

          const after = cursor === undefined ? 0 : ordered.findIndex((lane) => lane.id > cursor);

          const rotated =
            after <= 0 ? ordered : [...ordered.slice(after), ...ordered.slice(0, after)];

          const waves = Effect.forEach(
            rotated,
            (lane) =>
              Effect.gen(function* () {
                const row = queued.find((row) => row.id === lane.id);
                const now = yield* Clock.currentTimeMillis;

                if (
                  row === undefined ||
                  row.dueAt === null ||
                  row.dueAt > now ||
                  producing.has(lane.id)
                )
                  return;
                yield* Schema.decodeEffect(AuxiliaryDispatchMillis)(
                  lane.dispatchTimeoutMillis,
                ).pipe(Effect.mapError(alarmFailure("host dispatch allowance")));
                if (now + lane.dispatchTimeoutMillis > dispatchEnd) return;

                // Persist admission, not completion: slow or failing early lanes must not
                // monopolize the next event's remaining allowance. Domain receipts stay local
                // to the lane and its due-queue revision still fences acknowledgement.
                const admit = mutations.withSnapshot(() =>
                  runTransaction("advance post-native dispatch cursor", () =>
                    ctx.storage.transaction(async (transaction) => {
                      const { state } = await readMaintenanceState(transaction);

                      await transaction.put(
                        MAINTENANCE_STATE_KEY,
                        encodeMaintenanceState(
                          ThreadMaintenanceState.make({ ...state, lastAfterNativeLaneId: lane.id }),
                        ),
                      );
                    }),
                  ),
                );

                yield* runQueued(
                  row,
                  admit.pipe(Effect.andThen(lane.run.pipe(Effect.scoped))),
                  observed,
                  () => yielded,
                ).pipe(
                  Effect.timeoutOrElse({
                    duration: lane.dispatchTimeoutMillis,
                    orElse: () =>
                      DurableAlarmError.make({
                        operation: "host dispatch allowance",
                        message:
                          "The admitted host wave exceeded its allowance; durable work remains pending",
                      }),
                  }),
                );
              }).pipe(
                Effect.exit,
                Effect.tap((exit) =>
                  Effect.sync(() => {
                    if (!yielded || Exit.isSuccess(exit) || !Cause.hasInterruptsOnly(exit.cause))
                      completed.push(exit);
                  }),
                ),
              ),
            { concurrency: afterNativeDispatchConcurrency },
          ).pipe(Effect.as(completed));

          // raceFirst waits for interrupted wave scopes to close before the alarm can
          // retire and admit native work in its next event. Finished receipts stay finished.
          return yield* Effect.raceFirst(waves, yieldToConcurrent);
        }).pipe(Effect.exit);

        let failure = Exit.isFailure(concurrent) ? concurrent.cause : Cause.empty;

        if (Exit.isFailure(afterNative)) failure = Cause.combine(failure, afterNative.cause);
        else
          for (const exit of afterNative.value) {
            if (Exit.isFailure(exit)) failure = Cause.combine(failure, exit.cause);
          }
        if (Exit.isFailure(concurrent) || failure.reasons.length > 0)
          return yield* Effect.failCause(failure);
        const { result, phase, recovered, settled } = concurrent.value;

        yield* failpoint.hit("maintenance:finish:before");

        const disposition = yield* mutations.withSnapshot((active) =>
          Effect.gen(function* () {
            const now = yield* Clock.currentTimeMillis;

            return yield* runTransaction("finish maintenance event", () =>
              ctx.storage.transaction(async (transaction) => {
                const { state } = await readMaintenanceState(transaction);

                const native =
                  active > 0 || state.dirty > state.processed
                    ? state.retry?.generation === state.dirty && active === 0
                      ? Math.max(now + minimumAlarmDelay, state.retry.notBefore)
                      : state.dirty === observed.generation && active === 0
                        ? (result.nextAttemptAt ?? now + minimumAlarmDelay)
                        : now + minimumAlarmDelay
                    : Infinity;

                dueQueue.checkpointNative(Number.isFinite(native) ? native : null);
                const next = DueQueue.next(dueQueue.read());

                if (Number.isFinite(next)) {
                  await transaction.setAlarm(Math.max(now + minimumAlarmDelay, next));

                  return "rearmed" as const;
                }
                await transaction.deleteAlarm();

                return "cleared" as const;
              }),
            );
          }),
        );

        yield* failpoint.hit("maintenance:finish:after");

        const report = MaintenancePassReport.make({
          phase,
          recovered,
          settled,
          nonterminal: result.nonterminal,
          alarm: disposition,
        });

        yield* Effect.annotateCurrentSpan({
          phase: report.phase,
          recovered: report.recovered,
          settled: report.settled,
          nonterminal: report.nonterminal,
          alarm: report.alarm,
        });

        return report;
      });

      return ThreadMaintenance.of({
        // A mid-pass immediate hint is droppable; durable dirty state decides the final alarm.
        pass: Effect.gen(function* () {
          const now = yield* Clock.currentTimeMillis;
          const yieldAfter = DateTime.makeUnsafe(now + 10 * 60_000);
          const dispatchUntil = DateTime.makeUnsafe(now + 14 * 60_000);

          const observed: MaintenanceObservation = {
            nativeOnly: false,
            queue: new Map(),
            available: new Map(),
          };

          return yield* alarm.withWakesDeferred(
            maintenancePassGate.withPermit(
              Effect.scoped(pass(yieldAfter, dispatchUntil, observed)).pipe(
                // Close event-owned auxiliary work and release Attempt ownership before
                // failure rearming, while still holding the pass permit.
                Effect.onErrorIf(
                  () => true,
                  () => rearmFailure(observed),
                ),
              ),
            ),
          );
        }).pipe(
          // Include permit waiting, recovery and acknowledgement in the event deadline.
          // Interruption releases Attempt ownership, leaving the prearmed dirty generation
          // for recovery. It never changes the logical Run duration or settles a policy failure.
          // This cooperative timer cannot preempt synchronous CPU work or stuck finalizers.
          Effect.timeoutOrElse({
            duration: "14 minutes",
            orElse: () =>
              DurableAlarmError.make({
                operation: "maintenance pass deadline",
                message:
                  "The maintenance event exceeded its 14 minute deadline; durable recovery remains pending",
              }),
          }),
        ),
        ensureAlarm: mutations.withSnapshot(() => ensureAlarm()),
        withMutation: (body) =>
          mutations.withMutation(
            body.pipe(Effect.tap(() => publishCommitted.pipe(Effect.provide(publicationContext)))),
          ),
      });
    }),
  );
}

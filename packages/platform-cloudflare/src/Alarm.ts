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
  Schema,
  Scope,
  Semaphore,
  Stream,
  Struct,
} from "effect";
import { type DurableBindingFailure } from "effect-agent/agent-registration";
import {
  DurableAgentRuntime,
  type DurableWorkerFailure,
  type RecoveryReport,
} from "effect-agent/durable-agent-runtime";
import { ThreadId, SubmissionId } from "effect-agent/identifiers";
import { SubmissionLedger, type SubmissionSnapshot } from "effect-agent/submission-ledger";
import {
  ThreadProjectionMaintenance,
  drainDue,
  type ThreadProjectionError,
} from "effect-agent/thread-projection-maintenance";
import { SqlClient } from "effect/unstable/sql/SqlClient";

import { DurableObjectContext } from "./CloudflareBindings.ts";
import { AuxiliaryDispatchMillis, CloudflareDurableRuntimeConfig } from "./CloudflareConfig.ts";
import { safeCauseMessage } from "./internal/boundary.ts";

/**
 * The single multiplexed Durable Object alarm (decision D-P6-2). A Durable Object has ONE
 * alarm slot; every cadence the Node host ran on fibers (wake scan, lease expiry, settlement
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
const makeStorageOperation = Effect.map(
  SqlClient,
  (sql) =>
    <A>(operation: string, execute: () => Promise<A>) =>
      Effect.flatMap(Effect.serviceOption(sql.transactionService), (current) => {
        const body = Effect.uninterruptible(
          Effect.tryPromise({ try: execute, catch: alarmFailure(operation) }),
        );

        return current._tag === "Some"
          ? body
          : Effect.scoped(
              Effect.andThen(sql.reserve.pipe(Effect.mapError(alarmFailure(operation))), body),
            );
      }),
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
  /** Recovery decisions executed (or deferred) BEFORE any new claim in this pass. */
  recovered: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  /** Whether the head Attempt settled. Joined input may settle with that head. */
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
  | "maintenance:retry:before"
  | "maintenance:retry:after"
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
 * `invalidate`, `prepareGeneration` and `pendingDeadline` must be bounded local operations.
 * `prepareGeneration` durably invalidates a scan only when its generation changes; repeated
 * calls must preserve partial scan progress. It runs with no source mutation in flight.
 * Use this gate only for publication required before dependent native execution. Independent
 * UI relays and outboxes belong to ThreadHostMaintenance.
 * `drain` performs bounded delivery and persists retries before returning. A pending deadline
 * defers runtime recovery/Attempts, allowing committed host publications to drain first.
 * Unexpected hook failures leave the prearmed generation for retry. Hooks acquire per-call
 * resources with Effect.scoped; Layer construction owns incarnation resources (eviction need
 * not run finalizers). Do not hold a local hook behind network I/O or call back into producers.
 */
export interface ThreadPublicationService {
  readonly invalidate: Effect.Effect<void, DurableAlarmError>;
  readonly prepareGeneration: (generation: bigint) => Effect.Effect<void, DurableAlarmError>;
  readonly drain: Effect.Effect<void, DurableAlarmError>;
  readonly pendingDeadline: Effect.Effect<Option.Option<number>, DurableAlarmError>;
}

/** Opt in with `ThreadObject.layer(registrations, { publication: Layer.effect(ThreadPublication)(...) })`. */
export class ThreadPublication extends Context.Service<
  ThreadPublication,
  ThreadPublicationService
>()("@effect-agent/platform-cloudflare/ThreadPublication") {
  static readonly layer = Layer.succeed(this)({
    invalidate: Effect.void,
    prepareGeneration: () => Effect.void,
    drain: Effect.void,
    pendingDeadline: Effect.succeed(Option.none()),
  });
}

/**
 * Host-assembled native message recovery. The driver bounds each actual Claim and persists its
 * timeout/retry before this pump returns. Do not add a second timer starting at batch selection:
 * local Claim setup may take time, and expiration still owes the driver's local retry commit.
 */
export const ThreadMessageDelivery = Context.Reference<{
  readonly drainUntil: (
    sourceFinished: Effect.Effect<void>,
    dispatchUntil: DateTime.Utc,
  ) => Effect.Effect<void, DurableAlarmError, Scope.Scope>;
  readonly pendingDeadline: Effect.Effect<Option.Option<number>, DurableAlarmError>;
}>("@effect-agent/platform-cloudflare/ThreadMessageDelivery", {
  defaultValue: () => ({
    drainUntil: () => Effect.void,
    pendingDeadline: Effect.succeed(Option.none()),
  }),
});

/**
 * Application obligations sharing this Object's alarm. Admit one initial external wave even on
 * a caught-up pass, then respond to wakes while native work runs. sourceFinished closes only
 * admission of NEW external waves. Keep local admission/hub subscriptions in the supplied event
 * Scope until maintenance tears it down; do not scope them to sourceFinished or to this Effect.
 * No deadline sleeps or automatic retry loops. Return after already-admitted waves finish.
 *
 * Declare a finite whole-wave allowance (1..300000ms): maximum for parallel lanes, sum for
 * sequential operations. Admit a wave only if its allowance fits before dispatchUntil. Later
 * arrivals cannot renew the retirement window. Maintenance bounds the join after sourceFinished,
 * interrupts and joins event Scope, then reads local deadlines under the mutation gate.
 *
 * Setup and pendingDeadline are bounded local operations. Persist claims/envelopes before
 * dispatch; interrupted waits leave exact retries/receipts recoverable. Cancellation is local,
 * not remote rollback. Retry accounting is unchanged. Network waits/finalizers must be
 * interruptible; short local atomic commits may be uninterruptible. Hooks never write the raw
 * alarm slot. Required native publication gates belong to ThreadPublication.
 */
export const ThreadHostMaintenance = Context.Reference<{
  readonly dispatchTimeoutMillis: number;
  readonly drainUntil: (
    sourceFinished: Effect.Effect<void>,
    dispatchUntil: DateTime.Utc,
  ) => Effect.Effect<void, DurableAlarmError, Scope.Scope>;
  readonly pendingDeadline: Effect.Effect<Option.Option<number>, DurableAlarmError>;
}>("@effect-agent/platform-cloudflare/ThreadHostMaintenance", {
  defaultValue: () => ({
    dispatchTimeoutMillis: 1,
    drainUntil: () => Effect.void,
    pendingDeadline: Effect.succeed(Option.none()),
  }),
});

const earliestDeadline = (
  left: Option.Option<number>,
  right: Option.Option<number>,
): Option.Option<number> =>
  Option.isSome(left)
    ? Option.isSome(right)
      ? Option.some(Math.min(left.value, right.value))
      : left
    : right;

/** @internal A committed source operation must not become a failed operation because delivery failed. */
export const publishCommitted = Effect.gen(function* () {
  const publication = yield* ThreadPublication;

  yield* publication.invalidate.pipe(Effect.andThen(publication.drain));
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

class BindingRetry extends Schema.Class<BindingRetry>("BindingRetry")({
  threadId: ThreadId,
  submissionId: SubmissionId,
  attempts: Schema.Natural,
  notBefore: Schema.Finite,
  reportedAt: Schema.Finite,
}) {}

interface MaintenanceObservation {
  generation?: bigint;
  nativeOnly: boolean;
  /** Selected-head outcome survives an unrelated auxiliary failure before acknowledgement. */
  bindingRetry?: {
    readonly submissionId: SubmissionId;
    readonly retry: BindingRetry | undefined;
  };
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
  bindingRetries: Schema.optionalKey(Schema.Array(BindingRetry)),
  /** Absent on older records. A newer mutation makes this retry obsolete. */
  retry: Schema.optionalKey(MaintenanceRetry),
}) {}

const MAINTENANCE_STATE_KEY = "effect-agent:thread-maintenance:v1";
const decodeMaintenanceState = Schema.decodeUnknownSync(ThreadMaintenanceState);
const encodeMaintenanceState = Schema.encodeSync(ThreadMaintenanceState);

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
  transaction: DurableObjectTransaction,
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
  snapshot: SubmissionSnapshot,
  reports: ReadonlyMap<string, RecoveryReport>,
): boolean => {
  const decision = reports.get(snapshot.submissionId)?.decision._tag;

  // An accepted abort still owes cleanup/settlement even if its claim was deferred this pass.
  if (decision === "SettleAborted") return false;
  switch (snapshot.state) {
    case "suspended":
    case "joined":
      return true;
    case "unknown":
      return decision === "AwaitUnknownResolution" || decision === "MarkUnknown";
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
       * Projection/relay/reply receipt-only bookkeeping must use false: its local durable
       * pendingDeadline owns scheduling without creating native recovery debt.
       */
      options?: { readonly invalidatesRecovery: boolean },
    ) => Effect.Effect<A, E | DurableAlarmError, R>;
    readonly withSnapshot: <A, E, R>(
      body: (active: number) => Effect.Effect<A, E, R>,
    ) => Effect.Effect<A, E, R>;
  }
>()("@effect-agent/platform-cloudflare/internal/ThreadMutationGate") {
  static readonly layer = Layer.effect(this)(
    Effect.gen(function* () {
      const { ctx } = yield* DurableObjectContext;
      const config = yield* CloudflareDurableRuntimeConfig;
      const failpoint = yield* ThreadMaintenanceFailpoint;
      // A fresh incarnation has no live mutations; durable generations survive eviction.
      const activeMutations = yield* Ref.make(0);
      const generationGate = yield* Semaphore.make(1);
      const minimumAlarmDelay = Math.max(1, Math.ceil(config.alarmBackoffBase / 2));

      const runTransaction = yield* makeStorageOperation;

      const beginMutation = Effect.fn("ThreadMaintenance.beginMutation")(function* (
        invalidatesRecovery: boolean,
      ) {
        yield* failpoint.hit("maintenance:dirty:before");
        const now = yield* Clock.currentTimeMillis;

        yield* runTransaction("advance maintenance generation", () =>
          ctx.storage.transaction(async (transaction) => {
            const { state, initialized } = await readMaintenanceState(transaction);

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
      });

      const endMutation = generationGate.withPermit(
        Ref.update(activeMutations, (active) => Math.max(0, active - 1)),
      );

      const withMutation = <A, E, R>(
        body: Effect.Effect<A, E, R>,
        options?: { readonly invalidatesRecovery: boolean },
      ): Effect.Effect<A, E | DurableAlarmError, R> =>
        Effect.acquireUseRelease(
          generationGate.withPermit(beginMutation(options?.invalidatesRecovery ?? true)),
          () =>
            failpoint.hit("maintenance:mutation:armed").pipe(
              Effect.andThen(body),
              Effect.tap(() => failpoint.hit("maintenance:mutation:finished")),
            ),
          () => endMutation,
        );

      return ThreadMutationGate.of({
        withMutation,
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
 * `pass` = generation snapshot/pre-arm → recovery → one head Attempt → generation acknowledgement:
 *
 * 1. One storage transaction reads dirty/processed and re-arms before work. A caught-up forced
 *    alarm takes an O(1) path without recovery, ledger scans, or canonical-history reads.
 * 2. Recovery strictly precedes a new claim. One head Attempt advances the lane and requests
 *    a safe yield after ten minutes. The whole event has a fourteen-minute cooperative timeout.
 * 3. Auxiliary dispatch and listeners belong to the event Scope. After native completion,
 *    stop new external waves, join native delivery through its driver-owned Claim deadline,
 *    and bound host/backfill work by explicit allowances before closing and joining Scope.
 *    Ordinary auxiliary setup failures are reported after the one native opportunity.
 * 4. The final transaction acknowledges only the generation observed at pass start. A racing
 *    mutation therefore remains `dirty > processed` and retains its atomically-established alarm.
 * 5. Stable external waits acknowledge and clear. Autonomous retry, indeterminate, and lease
 *    recovery states leave their generation dirty and retain bounded backoff rearming.
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
    never,
    | ThreadMutationGate
    | ThreadPublication
    | ThreadProjectionMaintenance
    | DurableAgentRuntime
    | SubmissionLedger
    | DurableAlarmService
    | ThreadMaintenanceFailpoint
    | CloudflareDurableRuntimeConfig
    | DurableObjectContext
    | SqlClient
  > = Layer.effect(ThreadMaintenance)(
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const ledger = yield* SubmissionLedger;
      const alarm = yield* DurableAlarmService;
      const config = yield* CloudflareDurableRuntimeConfig;
      const { ctx } = yield* DurableObjectContext;
      const failpoint = yield* ThreadMaintenanceFailpoint;

      const mutations = yield* ThreadMutationGate;
      const publication = yield* ThreadPublication;
      const projection = yield* ThreadProjectionMaintenance;
      const messages = yield* ThreadMessageDelivery;
      const host = yield* ThreadHostMaintenance;

      // A broken disposable index still needs a retry alarm and must not prevent startup.
      const projectionDeadline = projection.pendingDeadline.pipe(
        Effect.catchCauseIf(
          (cause) => !Cause.hasInterrupts(cause),
          (cause) =>
            Effect.logError("Thread projection deadline unavailable", cause).pipe(
              Effect.as(Option.some(0)),
            ),
        ),
      );

      const pendingDeadline = Effect.gen(function* () {
        return earliestDeadline(
          earliestDeadline(yield* publication.pendingDeadline, yield* messages.pendingDeadline),
          earliestDeadline(yield* projectionDeadline, yield* host.pendingDeadline),
        );
      });

      const maintenancePassGate = yield* Semaphore.make(1);
      const minimumAlarmDelay = Math.max(1, Math.ceil(config.alarmBackoffBase / 2));

      const runTransaction = yield* makeStorageOperation;

      const ensureAlarm = Effect.fn("ThreadMaintenance.ensureAlarm")(function* () {
        yield* failpoint.hit("maintenance:ensure:before");
        const now = yield* Clock.currentTimeMillis;

        const retry = yield* runTransaction("ensure maintenance alarm", () =>
          ctx.storage.transaction(async (transaction) => {
            const { state, initialized } = await readMaintenanceState(transaction);

            if (!initialized) {
              await transaction.put(MAINTENANCE_STATE_KEY, encodeMaintenanceState(state));
            }
            if (state.dirty > state.processed) {
              await ensureTransactionAlarmBy(
                transaction,
                state.retry?.generation === state.dirty
                  ? Math.max(now + minimumAlarmDelay, state.retry.notBefore)
                  : now + config.wakeScanInterval,
              );
            }

            return state.retry?.generation === state.dirty ? state.retry : undefined;
          }),
        );

        const deadline = yield* pendingDeadline;

        if (Option.isSome(deadline)) {
          yield* runTransaction("ensure publication alarm", () =>
            ctx.storage.transaction((transaction) =>
              ensureTransactionAlarmBy(
                transaction,
                Math.max(
                  now + minimumAlarmDelay,
                  deadline.value <= now && retry !== undefined && !retry.nativeOnly
                    ? Math.max(deadline.value, retry.notBefore)
                    : deadline.value,
                ),
              ),
            ),
          );
        }
        yield* failpoint.hit("maintenance:ensure:after");
      });

      const beginPass = Effect.fn("ThreadMaintenance.beginPass")(function* (observed: {
        generation?: bigint;
      }) {
        yield* failpoint.hit("maintenance:begin:before");
        const now = yield* Clock.currentTimeMillis;

        const result = yield* runTransaction("begin maintenance pass", () =>
          ctx.storage.transaction(async (transaction) => {
            const { state, initialized } = await readMaintenanceState(transaction);

            observed.generation = state.dirty;
            if (!initialized) {
              await transaction.put(MAINTENANCE_STATE_KEY, encodeMaintenanceState(state));
            }
            const retryAt = state.retry?.generation === state.dirty ? state.retry.notBefore : 0;

            if (state.processed >= state.dirty || retryAt > now) {
              // Prearm even a publication-only pass before invoking any host hook.
              await ensureTransactionAlarmBy(transaction, now + minimumAlarmDelay);

              return {
                _tag: "CaughtUp" as const,
                nonterminal: state.nonterminal,
              };
            }
            // Pre-arm the earliest retry before recovery. A successful finish may move this slot
            // LATER to its bounded backoff, which does not cancel the running handler.
            await ensureTransactionAlarmBy(transaction, now + minimumAlarmDelay);

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
        const { generation, nativeOnly, bindingRetry } = observed;

        if (generation === undefined) return;
        yield* failpoint.hit("maintenance:retry:before");
        yield* mutations.withSnapshot((active) =>
          Effect.gen(function* () {
            // A failed deadline read must not prevent committing the native retry.
            const deadline = yield* pendingDeadline.pipe(
              Effect.catchCause(() => Effect.succeed(Option.none<number>())),
            );

            const now = yield* Clock.currentTimeMillis;

            const jitter = yield* Random.next;

            yield* runTransaction("back off failed maintenance", () =>
              ctx.storage.transaction(async (transaction) => {
                const { state } = await readMaintenanceState(transaction);

                const previous = state.retry?.generation === generation ? state.retry : undefined;

                const retry = MaintenanceRetry.make({
                  generation,
                  notBefore: Math.max(
                    previous?.notBefore ?? 0,
                    now + backoffDelay(previous?.stalls ?? 0, jitter),
                  ),
                  nativeOnly,
                  stalls: Math.min(30, (previous?.stalls ?? 0) + 1),
                });

                // Binding resolution already released its Claim. Retain that selected
                // submission's cooldown (or clear) even when auxiliary retirement failed.
                // Preserve every other lane and commit both retry facts with the alarm.
                const bindingRetries =
                  bindingRetry === undefined
                    ? undefined
                    : [
                        ...(state.bindingRetries ?? []).filter(
                          (entry) => entry.submissionId !== bindingRetry.submissionId,
                        ),
                        ...(bindingRetry.retry === undefined ? [] : [bindingRetry.retry]),
                      ];

                await transaction.put(
                  MAINTENANCE_STATE_KEY,
                  encodeMaintenanceState(
                    ThreadMaintenanceState.make({
                      ...state,
                      retry,
                      ...(bindingRetries === undefined ? {} : { bindingRetries }),
                    }),
                  ),
                );

                // Never postpone a producer that raced the failed observation. Host work
                // retains its own deadline; an early delivery skips native recovery below.
                const nativeDeadline =
                  active > 0 || state.dirty !== generation
                    ? now + minimumAlarmDelay
                    : retry.notBefore;

                await transaction.setAlarm(
                  Math.max(
                    now + minimumAlarmDelay,
                    Option.isSome(deadline) && (nativeOnly || deadline.value > now)
                      ? Math.min(nativeDeadline, deadline.value)
                      : nativeDeadline,
                  ),
                );
              }),
            );
          }),
        );
        yield* failpoint.hit("maintenance:retry:after");
      });

      const pass = Effect.fn("ThreadMaintenance.pass")(function* (
        yieldAfter: DateTime.Utc,
        dispatchUntil: DateTime.Utc,
        observed: MaintenanceObservation,
      ): Effect.fn.Return<MaintenancePassReport, MaintenancePassFailure, Scope.Scope> {
        const annotate = (report: MaintenancePassReport) =>
          Effect.annotateCurrentSpan({
            phase: report.phase,
            recovered: report.recovered,
            settled: report.settled,
            nonterminal: report.nonterminal,
            alarm: report.alarm,
          }).pipe(Effect.as(report));

        const started = yield* mutations.withSnapshot((activeAtStart) =>
          Effect.gen(function* () {
            const generation = yield* beginPass(observed);

            if (generation._tag === "Actionable" && activeAtStart === 0) {
              // The gate excludes a producer starting between the snapshot and certification.
              yield* publication.prepareGeneration(generation.generation);
            }

            return { ...generation, activeAtStart };
          }),
        );

        // This scope owns auxiliary dispatch and listeners, independently of source completion.
        // Close it before acknowledgement, including on failure or event interruption.
        const auxiliaryScope = yield* Effect.acquireRelease(Scope.make("parallel"), (scope, exit) =>
          Scope.close(scope, exit),
        );

        const sourceFinished = yield* Deferred.make<void>();
        const finished = Deferred.await(sourceFinished);

        // Fork setup too: an ordinary auxiliary setup failure is reported after native work,
        // rather than gating its opportunity. Event interruption still closes every fiber.
        const deliveryFiber = yield* Effect.forkIn(
          Scope.provide(auxiliaryScope)(messages.drainUntil(finished, dispatchUntil)),
          auxiliaryScope,
        );

        const hostFiber = yield* Effect.forkIn(
          Scope.provide(auxiliaryScope)(
            Effect.gen(function* () {
              yield* Schema.decodeEffect(AuxiliaryDispatchMillis)(host.dispatchTimeoutMillis).pipe(
                Effect.mapError((cause) =>
                  DurableAlarmError.make({
                    operation: "host dispatch allowance",
                    message:
                      "Declare an integer whole-wave allowance between 1 and 300000 milliseconds",
                    cause,
                  }),
                ),
              );
              yield* host.drainUntil(finished, dispatchUntil);
            }),
          ),
          auxiliaryScope,
        );

        // Backfill is one disposable wave; its timer starts beside native execution.
        const backfill = yield* Effect.forkIn(
          drainDue.pipe(
            Effect.provideService(ThreadProjectionMaintenance, projection),
            Effect.timeoutOption(config.projectionDispatchTimeoutMillis),
          ),
          auxiliaryScope,
        );

        const finishAuxiliary = Effect.gen(function* () {
          yield* Deferred.succeed(sourceFinished, undefined);

          const remaining = Math.max(
            1,
            DateTime.toEpochMillis(dispatchUntil) - (yield* Clock.currentTimeMillis),
          );

          const hostJoin = yield* Effect.forkIn(
            Fiber.join(hostFiber).pipe(
              Effect.timeoutOption(Math.min(host.dispatchTimeoutMillis, remaining)),
              Effect.tap((result) =>
                Effect.annotateCurrentSpan({ "host.timedOut": Option.isNone(result) }),
              ),
            ),
            auxiliaryScope,
          );

          // Native transport uses the driver's actual Claim deadline, through Retry persistence.
          // Only our host/backfill timers suppress their own interruption. Earlier failed exits,
          // including pure self-interruption, remain failures while siblings are still blocked.
          yield* Fiber.joinAll([deliveryFiber, hostJoin, backfill]);
          yield* Scope.close(auxiliaryScope, Exit.void);
        });

        const deadline = yield* publication.pendingDeadline;

        if (
          started._tag === "Actionable" ||
          (Option.isSome(deadline) && deadline.value <= (yield* Clock.currentTimeMillis))
        ) {
          yield* publication.drain;
        }
        const pending = yield* publication.pendingDeadline;

        if (started._tag === "CaughtUp" || Option.isSome(pending)) {
          yield* finishAuxiliary;
          yield* failpoint.hit("maintenance:finish:before");

          const disposition = yield* mutations.withSnapshot((active) =>
            Effect.gen(function* () {
              // Re-read under the producer gate: a concurrent append/host mutation cannot be
              // cleared using a stale empty deadline. Dirty generations bound all producer races.
              const latest = yield* pendingDeadline;
              const now = yield* Clock.currentTimeMillis;

              return yield* runTransaction("finish publication pass", () =>
                ctx.storage.transaction(async (transaction) => {
                  const { state } = await readMaintenanceState(transaction);

                  const nativeDeadline =
                    active > 0 || state.dirty > state.processed
                      ? state.retry?.generation === state.dirty && active === 0
                        ? Math.max(now + minimumAlarmDelay, state.retry.notBefore)
                        : now + config.wakeScanInterval
                      : Infinity;

                  const next = Option.isSome(latest)
                    ? Math.min(nativeDeadline, latest.value)
                    : nativeDeadline;

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

          return yield* annotate(
            MaintenancePassReport.make({
              phase: "caught-up",
              recovered: 0,
              settled: 0,
              nonterminal: started.nonterminal,
              alarm: disposition,
            }),
          );
        }
        // Step 2 — reconciliation strictly precedes new work in this pass (exit gate).
        observed.nativeOnly = true;
        const recovered: ReadonlyArray<RecoveryReport> = yield* runtime.runRecovery;
        const reports = new Map(recovered.map((report) => [report.submissionId, report]));
        const current = yield* Stream.runCollect(ledger.scanNonterminal);
        const heads = new Map<ThreadId, SubmissionSnapshot>();

        for (const row of current) {
          if (!heads.has(row.threadId)) heads.set(row.threadId, row);
        }

        const eligible = [...heads.values()]
          .filter((head) => !stableExternalWait(head, reports))
          .map((head) => head.threadId)
          .sort();

        const selectionTime = yield* Clock.currentTimeMillis;

        yield* failpoint.hit("maintenance:select:before");

        const selection = yield* runTransaction("select maintenance lane", () =>
          ctx.storage.transaction(async (transaction) => {
            const { state } = await readMaintenanceState(transaction);

            const retries = (state.bindingRetries ?? []).filter(
              (retry) => heads.get(retry.threadId)?.submissionId === retry.submissionId,
            );

            const runnable = eligible.filter(
              (threadId) =>
                !retries.some(
                  (retry) => retry.threadId === threadId && retry.notBefore > selectionTime,
                ),
            );

            const next =
              runnable.find(
                (threadId) =>
                  state.lastServedThreadId === undefined || threadId > state.lastServedThreadId,
              ) ?? runnable[0];

            if (next !== undefined) {
              await transaction.put(
                MAINTENANCE_STATE_KEY,
                encodeMaintenanceState(
                  ThreadMaintenanceState.make({ ...state, lastServedThreadId: next }),
                ),
              );
            }

            return { selected: next, retries };
          }),
        );

        yield* failpoint.hit("maintenance:select:after");
        const selected = selection.selected;
        let retries = selection.retries;
        let bindingFailure: DurableBindingFailure | undefined;
        let reportBindingFailure = false;

        // One FIFO head per event. An unavailable contract is a durable wait for compatible code,
        // including for children; other local lanes and host deliveries remain independently due.
        const settlement =
          selected === undefined
            ? Option.none()
            : yield* runtime.processThreadHead(selected, { yieldAfter }).pipe(
                Effect.catchTags({
                  BindingUnavailable: (failure) => {
                    bindingFailure = failure;

                    return Effect.succeed(Option.none());
                  },
                  BindingDigestMismatch: (failure) => {
                    bindingFailure = failure;

                    return Effect.succeed(Option.none());
                  },
                }),
              );

        if (selected !== undefined) {
          const previous = retries.find((retry) => retry.threadId === selected);

          retries = retries.filter((retry) => retry.threadId !== selected);
          const head = heads.get(selected);

          if (bindingFailure !== undefined && head !== undefined) {
            const now = yield* Clock.currentTimeMillis;
            const attempts = Math.min(30, (previous?.attempts ?? 0) + 1);

            reportBindingFailure =
              previous === undefined || now - previous.reportedAt >= 15 * 60_000;
            retries.push(
              BindingRetry.make({
                threadId: selected,
                submissionId: head.submissionId,
                attempts,
                notBefore: now + Math.min(60_000, 5_000 * 2 ** (attempts - 1)),
                reportedAt: reportBindingFailure ? now : previous!.reportedAt,
              }),
            );
          }
          if (head !== undefined) {
            observed.bindingRetry = {
              submissionId: head.submissionId,
              retry: retries.find((retry) => retry.submissionId === head.submissionId),
            };
          }
        }

        observed.nativeOnly = false;
        yield* finishAuxiliary;
        const remaining = yield* Stream.runCollect(ledger.scanNonterminal);
        const waitingHeads = new Map<ThreadId, boolean>();

        const autonomous = remaining.some((snapshot) => {
          const headWaiting = waitingHeads.get(snapshot.threadId);

          if (headWaiting === undefined)
            waitingHeads.set(snapshot.threadId, stableExternalWait(snapshot, reports));
          // FIFO followers cannot execute through a stable external wait. Only plain queued
          // input is dormant here; admission repairs and accepted aborts still need a pass.
          if (
            headWaiting === true &&
            snapshot.state === "ready" &&
            reports.get(snapshot.submissionId)?.decision._tag === "ApplyInput"
          )
            return false;

          return !stableExternalWait(snapshot, reports);
        });

        const progressed =
          Option.isSome(settlement) ||
          recovered.some((report) => report.disposition === "repaired");

        const now = yield* Clock.currentTimeMillis;
        const ordinaryDelay = autonomous ? yield* rearmDelay(progressed, started.stalls) : 0;

        const nextEligible = eligible.map(
          (threadId) => retries.find((retry) => retry.threadId === threadId)?.notBefore ?? now,
        );

        const bindingDelay =
          nextEligible.length === 0 ? 0 : Math.max(0, Math.min(...nextEligible) - now);

        const delay = Math.max(ordinaryDelay, bindingDelay);

        yield* failpoint.hit("maintenance:finish:before");

        const alarmDisposition = yield* mutations.withSnapshot((active) =>
          Effect.gen(function* () {
            const publicationDeadline = yield* pendingDeadline;

            return yield* runTransaction("finish maintenance pass", () =>
              ctx.storage.transaction(async (transaction) => {
                const { state } = await readMaintenanceState(transaction);

                // Autonomous work and in-flight mutations intentionally leave the observed
                // generation dirty. Otherwise acknowledge only the pass-start generation.
                const processed =
                  autonomous || started.activeAtStart > 0 || active > 0
                    ? state.processed
                    : state.processed > started.generation
                      ? state.processed
                      : started.generation;

                const next = ThreadMaintenanceState.make({
                  ...Struct.omit(state, ["retry"]),
                  processed,
                  nonterminal: remaining.length,
                  bindingRetries: retries.filter((retry) =>
                    remaining.some((row) => row.submissionId === retry.submissionId),
                  ),
                  ...(autonomous && !progressed
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
                if (autonomous) {
                  // Replace the crash-fallback slot with this pass's bounded backoff. The target
                  // is never earlier than the begin-pass fallback, so workerd does not cancel
                  // this running alarm handler before its report/span can complete.
                  const nativeDeadline =
                    started.activeAtStart > 0 || active > 0 || state.dirty !== started.generation
                      ? now + minimumAlarmDelay
                      : now + delay;

                  await transaction.setAlarm(
                    Option.isSome(publicationDeadline)
                      ? Math.max(
                          now + minimumAlarmDelay,
                          Math.min(nativeDeadline, publicationDeadline.value),
                        )
                      : nativeDeadline,
                  );

                  return "rearmed" as const;
                }
                if (started.activeAtStart > 0 || active > 0 || next.dirty > next.processed) {
                  // A mutation overlapped this pass's observation window or raced
                  // acknowledgement. It stays dirty and its pre-armed bounded alarm survives;
                  // unseen effects are never acknowledged. Do not accelerate that future alarm
                  // from inside the current handler: workerd cancels a running handler when it
                  // writes an earlier slot.
                  await ensureTransactionAlarmBy(
                    transaction,
                    Option.isSome(publicationDeadline)
                      ? Math.max(
                          now + minimumAlarmDelay,
                          Math.min(now + config.wakeScanInterval, publicationDeadline.value),
                        )
                      : now + config.wakeScanInterval,
                  );

                  return "rearmed" as const;
                }
                if (Option.isSome(publicationDeadline)) {
                  await transaction.setAlarm(
                    Math.max(now + minimumAlarmDelay, publicationDeadline.value),
                  );

                  return "rearmed" as const;
                }
                await transaction.deleteAlarm();

                return "cleared" as const;
              }),
            );
          }),
        );

        yield* failpoint.hit("maintenance:finish:after");
        if (bindingFailure !== undefined) {
          yield* reportBindingFailure
            ? Effect.logError(
                "Thread awaits a compatible binding; original work remains pending",
                Cause.fail(bindingFailure),
              )
            : Effect.logDebug("Thread binding retry remains pending", Cause.fail(bindingFailure));
        }

        return yield* annotate(
          MaintenancePassReport.make({
            phase: "actionable",
            recovered: recovered.length,
            settled: Option.isSome(settlement) ? 1 : 0,
            nonterminal: remaining.length,
            alarm: alarmDisposition,
          }),
        );
      });

      return ThreadMaintenance.of({
        // A mid-pass immediate hint is droppable; durable dirty state decides the final alarm.
        pass: Effect.gen(function* () {
          const now = yield* Clock.currentTimeMillis;
          const yieldAfter = DateTime.makeUnsafe(now + 10 * 60_000);
          const dispatchUntil = DateTime.makeUnsafe(now + 14 * 60_000);
          const observed: MaintenanceObservation = { nativeOnly: false };

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
            body.pipe(
              Effect.tap(() =>
                publishCommitted.pipe(Effect.provideService(ThreadPublication, publication)),
              ),
            ),
          ),
      });
    }),
  );
}

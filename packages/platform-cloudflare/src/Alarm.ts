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
  RecoveryFailure,
  type DurableWorkerFailure,
  type RecoveryReport,
} from "effect-agent/durable-agent-runtime";
import { ThreadId, SubmissionId } from "effect-agent/identifiers";
import {
  OperationAuthorizationRequest,
  OperationAuthorizer,
  type OperationDenied,
} from "effect-agent/operation-authorizer";
import { SubmissionLedger, type SubmissionSnapshot } from "effect-agent/submission-ledger";
import {
  ThreadProjectionMaintenance,
  drainDue,
  type ThreadProjectionError,
} from "effect-agent/thread-projection-maintenance";
import { WakeScheduler } from "effect-agent/wake-scheduler";
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
    dispatchClosed: Effect.Effect<void>,
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
 * a caught-up pass, then respond to wakes until dispatchClosed. This closes only admission
 * of NEW external waves; native work can continue while admitted waves finish. Keep local
 * admission/hub subscriptions in the event Scope until maintenance tears it down.
 * No deadline sleeps or automatic retry loops. Return after already-admitted waves finish.
 *
 * Declare a finite whole-wave allowance (1..300000ms): maximum for parallel lanes, sum for
 * sequential operations. Admit a wave only if its allowance fits before dispatchUntil. Later
 * arrivals cannot renew the retirement window. Maintenance bounds the join after dispatchClosed,
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
    dispatchClosed: Effect.Effect<void>,
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

/**
 * A durable observation of blocked recovery, independent of the execution journal. This is
 * neither a Settlement nor proof that external effects did not happen. A successful recovery
 * sweep clears it; repair must preserve canonical history and the original admission identity.
 */
export class ThreadRecoveryFault extends Schema.Class<ThreadRecoveryFault>(
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

interface NativePassResult {
  readonly phase: "caught-up" | "actionable";
  readonly recovered: number;
  readonly settled: number;
  readonly nonterminal: number;
  readonly nextAttemptAt: number | undefined;
}

interface MaintenanceObservation {
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
  const report = reports.get(snapshot.submissionId);
  const decision = report?.decision._tag;

  // A failed read cannot prove even a suspended lane is a stable external wait.
  if (decision === "RecoveryBlocked") return false;
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
 * One physical event owns native scheduling, auxiliary delivery and final alarm rearming.
 *
 * 1. Prearm before any work. A caught-up native step uses the O(1) generation record without
 *    recovery, ledger scans or canonical-history reads.
 * 2. Reconcile before each head Attempt, then checkpoint only the observed generation. A racing
 *    producer keeps its newer generation dirty. Native retries retain their durable backoff.
 * 3. After the initial native opportunity, stop admitting new external waves and let the
 *    active waves finish. While they remain in flight, native wakes and bounded scans can
 *    advance more heads. All Attempts share the event's original ten-minute yield deadline.
 * 4. Native message delivery retains its driver-owned Claim deadline. Host/backfill joins are
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
     * Authorize `explain`, then read one bounded local record without reading execution history.
     * None means no recorded fault, not proof of health or settlement. The host authenticates
     * callers and verifies local Thread membership before exposing this service across RPC.
     */
    readonly recoveryStatus: (
      threadId: ThreadId,
    ) => Effect.Effect<Option.Option<ThreadRecoveryFault>, DurableAlarmError | OperationDenied>;
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
    | WakeScheduler
    | DurableAlarmService
    | ThreadMaintenanceFailpoint
    | CloudflareDurableRuntimeConfig
    | DurableObjectContext
    | SqlClient
  > = Layer.effect(ThreadMaintenance)(
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const ledger = yield* SubmissionLedger;
      const wakes = yield* WakeScheduler;
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

      const recoveryStatus = Effect.fn("ThreadMaintenance.recoveryStatus")(function* (
        threadId: ThreadId,
      ) {
        const authorizer = yield* OperationAuthorizer;

        yield* authorizer.authorize(
          OperationAuthorizationRequest.make({ operation: "explain", threadId }),
        );

        return yield* runTransaction("read Thread recovery status", async () => {
          const encoded = await ctx.storage.get(recoveryFaultKey(threadId));

          return encoded === undefined
            ? Option.none()
            : Option.some(decodeRecoveryFault(threadId, encoded));
        });
      });

      const recordRecoveryStatus = Effect.fn("ThreadMaintenance.recordRecoveryStatus")(function* (
        reports: ReadonlyArray<RecoveryReport>,
      ) {
        const threads = new Map<ThreadId, RecoveryFailure | undefined>();

        for (const report of reports) {
          if (report.decision._tag === "RecoveryBlocked")
            threads.set(report.threadId, report.decision.failure);
          else if (!threads.has(report.threadId)) threads.set(report.threadId, undefined);
        }
        if (threads.size === 0) return new Map<ThreadId, ThreadRecoveryFault>();
        const now = yield* Clock.currentTimeMillis;

        yield* failpoint.hit("maintenance:recovery-status:before");

        const result = yield* runTransaction("record Thread recovery status", () =>
          ctx.storage.transaction(async (transaction) => {
            const newlyBlocked: Array<ThreadRecoveryFault> = [];
            const faults = new Map<ThreadId, ThreadRecoveryFault>();

            for (const [threadId, failure] of threads) {
              const key = recoveryFaultKey(threadId);
              const encoded = await transaction.get(key);

              const previous =
                encoded === undefined ? undefined : decodeRecoveryFault(threadId, encoded);

              if (failure === undefined) {
                if (previous !== undefined) await transaction.delete(key);
                continue;
              }

              const fault = ThreadRecoveryFault.make({
                schemaVersion: 1,
                threadId,
                firstFailedAt: previous?.firstFailedAt ?? now,
                lastFailedAt: now,
                attempts: Math.min(2_147_483_647, (previous?.attempts ?? 0) + 1),
                retryAt: now + Math.min(60_000, 5_000 * 2 ** Math.min(30, previous?.attempts ?? 0)),
                failure,
              });

              await transaction.put(key, encodeRecoveryFault(fault));
              faults.set(threadId, fault);
              if (previous === undefined) newlyBlocked.push(fault);
            }

            return { newlyBlocked, faults };
          }),
        );

        yield* failpoint.hit("maintenance:recovery-status:after");
        for (const fault of result.newlyBlocked)
          yield* Effect.logError("Native Thread recovery blocked; accepted work remains pending", {
            threadId: fault.threadId,
            failure: fault.failure,
          });

        return result.faults;
      });

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
            // A failed deadline read must not prevent committing the native retry.
            const deadline = yield* pendingDeadline.pipe(
              Effect.catchCause(() => Effect.succeed(Option.none<number>())),
            );

            const now = yield* Clock.currentTimeMillis;

            const jitter = yield* Random.next;

            yield* runTransaction("back off failed maintenance", () =>
              ctx.storage.transaction(async (transaction) => {
                const { state } = await readMaintenanceState(transaction);

                const previous =
                  state.retry?.generation === generation && state.retry.nativeOnly === nativeOnly
                    ? state.retry
                    : undefined;

                const retry = MaintenanceRetry.make({
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

      const beginNative = Effect.fn("ThreadMaintenance.beginNative")(function* (
        observed: MaintenanceObservation,
      ) {
        return yield* mutations.withSnapshot((activeAtStart) =>
          Effect.gen(function* () {
            const generation = yield* beginPass(observed);

            if (generation._tag === "Actionable" && activeAtStart === 0) {
              // The gate excludes a producer starting between the snapshot and certification.
              yield* publication.prepareGeneration(generation.generation);
            }

            return { ...generation, activeAtStart };
          }),
        );
      });

      const advance = Effect.fn("ThreadMaintenance.advance")(function* (
        started: Effect.Success<ReturnType<typeof beginNative>>,
        yieldAfter: DateTime.Utc,
        observed: MaintenanceObservation,
      ): Effect.fn.Return<NativePassResult, MaintenancePassFailure> {
        const deadline = yield* publication.pendingDeadline;

        if (
          started._tag === "Actionable" ||
          (Option.isSome(deadline) && deadline.value <= (yield* Clock.currentTimeMillis))
        ) {
          yield* publication.drain;
        }
        const pending = yield* publication.pendingDeadline;

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
            recovered: 0,
            settled: 0,
            nonterminal: started.nonterminal,
            nextAttemptAt,
          };
        }
        // Step 2 — reconciliation strictly precedes new work in this pass (exit gate).
        observed.nativeOnly = true;
        const pendingRecovery = yield* Stream.runCollect(ledger.scanNonterminal);
        const recoveryTime = yield* Clock.currentTimeMillis;

        const deferredFaults = yield* runTransaction("read Thread recovery deadlines", async () => {
          const faults = new Map<ThreadId, ThreadRecoveryFault>();

          for (const threadId of new Set(pendingRecovery.map((row) => row.threadId))) {
            const encoded = await ctx.storage.get(recoveryFaultKey(threadId));

            if (encoded === undefined) continue;
            const fault = decodeRecoveryFault(threadId, encoded);

            if (fault.retryAt > recoveryTime) faults.set(threadId, fault);
          }

          return faults;
        });

        const recovered: ReadonlyArray<RecoveryReport> = yield* runtime.recoverThreads({
          excludeThreads: new Set(deferredFaults.keys()),
        });

        // Commit visibility before any new claim or fallible host work. No journal read is
        // needed to inspect this fault after eviction, even when no Attempt ever started.
        const recoveryFaults = new Map([
          ...deferredFaults,
          ...(yield* recordRecoveryStatus(recovered)),
        ]);

        const reports = new Map(recovered.map((report) => [report.submissionId, report]));

        const waiting = (row: SubmissionSnapshot) =>
          !recoveryFaults.has(row.threadId) && stableExternalWait(row, reports);

        const current = yield* Stream.runCollect(ledger.scanNonterminal);
        const heads = new Map<ThreadId, SubmissionSnapshot>();

        for (const row of current) {
          // Parked uncertainty keeps its settlement obligation, but later input can run.
          // Accepted aborts and every other wait remain subject to the lane's FIFO barrier.
          if (row.state === "unknown" && waiting(row)) continue;
          if (!heads.has(row.threadId)) heads.set(row.threadId, row);
        }

        const eligible = [...heads.values()]
          .filter((head) => !recoveryFaults.has(head.threadId) && !waiting(head))
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

        const selected =
          selection.selected === undefined ? undefined : heads.get(selection.selected);

        let retries = selection.retries;
        let bindingFailure: DurableBindingFailure | undefined;

        // One runnable FIFO head per native opportunity. An absent agent waits for a deployment,
        // including for children; other local lanes and host deliveries remain independently due.
        const settlement =
          selected === undefined
            ? Option.none()
            : yield* runtime.processThreadHead(selected.threadId, { yieldAfter }).pipe(
                Effect.catchTag("BindingUnavailable", (failure) => {
                  bindingFailure = failure;

                  return Effect.succeed(Option.none());
                }),
              );

        if (selected !== undefined) {
          const previous = retries.find((retry) => retry.submissionId === selected.submissionId);
          let retry: BindingRetry | undefined;
          let reportBindingFailure = false;

          if (bindingFailure !== undefined) {
            const now = yield* Clock.currentTimeMillis;
            const attempts = Math.min(30, (previous?.attempts ?? 0) + 1);

            reportBindingFailure =
              previous === undefined || now - previous.reportedAt >= 15 * 60_000;
            retry = BindingRetry.make({
              threadId: selected.threadId,
              submissionId: selected.submissionId,
              attempts,
              notBefore: now + Math.min(60_000, 5_000 * 2 ** (attempts - 1)),
              reportedAt: reportBindingFailure ? now : (previous?.reportedAt ?? now),
            });
          }
          if (retry !== undefined || previous !== undefined) {
            // The Attempt released its Claim. Commit its binding wait (or clear) once,
            // before joining fallible auxiliary work. This local fact neither acknowledges
            // a generation nor changes the shared alarm.
            yield* failpoint.hit("maintenance:binding-retry:before");
            retries = yield* runTransaction("record submission binding retry", () =>
              ctx.storage.transaction(async (transaction) => {
                const { state } = await readMaintenanceState(transaction);

                const bindingRetries = [
                  ...(state.bindingRetries ?? []).filter(
                    (entry) => entry.submissionId !== selected.submissionId,
                  ),
                  ...(retry === undefined ? [] : [retry]),
                ];

                await transaction.put(
                  MAINTENANCE_STATE_KEY,
                  encodeMaintenanceState(ThreadMaintenanceState.make({ ...state, bindingRetries })),
                );

                return bindingRetries;
              }),
            );
            yield* failpoint.hit("maintenance:binding-retry:after");
          }
          if (bindingFailure !== undefined) {
            yield* reportBindingFailure
              ? Effect.logError(
                  "Thread awaits a current agent binding; original work remains pending",
                  Cause.fail(bindingFailure),
                )
              : Effect.logDebug("Thread binding retry remains pending", Cause.fail(bindingFailure));
          }
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

        const progressed =
          Option.isSome(settlement) ||
          recovered.some((report) => report.disposition === "repaired");

        const now = yield* Clock.currentTimeMillis;
        const ordinaryDelay = autonomous ? yield* rearmDelay(progressed, started.stalls) : 0;

        const nextEligible = [...recoveryFaults.values()]
          .map((fault) => fault.retryAt)
          .concat(
            eligible.map(
              (threadId) =>
                retries.find((retry) => retry.submissionId === heads.get(threadId)?.submissionId)
                  ?.notBefore ?? now,
            ),
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
                bindingRetries: (state.bindingRetries ?? []).filter((retry) =>
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
                return started.activeAtStart > 0 || active > 0 || state.dirty !== started.generation
                  ? now + minimumAlarmDelay
                  : now + delay;
              }

              return started.activeAtStart > 0 || active > 0 || next.dirty > next.processed
                ? now + minimumAlarmDelay
                : undefined;
            }),
          ),
        );

        yield* failpoint.hit("maintenance:checkpoint:after");

        return {
          phase: "actionable",
          recovered: recovered.length,
          settled: Option.isSome(settlement) ? 1 : 0,
          nonterminal: remaining.length,
          nextAttemptAt,
        };
      });

      const pass = Effect.fn("ThreadMaintenance.pass")(function* (
        yieldAfter: DateTime.Utc,
        dispatchUntil: DateTime.Utc,
        observed: MaintenanceObservation,
      ): Effect.fn.Return<MaintenancePassReport, MaintenancePassFailure, Scope.Scope> {
        // Subscribe before the first native snapshot. Hints accelerate rechecks; the
        // bounded scan and durable generation still recover dropped notifications.
        const notified = (yield* Stream.toPull(wakes.wakes)).pipe(
          Effect.asVoid,
          Effect.catch(() => Effect.never),
        );

        let started = yield* beginNative(observed);

        // This scope owns auxiliary dispatch and listeners, independently of native progress.
        // Close it before final alarm rearming, including on failure or event interruption.
        const auxiliaryScope = yield* Effect.acquireRelease(Scope.make("parallel"), (scope, exit) =>
          Scope.close(scope, exit),
        );

        const dispatchClosed = yield* Deferred.make<void>();
        const stopDispatch = Deferred.await(dispatchClosed);

        // Fork setup too: an ordinary auxiliary setup failure is reported after native work,
        // rather than gating its opportunity. Event interruption still closes every fiber.
        const deliveryFiber = yield* Effect.forkIn(
          Scope.provide(auxiliaryScope)(messages.drainUntil(stopDispatch, dispatchUntil)),
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
              yield* host.drainUntil(stopDispatch, dispatchUntil);
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

        let result = yield* advance(started, yieldAfter, observed);
        let phase = result.phase;
        let recovered = result.recovered;
        let settled = result.settled;

        observed.nativeOnly = false;

        // Close admission of new delivery waves once, then keep advancing native
        // work while the already-admitted waves finish. Neither lane restarts the
        // other's work or receives a fresh event budget.
        yield* Deferred.succeed(dispatchClosed, undefined);

        const remaining = Math.max(
          1,
          DateTime.toEpochMillis(dispatchUntil) - (yield* Clock.currentTimeMillis),
        );

        const hostJoin = yield* Effect.forkIn(
          Fiber.join(hostFiber).pipe(
            Effect.timeoutOption(Math.min(host.dispatchTimeoutMillis, remaining)),
            Effect.tap((outcome) =>
              Effect.annotateCurrentSpan({ "host.timedOut": Option.isNone(outcome) }),
            ),
          ),
          auxiliaryScope,
        );

        const retired = yield* Effect.forkChild(Fiber.joinAll([deliveryFiber, hostJoin, backfill]));

        const auxiliaryPending = () =>
          deliveryFiber.pollUnsafe() === undefined ||
          hostFiber.pollUnsafe() === undefined ||
          backfill.pollUnsafe() === undefined;

        while (retired.pollUnsafe() === undefined && auxiliaryPending()) {
          const now = yield* Clock.currentTimeMillis;
          const until = DateTime.toEpochMillis(yieldAfter);

          if (now >= until) break;

          const next = Math.min(
            result.nextAttemptAt ?? Infinity,
            now + config.wakeScanInterval,
            until,
          );

          const ready = yield* Effect.raceFirst(
            Effect.raceFirst(notified, Effect.sleep(Math.max(0, next - now))).pipe(Effect.as(true)),
            Fiber.await(retired).pipe(Effect.as(false)),
          );

          if (!ready || retired.pollUnsafe() !== undefined || !auxiliaryPending()) break;
          if ((yield* Clock.currentTimeMillis) >= until) break;

          started = yield* beginNative(observed);
          result = yield* advance(started, yieldAfter, observed);
          if (result.phase === "actionable") phase = "actionable";
          recovered += result.recovered;
          settled += result.settled;
          observed.nativeOnly = false;
        }
        // Preserve driver-owned Claim deadlines and failures, then close every
        // listener before the one final alarm decision.
        yield* Fiber.join(retired);
        yield* Scope.close(auxiliaryScope, Exit.void);
        yield* failpoint.hit("maintenance:finish:before");

        const disposition = yield* mutations.withSnapshot((active) =>
          Effect.gen(function* () {
            const latest = yield* pendingDeadline;
            const now = yield* Clock.currentTimeMillis;

            return yield* runTransaction("finish maintenance event", () =>
              ctx.storage.transaction(async (transaction) => {
                const { state } = await readMaintenanceState(transaction);

                const native =
                  active > 0 || state.dirty > state.processed
                    ? state.retry?.generation === state.dirty && active === 0
                      ? Math.max(now + minimumAlarmDelay, state.retry.notBefore)
                      : state.dirty === observed.generation && active === 0
                        ? (result.nextAttemptAt ?? now + config.wakeScanInterval)
                        : now + minimumAlarmDelay
                    : Infinity;

                const next = Option.isSome(latest) ? Math.min(native, latest.value) : native;

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
        recoveryStatus,
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

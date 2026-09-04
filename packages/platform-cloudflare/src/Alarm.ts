import { type DurableBindingFailure } from "@effect-agent/thread/AgentRegistration";
import {
  DurableAgentRuntime,
  type DurableWorkerFailure,
  type RecoveryReport,
} from "@effect-agent/thread/DurableAgentRuntime";
import { SubmissionLedger, type SubmissionSnapshot } from "@effect-agent/thread/SubmissionLedger";
import {
  Clock,
  Context,
  DateTime,
  Effect,
  Layer,
  Option,
  Random,
  Ref,
  Schema,
  Semaphore,
  Stream,
} from "effect";

import { ThreadObjectIdentity, DurableObjectContext } from "./CloudflareBindings.ts";
import { CloudflareDurableRuntimeConfig } from "./CloudflareConfig.ts";
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
  static readonly layer: Layer.Layer<DurableAlarmService, never, DurableObjectContext> =
    Layer.effect(DurableAlarmService)(
      Effect.gen(function* () {
        const { ctx } = yield* DurableObjectContext;
        /**
         * In-memory pass bookkeeping — a pure CACHE, never state: a fresh incarnation has no
         * running pass, and a deferred wake lost to eviction was only ever a promptness hint
         * on top of the already-committed pre-armed alarm.
         */
        const runningPasses = yield* Ref.make(0);

        const scheduled = Effect.tryPromise({
          try: () => ctx.storage.getAlarm(),
          catch: alarmFailure("get alarm"),
        }).pipe(
          Effect.map((deadline) =>
            deadline === null ? Option.none<number>() : Option.some(deadline),
          ),
        );

        const scheduleAt = (epochMillis: number) =>
          Effect.tryPromise({
            try: () => ctx.storage.setAlarm(epochMillis),
            catch: alarmFailure("set alarm"),
          });

        const ensureScheduledBy = (epochMillis: number) =>
          scheduled.pipe(
            Effect.flatMap((existing) =>
              Option.isSome(existing) && existing.value <= epochMillis
                ? Effect.void
                : scheduleAt(epochMillis),
            ),
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

        const cancel = Effect.tryPromise({
          try: () => ctx.storage.deleteAlarm(),
          catch: alarmFailure("delete alarm"),
        });

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
  /** `caught-up` is generation-only; `actionable` ran recovery and at most one head Attempt. */
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

const MaintenanceGeneration = Schema.BigIntFromString.check(
  Schema.isGreaterThanOrEqualToBigInt(0n),
);

/** Versioned, platform-private maintenance state stored through Durable Object KV. */
class ThreadMaintenanceState extends Schema.Class<ThreadMaintenanceState>(
  "@effect-agent/platform-cloudflare/ThreadMaintenanceState",
)({
  schemaVersion: Schema.Literal(1),
  dirty: MaintenanceGeneration,
  processed: MaintenanceGeneration,
  nonterminal: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
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

export type MaintenancePassFailure =
  | DurableWorkerFailure
  | DurableBindingFailure
  | DurableAlarmError;

/**
 * Incremental, quiescent maintenance over a durable dirty/processed generation (issue #93).
 *
 * `pass` = generation snapshot/pre-arm → recovery → one head Attempt → generation acknowledgement:
 *
 * 1. One storage transaction reads dirty/processed and re-arms before work. A caught-up forced
 *    alarm takes an O(1) path without recovery, ledger scans, or canonical-history reads.
 * 2. Recovery strictly precedes a new claim. One head Attempt advances the lane and requests
 *    a safe yield after ten minutes. The whole event has a fourteen-minute cooperative timeout.
 * 3. The final transaction acknowledges only the generation observed at pass start. A racing
 *    mutation therefore remains `dirty > processed` and retains its atomically-established alarm.
 * 4. Stable external waits acknowledge and clear. Autonomous retry, indeterminate, and lease
 *    recovery states leave their generation dirty and retain bounded backoff rearming.
 */
export class ThreadMaintenance extends Context.Service<
  ThreadMaintenance,
  {
    /** One idempotent maintenance pass; failures propagate so workerd retries the alarm. */
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
    | DurableAgentRuntime
    | SubmissionLedger
    | DurableAlarmService
    | ThreadMaintenanceFailpoint
    | CloudflareDurableRuntimeConfig
    | ThreadObjectIdentity
    | DurableObjectContext
  > = Layer.effect(ThreadMaintenance)(
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const ledger = yield* SubmissionLedger;
      const alarm = yield* DurableAlarmService;
      const config = yield* CloudflareDurableRuntimeConfig;
      const identity = yield* ThreadObjectIdentity;
      const { ctx } = yield* DurableObjectContext;
      const failpoint = yield* ThreadMaintenanceFailpoint;

      /**
       * Consecutive no-progress passes — an in-memory CACHE, not state: a fresh incarnation
       * restarts at zero and merely re-arms sooner than a long-lived one would have.
       */
      const stalls = yield* Ref.make(0);
      /**
       * Incarnation-local mutation count guarded with the generation transactions below. It is
       * deliberately not durable: after eviction every begun mutation has stopped, while its
       * pre-armed dirty generation remains durable for recovery. The short gate never spans the
       * caller's mutation or cross-Object I/O.
       */
      const activeMutations = yield* Ref.make(0);
      const generationGate = yield* Semaphore.make(1);
      // At-least-once deliveries are idempotent, but overlapping pass bodies could otherwise
      // acknowledge state while a sibling pass is still mutating it. Port/RPC mutations do not
      // take this permit, so cross-Object I/O cannot deadlock the maintenance serialization.
      const maintenancePassGate = yield* Semaphore.make(1);
      const minimumAlarmDelay = Math.max(1, Math.ceil(config.alarmBackoffBase / 2));

      const runTransaction = <A>(
        operation: string,
        transaction: () => Promise<A>,
      ): Effect.Effect<A, DurableAlarmError> =>
        Effect.tryPromise({
          try: transaction,
          catch: alarmFailure(operation),
        });

      const beginMutation = Effect.fn("ThreadMaintenance.beginMutation")(function* () {
        yield* failpoint.hit("maintenance:dirty:before");
        const now = yield* Clock.currentTimeMillis;

        yield* runTransaction("advance maintenance generation", () =>
          ctx.storage.transaction(async (transaction) => {
            const { state } = await readMaintenanceState(transaction);

            const next = ThreadMaintenanceState.make({
              ...state,
              dirty: state.dirty + 1n,
            });

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
      ): Effect.Effect<A, E | DurableAlarmError, R> =>
        Effect.acquireUseRelease(
          generationGate.withPermit(beginMutation()),
          () =>
            failpoint.hit("maintenance:mutation:armed").pipe(
              Effect.andThen(body),
              Effect.tap(() => failpoint.hit("maintenance:mutation:finished")),
            ),
          () => endMutation,
        );

      const ensureAlarm = Effect.fn("ThreadMaintenance.ensureAlarm")(function* () {
        yield* failpoint.hit("maintenance:ensure:before");
        const now = yield* Clock.currentTimeMillis;

        yield* runTransaction("ensure maintenance alarm", () =>
          ctx.storage.transaction(async (transaction) => {
            const { state, initialized } = await readMaintenanceState(transaction);

            if (!initialized) {
              await transaction.put(MAINTENANCE_STATE_KEY, encodeMaintenanceState(state));
            }
            if (state.dirty > state.processed) {
              await ensureTransactionAlarmBy(transaction, now + config.wakeScanInterval);
            }
          }),
        );
        yield* failpoint.hit("maintenance:ensure:after");
      });

      const beginPass = Effect.fn("ThreadMaintenance.beginPass")(function* () {
        yield* failpoint.hit("maintenance:begin:before");
        const now = yield* Clock.currentTimeMillis;

        const result = yield* runTransaction("begin maintenance pass", () =>
          ctx.storage.transaction(async (transaction) => {
            const { state, initialized } = await readMaintenanceState(transaction);

            if (!initialized) {
              await transaction.put(MAINTENANCE_STATE_KEY, encodeMaintenanceState(state));
            }
            if (state.processed >= state.dirty) {
              await transaction.deleteAlarm();

              return { _tag: "CaughtUp" as const, nonterminal: state.nonterminal };
            }
            // Pre-arm the earliest retry before recovery. A successful finish may move this slot
            // LATER to its bounded backoff, which does not cancel the running handler.
            await ensureTransactionAlarmBy(transaction, now + minimumAlarmDelay);

            return { _tag: "Actionable" as const, generation: state.dirty };
          }),
        );

        yield* failpoint.hit("maintenance:begin:after");

        return result;
      });

      const rearmDelay = Effect.fn("ThreadMaintenance.rearmDelay")(function* (progressed: boolean) {
        const priorStalls = yield* Ref.getAndUpdate(stalls, (count) =>
          progressed ? 0 : count + 1,
        );

        if (progressed) return config.alarmBackoffBase;
        const exponent = Math.min(priorStalls, 30);
        const backoff = Math.min(config.alarmBackoffCap, config.alarmBackoffBase * 2 ** exponent);
        const jitter = yield* Random.next;
        // Full jitter over [backoff/2, backoff]: desynchronizes retry storms without ever
        // waiting longer than the deterministic bound.
        const jittered = Math.ceil(backoff / 2 + (backoff / 2) * jitter);

        return Math.min(jittered, config.wakeScanInterval);
      });

      const pass = Effect.fn("ThreadMaintenance.pass")(function* (
        yieldAfter: DateTime.Utc,
      ): Effect.fn.Return<MaintenancePassReport, MaintenancePassFailure> {
        const annotate = (report: MaintenancePassReport) =>
          Effect.annotateCurrentSpan({
            phase: report.phase,
            recovered: report.recovered,
            settled: report.settled,
            nonterminal: report.nonterminal,
            alarm: report.alarm,
          }).pipe(Effect.as(report));

        const started = yield* generationGate.withPermit(
          Effect.gen(function* () {
            const activeAtStart = yield* Ref.get(activeMutations);
            const generation = yield* beginPass();

            return { ...generation, activeAtStart };
          }),
        );

        if (started._tag === "CaughtUp") {
          return yield* annotate(
            MaintenancePassReport.make({
              phase: "caught-up",
              recovered: 0,
              settled: 0,
              nonterminal: started.nonterminal,
              alarm: "cleared",
            }),
          );
        }
        // Step 2 — reconciliation strictly precedes new work in this pass (exit gate).
        const recovered: ReadonlyArray<RecoveryReport> = yield* runtime.runRecovery;
        // One head Attempt per event. The runtime yields after a committed turn when the
        // soft deadline is reached; queued followers belong to a subsequent alarm.
        const settlement = yield* runtime.processThreadHead(identity.threadId, { yieldAfter });
        // Observe residual state before acknowledging this exact pass-start generation.
        const remaining = yield* Stream.runCollect(ledger.scanNonterminal);
        const reports = new Map(recovered.map((report) => [report.submissionId, report]));
        const head = remaining[0];
        const headWaiting = head !== undefined && stableExternalWait(head, reports);

        const autonomous = remaining.some((snapshot, index) => {
          // FIFO followers cannot execute through a stable external wait. Only plain queued
          // input is dormant here; admission repairs and accepted aborts still need a pass.
          if (
            index > 0 &&
            headWaiting &&
            snapshot.state === "ready" &&
            reports.get(snapshot.submissionId)?.decision._tag === "ApplyInput"
          )
            return false;

          return !stableExternalWait(snapshot, reports);
        });

        const progressed =
          Option.isSome(settlement) ||
          recovered.some((report) => report.disposition === "repaired");

        const delay = autonomous ? yield* rearmDelay(progressed) : 0;
        const now = yield* Clock.currentTimeMillis;

        yield* failpoint.hit("maintenance:finish:before");

        const alarmDisposition = yield* generationGate.withPermit(
          Effect.gen(function* () {
            const active = yield* Ref.get(activeMutations);

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
                  ...state,
                  processed,
                  nonterminal: remaining.length,
                });

                await transaction.put(MAINTENANCE_STATE_KEY, encodeMaintenanceState(next));
                if (autonomous) {
                  // Replace the crash-fallback slot with this pass's bounded backoff. The target
                  // is never earlier than the begin-pass fallback, so workerd does not cancel
                  // this running alarm handler before its report/span can complete.
                  await transaction.setAlarm(now + delay);

                  return "rearmed" as const;
                }
                if (started.activeAtStart > 0 || active > 0 || next.dirty > next.processed) {
                  // A mutation overlapped this pass's observation window or raced
                  // acknowledgement. It stays dirty and its pre-armed bounded alarm survives;
                  // unseen effects are never acknowledged. Do not accelerate that future alarm
                  // from inside the current handler: workerd cancels a running handler when it
                  // writes an earlier slot.
                  await ensureTransactionAlarmBy(transaction, now + config.wakeScanInterval);

                  return "rearmed" as const;
                }
                await transaction.deleteAlarm();

                return "cleared" as const;
              }),
            );
          }),
        );

        yield* failpoint.hit("maintenance:finish:after");
        if (alarmDisposition === "cleared") {
          yield* Ref.set(stalls, 0);
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
          const yieldAfter = DateTime.makeUnsafe((yield* Clock.currentTimeMillis) + 10 * 60_000);

          return yield* alarm.withWakesDeferred(maintenancePassGate.withPermit(pass(yieldAfter)));
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
        ensureAlarm: ensureAlarm(),
        withMutation,
      });
    }),
  );
}

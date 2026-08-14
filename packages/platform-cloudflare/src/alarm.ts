import {
  AgentBindingResolver,
  DurableAgentRuntime,
  SubmissionLedger,
  type DurableBindingFailure,
  type DurableWorkerFailure,
  type RecoveryReport,
} from "@effect-agent/session";
import { Clock, Context, Effect, Layer, Option, Random, Ref, Schema, Stream } from "effect";

import { ConversationObjectIdentity, DurableObjectContext } from "./bindings.ts";
import { CloudflareDurableRuntimeConfig } from "./config.ts";

/**
 * The single multiplexed Durable Object alarm (decision D-P6-2). A Durable Object has ONE
 * alarm slot; every cadence the Node host ran on fibers (wake scan, lease expiry, settlement
 * and abort re-checks, retry backoff) multiplexes into one idempotent maintenance pass, and
 * the slot always holds the EARLIEST deadline any caller asked for.
 *
 * The alarm invariant (plan §1.4): committed nonterminal work implies a committed alarm.
 * It is established by pre-arming — every mutating entry point and every pass arms the alarm
 * BEFORE its durable mutations — so an eviction at any failpoint leaves a persisted alarm
 * that workerd re-delivers to a fresh incarnation WITHOUT any incoming request. Spurious
 * alarms are harmless by design: a pass over an all-settled lane simply deletes the slot.
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
      message: cause instanceof Error ? cause.message : String(cause),
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
     * call), and the deferred wake is flushed when the pass completes.
     */
    readonly scheduleNow: Effect.Effect<void, DurableAlarmError>;
    /**
     * Run one maintenance pass with wake deferral (see `scheduleNow`): `scheduleNow` calls
     * while `body` executes coalesce into one flag, flushed as an immediate re-arm after the
     * pass — failures of the flush are logged and swallowed (hints are droppable; the pass's
     * own re-arm policy already bounded the next delivery).
     */
    readonly withWakesDeferred: <A, E, R>(body: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>;
    /** Clear the slot; only the maintenance pass does this, and only when all work settled. */
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
        const wakeDeferred = yield* Ref.make(false);
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
          Effect.flatMap((passes) => (passes > 0 ? Ref.set(wakeDeferred, true) : armNow)),
        );
        /**
         * Flush after the LAST concurrent pass: the re-arm lands at the very end of the alarm
         * handler, where a superseding cancellation only re-delivers to the already-idempotent
         * pass. Runs inside `Effect.ensuring`, so flush failures are logged, never raised.
         */
        const flushDeferredWake = Ref.get(runningPasses).pipe(
          Effect.flatMap((passes) =>
            passes > 0
              ? Effect.void
              : Ref.getAndSet(wakeDeferred, false).pipe(
                  Effect.flatMap((wanted) => (wanted ? armNow : Effect.void)),
                ),
          ),
          Effect.catch((error) =>
            Effect.logWarning("DurableAlarmService: deferred wake flush failed", error),
          ),
        );
        const withWakesDeferred = <A, E, R>(body: Effect.Effect<A, E, R>): Effect.Effect<A, E, R> =>
          Ref.update(runningPasses, (passes) => passes + 1).pipe(
            Effect.andThen(body),
            Effect.ensuring(
              Ref.update(runningPasses, (passes) => passes - 1).pipe(
                Effect.andThen(flushDeferredWake),
              ),
            ),
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
  /** Recovery decisions executed (or deferred) BEFORE any new claim in this pass. */
  recovered: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  /** Settlements the drain pass finalized. */
  settled: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  /** Submissions still nonterminal after the pass (suspended/unknown lanes stay honest). */
  nonterminal: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  /** `rearmed` while nonterminal work remains, `cleared` once everything settled. */
  alarm: Schema.Literals(["rearmed", "cleared"]),
}) {}

export type MaintenancePassFailure =
  | DurableWorkerFailure
  | DurableBindingFailure
  | DurableAlarmError;

/**
 * The idempotent maintenance pass and the alarm-invariant helpers (plan §1.4, D-P6-1/2).
 *
 * `pass` = pre-arm → `runRecovery` → `processConversationResolved` → re-arm-or-clear:
 *
 * 1. **Pre-arm**: the alarm is re-armed at `now + wakeScanInterval` BEFORE any work, so an
 *    eviction (or thrown failure → workerd alarm retry) at any point of the pass leaves a
 *    committed alarm and the fresh incarnation converges without an incoming request.
 * 2. **Reconcile before new work** (exit gate): every pass classifies and repairs every
 *    nonterminal Submission before the drain claims anything; the pure classifier and the
 *    repair executors are idempotent, so at-least-once alarm delivery re-runs them safely.
 * 3. **Drain**: one bounded `processConversationResolved` pass over this Object's lane — the
 *    D-P6-1 shape; no infinite `runResolvedWorker` loop ever pins the Object.
 * 4. **Re-arm policy**: nonterminal work re-arms at `now + min(backoff-with-jitter,
 *    wakeScanInterval)` — the scan interval bounds every wait (lease expiry of a dead
 *    incarnation included, since claims retry each pass) and the backoff (reset on progress,
 *    grown otherwise) keeps stuck lanes from busy-spinning; all settled clears the slot.
 *
 * Every step is a durability-protocol step that already tolerates re-execution, which is
 * what makes double-fired alarms harmless (the alarm.test.ts gate).
 */
export class ConversationMaintenance extends Context.Service<
  ConversationMaintenance,
  {
    /** One idempotent maintenance pass; failures propagate so workerd retries the alarm. */
    readonly pass: Effect.Effect<MaintenancePassReport, MaintenancePassFailure>;
    /**
     * Defensive half of the alarm invariant, run by the constructor gate: if any local
     * Submission is nonterminal and no alarm is scheduled, arm one within the scan interval.
     * Local-only (one ledger scan + the alarm slot) — never a recovery pass, never transport.
     */
    readonly ensureAlarm: Effect.Effect<void, MaintenancePassFailure>;
    /**
     * The pre-arm every mutating entry point runs BEFORE its first durable mutation: with
     * the alarm committed first, a committed admission (or any other committed nonterminal
     * transition) can never be observed without the alarm that will finish it.
     */
    readonly preArm: Effect.Effect<void, DurableAlarmError>;
  }
>()("@effect-agent/platform-cloudflare/ConversationMaintenance") {
  static readonly layer: Layer.Layer<
    ConversationMaintenance,
    never,
    | DurableAgentRuntime
    | AgentBindingResolver
    | SubmissionLedger
    | DurableAlarmService
    | CloudflareDurableRuntimeConfig
    | ConversationObjectIdentity
  > = Layer.effect(ConversationMaintenance)(
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const resolver = yield* AgentBindingResolver;
      const ledger = yield* SubmissionLedger;
      const alarm = yield* DurableAlarmService;
      const config = yield* CloudflareDurableRuntimeConfig;
      const identity = yield* ConversationObjectIdentity;

      /**
       * Consecutive no-progress passes — an in-memory CACHE, not state: a fresh incarnation
       * restarts at zero and merely re-arms sooner than a long-lived one would have.
       */
      const stalls = yield* Ref.make(0);

      const preArm = Clock.currentTimeMillis.pipe(
        Effect.flatMap((now) => alarm.ensureScheduledBy(now + config.wakeScanInterval)),
      );

      const countNonterminal = Stream.runCollect(ledger.scanNonterminal).pipe(
        Effect.map((snapshots) => snapshots.length),
      );

      const rearmDelay = Effect.fn("ConversationMaintenance.rearmDelay")(function* (
        progressed: boolean,
      ) {
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

      const pass = Effect.fn("ConversationMaintenance.pass")(function* (): Effect.fn.Return<
        MaintenancePassReport,
        MaintenancePassFailure
      > {
        // Step 1 — pre-arm: an abort or throw anywhere below leaves a committed alarm.
        yield* preArm;
        // Step 2 — reconciliation strictly precedes new work in this pass (exit gate).
        const recovered: ReadonlyArray<RecoveryReport> = yield* runtime.runRecovery;
        // Step 3 — one bounded drain pass over this Object's own lane.
        const settlements = yield* runtime
          .processConversationResolved(identity.conversationId)
          .pipe(Effect.provideService(AgentBindingResolver, resolver));
        // Step 4 — re-arm or clear.
        const nonterminal = yield* countNonterminal;
        if (nonterminal === 0) {
          yield* alarm.cancel;
          yield* Ref.set(stalls, 0);
          // Close the cancel/admission interleaving window: a submission committing between
          // the count and the cancel (Durable Object events interleave at storage-operation
          // boundaries) must not be left without its alarm. The recount re-arms if anything
          // appeared; the submit entry's own `scheduleNow` covers commits after the recount.
          const appeared = yield* countNonterminal;
          if (appeared > 0) {
            yield* preArm;
            return MaintenancePassReport.make({
              recovered: recovered.length,
              settled: settlements.length,
              nonterminal: appeared,
              alarm: "rearmed",
            });
          }
          return MaintenancePassReport.make({
            recovered: recovered.length,
            settled: settlements.length,
            nonterminal,
            alarm: "cleared",
          });
        }
        const progressed =
          settlements.length > 0 || recovered.some((report) => report.disposition === "repaired");
        const delay = yield* rearmDelay(progressed);
        const now = yield* Clock.currentTimeMillis;
        yield* alarm.ensureScheduledBy(now + delay);
        return MaintenancePassReport.make({
          recovered: recovered.length,
          settled: settlements.length,
          nonterminal,
          alarm: "rearmed",
        });
      });

      const ensureAlarm = Effect.fn("ConversationMaintenance.ensureAlarm")(function* () {
        const nonterminal = yield* countNonterminal;
        if (nonterminal === 0) return;
        yield* preArm;
      });

      return ConversationMaintenance.of({
        // Wake deferral (see `DurableAlarmService.scheduleNow`): an immediate wake written
        // while THIS handler runs would make workerd cancel it mid-Attempt; deferring keeps
        // the running pass alive and flushes the hint as the pass's final re-arm.
        pass: alarm.withWakesDeferred(pass()),
        ensureAlarm: ensureAlarm(),
        preArm,
      });
    }),
  );
}

import { type ThreadId } from "@effect-agent/core/Identifiers";
import { SubmissionLedger } from "@effect-agent/thread/SubmissionLedger";
import { makeWakeSubscriptionHub, WakeScheduler } from "@effect-agent/thread/WakeScheduler";
import type { Duration } from "effect";
import { Context, Effect, Layer, PubSub, Stream } from "effect";

/**
 * Bounded in-process wake buffer. Wake hints are droppable by contract (the ledger-scan fallback
 * keeps liveness), so a full buffer slides out the oldest hint instead of growing without bound.
 */
const WAKE_BUFFER_CAPACITY = 1_024;

/** Cadence authority for the Node wake scheduler's ledger-scan fallback loop. */
export class NodeWakeSchedulerConfig extends Context.Service<
  NodeWakeSchedulerConfig,
  {
    /** Interval between ledger scans that re-emit every nonterminal Thread lane. */
    readonly scanInterval: Duration.Duration;
  }
>()("@effect-agent/platform-node/NodeWakeSchedulerConfig") {
  static layer(options: {
    readonly scanInterval: Duration.Duration;
  }): Layer.Layer<NodeWakeSchedulerConfig> {
    return Layer.succeed(NodeWakeSchedulerConfig)({ scanInterval: options.scanInterval });
  }
}

const makeWakeScheduler = Effect.gen(function* () {
  const ledger = yield* SubmissionLedger;
  const config = yield* NodeWakeSchedulerConfig;
  const hints = yield* PubSub.sliding<ThreadId>(WAKE_BUFFER_CAPACITY);
  const progress = yield* makeWakeSubscriptionHub;

  yield* Effect.addFinalizer(() => PubSub.shutdown(hints));

  /**
   * One fallback scan: every Thread lane with nonterminal work, deduplicated. A scan
   * failure degrades to "no hints this round" — the wake channel has no error contract and the
   * next round retries — but is logged so a persistently failing ledger stays visible.
   */
  const scanOnce: Effect.Effect<ReadonlyArray<ThreadId>> = Stream.runCollect(
    ledger.scanNonterminal,
  ).pipe(
    Effect.map((snapshots) => {
      const lanes = new Set<ThreadId>();

      for (const snapshot of snapshots) {
        lanes.add(snapshot.threadId);
      }

      return [...lanes];
    }),
    Effect.catch((error) =>
      Effect.logWarning("NodeWakeScheduler fallback scan failed", error).pipe(
        Effect.as([] as ReadonlyArray<ThreadId>),
      ),
    ),
  );

  /**
   * Deployment §3: correctness must not depend on in-memory notifications, so every `wakes` run
   * merges its PubSub subscription with shared Clock-driven ledger scans. Share whole snapshots:
   * sliding individual lanes would strand the beginning of scans larger than the hint buffer.
   * The scan starts with the first subscriber and stops when the last subscriber leaves.
   */
  const fallbackScans = yield* Stream.share(
    Stream.fromEffectRepeat(Effect.sleep(config.scanInterval).pipe(Effect.andThen(scanOnce))),
    { capacity: 1, strategy: "sliding" },
  );

  return WakeScheduler.of({
    notify: (threadId) =>
      progress
        .notify(threadId)
        .pipe(Effect.andThen(PubSub.publish(hints, threadId)), Effect.asVoid),
    subscribe: progress.subscribe,
    wakes: Stream.merge(
      Stream.fromPubSub(hints),
      fallbackScans.pipe(Stream.flatMap(Stream.fromIterable)),
    ),
  });
});

/**
 * In-process Node `WakeScheduler`: `notify` publishes to a bounded sliding PubSub for prompt
 * same-process wakeups, and active `wakes` subscriptions share one periodic
 * `SubmissionLedger.scanNonterminal` fallback so a dropped, coalesced, or never-sent notification
 * can never strand accepted work (persistence §14). Delivery may duplicate; consumers already
 * treat wakes as pure liveness hints.
 */
export const nodeWakeSchedulerLayer: Layer.Layer<
  WakeScheduler,
  never,
  SubmissionLedger | NodeWakeSchedulerConfig
> = Layer.effect(WakeScheduler)(makeWakeScheduler);

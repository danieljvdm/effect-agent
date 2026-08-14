import type { ConversationId } from "@effect-agent/core";
import { SubmissionLedger, WakeScheduler } from "@effect-agent/session";
import { Context, Duration, Effect, Layer, PubSub, Stream } from "effect";

/**
 * Bounded in-process wake buffer. Wake hints are droppable by contract (the ledger-scan fallback
 * keeps liveness), so a full buffer slides out the oldest hint instead of growing without bound.
 */
const WAKE_BUFFER_CAPACITY = 1_024;

/** Cadence authority for the Node wake scheduler's ledger-scan fallback loop. */
export class NodeWakeSchedulerConfig extends Context.Service<
  NodeWakeSchedulerConfig,
  {
    /** Interval between ledger scans that re-emit every nonterminal Conversation lane. */
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
  const hints = yield* PubSub.sliding<ConversationId>(WAKE_BUFFER_CAPACITY);
  yield* Effect.addFinalizer(() => PubSub.shutdown(hints));

  /**
   * One fallback scan: every Conversation lane with nonterminal work, deduplicated. A scan
   * failure degrades to "no hints this round" — the wake channel has no error contract and the
   * next round retries — but is logged so a persistently failing ledger stays visible.
   */
  const scanOnce: Effect.Effect<ReadonlyArray<ConversationId>> = Stream.runCollect(
    ledger.scanNonterminal,
  ).pipe(
    Effect.map((snapshots) => {
      const lanes = new Set<ConversationId>();
      for (const snapshot of snapshots) {
        lanes.add(snapshot.conversationId);
      }
      return [...lanes];
    }),
    Effect.catch((error) =>
      Effect.logWarning("NodeWakeScheduler fallback scan failed", error).pipe(
        Effect.as([] as ReadonlyArray<ConversationId>),
      ),
    ),
  );

  /**
   * Deployment §3: correctness must not depend on in-memory notifications, so every `wakes` run
   * merges its PubSub subscription with a Clock-driven ledger-scan loop. Both live entirely in
   * the consuming run's Scope; no fiber outlives its subscriber.
   */
  const fallbackScans: Stream.Stream<ConversationId> = Stream.fromIterableEffectRepeat(
    Effect.sleep(config.scanInterval).pipe(Effect.andThen(scanOnce)),
  );

  return WakeScheduler.of({
    notify: (conversationId) => PubSub.publish(hints, conversationId).pipe(Effect.asVoid),
    wakes: Stream.merge(Stream.fromPubSub(hints), fallbackScans),
  });
});

/**
 * In-process Node `WakeScheduler`: `notify` publishes to a bounded sliding PubSub for prompt
 * same-process wakeups, and every `wakes` subscription additionally runs a periodic
 * `SubmissionLedger.scanNonterminal` fallback so a dropped, coalesced, or never-sent notification
 * can never strand accepted work (persistence §14). Delivery may duplicate; consumers already
 * treat wakes as pure liveness hints.
 */
export const nodeWakeSchedulerLayer: Layer.Layer<
  WakeScheduler,
  never,
  SubmissionLedger | NodeWakeSchedulerConfig
> = Layer.effect(WakeScheduler)(makeWakeScheduler);

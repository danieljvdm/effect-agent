import { ConversationId } from "@effect-agent/core";
import { Context, Effect, Layer, Stream } from "effect";

/**
 * Liveness hint channel for durable schedulers. `notify` announces that a Conversation lane may
 * have claimable or settled work; `wakes` is the subscription surface for workers and
 * settlement waiters.
 *
 * Correctness must NEVER depend on delivery: notifications may be dropped, coalesced, duplicated,
 * or observed by no subscriber (a notify before any subscription is simply lost). Every consumer
 * must pair a wake subscription with periodic SubmissionLedger scans so a dropped notification
 * cannot cause lost work (persistence §14).
 *
 * Each run of `wakes` creates its own subscription whose resources live in that run's Scope and
 * are released when the consuming stream ends or is interrupted. Implementations must not spawn
 * daemon fibers.
 */
export class WakeScheduler extends Context.Service<
  WakeScheduler,
  {
    /** Best-effort hint that `conversationId` may have claimable or settled work. Never fails. */
    readonly notify: (conversationId: ConversationId) => Effect.Effect<void>;
    /** Scope-owned subscription to wake hints, starting at subscription time. */
    readonly wakes: Stream.Stream<ConversationId>;
  }
>()("@effect-agent/session/WakeScheduler") {
  /**
   * Discards every notification and never wakes anyone. Valid because WakeScheduler is only a
   * liveness hint: consumers relying on ledger scans stay correct, merely slower.
   */
  static readonly layerNoop: Layer.Layer<WakeScheduler> = Layer.succeed(WakeScheduler, {
    notify: () => Effect.void,
    wakes: Stream.never,
  });
}

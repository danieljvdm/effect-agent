import { Effect, Fiber, Layer, Schema, type Scope } from "effect";

import { SubagentReservationsMemoryLive } from "./capabilities/SubagentReservations.ts";
import { Store } from "./core/Thread.ts";
import { ThreadHistory, layer as historyLayer } from "./engine/ThreadHistory.ts";

/**
 * Run agents and attached subagents with in-memory conversation history.
 * Provide once around the parent program and all child handler Layers so siblings
 * share history and one reservation ledger. Reuse a Thread ID to continue a conversation.
 * Conversations can span any number of Runs within the store's capacity limits.
 * Use scoped for disposable request workflows within this shared store.
 * Each independent Layer build owns fresh state; state is released when its Scope closes.
 * Process loss loses history and active execution; this Layer provides no crash recovery.
 *
 * Models, tool handlers, and provider clients remain application-supplied. Default
 * IDs need no Layer; enclosing ID and context-preparation overrides are preserved.
 * For storage-backed history, provide PersistentHistory.layer and a shared
 * SubagentReservationsMemoryLive instead. Durable hosts own their own assembly.
 */
export const layer = Layer.merge(historyLayer, SubagentReservationsMemoryLive);

/** Disposable lifetime requires an in-memory store and its incremental history adapter. */
export class InMemoryScopeError extends Schema.TaggedError<InMemoryScopeError>()(
  "InMemoryScopeError",
  { message: Schema.String },
) {}

/**
 * Own disposable conversations for one complete workflow on the enclosing shared store.
 * Retain history across Runs and attached children inside the workflow, then release its
 * Threads and encoded bytes on every exit. Existing retained or foreign Threads cannot be
 * claimed. Counts and bytes share the application's original limits and reservation ledger.
 *
 * Consume streams, await start handles, and capture any snapshots inside this effect.
 * Returned outputs and snapshots remain usable; returned IDs lose their continuation history.
 * Persistent history adapters and stores without scoped ownership fail with InMemoryScopeError.
 * This wrapper is for ordinary Runs; durable hosts retain their own journal history.
 * Construct store-capturing hooks inside the workflow. The workflow's child fibers finish
 * cleanup before history is released. Default application retention remains unchanged.
 */
export const scoped = <A, E, R>(
  effect: Effect.Effect<A, E, R>,
): Effect.Effect<
  A,
  E | InMemoryScopeError,
  Exclude<Exclude<R, Store | ThreadHistory>, Scope.Scope> | Store | ThreadHistory
> =>
  Effect.scoped(
    Effect.gen(function* () {
      const store = yield* Store;
      const history = yield* ThreadHistory;

      if (
        store.scoped === undefined ||
        history.retention !== "incremental" ||
        history.memoryStore !== store
      ) {
        return yield* InMemoryScopeError.make({
          message: "InMemory.scoped requires a scoped in-memory store and incremental history",
        });
      }
      const owned = yield* store.scoped;

      // The enclosing Layer memo map may already contain the application adapter.
      const services = Layer.fresh(ThreadHistory.layerFromStore).pipe(
        Layer.provideMerge(Layer.succeed(Store, owned)),
      );

      const fiber = yield* effect.pipe(Effect.provide(services), Effect.forkScoped);

      return yield* Fiber.join(fiber);
    }),
  );

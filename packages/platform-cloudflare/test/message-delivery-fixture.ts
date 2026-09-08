import { MessageDeliveryFailpoint } from "@effect-agent/thread/MessageDelivery";
import { Context, Effect, Layer } from "effect";
import { DurableObjectState } from "effect-cf";

/** Harness controls survive Object eviction, like the external fault controller. */
export const messageEvictions = new Map<string, string>();
export const droppedMessageWakes = new Set<string>();

export const messageDeliveryHolds = new Map<
  string,
  {
    readonly point: string;
    readonly entered: () => void;
    readonly release: Promise<void>;
  }
>();

export const messageDeliveryResources = new Map<string, { acquired: number; released: number }>();

export const messageDeliveryFaultLayer = Layer.effectContext(
  Effect.gen(function* () {
    const state = yield* DurableObjectState.DurableObjectState;

    return Context.make(MessageDeliveryFailpoint, {
      hit: (point) =>
        Effect.gen(function* () {
          const thread = state.raw.id.name ?? "";

          if (messageEvictions.get(thread) === point) {
            messageEvictions.delete(thread);
            state.raw.abort("message delivery eviction");
          }

          const hold = messageDeliveryHolds.get(thread);

          if (hold?.point !== point) return;

          const resources = messageDeliveryResources.get(thread) ?? { acquired: 0, released: 0 };

          messageDeliveryResources.set(thread, resources);
          yield* Effect.acquireUseRelease(
            Effect.sync(() => {
              resources.acquired += 1;
              hold.entered();
            }),
            () => Effect.promise(() => hold.release),
            () =>
              Effect.sync(() => {
                resources.released += 1;
              }),
          );
        }),
    });
  }),
);

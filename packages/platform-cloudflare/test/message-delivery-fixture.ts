import { Context, Effect, Layer } from "effect";
import {
  MessageDeliveryDriver,
  MessageDeliveryFailpoint,
  MessageDeliveryStore,
} from "effect-agent/message-delivery";
import { PreparedInputAdmission } from "effect-agent/prepared-input-admission";
import { DurableObjectState } from "effect-cf";

import { ThreadObjectIdentity } from "../src/CloudflareBindings.ts";
import { CloudflareThreadClient } from "../src/CloudflareThreadClient.ts";
import { threadMessageDeliveryLayer } from "../src/internal/message-delivery.ts";
import { cloudflarePreparedInputAdmissionLayer } from "../src/internal/prepared-admission.ts";
import { observeWorkerInputDelivery } from "./helpers/worker-input-contention.ts";

export const messageClaimDelays = new Map<
  string,
  { readonly release: Promise<void>; readonly entered: () => void }
>();

const delayedClaimStore = Layer.effect(
  MessageDeliveryStore,
  Effect.gen(function* () {
    const store = yield* MessageDeliveryStore;
    const { threadId } = yield* ThreadObjectIdentity;

    return MessageDeliveryStore.of({
      ...store,
      get: (key) =>
        store.get(key).pipe(
          Effect.tap(() =>
            Effect.gen(function* () {
              const delay = messageClaimDelays.get(threadId);

              if (delay === undefined) return;
              messageClaimDelays.delete(threadId);
              delay.entered();
              yield* Effect.promise(() => delay.release);
            }),
          ),
        ),
    });
  }),
);

const admissionWithHeldResponse = Layer.effect(
  PreparedInputAdmission,
  Effect.gen(function* () {
    const admission = yield* PreparedInputAdmission;
    const { threadId } = yield* ThreadObjectIdentity;

    return PreparedInputAdmission.of({
      ...admission,
      submit: (input) =>
        admission
          .submit(input)
          .pipe(Effect.tap(() => holdDelivery(threadId, "message-delivery:admission:response"))),
    });
  }),
).pipe(
  Layer.provide(cloudflarePreparedInputAdmissionLayer),
  Layer.provide(CloudflareThreadClient.layer),
);

// Real delivery driver/SQL/admission wiring; only Claim timing and the transport response vary.
export const testMessageDriverLayer = MessageDeliveryDriver.layer({
  batchSize: 4,
  concurrency: 4,
}).pipe(Layer.provide(delayedClaimStore), Layer.provide(admissionWithHeldResponse));

export const testMessageRecovery = Layer.fresh(threadMessageDeliveryLayer).pipe(
  Layer.provide(testMessageDriverLayer),
);

/** Harness controls survive Object eviction, like the external fault controller. */
export const messageEvictions = new Map<string, string>();
export const messageInterruptions = new Set<string>();
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

const holdDelivery = (thread: string, point: string) =>
  Effect.gen(function* () {
    const hold = messageDeliveryHolds.get(thread);

    if (hold?.point !== point) return;
    const resources = messageDeliveryResources.get(thread) ?? { acquired: 0, released: 0 };

    messageDeliveryResources.set(thread, resources);
    yield* Effect.acquireUseRelease(
      Effect.sync(() => {
        resources.acquired++;
        hold.entered();
      }),
      () => Effect.promise(() => hold.release),
      () =>
        Effect.sync(() => {
          resources.released++;
        }),
    );
  });

export const messageDeliveryFaultLayer = Layer.effectContext(
  Effect.gen(function* () {
    const state = yield* DurableObjectState.DurableObjectState;

    return Context.make(MessageDeliveryFailpoint, {
      hit: (point) =>
        Effect.gen(function* () {
          const thread = state.raw.id.name ?? "";

          yield* observeWorkerInputDelivery(thread, point);

          if (point === "message-delivery:admission:after" && messageInterruptions.delete(thread)) {
            return yield* Effect.interrupt;
          }
          if (messageEvictions.get(thread) === point) {
            messageEvictions.delete(thread);
            state.raw.abort("message delivery eviction");
          }

          yield* holdDelivery(thread, point);
        }),
    });
  }),
);

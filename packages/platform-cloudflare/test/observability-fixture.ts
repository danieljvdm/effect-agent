import { Effect, Layer } from "effect";
import { OtlpExporter } from "effect/unstable/observability";

import { DurableObjectContext } from "../src/index.ts";

const flushes = new Map<string, number>();

export const flushCount = (conversationId: string): number => flushes.get(conversationId) ?? 0;

/** One event-scoped OTLP flusher proving the effect-cf native RPC integration. */
export const observabilityProbeLayer = Layer.effectDiscard(
  Effect.gen(function* () {
    const { ctx } = yield* DurableObjectContext;
    const flusher = yield* OtlpExporter.Flusher;
    const conversationId = ctx.id.name ?? ctx.id.toString();
    yield* flusher.register(
      Effect.sync(() => {
        flushes.set(conversationId, flushCount(conversationId) + 1);
      }),
    );
  }),
).pipe(Layer.provideMerge(OtlpExporter.layerFlusher));

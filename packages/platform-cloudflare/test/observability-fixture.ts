import { Effect, Layer, Option, Tracer } from "effect";
import type { DurableObject } from "effect-cf";
import { OtlpExporter } from "effect/unstable/observability";

import { DurableObjectContext } from "../src/index.ts";

const flushes = new Map<string, number>();
const failingFlushes = new Set<string>();

interface TelemetryProbe {
  readonly invocations: Array<DurableObject.RunOptions>;
  readonly layerParents: Array<Option.Option<Tracer.AnySpan>>;
  readonly spans: Array<Tracer.NativeSpan>;
}

// Test-only observations of live invocations, never exported or written to Object storage.
const probes = new Map<string, TelemetryProbe>();
export const telemetryProbe = (conversationId: string): TelemetryProbe => {
  const existing = probes.get(conversationId);
  if (existing !== undefined) return existing;
  const probe: TelemetryProbe = { invocations: [], layerParents: [], spans: [] };
  probes.set(conversationId, probe);
  return probe;
};

export const flushCount = (conversationId: string): number => flushes.get(conversationId) ?? 0;

export const failNextFlush = (conversationId: string): void => {
  failingFlushes.add(conversationId);
};

/** One event-scoped OTLP flusher proving the effect-cf native RPC integration. */
export const observabilityProbeLayer = Layer.effectDiscard(
  Effect.gen(function* () {
    const { ctx } = yield* DurableObjectContext;
    const flusher = yield* OtlpExporter.Flusher;
    const conversationId = ctx.id.name ?? ctx.id.toString();
    yield* flusher.register(
      Effect.sync(() => {
        flushes.set(conversationId, flushCount(conversationId) + 1);
        if (failingFlushes.delete(conversationId)) {
          throw new Error("fixture exporter defect");
        }
      }),
    );
  }),
).pipe(
  Layer.provideMerge(OtlpExporter.layerFlusher),
  Layer.merge(
    Layer.effect(Tracer.Tracer)(
      Effect.gen(function* () {
        const { ctx } = yield* DurableObjectContext;
        const probe = telemetryProbe(ctx.id.name ?? ctx.id.toString());
        probe.layerParents.push(yield* Effect.serviceOption(Tracer.ParentSpan));
        return Tracer.make({
          span(options) {
            const span = new Tracer.NativeSpan(options);
            probe.spans.push(span);
            return span;
          },
        });
      }),
    ),
  ),
);

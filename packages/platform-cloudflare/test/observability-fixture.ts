import { DurableObjectContext } from "@effect-agent/platform-cloudflare/cloudflare-bindings";
import type { Option } from "effect";
import { Effect, Layer, Tracer } from "effect";
import type { DurableObject } from "effect-cf";

interface TelemetryProbe {
  readonly invocations: Array<DurableObject.RunOptions>;
  readonly layerParents: Array<Option.Option<Tracer.AnySpan>>;
  readonly spans: Array<Tracer.NativeSpan>;
}

// Test-only observations of live invocations, never exported or written to Object storage.
const probes = new Map<string, TelemetryProbe>();

export const telemetryProbe = (threadId: string): TelemetryProbe => {
  const existing = probes.get(threadId);

  if (existing !== undefined) return existing;
  const probe: TelemetryProbe = { invocations: [], layerParents: [], spans: [] };

  probes.set(threadId, probe);

  return probe;
};

/** Event-scoped tracer for native RPC invocation observations. */
export const observabilityProbeLayer = Layer.effect(Tracer.Tracer)(
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
);

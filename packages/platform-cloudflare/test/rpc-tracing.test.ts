import { CloudflareThreadClient } from "@effect-agent/platform-cloudflare/CloudflareThreadClient";
import { describe, expect, it } from "@effect/vitest";
import { env, runInDurableObject } from "cloudflare:test";
import { Effect, Option, Tracer } from "effect";

import { decodeThreadId } from "./fixtures.ts";
import { telemetryProbe } from "./observability-fixture.ts";

describe("DEPLOY-016 native receiver invocation contract", () => {
  it.effect.each([
    { rpcTracing: true, sampled: true },
    { rpcTracing: true, sampled: false },
    { rpcTracing: false, sampled: true },
  ])("preserves live context and starts a fresh alarm root %#", (options) =>
    Effect.gen(function* () {
      const threadId = decodeThreadId(`native-tracing-${options.rpcTracing}-${options.sampled}`);
      const request = { limit: 7 };
      const spans: Array<Tracer.NativeSpan> = [];

      const tracer = Tracer.make({
        span(spanOptions) {
          const span = new Tracer.NativeSpan(spanOptions);

          spans.push(span);

          return span;
        },
      });

      const clientLayer = CloudflareThreadClient.layerFromBinding({
        namespace: env.TELEMETRY,
        ...(options.rpcTracing ? { rpcTracing: "TELEMETRY" } : {}),
      });

      const failure = yield* Effect.gen(function* () {
        const client = yield* CloudflareThreadClient;

        return yield* client.readPage(threadId, request).pipe(Effect.flip);
      }).pipe(
        Effect.withSpan("application-caller", { sampled: options.sampled }),
        Effect.provide(clientLayer),
        Effect.provideService(Tracer.Tracer, tracer),
        Effect.withTracerEnabled(true),
      );

      expect(failure).toMatchObject({ _tag: "ThreadNotMaterialized", threadId });
      const clientSpan = spans.find((span) => span.kind === "client");
      const stub = env.TELEMETRY.get(env.TELEMETRY.idFromName(threadId));

      yield* Effect.promise(() =>
        runInDurableObject(stub, (instance, state) => {
          const probe = telemetryProbe(state.id.name ?? state.id.toString());
          const invocation = probe.invocations.find((entry) => entry.event === "rpc");

          expect(invocation?.rpc).toMatchObject({ service: "TELEMETRY", method: "observePage" });
          expect(invocation?.rpc?.args).toHaveLength(1);
          expect(invocation?.rpc?.args[0]).toEqual(request);
          const serverSpan = probe.spans.find((span) => span.name === "TELEMETRY/observePage");

          if (serverSpan === undefined) throw new Error("Missing application-owned server span");
          expect(serverSpan.kind).toBe("server");
          expect(serverSpan.status._tag).toBe("Ended");
          const layerParent = Option.getOrUndefined(probe.layerParents[0] ?? Option.none());

          if (options.rpcTracing) {
            if (clientSpan === undefined) throw new Error("Missing native client span");
            expect(invocation?.rpc?.parent).toEqual({
              _tag: "effect-cf/RpcTraceContext/v1",
              traceId: clientSpan.traceId,
              spanId: clientSpan.spanId,
              sampled: options.sampled,
            });
            expect(layerParent?.spanId).toBe(clientSpan.spanId);
            expect(serverSpan.traceId).toBe(clientSpan.traceId);
            expect(Option.getOrUndefined(serverSpan.parent)?.spanId).toBe(clientSpan.spanId);
            expect(serverSpan.sampled).toBe(options.sampled);
          } else {
            expect(clientSpan).toBeUndefined();
            expect(invocation?.rpc?.parent).toBeUndefined();
            expect(layerParent).toBeUndefined();
            expect(Option.isNone(serverSpan.parent)).toBe(true);
          }

          return instance.alarm();
        }),
      );
      yield* Effect.promise(() =>
        runInDurableObject(stub, (_instance, state) => {
          const probe = telemetryProbe(state.id.name ?? state.id.toString());
          const alarm = probe.invocations.find((entry) => entry.event === "alarm");

          expect(alarm).toEqual({ event: "alarm" });
          const alarmSpan = probe.spans.find((span) => span.name === "TELEMETRY/alarm");
          const rpcSpan = probe.spans.find((span) => span.name === "TELEMETRY/observePage");

          if (alarmSpan === undefined) throw new Error("Missing application-owned alarm span");
          expect(Option.isNone(alarmSpan.parent)).toBe(true);
          expect(alarmSpan.traceId).not.toBe(rpcSpan?.traceId);
          expect(alarmSpan.status._tag).toBe("Ended");
          expect(probe.layerParents.at(-1)).toEqual(Option.none());
        }),
      );
    }),
  );

  it.effect("leaves malformed native metadata in the arguments and does not adopt it", () =>
    Effect.gen(function* () {
      const threadId = decodeThreadId("native-tracing-invalid-metadata");
      const stub = env.TELEMETRY.get(env.TELEMETRY.idFromName(threadId));

      const invalid = {
        _tag: "effect-cf/RpcTraceContext/v1",
        traceId: "invalid-trace-id",
        spanId: "1234567890abcdef",
        sampled: true,
      };

      const request = { limit: 1 };
      const result = yield* Effect.promise(() => stub.observePage(request, invalid));

      expect(result).toMatchObject({
        _tag: "HostFailed",
        failure: { _tag: "ThreadNotMaterialized", threadId },
      });
      yield* Effect.promise(() =>
        runInDurableObject(stub, (_instance, state) => {
          const probe = telemetryProbe(state.id.name ?? state.id.toString());
          const invocation = probe.invocations.find((entry) => entry.event === "rpc");

          expect(invocation?.rpc?.args).toEqual([request, invalid]);
          expect(invocation?.rpc?.parent).toBeUndefined();
          expect(probe.layerParents[0]).toEqual(Option.none());
        }),
      );
    }),
  );
});

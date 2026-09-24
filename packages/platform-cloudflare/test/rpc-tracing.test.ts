import { ThreadObjectNamespace } from "@effect-agent/platform-cloudflare/cloudflare-bindings";
import {
  LedgerLookupCall,
  encodePortRequest,
} from "@effect-agent/storage-cloudflare/port-protocol";
import { ThreadPortTransport } from "@effect-agent/storage-cloudflare/port-routing";
import { describe, expect, it } from "@effect/vitest";
import { env, runInDurableObject } from "cloudflare:test";
import { Effect, Layer, Option, Tracer } from "effect";
import { SubmissionLookupByKey } from "effect-agent/submission-ledger";

import { threadPortTransportLayer } from "../src/internal/transport.ts";
import { decodeIdempotencyKey, decodeThreadId, TEST_PRINCIPAL } from "./fixtures.ts";
import { telemetryProbe } from "./observability-fixture.ts";

describe("DEPLOY-016 native receiver invocation contract", () => {
  // Regression: https://github.com/danieljvdm/effect-agent/commit/baecd08f1d6f2c0698e16487cdcccf2f6ffcebca
  it.effect.each([
    { label: "enabled", rpcTracing: true, sampled: true, disablePropagation: false },
    { label: "disabled", rpcTracing: false, sampled: true, disablePropagation: false },
  ])("preserves one native port call with $label tracing", (options) =>
    Effect.gen(function* () {
      const threadId = decodeThreadId(`native-port-tracing-${options.label}`);

      const request = yield* encodePortRequest(
        LedgerLookupCall.make({
          request: SubmissionLookupByKey.make({
            threadId,
            principal: TEST_PRINCIPAL,
            idempotencyKey: decodeIdempotencyKey("native-port-tracing"),
          }),
        }),
      );

      const spans: Array<Tracer.NativeSpan> = [];

      const tracer = Tracer.make({
        span(spanOptions) {
          const span = new Tracer.NativeSpan(spanOptions);

          spans.push(span);

          return span;
        },
      });

      const transport = threadPortTransportLayer.pipe(
        Layer.provide(
          ThreadObjectNamespace.layer(
            env.TELEMETRY,
            options.rpcTracing ? { rpcTracing: "TELEMETRY" } : {},
          ),
        ),
      );

      const result = yield* ThreadPortTransport.use((port) => port.call(threadId, request)).pipe(
        Effect.withSpan("port-caller", { sampled: options.sampled }),
        Effect.provide(transport),
        Effect.provideService(Tracer.Tracer, tracer),
        Effect.provideService(Tracer.DisablePropagation, options.disablePropagation),
        Effect.withTracerEnabled(true),
      );

      expect(result).toEqual({ _tag: "PortSucceeded", result: { _tag: "LedgerLookupResult" } });
      const clientSpan = spans.find((span) => span.kind === "client");
      const stub = env.TELEMETRY.get(env.TELEMETRY.idFromName(threadId));

      yield* Effect.promise(() =>
        runInDurableObject(stub, (_instance, state) => {
          const probe = telemetryProbe(state.id.name ?? state.id.toString());
          const invocations = probe.invocations.filter((entry) => entry.event === "rpc");

          expect(invocations).toHaveLength(1);
          const invocation = invocations[0]?.rpc;

          expect(invocation).toMatchObject({ service: "TELEMETRY", method: "portCall" });
          expect(invocation?.args).toEqual([request]);
          const serverSpan = probe.spans.find((span) => span.name === "TELEMETRY/portCall");

          if (serverSpan === undefined) throw new Error("Missing native port server span");
          const layerParent = Option.getOrUndefined(probe.layerParents[0] ?? Option.none());

          if (options.rpcTracing) {
            if (clientSpan === undefined) throw new Error("Missing native port client span");
          } else {
            expect(clientSpan).toBeUndefined();
          }
          if (options.rpcTracing && !options.disablePropagation) {
            if (clientSpan === undefined) throw new Error("Missing native port client span");
            expect(invocation?.parent).toEqual({
              _tag: "effect-cf/RpcTraceContext/v1",
              traceId: clientSpan.traceId,
              spanId: clientSpan.spanId,
              sampled: options.sampled,
            });
            expect(layerParent?.spanId).toBe(clientSpan.spanId);
            expect(serverSpan.traceId).toBe(clientSpan.traceId);
            expect(Option.getOrUndefined(serverSpan.parent)?.spanId).toBe(clientSpan.spanId);
          } else {
            expect(invocation?.parent).toBeUndefined();
            expect(layerParent).toBeUndefined();
            expect(Option.isNone(serverSpan.parent)).toBe(true);
          }
        }),
      );
    }),
  );
});

import { dynamicWorkerCodeExecutorLayer } from "@effect-agent/platform-cloudflare/cloudflare-code-mode";
import { Effect, Layer, Schema, Tracer } from "effect";
import {
  CodeExecutionHost,
  CodeExecutionProtocolError,
  CodeExecutor,
  CodeHostCall,
  CodeHostCallResult,
} from "effect-agent/code-executor";
import { SandboxImplementation } from "effect-agent/sandbox";
import { FetchHttpClient } from "effect/unstable/http";
import { OtlpSerialization, OtlpTracer } from "effect/unstable/observability";

import * as Wire from "./code-executor-wire.ts";

interface Env {
  readonly LOADER: WorkerLoader;
  readonly HOSTCALL: Fetcher;
}

const telemetry = OtlpTracer.layer({
  url: "http://127.0.0.1:4318/v1/traces",
  resource: { serviceName: "effect-agent-browser-code-executor" },
}).pipe(Layer.provide(OtlpSerialization.layerJson), Layer.provide(FetchHttpClient.layer));

/** The generated program runs in the production Dynamic Worker executor, never this host. */
export default {
  async fetch(request: globalThis.Request, env: Env): Promise<globalThis.Response> {
    const input = Schema.decodeSync(Wire.Request)(await request.text());

    const execute = Effect.gen(function* () {
      const executor = yield* CodeExecutor;
      const result = yield* executor.execute(input.request).pipe(Effect.result);

      return Schema.encodeSync(Wire.Outcome)(
        result._tag === "Success" ? { success: result.success } : { failure: result.failure },
      );
    }).pipe(
      Effect.provideService(CodeExecutionHost, {
        call: (call) =>
          Effect.promise(async () => {
            const response = await env.HOSTCALL.fetch(`https://broker.invalid/${input.id}`, {
              method: "POST",
              body: Schema.encodeSync(Schema.fromJsonString(CodeHostCall))(call),
            });

            if (!response.ok) throw new Error("Acceptance broker transport refused the call");

            return Schema.decodeSync(Schema.fromJsonString(CodeHostCallResult))(
              await response.text(),
            );
          }),
      }),
      Effect.provide(dynamicWorkerCodeExecutorLayer({ loader: env.LOADER })),
      Effect.withParentSpan(Tracer.externalSpan({ traceId: input.traceId, spanId: input.spanId })),
      Effect.provide(telemetry),
      Effect.scoped,
    );

    try {
      return new Response(await Effect.runPromise(execute));
    } catch {
      return new Response(
        Schema.encodeSync(Wire.Outcome)({
          failure: new CodeExecutionProtocolError({
            implementation: SandboxImplementation.make({
              isolation: "isolated",
              identity: "cloudflare-dynamic-worker",
            }),
            message: "Acceptance worker transport failed.",
          }),
        }),
      );
    }
  },
};

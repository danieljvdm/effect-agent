import { Effect, Fiber, Layer, Schema, type Scope, type Tracer } from "effect";
import {
  CodeExecutionHost,
  CodeExecutionProtocolError,
  CodeExecutor,
  CodeHostCall,
  CodeHostCallResult,
} from "effect-agent/code-executor";
import { SandboxImplementation } from "effect-agent/sandbox";
import { build } from "esbuild";
import { Miniflare, Response, convertV4MiniflareOptions } from "miniflare";

import * as Wire from "./code-executor-wire.ts";

const implementation = SandboxImplementation.make({
  isolation: "isolated",
  identity: "cloudflare-dynamic-worker",
});

const protocol = (stage: string) =>
  new CodeExecutionProtocolError({
    implementation,
    message: `Acceptance executor ${stage} transport failed.`,
  });

/** Local standalone host transport to the existing isolated Worker executor.
 * Every callback uses the original Code Mode host/broker; no tools or DOM logic live here.
 */
export const workerExecutor = Layer.effect(
  CodeExecutor,
  Effect.gen(function* () {
    const bundle = yield* Effect.tryPromise({
      try: () =>
        build({
          entryPoints: [new URL("./code-executor-worker.ts", import.meta.url).pathname],
          bundle: true,
          write: false,
          format: "esm",
          target: "es2022",
          platform: "browser",
          conditions: ["workerd", "worker", "browser"],
          external: ["cloudflare:*", "node:*"],
          logLevel: "silent",
        }),
      catch: () => protocol("build"),
    });

    const source = bundle.outputFiles[0]?.text;

    if (source === undefined) return yield* protocol("build-output");

    const passes = new Map<
      string,
      {
        host: CodeExecutionHost["Service"];
        scope: Scope.Scope;
        span: Tracer.Span;
      }
    >();

    const runtime = yield* Effect.acquireRelease(
      Effect.try({
        try: () =>
          new Miniflare(
            convertV4MiniflareOptions({
              modules: true,
              script: source,
              modulesRoot: "/",
              compatibilityDate: "2025-05-01",
              compatibilityFlags: [
                "nodejs_compat",
                "enable_ctx_exports",
                "no_handle_cross_request_promise_resolution",
              ],
              workerLoaders: { LOADER: {} },
              serviceBindings: {
                HOSTCALL: async (request) => {
                  const pass = passes.get(new URL(request.url).pathname.slice(1));

                  if (pass === undefined) return new Response(null, { status: 410 });

                  const call = Schema.decodeSync(Schema.fromJsonString(CodeHostCall))(
                    await request.text(),
                  );

                  const value = await Effect.runPromise(
                    pass.host
                      .call(call)
                      .pipe(
                        Effect.withParentSpan(pass.span),
                        Effect.forkIn(pass.scope),
                        Effect.flatMap(Fiber.join),
                      ),
                    { signal: request.signal },
                  );

                  return new Response(
                    Schema.encodeSync(Schema.fromJsonString(CodeHostCallResult))(
                      Schema.decodeSync(CodeHostCallResult)(value),
                    ),
                  );
                },
              },
            }),
          ),
        catch: () => protocol("start"),
      }),
      (runtime) => Effect.promise(() => runtime.dispose()),
    );

    return CodeExecutor.of({
      execute: (request) =>
        Effect.gen(function* () {
          const host = yield* CodeExecutionHost;
          const scope = yield* Effect.scope;
          const span = yield* Effect.currentSpan.pipe(Effect.orDie);
          const id = crypto.randomUUID();

          const body = Schema.encodeSync(Wire.Request)({
            id,
            request,
            traceId: span.traceId,
            spanId: span.spanId,
          });

          passes.set(id, { host, scope, span });
          yield* Effect.addFinalizer(() => Effect.sync(() => passes.delete(id)));

          const response = yield* Effect.tryPromise({
            try: (signal) =>
              runtime.dispatchFetch("https://executor.invalid/", { method: "POST", body, signal }),
            catch: () => protocol("request"),
          });

          if (!response.ok) return yield* protocol(`response-${response.status}`);

          const text = yield* Effect.tryPromise({
            try: () => response.text(),
            catch: () => protocol("response-body"),
          });

          const result = yield* Schema.decodeEffect(Wire.Outcome)(text).pipe(
            Effect.mapError(() => protocol("decode")),
          );

          return "success" in result ? result.success : yield* result.failure;
        }),
    });
  }),
);

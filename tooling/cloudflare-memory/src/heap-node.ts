// Effect has no VM-isolate or V8 heap-counter service. These Node-only APIs are
// diagnostic adapters; the Worker bundle executes in a separate global realm.
import * as asyncHooks from "node:async_hooks";
import process from "node:process";
import * as vm from "node:vm";

import { Effect, FileSystem } from "effect";

import { HeapProbeError, NodeSample } from "./heap-contracts.ts";

const collect = Effect.try({
  try: () => {
    if (!globalThis.gc) throw new Error("Run with --expose-gc");
    globalThis.gc();
    globalThis.gc();
  },
  catch: (cause) => HeapProbeError.make({ operation: "Node GC", cause }),
});

const memory = Effect.sync(() => ({ ...process.memoryUsage() }));

export const measureNode = Effect.fn("heap.measureNode")(function* (bundle: string) {
  const fs = yield* FileSystem.FileSystem;

  const realm = yield* Effect.try({
    try: () => {
      const context = vm.createContext({
        console,
        setTimeout,
        clearTimeout,
        setInterval,
        clearInterval,
        queueMicrotask,
        TextEncoder,
        TextDecoder,
        URL,
        URLSearchParams,
        AbortController,
        AbortSignal,
        Headers,
        Request,
        Response,
        ReadableStream,
        WritableStream,
        TransformStream,
        crypto,
        performance,
        fetch,
        atob,
        btoa,
        structuredClone,
      });

      const synthetic = (exports: Record<string, unknown>, identifier: string) =>
        new vm.SyntheticModule(
          Object.keys(exports),
          function () {
            for (const [key, value] of Object.entries(exports)) this.setExport(key, value);
          },
          { context, identifier },
        );

      const imports = new Map([
        [
          "cloudflare:workers",
          synthetic(
            {
              RpcStub: class RpcStub {},
              RpcTarget: class RpcTarget {},
              WorkerEntrypoint: class WorkerEntrypoint {},
              DurableObject: class DurableObject {},
              WorkflowEntrypoint: class WorkflowEntrypoint {},
              tracing: {},
            },
            "cloudflare:workers",
          ),
        ],
        [
          "cloudflare:workflows",
          synthetic(
            { NonRetryableError: class NonRetryableError extends Error {} },
            "cloudflare:workflows",
          ),
        ],
        ["node:async_hooks", synthetic(asyncHooks, "node:async_hooks")],
      ]);

      return { context, imports };
    },
    catch: (cause) => HeapProbeError.make({ operation: "create Node VM realm", cause }),
  });

  yield* collect;
  const baseline = yield* memory;
  let source: string | undefined = yield* fs.readFileString(bundle);

  const module = yield* Effect.tryPromise({
    try: async () => {
      const module = new vm.SourceTextModule(source ?? "", {
        context: realm.context,
        identifier: bundle,
      });

      await module.link((specifier) => {
        const imported = realm.imports.get(specifier);

        if (!imported) throw new Error(`Unsupported external import ${specifier}`);

        return imported;
      });

      return module;
    },
    catch: (cause) => HeapProbeError.make({ operation: "compile exact bundle", cause }),
  });

  source = undefined;
  yield* Effect.tryPromise({
    try: () => module.evaluate(),
    catch: (cause) => HeapProbeError.make({ operation: "evaluate exact bundle", cause }),
  });
  const evaluated = yield* memory;

  yield* collect;
  const retained = yield* memory;

  return yield* Effect.sync(() =>
    NodeSample.make({
      node: process.version,
      v8: process.versions.v8,
      platform: process.platform,
      arch: process.arch,
      baseline,
      evaluated,
      retained,
      evaluatedHeapDelta: evaluated.heapUsed - baseline.heapUsed,
      retainedHeapDelta: retained.heapUsed - baseline.heapUsed,
      exports: Object.keys(module.namespace),
    }),
  );
});

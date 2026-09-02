import { NodeServices } from "@effect/platform-node";
import { Effect, FileSystem, Schema } from "effect";
import { build } from "esbuild";
import { convertV4MiniflareOptions, Miniflare } from "miniflare";
import { expect, it } from "vite-plus/test";

import { Sample } from "../src/contracts.ts";

class ProbeFailure extends Schema.TaggedError<ProbeFailure>()("ProbeFailure", {
  operation: Schema.String,
}) {}

const open = (script: string, path: string) =>
  Effect.acquireRelease(
    Effect.try({
      try: () =>
        new Miniflare(
          convertV4MiniflareOptions({
            modules: true,
            script,
            compatibilityDate: "2026-08-01",
            resourcePersistencePath: path,
            bindings: { BENCH_TOKEN: "test-only" },
            durableObjects: {
              MEMORIES: { className: "ProjectMemory", useSQLite: true },
              THREADS: { className: "BenchmarkThread", useSQLite: true },
            },
          }),
        ),
      catch: () => ProbeFailure.make({ operation: "open workerd" }),
    }),
    (runtime) => Effect.promise(() => runtime.dispose()),
  );
const request = (runtime: Miniflare, path: string) =>
  Effect.tryPromise({
    try: () =>
      runtime.dispatchFetch(`http://benchmark/${path}`, {
        headers: { authorization: "Bearer test-only" },
      }),
    catch: () => ProbeFailure.make({ operation: "request" }),
  });

it(
  "shares persisted memory and exact ingestion receipts across complete workerd restarts",
  () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const directory = yield* fs.makeTempDirectoryScoped({ prefix: "kom19-memory-" });
        const bundle = yield* Effect.tryPromise({
          try: () =>
            build({
              entryPoints: [new URL("../src/worker.ts", import.meta.url).pathname],
              bundle: true,
              write: false,
              format: "esm",
              platform: "browser",
              target: "es2022",
              external: ["cloudflare:*"],
            }),
          catch: () => ProbeFailure.make({ operation: "bundle" }),
        });
        const script = bundle.outputFiles[0]?.text;
        if (script === undefined) return yield* ProbeFailure.make({ operation: "bundle output" });
        yield* Effect.gen(function* () {
          const runtime = yield* open(script, directory);
          const denied = yield* Effect.promise(() =>
            runtime.dispatchFetch("http://benchmark/seed?case=4"),
          );
          expect(denied.status).toBe(401);
          expect((yield* request(runtime, "seed?case=4&caller=a")).status).toBe(200);
        }).pipe(Effect.scoped);
        yield* Effect.gen(function* () {
          const runtime = yield* open(script, directory);
          const response = yield* request(runtime, "sample?case=4&caller=b");
          const raw = yield* Effect.promise(() => response.json());
          const sample = yield* Schema.decodeUnknownEffect(Sample)(raw);
          expect(sample).toMatchObject({
            status: "ok",
            sourceCount: 4,
            candidateCount: 4,
            corpusTextBytes: 4096,
          });
          expect(sample.renderedBytes).toBeGreaterThan(0);
          // Re-ingestion uses the original null expected revision and must resolve receipts.
          expect((yield* request(runtime, "seed?case=4&caller=a")).status).toBe(200);
        }).pipe(Effect.scoped);
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
    ),
  30_000,
);

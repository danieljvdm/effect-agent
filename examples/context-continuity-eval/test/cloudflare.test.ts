import { fileURLToPath } from "node:url";

import { NodeCrypto, NodeServices } from "@effect/platform-node";
import { ConfigProvider, Effect, FileSystem, Layer } from "effect";
import { FetchHttpClient } from "effect/unstable/http";
import { build } from "esbuild";
import { convertV4MiniflareOptions, Miniflare } from "miniflare";
import { expect, it } from "vite-plus/test";

import { runCloudflareEvaluation } from "../src/cloudflare.ts";
import { scriptedResponse } from "./scripted-model.ts";

it("runs the same pressure and evidence gate through the public Cloudflare host in workerd", async () => {
  const bundle = await build({
    entryPoints: [fileURLToPath(new URL("../src/cloudflare-worker.ts", import.meta.url).href)],
    bundle: true,
    write: false,
    format: "esm",
    platform: "browser",
    target: "es2022",
    external: ["cloudflare:*", "node:*"],
    conditions: ["workerd", "worker", "browser"],
    define: {
      CONTEXT_EVAL_SOURCE_COMMIT: JSON.stringify("a".repeat(40)),
      CONTEXT_EVAL_DIRTY: "false",
    },
  });

  const script = bundle.outputFiles[0]?.text;

  if (script === undefined) throw new Error("No Cloudflare bundle");
  let countedInput = 0;
  let original: string | undefined;
  const calls = new Map<number, number>();

  const runtime = new Miniflare(
    convertV4MiniflareOptions({
      modules: true,
      script,
      modulesRoot: "/",
      compatibilityDate: "2026-08-01",
      compatibilityFlags: ["nodejs_compat"],
      durableObjects: { THREADS: { className: "ContinuityThread", useSQLite: true } },
      bindings: {
        CONTEXT_EVAL_TOKEN: "test-token",
        OPENAI_API_KEY: "test-only",
        CONTEXT_EVAL_MODEL: "gpt-6-astra",
      },
      outboundService: async (request) => {
        const json = await request.text();

        if (new URL(request.url).hostname !== "api.openai.com")
          throw new Error("Unexpected network request");
        if (request.url.endsWith("/input_tokens")) {
          countedInput = Math.ceil(new TextEncoder().encode(json).length / 4);

          return Response.json({ object: "response.input_tokens", input_tokens: countedInput });
        }

        const phase = Math.max(
          ...[...json.matchAll(/Project update (\d+)\./g)].map((m) => Number(m[1])),
        );

        const ordinal = calls.get(phase) ?? 0;

        calls.set(phase, ordinal + 1);

        const next = await Effect.runPromise(
          scriptedResponse(json, phase, ordinal, countedInput, "gpt-6-astra", 17, original),
        );

        original = next.original;

        return next.response;
      },
    }),
  );

  try {
    const unauthorized = await runtime.dispatchFetch("https://eval.test/identity");

    expect(unauthorized.status).toBe(401);

    const localFetch: typeof globalThis.fetch = async (input, init) => {
      const request = new Request(input, init);

      const response = await runtime.dispatchFetch(request.url, {
        method: request.method,
        headers: Object.fromEntries(request.headers),
        ...(request.method === "POST" ? { body: await request.text() } : {}),
      });

      return new Response(response.body, {
        status: response.status,
        headers: Object.fromEntries(response.headers),
      });
    };

    const report = await Effect.runPromise(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;

        const outputDirectory = yield* fs.makeTempDirectoryScoped({
          prefix: "continuity-cloudflare-test-",
        });

        return yield* runCloudflareEvaluation(
          {
            model: "gpt-6-astra",
            reasoningEffort: "low",
            seed: 17,
            outputDirectory,
            sourceCommit: "a".repeat(40),
            dirtyWorkingTree: false,
            maxCostMicrousd: 10_000_000,
          },
          "https://eval.test",
        );
      }).pipe(
        Effect.scoped,
        Effect.provide(
          Layer.mergeAll(
            FetchHttpClient.layer.pipe(
              Layer.provide(Layer.succeed(FetchHttpClient.Fetch, localFetch)),
            ),
            NodeServices.layer,
            NodeCrypto.layer,
            ConfigProvider.layer(ConfigProvider.fromUnknown({ CONTEXT_EVAL_TOKEN: "test-token" })),
          ),
        ),
      ),
    );

    expect(report.failure).toBeNull();
    expect(report.phases.flatMap((p) => p.checks.filter((c) => !c.passed))).toEqual([]);
    expect(report.checks.filter((c) => !c.passed)).toEqual([]);
    expect(report.status).toBe("passed");
    expect(report.restarts.map((r) => r.mechanism)).toEqual([
      "durable-object-eviction",
      "durable-object-eviction",
    ]);
  } finally {
    await runtime.dispose();
  }
}, 90_000);

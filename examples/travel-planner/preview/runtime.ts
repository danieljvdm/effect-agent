import { fileURLToPath } from "node:url";

import { Effect, FileSystem, Schema } from "effect";
import { build } from "esbuild";
import { convertV4MiniflareOptions, Miniflare } from "miniflare";

export class PreviewError extends Schema.TaggedError<PreviewError>()("PreviewError", {
  message: Schema.String,
  cause: Schema.Defect(),
}) {}

const root = fileURLToPath(new URL("../", import.meta.url));
const key = () => Buffer.from(crypto.getRandomValues(new Uint8Array(32))).toString("base64url");

/** A fresh, loopback-only runtime. No deployed bindings or credentials are read. */
export const localPreview = Effect.fn("localPreview")(function* (
  port: number,
  assets = { server: `${root}dist/server`, client: `${root}dist/client` },
) {
  const fs = yield* FileSystem.FileSystem;
  const directory = yield* fs.makeTempDirectoryScoped({ prefix: "travel-preview-" });
  const origin = `https://127.0.0.1:${port}`;
  const server = assets.server;
  const files = yield* fs.readDirectory(server, { recursive: true });

  const modules = ["worker.js", ...files.filter((file) => file !== "worker.js")]
    .filter((file) => file.endsWith(".js") || file.endsWith(".css"))
    .map((file) => ({
      type: file.endsWith(".css") ? ("Text" as const) : ("ESModule" as const),
      path: `${server}/${file}`,
    }));

  const bundle = yield* Effect.tryPromise({
    try: () =>
      build({
        entryPoints: [`${root}preview/planner.ts`],
        bundle: true,
        write: false,
        format: "esm",
        target: "es2022",
        platform: "browser",
        conditions: ["workerd", "worker", "browser"],
        external: ["cloudflare:*", "node:*"],
        alias: { "@tanstack/react-start/server-entry": `${root}test/fixtures/start.ts` },
        banner: {
          js: 'import { createRequire } from "node:module"; const require = createRequire("/preview.mjs");',
        },
        logLevel: "silent",
      }),
    catch: (cause) => new PreviewError({ message: "Could not build the offline planner.", cause }),
  });

  const bindings = {
    AUTH_ORIGIN: origin,
    AUTH_BINDING_KEY: key(),
    AUTH_PROOF_KEY: key(),
    AUTH_TRANSACTION_KEY: key(),
    AUTH_GITHUB_CLIENT_ID: "local-preview",
    AUTH_GITHUB_CLIENT_SECRET: "local-preview",
    AUTH_EMAIL_FROM: "signin@preview.invalid",
    BYOK_ENCRYPTION_KEY: Buffer.from(crypto.getRandomValues(new Uint8Array(32))).toString("base64"),
  };

  const preview = yield* Effect.acquireRelease(
    Effect.try({
      try: () =>
        new Miniflare(
          convertV4MiniflareOptions({
            host: "127.0.0.1",
            port,
            https: true,
            rootPath: directory,
            resourceTmpPath: directory,
            workers: [
              {
                name: "preview-ui",
                modules,
                modulesRoot: server,
                compatibilityDate: "2026-07-01",
                compatibilityFlags: ["nodejs_compat"],
                bindings,
                durableObjects: {
                  AUTH: { className: "PlannerAuth", useSQLite: true },
                  ACCOUNT_THREADS: {
                    className: "TravelPlannerThread",
                    scriptName: "preview-planner",
                    useSQLite: true,
                  },
                },
                email: { send_email: [{ name: "AUTH_EMAIL" }] },
                assets: {
                  directory: assets.client,
                  binding: "ASSETS",
                  run_worker_first: true,
                  routerConfig: { has_user_worker: true },
                },
                outboundService: () =>
                  new Response("Unavailable in local preview", { status: 503 }),
              },
              {
                name: "preview-planner",
                modules: true,
                script: bundle.outputFiles[0]!.text,
                modulesRoot: "/",
                compatibilityDate: "2026-07-01",
                compatibilityFlags: ["nodejs_compat"],
                bindings,
                durableObjects: {
                  ACCOUNT_THREADS: { className: "TravelPlannerThread", useSQLite: true },
                  AUTH: { className: "PlannerAuth", scriptName: "preview-ui", useSQLite: true },
                },
                r2Buckets: ["APP_BUILDS"],
                outboundService: (request) =>
                  new Response(null, {
                    status:
                      request.url === "https://api.openai.com/v1/models" &&
                      request.headers.get("authorization") === "Bearer sk-preview-local"
                        ? 200
                        : 503,
                  }),
              },
            ],
          }),
        ),
      catch: (cause) =>
        new PreviewError({ message: "Could not create the preview runtime.", cause }),
    }),
    (runtime) => Effect.promise(() => runtime.dispose()),
  );

  yield* Effect.tryPromise({
    try: () => preview.ready,
    catch: (cause) => new PreviewError({ message: "Could not start the preview runtime.", cause }),
  });

  return { preview, origin, directory };
});

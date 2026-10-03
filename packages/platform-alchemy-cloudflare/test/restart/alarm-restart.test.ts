import { builtinModules } from "node:module";

import { NodeFileSystem } from "@effect/platform-node";
import { it } from "@effect/vitest";
import { Effect, FileSystem, Schema } from "effect";
import { build } from "esbuild";
import { convertV4MiniflareOptions, Miniflare } from "miniflare";
import { expect } from "vite-plus/test";

import {
  alarmId,
  alarmTag,
  Introspection,
  objectName,
  Seeded,
  Status,
} from "./alarm-restart-contract.ts";

class RestartHarnessError extends Schema.TaggedError<RestartHarnessError>()("RestartHarnessError", {
  operation: Schema.String,
  cause: Schema.Defect(),
}) {}

const buildWorker = (filename: string) =>
  Effect.tryPromise({
    try: () =>
      build({
        entryPoints: [`${import.meta.dirname}/${filename}`],
        bundle: true,
        write: false,
        format: "esm",
        platform: "browser",
        target: "es2022",
        conditions: ["workerd", "worker", "browser"],
        define: { "globalThis.__ALCHEMY_RUNTIME__": "true" },
        external: ["cloudflare:*", "node:*"],
        logLevel: "silent",
        plugins: [
          {
            name: "node-builtins",
            setup(builder) {
              builder.onResolve({ filter: /^[^./]/ }, (args) =>
                builtinModules.includes(args.path)
                  ? { path: `node:${args.path}`, external: true }
                  : undefined,
              );
            },
          },
        ],
      }),
    catch: (cause) => RestartHarnessError.make({ operation: `bundle ${filename}`, cause }),
  }).pipe(
    Effect.flatMap((result) => {
      const output = result.outputFiles[0];

      if (!output)
        return RestartHarnessError.make({ operation: "read worker bundle", cause: filename });

      // Filesystem loaders are unreachable in this worker; Miniflare rejects their dynamic imports.
      return Effect.succeed(
        `const __disabledDynamicImport = () => Promise.reject(new Error("dynamic import is disabled in the restart fixture"));\n${output.text.replaceAll(/\bimport\s*\((?!\s*["'])/g, "__disabledDynamicImport(")}`,
      );
    }),
  );

const openRuntime = (directory: string, script: string) =>
  Effect.acquireRelease(
    Effect.try({
      try: () =>
        new Miniflare(
          convertV4MiniflareOptions({
            name: "alchemy-alarm-restart",
            modules: true,
            script,
            compatibilityDate: "2026-08-18",
            compatibilityFlags: ["nodejs_compat"],
            durableObjects: { RESTART_ALARMS: { className: "RestartAlarmOwner", useSQLite: true } },
            resourcePersistencePath: directory,
          }),
        ),
      catch: (cause) => RestartHarnessError.make({ operation: "open runtime", cause }),
    }),
    (runtime) => Effect.promise(() => runtime.dispose()),
  );

const read = <S extends Schema.Top>(runtime: Miniflare, path: string, schema: S) =>
  Effect.tryPromise({
    try: async () => {
      const response = await runtime.dispatchFetch(`http://restart${path}`);

      if (!response.ok) throw new Error(`${path}: ${response.status} ${await response.text()}`);

      return response.json();
    },
    catch: (cause) => RestartHarnessError.make({ operation: `request ${path}`, cause }),
  }).pipe(Effect.flatMap(Schema.decodeUnknownEffect(schema)));

it.live(
  "adopts and delivers the previous host's alarm after a full runtime restart without an Object request",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "alchemy-alarm-upgrade-" });
      const legacyScript = yield* buildWorker("legacy-alarm-worker.ts");
      const alchemyScript = yield* buildWorker("alchemy-alarm-worker.ts");

      const seeded = yield* Effect.scoped(
        Effect.gen(function* () {
          const runtime = yield* openRuntime(directory, legacyScript);
          const seeded = yield* read(runtime, "/seed", Seeded);

          expect(seeded.pendingEvents).toBe(1);
          expect(seeded.alarmDeliveries).toBe(0);

          return seeded;
        }),
      );

      const runtime = yield* openRuntime(directory, alchemyScript);
      let observed = yield* read(runtime, "/introspect", Introspection);

      // At most ten seconds for the real persisted alarm, matching the existing restart lane.
      for (
        let attempt = 0;
        attempt < 200 && observed.completedAlarmDeliveries === 0;
        attempt += 1
      ) {
        yield* Effect.sleep("50 millis");
        observed = yield* read(runtime, "/introspect", Introspection);
      }
      expect(observed.objectRequests).toBe(0);
      expect(observed.alarmDeliveries).toBeGreaterThan(0);
      expect(observed.completedAlarmDeliveries).toBeGreaterThan(0);
      expect(observed.handledEvents).toBe(1);

      const status = yield* read(runtime, "/status", Status);

      expect(status).toEqual({
        objectName,
        deliveries: [
          {
            id: alarmId,
            tag: alarmTag,
            message: "accepted before the runtime upgrade",
            scheduled_at: seeded.runAt,
          },
        ],
        pendingEvents: 0,
        legacyTables: 0,
        nativeAlarm: null,
      });
    }).pipe(Effect.scoped, Effect.provide(NodeFileSystem.layer)),
  120_000,
);

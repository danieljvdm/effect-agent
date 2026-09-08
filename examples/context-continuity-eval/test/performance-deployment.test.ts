import { NodeServices } from "@effect/platform-node";
import { ConfigProvider, Effect, Exit, FileSystem, Layer, Sink, Stream } from "effect";
import { HttpClient, HttpClientResponse } from "effect/unstable/http";
import { ChildProcessSpawner } from "effect/unstable/process";
import { expect, it } from "vite-plus/test";

import {
  makePerformanceDeployment,
  type PerformanceTarget,
} from "../src/performance-deployment.ts";

it.each(["deleted", "already-gone", "forbidden", "unconfirmed"] as const)(
  "retires the namespace and verifies Worker removal without KV access: %s",
  async (mode) => {
    const actions: Array<string> = [];
    let present = true;
    const name = `effect-agent-perf-${"a".repeat(32)}-candidate`;
    const url = `https://api.cloudflare.com/client/v4/accounts/${"b".repeat(32)}/workers/scripts/${name}`;

    const http = HttpClient.make((request) =>
      Effect.sync(() => {
        expect(request.url).toBe(url);
        expect(request.headers.authorization).toBe("Bearer test-token");
        actions.push(request.method);
        let status = present ? 200 : 404;

        if (request.method === "DELETE") {
          status = mode === "forbidden" ? 403 : mode === "already-gone" ? 404 : 204;
          if (mode === "deleted" || mode === "already-gone") present = false;
        }

        return HttpClientResponse.fromWeb(request, new Response(null, { status }));
      }),
    );

    const spawner = ChildProcessSpawner.make((command) =>
      Effect.sync(() => {
        if (command._tag !== "StandardCommand") throw new Error("Unexpected piped cleanup command");
        expect(command.command).toBe("vp");
        expect(command.args.slice(0, 4)).toEqual(["exec", "wrangler", "deploy", "--config"]);
        expect(command.args[4]).toMatch(/\/cleanup\.json$/);
        actions.push("retire-namespace");

        return ChildProcessSpawner.makeHandle({
          pid: ChildProcessSpawner.ProcessId(42),
          exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(0)),
          isRunning: Effect.succeed(false),
          kill: () => Effect.void,
          stdin: Sink.drain,
          stdout: Stream.empty,
          stderr: Stream.empty,
          all: Stream.empty,
          getInputFd: () => Sink.drain,
          getOutputFd: () => Stream.empty,
          unref: Effect.succeed(Effect.void),
        });
      }),
    );

    const exit = await Effect.runPromise(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const directory = yield* fs.makeTempDirectoryScoped({ prefix: "performance-cleanup-" });

        const target: typeof PerformanceTarget.Type = {
          label: "candidate",
          name,
          url: "https://unused.example",
          directory,
          sourceCommit: "a".repeat(40),
          cleanupRequired: true,
          cleanupComplete: false,
        };

        const deployment = yield* makePerformanceDeployment(undefined);

        return yield* deployment.remove(target).pipe(Effect.exit);
      }).pipe(
        Effect.provideService(HttpClient.HttpClient, http),
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
        Effect.scoped,
        Effect.provide(
          Layer.merge(
            NodeServices.layer,
            ConfigProvider.layer(
              ConfigProvider.fromUnknown({
                CLOUDFLARE_ACCOUNT_ID: "b".repeat(32),
                CLOUDFLARE_API_TOKEN: "test-token",
                CLOUDFLARE_WORKERS_SUBDOMAIN: "test",
                PATH: "/unused",
                HOME: "/unused",
              }),
            ),
          ),
        ),
      ),
    );

    expect(actions).toEqual(
      mode === "forbidden"
        ? ["GET", "retire-namespace", "DELETE"]
        : ["GET", "retire-namespace", "DELETE", "GET"],
    );
    expect(Exit.isSuccess(exit)).toBe(mode === "deleted" || mode === "already-gone");
  },
);

import { ThreadMaintenance } from "@effect-agent/platform-cloudflare/Alarm";
import * as ThreadObject from "@effect-agent/platform-cloudflare/ThreadObject";
import { Effect, Layer, Schema } from "effect";
import { DurableObject, type WorkerEnvironment } from "effect-cf";

import { authority, handlers, registrations, rootThread, type Models } from "./agents.ts";
import { Command, CommandResult, execute, Snapshot, snapshot } from "./application.ts";

export const threadLayer = (models: Models, modelVersion: string) =>
  ThreadObject.layer(registrations(models, modelVersion)).pipe(
    Layer.provide([authority, handlers(models)]),
  );

type ApplicationLayer = ReturnType<typeof threadLayer>;

export const makeOrchestrationThread = <E>(
  application: Layer.Layer<
    Layer.Success<ApplicationLayer>,
    E,
    Layer.Services<ApplicationLayer> | WorkerEnvironment
  >,
) =>
  class extends ThreadObject.make(application, {
    namespaceBinding: "THREADS",
    deploymentId: "durable-orchestration-v1",
    producerPrefix: "cloudflare-orchestration",
    wakeScanInterval: 100,
    settlementPollInterval: 25,
    alarmBackoffBase: 25,
    alarmBackoffCap: 1_000,
  }) {
    command(encoded: unknown) {
      return this[DurableObject.RunSymbol](
        Effect.gen(function* () {
          const command = yield* Schema.decodeUnknownEffect(Command)(encoded);
          const maintenance = yield* ThreadMaintenance;
          const result = yield* maintenance.withMutation(execute(command));

          return JSON.stringify(yield* Schema.encodeEffect(CommandResult)(result));
        }),
      );
    }
    status() {
      return this[DurableObject.RunSymbol](
        snapshot.pipe(Effect.flatMap(Schema.encodeEffect(Snapshot)), Effect.map(JSON.stringify)),
      );
    }
  };

/** Authenticated ingress; models never choose destination Thread IDs or authorization. */
export default {
  fetch(request: Request, env: Cloudflare.Env): Promise<Response> {
    return Effect.runPromise(
      Effect.gen(function* () {
        if (!env.DEMO_TOKEN || request.headers.get("authorization") !== `Bearer ${env.DEMO_TOKEN}`)
          return new Response("Unauthorized", { status: 401 });
        const object = env.THREADS.getByName(rootThread);
        const path = new URL(request.url).pathname;

        if (path === "/status" && request.method === "GET")
          return new Response(yield* Effect.tryPromise(() => object.status()), {
            headers: { "content-type": "application/json" },
          });
        if (path === "/command" && request.method === "POST") {
          const body = yield* Effect.tryPromise(() => request.json());
          const command = yield* Schema.decodeUnknownEffect(Command)(body);

          return new Response(yield* Effect.tryPromise(() => object.command(command)), {
            status: 202,
            headers: { "content-type": "application/json" },
          });
        }

        return new Response("Not found", { status: 404 });
      }).pipe(
        Effect.catchTag("SchemaError", () =>
          Effect.succeed(new Response("Invalid request", { status: 400 })),
        ),
        Effect.tapError((error) => Effect.logError(error)),
        Effect.catch(() => Effect.succeed(new Response("Request failed", { status: 500 }))),
      ),
    );
  },
};

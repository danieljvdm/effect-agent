import { ThreadMaintenance } from "@effect-agent/platform-cloudflare/Alarm";
import * as ThreadObject from "@effect-agent/platform-cloudflare/ThreadObject";
import { Effect, Layer, Schema } from "effect";
import { DurableObject } from "effect-cf";

import { authority, handlers, registrations, rootThread } from "./agents.ts";
import { Command, CommandResult, execute, Snapshot, snapshot } from "./application.ts";

declare global {
  namespace Cloudflare {
    interface Env {
      THREADS: DurableObjectNamespace<OrchestrationThread>;
      DEMO_TOKEN: string;
    }
  }
}

export class OrchestrationThread extends ThreadObject.make(
  ThreadObject.layer(registrations).pipe(Layer.provide([authority, handlers])),
  {
    namespaceBinding: "THREADS",
    deploymentId: "durable-orchestration-v1",
    producerPrefix: "cloudflare-orchestration",
    wakeScanInterval: 100,
    settlementPollInterval: 25,
    alarmBackoffBase: 25,
    alarmBackoffCap: 1_000,
  },
) {
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
}

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

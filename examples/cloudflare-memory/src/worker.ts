import { Memory } from "@effect-agent/capabilities";
import { MemoryAccess } from "@effect-agent/core";
import {
  MemoryObject,
  CloudflareMemoryClient,
  ThreadObject,
} from "@effect-agent/platform-cloudflare";
import {
  MemoryOwnerAuthorizer,
  MemoryOwnerIdentity,
  MemoryRpcError,
  memoryWireBytes,
} from "@effect-agent/storage-cloudflare";
import { Principal } from "@effect-agent/thread";
import { Clock, Effect, Layer, Schema } from "effect";
import { DurableObject, WorkerEnvironment } from "effect-cf";

import {
  BenchmarkCase,
  candidates,
  command,
  limits,
  memoryScope,
  Projects,
  Sample,
  sourceCount,
} from "./contracts.ts";

declare global {
  namespace Cloudflare {
    interface Env {
      MEMORIES: DurableObjectNamespace<ProjectMemory>;
      THREADS: DurableObjectNamespace<BenchmarkThread>;
      BENCH_TOKEN: string;
    }
  }
}

const policy = Layer.effect(
  MemoryOwnerAuthorizer,
  Effect.gen(function* () {
    const { namespace } = yield* MemoryOwnerIdentity;
    const project = yield* Projects.restore(namespace.address);

    yield* Schema.decodeUnknownEffect(BenchmarkCase)(project.identity);

    return {
      authorize: (request) =>
        request.principal === "benchmark" && request.access.scope === "benchmark"
          ? Effect.void
          : Effect.fail(MemoryRpcError.make({ reason: "denied" })),
    };
  }),
);

const placement = () =>
  Effect.tryPromise({
    try: () =>
      fetch("https://www.cloudflare.com/cdn-cgi/trace").then((response) => response.text()),
    catch: () => MemoryRpcError.make({ reason: "unavailable" }),
  }).pipe(
    Effect.map(
      (trace) =>
        trace
          .split("\n")
          .find((line) => line.startsWith("colo="))
          ?.slice(5) ?? "unknown",
    ),
  );

export class ProjectMemory extends MemoryObject.make(policy) {
  placement() {
    return this[DurableObject.RunSymbol](placement());
  }
}

const memoryClient = Effect.fn("benchmark.client")(function* (name: BenchmarkCase) {
  const env = yield* WorkerEnvironment;

  return yield* CloudflareMemoryClient.fromBinding(env.MEMORIES, {
    access: MemoryAccess.make({ namespace: Projects.make(name), scope: memoryScope }),
    principal: Principal.make("benchmark"),
  });
});

/** Real Thread host; no models are registered or invoked by this memory-only benchmark. */
export class BenchmarkThread extends ThreadObject.make(ThreadObject.layer([]), {
  namespaceBinding: "THREADS",
  deploymentId: "memory-benchmark",
  producerPrefix: "memory-benchmark",
}) {
  seed(name: BenchmarkCase) {
    return this[DurableObject.RunSymbol](
      Effect.gen(function* () {
        name = yield* Schema.decodeUnknownEffect(BenchmarkCase)(name);
        const client = yield* memoryClient(name);

        for (let i = 0; i < sourceCount(name); i++) yield* client.change(command(name, i));

        return true;
      }),
    );
  }

  sample(name: BenchmarkCase) {
    return this[DurableObject.RunSymbol](
      Effect.gen(function* () {
        name = yield* Schema.decodeUnknownEffect(BenchmarkCase)(name);
        const client = yield* memoryClient(name);
        const lookup = candidates(name);
        let validatedBytes = 0;
        let renderedBytes = 0;
        let validationRpcMillis = 0;
        const start = yield* Clock.currentTimeMillis;

        const result = yield* Effect.gen(function* () {
          const current = yield* client.revalidate(lookup, limits);

          validationRpcMillis = (yield* Clock.currentTimeMillis) - start;
          validatedBytes = memoryWireBytes(JSON.stringify(current));

          const recalled = yield* Memory.recall(
            [{ id: "project", essential: true, read: Effect.succeed(current) }],
            limits,
          );

          renderedBytes = recalled.bytes;
        }).pipe(Effect.result);

        const elapsed = (yield* Clock.currentTimeMillis) - start;

        return yield* Schema.encodeEffect(Sample)({
          case: name,
          sourceCount: sourceCount(name),
          candidateCount: lookup._tag === "Found" ? lookup.passages.length : 0,
          corpusTextBytes: sourceCount(name) * 1024,
          candidateBytes: memoryWireBytes(JSON.stringify(lookup)),
          validatedBytes,
          renderedBytes,
          validationRpcMillis,
          fullRecallMillis: elapsed,
          status:
            result._tag === "Success"
              ? "ok"
              : "reason" in result.failure && result.failure.reason === "timeout"
                ? "timeout"
                : "error",
          errorTag: result._tag === "Success" ? null : result.failure._tag,
        });
      }),
    );
  }
  placement() {
    return this[DurableObject.RunSymbol](placement());
  }
}

export default {
  fetch(request: Request, env: Cloudflare.Env): Promise<Response> {
    return Effect.runPromise(
      Effect.gen(function* () {
        if (
          !env.BENCH_TOKEN ||
          request.headers.get("authorization") !== `Bearer ${env.BENCH_TOKEN}`
        )
          return new Response("Unauthorized", { status: 401 });
        const url = new URL(request.url);
        const name = yield* Schema.decodeUnknownEffect(BenchmarkCase)(url.searchParams.get("case"));
        const caller = url.searchParams.get("caller") === "b" ? "b" : "a";
        const thread = env.THREADS.getByName(`benchmark-${name}-${caller}`);

        if (url.pathname === "/seed")
          return Response.json(yield* Effect.promise(() => thread.seed(name)));
        if (url.pathname === "/sample")
          return Response.json(yield* Effect.promise(() => thread.sample(name)));
        if (url.pathname === "/placement")
          return Response.json({
            callerEgressColo: yield* Effect.promise(() => thread.placement()),
            ownerEgressColo: yield* Effect.promise(() =>
              env.MEMORIES.getByName(Projects.make(name).address).placement(),
            ),
            ingressColo: request.cf?.colo ?? "unknown",
            placementHint: "automatic",
          });

        return new Response("Not found", { status: 404 });
      }).pipe(
        Effect.scoped,
        Effect.catch(() => Effect.succeed(new Response("Invalid request", { status: 400 }))),
      ),
    );
  },
};

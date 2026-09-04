import { Deferred, Effect, Fiber, FileSystem, Schedule, Schema } from "effect";
import { HttpClient, HttpClientResponse } from "effect/unstable/http";
import { Socket } from "effect/unstable/socket";
import { convertV4MiniflareOptions, Log, LogLevel, Miniflare } from "miniflare";

import { HeapProbeError, HeapUsage, ObjectStatus, WorkerdSample } from "./heap-contracts.ts";

const Message = Schema.Struct({
  id: Schema.optionalKey(Schema.Number),
  result: Schema.optionalKey(Schema.Unknown),
  error: Schema.optionalKey(Schema.Struct({ message: Schema.String })),
});

const Targets = Schema.Array(
  Schema.Struct({
    id: Schema.String,
    webSocketDebuggerUrl: Schema.String,
  }),
);

const inspector = Effect.fn("heap.inspector")(function* (url: string, method: string) {
  const response = yield* Deferred.make<unknown, HeapProbeError>();
  const socket = yield* Socket.makeWebSocket(url);
  const write = yield* socket.writer;

  yield* socket
    .runString(
      (text) =>
        Effect.gen(function* () {
          const message = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Message))(text);

          if (message.id !== 1) return;
          if (message.error)
            yield* Deferred.fail(
              response,
              HeapProbeError.make({ operation: `${method}: ${message.error.message}` }),
            );
          else yield* Deferred.succeed(response, message.result);
        }).pipe(
          Effect.catch((cause) =>
            Deferred.fail(
              response,
              HeapProbeError.make({ operation: "decode inspector response", cause }),
            ),
          ),
        ),
      {
        onOpen: write(JSON.stringify({ id: 1, method })).pipe(
          Effect.catch((cause) =>
            Deferred.fail(
              response,
              HeapProbeError.make({ operation: "write inspector request", cause }),
            ),
          ),
          Effect.asVoid,
        ),
      },
    )
    .pipe(
      Effect.catch((cause) =>
        Deferred.fail(response, HeapProbeError.make({ operation: method, cause })),
      ),
      Effect.forkScoped,
    );

  return yield* Deferred.await(response).pipe(
    Effect.timeout("10 seconds"),
    Effect.mapError((cause) =>
      HeapProbeError.make({ operation: `inspector command ${method}`, cause }),
    ),
  );
}, Effect.scoped);

const request = (runtime: Miniflare, route: string, id?: string) =>
  Effect.tryPromise({
    try: (signal) =>
      runtime.dispatchFetch(`http://benchmark/${route}${id ? `?id=${id}` : ""}`, {
        headers: { authorization: "Bearer local-heap-probe" },
        signal,
      }),
    catch: (cause) => HeapProbeError.make({ operation: route, cause }),
  }).pipe(
    Effect.flatMap((response) =>
      response.ok
        ? Effect.tryPromise({
            try: () => response.json(),
            catch: (cause) => HeapProbeError.make({ operation: `decode ${route}`, cause }),
          })
        : Effect.fail(HeapProbeError.make({ operation: `${route} returned ${response.status}` })),
    ),
  );

export const measureWorkerd = Effect.fn("heap.measureWorkerd")(
  function* (bundle: string, count: number) {
    const fs = yield* FileSystem.FileSystem;
    const script = yield* fs.readFileString(bundle);

    const runtime = yield* Effect.acquireRelease(
      Effect.try({
        try: () =>
          new Miniflare(
            convertV4MiniflareOptions({
              name: "heap-benchmark",
              modules: true,
              script,
              compatibilityDate: "2026-08-01",
              compatibilityFlags: ["nodejs_compat"],
              inspectorPort: 0,
              log: new Log(LogLevel.ERROR),
              bindings: { BENCH_TOKEN: "local-heap-probe" },
              durableObjects: { HEAP_THREADS: { className: "HeapThread", useSQLite: true } },
            }),
          ),
        catch: (cause) => HeapProbeError.make({ operation: "open local workerd", cause }),
      }),
      (runtime) => Effect.promise(() => runtime.dispose()),
    );

    yield* Effect.tryPromise({
      try: () => runtime.ready,
      catch: (cause) => HeapProbeError.make({ operation: "workerd ready", cause }),
    });

    const url = yield* Effect.tryPromise({
      try: () => runtime.getInspectorURL(),
      catch: (cause) => HeapProbeError.make({ operation: "inspector URL", cause }),
    });

    url.protocol = "http:";
    url.pathname = "/json";

    const targets = yield* HttpClient.get(url).pipe(
      Effect.flatMap(HttpClientResponse.schemaBodyJson(Targets)),
    );

    const target = targets.find((target) => target.id.includes("heap-benchmark"));

    if (!target)
      return yield* HeapProbeError.make({ operation: "find heap-benchmark inspector target" });

    const readHeap = inspector(target.webSocketDebuggerUrl, "Runtime.getHeapUsage").pipe(
      Effect.flatMap(Schema.decodeUnknownEffect(HeapUsage)),
    );

    yield* request(runtime, "ready");
    const startup = yield* readHeap;
    const ids = Array.from({ length: count }, (_, i) => `object-${i}`);

    const statuses = Effect.forEach(
      ids,
      (id) =>
        request(runtime, "status", id).pipe(
          Effect.flatMap(Schema.decodeUnknownEffect(ObjectStatus)),
        ),
      { concurrency: count },
    );

    yield* statuses;
    const initialized = yield* readHeap;

    const running = yield* Effect.forEach(
      ids,
      (id) =>
        request(runtime, "run", id).pipe(
          Effect.flatMap(Schema.decodeUnknownEffect(Schema.Literal("completed"))),
        ),
      { concurrency: count },
    ).pipe(Effect.forkScoped);

    yield* statuses.pipe(
      Effect.repeat({
        until: (items) => items.every((item) => item.active && item.toolCalls === 2),
        schedule: Schedule.spaced("20 millis"),
      }),
      Effect.timeout("30 seconds"),
    );
    const active = yield* readHeap;

    yield* Effect.forEach(ids, (id) => request(runtime, "release", id), { concurrency: count });
    yield* Fiber.join(running);
    const settled = yield* readHeap;
    const objects = yield* statuses;

    if (
      !objects.every(
        (object) => !object.active && object.modelCalls === 2 && object.toolCalls === 2,
      )
    )
      return yield* HeapProbeError.make({
        operation: "expected two model calls and two tools per completed Object",
      });

    return WorkerdSample.make({ startup, initialized, active, settled, objects });
  },
  Effect.scoped,
  Effect.timeout("60 seconds"),
);

import { Effect, Exit, Layer, Schema, Scope, Stream } from "effect";
import { HttpRouter } from "effect/unstable/http";
import { RpcSerialization, RpcServer } from "effect/unstable/rpc";

import type { AccessSession } from "../access-domain.ts";
import { PlannerError, PlannerProgress, ProgressRpcs } from "../domain.ts";
import { plannerOwner, privateConversation } from "./tenancy.ts";

export interface ProgressEnvironment {
  readonly THREADS: {
    readonly getByName: (name: string) => { readonly plannerProgress: () => Promise<string> };
  };
}

const unavailable = () =>
  new PlannerError({
    code: "unavailable",
    message: "Live progress is temporarily unavailable. Your planner can continue working.",
  });

/** Replacement frames tolerate skipped polls and reconnects without replaying text deltas. */
export const watchProgress = (read: Effect.Effect<PlannerProgress, PlannerError>) =>
  Stream.fromEffect(read).pipe(
    Stream.concat(Stream.fromEffectRepeat(Effect.sleep("200 millis").pipe(Effect.andThen(read)))),
    Stream.changesWith(
      (previous, next) =>
        previous.revision === next.revision &&
        previous.attemptId === next.attemptId &&
        previous.submissionId === next.submissionId,
    ),
  );

/** Transfer the handler lifetime to its body; completion, errors and cancellation all dispose it. */
const transferResponse = (response: Response, close: () => Promise<void>): Response => {
  const reader = response.body?.getReader();
  let closing: Promise<void> | undefined;
  const finish = () => (closing ??= close().finally(() => reader?.releaseLock()));

  const body = new ReadableStream<Uint8Array>({
    async pull(controller) {
      try {
        const next = await reader?.read();

        if (next === undefined || next.done) {
          await finish();
          controller.close();
        } else controller.enqueue(next.value);
      } catch (error) {
        await finish();
        controller.error(error);
      }
    },
    async cancel(reason) {
      try {
        await reader?.cancel(reason);
      } finally {
        await finish();
      }
    },
  });

  const headers = new Headers(response.headers);

  headers.set("cache-control", "no-store");

  return new Response(body, { status: response.status, headers });
};

/** Worker ingress has already verified the session and bounded the RPC request body. */
export const serveProgress = Effect.fn("serveProgress")(
  function* (request: Request, env: ProgressEnvironment, session: AccessSession) {
    const scope = yield* Scope.make();

    const response = yield* Effect.gen(function* () {
      const owner = yield* plannerOwner(session.email);

      const handlers = ProgressRpcs.toLayer({
        WatchProgress: ({ conversationId }) =>
          Stream.unwrap(
            Effect.gen(function* () {
              const id = yield* privateConversation(owner, conversationId);

              const read = Effect.tryPromise({
                try: () => env.THREADS.getByName(id).plannerProgress(),
                catch: unavailable,
              }).pipe(
                Effect.flatMap(Schema.decodeUnknownEffect(Schema.fromJsonString(PlannerProgress))),
                Effect.mapError(unavailable),
              );

              return watchProgress(read);
            }),
          ),
      });

      const web = yield* Effect.acquireRelease(
        Effect.sync(() =>
          HttpRouter.toWebHandler(
            RpcServer.layerHttp({
              group: ProgressRpcs,
              path: "/api/progress",
              protocol: "http",
              concurrency: 1,
            }).pipe(Layer.provide(handlers), Layer.provide(RpcSerialization.layerNdjson)),
            { disableLogger: true },
          ),
        ),
        (handler) => Effect.promise(() => handler.dispose()),
      );

      return yield* Effect.promise(() => web.handler(request));
    }).pipe(
      Effect.provideService(Scope.Scope, scope),
      Effect.onExit((exit) => (Exit.isFailure(exit) ? Scope.close(scope, exit) : Effect.void)),
    );

    return transferResponse(response, () => Effect.runPromise(Scope.close(scope, Exit.void)));
  },
  Effect.catchDefect(() =>
    Effect.succeed(
      new Response("Live progress is temporarily unavailable.", {
        status: 503,
        headers: { "cache-control": "no-store" },
      }),
    ),
  ),
);

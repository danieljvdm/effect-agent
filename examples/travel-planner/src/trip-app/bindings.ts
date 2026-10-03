import type { Sandbox } from "@cloudflare/sandbox";
import { R2Error } from "alchemy/Cloudflare/R2/BucketTypes";
import { Context, Effect, Layer } from "effect";

import { PlannerError } from "../domain.ts";
import { plannerEnvironment } from "../server/alchemy.ts";
import { AppBuildBucket } from "./bucket.ts";

export const AppBuildBucketLive = Layer.effect(AppBuildBucket)(
  Effect.gen(function* () {
    const env = yield* plannerEnvironment;

    if (!env.APP_BUILDS)
      return yield* new PlannerError({
        code: "unavailable",
        message: "App build storage isn't configured.",
      });

    const bucket = env.APP_BUILDS;

    const request = <A>(run: () => Promise<A>) =>
      Effect.tryPromise({
        try: run,
        catch: (cause) =>
          new R2Error({
            message: cause instanceof Error ? cause.message : "App build storage is unavailable.",
            cause: cause instanceof Error ? cause : new Error("R2 operation failed", { cause }),
          }),
      });

    return AppBuildBucket.of({
      get: (key) =>
        request(() => bucket.get(key)).pipe(
          Effect.map((object) =>
            object === null
              ? null
              : {
                  size: object.size,
                  get bodyUsed() {
                    return object.bodyUsed;
                  },
                  // The caller's Scope consumes or cancels this native body.
                  readable: object.body,
                  text: () => request(() => object.text()),
                },
          ),
        ),
      head: (key) => request(() => bucket.head(key)),
      put: (key, value, options) =>
        request(() => bucket.put(key, value, options)).pipe(Effect.asVoid),
    });
  }),
);

export class AppBuildSandbox extends Context.Service<
  AppBuildSandbox,
  DurableObjectNamespace<Sandbox>
>()("trip-app/AppBuildSandbox") {}

export const AppBuildSandboxLive = Layer.effect(AppBuildSandbox)(
  Effect.gen(function* () {
    const env = yield* plannerEnvironment;

    if (!env.APP_SANDBOX)
      return yield* new PlannerError({
        code: "unavailable",
        message: "The app builder isn't configured.",
      });

    return env.APP_SANDBOX;
  }),
);

import type { Sandbox } from "@cloudflare/sandbox";
import { makeBucketClient } from "alchemy/Cloudflare/R2/ReadWriteBucketBinding";
import type { WorkflowHandle } from "alchemy/Cloudflare/Workflows";
import { makeWorkflowClient } from "alchemy/Cloudflare/Workflows";
import { Context, Effect, Layer } from "effect";

import { type AppBuildRequest, PlannerError } from "../domain.ts";
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

    // The DOM and module declarations describe the same native Cloudflare binding.
    return makeBucketClient(env.APP_BUILDS as unknown as Parameters<typeof makeBucketClient>[0]);
  }),
);

export class SiteBuildBinding extends Context.Service<
  SiteBuildBinding,
  WorkflowHandle<AppBuildRequest, { readonly commitId: string }>
>()("trip-app/SiteBuild") {}

export const SiteBuildBindingLive = Layer.effect(SiteBuildBinding)(
  Effect.gen(function* () {
    const env = yield* plannerEnvironment;

    if (!env.SITE_BUILD)
      return yield* new PlannerError({
        code: "unavailable",
        message: "The app builder isn't configured.",
      });

    return makeWorkflowClient<AppBuildRequest, { readonly commitId: string }>(
      env.SITE_BUILD,
      "SiteBuild",
    );
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

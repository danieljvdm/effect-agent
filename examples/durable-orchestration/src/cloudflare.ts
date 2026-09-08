import { ConfigProvider, Effect, Layer } from "effect";
import { WorkerEnvironment } from "effect-cf";

import { makeOrchestrationThread, threadLayer } from "./cloudflare-host.ts";
import { openAiModels } from "./openai.ts";

export { default } from "./cloudflare-host.ts";

declare global {
  namespace Cloudflare {
    interface Env {
      THREADS: DurableObjectNamespace<OrchestrationThread>;
      DEMO_TOKEN: string;
      OPENAI_API_KEY: string;
      OPENAI_MODEL?: string;
    }
  }
}

export class OrchestrationThread extends makeOrchestrationThread(
  Layer.unwrap(
    Effect.gen(function* () {
      const env = yield* WorkerEnvironment;

      const { models, modelVersion } = yield* openAiModels.pipe(
        Effect.provideService(
          ConfigProvider.ConfigProvider,
          ConfigProvider.fromUnknown({
            OPENAI_API_KEY: env.OPENAI_API_KEY,
            OPENAI_MODEL: env.OPENAI_MODEL,
          }),
        ),
      );

      return threadLayer(models, modelVersion);
    }),
  ),
) {}

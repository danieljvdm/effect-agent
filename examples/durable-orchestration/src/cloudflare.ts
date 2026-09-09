import { Effect, Layer } from "effect";

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
    Effect.map(openAiModels, ({ models, modelVersion }) => threadLayer(models, modelVersion)),
  ),
) {}

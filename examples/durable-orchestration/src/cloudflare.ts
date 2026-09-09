import { Effect, Layer } from "effect";

import * as CloudflareHost from "./CloudflareHost.ts";
import { openAiModels } from "./openai.ts";

export { default } from "./CloudflareHost.ts";

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

export class OrchestrationThread extends CloudflareHost.make(
  Layer.unwrap(
    Effect.map(openAiModels, ({ models, modelVersion }) =>
      CloudflareHost.layer(models, modelVersion),
    ),
  ),
) {}

import { makeOrchestrationThread, threadLayer } from "../../src/cloudflare-host.ts";
import { models } from "./models.ts";

export { default } from "../../src/cloudflare-host.ts";

export class OrchestrationThread extends makeOrchestrationThread(
  threadLayer(models, "scripted-v1"),
) {}

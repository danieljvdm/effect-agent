import * as CloudflareHost from "../../src/CloudflareHost.ts";
import { models } from "./models.ts";

export { default } from "../../src/CloudflareHost.ts";

export class OrchestrationThread extends CloudflareHost.make(
  CloudflareHost.layer(models, "scripted-v1"),
) {}

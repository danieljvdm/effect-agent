import * as Host from "@effect-agent/platform-cloudflare/cloudflare-thread-client-host";
import { Layer } from "effect";

import * as Rpc from "./Rpc.ts";

export * from "@effect-agent/platform-cloudflare/cloudflare-thread-client-host";

/** Alchemy clients resolve native targets inside the current Rpc.withScope invocation. */
export class CloudflareThreadClient extends Host.CloudflareThreadClient {
  static override readonly layer = Host.CloudflareThreadClient.layer.pipe(Layer.provide(Rpc.layer));

  static override layerFromBinding(
    ...args: Parameters<typeof Host.CloudflareThreadClient.layerFromBinding>
  ) {
    return Host.CloudflareThreadClient.layerFromBinding(...args).pipe(Layer.provide(Rpc.layer));
  }
}

import { BrowserCrypto } from "@effect/platform-browser";
import { Layer } from "effect";

import { ThreadObjectNamespace, type ThreadObjectRpc } from "./CloudflareHostBindings.ts";
import * as Host from "./CloudflareThreadClientHost.ts";
import { effectCfRpcLayer } from "./internal/effect-cf-rpc.ts";

export * from "./CloudflareThreadClientHost.ts";

/** Native effect-cf client with invocation-scoped target reuse and opt-in tracing. */
export class CloudflareThreadClient extends Host.CloudflareThreadClient {
  static override readonly layer = Host.CloudflareThreadClient.layer.pipe(
    Layer.provide(effectCfRpcLayer),
  );

  static override layerFromBinding(options: {
    readonly namespace: DurableObjectNamespace<ThreadObjectRpc>;
    readonly rpcTracing?: string;
  }) {
    return CloudflareThreadClient.layer.pipe(
      Layer.provide([ThreadObjectNamespace.layer(options.namespace, options), BrowserCrypto.layer]),
    );
  }
}

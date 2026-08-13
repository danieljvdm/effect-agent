import { Context, Layer } from "effect";
import { FetchHttpClient } from "effect/unstable/http";
import * as RpcClient from "effect/unstable/rpc/RpcClient";
import type { RpcClientError } from "effect/unstable/rpc/RpcClientError";
import type * as RpcGroup from "effect/unstable/rpc/RpcGroup";
import * as RpcSerialization from "effect/unstable/rpc/RpcSerialization";

import { DemoRunRpcs } from "./run-rpc";

type DemoRunRpcClientApi = RpcClient.RpcClient<RpcGroup.Rpcs<typeof DemoRunRpcs>, RpcClientError>;

/** Generated browser client for the shared interactive operational RPC definitions. */
export class DemoRunRpcClient extends Context.Service<DemoRunRpcClient, DemoRunRpcClientApi>()(
  "@effect-agent/example-demo/DemoRunRpcClient",
) {
  static readonly layer = Layer.effect(this)(RpcClient.make(DemoRunRpcs)).pipe(
    Layer.provide(RpcClient.layerProtocolHttp({ url: "/api/rpc" })),
    Layer.provide([RpcSerialization.layerNdjson, FetchHttpClient.layer]),
  );
}

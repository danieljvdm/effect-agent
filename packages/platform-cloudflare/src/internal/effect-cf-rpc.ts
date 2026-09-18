import { Layer } from "effect";
import { RpcTargets, RpcTracing } from "effect-cf";

import { RpcStrategy } from "../CloudflareRpc.ts";

export const effectCfRpcLayer = Layer.succeed(RpcStrategy, {
  get: RpcTargets.get,
  invalidate: RpcTargets.invalidate,
  traceArguments: RpcTracing.withRpcTraceContext([]),
  withClientSpan: RpcTracing.withRpcClientSpan,
});

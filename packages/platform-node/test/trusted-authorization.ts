import {
  possessionChildAdmissionAuthorizerLayer,
  possessionOperationAuthorizerLayer,
} from "@effect-agent/session";
import { Layer } from "effect";

import {
  NodeDurableHost,
  NodeDurableRuntime,
  type NodeDurableRuntimeOptions,
} from "../src/index.ts";

/** Explicit trusted-local policy chosen at platform test composition roots. */
export const trustedAuthorizationLayer = Layer.merge(
  possessionOperationAuthorizerLayer,
  possessionChildAdmissionAuthorizerLayer,
);

export const trustedRuntimeLayer = (options: NodeDurableRuntimeOptions) =>
  NodeDurableRuntime.layer(options).pipe(Layer.provide(trustedAuthorizationLayer));

export const trustedHostLayer = (options: NodeDurableRuntimeOptions) =>
  NodeDurableHost.layerStack(options).pipe(Layer.provide(trustedAuthorizationLayer));

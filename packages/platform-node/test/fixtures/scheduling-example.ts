import {
  type DurableSubmitAgent,
  type ResolvedBinding,
  type ScheduleCreateOptions,
  Scheduling,
} from "@effect-agent/session";
import { Effect, Layer, type Schema } from "effect";

import {
  NodeDurableHost,
  type NodeDurableRuntimeOptions,
  NodeScheduling,
} from "../../src/index.ts";

/** The caller supplies registered bindings, their real digests, and an explicit authorizer. */
export const schedulingRuntimeLayer = (
  bindings: ReadonlyArray<ResolvedBinding>,
  runtimeOptions: NodeDurableRuntimeOptions,
) =>
  NodeScheduling.layer().pipe(
    Layer.provideMerge(NodeDurableHost.layerStack({ ...runtimeOptions, bindings })),
  );

/** Provide the runtime and application authorizer Layers once around this process workflow. */
export const runScheduledHost = Effect.fn("Example.runScheduledHost")(function* <
  InputSchema extends Schema.Top,
>(
  agent: DurableSubmitAgent<InputSchema>,
  input: InputSchema["Type"],
  options: ScheduleCreateOptions,
) {
  const scheduling = yield* Scheduling;
  const host = yield* NodeDurableHost;
  yield* scheduling.create(agent, input, options);
  return yield* host.runResolvedWorkers;
});

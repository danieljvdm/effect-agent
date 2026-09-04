import { type NodeDurableAgentRuntimeOptions } from "@effect-agent/platform-node/NodeDurableAgentRuntime";
import { NodeDurableHost } from "@effect-agent/platform-node/NodeDurableHost";
import { NodeScheduling } from "@effect-agent/platform-node/NodeScheduling";
import { type ResolvedBinding } from "@effect-agent/thread/AgentRegistration";
import { type DurableSubmitAgent } from "@effect-agent/thread/DurableAgentRuntime";
import { type ScheduleCreateOptions, Scheduling } from "@effect-agent/thread/Scheduling";
import { Effect, Layer, type Schema } from "effect";

/** The caller supplies registered bindings, their real digests, and an explicit authorizer. */
export const schedulingRuntimeLayer = (
  bindings: ReadonlyArray<ResolvedBinding>,
  runtimeOptions: NodeDurableAgentRuntimeOptions,
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

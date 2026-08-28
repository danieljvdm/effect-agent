import {
  type DurableSubmitAgent,
  ScheduleAuthorizer,
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
  runtimeOptions: NodeDurableRuntimeOptions,
  authorizer: ScheduleAuthorizer["Service"],
) =>
  NodeScheduling.layer().pipe(
    Layer.provideMerge(NodeDurableHost.layerStack(runtimeOptions)),
    Layer.provide(Layer.succeed(ScheduleAuthorizer)(authorizer)),
  );

/** Creating the Schedule and serving admitted inputs share the caller-owned process Scope. */
export const runScheduledHost = <InputSchema extends Schema.Top>(
  runtimeOptions: NodeDurableRuntimeOptions,
  authorizer: ScheduleAuthorizer["Service"],
  agent: DurableSubmitAgent<InputSchema>,
  input: InputSchema["Type"],
  options: ScheduleCreateOptions,
) =>
  Effect.gen(function* () {
    const scheduling = yield* Scheduling;
    const host = yield* NodeDurableHost;
    yield* scheduling.create(agent, input, options);
    return yield* host.runResolvedWorkers;
  }).pipe(Effect.provide(schedulingRuntimeLayer(runtimeOptions, authorizer)));

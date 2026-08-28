import {
  type ScheduleAuthorizer,
  type DurableSubmitAgent,
  type ScheduleCreateOptions,
} from "@effect-agent/session";
import { Effect, Layer, type Schema } from "effect";

import {
  CloudflareSchedulingClient,
  ScheduleOwnerNamespace,
  makeScheduleOwnerObjectClass,
  type ConversationObjectRpc,
  type ScheduleOwnerObjectRpc,
} from "../src/index.ts";

export interface SchedulingWorkerEnv {
  readonly CONVERSATIONS: DurableObjectNamespace<ConversationObjectRpc>;
  readonly SCHEDULES: DurableObjectNamespace<ScheduleOwnerObjectRpc>;
}

/** Export the returned class from the Worker and bind it as a SQLite Durable Object. */
export const makeSchedulingOwner = (authorizer: ScheduleAuthorizer["Service"]) =>
  makeScheduleOwnerObjectClass({
    conversationNamespaceBinding: "CONVERSATIONS",
    authorizer,
  });

/** Provide this Layer to Worker-side schedule management Effects. */
export const schedulingClientLayer = (env: SchedulingWorkerEnv) =>
  CloudflareSchedulingClient.layer.pipe(Layer.provide(ScheduleOwnerNamespace.layer(env.SCHEDULES)));

/** Input encoding requirements stay visible to the Worker caller. */
export const createSchedule = <InputSchema extends Schema.Top>(
  env: SchedulingWorkerEnv,
  agent: DurableSubmitAgent<InputSchema>,
  input: InputSchema["Type"],
  options: ScheduleCreateOptions,
) =>
  Effect.gen(function* () {
    const scheduling = yield* CloudflareSchedulingClient;
    return yield* scheduling.create(agent, input, options);
  }).pipe(Effect.provide(schedulingClientLayer(env)));

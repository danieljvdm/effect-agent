import {
  type ScheduleAuthorizer,
  type DurableSubmitAgent,
  type ScheduleCreateOptions,
} from "@effect-agent/session";
import { Effect, Layer, type Schema } from "effect";
import { WorkerEnvironment } from "effect-cf";

import {
  CloudflareSchedulingClient,
  ConversationObjectNamespace,
  ScheduleOwnerNamespace,
  makeScheduleOwnerObjectClass,
} from "../src/index.ts";

/** Export the returned class; the application policy Layer owns its dependencies. */
export const makeSchedulingOwner = <E>(
  authorizer: Layer.Layer<ScheduleAuthorizer, E, WorkerEnvironment>,
) =>
  makeScheduleOwnerObjectClass(
    Layer.merge(
      authorizer,
      Layer.effect(
        ConversationObjectNamespace,
        Effect.map(WorkerEnvironment, (env) => ({ namespace: env.CONVERSATIONS })),
      ),
    ),
  );

/** The native Worker entry point provides WorkerEnvironment once. */
export const schedulingClientLayer = CloudflareSchedulingClient.layer.pipe(
  Layer.provide(
    Layer.effect(
      ScheduleOwnerNamespace,
      Effect.map(WorkerEnvironment, (env) => ({ namespace: env.SCHEDULES })),
    ),
  ),
);

export const scheduleDailyReport = Effect.fn("Example.scheduleDailyReport")(function* <
  InputSchema extends Schema.Top,
>(
  agent: DurableSubmitAgent<InputSchema>,
  input: InputSchema["Type"],
  options: Omit<ScheduleCreateOptions, "timing">,
) {
  const scheduling = yield* CloudflareSchedulingClient;
  return yield* scheduling.create(agent, input, {
    ...options,
    timing: { _tag: "Cron", expression: "0 8 * * *", timeZone: "UTC" },
  });
});

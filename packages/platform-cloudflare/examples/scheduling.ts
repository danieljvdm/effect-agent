import {
  type ScheduleAuthorizer,
  type DurableSubmitAgent,
  type ScheduleCreateOptions,
  Scheduling,
} from "@effect-agent/thread";
import { Effect, Layer, type Schema } from "effect";
import { WorkerEnvironment } from "effect-cf";

import {
  CloudflareSchedulingClient,
  ThreadObjectNamespace,
  ScheduleOwnerNamespace,
  makeScheduleOwnerObjectClass,
} from "../src/index.ts";

/**
 * Export the returned class. The cached policy Layer must not acquire resources that need
 * cleanup on eviction. Acquire temporary resources within Effect.scoped in manage / prepare.
 */
export const makeSchedulingOwner = <E>(
  authorizer: Layer.Layer<ScheduleAuthorizer, E, WorkerEnvironment>,
) =>
  makeScheduleOwnerObjectClass(
    Layer.merge(
      authorizer,
      Layer.effect(
        ThreadObjectNamespace,
        Effect.map(WorkerEnvironment, (env) => ({ namespace: env.THREADS })),
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
  const scheduling = yield* Scheduling;
  return yield* scheduling.create(agent, input, {
    ...options,
    timing: { _tag: "Cron", expression: "0 8 * * *", timeZone: "UTC" },
  });
});

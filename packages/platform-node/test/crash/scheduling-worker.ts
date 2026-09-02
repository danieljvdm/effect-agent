import { AgentId } from "@effect-agent/core";
import {
  DefinitionDigests,
  Digest,
  Principal,
  ScheduleAuthorizer,
  ScheduleFailpoint,
  ScheduleId,
  Scheduling,
} from "@effect-agent/thread";
import { Config, Console, Effect, Layer, Ref, Schema } from "effect";

import { NodeDurableHost, NodeScheduling } from "../../src/index.ts";

export const SchedulingCrashBoundary = Schema.Literals([
  "schedule:insert:before",
  "schedule:insert:after",
  "schedule:prepare:after",
  "schedule:admission:after",
  "schedule:complete:before",
  "schedule:complete:after",
]);

export type SchedulingCrashBoundary = typeof SchedulingCrashBoundary.Type;

export const SchedulingCrashMarker = Schema.Struct({
  _tag: Schema.Literal("SchedulingCrashMarker"),
  boundary: SchedulingCrashBoundary,
});

const WorkerConfig = Schema.Struct({
  database: Schema.NonEmptyString,
  boundary: SchedulingCrashBoundary,
});

export const schedulingCrashAgent = {
  definition: {
    id: Schema.decodeSync(AgentId)("node-scheduling-crash-agent"),
    input: Schema.Struct({ question: Schema.String }),
  },
};

export const schedulingCrashDigest = Schema.decodeSync(Digest)("c".repeat(64));

export const schedulingCrashDefinitions = DefinitionDigests.make({
  agent: schedulingCrashDigest,
  model: schedulingCrashDigest,
  tools: schedulingCrashDigest,
});

export const schedulingCrashPrincipal = Schema.decodeSync(Principal)(
  "node-scheduling-crash-principal",
);

export const schedulingCrashScheduleId = Schema.decodeSync(ScheduleId)("node-crash-schedule");

export const schedulingCrashScope = {
  owner: { tenantId: "node-crash-tenant", ownerId: "node-crash-owner" },
  principal: schedulingCrashPrincipal,
};

export const schedulingCrashCreateOptions = {
  scope: schedulingCrashScope,
  scheduleId: schedulingCrashScheduleId,
  timing: { _tag: "At" as const, atMillis: 0 },
  destination: { _tag: "FreshThread" as const },
  deliveryPrincipal: schedulingCrashPrincipal,
  definitions: schedulingCrashDefinitions,
};

const authorizerLayer = Layer.succeed(ScheduleAuthorizer)({
  manage: () => Effect.void,
  prepare: () => Effect.succeed({ policyId: "node-crash-policy", decisionId: "allowed" }),
});

const decodeConfig = Effect.fn("SchedulingCrashWorker.decodeConfig")(function* () {
  const database = yield* Config.string("EFFECT_AGENT_SCHEDULE_DB");
  const boundary = yield* Config.string("EFFECT_AGENT_SCHEDULE_BOUNDARY");

  return yield* Schema.decodeUnknownEffect(WorkerConfig)({ database, boundary });
});

/** Fixed child workflow. The entrypoint supplies only NodeRuntime signal and exit handling. */
export const schedulingCrashWorker = Effect.gen(function* () {
  const config = yield* decodeConfig();
  const latched = yield* Ref.make(false);

  const encodedMarker = Schema.encodeSync(Schema.fromJsonString(SchedulingCrashMarker))({
    _tag: "SchedulingCrashMarker",
    boundary: config.boundary,
  });

  const failpoint = {
    hit: (point: string) =>
      Effect.gen(function* () {
        if (yield* Ref.get(latched)) return yield* Effect.never;
        if (point !== config.boundary) return;
        yield* Ref.set(latched, true);
        yield* Console.log(encodedMarker);

        return yield* Effect.never;
      }),
  };

  const host = NodeDurableHost.layerStack({
    filename: config.database,
    deploymentId: "node-scheduling-crash-deployment",
    producerId: "node-scheduling-crash-producer",
    wakeScanInterval: 1_000,
  });

  const stack = NodeScheduling.layer().pipe(
    Layer.provideMerge(host),
    Layer.provide(authorizerLayer),
  );

  return yield* Effect.gen(function* () {
    const scheduling = yield* Scheduling;

    yield* scheduling.create(
      schedulingCrashAgent,
      { question: "survive SIGKILL" },
      schedulingCrashCreateOptions,
    );

    return yield* Effect.never;
  }).pipe(Effect.provide(stack), Effect.provideService(ScheduleFailpoint, failpoint));
});

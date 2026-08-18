import { Config, Console, Effect, FileSystem, Layer, Path, Schema } from "effect";

import {
  admitOnlyAttemptPolicyLayer,
  admitOnlyHostLayer,
  admitOnlyImplementerLayer,
} from "./admit-host.ts";
import { ObservedActionsIdentity } from "./authenticate.ts";
import {
  DEFAULT_MENTION_COMMAND,
  DEFAULT_REACTION_CONTENT,
  DispatchTargetRejected,
  IngressPolicy,
  IngressPolicyConfig,
  PlatformDelivery,
} from "./contracts.ts";
import { liveGitHubApiLayer } from "./github-live.ts";
import { handleWorkOrderDelivery } from "./ingress.ts";
import { FileBackedAttemptStore, IngressStoreFailpoint } from "./store.ts";

const EventPullNumber = Schema.Struct({
  pull_request: Schema.Struct({
    number: Schema.Int.check(Schema.isGreaterThan(0)),
  }),
});

export const pullRequestNumberFromEvent = (
  rawBody: string,
): Effect.Effect<number, DispatchTargetRejected> =>
  Schema.decodeUnknownEffect(Schema.fromJsonString(EventPullNumber))(rawBody).pipe(
    Effect.map((event) => event.pull_request.number),
    Effect.mapError(() =>
      DispatchTargetRejected.make({
        reason: "event does not name a pull request",
      }),
    ),
  );

const actorIdsFromConfig = (raw: string): ReadonlyArray<string> => {
  const ids = raw
    .split(",")
    .map((part) => part.trim())
    .filter((part) => part.length > 0);
  return ids.length > 0 ? ids : ["3450486"];
};

export const runActionsDelivery = Effect.fn("runActionsDelivery")(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const token = yield* Config.nonEmptyString("GITHUB_TOKEN");
  const repository = yield* Config.nonEmptyString("GITHUB_REPOSITORY");
  const eventName = yield* Config.nonEmptyString("GITHUB_EVENT_NAME");
  const eventPath = yield* Config.nonEmptyString("GITHUB_EVENT_PATH");
  const runId = yield* Config.nonEmptyString("GITHUB_RUN_ID");
  const runAttempt = yield* Config.string("GITHUB_RUN_ATTEMPT").pipe(Config.withDefault("1"));
  const mentionCommand = yield* Config.nonEmptyString("EFFECT_AGENT_MENTION_COMMAND").pipe(
    Config.withDefault(DEFAULT_MENTION_COMMAND),
  );
  const reactionContent = yield* Config.nonEmptyString("EFFECT_AGENT_REACTION_CONTENT").pipe(
    Config.withDefault(DEFAULT_REACTION_CONTENT),
  );
  const actorIdsRaw = yield* Config.string("EFFECT_AGENT_AUTHORIZED_ACTOR_IDS").pipe(
    Config.withDefault("3450486"),
  );
  const runnerTemp = yield* Config.string("RUNNER_TEMP").pipe(
    Config.orElse(() => Config.string("TMPDIR")),
    Config.withDefault("/tmp"),
  );
  const apiUrl = yield* Config.option(Config.nonEmptyString("GITHUB_API_URL"));
  const rawBody = yield* fs.readFileString(eventPath);
  const pullRequestNumber = yield* pullRequestNumberFromEvent(rawBody);
  const policy = IngressPolicyConfig.make({
    repository,
    pullRequestNumber,
    authorizedActorIds: [...actorIdsFromConfig(actorIdsRaw)],
    mentionCommand,
    reactionContent,
    webhookSecret: "actions-mode",
  });
  const stateDir = path.join(runnerTemp, "pr-work-order-ingress");
  yield* fs.makeDirectory(stateDir, { recursive: true });
  const delivery = PlatformDelivery.make({
    deliveryId: `${runId}:${runAttempt}`,
    eventName,
    rawBody,
  });
  const policyLayer = IngressPolicy.layer(policy);
  const githubLayer = liveGitHubApiLayer({
    token,
    ...(apiUrl._tag === "Some" ? { apiUrl: apiUrl.value } : {}),
  });
  const result = yield* handleWorkOrderDelivery(delivery).pipe(
    Effect.provide(
      Layer.mergeAll(
        policyLayer,
        ObservedActionsIdentity.layerFromEnvironment,
        githubLayer,
        FileBackedAttemptStore.layer(stateDir).pipe(Layer.provide(IngressStoreFailpoint.layer)),
        admitOnlyHostLayer.pipe(Layer.provide(githubLayer), Layer.provide(policyLayer)),
        admitOnlyImplementerLayer,
        admitOnlyAttemptPolicyLayer,
      ),
    ),
  );
  yield* Console.log(
    result._tag === "published"
      ? `published ${result.publishedHeadSha}`
      : `settled ${result.disposition}`,
  );
  return result;
});

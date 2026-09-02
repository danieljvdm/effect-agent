import {
  SqliteStorageConfig,
  SqliteStorageConfigValue,
  SqliteStorageFailpoint,
  subscriptionStoreLayer,
} from "@effect-agent/storage-sqlite";
import {
  EventSources,
  SubscriptionInputBindings,
  makeSubscriptionInputBinding,
  type SubscriptionInputBinding,
  type SourcePartition,
  SubscriptionAuthorizer,
  SubscriptionIntake,
  Subscriptions,
  type EventSource,
} from "@effect-agent/thread";
import {
  acceptVerifiedGitHubWorkflowRunWebhook,
  githubWorkflowRunsHttpLayer,
  type GitHubRepository,
  type GitHubWebhookSignatureVerifier,
  GitHubWorkflowRunCompletion,
  GitHubWorkflowRunSourceVersion,
  GitHubWorkflowRunWatch,
  makeGitHubWorkflowRunSource,
  type VerifiedGitHubWorkflowRunWebhookRequest,
  webCryptoGitHubWebhookSignatureVerifierLayer,
} from "@effect-agent/thread/github";
import { NodeHttpClient } from "@effect/platform-node";
import { SqliteClient } from "@effect/sql-sqlite-node";
import { Effect, Layer, Schema, type Redacted } from "effect";

import {
  NodeDurableHost,
  type NodeDurableRuntimeOptions,
  NodeSubscriptions,
} from "../../src/index.ts";

const sqliteSubscriptionInfrastructure = (filename: string) =>
  Layer.mergeAll(
    SqliteClient.layer({ filename }),
    Layer.succeed(SqliteStorageConfig)(
      SqliteStorageConfigValue.make({
        observationPollInterval: 25,
        busyTimeout: 5_000,
        ownershipLeaseDuration: 30_000,
        verifyOnOpen: false,
      }),
    ),
    SqliteStorageFailpoint.layer,
  );

const subscriptionRuntimeFromSourcesLayer = <E, R>(options: {
  readonly runtime: NodeDurableRuntimeOptions;
  readonly partition: SourcePartition;
  readonly sources: Layer.Layer<EventSources | SubscriptionInputBindings, E, R>;
  readonly authorizer: SubscriptionAuthorizer["Service"];
}) => {
  const dependencies = Layer.mergeAll(
    NodeDurableHost.layerStack(options.runtime),
    subscriptionStoreLayer(options.partition).pipe(
      Layer.provide(sqliteSubscriptionInfrastructure(options.runtime.filename)),
    ),
    options.sources,
    Layer.succeed(SubscriptionAuthorizer)(options.authorizer),
  );

  return NodeSubscriptions.layer().pipe(Layer.provideMerge(dependencies));
};

/**
 * Assemble one source partition in the same process and SQLite database owner as the durable
 * host. The caller binds the permitted source catalog and explicit authorization policy.
 */
export const subscriptionRuntimeLayer = (options: {
  readonly runtime: NodeDurableRuntimeOptions;
  readonly partition: SourcePartition;
  readonly sources: ReadonlyArray<EventSource>;
  readonly bindings: ReadonlyArray<SubscriptionInputBinding>;
  readonly authorizer: SubscriptionAuthorizer["Service"];
}) =>
  subscriptionRuntimeFromSourcesLayer({
    ...options,
    sources: Layer.merge(
      Layer.succeed(EventSources)({ sources: options.sources }),
      Layer.succeed(SubscriptionInputBindings)({ bindings: options.bindings }),
    ),
  });

/** Durable context retained while the exact workflow attempt is still running. */
export const GitHubWorkflowContinuation = Schema.Struct({
  instruction: Schema.String,
});

/** Ordinary Agent input admitted after GitHub reports the registered attempt complete. */
export const GitHubWorkflowContinuationInput = Schema.Struct({
  instruction: Schema.String,
  workflowRun: GitHubWorkflowRunCompletion,
});

/**
 * Assemble verified webhook ingress and exact-attempt REST reconciliation over one Node
 * subscription partition. The host supplies secrets and its real authorization policy; neither
 * credential enters durable records. `NodeSubscriptions` polls the exact attempt after durable
 * registration, so a completion is still delivered through ordinary admission when no webhook
 * arrives and no caller is waiting.
 */
export const githubWorkflowSubscriptionRuntimeLayer = (options: {
  readonly runtime: NodeDurableRuntimeOptions;
  readonly partition: SourcePartition;
  readonly repository: GitHubRepository;
  readonly githubToken: Redacted.Redacted<string>;
  readonly webhookSecret: Redacted.Redacted<string>;
  readonly agentId: SubscribeOptions["agentId"];
  readonly definitions: SubscribeOptions["definitions"];
  readonly authorizer: SubscriptionAuthorizer["Service"];
}) => {
  const github = githubWorkflowRunsHttpLayer({
    repository: options.repository,
    token: options.githubToken,
  }).pipe(Layer.provide(NodeHttpClient.layerUndici));

  const sources = Layer.merge(
    Layer.effect(
      EventSources,
      makeGitHubWorkflowRunSource({
        repository: options.repository,
      }).pipe(Effect.map((source) => ({ sources: [source] }))),
    ).pipe(Layer.provide(github)),
    Layer.effect(
      SubscriptionInputBindings,
      makeSubscriptionInputBinding({
        source: GitHubWorkflowRunSourceVersion,
        agentId: options.agentId,
        definitions: options.definitions,
        event: GitHubWorkflowRunCompletion,
        parameters: GitHubWorkflowRunWatch,
        context: GitHubWorkflowContinuation,
        input: GitHubWorkflowContinuationInput,
        prepare: (completion, _watch, continuation) =>
          Effect.succeed({ instruction: continuation.instruction, workflowRun: completion }),
      }).pipe(Effect.map((binding) => ({ bindings: [binding] }))),
    ),
  );

  const subscriptions = subscriptionRuntimeFromSourcesLayer({ ...options, sources });

  const verifier = webCryptoGitHubWebhookSignatureVerifierLayer(
    options.webhookSecret,
    globalThis.crypto.subtle,
  );

  return Layer.merge(subscriptions, verifier);
};

type SubscribeOptions = Parameters<Subscriptions["Service"]["subscribe"]>[1];

/**
 * Persist one exact workflow-attempt watch and return immediately. The scoped driver performs
 * reconciliation and ordinary Agent admission independently of this caller's lifetime.
 */
export const registerGitHubWorkflowCompletion = Effect.fn(
  "Example.registerGitHubWorkflowCompletion",
)(function* (options: {
  readonly scope: Parameters<Subscriptions["Service"]["subscribe"]>[0];
  readonly subscriptionId: SubscribeOptions["subscriptionId"];
  readonly watch: typeof GitHubWorkflowRunWatch.Type;
  readonly continuation: typeof GitHubWorkflowContinuation.Type;
  readonly expiresAtMillis: SubscribeOptions["expiresAtMillis"];
  readonly destination: SubscribeOptions["destination"];
  readonly deliveryPrincipal: SubscribeOptions["deliveryPrincipal"];
  readonly agentId: SubscribeOptions["agentId"];
  readonly definitions: SubscribeOptions["definitions"];
}) {
  const subscriptions = yield* Subscriptions;

  return yield* subscriptions.subscribe(options.scope, {
    subscriptionId: options.subscriptionId,
    source: GitHubWorkflowRunSourceVersion,
    parameters: options.watch,
    context: options.continuation,
    mode: "once",
    expiresAtMillis: options.expiresAtMillis,
    destination: options.destination,
    deliveryPrincipal: options.deliveryPrincipal,
    agentId: options.agentId,
    definitions: options.definitions,
  });
});

/** Verify GitHub's raw HMAC signature before handing completion to durable intake. */
export const acceptGitHubWorkflowWebhook = (
  request: VerifiedGitHubWorkflowRunWebhookRequest,
): Effect.Effect<
  Effect.Success<ReturnType<typeof acceptVerifiedGitHubWorkflowRunWebhook>>,
  Effect.Error<ReturnType<typeof acceptVerifiedGitHubWorkflowRunWebhook>>,
  GitHubWebhookSignatureVerifier | SubscriptionIntake
> => acceptVerifiedGitHubWorkflowRunWebhook(request);

/** Registration returns immediately; the scoped driver later admits matching input. */
export const registerAndAccept = Effect.fn("Example.registerAndAccept")(function* (
  scope: Parameters<Subscriptions["Service"]["subscribe"]>[0],
  options: Parameters<Subscriptions["Service"]["subscribe"]>[1],
  event: {
    readonly principal: Parameters<SubscriptionIntake["Service"]["accept"]>[0];
    readonly payload: unknown;
  },
) {
  const subscriptions = yield* Subscriptions;
  const intake = yield* SubscriptionIntake;
  const registered = yield* subscriptions.subscribe(scope, options);
  const acknowledgement = yield* intake.accept(event.principal, options.source, event.payload);

  return { registered, acknowledgement };
});

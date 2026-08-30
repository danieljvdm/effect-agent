import { Agent, AgentId, AgentPolicy, ConversationId } from "@effect-agent/core";
import {
  DefinitionDigests,
  Digest,
  DurableAgentRuntime,
  DurableRuntimeFailpointError,
  EventSources,
  SubscriptionInputBindings,
  makeSubscriptionInputBinding,
  IdempotencyKey,
  Principal,
  SourcePartition,
  SubscriptionAuthorizer,
  type SubscriptionDeliverySnapshot,
  SubscriptionStore,
  Subscriptions,
  SubmissionLedger,
  SubmissionLookupByKey,
  defaultSubscriptionLimits,
  type DurableRuntimeFailpointHandler,
} from "@effect-agent/session";
import {
  GitHubRepository,
  GitHubWorkflowRunSourceVersion,
  GitHubWorkflowRuns,
  GitHubWorkflowRunCompletion,
  GitHubWorkflowRunWatch,
  makeGitHubWorkflowRunSource,
} from "@effect-agent/session/github";
import {
  SqliteStorageConfig,
  SqliteStorageConfigValue,
  SqliteStorageFailpoint,
  subscriptionStoreLayer,
} from "@effect-agent/storage-sqlite";
import { NodeFileSystem } from "@effect/platform-node";
import { SqliteClient } from "@effect/sql-sqlite-node";
import { expect, it } from "@effect/vitest";
import type { PlatformError } from "effect";
import {
  Context,
  Effect,
  Exit,
  FileSystem,
  Layer,
  Option,
  Ref,
  Schema,
  Scope,
  Stream,
} from "effect";
import { TestClock } from "effect/testing";
import { LanguageModel, Model, Toolkit, type Response } from "effect/unstable/ai";

import { NodeDurableHost, NodeSubscriptions } from "../src/index.ts";

const partition = SourcePartition.make({
  tenantId: "node-subscription-tenant",
  address: "github:repository:42",
});
const principal = Schema.decodeSync(Principal)("node-subscription-principal");
const conversationId = Schema.decodeSync(ConversationId)("node-subscription-conversation");
const digest = Schema.decodeSync(Digest)("b".repeat(64));
const definitions = DefinitionDigests.make({ agent: digest, model: digest, tools: digest });
const agentId = Schema.decodeSync(AgentId)("node-subscription-agent");
const repository = GitHubRepository.make({ id: 42, owner: "effect", name: "agent" });
const headSha = "a".repeat(40);
const scope = { partition, ownerId: "review-owner", principal } as const;
const initialAdmissionKey = Schema.decodeSync(IdempotencyKey)("initial-run");

const limits = {
  ...defaultSubscriptionLimits,
  batchSize: 4,
  concurrency: 2,
  retryMillis: 10,
  operationTimeoutMillis: 1_000,
};

const authorizerLayer = Layer.succeed(SubscriptionAuthorizer)({
  manage: () => Effect.void,
  intake: () => Effect.void,
  reconcile: () => Effect.void,
  prepare: () => Effect.succeed({ policyId: "node-subscription-policy", decisionId: "allow" }),
});

const sqliteInfrastructure = (filename: string) =>
  Layer.mergeAll(
    SqliteClient.layer({ filename }),
    Layer.succeed(SqliteStorageConfig)(
      SqliteStorageConfigValue.make({
        observationPollInterval: 0,
        busyTimeout: 5_000,
        ownershipLeaseDuration: 30_000,
        verifyOnOpen: false,
      }),
    ),
    SqliteStorageFailpoint.layer,
  );

const sourceLayer = (calls: Ref.Ref<number>, completed: Ref.Ref<boolean>) =>
  Layer.merge(
    Layer.effect(
      EventSources,
      makeGitHubWorkflowRunSource({
        repository,
      }).pipe(
        Effect.map((source) => ({ sources: [source] })),
        Effect.provideService(
          GitHubWorkflowRuns,
          GitHubWorkflowRuns.of({
            getAttempt: ({ runId, attempt }) =>
              Ref.update(calls, (count) => count + 1).pipe(
                Effect.andThen(Ref.get(completed)),
                Effect.map((isCompleted) => ({
                  id: runId,
                  run_attempt: attempt,
                  head_sha: headSha,
                  status: isCompleted ? "completed" : "in_progress",
                  conclusion: isCompleted ? "success" : null,
                  repository: { id: repository.id },
                })),
              ),
          }),
        ),
      ),
    ),
    Layer.effect(
      SubscriptionInputBindings,
      makeSubscriptionInputBinding({
        source: GitHubWorkflowRunSourceVersion,
        agentId,
        definitions,
        event: GitHubWorkflowRunCompletion,
        parameters: GitHubWorkflowRunWatch,
        context: Schema.Struct({ instruction: Schema.String }),
        input: Schema.Struct({ instruction: Schema.String, conclusion: Schema.String }),
        prepare: (completion, _watch, context) =>
          Effect.succeed({ instruction: context.instruction, conclusion: completion.conclusion }),
      }).pipe(Effect.map((binding) => ({ bindings: [binding] }))),
    ),
  );

interface DeadlineDefectProbe {
  readonly pending: Ref.Ref<boolean>;
  readonly attempts: Ref.Ref<number>;
}

const storeLayer = (filename: string, probe?: DeadlineDefectProbe) => {
  const sqliteStore = subscriptionStoreLayer(partition).pipe(
    Layer.provide(sqliteInfrastructure(filename)),
  );
  if (probe === undefined) return sqliteStore;
  return Layer.effect(
    SubscriptionStore,
    Effect.map(SubscriptionStore, (store) =>
      SubscriptionStore.of({
        ...store,
        nextDeadline: Ref.update(probe.attempts, (count) => count + 1).pipe(
          Effect.andThen(Ref.getAndSet(probe.pending, false)),
          Effect.flatMap((shouldDefect) =>
            shouldDefect ? Effect.die("transient nextDeadline defect") : store.nextDeadline,
          ),
        ),
      }),
    ),
  ).pipe(Layer.provide(sqliteStore));
};

const subscriptionLayer = (
  filename: string,
  calls: Ref.Ref<number>,
  completed: Ref.Ref<boolean>,
  runtimeFailpoint?: DurableRuntimeFailpointHandler,
  deadlineDefectProbe?: DeadlineDefectProbe,
) => {
  const dependencies = Layer.mergeAll(
    NodeDurableHost.layerStack({
      filename,
      deploymentId: "node-subscription-deployment",
      producerId: "node-subscription-producer",
      wakeScanInterval: 1_000,
      ...(runtimeFailpoint === undefined ? {} : { runtimeFailpoint }),
    }),
    storeLayer(filename, deadlineDefectProbe),
    authorizerLayer,
    sourceLayer(calls, completed),
  );
  return NodeSubscriptions.layer({ limits }).pipe(Layer.provideMerge(dependencies));
};

const withTemporaryDatabase = <A, E>(
  use: (filename: string) => Effect.Effect<A, E>,
): Effect.Effect<A, E | PlatformError.PlatformError> =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({
        prefix: "effect-agent-node-subscription-",
      });
      return yield* use(`${directory}/runtime.sqlite`);
    }),
  ).pipe(Effect.provide(NodeFileSystem.layer));

const subscribe = (subscriptions: Subscriptions["Service"]) =>
  subscriptions.subscribe(scope, {
    subscriptionId: "workflow-completion",
    source: GitHubWorkflowRunSourceVersion,
    parameters: { runId: 101, attempt: 1, expectedHeadSha: headSha },
    context: { instruction: "continue reviewing" },
    mode: "once",
    expiresAtMillis: 60_000,
    destination: { _tag: "ExistingConversation", conversationId },
    deliveryPrincipal: principal,
    agentId,
    definitions,
  });

const waitForDelivery = (
  subscriptions: Subscriptions["Service"],
  predicate: (delivery: typeof SubscriptionDeliverySnapshot.Type) => boolean,
) =>
  Effect.gen(function* () {
    for (let attempt = 0; attempt < 128; attempt += 1) {
      const page = yield* subscriptions.listDeliveries(scope, {
        partition,
        ownerId: scope.ownerId,
        subscriptionId: "workflow-completion",
      });
      const item = page.items[0];
      if (item !== undefined && predicate(item)) return item;
      yield* TestClock.adjust(10);
      yield* Effect.yieldNow;
    }
    return yield* Effect.die("Timed out waiting for subscription delivery");
  });

const usage = { inputTokens: {}, outputTokens: {} };
const finalParts = (text: string): ReadonlyArray<Response.StreamPartEncoded> => [
  { type: "text-start", id: "answer" },
  { type: "text-delta", id: "answer", delta: text },
  { type: "text-end", id: "answer" },
  { type: "finish", reason: "stop", usage },
];

const initialAgentDefinition = Agent.define("node-subscription-agent", {
  input: Schema.Struct({ question: Schema.String }),
  output: Schema.Struct({ answer: Schema.String }),
  instructions: ({ question }) => `Answer ${question} as JSON.`,
  toolkit: Toolkit.empty,
  policy: AgentPolicy.make({
    maxTurns: 1,
    maxToolCalls: 1,
    maxDuration: "30 seconds",
    toolConcurrency: 1,
  }),
});

const makeInitialAgent = Effect.sync(() => {
  const model = Model.make(
    "scripted",
    "node-subscription-initial-run",
    Layer.effect(
      LanguageModel.LanguageModel,
      LanguageModel.make({
        generateText: () => Effect.succeed([]),
        streamText: () => Stream.fromIterable(finalParts('{"answer":"registered"}')),
      }),
    ),
  );
  return Agent.withModel(initialAgentDefinition, model);
});

it.effect(
  "continues polling after a transient nextDeadline defect and stops when its Scope closes",
  () =>
    withTemporaryDatabase((filename) =>
      Effect.scoped(
        Effect.gen(function* () {
          const calls = yield* Ref.make(0);
          const completed = yield* Ref.make(false);
          const pending = yield* Ref.make(true);
          const attempts = yield* Ref.make(0);
          const driverScope = yield* Scope.make();
          yield* Effect.addFinalizer(() => Scope.close(driverScope, Exit.void));
          const context = yield* Layer.build(
            subscriptionLayer(filename, calls, completed, undefined, { pending, attempts }),
          ).pipe(Scope.provide(driverScope));
          const subscriptions = Context.get(context, Subscriptions);

          for (let poll = 0; poll < 128 && (yield* Ref.get(attempts)) === 0; poll += 1) {
            yield* Effect.yieldNow;
          }
          expect(yield* Ref.get(attempts)).toBe(1);
          expect(yield* Ref.get(pending)).toBe(false);

          yield* subscribe(subscriptions);
          for (
            let poll = 0;
            poll < 128 && ((yield* Ref.get(calls)) === 0 || (yield* Ref.get(attempts)) < 2);
            poll += 1
          ) {
            yield* TestClock.adjust(limits.retryMillis);
            yield* Effect.yieldNow;
          }
          expect(yield* Ref.get(calls)).toBeGreaterThan(0);
          expect(yield* Ref.get(attempts)).toBeGreaterThanOrEqual(2);

          yield* Scope.close(driverScope, Exit.void);
          const callsAfterClose = yield* Ref.get(calls);
          const attemptsAfterClose = yield* Ref.get(attempts);
          yield* TestClock.adjust(100);
          yield* Effect.yieldNow;
          expect(yield* Ref.get(calls)).toBe(callsAfterClose);
          expect(yield* Ref.get(attempts)).toBe(attemptsAfterClose);
        }),
      ),
    ),
  10_000,
);

it.effect(
  "delivers after a completed Run, restart, missed GitHub event, and lost admission reply",
  () =>
    withTemporaryDatabase((filename) =>
      Effect.scoped(
        Effect.gen(function* () {
          const calls = yield* Ref.make(0);
          const completed = yield* Ref.make(false);
          const firstScope = yield* Scope.make();
          yield* Effect.addFinalizer(() => Scope.close(firstScope, Exit.void));
          const firstContext = yield* Layer.build(
            subscriptionLayer(filename, calls, completed),
          ).pipe(Scope.provide(firstScope));
          const firstSubscriptions = Context.get(firstContext, Subscriptions);
          const firstHost = Context.get(firstContext, NodeDurableHost);
          const firstRuntime = Context.get(firstContext, DurableAgentRuntime);

          yield* subscribe(firstSubscriptions);
          const initialAgent = yield* makeInitialAgent;
          const initialReceipt = yield* firstHost.submit(
            initialAgent,
            { question: "is the workflow registered?" },
            { conversationId, principal, idempotencyKey: initialAdmissionKey, definitions },
          );
          yield* firstRuntime.processConversation(initialAgent, conversationId);
          const initialSettlement = yield* firstHost.awaitSettlement(initialReceipt);
          expect(initialSettlement.outcome).toBe("completed");

          for (let attempt = 0; attempt < 128 && (yield* Ref.get(calls)) === 0; attempt += 1) {
            yield* TestClock.adjust(10);
            yield* Effect.yieldNow;
          }
          expect(yield* Ref.get(calls)).toBeGreaterThan(0);

          yield* Scope.close(firstScope, Exit.void);
          expect(yield* firstHost.admissionOpen).toBe(false);
          const callsAfterClose = yield* Ref.get(calls);
          yield* TestClock.adjust(100);
          yield* Effect.yieldNow;
          expect(yield* Ref.get(calls)).toBe(callsAfterClose);

          yield* Ref.set(completed, true);
          const secondScope = yield* Scope.make();
          yield* Effect.addFinalizer(() => Scope.close(secondScope, Exit.void));
          const secondContext = yield* Layer.build(
            subscriptionLayer(filename, calls, completed, (location) =>
              location === "submit:after-admit"
                ? Effect.fail(DurableRuntimeFailpointError.make({ location }))
                : Effect.void,
            ),
          ).pipe(Scope.provide(secondScope));
          const secondSubscriptions = Context.get(secondContext, Subscriptions);
          yield* waitForDelivery(
            secondSubscriptions,
            (delivery) => delivery.receipt === null && delivery.retry.attempts > 0,
          );
          yield* Scope.close(secondScope, Exit.void);

          const thirdScope = yield* Scope.make();
          yield* Effect.addFinalizer(() => Scope.close(thirdScope, Exit.void));
          const thirdContext = yield* Layer.build(
            subscriptionLayer(filename, calls, completed),
          ).pipe(Scope.provide(thirdScope));
          const thirdSubscriptions = Context.get(thirdContext, Subscriptions);
          const ledger = Context.get(thirdContext, SubmissionLedger);
          const store = Context.get(thirdContext, SubscriptionStore);
          const delivered = yield* waitForDelivery(
            thirdSubscriptions,
            (delivery) => delivery.receipt !== null,
          );
          expect(delivered.receipt?.conversationId).toBe(conversationId);

          const delivery = yield* store.delivery(delivered.key);
          if (delivery === null)
            return yield* Effect.die("Expected retained subscription delivery");
          const admitted = yield* ledger.lookup(
            SubmissionLookupByKey.make({
              conversationId,
              principal,
              idempotencyKey: delivery.admissionKey,
            }),
          );
          expect(Option.isSome(admitted)).toBe(true);

          yield* Scope.close(thirdScope, Exit.void);
        }),
      ),
    ),
  30_000,
);

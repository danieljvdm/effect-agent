import { NodeDurableHost } from "@effect-agent/platform-node/node-durable-host";
import { NodeSubscriptions } from "@effect-agent/platform-node/node-subscriptions";
import { subscriptionStoreLayer } from "@effect-agent/storage-sqlite/sqlite-subscription-store";
import { NodeFileSystem } from "@effect/platform-node";
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
import * as Agent from "effect-agent/agent";
import { AgentPolicy } from "effect-agent/agent-policy";
import { DurableAgentRuntime } from "effect-agent/durable-agent-runtime";
import {
  DurableRuntimeFailpointError,
  type DurableRuntimeFailpointHandler,
} from "effect-agent/durable-failpoint";
import { EventSources } from "effect-agent/event-source";
import {
  GitHubRepository,
  GitHubWorkflowRunSourceVersion,
  GitHubWorkflowRuns,
  GitHubWorkflowRunCompletion,
  GitHubWorkflowRunWatch,
  makeGitHubWorkflowRunSource,
} from "effect-agent/git-hub-workflow-source";
import { AgentId, ThreadId } from "effect-agent/identifiers";
import { DefinitionDigests, Digest } from "effect-agent/records";
import {
  IdempotencyKey,
  Principal,
  SubmissionLedger,
  SubmissionLookupByKey,
} from "effect-agent/submission-ledger";
import {
  SourcePartition,
  SubscriptionAuthorizer,
  type SubscriptionDeliverySnapshot,
  SubscriptionStore,
  defaultSubscriptionLimits,
} from "effect-agent/subscription";
import {
  SubscriptionInputBindings,
  makeSubscriptionInputBinding,
} from "effect-agent/subscription-input";
import { Subscriptions } from "effect-agent/subscriptions";
import { TestClock } from "effect/testing";
import { LanguageModel, Model, Toolkit, type Response } from "effect/unstable/ai";

const partition = SourcePartition.make({
  tenantId: "node-subscription-tenant",
  address: "github:repository:42",
});

const principal = Schema.decodeSync(Principal)("node-subscription-principal");
const threadId = Schema.decodeSync(ThreadId)("node-subscription-thread");
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

const subscriptionLayer = (
  filename: string,
  calls: Ref.Ref<number>,
  completed: Ref.Ref<boolean>,
  runtimeFailpoint?: DurableRuntimeFailpointHandler,
) => {
  const dependencies = Layer.mergeAll(
    subscriptionStoreLayer(partition),
    authorizerLayer,
    sourceLayer(calls, completed),
  ).pipe(
    // Share the host's serialized connection, including startup recovery writes.
    Layer.provideMerge(
      NodeDurableHost.layerStack({
        filename,
        deploymentId: "node-subscription-deployment",
        producerId: "node-subscription-producer",
        wakeScanInterval: 1_000,
        observationPollInterval: 0,
        ...(runtimeFailpoint === undefined ? {} : { runtimeFailpoint }),
      }),
    ),
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
    destination: { _tag: "ExistingThread", threadId },
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

const initialAgentDefinition = Agent.make("node-subscription-agent", {
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
            { threadId, principal, idempotencyKey: initialAdmissionKey, definitions },
          );

          yield* firstRuntime.processThread(initialAgent, threadId);
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

          expect(delivered.receipt?.threadId).toBe(threadId);

          const delivery = yield* store.delivery(delivered.key);

          if (delivery === null)
            return yield* Effect.die("Expected retained subscription delivery");

          const admitted = yield* ledger.lookup(
            SubmissionLookupByKey.make({
              threadId,
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

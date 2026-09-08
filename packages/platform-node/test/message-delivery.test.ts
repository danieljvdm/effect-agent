import * as Agent from "@effect-agent/core/Agent";
import { AgentId, ThreadId } from "@effect-agent/core/Identifiers";
import { NodeDurableAgentRuntime } from "@effect-agent/platform-node/NodeDurableAgentRuntime";
import * as NodeHost from "@effect-agent/platform-node/NodeDurableHost";
import { NodeDurableHost } from "@effect-agent/platform-node/NodeDurableHost";
import { digestDefinitions, digestJson } from "@effect-agent/thread/Digest";
import { DurableAgentRuntime } from "@effect-agent/thread/DurableAgentRuntime";
import { DurableRuntimeFailpointError } from "@effect-agent/thread/DurableFailpoint";
import {
  MessageDeliveryError,
  MessageDeliveryStore,
  prepareMessageDelivery,
  type MessageDeliveryKey,
  type MessageDeliveryRecord,
} from "@effect-agent/thread/MessageDelivery";
import { DefinitionDigestInput, type DefinitionDigests } from "@effect-agent/thread/Records";
import {
  IdempotencyKey,
  Principal,
  SubmissionLedger,
  SubmissionLookupByKey,
} from "@effect-agent/thread/SubmissionLedger";
import { NodeCrypto, NodeFileSystem } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import {
  Clock,
  Context,
  Deferred,
  Effect,
  Exit,
  Fiber,
  FileSystem,
  Layer,
  Option,
  Ref,
  Schema,
  Scope,
  Stream,
  type PlatformError,
} from "effect";
import { TestClock } from "effect/testing";
import { LanguageModel, Model, Toolkit, type Response } from "effect/unstable/ai";

const sourceThreadId = Schema.decodeSync(ThreadId)("node-message-source-thread");
const destinationThreadId = Schema.decodeSync(ThreadId)("node-message-destination-thread");
const principal = Schema.decodeSync(Principal)("node-message-principal");
const recipientId = Schema.decodeSync(AgentId)("node-message-recipient");

const messageKey: MessageDeliveryKey = {
  ownerThreadId: sourceThreadId,
  messageId: Schema.decodeSync(IdempotencyKey)("node-message"),
};

const declarations = DefinitionDigestInput.make({ agent: "v1", model: "v1", tools: "v1" });

const options = (filename: string) => ({
  filename,
  deploymentId: "message-deployment",
  producerId: "message-producer",
  wakeScanInterval: 10,
  workerConcurrency: 1,
  settlementPollInterval: 10,
});

const finalParts: ReadonlyArray<Response.StreamPartEncoded> = [
  { type: "text-start", id: "answer" },
  { type: "text-delta", id: "answer", delta: '{"answer":"done"}' },
  { type: "text-end", id: "answer" },
  { type: "finish", reason: "stop", usage: { inputTokens: {}, outputTokens: {} } },
];

const makeAgent = (id: string, beforeReply: Effect.Effect<void> = Effect.void) =>
  Agent.withModel(
    Agent.make(id, {
      input: Schema.Struct({ question: Schema.String }),
      output: Schema.Struct({ answer: Schema.String }),
      instructions: "Answer as JSON.",
      toolkit: Toolkit.empty,
    }),
    Model.make(
      "scripted",
      "node-message-model",
      Layer.effect(
        LanguageModel.LanguageModel,
        LanguageModel.make({
          generateText: () => Effect.succeed([]),
          streamText: () =>
            Stream.unwrap(beforeReply.pipe(Effect.as(Stream.fromIterable(finalParts)))),
        }),
      ),
    ),
  );

const withTemporaryDatabase = <A, E>(
  use: (filename: string) => Effect.Effect<A, E>,
): Effect.Effect<A, E | PlatformError.PlatformError> =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "effect-agent-node-message-" });

      return yield* use(`${directory}/runtime.sqlite`);
    }),
  ).pipe(Effect.provide(NodeFileSystem.layer));

const prepare = Effect.fn("NodeMessageTest.prepare")(function* (definitions: DefinitionDigests) {
  const input = { question: "continue after the source has settled" };
  const nowMillis = yield* Clock.currentTimeMillis;

  return yield* prepareMessageDelivery({
    key: messageKey,
    createdAtMillis: nowMillis,
    deadlineAtMillis: nowMillis + 10_000,
    policy: {
      maxAutomaticAttempts: 4,
      attemptTimeoutMillis: 100,
      retryBaseMillis: 10,
      retryMaxMillis: 20,
      settlementPollMillis: 10,
    },
    envelope: {
      schemaVersion: 1,
      threadId: destinationThreadId,
      deliveryPrincipal: principal,
      agentId: recipientId,
      definitions,
      input,
      inputDigest: yield* digestJson(input),
      admissionKey: Schema.decodeSync(IdempotencyKey)("message-admission"),
      authorization: { policyId: "host", decisionId: "allow" },
    },
  });
});

const waitFor = Effect.fn("NodeMessageTest.waitFor")(function* (
  store: MessageDeliveryStore["Service"],
  predicate: (record: MessageDeliveryRecord) => boolean,
) {
  for (let attempt = 0; attempt < 200; attempt += 1) {
    const record = yield* store.get(messageKey);

    if (record !== null && predicate(record)) return record;
    yield* TestClock.adjust(10);
    yield* Effect.yieldNow;
  }

  return yield* Effect.die("Message delivery did not reach the expected state");
});

describe("Node message delivery recovery", () => {
  for (const pool of ["managed", "manual"] as const) {
    it.effect(
      `rediscovers source-owned work after restart with no live source or wake hint in a ${pool} pool`,
      () =>
        withTemporaryDatabase((filename) =>
          Effect.scoped(
            Effect.gen(function* () {
              const definitions = yield* digestDefinitions(declarations).pipe(
                Effect.provide(NodeCrypto.layer),
              );

              const firstScope = yield* Scope.make();

              yield* Effect.addFinalizer(() => Scope.close(firstScope, Exit.void));

              const firstContext = yield* Layer.build(
                NodeDurableHost.layerStack(options(filename)),
              ).pipe(Scope.provide(firstScope));

              const source = makeAgent("node-message-source");
              const firstHost = Context.get(firstContext, NodeDurableHost);
              const runtime = Context.get(firstContext, DurableAgentRuntime);

              const sourceReceipt = yield* firstHost.submit(
                source,
                { question: "finish source" },
                {
                  threadId: sourceThreadId,
                  principal,
                  idempotencyKey: Schema.decodeSync(IdempotencyKey)("source"),
                  definitions,
                },
              );

              yield* runtime.processThread(source, sourceThreadId);
              expect((yield* firstHost.awaitSettlement(sourceReceipt)).outcome).toBe("completed");
              expect(
                yield* Stream.runCollect(
                  Context.get(firstContext, SubmissionLedger).scanNonterminal,
                ),
              ).toEqual([]);
              const frozen = yield* prepare(definitions).pipe(Effect.provide(NodeCrypto.layer));

              yield* Context.get(firstContext, MessageDeliveryStore).insert(frozen);
              yield* Scope.close(firstScope, Exit.void);

              const release = yield* Deferred.make<void>();
              const recipient = makeAgent(recipientId, Deferred.await(release));
              const secondScope = yield* Scope.make();

              yield* Effect.addFinalizer(() => Scope.close(secondScope, Exit.void));

              const live =
                pool === "managed"
                  ? NodeHost.layer(
                      [{ agent: recipient, definitions: declarations }],
                      options(filename),
                    )
                  : NodeDurableHost.layerRegistered(
                      [{ agent: recipient, definitions: declarations }],
                      options(filename),
                    );

              const secondContext = yield* Layer.build(live).pipe(Scope.provide(secondScope));
              const host = Context.get(secondContext, NodeDurableHost);
              const store = Context.get(secondContext, MessageDeliveryStore);

              expect(host.startupRecovery).toEqual([]);
              if (pool === "manual") {
                expect((yield* store.get(messageKey))?.retry.attempts).toBe(0);
                yield* Effect.forkScoped(host.runResolvedWorkers).pipe(Scope.provide(secondScope));
              }
              const accepted = yield* waitFor(store, (record) => record.status === "accepted");

              expect(accepted.receipt?.threadId).toBe(destinationThreadId);
              expect(accepted.settlement).toBeNull();
              expect(accepted.envelope).toEqual(frozen.envelope);
              yield* Deferred.succeed(release, undefined);
              const processed = yield* waitFor(store, (record) => record.status === "processed");

              expect(processed.receipt).toEqual(accepted.receipt);
              expect(processed.settlement?.outcome).toBe("completed");
              yield* Scope.close(secondScope, Exit.void);
              expect(yield* host.admissionOpen).toBe(false);
            }),
          ),
        ),
      15_000,
    );
  }

  it.effect(
    "preserves one receiver admission after a lost acknowledgement and another host restart",
    () =>
      withTemporaryDatabase((filename) =>
        Effect.scoped(
          Effect.gen(function* () {
            const definitions = yield* digestDefinitions(declarations).pipe(
              Effect.provide(NodeCrypto.layer),
            );

            const seed = yield* prepare(definitions).pipe(Effect.provide(NodeCrypto.layer));

            yield* Effect.gen(function* () {
              yield* (yield* MessageDeliveryStore).insert(seed);
            }).pipe(Effect.provide(NodeDurableAgentRuntime.layer(options(filename))));
            const secondScope = yield* Scope.make();

            yield* Effect.addFinalizer(() => Scope.close(secondScope, Exit.void));

            const secondContext = yield* Layer.build(
              NodeDurableHost.layerStack({
                ...options(filename),
                runtimeFailpoint: (location) =>
                  location === "submit:after-admit"
                    ? Effect.fail(DurableRuntimeFailpointError.make({ location }))
                    : Effect.void,
              }),
            ).pipe(Scope.provide(secondScope));

            const host = Context.get(secondContext, NodeDurableHost);

            yield* Effect.forkScoped(host.runWorkers(Effect.never)).pipe(
              Scope.provide(secondScope),
            );
            yield* waitFor(
              Context.get(secondContext, MessageDeliveryStore),
              (record) => record.retry.attempts > 0 && record.retry.lastFailure !== null,
            );

            const admitted = yield* Context.get(secondContext, SubmissionLedger).lookup(
              SubmissionLookupByKey.make({
                threadId: destinationThreadId,
                principal,
                idempotencyKey: seed.envelope.admissionKey,
              }),
            );

            expect(Option.isSome(admitted)).toBe(true);
            if (Option.isNone(admitted))
              return yield* Effect.die("Expected an admitted receiver submission");
            yield* Scope.close(secondScope, Exit.void);

            const thirdScope = yield* Scope.make();

            yield* Effect.addFinalizer(() => Scope.close(thirdScope, Exit.void));

            const thirdContext = yield* Layer.build(
              NodeDurableHost.layerStack(options(filename)),
            ).pipe(Scope.provide(thirdScope));

            yield* Effect.forkScoped(
              Context.get(thirdContext, NodeDurableHost).runWorkers(Effect.never),
            ).pipe(Scope.provide(thirdScope));

            const accepted = yield* waitFor(
              Context.get(thirdContext, MessageDeliveryStore),
              (record) => record.status === "accepted",
            );

            expect(accepted.receipt?.receiptId).toBe(admitted.value.receiptId);
            expect(accepted.receipt?.submissionId).toBe(admitted.value.submissionId);
            expect(accepted.envelope).toEqual(seed.envelope);
            expect(
              (yield* Stream.runCollect(
                Context.get(thirdContext, SubmissionLedger).scanNonterminal,
              )).length,
            ).toBe(1);
            yield* Scope.close(thirdScope, Exit.void);
          }),
        ),
      ),
    15_000,
  );

  it.effect(
    "interrupts in-flight message admission and finalizes it when the managed host Scope closes",
    () =>
      withTemporaryDatabase((filename) =>
        Effect.scoped(
          Effect.gen(function* () {
            const definitions = yield* digestDefinitions(declarations).pipe(
              Effect.provide(NodeCrypto.layer),
            );

            const seed = yield* prepare(definitions).pipe(Effect.provide(NodeCrypto.layer));

            yield* Effect.gen(function* () {
              yield* (yield* MessageDeliveryStore).insert(seed);
            }).pipe(Effect.provide(NodeDurableAgentRuntime.layer(options(filename))));
            const entered = yield* Deferred.make<void>();
            const calls = yield* Ref.make(0);
            const finalized = yield* Ref.make(0);
            const hostScope = yield* Scope.make();

            yield* Effect.addFinalizer(() => Scope.close(hostScope, Exit.void));

            const context = yield* Layer.build(
              NodeHost.layer([], {
                ...options(filename),
                runtimeFailpoint: (location) =>
                  location === "submit:after-admit"
                    ? Ref.update(calls, (count) => count + 1).pipe(
                        Effect.andThen(Deferred.succeed(entered, undefined)),
                        Effect.andThen(Effect.never),
                        Effect.ensuring(Ref.update(finalized, (count) => count + 1)),
                      )
                    : Effect.void,
              }),
            ).pipe(Scope.provide(hostScope));

            yield* Deferred.await(entered);
            yield* Scope.close(hostScope, Exit.void);
            expect(yield* Context.get(context, NodeDurableHost).admissionOpen).toBe(false);
            expect(yield* Ref.get(finalized)).toBe(1);
            const callsAtClose = yield* Ref.get(calls);

            yield* TestClock.adjust(1_000);
            expect(yield* Ref.get(calls)).toBe(callsAtClose);
          }),
        ),
      ),
    15_000,
  );

  it.effect("stops and finalizes live workers when the delivery driver interrupts internally", () =>
    withTemporaryDatabase((filename) =>
      Effect.scoped(
        Effect.gen(function* () {
          const workerStarted = yield* Deferred.make<void>();
          const driverInterrupted = yield* Deferred.make<void>();
          const finalized = yield* Ref.make(0);

          const observed = Layer.effect(
            MessageDeliveryStore,
            Effect.map(MessageDeliveryStore, (store) =>
              MessageDeliveryStore.of({
                ...store,
                due: () =>
                  Deferred.await(workerStarted).pipe(
                    Effect.andThen(Deferred.succeed(driverInterrupted, undefined)),
                    Effect.andThen(Effect.interrupt),
                  ),
              }),
            ),
          ).pipe(Layer.provideMerge(NodeDurableAgentRuntime.layer(options(filename))));

          yield* Effect.gen(function* () {
            const host = yield* NodeDurableHost;

            const worker = Deferred.succeed(workerStarted, undefined).pipe(
              Effect.andThen(Effect.never),
              Effect.ensuring(Ref.update(finalized, (count) => count + 1)),
            );

            const running = yield* Effect.forkChild(
              host.runWorkers(worker).pipe(Effect.timeout(100)),
            );

            yield* Deferred.await(driverInterrupted);
            yield* TestClock.adjust(100);

            const exit = yield* Fiber.await(running);

            expect(Exit.hasInterrupts(exit)).toBe(true);
            expect(yield* Ref.get(finalized)).toBe(1);
          }).pipe(Effect.provide(NodeDurableHost.layer.pipe(Layer.provideMerge(observed))));
        }),
      ),
    ),
  );

  for (const failureAt of ["due", "deadline"] as const) {
    it.effect(
      `retries a transient ${failureAt} store failure at the configured bounded cadence`,
      () =>
        withTemporaryDatabase((filename) =>
          Effect.scoped(
            Effect.gen(function* () {
              const calls = yield* Ref.make(0);
              const runtimeLayer = NodeDurableAgentRuntime.layer(options(filename));

              const observed = Layer.effect(
                MessageDeliveryStore,
                Effect.map(MessageDeliveryStore, (store) =>
                  MessageDeliveryStore.of({
                    ...store,
                    due: (...args) =>
                      failureAt === "due"
                        ? Ref.updateAndGet(calls, (count) => count + 1).pipe(
                            Effect.flatMap((count) =>
                              count === 1
                                ? Effect.fail(
                                    MessageDeliveryError.make({
                                      reason: "storage",
                                      operation: "due",
                                    }),
                                  )
                                : store.due(...args),
                            ),
                          )
                        : store.due(...args),
                    nextDeadline: (...args) =>
                      failureAt === "deadline"
                        ? Ref.updateAndGet(calls, (count) => count + 1).pipe(
                            Effect.flatMap((count) =>
                              count === 1
                                ? Effect.fail(
                                    MessageDeliveryError.make({
                                      reason: "storage",
                                      operation: "deadline",
                                    }),
                                  )
                                : store.nextDeadline(...args),
                            ),
                          )
                        : store.nextDeadline(...args),
                  }),
                ),
              ).pipe(Layer.provideMerge(runtimeLayer));

              yield* Effect.gen(function* () {
                const host = yield* NodeDurableHost;
                const running = yield* Effect.forkChild(host.runWorkers(Effect.never));

                for (let count = 0; count < 128 && (yield* Ref.get(calls)) === 0; count += 1)
                  yield* Effect.yieldNow;
                expect(yield* Ref.get(calls)).toBe(1);
                yield* TestClock.adjust(9);
                expect(yield* Ref.get(calls)).toBe(1);
                yield* TestClock.adjust(1);
                for (let count = 0; count < 128 && (yield* Ref.get(calls)) < 2; count += 1)
                  yield* Effect.yieldNow;
                expect(yield* Ref.get(calls)).toBe(2);
                yield* Fiber.interrupt(running);
                yield* TestClock.adjust(100);
                expect(yield* Ref.get(calls)).toBe(2);
              }).pipe(Effect.provide(NodeDurableHost.layer.pipe(Layer.provideMerge(observed))));
            }),
          ),
        ),
      15_000,
    );
  }
});

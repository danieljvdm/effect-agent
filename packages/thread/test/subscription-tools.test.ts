import { AgentId, ThreadId } from "@effect-agent/core/Identifiers";
import { DurableStep, DurableStepError } from "@effect-agent/engine/DurableStep";
import { DefinitionDigests, Digest } from "@effect-agent/thread/Records";
import { Principal } from "@effect-agent/thread/SubmissionLedger";
import { SubscriptionSnapshot, type SubscriptionScope } from "@effect-agent/thread/Subscription";
import { Subscriptions, type SubscribeOptions } from "@effect-agent/thread/Subscriptions";
import { SubscriptionTools, subscriptionToolsLayer } from "@effect-agent/thread/SubscriptionTools";
import { NodeCrypto } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import { Effect, Layer, Schema, Stream } from "effect";

const digest = Schema.decodeSync(Digest)("c".repeat(64));
const principal = Schema.decodeSync(Principal)("tool-principal");
const agentId = Schema.decodeSync(AgentId)("tool-agent");
const threadId = Schema.decodeSync(ThreadId)("tool-thread");

const scope = Schema.decodeSync(
  Schema.Struct({
    partition: Schema.Struct({ tenantId: Schema.String, address: Schema.String }),
    ownerId: Schema.String,
    principal: Principal,
  }),
)({
  partition: { tenantId: "tenant", address: "github:101" },
  ownerId: "owner",
  principal,
});

const durableReplay = () => {
  const recorded = new Map<string, unknown>();

  const service: DurableStep["Service"] = {
    do: (name, output, execute) =>
      Effect.gen(function* () {
        if (recorded.has(name)) {
          return yield* Schema.decodeUnknownEffect(output)(recorded.get(name)).pipe(
            Effect.mapError((cause) =>
              DurableStepError.make({
                stepName: name,
                reason: "recorded-result-invalid",
                message: "Recorded test Step result is invalid",
                cause,
              }),
            ),
          );
        }
        const value = yield* execute;

        const encoded = yield* Schema.encodeEffect(output)(value).pipe(
          Effect.mapError((cause) =>
            DurableStepError.make({
              stepName: name,
              reason: "output-encoding-failed",
              message: "Test Step result could not be encoded",
              cause,
            }),
          ),
        );

        recorded.set(name, encoded);

        return value;
      }),
  };

  return service;
};

describe("restricted subscription Tools", () => {
  it.effect("binds host scope and replays a lost subscribe reply without registering twice", () =>
    Effect.gen(function* () {
      const calls: Array<{
        readonly scope: SubscriptionScope;
        readonly options: SubscribeOptions;
      }> = [];

      const subscriptions = Subscriptions.of({
        subscribe: (requestedScope, options) => {
          return Effect.sync(() => {
            calls.push({ scope: requestedScope, options });

            return SubscriptionSnapshot.make({
              key: {
                partition: requestedScope.partition,
                ownerId: requestedScope.ownerId,
                subscriptionId: options.subscriptionId,
              },
              source: options.source,
              mode: options.mode,
              state: "active",
              createdAtMillis: 50,
              expiresAtMillis: options.expiresAtMillis,
              recovery: null,
            });
          });
        },
        listSubscriptions: () => Effect.succeed({ items: [], next: null }),
        cancelSubscription: () => Effect.die("unused"),
        listDeliveries: () => Effect.die("unused"),
      });

      const handlers = subscriptionToolsLayer({
        scope,
        currentThreadId: threadId,
        deliveryPrincipal: principal,
        agentId,
        definitions: DefinitionDigests.make({ agent: digest, model: digest, tools: digest }),
        permittedSources: [
          {
            source: { name: "github-workflow-run-completed", version: "1" },
            agentId,
            description: "A completed exact GitHub Actions workflow run attempt.",
            parameters: Schema.Struct({
              runId: Schema.Int,
              attempt: Schema.Int,
              expectedHeadSha: Schema.String,
            }),
            context: Schema.Struct({ reason: Schema.String }),
          },
        ],
      }).pipe(
        Layer.provide(Layer.merge(NodeCrypto.layer, Layer.succeed(Subscriptions, subscriptions))),
      );

      const toolkit = yield* SubscriptionTools.pipe(Effect.provide(handlers));
      const step = durableReplay();

      const parameters = {
        source: { name: "github-workflow-run-completed" as const, version: "1" as const },
        parameters: { runId: 202, attempt: 3, expectedHeadSha: "a".repeat(40) },
        mode: "once" as const,
        expiresAtMillis: 9_999,
        context: { reason: "release" },
      };

      const invoke = Effect.gen(function* () {
        const stream = yield* toolkit.handle("subscribe_to_event", parameters, "provider-call");

        return yield* Stream.runCollect(stream);
      }).pipe(Effect.provideService(DurableStep, step));

      const first = yield* invoke;
      const replay = yield* invoke;

      expect(replay).toEqual(first);
      expect(calls).toHaveLength(1);
      expect(calls[0]).toMatchObject({
        scope,
        options: {
          expiresAtMillis: 9_999,
          destination: { _tag: "ExistingThread", threadId },
          deliveryPrincipal: principal,
          agentId,
        },
      });
      expect(calls[0]?.options.subscriptionId).toMatch(/^tool:/);
    }),
  );
});

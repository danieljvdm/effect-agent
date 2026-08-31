import { env, runInDurableObject } from "cloudflare:test";
import { Context, Crypto, Effect, Exit, Layer, Schema } from "effect";
import { DurableObjectState, WorkerEnvironment } from "effect-cf";
import { describe, expect, expectTypeOf, it } from "vite-plus/test";

import {
  CloudflareConversationClient,
  ConversationObject,
  ConversationObjectIdentity,
  ConversationObjectNamespace,
  DurableObjectContext,
} from "../src/index.ts";
import { PRODUCER_PREFIX, plannerDefinition, submitOptions } from "./fixtures.ts";
import { allSettled, drainAlarmsUntil, runClient, stubFor } from "./harness.ts";

class BindingSetupError extends Schema.TaggedError<BindingSetupError>()("BindingSetupError", {}) {}

class ApplicationConfig extends Context.Service<ApplicationConfig, { readonly enabled: boolean }>()(
  "@effect-agent/platform-cloudflare/test/ApplicationConfig",
) {}

const options = { deploymentId: "binding-layer", producerPrefix: "binding-layer" };

const dynamicStub = (conversation: string) =>
  env.DYNAMIC_BINDINGS.get(env.DYNAMIC_BINDINGS.idFromName(conversation));

describe("Cloudflare Agent registrations", () => {
  it("acquires once with each incarnation's yielded host services and identities", async () => {
    const firstConversation = `binding-source-first-${crypto.randomUUID()}`;
    const secondConversation = `binding-source-second-${crypto.randomUUID()}`;
    const first = dynamicStub(firstConversation);
    const second = dynamicStub(secondConversation);

    const firstProbe = await first.bindingSourceProbe();
    expect(await first.bindingSourceProbe()).toEqual(firstProbe);
    expect(firstProbe).toMatchObject({
      evaluationCount: 1,
      conversationId: firstConversation,
      producerId: `${PRODUCER_PREFIX}:${firstConversation}`,
      rawEnvHasNamespace: true,
      stateMatches: true,
    });

    const secondProbe = await second.bindingSourceProbe();
    expect(secondProbe).toMatchObject({
      evaluationCount: 1,
      conversationId: secondConversation,
      producerId: `${PRODUCER_PREFIX}:${secondConversation}`,
      rawEnvHasNamespace: true,
      stateMatches: true,
    });
    expect(secondProbe.incarnation).not.toBe(firstProbe.incarnation);

    const receipt = await runClient(
      CloudflareConversationClient.use((client) =>
        client.submit(
          { definition: plannerDefinition },
          { question: "plan", ref: firstConversation },
          submitOptions(firstConversation, "binding-layer"),
        ),
      ),
      "DYNAMIC_BINDINGS",
    );
    await drainAlarmsUntil(firstConversation, allSettled(firstConversation, "DYNAMIC_BINDINGS"), {
      namespace: "DYNAMIC_BINDINGS",
    });
    const settlement = await runClient(
      CloudflareConversationClient.use((client) => client.awaitSettlement(receipt)),
      "DYNAMIC_BINDINGS",
    );
    expect(settlement.outcome).toBe("completed");
    expect(await first.bindingSourceProbe()).toEqual(firstProbe);
  });

  it("retains application requirements and initialization failures in the Layer types", () => {
    const bindings = Effect.gen(function* () {
      const config = yield* ApplicationConfig;
      yield* WorkerEnvironment;
      yield* DurableObjectState.DurableObjectState;
      yield* ConversationObjectIdentity;
      yield* Crypto.Crypto;
      yield* Effect.scope;
      if (!config.enabled) return yield* BindingSetupError.make({});
      return [];
    });
    const runtime = ConversationObject.layer(bindings, options);
    expectTypeOf<Layer.Error<typeof runtime>>().toEqualTypeOf<
      BindingSetupError | ConversationObject.InitializationError
    >();
    expectTypeOf<Layer.Services<typeof runtime>>().toEqualTypeOf<
      | ApplicationConfig
      | WorkerEnvironment
      | DurableObjectState.DurableObjectState
      | DurableObjectContext
      | ConversationObjectNamespace
    >();

    const objectOptions = { ...options, namespaceBinding: "CONVERSATIONS" };
    type FactoryBindings = Parameters<typeof ConversationObject.make>[0];
    expectTypeOf<typeof bindings>().not.toExtend<FactoryBindings>();
    const provided = bindings.pipe(
      Effect.provide(Layer.succeed(ApplicationConfig, { enabled: true })),
    );
    expectTypeOf<Effect.Error<typeof provided>>().toEqualTypeOf<BindingSetupError>();
    ConversationObject.make(provided, objectOptions);
    expectTypeOf<typeof Effect.void>().not.toExtend<FactoryBindings>();
  });

  it("keeps application resources alive until the runtime Scope closes", () =>
    runInDurableObject(stubFor("registration-scope"), (_instance, state) =>
      Effect.runPromise(
        Effect.gen(function* () {
          const lifecycle: Array<string> = [];
          const application = Layer.effect(
            ApplicationConfig,
            Effect.acquireRelease(
              Effect.sync(() => {
                lifecycle.push("acquired");
                return { enabled: true };
              }),
              () => Effect.sync(() => lifecycle.push("released")),
            ),
          );
          const registrations = Effect.gen(function* () {
            const services = yield* Layer.build(application);
            const config = yield* ApplicationConfig.pipe(Effect.provide(services));
            expect(config.enabled).toBe(true);
            return [];
          });
          const runtime = ConversationObject.layer(registrations, options).pipe(
            Layer.provide([
              DurableObjectContext.layer(state, env),
              ConversationObjectNamespace.layer(env.CONVERSATIONS),
            ]),
          );
          yield* Effect.gen(function* () {
            yield* Layer.build(runtime);
            expect(lifecycle).toEqual(["acquired"]);
          }).pipe(Effect.scoped);
          expect(lifecycle).toEqual(["acquired", "released"]);
        }),
      ),
    ));

  it.each(["failure", "defect", "interruption"] as const)(
    "preserves registration %s and releases acquired resources",
    (kind) =>
      runInDurableObject(stubFor(`registration-${kind}`), (_instance, state) =>
        Effect.runPromise(
          Effect.gen(function* () {
            const lifecycle: Array<string> = [];
            const failure = BindingSetupError.make({});
            const bindings = Effect.gen(function* () {
              yield* Effect.acquireRelease(
                Effect.sync(() => lifecycle.push("acquired")),
                () => Effect.sync(() => lifecycle.push("released")),
              );
              if (kind === "failure") return yield* failure;
              if (kind === "defect") return yield* Effect.die("registration defect");
              return yield* Effect.interrupt;
            });
            const runtime = ConversationObject.layer(bindings, options).pipe(
              Layer.provide([
                DurableObjectContext.layer(state, env),
                ConversationObjectNamespace.layer(env.CONVERSATIONS),
              ]),
            );
            const exit = yield* Layer.build(runtime).pipe(Effect.scoped, Effect.exit);
            if (kind === "failure") expect(exit).toEqual(Exit.fail(failure));
            else if (kind === "defect") expect(exit).toEqual(Exit.die("registration defect"));
            else expect(Exit.hasInterrupts(exit)).toBe(true);
            expect(lifecycle).toEqual(["acquired", "released"]);
          }),
        ),
      ),
  );
});

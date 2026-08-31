import { digestDefinitions } from "@effect-agent/session";
import { BrowserCrypto } from "@effect/platform-browser";
import { env, runInDurableObject } from "cloudflare:test";
import { Cause, Context, Crypto, Effect, Exit, Layer, Schema } from "effect";
import { DurableObjectState, WorkerEnvironment } from "effect-cf";
import { describe, expect, expectTypeOf, it } from "vite-plus/test";

import {
  CloudflareConversationClient,
  ConversationObject,
  ConversationObjectIdentity,
  ConversationObjectNamespace,
  DurableObjectContext,
} from "../src/index.ts";
import {
  PRODUCER_PREFIX,
  plannerDefinition,
  registrationDefinitions,
  submitOptions,
} from "./fixtures.ts";
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
        Effect.gen(function* () {
          const definitions = yield* digestDefinitions(registrationDefinitions);
          return yield* client.submit(
            { definition: plannerDefinition },
            { question: "plan", ref: firstConversation },
            { ...submitOptions(firstConversation, "binding-layer"), definitions },
          );
        }),
      ).pipe(Effect.provide(BrowserCrypto.layer)),
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
    const registrations = Layer.unwrap(
      Effect.gen(function* () {
        const config = yield* ApplicationConfig;
        yield* WorkerEnvironment;
        yield* DurableObjectState.DurableObjectState;
        yield* ConversationObjectIdentity;
        yield* Crypto.Crypto;
        yield* Effect.scope;
        if (!config.enabled) return yield* BindingSetupError.make({});
        return ConversationObject.layer([]);
      }),
    );
    const runtime = registrations.pipe(Layer.provide(ConversationObject.layerConfig(options)));
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
    type FactoryLayer = Parameters<typeof ConversationObject.make>[0];
    expectTypeOf<typeof registrations>().not.toExtend<FactoryLayer>();
    const provided = registrations.pipe(
      Layer.provide(Layer.succeed(ApplicationConfig, { enabled: true })),
    );
    ConversationObject.make(provided, objectOptions);
    expectTypeOf<typeof Effect.void>().not.toExtend<FactoryLayer>();
    expectTypeOf<typeof Layer.empty>().not.toExtend<FactoryLayer>();

    // ApplicationConfig was consumed by Layer.provide, so an event cannot require it.
    // @ts-expect-error The application must expose event dependencies with Layer.provideMerge.
    ConversationObject.make(provided, {
      ...objectOptions,
      eventLayer: Layer.effectDiscard(ApplicationConfig),
    });
  });

  it("keeps ordinary application Layers alive until the runtime Scope closes", () =>
    runInDurableObject(stubFor("registration-scope"), (_instance, state) =>
      Effect.runPromise(
        Effect.gen(function* () {
          const lifecycle: Array<string> = [];
          const application = Layer.effect(
            ApplicationConfig,
            Effect.gen(function* () {
              const identity = yield* ConversationObjectIdentity;
              yield* Crypto.Crypto;
              expect(identity.conversationId).toBe("registration-scope");
              return yield* Effect.acquireRelease(
                Effect.sync(() => {
                  lifecycle.push("acquired");
                  return { enabled: true };
                }),
                () => Effect.sync(() => lifecycle.push("released")),
              );
            }),
          );
          const runtime = Layer.unwrap(
            Effect.map(ApplicationConfig, (config) => {
              expect(config.enabled).toBe(true);
              return ConversationObject.layer([]);
            }),
          ).pipe(
            Layer.provide(application),
            Layer.provide(ConversationObject.layerConfig(options)),
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

  it.each(["failure", "defect", "interruption", "timeout"] as const)(
    "preserves initialization %s and releases acquired resources",
    (kind) =>
      runInDurableObject(stubFor(`registration-${kind}`), (_instance, state) =>
        Effect.runPromise(
          Effect.gen(function* () {
            const lifecycle: Array<string> = [];
            const failure = BindingSetupError.make({});
            const application = Layer.effect(
              ApplicationConfig,
              Effect.gen(function* () {
                yield* Effect.acquireRelease(
                  Effect.sync(() => lifecycle.push("acquired")),
                  () => Effect.sync(() => lifecycle.push("released")),
                );
                if (kind === "failure") return yield* failure;
                if (kind === "defect") return yield* Effect.die("registration defect");
                if (kind === "timeout") return yield* Effect.never.pipe(Effect.timeout("0 millis"));
                return yield* Effect.interrupt;
              }),
            );
            const runtime = Layer.unwrap(
              Effect.as(ApplicationConfig, ConversationObject.layer([])),
            ).pipe(
              Layer.provide(application),
              Layer.provide(ConversationObject.layerConfig(options)),
              Layer.provide([
                DurableObjectContext.layer(state, env),
                ConversationObjectNamespace.layer(env.CONVERSATIONS),
              ]),
            );
            const exit = yield* Layer.build(runtime).pipe(Effect.scoped, Effect.exit);
            if (kind === "failure") expect(exit).toEqual(Exit.fail(failure));
            else if (kind === "defect") expect(exit).toEqual(Exit.die("registration defect"));
            else if (kind === "timeout")
              expect(
                Exit.isFailure(exit) ? Cause.findErrorOption(exit.cause) : undefined,
              ).toMatchObject({ _tag: "Some", value: { _tag: "TimeoutError" } });
            else expect(Exit.hasInterrupts(exit)).toBe(true);
            expect(lifecycle).toEqual(["acquired", "released"]);
          }),
        ),
      ),
  );
});

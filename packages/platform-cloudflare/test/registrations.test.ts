import {
  ThreadObjectIdentity,
  ThreadObjectPlacement,
  ThreadObjectNamespace,
  DurableObjectContext,
} from "@effect-agent/platform-cloudflare/cloudflare-bindings";
import { CloudflareThreadClient } from "@effect-agent/platform-cloudflare/cloudflare-thread-client";
import * as ThreadObject from "@effect-agent/platform-cloudflare/thread-object";
import { BrowserCrypto } from "@effect/platform-browser";
import { SqliteClient } from "@effect/sql-sqlite-do";
import { env, runInDurableObject } from "cloudflare:test";
import { Cause, Context, Crypto, Effect, Exit, Layer, Option, Schema } from "effect";
import { digestDefinitions, digestJson } from "effect-agent/digest";
import { AgentId, SubmissionId, ThreadId } from "effect-agent/identifiers";
import { IdempotencyKey, Principal } from "effect-agent/receipt";
import { DefinitionDigestInput, DeploymentId } from "effect-agent/records";
import { AdmissionRequest, SubmissionLedger } from "effect-agent/submission-ledger";
import { DurableObjectState, WorkerEnvironment } from "effect-cf";
import { Statement } from "effect/unstable/sql";
import { SqlClient } from "effect/unstable/sql/SqlClient";
import { describe, expect, expectTypeOf, it } from "vite-plus/test";

import { ThreadMessageDelivery } from "../src/Alarm.ts";
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

const dynamicStub = (thread: string) =>
  env.DYNAMIC_BINDINGS.get(env.DYNAMIC_BINDINGS.idFromName(thread));

describe("Cloudflare Agent registrations", () => {
  // Regression seam: https://linear.app/reve-ai/issue/KOM-125
  it("exposes the existing owner SQL client without installing Memory tables", () =>
    runInDurableObject(stubFor("registration-owner-sql"), (_instance, state) =>
      Effect.runPromise(
        Effect.gen(function* () {
          const tablesBefore = state.storage.sql
            .exec<{ name: string }>(
              "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name",
            )
            .toArray();

          const runtime = ThreadObject.layer([]).pipe(
            Layer.provide(ThreadObject.layerConfig(options)),
            Layer.provide([
              DurableObjectContext.layer(state, env),
              ThreadObjectNamespace.layer(env.THREADS),
            ]),
          );

          expectTypeOf<Extract<ThreadObject.Services, SqlClient>>().toEqualTypeOf<SqlClient>();
          expectTypeOf<
            Extract<Layer.Success<typeof runtime>, SqlClient>
          >().toEqualTypeOf<SqlClient>();

          const built = yield* Layer.build(runtime);
          const sql = Context.get(built, SqlClient);

          // Regression: https://github.com/danieljvdm/effect-agent/commit/4600d240f44b1ef1fe9b0fc58f39e293a6434f85
          // References carry no required R, but the host must still supply this actual instance.
          expect(Context.get(built, ThreadMessageDelivery)).not.toBe(
            Context.get(Context.empty(), ThreadMessageDelivery),
          );

          // Both native tags come from the one memoized infrastructure Layer shared with ports.
          expect(Context.getOption(built, SqliteClient.SqliteClient)).toEqual(Option.some(sql));
          expect(yield* sql<{ value: number }>`SELECT 1 AS value`).toEqual([{ value: 1 }]);

          const tablesAfter = state.storage.sql
            .exec<{ name: string }>(
              "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name",
            )
            .toArray();

          expect(tablesAfter).toEqual(tablesBefore);
          expect(tablesAfter.some(({ name }) => name.startsWith("effect_agent_memory_"))).toBe(
            false,
          );
        }).pipe(Effect.scoped),
      ),
    ));

  it("acquires once with each incarnation's yielded host services and identities", async () => {
    const firstThread = `binding-source-first-${crypto.randomUUID()}`;
    const secondThread = `binding-source-second-${crypto.randomUUID()}`;
    const first = dynamicStub(firstThread);
    const second = dynamicStub(secondThread);

    const firstProbe = await first.bindingSourceProbe();

    expect(await first.bindingSourceProbe()).toEqual(firstProbe);
    expect(firstProbe).toMatchObject({
      evaluationCount: 1,
      threadId: firstThread,
      producerId: `${PRODUCER_PREFIX}:${firstThread}`,
      rawEnvHasNamespace: true,
      configuredLabel: "configured-by-worker",
      stateMatches: true,
    });

    const secondProbe = await second.bindingSourceProbe();

    expect(secondProbe).toMatchObject({
      evaluationCount: 1,
      threadId: secondThread,
      producerId: `${PRODUCER_PREFIX}:${secondThread}`,
      rawEnvHasNamespace: true,
      configuredLabel: "configured-by-worker",
      stateMatches: true,
    });
    expect(secondProbe.incarnation).not.toBe(firstProbe.incarnation);

    const receipt = await runClient(
      CloudflareThreadClient.use((client) =>
        Effect.gen(function* () {
          const definitions = yield* digestDefinitions(registrationDefinitions);

          return yield* client.submit(
            { definition: plannerDefinition },
            { question: "plan", ref: firstThread },
            { ...submitOptions(firstThread, "binding-layer"), definitions },
          );
        }),
      ).pipe(Effect.provide(BrowserCrypto.layer)),
      "DYNAMIC_BINDINGS",
    );

    await drainAlarmsUntil(firstThread, allSettled(firstThread, "DYNAMIC_BINDINGS"), {
      namespace: "DYNAMIC_BINDINGS",
    });

    const settlement = await runClient(
      CloudflareThreadClient.use((client) => client.awaitSettlement(receipt)),
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
        yield* ThreadObjectIdentity;
        yield* Crypto.Crypto;
        yield* Effect.scope;
        if (!config.enabled) return yield* BindingSetupError.make({});

        return ThreadObject.layer([]);
      }),
    );

    const runtime = registrations.pipe(Layer.provide(ThreadObject.layerConfig(options)));

    expectTypeOf<Layer.Error<typeof runtime>>().toEqualTypeOf<
      BindingSetupError | ThreadObject.InitializationError
    >();
    expectTypeOf<Layer.Services<typeof runtime>>().toEqualTypeOf<
      | ApplicationConfig
      | WorkerEnvironment
      | DurableObjectState.DurableObjectState
      | DurableObjectContext
      | ThreadObjectNamespace
    >();

    const objectOptions = { ...options, namespaceBinding: "THREADS" };

    type FactoryLayer = Parameters<typeof ThreadObject.make>[0];
    expectTypeOf<typeof registrations>().not.toExtend<FactoryLayer>();

    const provided = registrations.pipe(
      Layer.provide(Layer.succeed(ApplicationConfig, { enabled: true })),
    );

    ThreadObject.make(provided, objectOptions);
    expectTypeOf<typeof Effect.void>().not.toExtend<FactoryLayer>();
    expectTypeOf<typeof Layer.empty>().not.toExtend<FactoryLayer>();

    // ApplicationConfig was consumed by Layer.provide, so an event cannot require it.
    // @ts-expect-error The application must expose event dependencies with Layer.provideMerge.
    ThreadObject.make(provided, {
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
              const identity = yield* ThreadObjectIdentity;

              yield* Crypto.Crypto;
              expect(identity.threadId).toBe("registration-scope");

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

              return ThreadObject.layer([]);
            }),
          ).pipe(
            Layer.provide(application),
            Layer.provide(ThreadObject.layerConfig(options)),
            Layer.provide([
              DurableObjectContext.layer(state, env),
              ThreadObjectNamespace.layer(env.THREADS),
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

            const runtime = Layer.unwrap(Effect.as(ApplicationConfig, ThreadObject.layer([]))).pipe(
              Layer.provide(application),
              Layer.provide(ThreadObject.layerConfig(options)),
              Layer.provide([
                DurableObjectContext.layer(state, env),
                ThreadObjectNamespace.layer(env.THREADS),
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

// Regression: https://github.com/danieljvdm/effect-agent/commit/227b5e8a98ce4d1b303bacbeaf35c15ff45f6c75
it("keeps local submission lookups inside the physical owner, including corrupt identity rows", async () => {
  const thread = `local-lookup-${crypto.randomUUID()}`;
  const colocated = ThreadId.make(`${thread}:colocated`);
  const foreign = ThreadId.make(`${thread}:foreign`);

  await runInDurableObject(stubFor(thread), (_instance, state) =>
    Effect.runPromise(
      Effect.gen(function* () {
        const runtime = ThreadObject.layer([]).pipe(
          Layer.provide(
            Layer.succeed(ThreadObjectPlacement, {
              ownsThread: (id) => id === thread || id === colocated,
            }),
          ),
          Layer.provide(ThreadObject.layerConfig(options)),
          Layer.provide([
            DurableObjectContext.layer(state, env),
            ThreadObjectNamespace.layer(env.THREADS),
          ]),
        );

        const built = yield* Layer.build(runtime);
        const ports = Context.get(built, ThreadObject.ThreadObjectPorts);
        const ledger = Context.get(built, SubmissionLedger);
        const sql = Context.get(built, SqlClient);

        const agentDigests = yield* digestDefinitions(
          DefinitionDigestInput.make({ agent: "local-lookup", model: "none", tools: "none" }),
        );

        const inputDigest = yield* digestJson({});

        const admit = (id: ThreadId) =>
          ledger.admit(
            AdmissionRequest.make({
              threadId: id,
              principal: Principal.make("local-lookup"),
              idempotencyKey: IdempotencyKey.make("local-lookup"),
              agentId: AgentId.make("local-lookup"),
              agentDigests,
              deploymentId: DeploymentId.make("local-lookup"),
              inputPayload: {},
              inputDigest,
            }),
          );

        const owned = yield* admit(ThreadId.make(thread));
        const sibling = yield* admit(colocated);

        expect(Option.getOrThrow(yield* ports.lookupSubmission(owned.submissionId)).threadId).toBe(
          thread,
        );
        expect(
          Option.getOrThrow(yield* ports.lookupSubmission(sibling.submissionId)).threadId,
        ).toBe(colocated);
        expect(yield* ports.lookupSubmission(SubmissionId.make("absent-local-submission"))).toEqual(
          Option.none(),
        );

        const foreignId = SubmissionId.make(`0198f6c0-0000-7000-8000-000000000001:${foreign}`);

        yield* sql`UPDATE effect_agent_submissions SET submission_id=${foreignId} WHERE submission_id=${owned.submissionId}`;
        let queries = 0;

        const rejected = yield* ports.lookupSubmission(foreignId).pipe(
          Effect.provideService(Statement.CurrentTransformer, (statement) => {
            queries++;

            return Effect.succeed(statement);
          }),
          Effect.result,
        );

        expect(rejected).toMatchObject({ _tag: "Failure", failure: { _tag: "LedgerError" } });
        expect(queries).toBe(0);
        expect(
          yield* ports
            .lookupSubmission(SubmissionId.make(`0198f6c0-0000-7000-8000-000000000002:${foreign}`))
            .pipe(Effect.result),
        ).toMatchObject({ _tag: "Failure", failure: { _tag: "LedgerError" } });
        expect(
          yield* ports.lookupSubmission(SubmissionId.make("x".repeat(1025))).pipe(Effect.result),
        ).toMatchObject({ _tag: "Failure", failure: { _tag: "LedgerError" } });

        yield* sql`UPDATE effect_agent_submissions SET submission_id=${owned.submissionId},thread_id=${foreign} WHERE submission_id=${foreignId}`;
        expect(yield* ports.lookupSubmission(owned.submissionId).pipe(Effect.result)).toMatchObject(
          { _tag: "Failure", failure: { _tag: "LedgerError" } },
        );
        const opaqueId = SubmissionId.make("opaque-local-submission");

        yield* sql`UPDATE effect_agent_submissions SET submission_id=${opaqueId} WHERE submission_id=${owned.submissionId}`;
        expect(yield* ports.lookupSubmission(opaqueId).pipe(Effect.result)).toMatchObject({
          _tag: "Failure",
          failure: { _tag: "LedgerError" },
        });
        yield* sql`UPDATE effect_agent_submissions SET thread_id=${thread} WHERE submission_id=${opaqueId}`;
        expect(Option.getOrThrow(yield* ports.lookupSubmission(opaqueId)).threadId).toBe(thread);
        yield* sql`UPDATE effect_agent_submissions SET input_json='{' WHERE submission_id=${opaqueId}`;
        expect(yield* ports.lookupSubmission(opaqueId).pipe(Effect.result)).toMatchObject({
          _tag: "Failure",
          failure: { _tag: "LedgerError" },
        });
      }).pipe(Effect.provide(BrowserCrypto.layer), Effect.scoped),
    ),
  );
});

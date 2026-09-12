import * as Agent from "@effect-agent/core/Agent";
import * as AgentUpdates from "@effect-agent/core/AgentUpdates";
import { IdGenerator } from "@effect-agent/core/IdGenerator";
import { IdempotencyKey } from "@effect-agent/core/Receipt";
import { expect, layer } from "@effect/vitest";
import { Deferred, Effect, Exit, Fiber, Layer, Ref, Schema, Stream } from "effect";
import { LanguageModel, Model, type Response, Tool, Toolkit } from "effect/unstable/ai";

import * as AgentRuntime from "../src/AgentRuntime.ts";
import { AgentUpdateAcceptance, ModelUsageAccounting } from "../src/RunOptions.ts";
import { ThreadHistory } from "../src/ThreadHistory.ts";

const usage = { inputTokens: {}, outputTokens: {} };

const model = (name: string, params: unknown) =>
  Model.make(
    "test",
    "updates",
    Layer.effect(
      LanguageModel.LanguageModel,
      Effect.gen(function* () {
        const turns = yield* Ref.make(0);

        return yield* LanguageModel.make({
          generateText: () => Effect.succeed([]),
          streamText: () =>
            Stream.unwrap(
              Ref.getAndUpdate(turns, (n) => n + 1).pipe(
                Effect.map((turn) =>
                  Stream.fromIterable<Response.StreamPartEncoded>(
                    turn === 0
                      ? [
                          {
                            type: "tool-call",
                            id: "update-call",
                            name,
                            params,
                            providerExecuted: false,
                          },
                          { type: "finish", reason: "tool-calls", usage },
                        ]
                      : [
                          { type: "text-start", id: "answer" },
                          { type: "text-delta", id: "answer", delta: '"done"' },
                          { type: "text-end", id: "answer" },
                          { type: "finish", reason: "stop", usage },
                        ],
                  ),
                ),
              ),
            ),
        });
      }),
    ),
  );

const definition = Agent.make("updates", {
  input: Schema.String,
  output: Schema.String,
  updates: Schema.Struct({ candidateCount: Schema.NumberFromString }),
  instructions: "Report findings then finish",
  toolkit: Toolkit.empty,
});

const base = Layer.mergeAll(IdGenerator.layer, ThreadHistory.layerTransient);

layer(base)("Agent updates", (it) => {
  it.effect(
    "encodes native updates once and decodes observed values without completing the run",
    () =>
      Effect.gen(function* () {
        const events = yield* AgentRuntime.stream(
          Agent.withModel(definition, model("emit_update", { value: { candidateCount: "2" } })),
          "go",
        ).pipe(Stream.runCollect);

        const updates = events.filter((event) => event._tag === "AgentUpdateEmitted");

        expect(updates).toHaveLength(1);
        expect(updates[0]!.update.value).toEqual({ candidateCount: "2" });
        expect(yield* AgentUpdates.decode(definition, updates[0]!.update)).toEqual({
          candidateCount: 2,
        });
        expect(events.at(-1)?._tag).toBe("RunCompleted");
      }),
  );

  it.effect("publishes only acknowledged durable updates", () =>
    Effect.gen(function* () {
      const events = yield* AgentRuntime.streamWithUsageAccountingUnknown(
        Agent.withModel(definition, model("emit_update", { value: { candidateCount: "2" } })),
        "go",
      ).pipe(
        Stream.runCollect,
        Effect.provide(ModelUsageAccounting.layerEphemeral),
        Effect.provideService(AgentUpdateAcceptance, {
          accept: () => Effect.fail(new AgentUpdates.UpdateError({ reason: "storage" })),
        }),
      );

      expect(events.some((event) => event._tag === "AgentUpdateEmitted")).toBe(false);
      expect(events.at(-1)?._tag).toBe("RunCompleted");
    }),
  );

  it.effect("waits for host acceptance and publishes the canonical sequence", () =>
    Effect.gen(function* () {
      const entered = yield* Deferred.make<void>();
      const accepted = yield* Deferred.make<void>();
      const observed = yield* Ref.make<ReadonlyArray<string>>([]);

      const fiber = yield* AgentRuntime.streamWithUsageAccountingUnknown(
        Agent.withModel(definition, model("emit_update", { value: { candidateCount: "2" } })),
        "go",
      ).pipe(
        Stream.tap((event) => Ref.update(observed, (tags) => [...tags, event._tag])),
        Stream.runCollect,
        Effect.provide(ModelUsageAccounting.layerEphemeral),
        Effect.provideService(AgentUpdateAcceptance, {
          accept: (update) =>
            Effect.gen(function* () {
              yield* Deferred.succeed(entered, undefined);
              yield* Deferred.await(accepted);

              return AgentUpdates.Update.make({ ...update, sequence: 42 });
            }),
        }),
        Effect.forkChild,
      );

      yield* Deferred.await(entered);
      expect(yield* Ref.get(observed)).not.toContain("AgentUpdateEmitted");
      yield* Deferred.succeed(accepted, undefined);
      const events = yield* Fiber.join(fiber);
      const updates = events.filter((event) => event._tag === "AgentUpdateEmitted");

      expect(updates).toHaveLength(1);
      expect(updates[0]!.update.sequence).toBe(42);
      expect(events.at(-1)?._tag).toBe("RunCompleted");
    }),
  );

  it.effect(
    "rejects invalid model updates and cumulative byte overflow without dropping completion",
    () =>
      Effect.gen(function* () {
        for (const params of [
          { value: { candidateCount: "invalid" } },
          { value: { candidateCount: "2" } },
        ]) {
          const events = yield* AgentRuntime.stream(
            Agent.withModel(definition, model("emit_update", params)),
            "go",
            { updates: { maxBytes: 1 } },
          ).pipe(Stream.runCollect);

          expect(events.some((event) => event._tag === "AgentUpdateEmitted")).toBe(false);
          expect(events.at(-1)?._tag).toBe("RunCompleted");
        }
      }),
  );

  it.effect("deduplicates keys, rejects changed values and closes captured emitters", () =>
    Effect.gen(function* () {
      const Job = Tool.make("job", {
        parameters: Schema.Struct({}),
        success: Schema.Void,
        failure: AgentUpdates.UpdateError,
        dependencies: [AgentUpdates.Emitter],
      });

      const tools = Toolkit.make(Job);

      const agent = Agent.make("programmatic-updates", {
        input: Schema.String,
        output: Schema.String,
        updates: Schema.String,
        instructions: "Work then finish",
        toolkit: tools,
      });

      let captured: AgentUpdates.Emitter["Service"] | undefined;
      const key = Schema.decodeSync(IdempotencyKey)("stable");

      const handlers = tools.toLayer({
        job: () =>
          Effect.gen(function* () {
            captured = yield* AgentUpdates.Emitter;

            const first = yield* AgentUpdates.emit(agent, "candidateCount", {
              idempotencyKey: key,
            });

            const second = yield* AgentUpdates.emit(agent, "candidateCount", {
              idempotencyKey: key,
            });

            expect(second).toEqual(first);
            expect(
              yield* AgentUpdates.emit(agent, "changed", { idempotencyKey: key }).pipe(
                Effect.match({ onFailure: (error) => error, onSuccess: () => undefined }),
              ),
            ).toEqual(new AgentUpdates.UpdateError({ reason: "conflict" }));
            expect(
              yield* AgentUpdates.emit(agent, "more", {
                idempotencyKey: Schema.decodeSync(IdempotencyKey)("second"),
              }).pipe(Effect.match({ onFailure: (error) => error, onSuccess: () => undefined })),
            ).toEqual(new AgentUpdates.UpdateError({ reason: "capacity" }));
          }),
      });

      const events = yield* AgentRuntime.stream(Agent.withModel(agent, model("job", {})), "go", {
        updates: { maxCount: 1 },
      }).pipe(Stream.runCollect, Effect.provide(handlers));

      expect(events.filter((event) => event._tag === "AgentUpdateEmitted")).toHaveLength(1);
      expect(captured).toBeDefined();
      expect(
        yield* captured!
          .emit({ target: agent, updateId: key, value: "candidateCount" })
          .pipe(Effect.match({ onFailure: (error) => error, onSuccess: () => undefined })),
      ).toEqual(new AgentUpdates.UpdateError({ reason: "unavailable" }));
    }),
  );

  it.effect("retains accepted values when callers and hosts mutate their references", () =>
    Effect.gen(function* () {
      const tools = Toolkit.make(
        Tool.make("job", {
          parameters: Schema.Struct({}),
          success: Schema.Void,
          failure: AgentUpdates.UpdateError,
          dependencies: [AgentUpdates.Emitter],
        }),
      );

      const agent = Agent.make("owned-updates", {
        input: Schema.String,
        output: Schema.String,
        updates: Schema.Struct({ finding: Schema.String }),
        instructions: "Report findings then finish",
        toolkit: tools,
      });

      const key = Schema.decodeSync(IdempotencyKey)("stable");
      const hostValue = { finding: "accepted" };

      const handlers = tools.toLayer({
        job: () =>
          Effect.gen(function* () {
            const input = { finding: "accepted" };
            const first = yield* AgentUpdates.emit(agent, input, { idempotencyKey: key });

            input.finding = "changed input";
            hostValue.finding = "changed host value";
            Reflect.set(first, "sequence", 99);
            if (typeof first.value === "object" && first.value !== null)
              Reflect.set(first.value, "finding", "changed acknowledgement");
            Reflect.set(first, "value", { finding: "replacement acknowledgement" });

            const retry = yield* AgentUpdates.emit(
              agent,
              { finding: "accepted" },
              { idempotencyKey: key },
            );

            expect(retry.sequence).toBe(42);
            expect(yield* AgentUpdates.decode(agent, retry)).toEqual({ finding: "accepted" });
          }),
      });

      const events = yield* AgentRuntime.streamWithUsageAccountingUnknown(
        Agent.withModel(agent, model("job", {})),
        "go",
      ).pipe(
        Stream.runCollect,
        Effect.provide(Layer.mergeAll(ModelUsageAccounting.layerEphemeral, handlers)),
        Effect.provideService(AgentUpdateAcceptance, {
          accept: (update) =>
            Effect.sync(() => {
              if (typeof update.value === "object" && update.value !== null)
                Reflect.set(update.value, "finding", "changed acceptance request");

              return AgentUpdates.Update.make({ ...update, sequence: 42, value: hostValue });
            }),
        }),
      );

      const updates = events.filter((event) => event._tag === "AgentUpdateEmitted");

      expect(updates).toHaveLength(1);
      expect(updates[0]!.update.sequence).toBe(42);
      expect(yield* AgentUpdates.decode(agent, updates[0]!.update)).toEqual({
        finding: "accepted",
      });
      expect(events.at(-1)?._tag).toBe("RunCompleted");
    }),
  );

  it.effect(
    "validates decoded constraints and retains emission across effective policy copies",
    () =>
      Effect.gen(function* () {
        const tools = Toolkit.make(
          Tool.make("job", {
            parameters: Schema.Struct({}),
            success: Schema.Void,
            failure: AgentUpdates.UpdateError,
            dependencies: [AgentUpdates.Emitter],
          }),
        );

        const agent = Agent.make("constrained-updates", {
          input: Schema.String,
          output: Schema.String,
          updates: Schema.NumberFromString.check(Schema.isGreaterThan(0)),
          instructions: "Report the number of verified candidates.",
          toolkit: tools,
        });

        const handlers = tools.toLayer({
          job: () =>
            Effect.gen(function* () {
              const emitter = yield* AgentUpdates.Emitter;

              const rejected = yield* emitter
                .emit({
                  target: agent,
                  updateId: Schema.decodeSync(IdempotencyKey)("invalid"),
                  value: "-1",
                })
                .pipe(Effect.result);

              expect(rejected).toMatchObject({
                _tag: "Failure",
                failure: { reason: "validation" },
              });
              yield* AgentUpdates.emit(agent, 2, {
                idempotencyKey: Schema.decodeSync(IdempotencyKey)("valid"),
              });
            }),
        });

        const effective = { ...agent, policy: { ...agent.policy, maxTurns: 2 } };

        const events = yield* AgentRuntime.stream(
          Agent.withModel(effective, model("job", {})),
          "go",
        ).pipe(Stream.runCollect, Effect.provide(handlers));

        const updates = events.filter((event) => event._tag === "AgentUpdateEmitted");

        expect(updates).toHaveLength(1);
        expect(yield* AgentUpdates.decode(agent, updates[0]!.update)).toBe(2);
        expect(events.at(-1)?._tag).toBe("RunCompleted");
      }),
  );
  it.effect("closes emission after interruption and handler defects", () =>
    Effect.gen(function* () {
      for (const mode of ["interrupt", "defect"] as const) {
        const captured = yield* Deferred.make<AgentUpdates.Emitter["Service"]>();

        const Job = Tool.make("job", {
          parameters: Schema.Struct({}),
          success: Schema.Void,
          dependencies: [AgentUpdates.Emitter],
        });

        const tools = Toolkit.make(Job);

        const agent = Agent.make("scoped-updates", {
          input: Schema.String,
          output: Schema.String,
          updates: Schema.String,
          instructions: "Find evidence",
          toolkit: tools,
        });

        const handlers = tools.toLayer({
          job: () =>
            Effect.gen(function* () {
              const emitter = yield* AgentUpdates.Emitter;

              yield* Deferred.succeed(captured, emitter);

              return yield* mode === "defect" ? Effect.die("test defect") : Effect.never;
            }),
        });

        const fiber = yield* AgentRuntime.run(Agent.withModel(agent, model("job", {})), "go").pipe(
          Effect.provide(handlers),
          Effect.forkChild,
        );

        const emitter = yield* Deferred.await(captured);

        if (mode === "interrupt") yield* Fiber.interrupt(fiber);
        const exit = yield* Fiber.await(fiber);

        expect(Exit.isFailure(exit)).toBe(true);

        const failure = yield* emitter
          .emit({
            target: agent,
            updateId: Schema.decodeSync(IdempotencyKey)("late"),
            value: "finding",
          })
          .pipe(Effect.match({ onFailure: (error) => error, onSuccess: () => undefined }));

        expect(failure).toEqual(new AgentUpdates.UpdateError({ reason: "unavailable" }));
      }
    }),
  );
});

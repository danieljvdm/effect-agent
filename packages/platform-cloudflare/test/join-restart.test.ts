import {
  DurableObjectContext,
  threadNamespaceLayer,
} from "@effect-agent/platform-cloudflare/cloudflare-bindings";
import * as ThreadObject from "@effect-agent/platform-cloudflare/thread-object";
import { env, runInDurableObject } from "cloudflare:test";
import { Clock, Deferred, Effect, Fiber, Layer, Schema, Stream } from "effect";
import * as Agent from "effect-agent/agent";
import { DurableAgentRuntime } from "effect-agent/durable-agent-runtime";
import type { Receipt } from "effect-agent/receipt";
import { SubmissionLedger } from "effect-agent/submission-ledger";
import { ThreadStore, ThreadExportRequest } from "effect-agent/thread-store";
import { LanguageModel, Model, Tool, Toolkit, type Response } from "effect/unstable/ai";
import { expect, it } from "vite-plus/test";

import { submitOptions } from "./fixtures.ts";
import { stubFor } from "./harness.ts";

// Requested Cloudflare seam: authoritative join claims, canonical reply coverage and
// eviction during replacement. Gated provider/handlers force the side-effect race.
// Regressions in b49078f7: recovered replacement backlog was truncated; rejected
// input cancelled a paid call. Gate real ledger claims to force both races.
it.each(["reply", "tools", "eviction", "backlog", "rejected"] as const)(
  "retains joined input across %s boundaries without replaying a tool",
  async (scenario) => {
    const thread = `cf-join-restart-${scenario}`;
    const evicts = scenario === "eviction" || scenario === "backlog";
    const usesTools = scenario === "tools" || scenario === "eviction";
    const prompts: Array<string> = [];
    const executions: Array<string> = [];
    const first = Deferred.makeUnsafe<void>();
    const replacement = Deferred.makeUnsafe<void>();
    const secondReplacement = Deferred.makeUnsafe<void>();
    const joinBatches = [Deferred.makeUnsafe<void>(), Deferred.makeUnsafe<void>()];
    const rejected = Deferred.makeUnsafe<void>();
    const releaseFirst = Deferred.makeUnsafe<void>();
    const joinedReceipts: Array<Receipt> = [];
    const toolStarted = Deferred.makeUnsafe<void>();
    const releaseTool = Deferred.makeUnsafe<void>();
    let recovering = false;
    const liveClock = Effect.runSync(Clock.Clock);
    const nowMillis = () => Date.now() + (recovering ? 31_000 : 0);

    const clock: Clock.Clock = {
      currentTimeMillisUnsafe: nowMillis,
      currentTimeMillis: Effect.sync(nowMillis),
      currentTimeNanosUnsafe: () => BigInt(nowMillis()) * 1_000_000n,
      currentTimeNanos: Effect.sync(() => BigInt(nowMillis()) * 1_000_000n),
      monotonicTimeNanosUnsafe: () => liveClock.monotonicTimeNanosUnsafe(),
      monotonicTimeNanos: liveClock.monotonicTimeNanos,
      sleep: (duration) => liveClock.sleep(duration),
    };

    const tools = Toolkit.make(
      Tool.make("work", {
        parameters: Schema.Struct({ name: Schema.String }),
        success: Schema.String,
      }),
    );

    const model = Model.make(
      "scripted",
      "join-restart",
      Layer.effect(
        LanguageModel.LanguageModel,
        LanguageModel.make({
          generateText: () => Effect.succeed([]),
          streamText: (options) =>
            Stream.unwrap(
              Effect.gen(function* () {
                const index = prompts.length;
                const prompt = JSON.stringify(options.prompt);

                prompts.push(prompt);
                if (index === 0) {
                  return Stream.fromIterable<Response.StreamPartEncoded>([
                    { type: "text-start", id: "stale" },
                    { type: "text-delta", id: "stale", delta: '"stale"' },
                    { type: "text-end", id: "stale" },
                    {
                      type: "finish",
                      reason: "stop",
                      usage: { inputTokens: { total: 10 }, outputTokens: { total: 2 } },
                    },
                  ]).pipe(
                    Stream.concat(
                      Stream.fromEffect(
                        Deferred.succeed(first, undefined).pipe(
                          Effect.andThen(
                            scenario === "rejected" ? Deferred.await(releaseFirst) : Effect.never,
                          ),
                        ),
                      ).pipe(Stream.drain),
                    ),
                  );
                }
                yield* Deferred.succeed(replacement, undefined);
                if (index === 2) yield* Deferred.succeed(secondReplacement, undefined);
                if (evicts && !recovering) return Stream.never;

                const parts: ReadonlyArray<Response.StreamPartEncoded> =
                  usesTools && !prompt.includes('"tool-result"')
                    ? [
                        {
                          type: "tool-call",
                          id: `left-${index}`,
                          name: "work",
                          params: { name: "left" },
                        },
                        {
                          type: "tool-call",
                          id: `right-${index}`,
                          name: "work",
                          params: { name: "right" },
                        },
                        {
                          type: "finish",
                          reason: "tool-calls",
                          usage: { inputTokens: {}, outputTokens: {} },
                        },
                      ]
                    : [
                        { type: "text-start", id: "answer" },
                        { type: "text-delta", id: "answer", delta: '"both inputs"' },
                        { type: "text-end", id: "answer" },
                        {
                          type: "finish",
                          reason: "stop",
                          usage: { inputTokens: {}, outputTokens: {} },
                        },
                      ];

                return Stream.fromIterable(parts);
              }),
            ),
        }),
      ),
    );

    const agent = Agent.withModel(
      Agent.make("join-restart", {
        input: Schema.Struct({ question: Schema.String, ref: Schema.String }),
        output: Schema.String,
        instructions: "Answer every input together.",
        inputPrompt: (input) =>
          input.question === "reject-this-input"
            ? Effect.fail("Rejected joined prompt")
            : Effect.succeed(JSON.stringify(input)),
        toolkit: tools,
        policy: { maxTurns: usesTools ? 5 : 1, toolConcurrency: 2, restartOnJoinedInput: true },
      }),
      model,
    );

    const handlers = tools.toLayer({
      work: ({ name }) =>
        Effect.gen(function* () {
          executions.push(name);
          yield* Deferred.succeed(toolStarted, undefined);
          yield* Deferred.await(releaseTool);

          return name;
        }),
    });

    const observedLedger = Layer.effect(
      SubmissionLedger,
      Effect.map(SubmissionLedger, (ledger) =>
        SubmissionLedger.of({
          ...ledger,
          claimJoining: (request) =>
            Effect.gen(function* () {
              const gate = joinBatches[prompts.length - 1];

              if (scenario === "backlog" && !recovering && gate !== undefined)
                yield* Deferred.await(gate);

              return yield* ledger.claimJoining(request);
            }),
          revertJoining: (request) =>
            ledger
              .revertJoining(request)
              .pipe(Effect.tap(() => Deferred.succeed(rejected, undefined))),
        }),
      ),
    );

    const run = <A, E>(
      body: Effect.Effect<A, E, DurableAgentRuntime | ThreadStore | DurableObjectContext>,
    ) =>
      runInDurableObject(stubFor(thread), (_instance, state) =>
        Effect.runPromise(
          body.pipe(
            Effect.provideService(DurableObjectContext, { ctx: state, env }),
            Effect.provide(
              DurableAgentRuntime.layer.pipe(
                Layer.provide(observedLedger),
                Layer.provideMerge(ThreadObject.layer([])),
                Layer.provide(
                  ThreadObject.layerConfig({
                    deploymentId: "join-restart",
                    producerPrefix: "join-restart",
                  }),
                ),
                Layer.provide([
                  DurableObjectContext.layer(state, env),
                  threadNamespaceLayer(env, "THREADS"),
                ]),
              ),
            ),
            Effect.provideService(Clock.Clock, clock),
          ),
        ),
      );

    let receipts: { readonly receipt: Receipt; readonly joined: Receipt } | undefined;

    const initialAttempt = run(
      Effect.scoped(
        Effect.gen(function* () {
          const runtime = yield* DurableAgentRuntime;

          const receipt = yield* runtime.submit(
            agent,
            { question: "first", ref: thread },
            submitOptions(thread, "first"),
          );

          const pending = yield* runtime
            .processThread(agent, receipt.threadId)
            .pipe(Effect.provide(handlers), Effect.forkScoped);

          yield* Deferred.await(first);

          const joined = yield* runtime.submit(
            agent,
            { question: scenario === "rejected" ? "reject-this-input" : "joined", ref: thread },
            submitOptions(thread, "joined"),
          );

          receipts = { receipt, joined };
          joinedReceipts.push(joined);
          if (scenario === "rejected") {
            yield* Deferred.await(rejected).pipe(Effect.timeout("2 seconds"));
            yield* Deferred.succeed(releaseFirst, undefined);
          } else if (scenario === "backlog") {
            for (let index = 1; index < 32; index++) {
              joinedReceipts.push(
                yield* runtime.submit(
                  agent,
                  { question: `joined-${index}-end`, ref: thread },
                  submitOptions(thread, `joined-${index}`),
                ),
              );
            }
            yield* Deferred.succeed(joinBatches[0]!, undefined);
            yield* Deferred.await(replacement).pipe(Effect.timeout("2 seconds"));
            for (let index = 32; index < 64; index++) {
              joinedReceipts.push(
                yield* runtime.submit(
                  agent,
                  { question: `joined-${index}-end`, ref: thread },
                  submitOptions(thread, `joined-${index}`),
                ),
              );
            }
            yield* Deferred.succeed(joinBatches[1]!, undefined);
            yield* Deferred.await(secondReplacement).pipe(Effect.timeout("2 seconds"));
          } else yield* Deferred.await(replacement).pipe(Effect.timeout("2 seconds"));
          if (evicts) {
            const { ctx } = yield* DurableObjectContext;

            // Abort the Object while the replacement is running: no graceful release
            // of its ownership or model Scope precedes the storage reconstruction.
            ctx.abort("joined-input replacement eviction");
          }
          if (scenario === "tools") {
            yield* Deferred.await(toolStarted);
            yield* runtime.submit(
              agent,
              { question: "after-tool", ref: thread },
              submitOptions(thread, "after-tool"),
            );
            yield* Deferred.succeed(releaseTool, undefined);
          }
          yield* Fiber.join(pending);

          return { receipt, joined };
        }),
      ),
    );

    if (evicts) {
      await expect(initialAttempt).rejects.toThrow("joined-input replacement eviction");
    } else {
      await initialAttempt;
    }
    const admitted = receipts;

    if (admitted === undefined) throw new Error("Expected both durable receipts");

    if (evicts) {
      // The dead owner's lease expires before the new incarnation may claim the Run.
      recovering = true;
      await run(
        Effect.scoped(
          Effect.gen(function* () {
            const runtime = yield* DurableAgentRuntime;

            const pending = yield* runtime
              .processThread(agent, admitted.receipt.threadId)
              .pipe(Effect.provide(handlers), Effect.forkScoped);

            if (usesTools) {
              yield* Deferred.await(toolStarted);
              yield* runtime.submit(
                agent,
                { question: "after-tool", ref: thread },
                submitOptions(thread, "after-tool"),
              );
              yield* Deferred.succeed(releaseTool, undefined);
            }
            yield* Fiber.join(pending);
          }),
        ),
      );
    }
    await run(
      Effect.gen(function* () {
        const runtime = yield* DurableAgentRuntime;
        const store = yield* ThreadStore;

        expect(yield* runtime.submissionStatus(admitted.receipt)).toMatchObject({
          _tag: "settled",
        });
        expect(yield* runtime.submissionStatus(admitted.joined)).toMatchObject({ _tag: "settled" });

        const records = (yield* store.export(
          ThreadExportRequest.make({ threadId: admitted.receipt.threadId }),
        )).records;

        const responses = records.filter(
          ({ record }) => record.payload._tag === "ModelResponseRecorded",
        );

        if (scenario === "backlog") {
          // Every previously consumed message must be present before this one-turn reply.
          for (let index = 1; index < 64; index++)
            expect(prompts[3]).toContain(`joined-${index}-end`);
          expect(prompts).toHaveLength(4);
          for (const joined of joinedReceipts)
            expect(yield* runtime.submissionStatus(joined)).toMatchObject({ _tag: "settled" });
        }
        expect(responses).toHaveLength(usesTools ? 2 : 1);
        expect(records.filter(({ record }) => record.payload._tag === "RunCompleted")).toHaveLength(
          1,
        );
        expect(prompts[scenario === "rejected" ? 0 : 1]).toContain("first");
        if (scenario === "rejected") {
          expect(prompts).toHaveLength(1);
          expect(prompts[0]).not.toContain("reject-this-input");
        } else expect(prompts[1]).toContain("joined");
        expect(
          records
            .filter(({ record }) => record.payload._tag === "ModelCallAborted")
            .map(({ record }) => record.payload),
        ).toMatchObject(
          scenario === "rejected"
            ? []
            : [
                {
                  restart: 1,
                  reason: "joined-input",
                  modelUsage: [{ inputTokens: { total: 10 }, outputTokens: { total: 2 } }],
                },
                ...(scenario === "backlog" ? [{ restart: 2 }] : []),
              ],
        );
        if (scenario === "eviction") {
          expect(prompts[2]).toContain("first");
          expect(prompts[2]).toContain("joined");
          expect(prompts[2]).not.toContain("stale");
        }
        if (usesTools) {
          expect(prompts.at(-1)).toContain("after-tool");
          expect(executions.toSorted()).toEqual(["left", "right"]);
          expect(
            records.filter(({ record }) => record.payload._tag === "ToolCallSettled"),
          ).toHaveLength(2);
        }

        const input = records.find(
          ({ record }) =>
            record.payload._tag === "UserInputRecorded" &&
            record.payload.submissionId === admitted.joined.submissionId,
        );

        if (scenario !== "rejected") {
          expect(input).toBeDefined();
          expect(responses[0]!.sequence).toBeGreaterThan(input!.sequence);
        }

        // Input arriving after the reply committed gets the next Run, never rewrites it.
        const late = yield* runtime.submit(
          agent,
          { question: "after-reply", ref: thread },
          submitOptions(thread, "after-reply"),
        );

        yield* runtime.processThread(agent, late.threadId).pipe(Effect.provide(handlers));
        expect(yield* runtime.submissionStatus(late)).toMatchObject({ _tag: "settled" });
        expect(executions.toSorted()).toEqual(usesTools ? ["left", "right"] : []);
      }),
    );
  },
  10_000,
);

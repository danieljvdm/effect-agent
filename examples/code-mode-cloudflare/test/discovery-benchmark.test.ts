import { Effect, Exit, Fiber, Layer, Stream } from "effect";
import { beforeAll, describe, expect, it } from "vite-plus/test";

import { bundleWorker, openRuntime, runSample } from "../src/discovery-benchmark-command.ts";
import {
  grade,
  modes,
  type Request,
  type Result,
  type Sample,
  seeds,
  workloads,
} from "../src/discovery-benchmark-contracts.ts";
import {
  businessTools,
  businessToolkit,
  expected,
  FixtureObserver,
  handlers,
  Names,
  question,
  requiredNames,
} from "../src/discovery-benchmark-fixture.ts";

const success: Result = {
  passed: true,
  failure: null,
  output: { amountCents: 1, receipts: ["r_opaque"] },
  firstUsefulActionMillis: 1,
  firstUsefulResultMillis: 1,
  modelCalls: 2,
  modelFinalizers: 2,
  businessCalls: ["read"],
  discoveryCalls: 0,
  codeCalls: 0,
  audits: [],
  costMicrousd: 0,
  pendingMicrousd: 0,
  scopeClosed: true,
};

const passingSamples = (): Array<Sample> =>
  [0, 1].flatMap((cohort) =>
    seeds.flatMap((seed, block) =>
      modes.flatMap((mode) =>
        workloads.map((workload) => ({
          cohort,
          block,
          seed,
          mode,
          workload,
          taskMillis: mode === "eager" || workload === "common" ? 2_000 : 1_000,
          result: success,
        })),
      ),
    ),
  );

describe("tool discovery benchmark grading", () => {
  it("observes invalid business reads even when their returned failure envelope is caught", async () => {
    const failed: Array<string> = [];
    const succeeded: Array<string> = [];

    const observer = FixtureObserver.of({
      seed: 17,
      start: () => Effect.void,
      end: (name) =>
        Effect.sync(() => {
          succeeded.push(name);
        }),
      fail: (name) =>
        Effect.sync(() => {
          failed.push(name);
        }),
    });

    await Effect.runPromise(
      Effect.gen(function* () {
        const toolkit = yield* businessToolkit;

        yield* toolkit
          .handle(Names.fee, { key: "guessed-key" })
          .pipe(Effect.flatMap(Stream.runCollect));
        yield* toolkit
          .handle(Names.invoice, { key: "acct_17" })
          .pipe(Effect.flatMap(Stream.runCollect));
      }).pipe(
        Effect.provide(handlers.pipe(Layer.provide(Layer.succeed(FixtureObserver, observer)))),
        Effect.scoped,
      ),
    );
    expect(failed).toEqual([Names.fee]);
    expect(succeeded).toEqual([Names.invoice]);
  });

  it("requires both independent cohorts and keeps native and Code Mode conclusions separate", () => {
    const samples = passingSamples();

    expect(grade(samples)).toMatchObject({
      complete: true,
      nativePassed: true,
      codeModePassed: true,
    });

    const nativeRegression = samples.map((s) =>
      s.mode === "native" ? { ...s, taskMillis: 2_100 } : s,
    );

    expect(grade(nativeRegression)).toMatchObject({ nativePassed: false, codeModePassed: true });

    const failedReplication = samples.map((s) =>
      s.cohort === 1 && s.mode === "native" ? { ...s, taskMillis: 2_000 } : s,
    );

    expect(grade(failedReplication).nativePassed).toBe(false);
  });

  it("rejects missing, duplicated, failed, unresolved and unclosed cells", () => {
    const samples = passingSamples();

    expect(grade(samples.slice(1)).complete).toBe(false);
    expect(grade([samples[1]!, ...samples.slice(1)]).complete).toBe(false);
    for (const result of [
      { ...success, passed: false },
      { ...success, pendingMicrousd: 1 },
      { ...success, scopeClosed: false },
    ]) {
      expect(grade([{ ...samples[0]!, result }, ...samples.slice(1)]).complete).toBe(false);
    }
  });

  it("enforces the absolute improvement and common regression thresholds", () => {
    const smallImprovement = passingSamples().map((s) => ({
      ...s,
      taskMillis: s.mode === "eager" ? 1_000 : 800,
    }));

    expect(grade(smallImprovement).nativePassed).toBe(false);

    const commonRegression = passingSamples().map((s) =>
      s.mode === "native" && s.workload === "common" ? { ...s, taskMillis: 2_501 } : s,
    );

    expect(grade(commonRegression).nativePassed).toBe(false);

    const allowedCommon = passingSamples().map((s) =>
      s.mode === "native" && s.workload === "common" ? { ...s, taskMillis: 2_499 } : s,
    );

    expect(grade(allowedCommon).nativePassed).toBe(true);
  });
});

describe("credential-free benchmark in the real isolated executor", () => {
  let script = "";

  beforeAll(async () => {
    script = await Effect.runPromise(bundleWorker());
  }, 120_000);

  it("validates every mode/workload, exact receipts, model consumption and resource release", async () => {
    let disposed = false;

    await Effect.runPromise(
      Effect.gen(function* () {
        const runtime = yield* openRuntime(script, "", () => {
          disposed = true;
        });

        expect(businessTools).toHaveLength(120);
        for (const mode of modes)
          for (const workload of workloads) {
            const request: Request = {
              mode,
              workload,
              seed: 17,
              live: false,
              model: "scripted",
              prices: { input: 0, cachedInput: 0, output: 0 },
              remainingMicrousd: 0,
            };

            for (const receipt of expected(request).receipts)
              expect(question(request)).not.toContain(receipt);
            const { result } = yield* runSample(runtime, request);

            expect(result.failure, `${mode}/${workload}`).toBeNull();
            expect(result.passed).toBe(true);
            expect(result.output).toEqual(expected(request));
            expect(result.businessCalls).toEqual(requiredNames(request));
            expect(result.scopeClosed).toBe(true);
            expect(result.modelFinalizers).toBe(result.modelCalls);
            expect(result.audits).toEqual([]);
            expect(result.codeCalls).toBe(mode === "code-mode" && workload !== "common" ? 1 : 0);
          }
      }).pipe(Effect.scoped),
    );
    expect(disposed).toBe(true);
  }, 120_000);

  it("disposes workerd after expected failure and interruption", async () => {
    for (const interrupt of [false, true]) {
      let disposed = false;
      let acquired = false;

      const operation = Effect.gen(function* () {
        yield* openRuntime(script, "", () => {
          disposed = true;
        });
        acquired = true;

        return yield* interrupt ? Effect.never : Effect.fail("expected fixture failure");
      }).pipe(Effect.scoped);

      const outcome = interrupt
        ? Effect.gen(function* () {
            const fiber = yield* Effect.forkChild(operation);

            while (!acquired) yield* Effect.sleep("10 millis");
            yield* Fiber.interrupt(fiber);

            return yield* Fiber.await(fiber);
          })
        : Effect.exit(operation);

      expect(Exit.isFailure(await Effect.runPromise(outcome))).toBe(true);
      expect(disposed).toBe(true);
    }
  }, 120_000);

  it("audits actual OpenAI requests and cached usage, and rejects an unaffordable call before dispatch", async () => {
    let calls = 0;

    const request: Request = {
      mode: "eager",
      workload: "common",
      seed: 17,
      live: true,
      model: "fixture-model",
      prices: { input: 1, cachedInput: 0.1, output: 1 },
      remainingMicrousd: 10_000_000,
    };

    await Effect.runPromise(
      Effect.gen(function* () {
        const runtime = yield* openRuntime(
          script,
          "test-only",
          () => {},
          async (incoming) => {
            expect(new URL(incoming.url).pathname).toBe("/v1/responses");
            calls++;
            const text = JSON.stringify(expected(request));
            const argumentsText = JSON.stringify({ key: "acct_17" });

            const item =
              calls === 1
                ? {
                    type: "function_call",
                    id: "fc_1",
                    call_id: "call_1",
                    name: Names.balance,
                    arguments: argumentsText,
                    status: "completed",
                  }
                : {
                    type: "message",
                    id: "msg_1",
                    role: "assistant",
                    status: "completed",
                    content: [{ type: "output_text", text, annotations: [] }],
                  };

            const events = [
              { type: "response.output_item.added", output_index: 0, item },
              ...(calls === 1
                ? [
                    {
                      type: "response.function_call_arguments.delta",
                      output_index: 0,
                      item_id: item.id,
                      delta: argumentsText,
                    },
                    {
                      type: "response.function_call_arguments.done",
                      output_index: 0,
                      item_id: item.id,
                      arguments: argumentsText,
                    },
                  ]
                : [
                    {
                      type: "response.output_text.delta",
                      output_index: 0,
                      item_id: item.id,
                      content_index: 0,
                      delta: text,
                    },
                  ]),
              { type: "response.output_item.done", output_index: 0, item },
              {
                type: "response.completed",
                response: {
                  id: `resp_${calls}`,
                  object: "response",
                  model: "fixture-model",
                  created_at: 1,
                  status: "completed",
                  service_tier: "default",
                  output: [item],
                  usage: {
                    input_tokens: 100,
                    output_tokens: 10,
                    total_tokens: 110,
                    input_tokens_details: { cached_tokens: 25 },
                    output_tokens_details: { reasoning_tokens: 2 },
                  },
                },
              },
            ];

            return new Response(
              events
                .map(
                  (event, sequence_number) =>
                    `data: ${JSON.stringify({ ...event, sequence_number })}\n\n`,
                )
                .join(""),
              { headers: { "content-type": "text/event-stream" } },
            );
          },
        );

        const rejected = yield* runSample(runtime, { ...request, remainingMicrousd: 0 });

        expect(rejected.result.passed).toBe(false);
        expect(rejected.result.failure).toContain("Insufficient remaining budget");
        expect(calls).toBe(0);
        const { result } = yield* runSample(runtime, request);

        expect(result.failure).toBeNull();
        expect(calls).toBe(2);
        expect(result.audits).toHaveLength(2);
        expect(result.audits[0]).toMatchObject({
          inputTokens: 100,
          cachedInputTokens: 25,
          outputTokens: 10,
          reasoningTokens: 2,
          returnedModel: "fixture-model",
          returnedTier: "default",
          completed: true,
        });
        expect(result.audits[0]?.toolNames).toHaveLength(120);
        expect(result.audits[0]?.toolBytes).toBeGreaterThan(10_000);
        expect(result.costMicrousd).toBe(176);
        expect(result.pendingMicrousd).toBe(0);
      }).pipe(Effect.scoped),
    );
  }, 120_000);
});

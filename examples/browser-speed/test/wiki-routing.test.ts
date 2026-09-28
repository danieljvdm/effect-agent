import { assert, expectTypeOf, it } from "@effect/vitest";
import { Cause, Deferred, Effect, Exit, Fiber, Layer } from "effect";
import { TestClock } from "effect/testing";
import { AiError, DecisionModel } from "effect/unstable/ai";

import { type LabError } from "../src/contract.ts";
import { makeTrace, Trace } from "../src/telemetry.ts";
import { chooseRoute, type RoutePage } from "../src/wiki-routing.ts";

expectTypeOf<Effect.Error<ReturnType<typeof chooseRoute>>>().toEqualTypeOf<LabError>();
expectTypeOf<Effect.Services<ReturnType<typeof chooseRoute>>>().toEqualTypeOf<
  DecisionModel.DecisionModel | Trace
>();

const trace = () =>
  makeTrace(
    {
      id: crypto.randomUUID(),
      scenario: "wikipedia",
      mode: "agent",
      temperature: "cold",
      prompt: "",
      screenshots: false,
      liveView: false,
      wikiDriver: "jev",
    },
    "jev-latest",
  );

const page: RoutePage = {
  context: {
    current: "Mars",
    destination: "Nelson Mandela",
    excerpt: "The planet Mars",
    path: ["Mars"],
    remainingHops: 20,
  },
  links: Array.from({ length: 256 }, (_, index) => ({
    ref: `p1-l${index}`,
    title: `Article ${index}`,
    label: `Link ${index}`,
    href: `https://en.wikipedia.org/wiki/Article_${index}`,
  })),
};

it.effect(
  "considers every href, advances group winners without comparing probabilities, and permits uncertain route choices",
  () =>
    Effect.gen(function* () {
      const telemetry = yield* trace();
      let calls = 0;
      const seen = new Set<string>();

      const model = Layer.effect(
        DecisionModel.DecisionModel,
        DecisionModel.make({
          decide: (request) =>
            Effect.sync(() => {
              calls++;
              assert.deepStrictEqual(request.state, page.context);

              const answers = Object.fromEntries(
                Object.entries(request.decisions).map(([name, question]) => {
                  assert.strictEqual(question._tag, "Classify");
                  if (question._tag !== "Classify") throw new Error("Expected classify");
                  const keys = Object.keys(question.criteria);

                  assert.isAtMost(keys.length, 255);
                  assert.notInclude(
                    keys,
                    "__none__",
                    "A route choice must permit exploring an indirect connection",
                  );
                  for (const key of keys) seen.add(key);
                  const selected = keys.includes("p1-l255") ? "p1-l255" : "p1-l0";

                  if (calls === 2) assert.deepStrictEqual(keys, ["p1-l0", "p1-l255"]);

                  const probability =
                    keys.length === 1 ? 1 : calls === 2 ? 0.55 : name === "route_0" ? 0.99 : 0.7;

                  return [
                    name,
                    {
                      _tag: "Classify" as const,
                      label: selected,
                      probabilities: Object.fromEntries(
                        keys.map((key) => [
                          key,
                          key === selected ? probability : (1 - probability) / (keys.length - 1),
                        ]),
                      ),
                    },
                  ];
                }),
              );

              return { answers, usage: { inputTokens: 120, outputTokens: 3 } };
            }),
        }),
      );

      const chosen = yield* chooseRoute(page).pipe(
        Effect.provide(model),
        Effect.provideService(Trace, telemetry),
      );

      assert.strictEqual(chosen.href, "https://en.wikipedia.org/wiki/Article_255");
      assert.strictEqual(seen.size, 256);
      assert.strictEqual(calls, 2);
      assert.deepStrictEqual(
        telemetry
          .snapshot()
          .spans.map((span) => [span.phase, span.candidateCount, span.questionCount]),
        [
          ["decision", 256, 2],
          ["decision", 2, 1],
        ],
      );

      const only = yield* chooseRoute({ ...page, links: page.links.slice(0, 1) }).pipe(
        Effect.provideService(Trace, telemetry),
        Effect.provide(model),
      );

      assert.strictEqual(only.ref, "p1-l0");
      assert.strictEqual(
        calls,
        2,
        "A sole eligible link needs no provider call or synthetic alternative",
      );
    }),
);

it.effect(
  "rejects abstention, invented choices and invalid distributions without returning a link",
  () =>
    Effect.gen(function* () {
      for (const [label, probabilities] of [
        ["__none__", { "p1-l1": 1, "p1-l0": 0 }],
        ["invented", { "p1-l1": 0, "p1-l0": 1 }],
        ["p1-l0", { "p1-l1": 0.2, "p1-l0": 0.7 }],
      ] as const) {
        const telemetry = yield* trace();

        const model = Layer.effect(
          DecisionModel.DecisionModel,
          DecisionModel.make({
            decide: () =>
              Effect.succeed({
                answers: { route_0: { _tag: "Classify", label, probabilities } },
                usage: { inputTokens: undefined, outputTokens: undefined },
              }),
          }),
        );

        const result = yield* chooseRoute({ ...page, links: page.links.slice(0, 2) }).pipe(
          Effect.provide(model),
          Effect.provideService(Trace, telemetry),
          Effect.result,
        );

        assert.strictEqual(result._tag, "Failure", label);
      }
    }),
);

it.effect(
  "preserves provider failure, defect and interruption, and finalizes timed-out decisions",
  () =>
    Effect.gen(function* () {
      for (const outcome of ["failure", "defect", "timeout", "interruption"] as const) {
        const telemetry = yield* trace();
        const started = yield* Deferred.make<void>();
        let finalized = false;

        const pending = Deferred.succeed(started, undefined).pipe(
          Effect.andThen(Effect.never),
          Effect.ensuring(
            Effect.sync(() => {
              finalized = true;
            }),
          ),
        );

        const model = Layer.effect(
          DecisionModel.DecisionModel,
          DecisionModel.make({
            decide: () =>
              outcome === "failure"
                ? Effect.fail(
                    new AiError.AiError({
                      module: "test",
                      method: "decide",
                      reason: new AiError.InvalidOutputError({ description: "Invalid route" }),
                    }),
                  )
                : outcome === "defect"
                  ? Effect.die("provider defect")
                  : pending,
          }),
        );

        const fiber = yield* Effect.forkChild(
          chooseRoute(page).pipe(Effect.provide(model), Effect.provideService(Trace, telemetry)),
        );

        if (outcome === "timeout" || outcome === "interruption") {
          yield* Deferred.await(started);
          if (outcome === "timeout") yield* TestClock.adjust("15 seconds");
          else yield* Fiber.interrupt(fiber);
        }
        const exit = yield* Fiber.await(fiber);

        assert.isTrue(Exit.isFailure(exit));
        if (Exit.isFailure(exit)) {
          assert.strictEqual(Cause.hasInterrupts(exit.cause), outcome === "interruption");
          assert.strictEqual(Cause.hasDies(exit.cause), outcome === "defect");
        }
        if (outcome === "timeout" || outcome === "interruption") assert.isTrue(finalized);
      }
    }),
);

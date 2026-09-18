import { Effect, Result } from "effect";
import { expect, it } from "vite-plus/test";

import { ModelUsage } from "../src/contracts.ts";
import { RequestAudit } from "../src/request-audit.ts";
import { cases } from "../src/selective-cases.ts";
import { validateCases, validateComparisonResume } from "../src/selective-eval.ts";

it("refuses a stale or unresolved resume budget after provider dispatch", async () => {
  const usage = ModelUsage.make({
    calls: 1,
    completedCalls: 1,
    inputTokens: 100,
    outputTokens: 20,
    maxInputTokens: 100,
    estimatedCostMicrousd: 49,
    reservedCostMicrousd: 0,
    returnedModels: ["gpt-5.6-luna"],
  });

  const request = RequestAudit.make({
    kind: "request",
    request: 1,
    phase: 0,
    inputTokens: 100,
    outputTokens: 0,
    json: "{}",
  });

  const response = RequestAudit.make({ ...request, kind: "response", outputTokens: 20 });

  await Effect.runPromise(validateComparisonResume(usage, [request, response]));

  const stale = await Effect.runPromise(
    validateComparisonResume(usage, [request, response, { ...request, request: 2 }]).pipe(
      Effect.result,
    ),
  );

  const unresolved = await Effect.runPromise(
    validateComparisonResume({ ...usage, calls: 2, reservedCostMicrousd: 100 }, [
      request,
      response,
      { ...request, request: 2 },
    ]).pipe(Effect.result),
  );

  expect(Result.isFailure(stale)).toBe(true);
  expect(Result.isFailure(unresolved)).toBe(true);
});

it("keeps oracle answers out of model instructions and admits the complete corpus with oracle selection", async () => {
  for (const scenario of cases) {
    const text = JSON.stringify({
      task: scenario.task,
      question: scenario.question,
      history: scenario.history.filter((entry) => entry.kind === "text"),
    });

    for (const fact of scenario.required) {
      expect(text).not.toContain(fact.value);
      expect(
        scenario.history.some(
          (entry) =>
            entry.kind === "tool" &&
            entry.id === fact.toolCallId &&
            entry.result.includes(fact.value),
        ),
      ).toBe(true);
    }
  }
  await Effect.runPromise(validateCases());
});

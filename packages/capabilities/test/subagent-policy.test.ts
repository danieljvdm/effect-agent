import { SubagentPolicy } from "@effect-agent/capabilities/Subagent";
import * as Agent from "@effect-agent/core/Agent";
import { AgentPolicy } from "@effect-agent/core/AgentPolicy";
import { describe, expect, it } from "@effect/vitest";
import { Duration, Schema } from "effect";
import { Toolkit } from "effect/unstable/ai";

import { resolveSubagentPolicy } from "../src/internal/subagent-policy.ts";

const parent = AgentPolicy.make({
  maxTurns: 2,
  maxToolCalls: 1,
  maxDuration: "1 minute",
  toolConcurrency: 1,
  toolResultBounds: { maxBytes: 512 },
  tokenBudget: 100,
  costBudgetMicrousd: 10,
});

const target = Agent.make("policy-child", {
  input: Schema.String,
  output: Schema.String,
  instructions: "Answer the input.",
  toolkit: Toolkit.empty,
  policy: {
    maxTurns: 5,
    maxToolCalls: 4,
    maxDuration: "3 minutes",
    toolConcurrency: 3,
    toolResultBounds: { maxBytes: 4096 },
    tokenBudget: 1000,
    costBudgetMicrousd: 100,
  },
});

const policy = SubagentPolicy.make({
  maxChildren: 1,
  maxConcurrency: 1,
  maxTurns: 6,
  maxToolCalls: 3,
  maxDuration: "2 minutes",
  maxInputTokens: 300,
  maxOutputTokens: 400,
  maxCostMicrousd: 80,
  maxResultBytes: 2048,
});

describe("subagent policy inheritance", () => {
  it("keeps explicit root attached child ceilings independent of the parent's own counters", () => {
    const { childPolicy } = resolveSubagentPolicy(
      { target, policy },
      parent,
      undefined,
      "root-attached",
    );

    expect(childPolicy).toMatchObject({
      maxTurns: 5,
      maxToolCalls: 3,
      toolConcurrency: 3,
      toolResultBounds: { maxBytes: 4096 },
      tokenBudget: 700,
      costBudgetMicrousd: 80,
    });
    expect(Duration.toMillis(childPolicy.maxDuration)).toBe(120_000);
  });

  it("keeps nested and background child execution within the source policy", () => {
    const { childPolicy } = resolveSubagentPolicy({ target, policy }, parent);

    expect(childPolicy).toMatchObject({
      maxTurns: 2,
      maxToolCalls: 1,
      toolConcurrency: 1,
      toolResultBounds: { maxBytes: 512 },
      tokenBudget: 100,
      costBudgetMicrousd: 10,
    });
    expect(Duration.toMillis(childPolicy.maxDuration)).toBe(60_000);
  });

  it("bounds an omitted root attached pool by the parent even with explicit child overrides", () => {
    const { childPolicy } = resolveSubagentPolicy({ target }, parent, undefined, "root-attached");

    expect(childPolicy).toMatchObject({
      maxTurns: 2,
      maxToolCalls: 1,
      tokenBudget: 200,
      costBudgetMicrousd: 10,
    });
    expect(Duration.toMillis(childPolicy.maxDuration)).toBe(60_000);
  });
});

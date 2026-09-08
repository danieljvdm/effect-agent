import * as Agent from "@effect-agent/core/Agent";
import { CanonicalRecordEnvelope, DefinitionDigestInput } from "@effect-agent/thread/Records";
import { Schema } from "effect";
import { Tool, Toolkit } from "effect/unstable/ai";

import { ModelUsage } from "./contracts.ts";
import { ModelId } from "./live-model.ts";
import { RequestAudit } from "./request-audit.ts";

export const PERFORMANCE_FIXTURE = "cloudflare-order-v1";
export const PERFORMANCE_BUDGET = 2_000_000;
export const PerformancePhase = Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 2 }));

export const PerformanceOutput = Schema.Struct({
  sku: Schema.Literal("lamp"),
  units: Schema.Int,
  unitPriceCents: Schema.Int,
  totalCents: Schema.Int,
  available: Schema.Boolean,
  priceEvidence: Schema.String,
  stockEvidence: Schema.String,
  previousTotalCents: Schema.NullOr(Schema.Int),
});

export const performanceToolkit = Toolkit.make(
  Tool.make("read_price", {
    description: "Read the current lamp unit price and its evidence code.",
    parameters: Schema.Struct({ sku: Schema.Literal("lamp") }),
    success: Schema.Struct({ unitPriceCents: Schema.Int, evidence: Schema.String }),
  }),
  Tool.make("read_stock", {
    description: "Read the current lamp stock and its evidence code. Independent of price.",
    parameters: Schema.Struct({ sku: Schema.Literal("lamp") }),
    success: Schema.Struct({ availableUnits: Schema.Int, evidence: Schema.String }),
  }),
);

export const performanceDefinition = Agent.make("performance-cloudflare-order", {
  input: Schema.String,
  output: PerformanceOutput,
  instructions:
    "For every order update call both read_price and read_stock, preferably together. Never reuse stale tool evidence. Use their results to calculate the total in cents and whether stock covers the order. Copy both evidence codes into your final structured output. Carry previousTotalCents from the most recent completed order on this thread; null only for its first order. Do not invent tool values.",
  toolkit: performanceToolkit,
  policy: {
    maxTurns: 4,
    maxToolCalls: 4,
    maxDuration: "2 minutes",
    toolConcurrency: 2,
    contextTokenLimit: 8_192,
    onExhaustion: "fail",
  },
});

export const performanceSettings = {
  max_output_tokens: 4_096,
  reasoning: { effort: "low" },
  store: false,
  service_tier: "default",
  strictJsonSchema: true,
} as const;

export const PerformanceIdentity = Schema.Struct({
  sourceCommit: Schema.String.check(Schema.isPattern(/^[a-f0-9]{40}$/)),
  dirtyWorkingTree: Schema.Boolean,
  fixture: Schema.Literal(PERFORMANCE_FIXTURE),
  fixtureDigest: Schema.String,
  deploymentId: Schema.String,
  model: ModelId,
  maxCostMicrousd: Schema.Literal(PERFORMANCE_BUDGET),
  maxModelCalls: Schema.Literal(18),
  maxInputTokens: Schema.Literal(8_192),
  settings: Schema.Struct({
    max_output_tokens: Schema.Literal(4_096),
    reasoning: Schema.Struct({ effort: Schema.Literal("low") }),
    store: Schema.Literal(false),
    service_tier: Schema.Literal("default"),
    strictJsonSchema: Schema.Literal(true),
  }),
});

export const PerformanceEvent = Schema.Struct({
  index: Schema.Natural,
  phase: PerformancePhase,
  incarnation: Schema.Int,
  kind: Schema.String,
  atMillis: Schema.Finite,
  request: Schema.NullOr(Schema.Int),
});

export const PerformanceSnapshot = Schema.Struct({
  identity: PerformanceIdentity,
  phase: PerformancePhase,
  incarnation: Schema.Int,
  events: Schema.Array(PerformanceEvent),
  records: Schema.Array(CanonicalRecordEnvelope),
  audits: Schema.Array(RequestAudit),
  usage: ModelUsage,
  failure: Schema.NullOr(Schema.String),
  databaseBytes: Schema.Natural,
});

export const PerformanceState = Schema.Struct({
  identity: PerformanceIdentity,
  phase: PerformancePhase,
  incarnation: Schema.Natural,
  usage: ModelUsage,
  aborted: Schema.Boolean,
  closed: Schema.Boolean,
});

export const emptyPerformanceUsage: typeof ModelUsage.Type = {
  calls: 0,
  completedCalls: 0,
  inputTokens: 0,
  outputTokens: 0,
  maxInputTokens: 0,
  estimatedCostMicrousd: 0,
  reservedCostMicrousd: 0,
  returnedModels: [],
};

export const performanceDefinitions = (identity: typeof PerformanceIdentity.Type) =>
  DefinitionDigestInput.make({
    agent: {
      fixture: identity.fixture,
      fixtureDigest: identity.fixtureDigest,
      sourceCommit: identity.sourceCommit,
    },
    model: { name: identity.model, ...identity.settings },
    tools: Object.keys(performanceToolkit.tools).map((name) => ({
      name,
      revision: identity.fixtureDigest,
    })),
  });

export const performanceMessage = (phase: number) =>
  `Order update ${phase}: quote ${phase + 2} lamps. Read both current tools, compute the order and preserve the previous completed order total.`;

export const expectedPerformanceOutput = (phase: number): typeof PerformanceOutput.Type => ({
  sku: "lamp",
  units: phase + 2,
  unitPriceCents: 3_700,
  totalCents: (phase + 2) * 3_700,
  available: true,
  priceEvidence: `price-${phase}-q7`,
  stockEvidence: `stock-${phase}-m9`,
  previousTotalCents: phase === 0 ? null : (phase + 1) * 3_700,
});

import * as Agent from "@effect-agent/core/Agent";
import { AgentPolicy } from "@effect-agent/core/AgentPolicy";
import { CanonicalRecordEnvelope, DefinitionDigestInput } from "@effect-agent/thread/Records";
import { Schema } from "effect";

import { CompactionEvidence, ModelUsage, ProjectStatus, RestartEvidence } from "./contracts.ts";
import { ModelId, MAX_OUTPUT_TOKENS } from "./live-model.ts";
import { pressureInstructions, pressureToolkit } from "./pressure.ts";
import { RequestAudit } from "./request-audit.ts";

export const CloudflareIdentity = Schema.Struct({
  sourceCommit: Schema.String,
  dirtyWorkingTree: Schema.Boolean,
  profile: Schema.Literal("pressure-cloudflare-v1"),
  model: ModelId,
  seed: Schema.Literal(17),
  contextTokenLimit: Schema.Literal(16_000),
  maxCostMicrousd: Schema.Literal(10_000_000),
});

export const CloudflareSnapshot = Schema.Struct({
  identity: CloudflareIdentity,
  records: Schema.Array(CanonicalRecordEnvelope),
  notes: Schema.Struct({ revision: Schema.NullOr(Schema.String), text: Schema.String }),
  usage: ModelUsage,
  failure: Schema.NullOr(Schema.String),
  audits: Schema.Array(RequestAudit),
  compactions: Schema.Array(CompactionEvidence),
  restarts: Schema.Array(RestartEvidence),
});

export const cloudflareDefinition = Agent.make("context-continuity-cloudflare", {
  input: Schema.String,
  output: ProjectStatus,
  instructions: pressureInstructions,
  toolkit: pressureToolkit,
  policy: AgentPolicy.make({
    maxTurns: 16,
    maxToolCalls: 24,
    maxDuration: "8 minutes",
    toolConcurrency: 1,
    contextTokenLimit: 16_000,
    onExhaustion: "fail",
  }),
});

export const cloudflareModelSettings = {
  max_output_tokens: MAX_OUTPUT_TOKENS,
  reasoning: { effort: "low" },
  store: false,
  service_tier: "default",
  strictJsonSchema: true,
} as const;

export const cloudflareDefinitions = (identity: typeof CloudflareIdentity.Type) =>
  DefinitionDigestInput.make({
    agent: {
      sourceCommit: identity.sourceCommit,
      profile: identity.profile,
      scenario: "harbor-handoff-v2",
      contextTokenLimit: 16_000,
    },
    model: { name: identity.model, ...cloudflareModelSettings },
    tools: Object.keys(pressureToolkit.tools).map((name) => ({
      name,
      revision: identity.sourceCommit,
    })),
  });

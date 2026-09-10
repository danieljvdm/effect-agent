import { AgentPolicy } from "@effect-agent/core/AgentPolicy";

/** Operational allowances for work admitted by the v11 coordinator. */
export const researchScoutLimit = 6;
export const activeWorkerLimit = researchScoutLimit + 1;

export const plannerLimits = {
  maxTurns: 48,
  maxToolCalls: 96,
  maxDuration: "15 minutes",
  tokenBudget: 8_000_000,
  contextTokenLimit: 128_000,
  runStatus: "appended",
} as const;

export const scoutPolicy = AgentPolicy.make({
  maxTurns: 32,
  maxToolCalls: 64,
  maxDuration: "10 minutes",
  toolConcurrency: 4,
  contextTokenLimit: 128_000,
  runStatus: "appended",
});

export const editorPolicy = AgentPolicy.make({
  maxTurns: 48,
  maxToolCalls: 96,
  maxDuration: "15 minutes",
  toolConcurrency: 1,
  contextTokenLimit: 128_000,
  runStatus: "appended",
});

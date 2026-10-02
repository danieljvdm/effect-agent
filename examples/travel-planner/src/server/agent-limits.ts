import { AgentPolicy } from "@yielded/agent/agent-policy";

/** Operational allowances for the current planner and workers. */
export const researchScoutLimit = 6;
export const activeWorkerLimit = researchScoutLimit + 1;

export const plannerLimits = {
  restartOnJoinedInput: true,
  maxTurns: 48,
  maxToolCalls: 96,
  maxDuration: "15 minutes",
  toolConcurrency: 1,
  toolResultBounds: { maxBytes: 24 * 1024 },
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

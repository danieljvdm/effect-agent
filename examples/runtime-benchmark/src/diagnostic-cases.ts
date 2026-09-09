import type { DiagnosticCase } from "./diagnostic-contracts.js";

/** Fixed request data only: safe for both the Bun controller and Node workers. */
export const policyCases: ReadonlyArray<DiagnosticCase> = [1, 4].flatMap((rounds) =>
  [0, 2, 20].flatMap((authorizationMs) =>
    [0, 20].map((modelAcquisitionMs) => ({
      name: `policy-rounds-${rounds}-authorize-${authorizationMs}-model-${modelAcquisitionMs}`,
      family: "policy" as const,
      parameters: { rounds, authorizationMs, modelAcquisitionMs },
    })),
  ),
);

export const capabilityCases: ReadonlyArray<DiagnosticCase> = [
  ...[
    { name: "history-unchanged", suffix: 0, threads: 1 },
    { name: "history-single", suffix: 1, threads: 1 },
    { name: "history-suffix-64", suffix: 64, threads: 1 },
    { name: "history-64-threads", suffix: 64, threads: 64 },
  ].map(({ name, suffix, threads }) => ({
    name,
    family: "history" as const,
    parameters: { prefix: 256, suffix, threads },
  })),
  {
    name: "history-near-bound",
    family: "history",
    parameters: { prefix: 768, suffix: 256, threads: 1 },
  },
  ...[0, 1].map((enabled) => ({
    name: `history-run-${enabled ? "on" : "off"}`,
    family: "history" as const,
    parameters: { enabled, prefix: 256 },
  })),
  ...[0, 1, 2].map((mode) => ({
    name: `memory-run-${["off", "empty", "recall"][mode]}`,
    family: "memory" as const,
    parameters: { mode },
  })),
  ...[0, 1].map((enabled) => ({
    name: `mcp-in-process-http-${enabled ? "on" : "off"}`,
    family: "mcp" as const,
    parameters: { enabled },
  })),
  ...[0, 1].map((enabled) => ({
    name: `remembering-run-${enabled ? "on" : "off"}`,
    family: "memory" as const,
    parameters: { enabled },
  })),
  ...[0, 1].map((enabled) => ({
    name: `subagent-run-${enabled ? "on" : "off"}`,
    family: "subagent" as const,
    parameters: { enabled },
  })),
];

export const ledgerCases: ReadonlyArray<DiagnosticCase> = [
  {
    name: "ledger-scan-sparse-8192-16",
    family: "ledger",
    parameters: { mode: 0, settled: 8192, unfinished: 16 },
  },
  {
    name: "ledger-scan-dense-768",
    family: "ledger",
    parameters: { mode: 0, settled: 0, unfinished: 768 },
  },
  { name: "ledger-finalize-active", family: "ledger", parameters: { mode: 1, repeats: 1 } },
  ...[2, 3].flatMap((mode) => [
    {
      name: `ledger-${mode === 2 ? "replay" : "status"}-repeat-${mode === 2 ? 32 : 16}`,
      family: "ledger" as const,
      parameters: { mode, repeats: mode === 2 ? 32 : 16, holdMs: -1 },
    },
    ...[0, 25, 100].map((holdMs) => ({
      name: `ledger-${mode === 2 ? "replay" : "status"}-writer-${holdMs}`,
      family: "ledger" as const,
      parameters: { mode, repeats: 1, holdMs },
    })),
  ]),
];

export const fairnessCases: ReadonlyArray<DiagnosticCase> = [1, 2, 4].flatMap((workerConcurrency) =>
  [0, 1].map((warmArrival) => ({
    name: `fairness-workers-${workerConcurrency}-${warmArrival ? "warm" : "initial"}`,
    family: "fairness" as const,
    parameters: {
      workerConcurrency,
      warmArrival,
      busyThreads: 4,
      toolCalls: 4,
      toolConcurrency: 2,
      toolDelayMs: 100,
    },
  })),
);

export const diagnosticCases: ReadonlyArray<DiagnosticCase> = [
  ...policyCases,
  ...capabilityCases,
  ...ledgerCases,
  ...fairnessCases,
];

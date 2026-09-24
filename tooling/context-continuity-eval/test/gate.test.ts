import { contextWindowId } from "effect-agent/compaction";
import { expect, it } from "vite-plus/test";

import { type EvaluationReport } from "../src/contracts.ts";
import { gateChecks } from "../src/evaluate.ts";
import { gradeStatus, makeScenario, RESTARTS } from "../src/scenario.ts";

const scenario = makeScenario(17);

const completeReport = (): EvaluationReport => ({
  version: 3,
  compactions: [],
  status: "running",
  sourceCommit: "a".repeat(40),
  dirtyWorkingTree: false,
  scenarioDigest: "b".repeat(64),
  seed: 17,
  provider: "openai",
  model: "gpt-6-astra",
  reasoningEffort: "low",
  serviceTier: "default",
  pricingVersion: "openai-2026-09-08-conservative",
  contextTokenLimit: 16_000,
  maxOutputTokens: 4_096,
  maxCostMicrousd: 10_000_000,
  maxModelCalls: 200,
  profile: "explicit-rollover-sqlite-v1",
  startedAt: "2026-09-08T00:00:00.000Z",
  elapsedMillis: 1_000,
  phases: scenario.map((phase) => {
    const output = {
      ...phase.expected,
      receipts: phase.receipt === null ? [] : [{ ...phase.receipt, recordId: "original" }],
    };

    return {
      index: phase.index,
      runId: `run-${phase.index}`,
      modelCalls: 5,
      output,
      checks: gradeStatus(phase, output),
    };
  }),
  windows: Array.from({ length: 12 }, (_, i) => ({
    id: `window-${i}`,
    recordId: `boundary-${i}`,
    sequence: 20 * (i + 1),
    coversThrough: 20 * (i + 1) - 1,
  })),
  restarts: RESTARTS.map((restart) => ({
    ...restart,
    runId: `run-${restart.phase}`,
    notesRevisionBefore: "rev-1",
    notesRevisionAfter: "rev-1",
    notesTextUnchanged: true,
    mechanism: "service-reacquisition",
    processBefore: null,
    processAfter: null,
    killConfirmed: false,
  })),
  checks: [],
  usage: {
    calls: 65,
    completedCalls: 65,
    inputTokens: 100_000,
    outputTokens: 5_000,
    maxInputTokens: 10_000,
    estimatedCostMicrousd: 2_000_000,
    reservedCostMicrousd: 0,
    returnedModels: ["gpt-6-astra"],
  },
  failure: null,
});

const pressureReport = (): EvaluationReport => {
  const base = completeReport();

  return {
    ...base,
    profile: "pressure-restart-sqlite-v1",
    windows: base.windows.map((w, i) => ({ ...w, id: contextWindowId(`run-${i}`, 1) })),
    compactions: base.windows.map((_, i) => ({
      runId: `run-${i}`,
      turn: 1,
      trigger: "pressure",
      kind: "rollover",
      estimatedTokens: 20_000,
      targetTokens: 16_000,
    })),
    restarts: base.restarts.map((r) => ({
      ...r,
      mechanism: "SIGKILL",
      killConfirmed: true,
      processBefore: 10,
      processAfter: 20,
    })),
  };
};

it("accepts measured pressure and confirmed separate processes", () => {
  expect(gateChecks(pressureReport()).filter((c) => !c.passed)).toEqual([]);
});

it.each(["same-process", "requested"] as const)(
  "rejects false pressure/restart coverage: %s",
  (failure) => {
    const base = pressureReport();

    const report: EvaluationReport = {
      ...base,
      compactions: base.compactions.map((c) => ({
        ...c,
        trigger: failure === "requested" ? "requested" : c.trigger,
        estimatedTokens: c.estimatedTokens,
      })),
      restarts: base.restarts.map((r) => ({
        ...r,
        killConfirmed: true,
        processAfter: failure === "same-process" ? r.processBefore : r.processAfter,
      })),
    };

    const failures = gateChecks(report)
      .filter((c) => !c.passed)
      .map((c) => c.name);

    expect(failures).toEqual([
      failure === "same-process" || false
        ? "actual-process-kills"
        : "pressure-caused-committed-windows",
    ]);
  },
);

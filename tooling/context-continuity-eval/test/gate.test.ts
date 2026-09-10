import { contextWindowId } from "@effect-agent/engine/Compaction";
import { describe, expect, it } from "vite-plus/test";

import { type EvaluationReport, type ProjectStatus } from "../src/contracts.ts";
import { gateChecks } from "../src/evaluate.ts";
import { gradeStatus, makeScenario, RESTARTS } from "../src/scenario.ts";

const scenario = makeScenario(17);
const finalPhase = scenario[12]!;

const finalStatus: ProjectStatus = {
  project: "Harbor-17",
  objective: "prepare-export-beta",
  region: "eu-central",
  owner: "Ivo",
  launchDate: "2026-10-21",
  budgetUsd: 10_500,
  customerData: "synthetic-only",
  externalPublicationAllowed: false,
  completed: [
    "backup-verified",
    "rollback-fixed",
    "rollback-verified",
    "security-approved",
    "handoff-drafted",
  ],
  nextAction: "request-final-approval",
  receipts: [{ label: "dock-17-17", code: finalPhase.receipt!.code, recordId: "original-receipt" }],
};

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

describe("context continuity release gate", () => {
  it("recognizes the final decisions and unfinished approval from the scripted conversation", () => {
    expect(gradeStatus(finalPhase, finalStatus).every((check) => check.passed)).toBe(true);
    expect(makeScenario(41)[12]?.receipt?.code).not.toBe(finalPhase.receipt?.code);
  });

  it.each([
    { project: "Harbor-41" },
    { objective: "publish-export-beta" as const },
    { owner: "Maya" },
    { region: "us-east" },
    { launchDate: "2026-10-14" },
    { budgetUsd: 9_000 },
    { customerData: "production-allowed" as const },
    { externalPublicationAllowed: true },
    { nextAction: "verify-backup" as const },
    { completed: ["backup-verified"] },
    { receipts: [{ label: "dock-17-17", code: "invented", recordId: "original-receipt" }] },
  ])("rejects stale decisions, forgotten constraints, or invented recall: %j", (change) => {
    expect(
      gradeStatus(finalPhase, { ...finalStatus, ...change }).some((check) => !check.passed),
    ).toBe(true);
  });

  it("requires a complete, metered run with forward window coverage and both restarts", () => {
    expect(gateChecks(completeReport()).every((check) => check.passed)).toBe(true);
  });

  const invalidReports: ReadonlyArray<
    readonly [string, (report: EvaluationReport) => EvaluationReport]
  > = [
    [
      "missing conversation update",
      (r) => ({ ...r, phases: r.phases.filter((p) => p.index !== 7) }),
    ],
    ["only eleven rollovers", (r) => ({ ...r, windows: r.windows.slice(0, 11) })],
    ["duplicated window", (r) => ({ ...r, windows: [...r.windows.slice(0, 11), r.windows[0]!] })],
    [
      "non-forward coverage",
      (r) => ({ ...r, windows: r.windows.map((w) => ({ ...w, coversThrough: 1 })) }),
    ],
    ["missing recovery", (r) => ({ ...r, restarts: r.restarts.slice(0, 1) })],
    [
      "lost notes",
      (r) => ({ ...r, restarts: r.restarts.map((s) => ({ ...s, notesTextUnchanged: false })) }),
    ],
    [
      "changed notes revision",
      (r) => ({ ...r, restarts: r.restarts.map((s) => ({ ...s, notesRevisionAfter: "rev-2" })) }),
    ],
    [
      "bad semantic result",
      (r) => ({
        ...r,
        phases: r.phases.map((p) => ({
          ...p,
          checks: [{ name: "recall", passed: false, expected: "a", actual: "b" }],
        })),
      }),
    ],
    ["unmetered call", (r) => ({ ...r, usage: { ...r.usage, completedCalls: 64 } })],
    ["no model calls", (r) => ({ ...r, usage: { ...r.usage, calls: 0, completedCalls: 0 } })],
    [
      "uncertain provider spending",
      (r) => ({ ...r, usage: { ...r.usage, reservedCostMicrousd: 100 } }),
    ],
    ["oversized live prompt", (r) => ({ ...r, usage: { ...r.usage, maxInputTokens: 100_000 } })],
    ["over budget", (r) => ({ ...r, usage: { ...r.usage, estimatedCostMicrousd: 10_000_001 } })],
    ["interrupted run", (r) => ({ ...r, failure: "interrupted" })],
  ];

  it.each(invalidReports)("refuses publication for %s", (_, invalidate) => {
    expect(gateChecks(invalidate(completeReport())).some((check) => !check.passed)).toBe(true);
  });
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

it.each(["same-process", "no-kill", "requested", "overflow", "under-limit"] as const)(
  "rejects false pressure/restart coverage: %s",
  (failure) => {
    const base = pressureReport();

    const report: EvaluationReport = {
      ...base,
      compactions: base.compactions.map((c) => ({
        ...c,
        trigger:
          failure === "requested" ? "requested" : failure === "overflow" ? "overflow" : c.trigger,
        estimatedTokens: failure === "under-limit" ? 100 : c.estimatedTokens,
      })),
      restarts: base.restarts.map((r) => ({
        ...r,
        killConfirmed: failure !== "no-kill",
        processAfter: failure === "same-process" ? r.processBefore : r.processAfter,
      })),
    };

    const failures = gateChecks(report)
      .filter((c) => !c.passed)
      .map((c) => c.name);

    expect(failures).toEqual([
      failure === "same-process" || failure === "no-kill"
        ? "actual-process-kills"
        : "pressure-caused-committed-windows",
    ]);
  },
);

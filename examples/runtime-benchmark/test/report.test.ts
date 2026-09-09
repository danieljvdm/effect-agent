import { Schema } from "effect";
import { expect, it } from "vite-plus/test";

import { PerformanceReport, renderPerformanceReport } from "../../../scripts/runtime-benchmark.ts";
import { FIXTURE_VERSION } from "../src/contracts.ts";

it("reports and excludes an invalid batch even when its process exited successfully", () => {
  const report: typeof PerformanceReport.Type = {
    fixture: FIXTURE_VERSION,
    fixtureSha256: "fixture",
    transpiler: "test",
    profile: "smoke",
    environment: {
      platform: "test",
      release: "test",
      architecture: "test",
      cpu: "test",
      cpuCount: 1,
      memoryBytes: 1,
      node: "v24",
    },
    settings: {
      batches: 1,
      warmupsPerBatch: 0,
      samplesPerBatch: 1,
      production: true,
      execution: "unbundled published ESM",
      timingGate: "informational",
    },
    revisions: [],
    activeBatch: null,
    failure: null,
    batches: [
      {
        role: "head",
        cohort: 0,
        cold: false,
        subprocessMs: 1,
        exitCode: 0,
        complete: false,
        failure: "Worker runtime differs from the controller",
        report: {
          fixture: FIXTURE_VERSION,
          profile: "smoke",
          runtime: "v22",
          platform: "test",
          architecture: "test",
          active: null,
          failure: null,
          samples: [
            {
              case: "small-run",
              ordinal: 0,
              warmup: false,
              totalMs: 0.01,
              attemptMs: 1,
              setupMs: 0.5,
              failurePhase: null,
              modelEntryMs: 0.005,
              checkpointCreationMs: null,
              retainedPromptMessages: 0,
              modelCalls: 1,
              finalizers: 1,
              toolCalls: 0,
              outputBytes: 15,
              status: "passed",
              failure: null,
            },
          ],
        },
      },
    ],
  };

  const rendered = renderPerformanceReport(report);

  expect(rendered).toContain(
    "| Workload | Base | Head | Head/base |\n| --- | ---: | ---: | ---: |\n",
  );
  expect(rendered).not.toMatch(/reference/i);
  expect(Schema.is(PerformanceReport)({ ...report, fixture: "runtime-v2" })).toBe(false);
  expect(
    Schema.is(PerformanceReport)({
      ...report,
      batches: [{ ...report.batches[0], role: "reference" }],
    }),
  ).toBe(false);
  expect(rendered).toContain("Invalid/incomplete batches: 1; processes recorded: 1/4");
  expect(rendered).toContain("Worker runtime differs from the controller");
  expect(rendered).toContain("| small-run | n/a | n/a | n/a |");

  const interrupted = renderPerformanceReport({
    ...report,
    activeBatch: { role: "base", cohort: 0, cold: false },
    failure: "TimeoutException: comparison deadline",
    batches: report.batches.map((batch) => ({
      ...batch,
      exitCode: -1,
      report:
        batch.report === null
          ? null
          : {
              ...batch.report,
              active: {
                case: "settled-ledger-16",
                ordinal: 0,
                warmup: false,
                phase: "setup",
                elapsedMs: 1200,
              },
            },
    })),
  });

  expect(interrupted).toContain("Interrupted active batch: base/0/warm");
  expect(interrupted).toContain("Comparison failure: TimeoutException: comparison deadline");
  expect(interrupted).toContain("settled-ledger-16:0 setup at 1200.00 ms");
});

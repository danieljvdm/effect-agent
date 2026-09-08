import { expect, it } from "vite-plus/test";

import type { PerformanceReport } from "../../../scripts/runtime-benchmark.ts";
import { renderPerformanceReport } from "../../../scripts/runtime-benchmark.ts";
import { FIXTURE_VERSION, REFERENCE } from "../src/contracts.ts";

it("reports and excludes an invalid batch even when its process exited successfully", () => {
  const report: typeof PerformanceReport.Type = {
    fixture: FIXTURE_VERSION,
    fixtureSha256: "fixture",
    transpiler: "test",
    profile: "smoke",
    referenceVersion: REFERENCE.version,
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
          samples: [
            {
              case: "small-run",
              ordinal: 0,
              warmup: false,
              totalMs: 0.01,
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

  expect(rendered).toContain("Invalid/incomplete batches: 1; processes recorded: 1/6");
  expect(rendered).toContain("Worker runtime differs from the controller");
  expect(rendered).toContain("| small-run | n/a | n/a | n/a |");
});

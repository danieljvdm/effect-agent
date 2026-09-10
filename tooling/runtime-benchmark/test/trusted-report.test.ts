import { readFileSync } from "node:fs";
import { runInNewContext } from "node:vm";

import { expect, it } from "vite-plus/test";

import type { PerformanceReport } from "../../../scripts/runtime-benchmark.ts";
import { casesFor, FIXTURE_VERSION } from "../src/contracts.ts";

type Mutable<T> = { -readonly [K in keyof T]: T[K] extends object ? Mutable<T[K]> : T[K] };
type Report = Mutable<typeof PerformanceReport.Type>;
const head = "a".repeat(40);
const base = "b".repeat(40);

const makeReport = (): Report => ({
  fixture: FIXTURE_VERSION,
  fixtureSha256: "c".repeat(64),
  transpiler: "test",
  profile: "pr",
  activeBatch: null,
  failure: null,
  environment: {
    node: "v24.20.0",
    platform: "linux",
    architecture: "x64",
    release: "test",
    cpu: "test",
    cpuCount: 2,
    memoryBytes: 1,
  },
  settings: {
    batches: 3,
    warmupsPerBatch: 2,
    samplesPerBatch: 3,
    production: true,
    execution: "unbundled published ESM",
    timingGate: "informational",
  },
  revisions: (["base", "head"] as const).map((role) => ({
    role,
    revision: role === "base" ? base : head,
    dirty: false,
    lockfileSha256: "d".repeat(64),
    builtArtifactsSha256: "e".repeat(64),
    effect: "test",
  })),
  batches: [0, 1, 2].flatMap((cohort) =>
    (["base", "head"] as const).flatMap((role) =>
      [true, false].map((cold) => ({
        role,
        cohort,
        cold,
        subprocessMs: 1000,
        exitCode: 0,
        complete: true,
        failure: null,
        report: {
          fixture: FIXTURE_VERSION,
          profile: "pr" as const,
          runtime: "v24.20.0",
          platform: "linux",
          architecture: "x64",
          active: null,
          failure: null,
          samples: (cold ? casesFor("pr").slice(0, 1) : casesFor("pr")).flatMap((workload) =>
            Array.from({ length: cold ? 1 : 5 }, (_, ordinal) => ({
              case: workload.name,
              ordinal,
              warmup: !cold && ordinal < 2,
              totalMs: 2,
              attemptMs: 4,
              setupMs: 1,
              failurePhase: null,
              modelEntryMs: 1,
              checkpointCreationMs: workload.kind === "recovery" ? 1 : null,
              retainedPromptMessages: 0,
              modelCalls: 1,
              finalizers: 1,
              toolCalls: 0,
              outputBytes: 15,
              status: "passed" as const,
              failure: null,
            })),
          ),
        },
      })),
    ),
  ),
});

// Exercise the exact trusted inline publisher, with artifact bytes and GitHub writes isolated.
const workflow = readFileSync(
  new URL("../../../.github/workflows/performance-comment.yml", import.meta.url),
  "utf8",
);

const script = workflow.split("          script: |\n")[1]!.replace(/^ {12}/gm, "");

const publish = async (report: unknown, currentHead = head) => {
  const comments: string[] = [];

  const files: Record<string, string> = {
    "runtime-performance/report.json": JSON.stringify(report),
    "runtime-performance/pr-number.txt": "1",
  };

  const execution: Promise<unknown> = runInNewContext(`(async () => { ${script} })()`, {
    require: (name: string) => {
      if (name !== "node:fs") throw new Error("Unexpected trusted publisher import");

      return {
        lstatSync: (file: string) => ({
          isFile: () => true,
          size: Buffer.byteLength(files[file]!),
        }),
        readFileSync: (file: string) => files[file],
      };
    },
    context: {
      repo: { owner: "owner", repo: "repository" },
      payload: {
        workflow_run: {
          head_sha: head,
          head_repository: { id: 1 },
          html_url: "https://example.test/run",
        },
      },
    },
    github: {
      rest: {
        pulls: {
          get: async () => ({
            data: {
              number: 1,
              state: "open",
              head: { sha: currentHead, repo: { id: 1 } },
              base: { sha: base, repo: { full_name: "owner/repository" } },
            },
          }),
        },
        issues: {
          listComments: () => {},
          createComment: async (value: { body: string }) => {
            comments.push(value.body);
          },
          updateComment: async () => {
            throw new Error("Unexpected update");
          },
        },
      },
      paginate: async () => [],
    },
  });

  await execution;

  return comments;
};

it("publishes all 12 valid batches and rejects stale PR identity", async () => {
  const comments = await publish(makeReport());

  expect(comments).toHaveLength(1);
  expect(comments[0]).toContain("nine samples per workload and revision");
  expect(comments[0]).toContain(
    "| Workload | Base | Head | Head/base |\n| --- | ---: | ---: | ---: |\n",
  );
  expect(comments[0]).toContain("| settled-ledger-2048 | 2.00 | 2.00 | 0.0% |");
  expect(comments[0]).not.toMatch(/reference/i);
  expect(await publish(makeReport(), "f".repeat(40))).toEqual([]);
});

it("computes Head/base from measured warm samples only", async () => {
  const report = makeReport();

  for (const batch of report.batches) {
    const worker = batch.report!;

    batch.report = {
      ...worker,
      samples: worker.samples.map((sample) => ({
        ...sample,
        totalMs: batch.cold || sample.warmup ? 100 : batch.role === "base" ? 4 : 2,
        attemptMs: 102,
      })),
    };
  }

  const comments = await publish(report);

  expect(comments[0]).toContain("| small-run | 4.00 | 2.00 | -50.0% |");
  expect(comments[0]).toContain("| settled-ledger-2048 | 4.00 | 2.00 | -50.0% |");
});

it("rejects historical contracts and reference roles before commenting", async () => {
  const report = makeReport();

  await expect(publish({ ...report, fixture: "runtime-v2" })).rejects.toThrow(
    "Invalid report contract",
  );
  await expect(
    publish({
      ...report,
      revisions: [...report.revisions, { ...report.revisions[0], role: "reference" }],
    }),
  ).rejects.toThrow("Invalid report contract");
  await expect(
    publish({
      ...report,
      revisions: [report.revisions[0], { ...report.revisions[1], role: "reference" }],
    }),
  ).rejects.toThrow("Invalid revision");
  await expect(
    publish({
      ...report,
      batches: report.batches.map((batch, index) =>
        index === 0 ? { ...batch, role: "reference" } : batch,
      ),
    }),
  ).rejects.toThrow("Invalid cohort");
});

const mutations: ReadonlyArray<readonly [string, (report: Report) => void]> = [
  [
    "missing revision",
    (report) => {
      report.revisions.pop();
    },
  ],
  [
    "duplicated revision",
    (report) => {
      report.revisions[1] = report.revisions[0]!;
    },
  ],
  [
    "reduced samples",
    (report) => {
      report.settings.samplesPerBatch = 2;
    },
  ],
  [
    "missing batch",
    (report) => {
      report.batches.pop();
    },
  ],
  [
    "duplicated identity",
    (report) => {
      report.batches[1] = report.batches[0]!;
    },
  ],
  [
    "missing cold sample",
    (report) => {
      report.batches[0]!.report = { ...report.batches[0]!.report!, samples: [] };
    },
  ],
  [
    "missing warm sample",
    (report) => {
      report.batches[1]!.report = { ...report.batches[1]!.report!, samples: [] };
    },
  ],
  [
    "failed cold process",
    (report) => {
      report.batches[0]!.exitCode = -1;
    },
  ],
  [
    "incomplete warm process",
    (report) => {
      report.batches[1]!.complete = false;
    },
  ],
  [
    "different runtime",
    (report) => {
      report.batches[0]!.report = { ...report.batches[0]!.report!, runtime: "v22.0.0" };
    },
  ],
  [
    "different fixture",
    (report) => {
      report.fixtureSha256 = "invalid";
    },
  ],
  [
    "missing public artifact hash",
    (report) => {
      report.revisions[1]!.builtArtifactsSha256 = "";
    },
  ],
  [
    "dirty head",
    (report) => {
      report.revisions[1]!.dirty = true;
    },
  ],
  [
    "active sample",
    (report) => {
      report.batches[0]!.report = {
        ...report.batches[0]!.report!,
        active: { case: "small-run", ordinal: 0, warmup: false, phase: "setup", elapsedMs: 1 },
      };
    },
  ],
  [
    "controller interruption",
    (report) => {
      report.failure = "Interrupted";
    },
  ],
  [
    "active batch",
    (report) => {
      report.activeBatch = { role: "head", cohort: 2, cold: false };
    },
  ],
  [
    "unfinalized model",
    (report) => {
      const worker = report.batches[0]!.report!;

      report.batches[0]!.report = {
        ...worker,
        samples: [{ ...worker.samples[0]!, finalizers: 0 }],
      };
    },
  ],
  [
    "impossible setup timing",
    (report) => {
      const worker = report.batches[0]!.report!;

      report.batches[0]!.report = { ...worker, samples: [{ ...worker.samples[0]!, setupMs: 10 }] };
    },
  ],
];

it.each(mutations)("rejects %s before commenting", async (_name, mutate) => {
  const report = makeReport();

  mutate(report);
  await expect(publish(report)).rejects.toThrow(/Invalid|Incomplete|Unexpected/);
});

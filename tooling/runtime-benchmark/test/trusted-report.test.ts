import { readFileSync } from "node:fs";
import { runInNewContext } from "node:vm";

import { expect, it } from "vite-plus/test";

import type { PerformanceReport } from "../../../scripts/runtime-benchmark.ts";
import { renderPerformanceReport } from "../../../scripts/runtime-benchmark.ts";
import { casesFor, FIXTURE_VERSION } from "../src/contracts.ts";

type Mutable<T> = { -readonly [K in keyof T]: T[K] extends object ? Mutable<T[K]> : T[K] };
type Report = Mutable<typeof PerformanceReport.Type>;
const head = "a".repeat(40);
const base = "b".repeat(40);
const baselineTag = "effect-agent@0.1.0-beta.76";

const makeReport = (): Report => ({
  fixture: FIXTURE_VERSION,
  baselineTag,
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
    builtArtifactsSha256: (role === "base" ? "e" : "f").repeat(64),
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

const publish = async (
  report: unknown,
  options: {
    currentMain?: string;
    releaseTag?: string;
    releaseCommit?: string;
    runHead?: string;
    runRepository?: string;
    annotatedTag?: boolean;
    releasePr?: boolean;
  } = {},
) => {
  const comments: string[] = [];

  const files: Record<string, string> = {
    "runtime-performance/report.json": JSON.stringify(report),
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
          event: "push",
          head_branch: "main",
          head_sha: options.runHead ?? head,
          head_repository: { full_name: options.runRepository ?? "owner/repository" },
          html_url: "https://example.test/run",
        },
      },
    },
    github: {
      rest: {
        repos: { listReleases: "releases" },
        git: {
          getRef: async ({ ref }: { ref: string }) => ({
            data: {
              object: {
                type: ref === "heads/main" || !options.annotatedTag ? "commit" : "tag",
                sha:
                  ref === "heads/main"
                    ? (options.currentMain ?? head)
                    : (options.releaseCommit ?? base),
              },
            },
          }),
          getTag: async () => ({
            data: { object: { type: "commit", sha: options.releaseCommit ?? base } },
          }),
        },
        pulls: {
          list: "pulls",
        },
        issues: {
          listComments: "comments",
          createComment: async (value: { body: string }) => {
            comments.push(value.body);
          },
          updateComment: async () => {
            throw new Error("Unexpected update");
          },
        },
      },
      paginate: async (endpoint: string) => {
        if (endpoint === "releases")
          return [
            {
              id: 1,
              draft: false,
              prerelease: true,
              published_at: "2026-09-10T03:17:46Z",
              tag_name: options.releaseTag ?? baselineTag,
            },
          ];
        if (endpoint === "pulls")
          return options.releasePr === false
            ? []
            : [
                {
                  number: 1,
                  state: "open",
                  head: {
                    ref: "changeset-release/main",
                    sha: "9".repeat(40),
                    repo: { full_name: "owner/repository" },
                  },
                  base: {
                    ref: "main",
                    sha: options.currentMain ?? head,
                    repo: { full_name: "owner/repository" },
                  },
                },
              ];
        if (endpoint === "comments") return [];
        throw new Error("Unexpected GitHub endpoint");
      },
    },
  });

  await execution;

  return comments;
};

it("publishes release versus main even though the release PR head is a version-only commit", async () => {
  const comments = await publish(makeReport());

  expect(comments).toHaveLength(1);
  expect(comments[0]).toContain(
    "nine samples per workload and revision across three worker processes",
  );
  expect(comments[0]).toContain(baselineTag);
  expect(comments[0]).toContain(`/commit/${head}`);
  expect(comments[0]).toContain(
    "| Workload | Latest release | Main | Change |\n| --- | ---: | ---: | ---: |\n",
  );
  expect(comments[0]).toContain(
    "| settled-ledger-2048 | 2.00 [2.00–2.00] | 2.00 [2.00–2.00] | 0.0% |",
  );
  expect(comments[0]).not.toMatch(/reference/i);
  expect(await publish(makeReport(), { annotatedTag: true })).toHaveLength(1);
});

it.each([
  { currentMain: "f".repeat(40) },
  { runHead: "f".repeat(40) },
  { runRepository: "someone/fork" },
  { releaseTag: "effect-agent@0.1.0-beta.77" },
  { releaseCommit: "f".repeat(40) },
  { releasePr: false },
])("does not publish stale or unrelated comparison %j", async (options) => {
  expect(await publish(makeReport(), options)).toEqual([]);
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

  expect(comments[0]).toContain("| small-run | 4.00 [4.00–4.00] | 2.00 [2.00–2.00] | -50.0% |");
  expect(comments[0]).toContain(
    "| settled-ledger-2048 | 4.00 [4.00–4.00] | 2.00 [2.00–2.00] | -50.0% |",
  );
});

it("preserves slow samples and spread without claiming identical builds regressed", async () => {
  const report = makeReport();

  report.revisions[1]!.builtArtifactsSha256 = report.revisions[0]!.builtArtifactsSha256;
  for (const batch of report.batches) {
    const worker = batch.report!;

    batch.report = {
      ...worker,
      samples: worker.samples.map((sample) => ({
        ...sample,
        totalMs: batch.role === "base" ? 2 : [2, 2, 3, 4, 100][sample.ordinal]!,
        attemptMs: 102,
      })),
    };
  }
  const comments = await publish(report);

  for (const output of [comments[0]!, renderPerformanceReport(report)]) {
    expect(output).toContain("Identical built JavaScript and lockfiles");
    expect(output).toContain("| small-run | 2.00 [2.00–2.00] | 4.00 [3.00–100.00] | n/a |");
    expect(output).not.toContain("100.0%");
  }
  report.revisions[1]!.lockfileSha256 = "a".repeat(64);
  expect((await publish(report))[0]).toContain("| 100.0% |");
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
    "invalid release tag",
    (report) => {
      report.baselineTag = "main";
    },
  ],
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

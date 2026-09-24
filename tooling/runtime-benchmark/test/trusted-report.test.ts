import { readFileSync } from "node:fs";
import { runInNewContext } from "node:vm";

import { expect, it } from "vite-plus/test";

import type { PerformanceReport } from "../../../scripts/runtime-benchmark.ts";
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

it.each([
  { currentMain: "f".repeat(40) },
  { runRepository: "someone/fork" },
  { releaseCommit: "f".repeat(40) },
])("does not publish stale or unrelated comparison %j", async (options) => {
  expect(await publish(makeReport(), options)).toEqual([]);
});

const mutations: ReadonlyArray<readonly [string, (report: Report) => void]> = [
  [
    "incomplete warm process",
    (report) => {
      report.batches[1]!.complete = false;
    },
  ],
];

it.each(mutations)("rejects %s before commenting", async (_name, mutate) => {
  const report = makeReport();

  mutate(report);
  await expect(publish(report)).rejects.toThrow(/Invalid|Incomplete|Unexpected/);
});

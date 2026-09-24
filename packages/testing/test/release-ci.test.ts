import { NodeServices } from "@effect/platform-node";
import { expect, it, layer } from "@effect/vitest";
import { Effect, Exit, Fiber, FileSystem } from "effect";
import { TestClock } from "effect/testing";
import { HttpClient, HttpClientResponse } from "effect/unstable/http";

import { verifyPackedFiles } from "../../../scripts/check-release-packages.ts";
import {
  type Jobs,
  type Run,
  decideReleaseCi,
  type MetadataChange,
  proveReleaseCi,
  proveMergedReleaseCi,
  readCommand,
  readMetadata,
  verifyBuildEvidence,
  verifyMainBuild,
  verifyMetadata,
} from "../../../scripts/release-ci.ts";

const repository = "danieljvdm/effect-agent";
const packages = ["effect-agent", "@effect-agent/ai-decision"];
const base = "a".repeat(40);
const head = "b".repeat(40);
const checkout = "c".repeat(40);

const pre = {
  mode: "pre",
  tag: "beta",
  initialVersions: { "effect-agent": "0.1.0-beta.44" },
  changesets: ["previous-change"],
};

const nextPre = {
  ...pre,
  initialVersions: { ...pre.initialVersions, "@effect-agent/ai-decision": "0.1.0-beta.99" },
  changesets: ["previous-change", "new-change"],
};

const manifest = (name: string, version: string) =>
  JSON.stringify(
    {
      name,
      version,
      type: "module",
      exports: { ".": "./src/index.ts" },
      scripts: { test: "vp test" },
      dependencies: { effect: "catalog:" },
    },
    null,
    2,
  ) + "\n";

const lock = (version: string) => `{
  "lockfileVersion": 1,
  "workspaces": {
    "packages/effect-agent": {
      "name": "effect-agent",
      "version": "${version}",
    },
    "packages/ai-decision": {
      "name": "@effect-agent/ai-decision",
      "version": "${version}",
    },
  },
  "packages": { "effect": ["effect@4.0.0-rc.115", "", {}, "sha512-original"] },
}
`;

const fixture = (): Array<MetadataChange> => [
  {
    path: "bun.lock",
    before: lock("0.1.0-beta.99"),
    after: lock("0.1.0-beta.100"),
    oldMode: "100644",
    newMode: "100644",
  },
  {
    path: ".changeset/pre.json",
    before: JSON.stringify(pre),
    after: JSON.stringify(nextPre),
    oldMode: "100644",
    newMode: "100644",
  },
  ...packages.flatMap((name) => {
    const directory = `packages/${name.replace("@effect-agent/", "")}`;

    return [
      {
        path: `${directory}/package.json`,
        before: manifest(name, "0.1.0-beta.99"),
        after: manifest(name, "0.1.0-beta.100"),
        oldMode: "100644",
        newMode: "100644",
      },
      {
        path: `${directory}/CHANGELOG.md`,
        before:
          name === "effect-agent"
            ? "# effect-agent\n\n## 0.1.0-beta.99\n\nPrevious release.\n"
            : null,
        after:
          name === "effect-agent"
            ? "# effect-agent\n\n## 0.1.0-beta.100\n\n## 0.1.0-beta.99\n\nPrevious release.\n"
            : "# @effect-agent/ai-decision\n\n## 0.1.0-beta.100\n\n### Minor Changes\n\n- New provider.\n",
        oldMode: name === "effect-agent" ? "100644" : null,
        newMode: "100644",
      },
    ];
  }),
];

const replace = (path: string, transform: (change: MetadataChange) => MetadataChange) =>
  fixture().map((change) => (change.path === path ? transform(change) : change));

const decideMetadata = (changes: ReadonlyArray<MetadataChange>) =>
  decideReleaseCi(verifyMetadata(packages, ["new-change"], changes));

it.effect("rejects executable manifest changes even beside a valid version bump", () =>
  Effect.gen(function* () {
    {
      const [from, to] = ['"vp test"', '"echo skipped"'] as const;

      const changes = replace("packages/effect-agent/package.json", (change) => ({
        ...change,
        after: change.after.replace(from!, to!),
      }));

      expect(yield* decideMetadata(changes)).toEqual({ fast: false });
    }
  }),
);

const run: typeof Run.Type = {
  id: 42,
  run_attempt: 1,
  workflow_id: 12,
  path: ".github/workflows/ci.yml",
  name: "CI",
  event: "push",
  head_branch: "main",
  head_sha: base,
  status: "completed",
  conclusion: "success",
  repository: { full_name: repository },
  head_repository: { full_name: repository },
};

const jobs: typeof Jobs.Type = {
  total_count: 10,
  jobs: [
    ["Static checks", "Format, lint, and type checks"],
    ["Tests (workspace)", "Run workspace test suites"],
    ["Tests (travel-planner)", "Run workspace test suites"],
    ["Tests (context-continuity)", "Run workspace test suites"],
    ["Tests (runtime-benchmark)", "Run workspace test suites"],
    ["Tests (platform-node)", "Run workspace test suites"],
    ["Tests (testing)", "Run workspace test suites"],
    ["Tests (platform-cloudflare)", "Run workspace test suites"],
    ["Tests (storage-cloudflare)", "Run workspace test suites"],
    ["Build", "Build packages, examples, and docs"],
  ].map(([name, command]) => ({
    name: name!,
    run_id: 42,
    head_sha: base,
    status: "completed",
    conclusion: "success",
    steps: [{ name: command!, status: "completed", conclusion: "success" }],
  })),
};

const pull = {
  number: 516,
  merged: false,
  state: "open",
  merge_commit_sha: checkout,
  base: { ref: "main", sha: base, repo: { full_name: repository } },
  head: { ref: "changeset-release/main", sha: head, repo: { full_name: repository } },
};

it.effect("falls back on errors, defects and bounded timeout and finalizes interrupted work", () =>
  Effect.gen(function* () {
    for (const failure of [Effect.fail("API error"), Effect.die("invalid API response")]) {
      expect(yield* decideReleaseCi(failure)).toEqual({ fast: false });
    }
    let finalized = 0;

    const pending = Effect.never.pipe(
      Effect.ensuring(
        Effect.sync(() => {
          finalized += 1;
        }),
      ),
    );

    const timeout = yield* decideReleaseCi(pending).pipe(Effect.forkChild);

    yield* TestClock.adjust("46 seconds");
    expect(yield* Fiber.join(timeout)).toEqual({ fast: false });
    expect(finalized).toBe(1);
    const interrupted = yield* decideReleaseCi(pending).pipe(Effect.forkChild);

    yield* Effect.yieldNow;
    yield* Fiber.interrupt(interrupted);
    expect(finalized).toBe(2);
  }),
);

// Regression: https://github.com/danieljvdm/effect-agent/commit/bd161066d6b805461f173c7c537014ae816d5177
// Publication uses npm 12's package-name map; ordinary CI can use npm 11's array.
it.effect("checks packed identity and every export for both supported npm output formats", () =>
  Effect.gen(function* () {
    const manifest = {
      name: "effect-agent",
      version: "0.1.0-beta.100",
      exports: { ".": { default: "./dist/index.mjs", types: "./dist/index.d.mts" } },
    };

    const pack = {
      name: "effect-agent",
      version: "0.1.0-beta.100",
      files: [{ path: "package.json" }, { path: "dist/index.mjs" }, { path: "dist/index.d.mts" }],
    };

    yield* verifyPackedFiles(manifest, [pack]);
    yield* verifyPackedFiles(manifest, { "effect-agent": pack });
    {
      const altered = {
        ...pack,
        files: pack.files.filter((entry) => entry.path !== "dist/index.mjs"),
      } as const;

      expect(Exit.isFailure(yield* Effect.exit(verifyPackedFiles(manifest, [altered])))).toBe(true);
      expect(
        Exit.isFailure(
          yield* Effect.exit(verifyPackedFiles(manifest, { "effect-agent": altered })),
        ),
      ).toBe(true);
    }
  }),
);

layer(NodeServices.layer)((it) => {
  it.effect(
    "wires Git objects and read-only attempt-specific API evidence without checking out candidate code",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const directory = yield* fs.makeTempDirectoryScoped({ prefix: "release-proof-test-" });

        const git = (...args: ReadonlyArray<string>) =>
          readCommand(directory, "git", args).pipe(Effect.map((output) => output.trim()));

        const write = Effect.fn(function* (path: string, text: string) {
          if (path.includes("/"))
            yield* fs.makeDirectory(`${directory}/${path.slice(0, path.lastIndexOf("/"))}`, {
              recursive: true,
            });
          yield* fs.writeFileString(`${directory}/${path}`, text);
        });

        yield* git("init", "--initial-branch=main");
        yield* git("config", "user.name", "Release proof test");
        yield* git("config", "user.email", "proof@example.invalid");
        yield* git("config", "commit.gpgsign", "false");
        yield* git("config", "core.hooksPath", `${directory}/.git/hooks`);
        for (const change of fixture())
          if (change.before !== null) yield* write(change.path, change.before);
        yield* write(".changeset/config.json", JSON.stringify({ fixed: [packages] }));
        yield* write(".changeset/new-change.md", '---\n"effect-agent": patch\n---\nA change.\n');
        yield* write(
          "scripts/release-ci.ts",
          "throw new Error('Candidate code must not execute');\n",
        );
        yield* git("add", ".");
        yield* git("commit", "-m", "Validated source");
        const base = yield* git("rev-parse", "HEAD");

        yield* git("checkout", "-b", "changeset-release/main");
        for (const change of fixture()) yield* write(change.path, change.after);
        yield* git("add", ".");
        yield* git("commit", "-m", "Version packages");
        const head = yield* git("rev-parse", "HEAD");

        yield* git("checkout", "main");
        yield* git("merge", "--no-ff", "changeset-release/main", "-m", "Synthetic PR merge");
        const checkout = yield* git("rev-parse", "HEAD");

        yield* git("remote", "add", "origin", directory);
        yield* git("checkout", "--detach", base);
        const evidence = { ...run, head_sha: base };
        const requests: Array<string> = [];
        let scenario = "success";
        let merged = false;
        let mergedCheckout = checkout;

        const buildRun = {
          ...evidence,
          id: 43,
          event: "pull_request",
          head_branch: "changeset-release/main",
          head_sha: head,
        };

        const buildJobs = {
          total_count: 2,
          jobs: [
            {
              ...jobs.jobs[0]!,
              name: "Build",
              run_id: 43,
              head_sha: head,
              steps: [
                "Build packages, examples, and docs",
                "Validate versioned release packages",
                "Upload release build",
              ].map((name) => ({ name, status: "completed", conclusion: "success" })),
            },
            {
              ...jobs.jobs[0]!,
              name: "ready",
              run_id: 43,
              head_sha: head,
              steps: [
                {
                  name: "Verify all required gates passed",
                  status: "completed",
                  conclusion: "success",
                },
              ],
            },
          ],
        };

        const client = HttpClient.make((request, url) => {
          requests.push(`${request.method} ${url.pathname}${url.search}`);
          const prefix = `/repos/${repository}/`;
          const route = url.pathname.slice(prefix.length);
          let body: unknown;

          switch (route) {
            case `commits/${mergedCheckout}/pulls`:
              body = [{ number: 516, head: { sha: head } }];
              if (scenario === "ambiguous") body = [body, body];
              break;
            case "pulls/516":
              body = {
                ...pull,
                state: merged ? "closed" : "open",
                merged,
                merge_commit_sha: mergedCheckout,
                base: { ...pull.base, sha: base },
                head: { ...pull.head, sha: head },
              };
              break;
            case "git/ref/heads/main":
              body = {
                object: { sha: scenario === "moved" ? head : merged ? mergedCheckout : base },
              };
              break;
            case "actions/workflows/ci.yml":
              body = { id: 12, path: ".github/workflows/ci.yml", state: "active" };
              break;
            case "actions/workflows/12/runs":
              if (url.searchParams.get("event") === "pull_request") {
                body = {
                  total_count: scenario === "no-build" ? 0 : 1,
                  workflow_runs:
                    scenario === "no-build"
                      ? []
                      : [
                          {
                            ...buildRun,
                            conclusion: scenario === "failed-build" ? "failure" : "success",
                          },
                        ],
                };
                break;
              }
              body = {
                total_count: scenario === "missing" ? 0 : 1,
                workflow_runs:
                  scenario === "missing"
                    ? []
                    : [
                        {
                          ...evidence,
                          status: scenario === "pending" ? "in_progress" : "completed",
                        },
                      ],
              };
              break;
            case "actions/runs/42/attempts/1/jobs":
              body = { ...jobs, jobs: jobs.jobs.map((job) => ({ ...job, head_sha: base })) };
              break;
            case "actions/runs/43/attempts/1/jobs":
              body =
                scenario === "skipped-upload"
                  ? {
                      ...buildJobs,
                      jobs: buildJobs.jobs.map((job) => ({
                        ...job,
                        steps: job.steps.filter((step) => step.name !== "Upload release build"),
                      })),
                    }
                  : buildJobs;
              break;
            case "actions/runs/43":
              body = { ...buildRun, run_attempt: scenario === "build-rerun" ? 2 : 1 };
              break;
            case "actions/runs/42":
              body = { ...evidence, run_attempt: scenario === "rerun" ? 2 : 1 };
              break;
            default:
              return Effect.die(`Unexpected API route: ${route}`);
          }

          return Effect.succeed(
            HttpClientResponse.fromWeb(
              request,
              Response.json(scenario === "malformed" ? {} : body, {
                status: scenario === "api-error" ? 503 : 200,
              }),
            ),
          );
        });

        const prove = decideReleaseCi(
          proveReleaseCi(directory, { base, head, checkout }, 516, "test-read-only-token"),
        ).pipe(Effect.provideService(HttpClient.HttpClient, client));

        expect(yield* prove).toEqual({
          fast: true,
          evidence: { base, head, checkout, runId: 42, runAttempt: 1 },
        });
        expect(requests).toContain(
          `GET /repos/${repository}/actions/workflows/12/runs?head_sha=${base}&event=push&branch=main&per_page=100`,
        );
        expect(requests).toContain(
          `GET /repos/${repository}/actions/runs/42/attempts/1/jobs?per_page=100`,
        );
        expect(requests.every((request) => request.startsWith("GET "))).toBe(true);
        for (scenario of ["missing", "pending", "api-error", "malformed", "rerun", "moved"]) {
          expect(yield* prove).toEqual({ fast: false });
        }
        scenario = "success";
        merged = true;

        const proveMerged = () =>
          decideReleaseCi(
            proveMergedReleaseCi(directory, base, mergedCheckout, "test-read-only-token"),
          ).pipe(Effect.provideService(HttpClient.HttpClient, client));

        // The same source proof survives both supported GitHub merge topologies.
        for (const candidate of [
          checkout,
          yield* git("commit-tree", `${head}^{tree}`, "-p", base, "-m", "Squashed version PR"),
        ]) {
          mergedCheckout = candidate;
          expect(yield* proveMerged()).toEqual({
            fast: true,
            evidence: {
              base,
              head,
              checkout: candidate,
              runId: 42,
              runAttempt: 1,
              buildRunId: 43,
              buildRunAttempt: 1,
            },
          });
          for (scenario of [
            "moved",
            "no-build",
            "ambiguous",
            "failed-build",
            "skipped-upload",
            "build-rerun",
            "rerun",
            "api-error",
          ]) {
            expect(yield* proveMerged()).toEqual({ fast: false });
          }
          scenario = "success";
        }
        yield* verifyBuildEvidence(head, 12, buildRun, buildJobs, "pull_request");
        expect(
          (yield* decideReleaseCi(
            verifyBuildEvidence(
              head,
              12,
              { ...buildRun, head_repository: { full_name: "fork/effect-agent" } },
              buildJobs,
              "pull_request",
            ),
          )).fast,
        ).toBe(false);

        // A successful main artifact still needs its exact attempt and current main.
        const mainBuild = {
          ...buildRun,
          event: "push",
          head_branch: "main",
          head_sha: mergedCheckout,
        };

        const mainClient = HttpClient.make((request, url) =>
          Effect.succeed(
            HttpClientResponse.fromWeb(
              request,
              Response.json(
                url.pathname.endsWith("ci.yml")
                  ? { id: 12, path: ".github/workflows/ci.yml", state: "active" }
                  : url.pathname.endsWith("/jobs")
                    ? {
                        ...buildJobs,
                        jobs: buildJobs.jobs.map((job) => ({ ...job, head_sha: mergedCheckout })),
                      }
                    : url.pathname.endsWith("/main")
                      ? { object: { sha: scenario === "moved" ? base : mergedCheckout } }
                      : { ...mainBuild, run_attempt: scenario === "rerun" ? 2 : 1 },
              ),
            ),
          ),
        );

        const verifyMain = decideReleaseCi(
          verifyMainBuild(directory, mergedCheckout, 43, 1, "test-token"),
        ).pipe(Effect.provideService(HttpClient.HttpClient, mainClient));

        expect((yield* verifyMain).fast).toBe(true);
        for (scenario of ["moved", "rerun"]) expect((yield* verifyMain).fast).toBe(false);
        expect(yield* git("rev-parse", "HEAD")).toBe(base);
        expect(yield* git("status", "--porcelain")).toBe("");
        // Lossy stream decoding must not hide a changed trailing byte in the lockfile.
        yield* git("checkout", "--detach", head);
        yield* fs.writeFile(
          `${directory}/bun.lock`,
          new Uint8Array([...new TextEncoder().encode(lock("0.1.0-beta.100")), 0xc3]),
        );
        yield* git("add", "bun.lock");
        yield* git("commit", "-m", "Invalid UTF-8 metadata");
        expect(
          yield* decideReleaseCi(readMetadata(directory, base, yield* git("rev-parse", "HEAD"))),
        ).toEqual({ fast: false });
        // A deleted file is absent from candidate listings but must still reject reuse.
        yield* git("checkout", "--detach", head);
        yield* git("rm", "scripts/release-ci.ts");
        yield* git("commit", "-m", "Delete source");
        expect(
          yield* decideReleaseCi(readMetadata(directory, base, yield* git("rev-parse", "HEAD"))),
        ).toEqual({ fast: false });
      }),
    30_000,
  );
});

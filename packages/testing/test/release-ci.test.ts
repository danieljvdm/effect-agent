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
  verifyEvidence,
  verifyBuildEvidence,
  verifyMainBuild,
  verifyMetadata,
  verifyRevisions,
} from "../../../scripts/release-ci.ts";

const repository = "danieljvdm/effect-agent";
const packages = ["effect-agent", "@effect-agent/ai-typesafe"];
const base = "a".repeat(40);
const head = "b".repeat(40);
const checkout = "c".repeat(40);
const revisions = { base, head, checkout };

const pre = {
  mode: "pre",
  tag: "beta",
  initialVersions: { "effect-agent": "0.1.0-beta.44" },
  changesets: ["previous-change"],
};

const nextPre = {
  ...pre,
  initialVersions: { ...pre.initialVersions, "@effect-agent/ai-typesafe": "0.1.0-beta.99" },
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
    "packages/ai-typesafe": {
      "name": "@effect-agent/ai-typesafe",
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
            : "# @effect-agent/ai-typesafe\n\n## 0.1.0-beta.100\n\n### Minor Changes\n\n- New provider.\n",
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

it.effect("accepts fixed beta metadata including a new changelog and initial version", () =>
  Effect.gen(function* () {
    expect(yield* decideMetadata(fixture())).toEqual({ fast: true, evidence: "0.1.0-beta.100" });
    expect(
      yield* decideReleaseCi(
        verifyMetadata(packages, ["new-change", "unconsumed-change"], fixture()),
      ),
    ).toEqual({ fast: false });
  }),
);

it.effect("rejects executable manifest changes even beside a valid version bump", () =>
  Effect.gen(function* () {
    for (const [from, to] of [
      ['"module"', '"commonjs"'],
      ['"./src/index.ts"', '"./src/other.ts"'],
      ['"vp test"', '"echo skipped"'],
      ['"catalog:"', '"4.0.0-rc.114"'],
      ['"version": "0.1.0-beta.100"', '"version": "0.2.0-beta.0"'],
      ['"version": "0.1.0-beta.100"', '"version": "0.1.0-beta.101"'],
      ['"version": "0.1.0-beta.100"', '"version": "0.1.0"'],
      ['"name": "effect-agent"', '"name": "effect-agent", "private": true'],
    ]) {
      const changes = replace("packages/effect-agent/package.json", (change) => ({
        ...change,
        after: change.after.replace(from!, to!),
      }));

      expect(yield* decideMetadata(changes)).toEqual({ fast: false });
    }
  }),
);

it.effect("rejects source, tests, config, workflow, changeset and private-workspace deltas", () =>
  Effect.gen(function* () {
    for (const path of [
      "packages/effect-agent/src/index.ts",
      "packages/testing/test/example.test.ts",
      "packages/effect-agent/vite.config.ts",
      "vite.config.ts",
      "package.json",
      ".github/workflows/ci.yml",
      ".github/workflows/release.yml",
      "scripts/release-ci.ts",
      ".changeset/config.json",
      ".changeset/new-change.md",
      "examples/travel-planner/package.json",
    ]) {
      expect(
        yield* decideMetadata([
          ...fixture(),
          { path, before: "original", after: "edited", oldMode: "100644", newMode: "100644" },
        ]),
      ).toEqual({ fast: false });
    }
    for (const newMode of ["100755", "120000", "160000"]) {
      expect(
        yield* decideMetadata(
          replace("packages/effect-agent/CHANGELOG.md", (change) => ({ ...change, newMode })),
        ),
      ).toEqual({ fast: false });
    }
  }),
);

it.effect("rejects changed resolutions, rewritten history and unsupported prerelease state", () =>
  Effect.gen(function* () {
    for (const changes of [
      replace("bun.lock", (change) => ({
        ...change,
        after: change.after.replace("sha512-original", "sha512-other"),
      })),
      replace("bun.lock", (change) => ({
        ...change,
        after: change.after.replace("rc.115", "rc.114"),
      })),
      replace("packages/effect-agent/CHANGELOG.md", (change) => ({
        ...change,
        after: change.after.replace("Previous release.", "Rewritten release."),
      })),
      replace("packages/effect-agent/CHANGELOG.md", (change) => ({
        ...change,
        after: change.after.replace("beta.100", "beta.101"),
      })),
      ...[
        { ...nextPre, mode: "exit" },
        { ...nextPre, tag: "next" },
        { ...nextPre, initialVersions: { "effect-agent": "0.1.0-beta.99" } },
        { ...nextPre, initialVersions: pre.initialVersions },
        { ...nextPre, initialVersions: { ...nextPre.initialVersions, private: "1.0.0" } },
        { ...nextPre, changesets: ["new-change"] },
        { ...nextPre, changesets: ["previous-change", "unknown-change"] },
        { ...nextPre, changesets: ["previous-change", "new-change", "new-change"] },
        { ...nextPre, extra: "unsupported" },
      ].map((value) =>
        replace(".changeset/pre.json", (change) => ({ ...change, after: JSON.stringify(value) })),
      ),
      fixture().filter((change) => !change.path.endsWith("ai-typesafe/package.json")),
    ])
      expect(yield* decideMetadata(changes)).toEqual({ fast: false });
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

it.effect("requires exact ordinary workflow, revision, run and successful command evidence", () =>
  Effect.gen(function* () {
    expect((yield* decideReleaseCi(verifyEvidence(base, 12, run, jobs))).fast).toBe(true);
    for (const altered of [
      { ...run, head_sha: head },
      { ...run, workflow_id: 13 },
      { ...run, event: "pull_request" },
      { ...run, head_branch: "changeset-release/main" },
      { ...run, path: ".github/workflows/other.yml" },
      { ...run, name: "Other CI" },
      { ...run, repository: { full_name: "attacker/effect-agent" } },
      { ...run, status: "in_progress" },
      { ...run, conclusion: "failure" },
      { ...run, conclusion: "cancelled" },
      { ...run, conclusion: null },
      { ...run, run_attempt: 0 },
    ])
      expect(yield* decideReleaseCi(verifyEvidence(base, 12, altered, jobs))).toEqual({
        fast: false,
      });
    for (const job of jobs.jobs) {
      for (const altered of [
        { ...job, conclusion: "skipped" },
        { ...job, head_sha: head },
        { ...job, run_id: 41 },
        { ...job, steps: [] },
        { ...job, steps: job.steps.map((step) => ({ ...step, conclusion: "skipped" })) },
      ]) {
        expect(
          yield* decideReleaseCi(
            verifyEvidence(base, 12, run, {
              ...jobs,
              jobs: jobs.jobs.map((existing) => (existing === job ? altered : existing)),
            }),
          ),
        ).toEqual({ fast: false });
      }
    }
    for (const altered of [
      { ...jobs, total_count: 11 },
      { ...jobs, total_count: 9, jobs: jobs.jobs.slice(1) },
      { ...jobs, total_count: 11, jobs: [...jobs.jobs, jobs.jobs[0]!] },
    ])
      expect(yield* decideReleaseCi(verifyEvidence(base, 12, run, altered))).toEqual({
        fast: false,
      });
  }),
);

const pull = {
  number: 516,
  merged: false,
  state: "open",
  merge_commit_sha: checkout,
  base: { ref: "main", sha: base, repo: { full_name: repository } },
  head: { ref: "changeset-release/main", sha: head, repo: { full_name: repository } },
};

it.effect("binds head and merge checkout to the current base and rejects base movement", () =>
  Effect.gen(function* () {
    const verify = (pr = pull, main = base, parents = `${base} ${head}`, tree = head) =>
      decideReleaseCi(verifyRevisions(revisions, pr, main, parents, tree, head));

    expect((yield* verify()).fast).toBe(true);
    for (const candidate of [
      { ...pull, state: "closed" },
      { ...pull, merge_commit_sha: head },
      { ...pull, base: { ...pull.base, sha: checkout } },
      { ...pull, head: { ...pull.head, sha: checkout } },
      { ...pull, head: { ...pull.head, repo: { full_name: "attacker/effect-agent" } } },
    ])
      expect(yield* verify(candidate)).toEqual({ fast: false });
    expect(yield* verify(pull, checkout)).toEqual({ fast: false });
    expect(yield* verify(pull, base, `${head} ${base}`)).toEqual({ fast: false });
    expect(yield* verify(pull, base, `${base} ${head} ${checkout}`)).toEqual({ fast: false });
    expect(yield* verify(pull, base, `${base} ${head}`, checkout)).toEqual({ fast: false });
  }),
);

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

it.effect("checks packed identity and every JavaScript and type export", () =>
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
    for (const altered of [
      { ...pack, name: "wrong" },
      { ...pack, version: "0.1.0-beta.99" },
      ...pack.files.map((file) => ({
        ...pack,
        files: pack.files.filter((entry) => entry !== file),
      })),
    ])
      expect(Exit.isFailure(yield* Effect.exit(verifyPackedFiles(manifest, [altered])))).toBe(true);
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
          verifyMainBuild(mergedCheckout, 43, 1, "test-token"),
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

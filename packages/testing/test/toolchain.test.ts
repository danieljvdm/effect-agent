import { NodeServices } from "@effect/platform-node";
import { expect, layer } from "@effect/vitest";
import { Effect, FileSystem, Path, Schema, Stream } from "effect";
import { Yaml } from "effect/unstable/encoding";
import { ChildProcess } from "effect/unstable/process";

import {
  InvalidCommitSha,
  ReleaseTreeMismatch,
  verifyChangesetsRelease,
} from "../../../scripts/verify-changesets-release.ts";

const Dependencies = Schema.Record(Schema.String, Schema.String);
const PackageManifest = Schema.Struct({
  name: Schema.optionalKey(Schema.String),
  version: Schema.optionalKey(Schema.String),
  private: Schema.optionalKey(Schema.Boolean),
  workspaces: Schema.optionalKey(Schema.Array(Schema.String)),
  scripts: Schema.optionalKey(Dependencies),
  catalog: Schema.optionalKey(Dependencies),
  dependencies: Schema.optionalKey(Dependencies),
  devDependencies: Schema.optionalKey(Dependencies),
  optionalDependencies: Schema.optionalKey(Dependencies),
  overrides: Schema.optionalKey(Dependencies),
  peerDependencies: Schema.optionalKey(Dependencies),
});
type PackageManifest = typeof PackageManifest.Type;

const ChangesetConfig = Schema.Struct({
  fixed: Schema.Array(Schema.Array(Schema.String)),
  linked: Schema.Array(Schema.Array(Schema.String)),
});

const WorkflowStep = Schema.Struct({
  id: Schema.optionalKey(Schema.String),
  name: Schema.optionalKey(Schema.String),
  if: Schema.optionalKey(Schema.String),
  uses: Schema.optionalKey(Schema.String),
  with: Schema.optionalKey(Schema.Record(Schema.String, Schema.Unknown)),
  env: Schema.optionalKey(Schema.Record(Schema.String, Schema.String)),
  run: Schema.optionalKey(Schema.String),
});
const WorkflowJob = Schema.Struct({
  name: Schema.optionalKey(Schema.String),
  if: Schema.optionalKey(Schema.String),
  needs: Schema.optionalKey(Schema.Union([Schema.String, Schema.Array(Schema.String)])),
  outputs: Schema.optionalKey(Schema.Record(Schema.String, Schema.String)),
  permissions: Schema.optionalKey(Schema.Record(Schema.String, Schema.String)),
  steps: Schema.optionalKey(Schema.Array(WorkflowStep)),
});
const WorkflowFile = Schema.Struct({
  on: Schema.Record(Schema.String, Schema.Unknown),
  permissions: Schema.optionalKey(Schema.Record(Schema.String, Schema.String)),
  jobs: Schema.Record(Schema.String, WorkflowJob),
});
type WorkflowFile = typeof WorkflowFile.Type;

// Vite+ runs this package test from packages/testing; Bun is the pinned test runtime in CI and locally.
const repositoryRoot = "../..";
const packageNames = [
  "capabilities",
  "core",
  "effect-agent",
  "engine",
  "platform-cloudflare",
  "platform-node",
  "pr-review",
  "sandbox",
  "sandbox-local",
  "session",
  "storage-cloudflare",
  "storage-memory",
  "storage-sqlite",
  "testing",
] as const;

/**
 * The one framework package allowed to depend on upstream provider adapters:
 * its CLI/Action host entrypoints ship batteries-included provider bindings
 * (D-034, ADR-0016). The factory surface itself stays Model-agnostic, and
 * provider WRAPPER packages remain deliberately absent.
 */
const providerConsumingPackages = new Set<string>(["pr-review"]);
const exampleNames = [
  "code-mode-cloudflare",
  "demo",
  "pr-review",
  "providers",
  "repo-ops",
] as const;
const effectTestPackageNames = [
  "capabilities",
  "engine",
  "platform-cloudflare",
  "platform-node",
  "sandbox-local",
  "session",
  "storage-cloudflare",
  "storage-memory",
  "storage-sqlite",
  "testing",
] as const;
const productionPackageNames = [
  "capabilities",
  "core",
  "effect-agent",
  "engine",
  "platform-cloudflare",
  "platform-node",
  "sandbox",
  "sandbox-local",
  "session",
  "storage-cloudflare",
  "storage-memory",
  "storage-sqlite",
] as const;
// Phase 6: only these two packages may carry Cloudflare dependencies, and only
// the types-only package plus the in-workerd test harness — wrangler and
// application scaffolds stay banned everywhere.
const cloudflarePackageNames: ReadonlyArray<string> = ["platform-cloudflare", "storage-cloudflare"];
const allowedCloudflareToolchainDependencies = new Set([
  "@cloudflare/vitest-pool-workers",
  "@cloudflare/workers-types",
]);
// Phase 6 exit gate "Agent/core/engine packages import no Cloudflare platform
// types", audited at the manifest layer: only the two Cloudflare packages may
// depend on @cloudflare/* or the Durable Object SqlClient, in ANY dependency
// section. Everything inward of them must stay platform-clean so the semantic
// coordinator never gains a conditional platform branch (deployment spec §3.1).
const inwardPackageNames = [
  "capabilities",
  "core",
  "engine",
  "sandbox",
  "sandbox-local",
  "session",
  "storage-memory",
  "storage-sqlite",
] as const;
const cloudflareOnlyDependencies = new Set(["@effect/sql-sqlite-do"]);
const dependencySections = [
  "dependencies",
  "devDependencies",
  "optionalDependencies",
  "peerDependencies",
] as const;
const forbiddenScaffoldDependencies = new Set([
  "@cloudflare/workers-types",
  "@types/react",
  "@types/react-dom",
  "@vitejs/plugin-react",
  "react",
  "react-dom",
  "wrangler",
]);
const providerAdapterDependencies = ["@effect/ai-openai", "@effect/ai-anthropic"] as const;
/**
 * The documented inward-only package graph (ARCHITECTURE.md §11, AGENTS.md
 * "Package dependency direction"): every `@effect-agent/*` edge a framework
 * manifest may declare, in any dependency section. `capabilities -> sandbox`
 * is the CodeExecutor port edge registered by ADR-0017 (declared here before
 * C1 adds the manifest edge). The `-> testing` entries are the two documented
 * dev-only exceptions; the graph test itself pins them to `devDependencies`,
 * and every other package stays clean of `testing` in every section.
 */
const allowedWorkspaceEdges: Record<(typeof packageNames)[number], ReadonlyArray<string>> = {
  capabilities: ["core", "engine", "sandbox"],
  core: [],
  "effect-agent": ["capabilities", "core", "engine"],
  engine: ["core"],
  "platform-cloudflare": [
    "capabilities",
    "core",
    "engine",
    "sandbox",
    "session",
    "storage-cloudflare",
    "testing",
  ],
  "platform-node": ["capabilities", "core", "engine", "session", "storage-sqlite"],
  "pr-review": ["effect-agent"],
  sandbox: ["core"],
  "sandbox-local": ["core", "sandbox"],
  session: ["core", "engine"],
  "storage-cloudflare": ["core", "session", "testing"],
  "storage-memory": ["core", "session"],
  "storage-sqlite": ["core", "session"],
  testing: [
    "capabilities",
    "core",
    "engine",
    "platform-node",
    "sandbox",
    "session",
    "storage-memory",
    "storage-sqlite",
  ],
};

const readManifest = (path: string) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const contents = yield* fs.readFileString(path);
    return yield* Schema.decodeEffect(Schema.fromJsonString(PackageManifest))(contents);
  });

const readChangesetConfig = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const contents = yield* fs.readFileString(`${repositoryRoot}/.changeset/config.json`);
  return yield* Schema.decodeEffect(Schema.fromJsonString(ChangesetConfig))(contents);
});

const readRepositoryFile = (path: string) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    return yield* fs.readFileString(`${repositoryRoot}/${path}`);
  });

const readWorkflow = (path: string) =>
  Effect.gen(function* () {
    const contents = yield* readRepositoryFile(path);
    return yield* Schema.decodeUnknownEffect(WorkflowFile)(Yaml.parse(contents));
  });

const workflowStep = (workflow: WorkflowFile, jobName: string, stepName: string) => {
  const step = workflow.jobs[jobName]?.steps?.find((candidate) => candidate.name === stepName);
  expect(step, `${jobName} must contain the ${stepName} step`).toBeDefined();
  return step;
};

const readDirectory = (path: string) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    return yield* fs.readDirectory(path);
  });

const exists = (path: string) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    return yield* fs.exists(path);
  });

const runFixtureCommand = Effect.fn("toolchainTest.runFixtureCommand")(function* (
  cwd: string,
  command: string,
  args: ReadonlyArray<string>,
) {
  const child = yield* ChildProcess.make(command, args, {
    cwd,
    stderr: "pipe",
    stdout: "pipe",
  });
  const [output, exitCode] = yield* Effect.all([
    Stream.mkString(Stream.decodeText(child.all)),
    child.exitCode,
  ]);
  if (exitCode !== 0) {
    return yield* Effect.die(
      new Error(`${[command, ...args].join(" ")} exited with ${exitCode}:\n${output}`),
    );
  }
  return output.trim();
});

const manifestDependencies = (manifest: PackageManifest): ReadonlyArray<string> =>
  dependencySections.flatMap((section) => Object.keys(manifest[section] ?? {}));

const effectDependencies = (manifest: PackageManifest): ReadonlyArray<string> =>
  dependencySections
    .map((section) => manifest[section]?.effect)
    .filter((version): version is string => version !== undefined);

layer(NodeServices.layer)("workspace toolchain", (it) => {
  it.effect("executes an Effect program through the Vite+ test runner", () =>
    Effect.sync(() => {
      expect("ready").toBe("ready");
    }),
  );

  it.effect("keeps framework packages separate from leaf example workspaces", () =>
    Effect.gen(function* () {
      const [activePackages, activeExamples, rootEntries, rootManifest] = yield* Effect.all([
        readDirectory(`${repositoryRoot}/packages`),
        readDirectory(`${repositoryRoot}/examples`),
        readDirectory(repositoryRoot),
        readManifest(`${repositoryRoot}/package.json`),
      ]);

      expect([...activePackages].sort()).toEqual([...packageNames].sort());
      expect([...activeExamples].sort()).toEqual([...exampleNames].sort());
      expect(rootManifest.workspaces).toEqual(["packages/*", "examples/*"]);
      expect(rootEntries).not.toContain("apps");
      expect(rootEntries).not.toContain("wrangler.toml");
      expect(rootEntries).not.toContain("wrangler.json");
      expect(rootEntries).not.toContain("wrangler.jsonc");
    }),
  );

  it.effect("keeps every public framework package on one fixed release train", () =>
    Effect.gen(function* () {
      const config = yield* readChangesetConfig;
      const publicPackageNames: Array<string> = [];

      for (const packageName of packageNames) {
        const manifest = yield* readManifest(
          `${repositoryRoot}/packages/${packageName}/package.json`,
        );
        if (manifest.private !== true && manifest.name !== undefined) {
          publicPackageNames.push(manifest.name);
        }
      }

      expect(config.fixed).toHaveLength(1);
      expect([...(config.fixed[0] ?? [])].sort()).toEqual(publicPackageNames.sort());
      expect(config.linked).toEqual([]);
    }),
  );

  it.effect("keeps generated release PRs behind a trusted integrity gate", () =>
    Effect.gen(function* () {
      const [ciWorkflow, reviewWorkflow, releaseWorkflow, rootManifest] = yield* Effect.all([
        readWorkflow(".github/workflows/ci.yml"),
        readWorkflow(".github/workflows/pr-review.yml"),
        readWorkflow(".github/workflows/release.yml"),
        readManifest(`${repositoryRoot}/package.json`),
      ]);
      const generatedPaths = [
        ".changeset/**",
        "bun.lock",
        "packages/*/CHANGELOG.md",
        "packages/*/package.json",
      ];

      expect(ciWorkflow.on).toEqual({
        pull_request: { "paths-ignore": generatedPaths },
      });
      expect(ciWorkflow.jobs.checks?.if).toBeUndefined();
      expect(ciWorkflow.jobs.test?.if).toBeUndefined();
      expect(ciWorkflow.jobs.build?.if).toBeUndefined();
      expect(ciWorkflow.jobs["release-integrity"]).toBeUndefined();

      const readyJob = ciWorkflow.jobs.ready;
      expect(readyJob?.needs).toEqual(["checks", "test", "build"]);
      expect(readyJob?.if).toBe("${{ always() }}");
      const readyStep = workflowStep(ciWorkflow, "ready", "Verify all ordinary gates passed");
      expect(readyStep?.env).toEqual({
        BUILD_RESULT: "${{ needs.build.result }}",
        CHECKS_RESULT: "${{ needs.checks.result }}",
        TEST_RESULT: "${{ needs.test.result }}",
      });
      expect(readyStep?.run).toContain('test "$CHECKS_RESULT" = "success"');
      expect(readyStep?.run).toContain('test "$TEST_RESULT" = "success"');
      expect(readyStep?.run).toContain('test "$BUILD_RESULT" = "success"');

      expect(reviewWorkflow.on).toEqual({
        pull_request: {
          types: ["opened", "reopened", "ready_for_review", "synchronize", "labeled"],
          "paths-ignore": generatedPaths,
        },
      });
      expect(reviewWorkflow.jobs.review?.if).toBe(
        "${{ !github.event.pull_request.draft && (github.event.action != 'labeled' || github.event.label.name == 'pr-review:final-audit') }}",
      );

      expect(releaseWorkflow.on).toEqual({ push: { branches: ["main"] } });
      expect(releaseWorkflow.permissions).toEqual({});
      expect(releaseWorkflow.jobs.release?.permissions).toEqual({
        contents: "write",
        "id-token": "write",
        "pull-requests": "write",
      });
      expect(releaseWorkflow.jobs.release?.permissions?.checks).toBeUndefined();
      const externalActionReferences = (releaseWorkflow.jobs.release?.steps ?? []).flatMap(
        (step) => (step.uses === undefined ? [] : [step.uses]),
      );
      expect(externalActionReferences).not.toHaveLength(0);
      expect(externalActionReferences.every((uses) => /^[^@]+@[0-9a-f]{40}$/.test(uses))).toBe(
        true,
      );
      const changesetsStep = workflowStep(releaseWorkflow, "release", "Version or publish");
      expect(changesetsStep?.id).toBe("changesets");
      expect(changesetsStep?.uses).toBe(
        "changesets/action@a45c4d594aa4e2c509dc14a9f2b3b67ba3780d0d",
      );
      expect(changesetsStep?.with).toMatchObject({ publish: "bun run ci:publish" });

      const resolveStep = workflowStep(
        releaseWorkflow,
        "release",
        "Resolve the generated release head",
      );
      expect(resolveStep?.if).toBe("${{ steps.changesets.outputs.pullRequestNumber != '' }}");
      expect(resolveStep?.run).toContain('test "$HEAD_REPOSITORY" = "$GITHUB_REPOSITORY"');
      expect(resolveStep?.run).toContain('test "$HEAD_REF" = "changeset-release/main"');
      expect(resolveStep?.run).toContain('test "$BASE_SHA" = "$GITHUB_SHA"');

      const verifyStep = workflowStep(
        releaseWorkflow,
        "release",
        "Regenerate and verify the release tree",
      );
      expect(verifyStep?.id).toBe("verify-release");
      expect(verifyStep?.env).toEqual({ GITHUB_TOKEN: "${{ github.token }}" });
      expect(verifyStep?.run).toContain("vp run verify:changesets-release");

      const reportStep = workflowStep(
        releaseWorkflow,
        "report-release-check",
        "Report the generated release check",
      );
      const reportJob = releaseWorkflow.jobs["report-release-check"];
      expect(reportJob?.needs).toBe("release");
      expect(reportJob?.if).toBe("${{ always() && needs.release.outputs.release-head-sha != '' }}");
      expect(reportJob?.permissions).toEqual({
        checks: "write",
        contents: "read",
        "pull-requests": "read",
      });
      expect(reportJob?.steps?.every((step) => step.uses === undefined)).toBe(true);
      expect(reportStep?.run).toContain('CONCLUSION="failure"');
      expect(reportStep?.run).toContain('if [ "$VERIFY_OUTCOME" != "success" ]');
      expect(reportStep?.run).toContain('CONCLUSION="success"');
      expect(reportStep?.run).toContain('CURRENT_HEAD_SHA="$(gh api');
      expect(reportStep?.run).toContain('CURRENT_BASE_SHA="$(gh api');
      expect(reportStep?.run).toContain("strict_required_status_checks_policy == true");
      expect(reportStep?.run).toContain(
        'gh api --method POST "repos/${GITHUB_REPOSITORY}/check-runs"',
      );
      expect(reportStep?.run).toContain('-f name="ready"');
      expect(
        Object.entries(releaseWorkflow.jobs)
          .filter(([, job]) => job.permissions?.checks === "write")
          .map(([jobName]) => jobName),
      ).toEqual(["report-release-check"]);

      expect(rootManifest.scripts?.["verify:changesets-release"]).toBe(
        "bun scripts/verify-changesets-release.ts",
      );
    }),
  );

  it.effect(
    "verifies generated release trees and cleans temporary worktrees",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const temporaryRoot = yield* fs.makeTempDirectoryScoped({
          prefix: "effect-agent-release-verifier-test-",
        });
        const fixtureRoot = path.join(temporaryRoot, "repository");
        const packageDirectory = path.join(fixtureRoot, "packages", "fixture");
        const changesetDirectory = path.join(fixtureRoot, ".changeset");
        const changesetBinary = path.resolve(repositoryRoot, "node_modules", ".bin", "changeset");

        yield* fs.makeDirectory(packageDirectory, { recursive: true });
        yield* fs.makeDirectory(changesetDirectory, { recursive: true });
        yield* fs.writeFileString(
          path.join(fixtureRoot, "package.json"),
          `{
  "name": "release-verifier-fixture",
  "private": true,
  "workspaces": ["packages/*"]
}\n`,
        );
        yield* fs.writeFileString(
          path.join(packageDirectory, "package.json"),
          `{
  "name": "release-verifier-package",
  "version": "1.0.0"
}\n`,
        );
        yield* fs.writeFileString(
          path.join(changesetDirectory, "config.json"),
          `{
  "changelog": false,
  "commit": false,
  "fixed": [],
  "linked": [],
  "access": "restricted",
  "baseBranch": "main",
  "updateInternalDependencies": "patch",
  "ignore": []
}\n`,
        );
        yield* fs.writeFileString(
          path.join(changesetDirectory, "fixture.md"),
          `---
"release-verifier-package": patch
---

Exercise the generated release verifier.
`,
        );

        yield* runFixtureCommand(temporaryRoot, "git", [
          "init",
          "--initial-branch=main",
          fixtureRoot,
        ]);
        yield* runFixtureCommand(fixtureRoot, "git", ["config", "user.name", "Verifier Test"]);
        yield* runFixtureCommand(fixtureRoot, "git", [
          "config",
          "user.email",
          "verifier@example.invalid",
        ]);
        yield* runFixtureCommand(fixtureRoot, "git", ["add", "--all"]);
        yield* runFixtureCommand(fixtureRoot, "git", ["commit", "-m", "fixture base"]);
        const baseSha = yield* runFixtureCommand(fixtureRoot, "git", ["rev-parse", "HEAD"]);

        yield* runFixtureCommand(fixtureRoot, changesetBinary, ["version"]);
        yield* runFixtureCommand(fixtureRoot, "bun", ["install", "--ignore-scripts"]);
        yield* runFixtureCommand(fixtureRoot, "git", ["add", "--all"]);
        yield* runFixtureCommand(fixtureRoot, "git", ["commit", "-m", "generated release"]);
        const generatedHead = yield* runFixtureCommand(fixtureRoot, "git", ["rev-parse", "HEAD"]);

        yield* verifyChangesetsRelease({
          baseSha,
          changesetBinary,
          headSha: generatedHead,
          repositoryRoot: fixtureRoot,
        });
        const worktreesAfterSuccess = yield* runFixtureCommand(fixtureRoot, "git", [
          "worktree",
          "list",
          "--porcelain",
        ]);
        expect(worktreesAfterSuccess.match(/^worktree /gm)).toHaveLength(1);

        yield* fs.writeFileString(
          path.join(packageDirectory, "package.json"),
          `{
  "name": "release-verifier-package",
  "version": "9.9.9"
}\n`,
        );
        yield* runFixtureCommand(fixtureRoot, "git", ["add", "--all"]);
        yield* runFixtureCommand(fixtureRoot, "git", ["commit", "-m", "alter generated output"]);
        const alteredHead = yield* runFixtureCommand(fixtureRoot, "git", ["rev-parse", "HEAD"]);
        const alteredFailure = yield* Effect.flip(
          verifyChangesetsRelease({
            baseSha,
            changesetBinary,
            headSha: alteredHead,
            repositoryRoot: fixtureRoot,
          }),
        );
        expect(
          Schema.decodeUnknownSync(ReleaseTreeMismatch)(alteredFailure).changedPaths,
        ).toContain("packages/fixture/package.json");

        yield* runFixtureCommand(fixtureRoot, "git", ["checkout", "--detach", generatedHead]);
        yield* fs.writeFileString(path.join(fixtureRoot, "unexpected.txt"), "unexpected\n");
        yield* runFixtureCommand(fixtureRoot, "git", ["add", "--all"]);
        yield* runFixtureCommand(fixtureRoot, "git", ["commit", "-m", "add unexpected file"]);
        const unexpectedHead = yield* runFixtureCommand(fixtureRoot, "git", ["rev-parse", "HEAD"]);
        const unexpectedFailure = yield* Effect.flip(
          verifyChangesetsRelease({
            baseSha,
            changesetBinary,
            headSha: unexpectedHead,
            repositoryRoot: fixtureRoot,
          }),
        );
        expect(
          Schema.decodeUnknownSync(ReleaseTreeMismatch)(unexpectedFailure).changedPaths,
        ).toContain("unexpected.txt");

        const invalidShaFailure = yield* Effect.flip(
          verifyChangesetsRelease({
            baseSha: "not-a-commit",
            changesetBinary,
            headSha: generatedHead,
            repositoryRoot: fixtureRoot,
          }),
        );
        expect(Schema.decodeUnknownSync(InvalidCommitSha)(invalidShaFailure).label).toBe(
          "--base-sha",
        );

        const worktreesAfterFailures = yield* runFixtureCommand(fixtureRoot, "git", [
          "worktree",
          "list",
          "--porcelain",
        ]);
        expect(worktreesAfterFailures.match(/^worktree /gm)).toHaveLength(1);
      }),
    60_000,
  );

  it.effect("keeps browser and deployment scaffolds out of framework manifests", () =>
    Effect.gen(function* () {
      const rootDependencies = manifestDependencies(
        yield* readManifest(`${repositoryRoot}/package.json`),
      );
      expect(
        rootDependencies.filter((dependency) => forbiddenScaffoldDependencies.has(dependency)),
      ).toEqual([]);
      expect(rootDependencies.some((dependency) => dependency.startsWith("@cloudflare/"))).toBe(
        false,
      );

      for (const packageName of packageNames) {
        const manifest = yield* readManifest(
          `${repositoryRoot}/packages/${packageName}/package.json`,
        );
        const dependencies = manifestDependencies(manifest);
        const cloudflareAllowance = cloudflarePackageNames.includes(packageName)
          ? allowedCloudflareToolchainDependencies
          : new Set<string>();

        expect(
          dependencies.filter(
            (dependency) =>
              forbiddenScaffoldDependencies.has(dependency) && !cloudflareAllowance.has(dependency),
          ),
        ).toEqual([]);
        expect(
          dependencies.filter(
            (dependency) =>
              dependency.startsWith("@cloudflare/") && !cloudflareAllowance.has(dependency),
          ),
        ).toEqual([]);
      }
    }),
  );

  it.effect("keeps Cloudflare platform dependencies out of inward framework manifests", () =>
    Effect.gen(function* () {
      // The P6 gate at the manifest layer: core/engine/capabilities/session and
      // the non-Cloudflare storage adapters carry no @cloudflare/* dependency and
      // no @effect/sql-sqlite-do in any dependency section — the Durable Object
      // SqlClient and the Cloudflare types stay confined to storage-cloudflare
      // and platform-cloudflare (import-level: storage-cloudflare never imports
      // the cloudflare:workers runtime module; platform-cloudflare alone does).
      for (const packageName of inwardPackageNames) {
        const manifest = yield* readManifest(
          `${repositoryRoot}/packages/${packageName}/package.json`,
        );
        const dependencies = manifestDependencies(manifest);
        expect(
          dependencies.filter(
            (dependency) =>
              dependency.startsWith("@cloudflare/") || cloudflareOnlyDependencies.has(dependency),
          ),
        ).toEqual([]);
      }

      // The confinement side: every workspace consumer of the Durable Object
      // SqlClient is one of the two Cloudflare packages, catalog-pinned.
      for (const packageName of cloudflarePackageNames) {
        const manifest = yield* readManifest(
          `${repositoryRoot}/packages/${packageName}/package.json`,
        );
        expect(manifest.dependencies?.["@effect/sql-sqlite-do"]).toBe("catalog:");
      }
    }),
  );

  it.effect("keeps workspace dependency edges on the documented inward-only graph", () =>
    Effect.gen(function* () {
      // A dependency's effective target is the aliased package for an
      // `npm:` specifier, otherwise the key itself — so a forbidden edge
      // cannot hide behind an innocuous dependency key.
      const npmAliasTarget = (specifier: string): string | undefined => {
        if (!specifier.startsWith("npm:")) {
          return undefined;
        }
        const aliased = specifier.slice("npm:".length);
        const versionSeparator = aliased.indexOf("@", aliased.startsWith("@") ? 1 : 0);
        return versionSeparator === -1 ? aliased : aliased.slice(0, versionSeparator);
      };
      const workspaceTarget = (name: string, specifier: string): string | undefined => {
        const target = npmAliasTarget(specifier) ?? name;
        if (target === "effect-agent") {
          return target;
        }
        return target.startsWith("@effect-agent/")
          ? target.slice("@effect-agent/".length)
          : undefined;
      };

      for (const packageName of packageNames) {
        const manifest = yield* readManifest(
          `${repositoryRoot}/packages/${packageName}/package.json`,
        );
        const allowed = allowedWorkspaceEdges[packageName];
        for (const section of dependencySections) {
          const edges = Object.entries(manifest[section] ?? {})
            .map(([name, specifier]) => workspaceTarget(name, specifier))
            .filter((edge): edge is string => edge !== undefined);
          expect(
            edges.filter((edge) => !allowed.includes(edge)),
            `${packageName} ${section} declares a workspace edge outside the documented dependency graph`,
          ).toEqual([]);
          // The two permitted `-> testing` edges are dev-only: they must not
          // reappear in dependencies, peerDependencies, or optionalDependencies,
          // where they would ship or leak into consumer resolution.
          if (section !== "devDependencies") {
            expect(
              edges,
              `${packageName} may consume @effect-agent/testing only as a devDependency`,
            ).not.toContain("testing");
          }
        }
      }
    }),
  );

  it.effect("keeps provider adapters catalog-pinned and confined to leaf examples", () =>
    Effect.gen(function* () {
      const root = yield* readManifest(`${repositoryRoot}/package.json`);
      const demo = yield* readManifest(`${repositoryRoot}/examples/demo/package.json`);
      const providers = yield* readManifest(`${repositoryRoot}/examples/providers/package.json`);
      const repoOps = yield* readManifest(`${repositoryRoot}/examples/repo-ops/package.json`);
      const prReview = yield* readManifest(`${repositoryRoot}/examples/pr-review/package.json`);
      const demoDependencies = manifestDependencies(demo);
      const providerDependencies = manifestDependencies(providers);
      const repoOpsDependencies = manifestDependencies(repoOps);
      const prReviewDependencies = manifestDependencies(prReview);

      expect(demo.name).toBe("@effect-agent/example-demo");
      expect(demo.dependencies?.["@effect-agent/core"]).toBe("workspace:*");
      expect(demo.dependencies?.["@effect-agent/engine"]).toBe("workspace:*");
      expect(demo.dependencies?.["@effect-agent/testing"]).toBe("workspace:*");
      expect(demo.dependencies?.["@effect/ai-openai"]).toBe("catalog:");
      expect(demo.dependencies?.["@effect/atom-react"]).toBe("catalog:");
      expect(demo.dependencies?.["@tanstack/react-start"]).toBe("catalog:");
      expect(demo.dependencies?.["@base-ui/react"]).toBe("catalog:");
      expect(demo.dependencies?.react).toBe("catalog:");
      expect(demo.dependencies?.effect).toBe("catalog:");
      expect(root.catalog?.["@effect/ai-openai"]).toBe(root.catalog?.effect);
      expect(root.catalog?.["@effect/ai-anthropic"]).toBe(root.catalog?.effect);
      expect(demoDependencies).not.toContain("wrangler");
      expect(demoDependencies.some((dependency) => dependency.startsWith("@cloudflare/"))).toBe(
        false,
      );
      expect(providers.name).toBe("@effect-agent/example-providers");
      expect(providers.dependencies?.["@effect-agent/core"]).toBe("workspace:*");
      expect(providers.dependencies?.["@effect-agent/testing"]).toBe("workspace:*");
      expect(providers.dependencies?.effect).toBe("catalog:");
      expect(providers.dependencies?.["@effect/ai-openai"]).toBe("catalog:");
      expect(providers.dependencies?.["@effect/ai-anthropic"]).toBe("catalog:");
      expect(providerDependencies).not.toContain("wrangler");
      expect(providerDependencies.some((dependency) => dependency.startsWith("@cloudflare/"))).toBe(
        false,
      );
      // P7: the repo-ops evidence auditor is the third leaf example workspace.
      expect(repoOps.name).toBe("@effect-agent/example-repo-ops");
      expect(repoOps.dependencies?.["@effect-agent/core"]).toBe("workspace:*");
      expect(repoOps.dependencies?.["@effect-agent/testing"]).toBe("workspace:*");
      expect(repoOps.dependencies?.effect).toBe("catalog:");
      expect(repoOps.dependencies?.["@effect/ai-openai"]).toBe("catalog:");
      expect(repoOpsDependencies).not.toContain("wrangler");
      expect(repoOpsDependencies.some((dependency) => dependency.startsWith("@cloudflare/"))).toBe(
        false,
      );
      // The pr-review example is a consumer of the packaged reviewer (D-034):
      // provider bindings reach it through @effect-agent/pr-review, so it no
      // longer declares provider adapters itself.
      expect(prReview.name).toBe("@effect-agent/example-pr-review");
      expect(prReview.dependencies?.["@effect-agent/pr-review"]).toBe("workspace:*");
      expect(prReview.dependencies?.["effect-agent"]).toBe("workspace:*");
      expect(prReview.dependencies?.effect).toBe("catalog:");
      expect(prReviewDependencies).not.toContain("wrangler");
      expect(prReviewDependencies.some((dependency) => dependency.startsWith("@cloudflare/"))).toBe(
        false,
      );

      // The Code Mode Cloudflare demo is the one example that legitimately
      // deploys to Cloudflare (D-035, ADR-0017): it consumes the Dynamic
      // Worker executor from @effect-agent/platform-cloudflare and queries a
      // SQLite Durable Object, so it carries the Durable Object SqlClient and
      // the types-only Cloudflare package. wrangler is NOT a dependency — the
      // deploy/dev scripts invoke it through bunx, and the test lane uses
      // programmatic Miniflare.
      const codeModeCloudflare = yield* readManifest(
        `${repositoryRoot}/examples/code-mode-cloudflare/package.json`,
      );
      const codeModeCloudflareDependencies = manifestDependencies(codeModeCloudflare);
      expect(codeModeCloudflare.name).toBe("@effect-agent/example-code-mode-cloudflare");
      expect(codeModeCloudflare.dependencies?.["@effect-agent/platform-cloudflare"]).toBe(
        "workspace:*",
      );
      expect(codeModeCloudflare.dependencies?.["@effect-agent/capabilities"]).toBe("workspace:*");
      expect(codeModeCloudflare.dependencies?.["@effect/sql-sqlite-do"]).toBe("catalog:");
      expect(codeModeCloudflare.dependencies?.["@effect/ai-openai"]).toBe("catalog:");
      expect(codeModeCloudflare.dependencies?.effect).toBe("catalog:");
      expect(codeModeCloudflare.devDependencies?.["@cloudflare/workers-types"]).toBe("catalog:");
      expect(codeModeCloudflareDependencies).not.toContain("wrangler");
      // The only allowed @cloudflare/* dependency is the types-only package.
      expect(
        codeModeCloudflareDependencies.filter(
          (dependency) =>
            dependency.startsWith("@cloudflare/") && dependency !== "@cloudflare/workers-types",
        ),
      ).toEqual([]);

      // The packaged reviewer pins its provider adapters through the catalog
      // like every other shared dependency (D-034, ADR-0016).
      const prReviewPackage = yield* readManifest(
        `${repositoryRoot}/packages/pr-review/package.json`,
      );
      for (const adapter of providerAdapterDependencies) {
        expect(prReviewPackage.dependencies?.[adapter]).toBe("catalog:");
      }

      for (const packageName of packageNames) {
        const manifest = yield* readManifest(
          `${repositoryRoot}/packages/${packageName}/package.json`,
        );
        expect(manifestDependencies(manifest)).not.toContain(demo.name);
        expect(manifestDependencies(manifest)).not.toContain(providers.name);
        expect(manifestDependencies(manifest)).not.toContain(repoOps.name);
        expect(manifestDependencies(manifest)).not.toContain(prReview.name);
        expect(manifestDependencies(manifest)).not.toContain(codeModeCloudflare.name);
        if (providerConsumingPackages.has(packageName)) continue;
        for (const adapter of providerAdapterDependencies) {
          expect(manifestDependencies(manifest)).not.toContain(adapter);
        }
      }
    }),
  );

  it.effect(
    "pins Effect test helpers and the single Vite+ Vitest runtime through the root catalog",
    () =>
      Effect.gen(function* () {
        const rootManifest = yield* readManifest(`${repositoryRoot}/package.json`);
        const vitePlusManifest = yield* readManifest(
          `${repositoryRoot}/node_modules/vite-plus/package.json`,
        );
        const effectVersion = rootManifest.catalog?.effect;

        expect(effectVersion).toMatch(/^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?$/);
        expect(rootManifest.catalog?.["@effect/platform-browser"]).toBe(effectVersion);
        expect(rootManifest.catalog?.["@effect/platform-node"]).toBe(effectVersion);
        expect(rootManifest.catalog?.["@effect/sql-sqlite-do"]).toBe(effectVersion);
        expect(rootManifest.catalog?.["@effect/sql-sqlite-node"]).toBe(effectVersion);
        expect(rootManifest.catalog?.["@effect/vitest"]).toBe(effectVersion);
        expect(rootManifest.catalog?.vitest).toBe(vitePlusManifest.dependencies?.vitest);
        expect(rootManifest.overrides?.vitest).toBe("catalog:");
        expect(rootManifest.devDependencies?.effect).toBe("catalog:");
        expect(manifestDependencies(rootManifest)).not.toContain("vitest");

        for (const packageName of packageNames) {
          const manifest = yield* readManifest(
            `${repositoryRoot}/packages/${packageName}/package.json`,
          );
          expect(effectDependencies(manifest)).toEqual(["catalog:"]);
          if (cloudflarePackageNames.includes(packageName)) {
            // P6 WP0 probe outcome (D-P6-7): `vp test` cannot drive the
            // workers pool runner, so the Cloudflare packages run `vitest run`
            // directly against the same catalog-pinned Vitest instance.
            expect(manifest.devDependencies?.vitest).toBe("catalog:");
          } else {
            expect(manifestDependencies(manifest)).not.toContain("vitest");
          }
        }

        for (const packageName of effectTestPackageNames) {
          const manifest = yield* readManifest(
            `${repositoryRoot}/packages/${packageName}/package.json`,
          );
          expect(manifest.devDependencies?.["@effect/vitest"]).toBe("catalog:");
        }

        for (const packageName of productionPackageNames) {
          const manifest = yield* readManifest(
            `${repositoryRoot}/packages/${packageName}/package.json`,
          );
          // No production package ever SHIPS depending on the testing package.
          expect(Object.keys(manifest.dependencies ?? {})).not.toContain("@effect-agent/testing");
          // Exactly two packages may consume it as a devDependency: platform-cloudflare's
          // DC Travel Planner equivalence suite must assemble the SAME fixtures the DN
          // suite runs (P6 plan §6), and storage-cloudflare's in-workerd certification
          // runner executes `certifyDurableAdapters` against the real Durable Object
          // adapters (P7 WP2; the memory/SQLite runners live inside packages/testing
          // because vp's task graph rejects the storage-* → testing dev-edge cycle).
          // Both edges are dev-only and test-only; every other production package stays
          // clean in every dependency section.
          if (packageName !== "platform-cloudflare" && packageName !== "storage-cloudflare") {
            expect(manifestDependencies(manifest)).not.toContain("@effect-agent/testing");
          }
        }
      }),
  );

  it.effect("matches an initialized local Effect checkout to the catalog pin", () =>
    Effect.gen(function* () {
      const rootManifest = yield* readManifest(`${repositoryRoot}/package.json`);
      const effectVersion = rootManifest.catalog?.effect;
      const effectSource = `${repositoryRoot}/repos/effect`;
      const effectPackagePath = `${effectSource}/packages/effect/package.json`;

      // CI deliberately leaves source submodules uninitialized; installed packages remain authoritative there.
      if (!(yield* exists(effectPackagePath))) {
        return;
      }

      const effectPackage = yield* readManifest(effectPackagePath);
      const tagsAtHead = yield* Effect.gen(function* () {
        const child = yield* ChildProcess.make(
          "git",
          ["-C", effectSource, "tag", "--points-at", "HEAD"],
          { stderr: "pipe", stdout: "pipe" },
        );
        const [output, exitCode] = yield* Effect.all([
          Stream.mkString(Stream.decodeText(child.all)),
          child.exitCode,
        ]);

        expect(exitCode).toBe(0);
        return output.split("\n").filter(Boolean);
      });

      expect(effectPackage.name).toBe("effect");
      expect(effectPackage.version).toBe(effectVersion);
      expect(tagsAtHead).toContain(`effect@${effectVersion}`);
    }),
  );
});

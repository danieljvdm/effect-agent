import { fileURLToPath } from "node:url";

import { NodeServices } from "@effect/platform-node";
import { expect, layer } from "@effect/vitest";
import {
  Cause,
  Config,
  Effect,
  Exit,
  FileSystem,
  Path,
  PlatformError,
  Schema,
  Stream,
} from "effect";
import { Command } from "effect/unstable/cli";
import { Yaml } from "effect/unstable/encoding";
import { ChildProcess } from "effect/unstable/process";

import { compareBundles } from "../../../scripts/bundle-size.ts";
import {
  command as releaseCommand,
  PublishManifest,
  withTemporaryManifest,
  withPublishManifests,
} from "../../../scripts/release-publish.ts";
import { verifyPackageExports } from "../../../scripts/verify-package-exports.ts";
import { verifyPackagePurity } from "../../../scripts/verify-package-purity.ts";

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
  uses: Schema.optionalKey(Schema.String),
  needs: Schema.optionalKey(Schema.Union([Schema.String, Schema.Array(Schema.String)])),
  outputs: Schema.optionalKey(Schema.Record(Schema.String, Schema.String)),
  permissions: Schema.optionalKey(Schema.Record(Schema.String, Schema.String)),
  steps: Schema.optionalKey(Schema.Array(WorkflowStep)),
});

const WorkflowFile = Schema.Struct({
  on: Schema.Record(Schema.String, Schema.Unknown),
  concurrency: Schema.optionalKey(
    Schema.Struct({
      group: Schema.String,
      "cancel-in-progress": Schema.Boolean,
    }),
  ),
  permissions: Schema.optionalKey(Schema.Record(Schema.String, Schema.String)),
  jobs: Schema.Record(Schema.String, WorkflowJob),
});

type WorkflowFile = typeof WorkflowFile.Type;

// Vite+ runs this package test from packages/testing; Bun is the pinned test runtime in CI and locally.
// Anchored to this file, not the process CWD: a CWD-relative "../.." escapes
// nested git worktrees (.worktrees/<branch>) into the primary checkout and
// silently audits the wrong tree.
const repositoryRoot = fileURLToPath(new URL("../../..", import.meta.url)).replace(/\/$/, "");

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
  "thread",
  "storage-cloudflare",
  "storage-memory",
  "storage-sqlite",
  "testing",
  "workflow",
] as const;

const privatePackageNames = ["pr-review-action"] as const;

/** Provider bindings belong to leaf applications, never framework packages. */
const providerConsumingPackages = new Set<string>();

const exampleNames = [
  "browser-run-worker-proof",
  "cloudflare-memory",
  "code-mode-cloudflare",
  "demo",
  "pr-review-eval",
  "providers",
  "repo-ops",
  "semantic-memory-eval",
] as const;

const effectTestPackageNames = [
  "capabilities",
  "engine",
  "platform-cloudflare",
  "platform-node",
  "pr-review",
  "sandbox-local",
  "thread",
  "storage-cloudflare",
  "storage-memory",
  "storage-sqlite",
  "testing",
  "workflow",
] as const;

const productionPackageNames = [
  "capabilities",
  "core",
  "effect-agent",
  "engine",
  "platform-cloudflare",
  "platform-node",
  "pr-review",
  "sandbox",
  "sandbox-local",
  "thread",
  "storage-cloudflare",
  "storage-memory",
  "storage-sqlite",
  "workflow",
] as const;

// Only these two packages may carry Cloudflare dependencies. The shared
// allowance is types plus the in-workerd test harness; provider SDKs are
// admitted separately to the outward adapter that owns them. Wrangler and
// application scaffolds stay banned everywhere.
const cloudflarePackageNames: ReadonlyArray<string> = ["platform-cloudflare", "storage-cloudflare"];

const allowedCloudflareToolchainDependencies = new Set([
  "@cloudflare/vitest-pool-workers",
  "@cloudflare/workers-types",
]);

const platformCloudflareProviderDependencies = new Set(["@cloudflare/puppeteer"]);

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
  "thread",
  "storage-memory",
  "storage-sqlite",
  "workflow",
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
 * The documented inward-only package graph (AGENTS.md "Package dependency direction"):
 * every `@effect-agent/*` edge a framework
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
    "thread",
    "storage-cloudflare",
    "testing",
  ],
  "platform-node": ["capabilities", "core", "engine", "thread", "storage-sqlite", "workflow"],
  "pr-review": ["core", "engine", "effect-agent"],
  sandbox: ["core"],
  "sandbox-local": ["core", "sandbox"],
  thread: ["core", "engine"],
  "storage-cloudflare": ["core", "thread", "testing"],
  "storage-memory": ["core", "thread", "engine"],
  "storage-sqlite": ["core", "thread"],
  workflow: ["core", "thread", "engine", "storage-memory"],
  testing: [
    "capabilities",
    "core",
    "engine",
    "platform-node",
    "sandbox",
    "thread",
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

const readWorkspaceNames = (path: string) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const names: Array<string> = [];

    for (const entry of yield* fs.readDirectory(path)) {
      if (yield* fs.exists(`${path}/${entry}/package.json`)) {
        names.push(entry);
      }
    }

    return names;
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

layer(NodeServices.layer)("workspace toolchain", (it) => {
  it.effect(
    "compares built checkouts independently and counts shared and deferred chunks once",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const scratch = yield* fs.makeTempDirectoryScoped({ prefix: "bundle-comparison-test-" });

        for (const side of ["base", "head"]) {
          const root = path.join(scratch, side);
          const pkg = path.join(root, "packages", "effect-agent");

          yield* fs.makeDirectory(path.join(pkg, "dist"), { recursive: true });
          yield* fs.makeDirectory(path.join(root, "node_modules", "effect"), { recursive: true });
          yield* fs.writeFileString(path.join(root, "package.json"), '{"catalog":{}}');
          yield* fs.writeFileString(
            path.join(root, "node_modules", "effect", "package.json"),
            '{"version":"test"}',
          );
          yield* fs.writeFileString(
            path.join(pkg, "package.json"),
            JSON.stringify({
              name: "effect-agent",
              version: "1.0.0",
              type: "module",
              exports: {
                ".": "./src/index.ts",
                "./Agent": "./src/Agent.ts",
                "./AgentRuntime": "./src/AgentRuntime.ts",
              },
            }),
          );

          // No src directory: accidentally measuring source instead of published
          // artifacts must fail. The two checkouts also contain different values.
          const modules = {
            index: 'export * from "./Agent.mjs"; export * from "./AgentRuntime.mjs";',
            Agent: 'export { shared as agent } from "./shared.mjs";',
            AgentRuntime: `import { shared } from "./shared.mjs"; export const run = [shared, ${JSON.stringify(side.repeat(side === "head" ? 20000 : 10000))}];`,
            shared: `export const shared = ${JSON.stringify("shared".repeat(200))};`,
          };

          for (const [name, source] of Object.entries(modules)) {
            yield* fs.writeFileString(path.join(pkg, "dist", `${name}.mjs`), source);
            yield* fs.writeFileString(path.join(pkg, "dist", `${name}.d.mts`), "export {};");
          }
        }
        const head = path.join(scratch, "head");
        const fixtures = path.join(head, "scripts", "bundle");

        yield* fs.makeDirectory(fixtures, { recursive: true });
        for (const kind of ["root", "module"]) {
          yield* fs.writeFileString(
            path.join(fixtures, `agent-${kind}.ts`),
            `export { agent } from "effect-agent${kind === "module" ? "/Agent" : ""}";`,
          );
          yield* fs.writeFileString(
            path.join(fixtures, `runtime-${kind}.ts`),
            `export { run } from "effect-agent${kind === "module" ? "/AgentRuntime" : ""}";`,
          );
          yield* fs.writeFileString(
            path.join(fixtures, `lazy-${kind}.ts`),
            `export { agent } from "./agent-${kind}.ts"; export const loadRuntime = () => import("./runtime-${kind}.ts");`,
          );
        }
        const smoke = path.join(fixtures, "runtime-smoke.ts");

        yield* fs.writeFileString(
          smoke,
          'import { agent } from "effect-agent/Agent"; if (!agent.startsWith("shared")) throw new Error("wrong bundled value");',
        );
        // Exercise canonicalization even on hosts without macOS's /var symlink.
        const alias = path.join(scratch, "head-link");

        yield* fs.symlink(head, alias);

        const report = yield* compareBundles({
          root: alias,
          base: path.join(scratch, "base"),
          output: path.join(scratch, "report"),
        });

        const lazy = report.fixtures.find((fixture) => fixture.name === "lazy-module");

        expect(lazy).toBeDefined();
        expect(lazy!.head.initial.raw).toBeLessThan(2000);
        expect(lazy!.head.deferred.raw).toBeGreaterThan(80000);
        expect(lazy!.base!.deferred.raw).toBeLessThan(41000);
        expect(lazy!.head.chunks.filter((chunk) => chunk.initial)).toHaveLength(2);
        expect(lazy!.head.total.raw).toBe(lazy!.head.initial.raw + lazy!.head.deferred.raw);
        expect(lazy!.head.total.gzip).toBe(lazy!.head.initial.gzip + lazy!.head.deferred.gzip);
        expect(report.fixtures.every((fixture) => fixture.head.initial.raw > 0)).toBe(true);

        // A newly introduced direct entry gets no fake zero/unchanged baseline.
        const baseManifest = path.join(scratch, "base", "packages", "effect-agent", "package.json");

        yield* fs.writeFileString(
          baseManifest,
          JSON.stringify({
            name: "effect-agent",
            version: "1.0.0",
            type: "module",
            exports: { ".": "./src/index.ts" },
          }),
        );

        const second = yield* compareBundles({
          root: alias,
          base: path.join(scratch, "base"),
          output: path.join(scratch, "report"),
        });

        expect(second.fixtures.find((fixture) => fixture.name === "agent-module")?.base).toBeNull();
        expect(yield* fs.exists(path.join(scratch, "report", "base", "agent-module"))).toBe(false);
        expect(
          second.fixtures.find((fixture) => fixture.name === "agent-root")?.base,
        ).not.toBeNull();
        // A smaller but broken bundle must fail, not publish a successful report.
        yield* fs.writeFileString(smoke, 'throw new Error("broken bundled runtime");');

        const brokenRuntime = yield* Effect.flip(
          compareBundles({
            root: alias,
            base: path.join(scratch, "base"),
            output: path.join(scratch, "report"),
          }),
        );

        expect(brokenRuntime.message).toContain("broken bundled runtime");
        expect(yield* fs.exists(path.join(scratch, "report", "report.json"))).toBe(false);
        // A missing head export is a failed measurement, never a zero-size success.
        yield* fs.copyFile(
          baseManifest,
          path.join(head, "packages", "effect-agent", "package.json"),
        );

        const failure = yield* Effect.flip(
          compareBundles({
            root: alias,
            base: path.join(scratch, "base"),
            output: path.join(scratch, "report"),
          }),
        );

        expect(failure._tag).toBe("BundleSizeError");
        expect(yield* fs.exists(path.join(scratch, "report", "report.json"))).toBe(false);
      }),
  );

  it.effect("executes an Effect program through the Vite+ test runner", () =>
    Effect.sync(() => {
      expect("ready").toBe("ready");
    }),
  );

  it.effect("keeps package workspaces separate from leaf example workspaces", () =>
    Effect.gen(function* () {
      const [activePackages, activeExamples, rootEntries, rootManifest] = yield* Effect.all([
        readWorkspaceNames(`${repositoryRoot}/packages`),
        readWorkspaceNames(`${repositoryRoot}/examples`),
        readDirectory(repositoryRoot),
        readManifest(`${repositoryRoot}/package.json`),
      ]);

      expect([...activePackages].sort()).toEqual([...packageNames, ...privatePackageNames].sort());
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

  it.effect("validates package manifest fields before publishing", () =>
    Effect.gen(function* () {
      const decoded = yield* Schema.decodeUnknownEffect(PublishManifest)({
        name: "@effect-agent/fixture",
        version: "0.0.1-beta.7",
        description: "preserved package metadata",
        dependencies: { "@effect-agent/core": "workspace:*" },
      });

      expect(decoded.description).toBe("preserved package metadata");
      expect(decoded.dependencies).toEqual({ "@effect-agent/core": "workspace:*" });

      yield* Schema.decodeUnknownEffect(PublishManifest)({
        name: "@effect-agent/fixture",
        version: "0.0.1-beta.7",
        dependencies: { "@effect-agent/core": 7 },
      }).pipe(Effect.flip);
    }),
  );

  it.effect("restores the source manifest after a partial temporary-install failure", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;

      const temporaryRoot = yield* fs.makeTempDirectoryScoped({
        prefix: "effect-agent-manifest-swap-test-",
      });

      const manifestPath = path.join(temporaryRoot, "package.json");
      const originalBytes = '{"name":"fixture","version":"1.0.0"}\n';
      const publishBytes = '{"name":"fixture","version":"1.0.0","exports":{}}\n';

      yield* fs.writeFileString(manifestPath, originalBytes);

      const installCause = PlatformError.systemError({
        _tag: "WriteZero",
        module: "FileSystem",
        method: "writeFileString",
        pathOrDescriptor: manifestPath,
      });

      let writes = 0;
      let useStarted = false;

      const installFailingFileSystem = FileSystem.FileSystem.of({
        ...fs,
        writeFileString: (target, contents) => {
          if (target !== manifestPath) return fs.writeFileString(target, contents);
          writes += 1;
          if (writes === 1) {
            return fs
              .writeFileString(target, "partial\n")
              .pipe(Effect.andThen(Effect.fail(installCause)));
          }

          return fs.writeFileString(target, contents);
        },
      });

      const installFailure = yield* Effect.flip(
        withTemporaryManifest(
          manifestPath,
          originalBytes,
          publishBytes,
          Effect.sync(() => {
            useStarted = true;
          }),
        ).pipe(Effect.provideService(FileSystem.FileSystem, installFailingFileSystem)),
      );

      expect(installFailure).toMatchObject({
        _tag: "ReleaseManifestSwapError",
        operation: "install",
      });
      expect(writes).toBe(2);
      expect(useStarted).toBe(false);
      expect(yield* fs.readFileString(manifestPath)).toBe(originalBytes);

      const restoreCause = PlatformError.systemError({
        _tag: "Busy",
        module: "FileSystem",
        method: "writeFileString",
        pathOrDescriptor: manifestPath,
      });

      writes = 0;

      const installAndRestoreFailingFileSystem = FileSystem.FileSystem.of({
        ...fs,
        writeFileString: (target, contents) => {
          if (target !== manifestPath) return fs.writeFileString(target, contents);
          writes += 1;
          if (writes === 1) {
            return fs
              .writeFileString(target, "partial-again\n")
              .pipe(Effect.andThen(Effect.fail(installCause)));
          }

          return Effect.fail(restoreCause);
        },
      });

      const combinedExit = yield* withTemporaryManifest(
        manifestPath,
        originalBytes,
        publishBytes,
        Effect.void,
      ).pipe(
        Effect.provideService(FileSystem.FileSystem, installAndRestoreFailingFileSystem),
        Effect.exit,
      );

      expect(Exit.isFailure(combinedExit)).toBe(true);
      if (Exit.isFailure(combinedExit)) {
        expect(Cause.hasDies(combinedExit.cause)).toBe(false);
        const diagnostics = Cause.pretty(combinedExit.cause);

        expect(diagnostics).toContain("Could not install temporary publish manifest");
        expect(diagnostics).toContain("WriteZero: FileSystem.writeFileString");
        expect(diagnostics).toContain("Could not restore temporary publish manifest");
        expect(diagnostics).toContain("Busy: FileSystem.writeFileString");
      }
    }),
  );

  it.effect(
    "publishes Action artifacts atomically without changing source commits",
    () =>
      Effect.gen(function* () {
        const workflow = yield* readWorkflow(".github/workflows/ci.yml");
        const publisher = workflow.jobs["publish-action"];

        expect(publisher?.needs).toEqual(["checks", "test", "build"]);
        expect(publisher?.if).toBe(
          "${{ github.event_name == 'push' && github.ref == 'refs/heads/main' }}",
        );
        expect(publisher?.permissions).toEqual({ contents: "write" });

        const script = workflowStep(
          workflow,
          "publish-action",
          "Publish immutable source tag and advance action-v1",
        )?.run;

        if (script === undefined) return yield* Effect.die("Missing Action publisher");

        const fs = yield* FileSystem.FileSystem;
        const root = yield* fs.makeTempDirectoryScoped({ prefix: "action-release-test-" });
        const source = `${root}/source`;
        const remote = `${root}/remote.git`;

        yield* runFixtureCommand(root, "git", ["init", "--bare", remote]);
        yield* runFixtureCommand(root, "git", ["init", "--initial-branch=main", source]);
        const git = (...args: ReadonlyArray<string>) => runFixtureCommand(source, "git", args);

        yield* git("config", "user.name", "Action release test");
        yield* git("config", "user.email", "action-test@example.test");
        yield* git("config", "commit.gpgsign", "false");
        yield* git("config", "core.hooksPath", `${source}/.git/hooks`);
        yield* git("remote", "add", "origin", remote);
        yield* fs.writeFileString(`${source}/.gitignore`, "action/dist/\n");
        yield* git("add", ".gitignore");
        yield* git("commit", "-m", "Source without generated output");
        yield* git("push", "origin", "main");
        const firstSource = yield* git("rev-parse", "HEAD");

        yield* git("checkout", "--detach", firstSource);
        yield* fs.makeDirectory(`${source}/action/dist`, { recursive: true });
        const bundle = "console.log('exact CI artifact');\n";

        yield* fs.writeFileString(`${source}/action/dist/index.mjs`, bundle);

        const publish = (sha: string) =>
          runFixtureCommand(source, "env", [
            `GITHUB_SHA=${sha}`,
            `GITHUB_STEP_SUMMARY=${root}/summary`,
            "bash",
            "-c",
            script,
          ]);

        const remoteGit = (...args: ReadonlyArray<string>) =>
          runFixtureCommand(root, "git", ["--git-dir", remote, ...args]);

        yield* publish(firstSource);
        const firstRelease = yield* remoteGit("rev-parse", "action-v1");

        expect(yield* remoteGit("rev-parse", `action-${firstSource}`)).toBe(firstRelease);
        expect(yield* remoteGit("rev-parse", "action-v1^")).toBe(firstSource);
        expect(yield* remoteGit("show", "action-v1:action/dist/index.mjs")).toBe(bundle.trim());
        expect(yield* remoteGit("ls-tree", "-r", "--name-only", "main")).toBe(".gitignore");
        yield* publish(firstSource);
        expect(yield* remoteGit("rev-parse", "action-v1")).toBe(firstRelease);

        yield* git("checkout", "main");
        yield* git("commit", "--allow-empty", "-m", "Next validated source");
        const secondSource = yield* git("rev-parse", "HEAD");

        yield* git("push", "origin", "main");
        yield* git("checkout", "--detach", secondSource);
        yield* fs.makeDirectory(`${source}/action/dist`, { recursive: true });
        yield* fs.writeFileString(`${source}/action/dist/index.mjs`, bundle);
        yield* publish(firstSource);
        expect(yield* remoteGit("rev-parse", "action-v1")).toBe(firstRelease);

        // Reject the moving channel to prove the immutable tag cannot leak out
        // of a failed publication. Removing the hook then exercises a retry.
        const hook = `${remote}/hooks/update`;

        yield* fs.writeFileString(hook, '#!/bin/sh\n[ "$1" != "refs/tags/action-v1" ]\n');
        yield* fs.chmod(hook, 0o755);
        expect(Exit.isFailure(yield* Effect.exit(publish(secondSource)))).toBe(true);
        expect(yield* remoteGit("rev-parse", "action-v1")).toBe(firstRelease);
        expect(yield* remoteGit("tag", "--list", `action-${secondSource}`)).toBe("");
        yield* fs.remove(hook);
        yield* git("checkout", "--detach", secondSource);
        yield* fs.makeDirectory(`${source}/action/dist`, { recursive: true });
        yield* fs.writeFileString(`${source}/action/dist/index.mjs`, bundle);
        yield* publish(secondSource);
        expect(yield* remoteGit("rev-parse", "action-v1^")).toBe(secondSource);
        expect(yield* remoteGit("rev-parse", `action-${firstSource}`)).toBe(firstRelease);
        expect(yield* remoteGit("ls-tree", "-r", "--name-only", "main")).toBe(".gitignore");
      }),
    { timeout: 30_000 },
  );

  it.effect("runs ordinary CI on release PRs and uses App-authored Changesets updates", () =>
    Effect.gen(function* () {
      const ci = yield* readWorkflow(".github/workflows/ci.yml");
      const release = yield* readWorkflow(".github/workflows/release.yml");

      expect(ci.on).toHaveProperty("pull_request");
      expect(ci.on.pull_request).toBeNull();
      const checkout = workflowStep(release, "release", "Check out repository");

      expect(checkout?.with?.["persist-credentials"]).toBe(false);
      const token = workflowStep(release, "release", "Mint the release token");

      expect(token?.with?.["permission-contents"]).toBe("write");
      expect(token?.with?.["permission-pull-requests"]).toBe("write");
      const changesets = workflowStep(release, "release", "Create release pull request or publish");

      expect(changesets?.env?.GITHUB_TOKEN).toBe("${{ steps.app-token.outputs.token }}");
      expect(changesets?.with?.publish).toBe("./node_modules/.bin/vp run release:publish");
      expect(release.jobs.release?.permissions?.["id-token"]).toBe("write");
    }),
  );

  it.effect("publishes without flags and requires an explicit dry-run opt-in", () =>
    Effect.gen(function* () {
      const modes: Array<boolean> = [];

      const run = Command.runWith(
        Command.withHandler(releaseCommand, ({ dryRun }) =>
          Effect.sync(() => {
            modes.push(dryRun);
          }),
        ),
        { version: "1.0.0", renderErrors: false },
      );

      yield* run([]);
      yield* run(["--dry-run"]);
      expect(modes).toEqual([false, true]);
    }),
  );

  it.effect(
    "validates and packs public groups and forwarding modules and restores source manifests",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const root = yield* fs.makeTempDirectoryScoped({ prefix: "release-manifest-test-" });

        yield* fs.writeFileString(
          root + "/package.json",
          JSON.stringify({
            name: "release-fixture",
            private: true,
            workspaces: ["packages/*"],
            packageManager: "bun@1.4.0",
            catalog: { effect: "4.0.0-rc.111" },
          }),
        );
        const originals = new Map<string, string>();

        yield* fs.makeDirectory(root + "/.changeset");

        const pre = JSON.stringify({
          mode: "pre",
          tag: "beta",
          initialVersions: {},
          changesets: [],
        });

        yield* fs.writeFileString(root + "/.changeset/pre.json", pre);
        originals.set(root + "/.changeset/pre.json", pre);
        yield* fs.writeFileString(
          root + "/.changeset/config.json",
          JSON.stringify({
            changelog: false,
            commit: false,
            fixed: [],
            linked: [],
            access: "public",
            baseBranch: "main",
            updateInternalDependencies: "patch",
            ignore: [],
          }),
        );

        for (const name of ["core", "consumer"]) {
          const directory = root + "/packages/" + name;

          const sources = {
            index: 'export * as Agent from "./Agent.ts"; export { make } from "./Agent.ts";\n',
            Agent:
              name === "core"
                ? "export const make = () => 1;\n"
                : 'export * from "@fixture/core/Agent";\n',
            Support: 'export * as Failpoint from "./Failpoint.ts";\n',
            Failpoint: "export const armed = false;\n",
          };

          yield* fs.makeDirectory(directory + "/dist", { recursive: true });
          yield* fs.makeDirectory(directory + "/src");
          for (const [entry, source] of Object.entries(sources)) {
            yield* fs.writeFileString(`${directory}/src/${entry}.ts`, source);
            yield* fs.writeFileString(`${directory}/dist/${entry}.mjs`, "export {};\n");
            yield* fs.writeFileString(`${directory}/dist/${entry}.d.mts`, "export {};\n");
          }
          yield* fs.writeFileString(
            directory + "/vite.config.ts",
            "export default " +
              JSON.stringify({
                pack: { entry: Object.keys(sources).map((entry) => `src/${entry}.ts`) },
              }),
          );

          const original =
            JSON.stringify({
              name: "@fixture/" + name,
              version: "1.0.0-beta.17",
              exports: {
                ".": "./src/index.ts",
                "./Agent": "./src/Agent.ts",
                "./testing": "./src/Support.ts",
                "./testing/Failpoint": "./src/Failpoint.ts",
              },
              files: ["dist"],
              dependencies: name === "consumer" ? { "@fixture/core": "workspace:*" } : {},
              peerDependencies: { effect: "catalog:" },
              devDependencies: { effect: "catalog:" },
            }) + "\n";

          originals.set(directory + "/package.json", original);
          yield* fs.writeFileString(directory + "/package.json", original);
        }

        yield* verifyPackageExports(root);
        yield* verifyPackagePurity(root);

        const forwardedModule = root + "/packages/consumer/src/Agent.ts";
        const originalForwarder = yield* fs.readFileString(forwardedModule);

        yield* fs.writeFileString(
          forwardedModule,
          'export * from "@fixture/core/internal/Secret";\n',
        );
        const privateForwarding = yield* Effect.flip(verifyPackageExports(root));

        expect(privateForwarding.message).toContain("is not a public export of @fixture/core");
        yield* fs.writeFileString(forwardedModule, originalForwarder);

        // The testing export key defines this boundary even though Support.ts has no test-like name.
        const productionRoot = root + "/packages/core/src/index.ts";
        const originalRoot = yield* fs.readFileString(productionRoot);

        yield* fs.writeFileString(
          productionRoot,
          originalRoot + 'export * as Support from "./Support.ts";\n',
        );
        const testingLeak = yield* Effect.flip(verifyPackagePurity(root));

        expect(testingLeak.message).toContain("bundles a declared testing entry point");
        yield* fs.writeFileString(productionRoot, originalRoot);

        const assertRestored = Effect.gen(function* () {
          for (const [path, original] of originals) {
            expect(yield* fs.readFileString(path)).toBe(original);
          }
        });

        yield* withPublishManifests(root, () =>
          Effect.gen(function* () {
            const directory = root + "/packages/consumer";

            yield* runFixtureCommand(directory, "npm", [
              "pack",
              "--ignore-scripts",
              "--pack-destination",
              root,
              "--cache",
              root + "/.npm",
            ]);

            const packed = yield* runFixtureCommand(root, "tar", [
              "-xOf",
              "fixture-consumer-1.0.0-beta.17.tgz",
              "package/package.json",
            ]);

            expect(
              yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Unknown))(packed),
            ).toMatchObject({
              version: "1.0.0-beta.17",
              dependencies: { "@fixture/core": "1.0.0-beta.17" },
              peerDependencies: { effect: "4.0.0-rc.111" },
              devDependencies: { effect: "4.0.0-rc.111" },
              exports: {
                ".": { types: "./dist/index.d.mts", default: "./dist/index.mjs" },
                "./Agent": { types: "./dist/Agent.d.mts", default: "./dist/Agent.mjs" },
                "./testing": { types: "./dist/Support.d.mts", default: "./dist/Support.mjs" },
                "./testing/Failpoint": {
                  types: "./dist/Failpoint.d.mts",
                  default: "./dist/Failpoint.mjs",
                },
              },
            });
          }),
        );
        yield* assertRestored;

        // Exercise Changesets itself without registry writes. This catches its
        // pre-mode --tag rejection and its latest default for beta-only packages.
        const bin = root + "/bin";

        yield* fs.makeDirectory(bin);
        yield* fs.writeFileString(
          bin + "/npm",
          `#!/bin/sh
case "$1" in
  info) printf '%s\\n' '{"versions":["1.0.0-beta.1"],"dist-tags":{"latest":"1.0.0-beta.1"}}' ;;
  publish) printf '%s\\n' "$*" >> "$PUBLISH_CAPTURE" ;;
  *) echo "Unexpected npm command: $*" >&2; exit 1 ;;
esac
`,
        );
        yield* fs.chmod(bin + "/npm", 0o755);
        yield* runFixtureCommand(root, "git", ["init", "--initial-branch=main"]);
        yield* runFixtureCommand(root, "git", ["config", "user.name", "Release test"]);
        yield* runFixtureCommand(root, "git", ["config", "user.email", "release@example.invalid"]);
        yield* runFixtureCommand(root, "git", ["config", "commit.gpgsign", "false"]);
        yield* runFixtureCommand(root, "git", ["config", "tag.gpgsign", "false"]);
        yield* runFixtureCommand(root, "git", ["add", "package.json", "packages", ".changeset"]);
        yield* runFixtureCommand(root, "git", ["commit", "-m", "Version packages"]);
        const executablePath = yield* Config.string("PATH");

        yield* withPublishManifests(root, () =>
          runFixtureCommand(root, "env", [
            "PATH=" + bin + ":" + executablePath,
            "PUBLISH_CAPTURE=" + root + "/publishes",
            repositoryRoot + "/node_modules/.bin/changeset",
            "publish",
            "--tag",
            "beta",
          ]),
        );
        const publishes = (yield* fs.readFileString(root + "/publishes")).trim().split("\n");

        expect(publishes).toHaveLength(2);
        for (const invocation of publishes) {
          expect(invocation).toContain("--tag beta");
          expect(invocation).not.toContain("--tag latest");
        }
        expect((yield* runFixtureCommand(root, "git", ["tag", "--list"])).split("\n")).toEqual([
          "@fixture/consumer@1.0.0-beta.17",
          "@fixture/core@1.0.0-beta.17",
        ]);
        yield* assertRestored;

        for (const failure of [
          Effect.fail("publish failed"),
          Effect.die("publisher defect"),
          Effect.interrupt,
        ]) {
          const exit = yield* Effect.exit(withPublishManifests(root, () => failure));

          expect(Exit.isFailure(exit)).toBe(true);
          yield* assertRestored;
        }

        yield* fs.remove(root + "/packages/core/dist/Failpoint.mjs");
        let published = false;

        const failure = yield* Effect.flip(
          withPublishManifests(root, () =>
            Effect.sync(() => {
              published = true;
            }),
          ),
        );

        expect(failure).toMatchObject({
          _tag: "ReleaseError",
          reason: "Missing built artifact: ./dist/Failpoint.mjs",
        });
        expect(published).toBe(false);
        yield* assertRestored;
      }),
    15_000,
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

        const cloudflareAllowance = new Set([
          ...(cloudflarePackageNames.includes(packageName)
            ? allowedCloudflareToolchainDependencies
            : []),
          ...(packageName === "platform-cloudflare" ? platformCloudflareProviderDependencies : []),
        ]);

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
      // The P6 gate at the manifest layer: core/engine/capabilities/thread and
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
            if (packageName === "storage-memory") {
              expect(edges, "storage-memory uses engine only in its tests").not.toContain("engine");
            }
          }
        }
      }
    }),
  );

  it.effect("PRR-005 confines provider adapters to leaf applications", () =>
    Effect.gen(function* () {
      const root = yield* readManifest(`${repositoryRoot}/package.json`);
      const demo = yield* readManifest(`${repositoryRoot}/examples/demo/package.json`);
      const providers = yield* readManifest(`${repositoryRoot}/examples/providers/package.json`);
      const repoOps = yield* readManifest(`${repositoryRoot}/examples/repo-ops/package.json`);

      const prReviewAction = yield* readManifest(
        `${repositoryRoot}/packages/pr-review-action/package.json`,
      );

      const demoDependencies = manifestDependencies(demo);
      const providerDependencies = manifestDependencies(providers);
      const repoOpsDependencies = manifestDependencies(repoOps);
      const prReviewDependencies = manifestDependencies(prReviewAction);

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
      // The GitHub channel owns the concrete provider and platform edges;
      // the reusable reviewer package stays provider-neutral (PRR-005).
      expect(prReviewAction.name).toBe("@effect-agent/pr-review-action");
      expect(prReviewAction.private).toBe(true);
      expect(prReviewAction.dependencies?.["@effect-agent/pr-review"]).toBe("workspace:*");
      expect(prReviewAction.dependencies?.["@effect/ai-openai"]).toBe("catalog:");
      expect(prReviewAction.dependencies?.["@effect/platform-node"]).toBe("catalog:");
      expect(prReviewAction.devDependencies?.["@effect-agent/engine"]).toBe("workspace:*");
      expect(prReviewAction.dependencies?.["@effect-agent/engine"]).toBeUndefined();
      expect(prReviewAction.dependencies?.["effect-agent"]).toBeUndefined();
      expect(prReviewAction.dependencies?.effect).toBe("catalog:");
      expect(prReviewDependencies).not.toContain("wrangler");
      expect(prReviewDependencies.some((dependency) => dependency.startsWith("@cloudflare/"))).toBe(
        false,
      );
      const prReviewPublicIndex = yield* readRepositoryFile("packages/pr-review/src/index.ts");

      expect(prReviewPublicIndex).not.toMatch(/work-?order|remediation|handoff|implementer/i);

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

      // The packaged reviewer is transport- and provider-neutral (PRR-005).
      const prReviewPackage = yield* readManifest(
        `${repositoryRoot}/packages/pr-review/package.json`,
      );

      for (const adapter of providerAdapterDependencies) {
        expect(prReviewPackage.dependencies?.[adapter]).toBeUndefined();
      }

      for (const packageName of packageNames) {
        const manifest = yield* readManifest(
          `${repositoryRoot}/packages/${packageName}/package.json`,
        );

        expect(manifestDependencies(manifest)).not.toContain(demo.name);
        expect(manifestDependencies(manifest)).not.toContain(providers.name);
        expect(manifestDependencies(manifest)).not.toContain(repoOps.name);
        expect(manifestDependencies(manifest)).not.toContain(prReviewAction.name);
        expect(manifestDependencies(manifest)).not.toContain(codeModeCloudflare.name);
        if (providerConsumingPackages.has(packageName)) continue;
        for (const adapter of providerAdapterDependencies) {
          expect(manifestDependencies(manifest)).not.toContain(adapter);
        }
      }
    }),
  );

  it.effect(
    "declares Effect peers and pins development dependencies through the root catalog",
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

          expect(manifest.peerDependencies?.effect).toBe("^4.0.0-rc.112");
          expect(manifest.devDependencies?.effect).toBe("catalog:");
          expect(manifest.dependencies?.effect).toBeUndefined();
          expect(manifest.optionalDependencies?.effect).toBeUndefined();
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
});

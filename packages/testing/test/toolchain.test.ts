import { fileURLToPath } from "node:url";

import { NodeServices } from "@effect/platform-node";
import { expect, layer } from "@effect/vitest";
import {
  Cause,
  ConfigProvider,
  Effect,
  Exit,
  FileSystem,
  Option,
  Path,
  PlatformError,
  Schema,
  Stream,
} from "effect";
import { Yaml } from "effect/unstable/encoding";
import { HttpClient, HttpClientResponse } from "effect/unstable/http";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

import { publishRelease, withTemporaryManifest } from "../../../scripts/release-publish.ts";

const WorkflowStep = Schema.Struct({
  "continue-on-error": Schema.optionalKey(Schema.Boolean),
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
  env: Schema.optionalKey(Schema.Record(Schema.String, Schema.String)),
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

layer(NodeServices.layer)("workspace toolchain", (it) => {
  it.effect("requires a successful paid gate before publication", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const root = yield* fs.makeTempDirectoryScoped({ prefix: "release-gates-test-" });
      const manifest = { name: "release-fixture", version: "1.0.0" };

      yield* fs.makeDirectory(`${root}/packages/fixture`, { recursive: true });
      yield* fs.writeFileString(`${root}/package.json`, JSON.stringify({ catalog: {} }));
      yield* fs.writeFileString(`${root}/packages/fixture/package.json`, JSON.stringify(manifest));

      for (const scenario of [
        {
          published: false,
          continuity: true,
          dryRun: false,
          failure: "context-continuity-eval",
          expected: [],
        },
        {
          published: false,
          continuity: true,
          dryRun: false,
          failure: "",
          expected: ["publish"],
        },
      ]) {
        const observed: Array<string> = [];

        const commands = ChildProcessSpawner.make((command) => {
          if (command._tag !== "StandardCommand") return Effect.die("Unexpected pipeline");

          const operation = command.args.find((arg) =>
            ["build", "context-continuity-eval", "prove:live", "publish", "pack"].includes(arg),
          );

          if (operation === undefined) return Effect.die("Unexpected release command");
          if (operation === "publish") observed.push(operation);

          // Exercise the real process exit boundary without building, deploying, or publishing.
          return spawner.spawn(
            ChildProcess.make("node", [
              "-e",
              `process.exit(${operation === scenario.failure ? 1 : 0})`,
            ]),
          );
        });

        const registry = HttpClient.make((request) =>
          Effect.succeed(
            HttpClientResponse.fromWeb(
              request,
              Response.json(manifest, { status: scenario.published ? 200 : 404 }),
            ),
          ),
        );

        const exit = yield* publishRelease(root, {
          dryRun: scenario.dryRun,
          checkContinuity: scenario.continuity,
          checkCheckout: true,
          otp: Option.none(),
        }).pipe(
          Effect.provide(ConfigProvider.layer(ConfigProvider.fromUnknown({}))),
          Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, commands),
          Effect.provideService(HttpClient.HttpClient, registry),
          Effect.exit,
        );

        expect(observed).toEqual(scenario.expected);
        expect(Exit.isFailure(exit)).toBe(scenario.failure !== "");
        expect(yield* fs.readFileString(`${root}/packages/fixture/package.json`)).toBe(
          JSON.stringify(manifest),
        );
      }
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
});

import { fileURLToPath } from "node:url";

import { NodeServices } from "@effect/platform-node";
import { expect, layer } from "@effect/vitest";
import { Cause, Effect, Exit, FileSystem, Path, PlatformError, Schema, Stream } from "effect";
import { Yaml } from "effect/unstable/encoding";
import { ChildProcess } from "effect/unstable/process";

import { prepareReleaseArtifactDirectory } from "../../../scripts/release-artifact-directory.ts";
import { PreparedReleaseManifest, PublishManifest } from "../../../scripts/release-publish.ts";
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
  uses: Schema.optionalKey(Schema.String),
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

const runReleaseCheckReporter = Effect.fn("toolchainTest.runReleaseCheckReporter")(function* (
  script: string,
  options: {
    readonly baseRef: string | undefined;
    readonly currentBaseSha: string;
    readonly currentHeadSha: string;
    readonly expectedBaseSha: string;
    readonly headRef: string | undefined;
    readonly headRepository: string | undefined;
    readonly strictReady: string | undefined;
    readonly verifiedHeadSha: string;
    readonly verifyOutcome: string;
  },
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const temporaryRoot = yield* fs.makeTempDirectoryScoped({
    prefix: "effect-agent-release-check-test-",
  });
  const binDirectory = path.join(temporaryRoot, "bin");
  const capturePath = path.join(temporaryRoot, "check-run-arguments");
  const ghPath = path.join(binDirectory, "gh");
  yield* fs.makeDirectory(binDirectory, { recursive: true });
  yield* fs.writeFileString(
    ghPath,
    `#!/usr/bin/env bash
set -euo pipefail

test "\${1:-}" = "api"
shift
if [ "\${1:-}" = "--method" ]; then
  test "\${2:-}" = "POST"
  test "\${3:-}" = "repos/$GITHUB_REPOSITORY/check-runs"
  printf '%s\\n' "$@" > "$GH_STUB_CAPTURE"
  exit 0
fi

ENDPOINT="\${1:-}"

QUERY=""
while [ "$#" -gt 0 ]; do
  if [ "$1" = "--jq" ]; then
    shift
    QUERY="\${1:-}"
    break
  fi
  shift
done

case "$QUERY" in
  ".head.sha")
    test "$ENDPOINT" = "repos/$GITHUB_REPOSITORY/pulls/$PR_NUMBER"
    printf '%s\\n' "$GH_STUB_HEAD_SHA"
    ;;
  ".head.repo.full_name")
    test "$ENDPOINT" = "repos/$GITHUB_REPOSITORY/pulls/$PR_NUMBER"
    printf '%s\\n' "$GH_STUB_HEAD_REPOSITORY"
    ;;
  ".head.ref")
    test "$ENDPOINT" = "repos/$GITHUB_REPOSITORY/pulls/$PR_NUMBER"
    printf '%s\\n' "$GH_STUB_HEAD_REF"
    ;;
  ".base.sha")
    test "$ENDPOINT" = "repos/$GITHUB_REPOSITORY/pulls/$PR_NUMBER"
    printf '%s\\n' "$GH_STUB_BASE_SHA"
    ;;
  ".base.ref")
    test "$ENDPOINT" = "repos/$GITHUB_REPOSITORY/pulls/$PR_NUMBER"
    printf '%s\\n' "$GH_STUB_BASE_REF"
    ;;
  *"strict_required_status_checks_policy"*)
    test "$ENDPOINT" = "repos/$GITHUB_REPOSITORY/rules/branches/main"
    printf '%s\\n' "$GH_STUB_STRICT_READY"
    ;;
  *) echo "Unexpected gh query: $QUERY" >&2; exit 64 ;;
esac
`,
  );
  yield* fs.chmod(ghPath, 0o755);

  const child = yield* ChildProcess.make("bash", ["-euo", "pipefail", "-c", script], {
    cwd: temporaryRoot,
    env: {
      EXPECTED_BASE_SHA: options.expectedBaseSha,
      GH_STUB_BASE_REF: options.baseRef ?? "main",
      GH_STUB_BASE_SHA: options.currentBaseSha,
      GH_STUB_CAPTURE: capturePath,
      GH_STUB_HEAD_REF: options.headRef ?? "changeset-release/main",
      GH_STUB_HEAD_REPOSITORY: options.headRepository ?? "effect-agent/release-check-fixture",
      GH_STUB_HEAD_SHA: options.currentHeadSha,
      GH_STUB_STRICT_READY: options.strictReady ?? "true",
      GH_TOKEN: "test-token",
      GITHUB_REPOSITORY: "effect-agent/release-check-fixture",
      GITHUB_RUN_ATTEMPT: "1",
      GITHUB_RUN_ID: "1234",
      GITHUB_SERVER_URL: "https://github.example.invalid",
      PATH: `${binDirectory}:${globalThis.process.env["PATH"] ?? ""}`,
      PR_NUMBER: "22",
      VERIFIED_HEAD_SHA: options.verifiedHeadSha,
      VERIFY_OUTCOME: options.verifyOutcome,
    },
    extendEnv: true,
    stderr: "pipe",
    stdout: "pipe",
  });
  const [output, exitCode] = yield* Effect.all([
    Stream.mkString(Stream.decodeText(child.all)),
    child.exitCode,
  ]);
  const invocation = (yield* fs.exists(capturePath)) ? yield* fs.readFileString(capturePath) : "";
  return { exitCode, invocation, output } as const;
});

const runReleasePublisher = Effect.fn("toolchainTest.runReleasePublisher")(function* (
  script: string,
  options: {
    readonly checksumFailure?: "artifact" | "npm";
    readonly packageMetadata: string;
    readonly registryStatus: "200" | "404";
    readonly tarball?: string;
    readonly versionMetadata: string;
  },
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const temporaryRoot = yield* fs.makeTempDirectoryScoped({
    prefix: "effect-agent-release-publisher-test-",
  });
  const artifactDirectory = path.join(temporaryRoot, "artifact-source");
  const binDirectory = path.join(temporaryRoot, "bin");
  const expectedSumsPath = path.join(temporaryRoot, "expected-SHA256SUMS");
  const publishCapture = path.join(temporaryRoot, "npm-publish-invoked");
  const trustedManifestDirectory = path.join(temporaryRoot, "trusted-manifests");
  const version = "0.0.1-beta.7";
  yield* fs.makeDirectory(path.join(artifactDirectory, "packages"), { recursive: true });
  yield* fs.makeDirectory(binDirectory, { recursive: true });
  yield* fs.makeDirectory(trustedManifestDirectory, { recursive: true });

  const releases: Array<{
    readonly distTag: "beta";
    readonly name: string;
    readonly tarball: string | null;
    readonly version: string;
  }> = [];
  for (const directory of packageNames) {
    const manifest = yield* readManifest(`${repositoryRoot}/packages/${directory}/package.json`);
    if (manifest.name === undefined) {
      return yield* Effect.die(new Error(`Release package ${directory} must have a name.`));
    }
    yield* fs.writeFileString(
      path.join(trustedManifestDirectory, `${directory}.json`),
      `${JSON.stringify({ name: manifest.name, version })}\n`,
    );
    releases.push({
      distTag: "beta",
      name: manifest.name,
      tarball:
        directory === "capabilities" ? (options.tarball ?? "packages/capabilities.tgz") : null,
      version,
    });
  }
  yield* fs.writeFileString(
    path.join(artifactDirectory, "release-manifest.json"),
    `${JSON.stringify({ version: 1, packages: releases })}\n`,
  );
  const expectedSums = [
    "fixture-sha  npm-12.0.2.tgz",
    "fixture-sha  packages/capabilities.tgz",
    "fixture-sha  release-manifest.json",
    "",
  ].join("\n");
  yield* fs.writeFileString(path.join(artifactDirectory, "SHA256SUMS"), expectedSums);
  yield* fs.writeFileString(expectedSumsPath, expectedSums);
  yield* fs.writeFileString(path.join(artifactDirectory, "npm-12.0.2.tgz"), "fixture\n");
  yield* fs.writeFileString(
    path.join(artifactDirectory, "packages", "capabilities.tgz"),
    "fixture\n",
  );

  const writeExecutable = (name: string, contents: string) =>
    Effect.gen(function* () {
      const file = path.join(binDirectory, name);
      yield* fs.writeFileString(file, contents);
      yield* fs.chmod(file, 0o755);
    });
  yield* Effect.all([
    writeExecutable(
      "gh",
      `#!/usr/bin/env bash
set -euo pipefail
REQUEST="$*"
if [[ "$REQUEST" == "api repos/$GITHUB_REPOSITORY/actions/runs/$GITHUB_RUN_ID/artifacts --jq "* ]]; then
  printf '731\n'
elif [ "$REQUEST" = "api repos/$GITHUB_REPOSITORY/actions/artifacts/731/zip" ]; then
  printf 'fixture archive\n'
elif [[ "$REQUEST" =~ /contents/packages/([^/]+)/package.json ]]; then
  EXPECTED_CONTENT="repos/$GITHUB_REPOSITORY/contents/packages/\${BASH_REMATCH[1]}/package.json?ref=$GITHUB_SHA"
  [[ " $REQUEST " == *" $EXPECTED_CONTENT"* ]]
  cat "$PUBLISH_TRUSTED_MANIFESTS/\${BASH_REMATCH[1]}.json"
else
  echo "Unexpected gh invocation: $REQUEST" >&2
  exit 64
fi
`,
    ),
    writeExecutable(
      "unzip",
      `#!/usr/bin/env bash
set -euo pipefail
DESTINATION=""
while [ "$#" -gt 0 ]; do
  if [ "$1" = "-d" ]; then
    shift
    DESTINATION="\${1:-}"
  fi
  shift
done
test -n "$DESTINATION"
mkdir -p "$DESTINATION"
cp -R "$PUBLISH_ARTIFACT_SOURCE"/. "$DESTINATION"/
`,
    ),
    writeExecutable(
      "sha256sum",
      `#!/usr/bin/env bash
set -euo pipefail
if [ "$#" = "3" ] && [ "$1" = "--check" ] && [ "$2" = "--strict" ] && [ "$3" = "SHA256SUMS" ]; then
  cmp SHA256SUMS "$PUBLISH_EXPECTED_SUMS"
  test "$PUBLISH_CHECKSUM_FAILURE" != "artifact"
  exit 0
fi
if [ "$#" = "2" ] && [ "$1" = "--check" ] && [ "$2" = "--strict" ]; then
  IFS= read -r EXPECTED_LINE
  test "$EXPECTED_LINE" = "$NPM_CLI_SHA256  npm-$NPM_CLI_VERSION.tgz"
  test "$PUBLISH_CHECKSUM_FAILURE" != "npm"
  exit 0
fi
echo "Unexpected sha256sum invocation: $*" >&2
exit 64
`,
    ),
    writeExecutable(
      "node",
      `#!/usr/bin/env bash
set -euo pipefail
if [ "\${1:-}" = "--eval" ]; then
  exit 0
fi
if [ "\${2:-}" = "publish" ]; then
  printf '%s\n' "$@" > "$PUBLISH_CAPTURE"
  exit 0
fi
echo "Unexpected node invocation: $*" >&2
exit 64
`,
    ),
    writeExecutable(
      "tar",
      `#!/usr/bin/env bash
set -euo pipefail
if [ "\${1:-}" = "-xzf" ]; then
  while [ "$#" -gt 0 ]; do
    if [ "$1" = "-C" ]; then
      shift
      mkdir -p "$1/package/bin"
      printf 'fixture\n' > "$1/package/bin/npm-cli.js"
      exit 0
    fi
    shift
  done
fi
if [ "\${1:-}" = "-xOf" ]; then
  printf '{"name":"@effect-agent/capabilities","version":"0.0.1-beta.7"}\n'
  exit 0
fi
exit 64
`,
    ),
    writeExecutable(
      "stat",
      `#!/usr/bin/env bash
printf '128\n'
`,
    ),
    writeExecutable(
      "curl",
      `#!/usr/bin/env bash
set -euo pipefail
URL="\${!#}"
if [[ "$*" == *"--write-out"* ]]; then
  printf '%s' "$PUBLISH_REGISTRY_STATUS"
elif [[ "$URL" == */0.0.1-beta.7 ]]; then
  printf '%s' "$PUBLISH_VERSION_METADATA"
else
  printf '%s' "$PUBLISH_PACKAGE_METADATA"
fi
`,
    ),
    writeExecutable(
      "openssl",
      `#!/usr/bin/env bash
printf 'fixture bytes'
`,
    ),
    writeExecutable(
      "base64",
      `#!/usr/bin/env bash
cat > /dev/null
printf 'fixture-integrity'
`,
    ),
  ]);

  const child = yield* ChildProcess.make("bash", ["-euo", "pipefail", "-c", script], {
    cwd: temporaryRoot,
    env: {
      GH_TOKEN: "test-token",
      GITHUB_REPOSITORY: "effect-agent/release-publisher-fixture",
      GITHUB_RUN_ID: "1234",
      GITHUB_SHA: "1111111111111111111111111111111111111111",
      NPM_CLI_SHA256: "fixture-checksum",
      NPM_CLI_VERSION: "12.0.2",
      PATH: `${binDirectory}:${globalThis.process.env["PATH"] ?? ""}`,
      PUBLISH_ARTIFACT_SOURCE: artifactDirectory,
      PUBLISH_CAPTURE: publishCapture,
      PUBLISH_CHECKSUM_FAILURE: options.checksumFailure ?? "none",
      PUBLISH_EXPECTED_SUMS: expectedSumsPath,
      PUBLISH_PACKAGE_METADATA: options.packageMetadata,
      PUBLISH_REGISTRY_STATUS: options.registryStatus,
      PUBLISH_TRUSTED_MANIFESTS: trustedManifestDirectory,
      PUBLISH_VERSION_METADATA: options.versionMetadata,
      RUNNER_TEMP: path.join(temporaryRoot, "runner"),
    },
    extendEnv: true,
    stderr: "pipe",
    stdout: "pipe",
  });
  const [output, exitCode] = yield* Effect.all([
    Stream.mkString(Stream.decodeText(child.all)),
    child.exitCode,
  ]);
  const publishInvocation = (yield* fs.exists(publishCapture))
    ? (yield* fs.readFileString(publishCapture)).trim().split("\n")
    : [];
  return {
    exitCode,
    output,
    publishInvocation,
    publishInvoked: publishInvocation.length > 0,
  } as const;
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

  it.effect(
    "validates mutable package fields and transferred tarball paths at Schema boundaries",
    () =>
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
        yield* Schema.decodeUnknownEffect(PreparedReleaseManifest)({
          version: 1,
          packages: [
            {
              name: "@effect-agent/fixture",
              version: "0.0.1-beta.7",
              distTag: "beta",
              tarball: "../outside.tgz",
            },
          ],
        }).pipe(Effect.flip);
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
      const versionJob = releaseWorkflow.jobs["version-release"];
      expect(versionJob?.permissions).toEqual({
        contents: "write",
        "pull-requests": "write",
      });
      expect(versionJob?.permissions?.checks).toBeUndefined();
      expect(versionJob?.permissions?.["id-token"]).toBeUndefined();
      const externalActionReferences = Object.values(releaseWorkflow.jobs).flatMap((job) => [
        ...(job.uses === undefined ? [] : [job.uses]),
        ...(job.steps ?? []).flatMap((step) => (step.uses === undefined ? [] : [step.uses])),
      ]);
      expect(externalActionReferences).not.toHaveLength(0);
      expect(externalActionReferences.every((uses) => /^[^@]+@[0-9a-f]{40}$/.test(uses))).toBe(
        true,
      );
      for (const job of Object.values(releaseWorkflow.jobs)) {
        for (const step of job.steps ?? []) {
          if (step.run === undefined) continue;
          yield* runFixtureCommand(repositoryRoot, "bash", [
            "-n",
            "-c",
            step.run.replace(/\$\{\{[^}]+\}\}/g, "github-expression"),
          ]);
        }
      }
      const changesetsStep = workflowStep(
        releaseWorkflow,
        "version-release",
        "Create or update the version PR",
      );
      expect(changesetsStep?.id).toBe("changesets");
      expect(changesetsStep?.uses).toBe(
        "changesets/action@a45c4d594aa4e2c509dc14a9f2b3b67ba3780d0d",
      );
      expect(changesetsStep?.with).toMatchObject({
        publish: "true",
        version: "./node_modules/.bin/vp run ci:version",
      });

      const verifyJob = releaseWorkflow.jobs["verify-release"];
      expect(verifyJob?.needs).toBe("version-release");
      expect(verifyJob?.permissions).toEqual({
        contents: "read",
        "pull-requests": "read",
      });
      const trustedCheckout = workflowStep(
        releaseWorkflow,
        "verify-release",
        "Check out trusted base",
      );
      expect(trustedCheckout?.with).toMatchObject({ ref: "${{ github.sha }}" });

      const resolveStep = workflowStep(
        releaseWorkflow,
        "verify-release",
        "Resolve the generated release head",
      );
      expect(resolveStep?.run).toContain('test "$HEAD_REPOSITORY" = "$GITHUB_REPOSITORY"');
      expect(resolveStep?.run).toContain('test "$HEAD_REF" = "changeset-release/main"');
      expect(resolveStep?.run).toContain('test "$BASE_SHA" = "$GITHUB_SHA"');

      const verifyStep = workflowStep(
        releaseWorkflow,
        "verify-release",
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
      expect(reportJob?.needs).toEqual(["version-release", "verify-release"]);
      expect(reportJob?.if).toBe(
        "${{ always() && needs.version-release.outputs.release-pr-number != '' }}",
      );
      expect(reportJob?.permissions).toEqual({
        checks: "write",
        contents: "read",
        "pull-requests": "read",
      });
      expect(reportJob?.steps?.every((step) => step.uses === undefined)).toBe(true);
      expect(reportStep?.run).toBeDefined();

      const prepareJob = releaseWorkflow.jobs["prepare-publish"];
      expect(prepareJob?.needs).toBe("version-release");
      expect(prepareJob?.if).toBe("${{ needs.version-release.outputs.has-changesets == 'false' }}");
      expect(prepareJob?.permissions).toEqual({ contents: "read" });
      const prepareStep = workflowStep(
        releaseWorkflow,
        "prepare-publish",
        "Prepare package and npm CLI artifacts",
      );
      expect(prepareStep?.run).toContain("vp run release:prepare");
      expect(prepareStep?.run).toContain("sha256sum --check --strict");

      const publishJob = releaseWorkflow.jobs["publish-packages"];
      expect(publishJob?.permissions).toEqual({
        actions: "read",
        contents: "read",
        "id-token": "write",
      });
      expect(publishJob?.steps).toHaveLength(1);
      expect(publishJob?.steps?.every((step) => step.uses === undefined)).toBe(true);
      const publishStep = workflowStep(
        releaseWorkflow,
        "publish-packages",
        "Verify and publish prepared tarballs",
      );
      expect(publishStep?.run).toContain("sha256sum --check --strict SHA256SUMS");
      expect(publishStep?.run).toContain("([.packages[].name] | unique | length) == 14");
      expect(publishStep?.run).toContain("([.packages[].version] | unique) == [$expectedVersion]");
      expect(publishStep?.run).toContain(
        "repos/${GITHUB_REPOSITORY}/contents/packages/${PACKAGE_DIRECTORY}/package.json?ref=${GITHUB_SHA}",
      );
      expect(publishStep?.run).toContain('.distTag == "beta"');
      expect(publishStep?.run).toContain('tar -xOf "$TARBALL" package/package.json');
      expect(publishStep?.run).toContain('REMOTE_INTEGRITY="$(jq');
      expect(publishStep?.run).toContain('REMOTE_BETA_VERSION="$(jq');
      expect(publishStep?.run).toContain('if [ "$REMOTE_INTEGRITY" != "$LOCAL_INTEGRITY" ]');
      expect(publishStep?.run).toContain('if [ "$REMOTE_BETA_VERSION" != "$VERSION" ]');
      expect(publishStep?.run).toContain('node "$NPM_CLI" publish "$TARBALL"');
      expect(publishStep?.run).toContain("--ignore-scripts");
      expect(publishStep?.run).toContain("--registry https://registry.npmjs.org");
      expect(publishStep?.run).not.toContain("node_modules");
      expect(publishStep?.run).not.toContain("vp run");

      const tagJob = releaseWorkflow.jobs["tag-release"];
      expect(tagJob?.permissions).toEqual({ actions: "read", contents: "write" });
      expect(tagJob?.steps?.every((step) => step.uses === undefined)).toBe(true);
      const tagStep = workflowStep(
        releaseWorkflow,
        "tag-release",
        "Verify manifest and create package tags",
      );
      expect(tagStep?.run).toContain("([.packages[].name] | unique | length) == 14");
      expect(tagStep?.run).toContain("([.packages[].version] | unique) == [$expectedVersion]");
      expect(tagStep?.run).toContain('.distTag == "beta"');
      expect(tagStep?.run).toContain(
        "repos/${GITHUB_REPOSITORY}/contents/packages/${PACKAGE_DIRECTORY}/package.json?ref=${GITHUB_SHA}",
      );
      expect(tagStep?.env).toEqual({
        GH_TOKEN: "${{ github.token }}",
        PUBLISH_RESULT: "${{ needs.publish-packages.result }}",
      });
      expect(tagStep?.run).toContain(
        'if [ "$PUBLISH_RESULT" = "skipped" ] && [ "$TARBALL_COUNT" != "0" ]',
      );
      expect(tagStep?.run).toContain('-f "ref=refs/tags/${TAG}"');
      expect(tagStep?.run).toContain('-f "sha=${GITHUB_SHA}"');
      expect(tagStep?.run).toContain('REF_TYPE="$(jq --raw-output');
      expect(tagStep?.run).toContain('if [ "$REF_TYPE" != "commit" ]');
      expect(tagStep?.run).toContain('[ "$REF_SHA" != "$GITHUB_SHA" ]');
      for (const packageDirectory of packageNames) {
        const manifest = yield* readManifest(
          `${repositoryRoot}/packages/${packageDirectory}/package.json`,
        );
        expect(manifest.name).toBeDefined();
        const trustedPolicyEntry = `${packageDirectory}\t${manifest.name}`;
        expect(publishStep?.run).toContain(trustedPolicyEntry);
        expect(tagStep?.run).toContain(trustedPolicyEntry);
      }

      expect(
        Object.entries(releaseWorkflow.jobs)
          .filter(([, job]) => job.permissions?.["id-token"] === "write")
          .map(([jobName]) => jobName),
      ).toEqual(["publish-packages"]);
      expect(
        Object.entries(releaseWorkflow.jobs)
          .filter(([, job]) => job.permissions?.checks === "write")
          .map(([jobName]) => jobName),
      ).toEqual(["report-release-check"]);

      expect(rootManifest.scripts?.["verify:changesets-release"]).toBe(
        "bun scripts/verify-changesets-release.ts",
      );
      expect(rootManifest.scripts?.["release:prepare"]).toBe("bun scripts/release-publish.ts");
      expect(rootManifest.scripts?.["ci:publish"]).toBeUndefined();
    }),
  );

  it.effect(
    "executes the generated release check reporter fail-closed",
    () =>
      Effect.gen(function* () {
        const releaseWorkflow = yield* readWorkflow(".github/workflows/release.yml");
        const reportScript = workflowStep(
          releaseWorkflow,
          "report-release-check",
          "Report the generated release check",
        )?.run;
        if (reportScript === undefined) {
          return yield* Effect.die(
            new Error("Release check reporter must have an executable body."),
          );
        }

        const expectedBaseSha = "1111111111111111111111111111111111111111";
        const verifiedHeadSha = "2222222222222222222222222222222222222222";
        const changedHeadSha = "3333333333333333333333333333333333333333";
        const changedBaseSha = "4444444444444444444444444444444444444444";
        const run = (overrides: {
          readonly baseRef?: string;
          readonly currentBaseSha?: string;
          readonly currentHeadSha?: string;
          readonly headRef?: string;
          readonly headRepository?: string;
          readonly strictReady?: string;
          readonly verifyOutcome?: string;
        }) =>
          runReleaseCheckReporter(reportScript, {
            baseRef: overrides.baseRef,
            currentBaseSha: overrides.currentBaseSha ?? expectedBaseSha,
            currentHeadSha: overrides.currentHeadSha ?? verifiedHeadSha,
            expectedBaseSha,
            headRef: overrides.headRef,
            headRepository: overrides.headRepository,
            strictReady: overrides.strictReady,
            verifiedHeadSha,
            verifyOutcome: overrides.verifyOutcome ?? "success",
          });

        const success = yield* run({});
        expect({ exitCode: success.exitCode, output: success.output }).toMatchObject({
          exitCode: 0,
        });
        expect(success.invocation).toContain("--method\nPOST\n");
        expect(success.invocation).toContain(
          "repos/effect-agent/release-check-fixture/check-runs\n",
        );
        expect(success.invocation).toContain("conclusion=success\n");
        expect(success.invocation).toContain(`head_sha=${verifiedHeadSha}\n`);
        expect(success.invocation).toContain("output[title]=Generated release tree verified\n");

        const verificationFailure = yield* run({ verifyOutcome: "failure" });
        expect({
          exitCode: verificationFailure.exitCode,
          output: verificationFailure.output,
        }).toMatchObject({ exitCode: 0 });
        expect(verificationFailure.invocation).toContain("conclusion=failure\n");
        expect(verificationFailure.invocation).toContain(
          "output[title]=Generated release tree did not verify\n",
        );

        const changedHead = yield* run({ currentHeadSha: changedHeadSha });
        expect({ exitCode: changedHead.exitCode, output: changedHead.output }).toMatchObject({
          exitCode: 0,
        });
        expect(changedHead.invocation).toContain("conclusion=failure\n");
        expect(changedHead.invocation).toContain(`head_sha=${changedHeadSha}\n`);
        expect(changedHead.invocation).toContain(
          "output[title]=Generated release head changed during verification\n",
        );

        const changedBase = yield* run({ currentBaseSha: changedBaseSha });
        expect({ exitCode: changedBase.exitCode, output: changedBase.output }).toMatchObject({
          exitCode: 0,
        });
        expect(changedBase.invocation).toContain("conclusion=failure\n");
        expect(changedBase.invocation).toContain(
          "output[title]=Generated release base changed during verification\n",
        );

        const nonStrictRule = yield* run({ strictReady: "false" });
        expect(nonStrictRule.invocation).toContain("conclusion=failure\n");
        expect(nonStrictRule.invocation).toContain(
          "output[title]=Strict required-check policy is missing\n",
        );

        const forkHead = yield* run({ headRepository: "someone-else/effect-agent" });
        expect(forkHead.invocation).toContain("conclusion=failure\n");
        expect(forkHead.invocation).toContain(
          "output[title]=Generated release source changed during verification\n",
        );

        const wrongHeadRef = yield* run({ headRef: "untrusted-release" });
        expect(wrongHeadRef.invocation).toContain("conclusion=failure\n");
        expect(wrongHeadRef.invocation).toContain(
          "output[title]=Generated release source changed during verification\n",
        );

        const wrongBaseRef = yield* run({ baseRef: "release-target" });
        expect(wrongBaseRef.invocation).toContain("conclusion=failure\n");
        expect(wrongBaseRef.invocation).toContain(
          "output[title]=Generated release target changed during verification\n",
        );
      }),
    15_000,
  );

  it.effect(
    "executes the isolated publisher fail-closed before npm publish",
    () =>
      Effect.gen(function* () {
        const releaseWorkflow = yield* readWorkflow(".github/workflows/release.yml");
        const publishScript = workflowStep(
          releaseWorkflow,
          "publish-packages",
          "Verify and publish prepared tarballs",
        )?.run;
        if (publishScript === undefined) {
          return yield* Effect.die(new Error("Release publisher must have an executable body."));
        }

        const validVersionMetadata = JSON.stringify({
          dist: { integrity: "sha512-fixture-integrity" },
        });
        const validPackageMetadata = JSON.stringify({
          "dist-tags": { beta: "0.0.1-beta.7" },
        });
        const existingVersion = yield* runReleasePublisher(publishScript, {
          packageMetadata: validPackageMetadata,
          registryStatus: "200",
          versionMetadata: validVersionMetadata,
        });
        expect(existingVersion).toMatchObject({ exitCode: 0, publishInvoked: false });

        const invalidMetadata = [
          {
            packageMetadata: validPackageMetadata,
            versionMetadata: "not-json",
          },
          {
            packageMetadata: validPackageMetadata,
            versionMetadata: JSON.stringify({ dist: {} }),
          },
          {
            packageMetadata: validPackageMetadata,
            versionMetadata: JSON.stringify({ dist: { integrity: "sha512-other" } }),
          },
          {
            packageMetadata: JSON.stringify({ "dist-tags": { beta: "0.0.1-beta.6" } }),
            versionMetadata: validVersionMetadata,
          },
        ];
        for (const fixture of invalidMetadata) {
          const result = yield* runReleasePublisher(publishScript, {
            packageMetadata: fixture.packageMetadata,
            registryStatus: "200",
            versionMetadata: fixture.versionMetadata,
          });
          expect(result.exitCode).not.toBe(0);
          expect(result.publishInvoked).toBe(false);
        }

        const unpublishedVersion = yield* runReleasePublisher(publishScript, {
          packageMetadata: validPackageMetadata,
          registryStatus: "404",
          versionMetadata: validVersionMetadata,
        });
        expect(unpublishedVersion).toMatchObject({ exitCode: 0, publishInvoked: true });
        expect(unpublishedVersion.publishInvocation[0]).toMatch(
          /[/]release-artifact[/][.]npm-cli[/]package[/]bin[/]npm-cli[.]js$/,
        );
        expect(unpublishedVersion.publishInvocation.slice(1)).toEqual([
          "publish",
          "packages/capabilities.tgz",
          "--access",
          "public",
          "--ignore-scripts",
          "--registry",
          "https://registry.npmjs.org",
          "--tag",
          "beta",
          "--provenance",
        ]);

        for (const checksumFailure of ["artifact", "npm"] as const) {
          const checksumResult = yield* runReleasePublisher(publishScript, {
            checksumFailure,
            packageMetadata: validPackageMetadata,
            registryStatus: "404",
            versionMetadata: validVersionMetadata,
          });
          expect(checksumResult.exitCode).not.toBe(0);
          expect(checksumResult.publishInvoked).toBe(false);
        }

        const unsafeTarball = yield* runReleasePublisher(publishScript, {
          packageMetadata: validPackageMetadata,
          registryStatus: "404",
          tarball: "../outside.tgz",
          versionMetadata: validVersionMetadata,
        });
        expect(unsafeTarball.exitCode).not.toBe(0);
        expect(unsafeTarball.publishInvoked).toBe(false);
      }),
    60_000,
  );

  it.effect("publishes prepared release directories atomically and cleans failed staging", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const temporaryRoot = yield* fs.makeTempDirectoryScoped({
        prefix: "effect-agent-release-artifact-test-",
      });
      const destination = path.join(temporaryRoot, "release-artifacts");
      let failedStaging = "";

      const failure = yield* Effect.flip(
        prepareReleaseArtifactDirectory(destination, (staging) =>
          Effect.gen(function* () {
            failedStaging = staging;
            yield* fs.writeFileString(path.join(failedStaging, "partial"), "incomplete\n");
            return yield* Effect.fail("fixture-failure" as const);
          }),
        ),
      );
      expect(failure).toBe("fixture-failure");
      expect(yield* fs.exists(failedStaging)).toBe(false);
      expect(yield* fs.exists(destination)).toBe(false);

      yield* prepareReleaseArtifactDirectory(destination, (staging) =>
        Effect.gen(function* () {
          expect(path.dirname(staging)).toBe(path.dirname(destination));
          yield* fs.writeFileString(path.join(staging, "release-manifest.json"), "{}\n");
        }),
      );
      expect(yield* fs.readFileString(path.join(destination, "release-manifest.json"))).toBe(
        "{}\n",
      );
      expect(
        (yield* fs.readDirectory(temporaryRoot)).filter((entry) =>
          entry.startsWith(".effect-agent-release-staging-"),
        ),
      ).toEqual([]);

      const commitCause = PlatformError.systemError({
        _tag: "PermissionDenied",
        module: "FileSystem",
        method: "rename",
        pathOrDescriptor: destination,
      });
      const cleanupCause = PlatformError.systemError({
        _tag: "Busy",
        module: "FileSystem",
        method: "remove",
        pathOrDescriptor: failedStaging,
      });
      const failingFileSystem = FileSystem.FileSystem.of({
        ...fs,
        remove: () => Effect.fail(cleanupCause),
        rename: () => Effect.fail(commitCause),
      });
      const cleanupExit = yield* prepareReleaseArtifactDirectory(
        path.join(temporaryRoot, "cleanup-failure"),
        () => Effect.void,
      ).pipe(Effect.provideService(FileSystem.FileSystem, failingFileSystem), Effect.exit);
      expect(Exit.isFailure(cleanupExit)).toBe(true);
      if (Exit.isFailure(cleanupExit)) {
        expect(Cause.hasDies(cleanupExit.cause)).toBe(false);
        const diagnostics = Cause.pretty(cleanupExit.cause);
        expect(diagnostics).toContain("Could not commit release artifacts");
        expect(diagnostics).toContain("PermissionDenied: FileSystem.rename");
        expect(diagnostics).toContain("Could not cleanup release artifacts");
        expect(diagnostics).toContain("Busy: FileSystem.remove");
      }
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
        const verifierSource = yield* readRepositoryFile("scripts/verify-changesets-release.ts");
        expect(verifierSource).toContain('"--frozen-lockfile"');
        expect(verifierSource).toContain(
          'path.join(expectedWorktree, "node_modules", ".bin", "changeset")',
        );
        expect(verifierSource).not.toContain(
          'path.join(root, "node_modules", ".bin", "changeset")',
        );

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

        let failedCleanupPath = "";
        const cleanupCause = PlatformError.systemError({
          _tag: "Busy",
          module: "FileSystem",
          method: "remove",
        });
        const cleanupFailingFileSystem = FileSystem.FileSystem.of({
          ...fs,
          remove: (target, options) => {
            if (String(target).includes("effect-agent-changesets-release-")) {
              failedCleanupPath = String(target);
              return Effect.fail(cleanupCause);
            }
            return fs.remove(target, options);
          },
        });
        const cleanupFailure = yield* Effect.flip(
          verifyChangesetsRelease({
            baseSha,
            changesetBinary,
            headSha: generatedHead,
            repositoryRoot: fixtureRoot,
          }).pipe(Effect.provideService(FileSystem.FileSystem, cleanupFailingFileSystem)),
        );
        expect(cleanupFailure).toMatchObject({
          _tag: "VerificationCleanupError",
          operation: "temporary directory removal",
          message: expect.stringContaining("Busy: FileSystem.remove"),
        });
        expect(failedCleanupPath).not.toBe("");
        yield* fs.remove(failedCleanupPath, { force: true, recursive: true });

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

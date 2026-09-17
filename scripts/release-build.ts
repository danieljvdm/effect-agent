import { createHash } from "node:crypto";

import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Config, Console, Effect, FileSystem, Path, Schema } from "effect";
import { FetchHttpClient, HttpClient } from "effect/unstable/http";

import { githubGet, readCommand, requireProof, Sha } from "./release-ci.ts";

const BuildPath = Schema.String.check(
  Schema.isPattern(
    /^(?:packages\/[a-z][a-z0-9-]*\/dist\/(?:[A-Za-z0-9_-]+\/)*[A-Za-z0-9_][A-Za-z0-9_.-]*|action\/dist\/index\.mjs)$/,
  ),
);

const PositiveInt = Schema.Int.check(Schema.isGreaterThan(0));

/** One inert artifact: only generated package files and the Action, never executable source. */
export const ReleaseBuild = Schema.Struct({
  version: Schema.Literal(1),
  runId: PositiveInt,
  runAttempt: PositiveInt,
  commit: Sha,
  tree: Sha,
  parents: Schema.Array(Sha),
  files: Schema.Array(
    Schema.Struct({
      path: BuildPath,
      sha256: Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/)),
      content: Schema.String.check(Schema.isBase64()),
    }),
  ),
});

const digest = (bytes: Uint8Array) => createHash("sha256").update(bytes).digest("hex");

export const verifyArchiveDigest = (bytes: Uint8Array, expected: string) =>
  requireProof(`sha256:${digest(bytes)}` === expected, "GitHub artifact digest mismatch");

/** GitHub's authenticated digest covers the complete archive, including the file inventory.
 * Download actions only warn on digest mismatch. Verify it before even decoding the payload.
 */
export const downloadReleaseBuild = Effect.fn("releaseBuild.download")(function* (
  root: string,
  runId: number,
  attempt: number,
  head: string,
  token: string,
) {
  const fs = yield* FileSystem.FileSystem;
  const client = yield* HttpClient.HttpClient;
  const name = `release-build-${runId}-${attempt}`;

  const listing = yield* githubGet(token)(
    `actions/runs/${runId}/artifacts?name=${name}&per_page=100`,
    Schema.Struct({
      total_count: Schema.Int,
      artifacts: Schema.Array(
        Schema.Struct({
          id: PositiveInt,
          name: Schema.String,
          expired: Schema.Boolean,
          digest: Schema.String.check(Schema.isPattern(/^sha256:[a-f0-9]{64}$/)),
          workflow_run: Schema.Struct({ id: PositiveInt, head_sha: Sha }),
        }),
      ),
    }),
  );

  const artifact = listing.artifacts[0];

  yield* requireProof(
    listing.total_count === 1 &&
      listing.artifacts.length === 1 &&
      artifact?.name === name &&
      !artifact.expired &&
      artifact.workflow_run.id === runId &&
      artifact.workflow_run.head_sha === head,
    "Missing or mismatched CI artifact",
  );
  if (artifact === undefined) return yield* requireProof(false, "Missing artifact");

  const redirect = yield* client
    .get(
      `https://api.github.com/repos/danieljvdm/effect-agent/actions/artifacts/${artifact.id}/zip`,
      {
        headers: { authorization: `Bearer ${token}`, accept: "application/vnd.github+json" },
      },
    )
    .pipe(Effect.provideService(FetchHttpClient.RequestInit, { redirect: "manual" }));

  const location = redirect.headers.location;

  yield* requireProof(
    redirect.status === 302 && location !== undefined && location.startsWith("https://"),
    "Missing artifact download redirect",
  );
  if (location === undefined) return yield* requireProof(false, "Missing download URL");
  // The signed storage request deliberately has no GitHub authorization header.
  const response = yield* client.get(location);

  yield* requireProof(response.status === 200, "Artifact download failed");
  const bytes = new Uint8Array(yield* response.arrayBuffer);

  yield* verifyArchiveDigest(bytes, artifact.digest);
  const directory = yield* fs.makeTempDirectoryScoped({ prefix: "release-build-" });
  const zip = `${directory}/build.zip`;

  yield* fs.writeFile(zip, bytes);
  yield* requireProof(
    (yield* readCommand(root, "unzip", ["-Z1", zip])).trim() === "build.json",
    "Unexpected artifact archive entries",
  );
  const json = yield* readCommand(root, "unzip", ["-p", zip, "build.json"]);

  yield* fs.makeDirectory(`${root}/.release-build`, { recursive: true });
  yield* fs.writeFileString(`${root}/.release-build/build.json`, json);
}, Effect.scoped);

const git = (root: string, ...args: ReadonlyArray<string>) =>
  readCommand(root, "git", args).pipe(Effect.map((text) => text.trim()));

const cleanTree = Effect.fn("releaseBuild.cleanTree")(function* (root: string) {
  yield* requireProof(
    (yield* git(root, "status", "--porcelain")) === "",
    "Build checkout is dirty",
  );

  return yield* git(root, "rev-parse", "HEAD^{tree}");
});

export const writeReleaseBuild = Effect.fn("releaseBuild.write")(function* (
  root: string,
  destination: string,
  runId: number,
  runAttempt: number,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const realRoot = yield* fs.realPath(root);
  const tree = yield* cleanTree(root);
  const files: Array<(typeof ReleaseBuild.Type.files)[number]> = [];
  const pending = ["action/dist/index.mjs"];

  for (const name of (yield* fs.readDirectory(path.join(root, "packages"))).sort()) {
    const relative = `packages/${name}/dist`;

    if (yield* fs.exists(path.join(root, relative))) pending.push(relative);
  }
  while (pending.length > 0) {
    const relative = pending.pop();

    if (relative === undefined) break;
    const absolute = path.join(realRoot, relative);

    yield* requireProof((yield* fs.realPath(absolute)) === absolute, "Build contains a symlink");
    const stat = yield* fs.stat(absolute);

    if (stat.type === "Directory") {
      for (const name of (yield* fs.readDirectory(absolute)).sort())
        pending.push(`${relative}/${name}`);
    } else {
      yield* requireProof(stat.type === "File", "Build contains a non-file");
      yield* Schema.decodeEffect(BuildPath)(relative);
      const bytes = yield* fs.readFile(absolute);

      files.push({
        path: relative,
        sha256: digest(bytes),
        content: Buffer.from(bytes).toString("base64"),
      });
    }
  }

  const build = {
    version: 1 as const,
    runId,
    runAttempt,
    tree,
    commit: yield* git(root, "rev-parse", "HEAD"),
    parents: (yield* git(root, "show", "-s", "--format=%P", "HEAD")).split(" "),
    files,
  };

  const json = yield* Schema.encodeEffect(Schema.fromJsonString(ReleaseBuild))(build);

  yield* fs.makeDirectory(path.dirname(destination), { recursive: true });
  yield* fs.writeFileString(destination, json);
  yield* Console.log(
    `Recorded ${files.length} build files for tree ${tree}, CI ${runId}/${runAttempt}`,
  );
});

/** Identity comes from trusted CI/API evidence, never from the downloaded file itself. */
export const restoreReleaseBuild = Effect.fn("releaseBuild.restore")(function* (
  root: string,
  artifact: string,
  expected: {
    readonly runId: number;
    readonly runAttempt: number;
    readonly commit?: string;
    readonly parents?: ReadonlyArray<string>;
  },
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const tree = yield* cleanTree(root);

  const build = yield* Schema.decodeEffect(Schema.fromJsonString(ReleaseBuild), {
    onExcessProperty: "error",
  })(yield* fs.readFileString(artifact));

  yield* requireProof(
    build.runId === expected.runId &&
      build.runAttempt === expected.runAttempt &&
      build.tree === tree &&
      (expected.commit === undefined || build.commit === expected.commit) &&
      (expected.parents === undefined || build.parents.join(" ") === expected.parents.join(" ")) &&
      build.files.length > 0 &&
      new Set(build.files.map((file) => file.path)).size === build.files.length &&
      build.files.some((file) => file.path === "action/dist/index.mjs"),
    "Release build identity does not match the verified CI checkout",
  );
  // Validate every byte before touching output. No archives, symlinks, manifests,
  // source files or paths outside these generated directories can be installed.
  for (const file of build.files)
    yield* requireProof(
      digest(Buffer.from(file.content, "base64")) === file.sha256,
      `Corrupt build file: ${file.path}`,
    );
  for (const name of yield* fs.readDirectory(path.join(root, "packages")))
    yield* fs.remove(path.join(root, "packages", name, "dist"), { recursive: true, force: true });
  yield* fs.remove(path.join(root, "action", "dist"), { recursive: true, force: true });
  for (const file of build.files) {
    const target = path.join(root, file.path);

    yield* fs.makeDirectory(path.dirname(target), { recursive: true });
    yield* fs.writeFile(target, Buffer.from(file.content, "base64"));
  }
  yield* Console.log(
    `Restored ${build.files.length} verified build files for tree ${tree}, CI ${build.runId}/${build.runAttempt}`,
  );
});

const program = Effect.gen(function* () {
  const root = yield* Config.String("GITHUB_WORKSPACE");
  const mode = yield* Config.String("RELEASE_BUILD_MODE");

  if (mode === "write") {
    yield* writeReleaseBuild(
      root,
      `${root}/.release-build/build.json`,
      yield* Config.Number("GITHUB_RUN_ID"),
      yield* Config.Number("GITHUB_RUN_ATTEMPT"),
    );
  } else {
    yield* requireProof(mode === "restore", "Unknown build operation");
    yield* downloadReleaseBuild(
      root,
      yield* Config.Number("RELEASE_BUILD_RUN"),
      yield* Config.Number("RELEASE_BUILD_ATTEMPT"),
      yield* Config.String("RELEASE_HEAD"),
      yield* Config.String("GITHUB_TOKEN"),
    );
    yield* restoreReleaseBuild(root, `${root}/.release-build/build.json`, {
      runId: yield* Config.Number("RELEASE_BUILD_RUN"),
      runAttempt: yield* Config.Number("RELEASE_BUILD_ATTEMPT"),
      parents: [yield* Config.String("RELEASE_BASE"), yield* Config.String("RELEASE_HEAD")],
    });
  }
});

if (import.meta.main)
  NodeRuntime.runMain(program.pipe(Effect.provide([NodeServices.layer, FetchHttpClient.layer])));

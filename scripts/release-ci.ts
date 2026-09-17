import { Buffer } from "node:buffer";

import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Config, Console, Effect, FileSystem, Schema, Stream } from "effect";
import { FetchHttpClient, HttpClient, HttpClientResponse } from "effect/unstable/http";
import { ChildProcess } from "effect/unstable/process";

const repository = "danieljvdm/effect-agent";
const workflowPath = ".github/workflows/ci.yml";

export const Sha = Schema.String.check(Schema.isPattern(/^[a-f0-9]{40}$/));

const PackageName = Schema.String.check(
  Schema.isPattern(/^(?:effect-agent|@effect-agent\/[a-z][a-z0-9-]*)$/),
);

const Beta = Schema.String.check(Schema.isPattern(/^\d+\.\d+\.\d+-beta\.(?:0|[1-9]\d*)$/));

const Manifest = Schema.Struct({
  name: PackageName,
  version: Beta,
  private: Schema.optionalKey(Schema.Literal(false)),
});

const PreState = Schema.Struct({
  mode: Schema.Literal("pre"),
  tag: Schema.Literal("beta"),
  initialVersions: Schema.Record(Schema.String, Schema.String),
  changesets: Schema.Array(Schema.String.check(Schema.isPattern(/^[a-z0-9-]+$/))),
});

export class ProofUnavailable extends Schema.TaggedError<ProofUnavailable>()("ProofUnavailable", {
  message: Schema.String,
}) {}

export const requireProof = (condition: boolean, message: string) =>
  condition ? Effect.void : Effect.fail(new ProofUnavailable({ message }));

/** Full Git tree differences, including file modes; never a GitHub paths summary. */
export interface MetadataChange {
  readonly path: string;
  readonly before: string | null;
  readonly after: string;
  readonly oldMode: string | null;
  readonly newMode: string;
}

/**
 * Deliberately recognizes only the current fixed beta train. Manifest and lock
 * edits are byte-for-byte replacements, not lossy JSON normalization. Policy,
 * source, dependencies and every other file must remain identical.
 */
export const verifyMetadata = Effect.fn("releaseCi.verifyMetadata")(function* (
  packages: ReadonlyArray<string>,
  changesetIds: ReadonlyArray<string>,
  changes: ReadonlyArray<MetadataChange>,
) {
  yield* Schema.decodeEffect(Schema.Array(PackageName))(packages);
  yield* requireProof(
    packages.length > 0 &&
      new Set(packages).size === packages.length &&
      new Set(changes.map((change) => change.path)).size === changes.length,
    "Fixed group",
  );
  const allowed = new Set(["bun.lock", ".changeset/pre.json"]);
  const versions = new Map<string, string>();
  const byPath = new Map(changes.map((change) => [change.path, change]));
  let releaseVersion: string | undefined;
  const lock = byPath.get("bun.lock");
  let expectedLock = lock?.before ?? "";

  for (const name of packages) {
    const directory = `packages/${name.replace("@effect-agent/", "")}`;
    const manifestPath = `${directory}/package.json`;
    const changelogPath = `${directory}/CHANGELOG.md`;

    allowed.add(manifestPath);
    allowed.add(changelogPath);
    const change = byPath.get(manifestPath);
    const changelog = byPath.get(changelogPath);

    if (change === undefined || change.before === null || changelog === undefined)
      return yield* new ProofUnavailable({ message: "Missing package version or changelog" });
    const before = yield* Schema.decodeEffect(Schema.fromJsonString(Manifest))(change.before);
    const after = yield* Schema.decodeEffect(Schema.fromJsonString(Manifest))(change.after);
    const dot = before.version.lastIndexOf(".");
    const beta = Number(before.version.slice(dot + 1));
    const next = `${before.version.slice(0, dot)}.${beta + 1}`;
    const versionLine = `  "version": "${before.version}",\n`;

    yield* requireProof(
      before.name === name &&
        after.name === name &&
        Number.isSafeInteger(beta + 1) &&
        after.version === next &&
        (releaseVersion === undefined || releaseVersion === next) &&
        change.before.split(versionLine).length === 2 &&
        change.after === change.before.replace(versionLine, `  "version": "${next}",\n`),
      "Only a synchronized one-step beta version bump is supported",
    );
    releaseVersion = next;
    versions.set(name, before.version);

    const header = `# ${name}\n\n`;
    const previous = changelog.before ?? header;
    const prefix = `${header}## ${next}\n`;
    const history = previous.slice(header.length);
    const added = changelog.after.slice(prefix.length, changelog.after.length - history.length);

    yield* requireProof(
      previous.startsWith(header) &&
        changelog.after.startsWith(prefix) &&
        changelog.after.endsWith(history) &&
        added.startsWith("\n") &&
        !added.includes("\n## "),
      "Changelog must prepend only the new release and preserve history",
    );

    // Bun's lockfile is JSONC. Do not parse and reserialize it: preserve every
    // byte outside these exact workspace version fields, including resolutions.
    const workspace = `    "${directory}": {\n      "name": "${name}",\n      "version": "${before.version}",`;

    yield* requireProof(expectedLock.split(workspace).length === 2, "Unknown Bun lockfile layout");
    expectedLock = expectedLock.replace(workspace, workspace.replace(before.version, next));
  }
  yield* requireProof(lock?.after === expectedLock, "Lockfile changed beyond workspace versions");
  for (const change of changes) {
    yield* requireProof(
      allowed.has(change.path) &&
        change.newMode === "100644" &&
        (change.oldMode === "100644" ||
          (change.oldMode === null && change.path.endsWith("/CHANGELOG.md"))),
      "Unsupported file or mode change",
    );
  }
  const pre = byPath.get(".changeset/pre.json");

  if (pre === undefined || pre.before === null)
    return yield* new ProofUnavailable({ message: "Missing prerelease state" });

  const decodePre = Schema.decodeEffect(Schema.fromJsonString(PreState), {
    onExcessProperty: "error",
  });

  const before = yield* decodePre(pre.before);
  const after = yield* decodePre(pre.after);

  for (const [name, version] of Object.entries(before.initialVersions)) {
    yield* requireProof(after.initialVersions[name] === version, "Rewritten initial version");
  }
  for (const [name, version] of Object.entries(after.initialVersions)) {
    yield* requireProof(
      before.initialVersions[name] === version ||
        (before.initialVersions[name] === undefined && versions.get(name) === version),
      "Unsupported initial version addition",
    );
  }
  for (const [name, version] of versions) {
    yield* requireProof(
      after.initialVersions[name] === (before.initialVersions[name] ?? version),
      "Missing public package initial version",
    );
  }
  const consumed = new Set(after.changesets);
  const added = after.changesets.filter((id) => !before.changesets.includes(id));

  yield* requireProof(
    new Set(before.changesets).size === before.changesets.length &&
      consumed.size === after.changesets.length &&
      before.changesets.every((id) => consumed.has(id)) &&
      changesetIds.every((id) => consumed.has(id)) &&
      added.length > 0 &&
      added.every((id) => changesetIds.includes(id)),
    "Prerelease consumption must only add existing changesets",
  );

  return releaseVersion;
});

export const Run = Schema.Struct({
  id: Schema.Int,
  run_attempt: Schema.Int,
  workflow_id: Schema.Int,
  path: Schema.String,
  name: Schema.String,
  event: Schema.String,
  head_branch: Schema.String,
  head_sha: Sha,
  status: Schema.String,
  conclusion: Schema.NullOr(Schema.String),
  repository: Schema.Struct({ full_name: Schema.String }),
  head_repository: Schema.Struct({ full_name: Schema.String }),
});

export const Jobs = Schema.Struct({
  total_count: Schema.Int,
  jobs: Schema.Array(
    Schema.Struct({
      name: Schema.String,
      run_id: Schema.Int,
      head_sha: Sha,
      status: Schema.String,
      conclusion: Schema.NullOr(Schema.String),
      steps: Schema.Array(
        Schema.Struct({
          name: Schema.String,
          status: Schema.String,
          conclusion: Schema.NullOr(Schema.String),
        }),
      ),
    }),
  ),
});

// Require the actual ordinary command steps, not a green fan-in, skipped job,
// artifact supplied by a PR, or a preceding fast-path approval.
const sourceGates = [
  ["Static checks", "Format, lint, and type checks"],
  ...[
    "workspace",
    "travel-planner",
    "context-continuity",
    "runtime-benchmark",
    "platform-node",
    "testing",
    "platform-cloudflare",
    "storage-cloudflare",
  ].map((suite) => [`Tests (${suite})`, "Run workspace test suites"]),
  ["Build", "Build packages, examples, and docs"],
] as const;

export const verifyEvidence = Effect.fn("releaseCi.verifyEvidence")(function* (
  base: string,
  workflowId: number,
  run: typeof Run.Type,
  jobs: typeof Jobs.Type,
) {
  yield* requireProof(
    run.repository.full_name === repository &&
      run.head_repository.full_name === repository &&
      run.workflow_id === workflowId &&
      run.path === workflowPath &&
      run.name === "CI" &&
      run.event === "push" &&
      run.head_branch === "main" &&
      run.head_sha === base &&
      run.status === "completed" &&
      run.conclusion === "success" &&
      run.run_attempt > 0 &&
      jobs.total_count === jobs.jobs.length &&
      jobs.total_count <= 100,
    "No complete ordinary CI run for the exact base and workflow",
  );
  yield* verifyJobs(base, run.id, jobs, sourceGates);
});

const verifyJobs = Effect.fn("releaseCi.verifyJobs")(function* (
  base: string,
  runId: number,
  jobs: typeof Jobs.Type,
  gates: ReadonlyArray<ReadonlyArray<string>>,
) {
  for (const [name, command] of gates) {
    const matches = jobs.jobs.filter((job) => job.name === name);
    const job = matches[0];
    const steps = job?.steps.filter((step) => step.name === command) ?? [];

    yield* requireProof(
      matches.length === 1 &&
        job?.run_id === runId &&
        job.head_sha === base &&
        job.status === "completed" &&
        job.conclusion === "success" &&
        steps.length === 1 &&
        steps[0]?.status === "completed" &&
        steps[0].conclusion === "success",
      "Missing or unsuccessful ordinary source gate",
    );
  }
});

export const Revisions = Schema.Struct({ base: Sha, head: Sha, checkout: Sha });

const Pull = Schema.Struct({
  number: Schema.Int,
  merged: Schema.Boolean,
  state: Schema.String,
  merge_commit_sha: Schema.NullOr(Sha),
  base: Schema.Struct({
    ref: Schema.String,
    sha: Sha,
    repo: Schema.Struct({ full_name: Schema.String }),
  }),
  head: Schema.Struct({
    ref: Schema.String,
    sha: Sha,
    repo: Schema.Struct({ full_name: Schema.String }),
  }),
});

export const verifyRevisions = Effect.fn("releaseCi.verifyRevisions")(function* (
  revisions: typeof Revisions.Type,
  pull: typeof Pull.Type,
  main: string,
  parents: string,
  checkoutTree: string,
  headTree: string,
  merged = false,
) {
  yield* requireProof(
    pull.state === (merged ? "closed" : "open") &&
      pull.merged === merged &&
      pull.base.repo.full_name === repository &&
      pull.head.repo.full_name === repository &&
      pull.base.ref === "main" &&
      pull.head.ref === "changeset-release/main" &&
      pull.base.sha === revisions.base &&
      pull.head.sha === revisions.head &&
      main === (merged ? revisions.checkout : revisions.base) &&
      pull.merge_commit_sha === revisions.checkout &&
      (parents === `${revisions.base} ${revisions.head}` ||
        (merged && parents === revisions.base)) &&
      checkoutTree === headTree,
    "PR, current main, and immutable merge checkout do not agree",
  );
});

/** Child-process boundary: nonzero exit must never be mistaken for empty output. */
export const readCommand = Effect.fn("releaseCi.readCommand")(function* (
  cwd: string,
  executable: string,
  args: ReadonlyArray<string>,
) {
  const child = yield* ChildProcess.make(executable, args, { cwd, stdout: "pipe", stderr: "pipe" });

  const [stdout, , code] = yield* Effect.all(
    [Stream.runCollect(child.stdout), Stream.runDrain(child.stderr), child.exitCode],
    { concurrency: "unbounded" },
  );

  yield* requireProof(code === 0, `${executable} failed`);

  // A replacement character or an unflushed partial UTF-8 sequence could hide
  // changed Git bytes. Preserve BOMs and reject invalid encoding, including EOF.
  return yield* Effect.try({
    try: () =>
      new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(Buffer.concat(stdout)),
    catch: () => ProofUnavailable.make({ message: "Command output is not valid UTF-8" }),
  });
}, Effect.scoped);

export const readMetadata = Effect.fn("releaseCi.readMetadata")(function* (
  root: string,
  base: string,
  head: string,
) {
  yield* Schema.decodeEffect(Schema.Struct({ base: Sha, head: Sha }))({ base, head });
  const git = (...args: ReadonlyArray<string>) => readCommand(root, "git", args);
  const read = (sha: string, path: string) => git("show", `${sha}:${path}`);

  const config = yield* Schema.decodeEffect(
    Schema.fromJsonString(
      Schema.Struct({
        fixed: Schema.Array(Schema.Array(PackageName)),
      }),
    ),
  )(yield* read(base, ".changeset/config.json"));

  yield* requireProof(config.fixed.length === 1, "Unsupported fixed groups");

  const raw = yield* git(
    "diff",
    "--raw",
    "--no-abbrev",
    "--no-renames",
    "--no-ext-diff",
    "--no-textconv",
    "--ignore-submodules=none",
    "-z",
    base,
    head,
    "--",
  );

  const fields = raw.split("\0");
  const changes: Array<MetadataChange> = [];

  yield* requireProof(fields.pop() === "" && fields.length % 2 === 0, "Incomplete Git diff");
  for (let index = 0; index < fields.length; index += 2) {
    const record = fields[index];
    const path = fields[index + 1];
    const match = record?.match(/^:(\d{6}) (\d{6}) [a-f0-9]{40} [a-f0-9]{40} ([AM])$/);

    if (match === null || match === undefined || path === undefined)
      return yield* new ProofUnavailable({ message: "Unsupported Git change" });
    changes.push({
      path,
      oldMode: match[1] === "000000" ? null : (match[1] ?? null),
      newMode: match[2] ?? "",
      before: match[3] === "A" ? null : yield* read(base, path),
      after: yield* read(head, path),
    });
  }

  const ids = (yield* git("ls-tree", "-r", "--name-only", base, ".changeset"))
    .split("\n")
    .flatMap((path) => /^\.changeset\/([a-z0-9-]+)\.md$/.exec(path)?.[1] ?? []);

  return yield* verifyMetadata(config.fixed[0] ?? [], ids, changes);
});

/** Read-only GitHub boundary shared by proof and exact-main artifact consumption. */
export const githubGet = (token: string) =>
  Effect.fn("releaseCi.githubGet")(function* <
    S extends Schema.Top & { readonly DecodingServices: never },
  >(path: string, schema: S) {
    const client = yield* HttpClient.HttpClient;

    const response = yield* client.get(`https://api.github.com/repos/${repository}/${path}`, {
      headers: {
        authorization: `Bearer ${token}`,
        accept: "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
      },
    });

    yield* requireProof(response.status === 200, "GitHub API unavailable");

    return yield* HttpClientResponse.schemaBodyJson(schema)(response);
  });

const Workflow = Schema.Struct({ id: Schema.Int, path: Schema.String, state: Schema.String });
const Main = Schema.Struct({ object: Schema.Struct({ sha: Sha }) });

export const verifyBuildEvidence = Effect.fn("releaseCi.verifyBuildEvidence")(function* (
  sha: string,
  workflowId: number,
  run: typeof Run.Type,
  jobs: typeof Jobs.Type,
  event: "push" | "pull_request",
) {
  yield* requireProof(
    run.repository.full_name === repository &&
      run.head_repository.full_name === repository &&
      run.workflow_id === workflowId &&
      run.path === workflowPath &&
      run.name === "CI" &&
      run.event === event &&
      run.head_branch === (event === "push" ? "main" : "changeset-release/main") &&
      run.head_sha === sha &&
      run.status === "completed" &&
      run.conclusion === "success" &&
      run.run_attempt > 0 &&
      jobs.total_count === jobs.jobs.length &&
      jobs.total_count <= 100,
    "No successful exact-input build run",
  );

  const gates = [
    ["Build", "Validate versioned release packages"],
    ["Build", "Upload release build"],
    ...(event === "pull_request"
      ? [
          ["Build", "Build packages, examples, and docs"],
          ["ready", "Verify all required gates passed"],
        ]
      : []),
  ];

  yield* verifyJobs(sha, run.id, jobs, gates);
});

/** The successful workflow_run event selects a run; recheck its attempt and current main. */
export const verifyMainBuild = Effect.fn("releaseCi.verifyMainBuild")(function* (
  sha: string,
  runId: number,
  attempt: number,
  token: string,
) {
  const get = githubGet(token);
  const workflow = yield* get("actions/workflows/ci.yml", Workflow);

  yield* requireProof(
    workflow.path === workflowPath && workflow.state === "active",
    "Workflow identity",
  );
  const run = yield* get(`actions/runs/${runId}`, Run);
  const jobs = yield* get(`actions/runs/${runId}/attempts/${attempt}/jobs?per_page=100`, Jobs);

  yield* requireProof(run.id === runId && run.run_attempt === attempt, "CI attempt changed");
  yield* verifyBuildEvidence(sha, workflow.id, run, jobs, "push");
  yield* requireProof(
    JSON.stringify(yield* get(`actions/runs/${runId}`, Run)) === JSON.stringify(run),
    "CI attempt changed during artifact verification",
  );
  yield* requireProof(
    (yield* get("git/ref/heads/main", Main)).object.sha === sha,
    "Main moved before publication",
  );
});

export const proveReleaseCi = Effect.fn("releaseCi.prove")(function* (
  root: string,
  revisions: typeof Revisions.Type,
  pullNumber: number,
  token: string,
  merged = false,
) {
  yield* Schema.decodeEffect(Revisions)(revisions);
  yield* requireProof(Number.isSafeInteger(pullNumber) && pullNumber > 0, "Invalid PR number");
  const get = githubGet(token);

  const git = (...args: ReadonlyArray<string>) => readCommand(root, "git", args);

  // The executable and dependencies must have come from the exact trusted base.
  yield* requireProof(
    (yield* git("rev-parse", "HEAD")).trim() === revisions.base,
    "Verifier is not on the base",
  );
  yield* git("fetch", "--no-tags", "origin", revisions.head, revisions.checkout);
  const parents = (yield* git("show", "-s", "--format=%P", revisions.checkout)).trim();
  const checkoutTree = (yield* git("rev-parse", `${revisions.checkout}^{tree}`)).trim();
  const headTree = (yield* git("rev-parse", `${revisions.head}^{tree}`)).trim();

  const checkCurrent = Effect.gen(function* () {
    const pull = yield* get(`pulls/${pullNumber}`, Pull);

    const main = yield* get("git/ref/heads/main", Main);

    yield* verifyRevisions(
      revisions,
      pull,
      main.object.sha,
      parents,
      checkoutTree,
      headTree,
      merged,
    );
  });

  yield* checkCurrent;
  yield* readMetadata(root, revisions.base, revisions.head);

  const workflow = yield* get("actions/workflows/ci.yml", Workflow);

  yield* requireProof(
    workflow.path === workflowPath && workflow.state === "active",
    "Workflow identity",
  );

  const runs = yield* get(
    `actions/workflows/${workflow.id}/runs?head_sha=${revisions.base}&event=push&branch=main&per_page=100`,
    Schema.Struct({ total_count: Schema.Int, workflow_runs: Schema.Array(Run) }),
  );

  yield* requireProof(
    runs.total_count === runs.workflow_runs.length && runs.total_count <= 100,
    "Incomplete run listing",
  );
  const run = runs.workflow_runs.toSorted((a, b) => b.id - a.id)[0];

  if (run === undefined) return yield* new ProofUnavailable({ message: "No base CI run" });

  const jobs = yield* get(
    `actions/runs/${run.id}/attempts/${run.run_attempt}/jobs?per_page=100`,
    Jobs,
  );

  yield* verifyEvidence(revisions.base, workflow.id, run, jobs);
  const refreshed = yield* get(`actions/runs/${run.id}`, Run);

  yield* requireProof(
    JSON.stringify(refreshed) === JSON.stringify(run),
    "CI attempt changed during proof",
  );
  let buildRun: typeof Run.Type | undefined;

  if (merged) {
    const builds = yield* get(
      `actions/workflows/${workflow.id}/runs?head_sha=${revisions.head}&event=pull_request&per_page=100`,
      Schema.Struct({ total_count: Schema.Int, workflow_runs: Schema.Array(Run) }),
    );

    yield* requireProof(
      builds.total_count === builds.workflow_runs.length && builds.total_count <= 100,
      "Incomplete build listing",
    );
    buildRun = builds.workflow_runs.toSorted((a, b) => b.id - a.id)[0];
    if (buildRun === undefined)
      return yield* new ProofUnavailable({ message: "No version PR build" });

    const buildJobs = yield* get(
      `actions/runs/${buildRun.id}/attempts/${buildRun.run_attempt}/jobs?per_page=100`,
      Jobs,
    );

    yield* verifyBuildEvidence(revisions.head, workflow.id, buildRun, buildJobs, "pull_request");
    yield* requireProof(
      JSON.stringify(yield* get(`actions/runs/${buildRun.id}`, Run)) === JSON.stringify(buildRun),
      "Build attempt changed during proof",
    );
  }
  yield* checkCurrent;

  return {
    ...revisions,
    runId: run.id,
    runAttempt: run.run_attempt,
    ...(buildRun === undefined
      ? {}
      : { buildRunId: buildRun.id, buildRunAttempt: buildRun.run_attempt }),
  };
});

/** Commit association responses omit `merged`; the full PR is re-read by the proof. */
export const proveMergedReleaseCi = Effect.fn("releaseCi.proveMerged")(function* (
  root: string,
  base: string,
  checkout: string,
  token: string,
) {
  yield* Schema.decodeEffect(Schema.Struct({ base: Sha, checkout: Sha }))({ base, checkout });

  const pulls = yield* githubGet(token)(
    `commits/${checkout}/pulls?per_page=100`,
    Schema.Array(Schema.Struct({ number: Schema.Int, head: Schema.Struct({ sha: Sha }) })),
  );

  yield* requireProof(pulls.length === 1, "Ambiguous merged PR");
  const pull = pulls[0];

  if (pull === undefined) return yield* new ProofUnavailable({ message: "No merged PR" });

  return yield* proveReleaseCi(
    root,
    { base, head: pull.head.sha, checkout },
    pull.number,
    token,
    true,
  );
});

/** A timeout, API/schema error or defect is an ordinary-CI decision, never success evidence. */
export const decideReleaseCi = <A, E, R>(proof: Effect.Effect<A, E, R>) =>
  proof.pipe(
    Effect.timeout("45 seconds"),
    Effect.map((evidence) => ({ fast: true as const, evidence })),
    Effect.catchCause(() => Effect.succeed({ fast: false as const })),
  );

const program = Effect.gen(function* () {
  const decision = yield* decideReleaseCi(
    Effect.gen(function* () {
      const event = yield* Config.String("GITHUB_EVENT_NAME");

      yield* requireProof(
        (yield* Config.String("GITHUB_REPOSITORY")) === repository &&
          (event === "pull_request" || event === "push"),
        "Only this repository's PR/main CI is eligible",
      );
      const root = yield* Config.String("GITHUB_WORKSPACE");
      const base = yield* Config.String("RELEASE_BASE");
      const checkout = yield* Config.String("GITHUB_SHA");
      const token = yield* Config.String("GITHUB_TOKEN");

      if (event === "push") {
        yield* requireProof(
          (yield* Config.String("GITHUB_REF")) === "refs/heads/main",
          "Only main merges",
        );

        return yield* proveMergedReleaseCi(root, base, checkout, token);
      }

      return yield* proveReleaseCi(
        root,
        {
          base,
          head: yield* Config.String("RELEASE_HEAD"),
          checkout,
        },
        yield* Config.Number("RELEASE_PR"),
        token,
      );
    }),
  );

  const fs = yield* FileSystem.FileSystem;

  yield* Console.log(JSON.stringify(decision));
  yield* fs.writeFileString(
    yield* Config.String("GITHUB_OUTPUT"),
    `fast=${decision.fast}\n${decision.fast && decision.evidence.buildRunId !== undefined ? `build-run=${decision.evidence.buildRunId}\nbuild-attempt=${decision.evidence.buildRunAttempt}\nbase=${decision.evidence.base}\nhead=${decision.evidence.head}\n` : ""}`,
    {
      flag: "a",
    },
  );
  yield* fs.writeFileString(
    yield* Config.String("GITHUB_STEP_SUMMARY"),
    decision.fast
      ? `Reused ordinary [CI ${decision.evidence.runId}, attempt ${decision.evidence.runAttempt}](https://github.com/${repository}/actions/runs/${decision.evidence.runId}) for base \`${decision.evidence.base}\`. Release head \`${decision.evidence.head}\`, merge checkout \`${decision.evidence.checkout}\`. Exact-tree build evidence and package validation remain required.\n`
      : "Release equivalence or CI evidence was not proven; running ordinary CI.\n",
    { flag: "a" },
  );
});

if (import.meta.main)
  NodeRuntime.runMain(program.pipe(Effect.provide([NodeServices.layer, FetchHttpClient.layer])));

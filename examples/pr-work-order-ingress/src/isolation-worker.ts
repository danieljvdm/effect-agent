/// <reference types="node" />
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { readFileSync, renameSync, unlinkSync, writeFileSync } from "node:fs";

import { Schema } from "effect";

import { IsolatedCheckRequest, IsolatedPublishWorkerRequest } from "./worker-contracts.ts";

const WRITE_TOKEN = "EFFECT_AGENT_GITHUB_WRITE_TOKEN";
const MODEL_SECRET = "EFFECT_AGENT_MODEL_SECRET";
const role = process.argv[2];
const requestPath = process.argv[3];

const fail = (payload: unknown): never => {
  process.stdout.write(JSON.stringify(payload));
  process.exit(0);
};

const environment = {
  hasWriteToken: Object.prototype.hasOwnProperty.call(process.env, WRITE_TOKEN),
  hasModelSecret: Object.prototype.hasOwnProperty.call(process.env, MODEL_SECRET),
};

const requestText = (() => {
  try {
    return readFileSync(requestPath ?? "", "utf8");
  } catch {
    return undefined;
  }
})();

const decodeRequest = <A>(schema: Schema.Codec<A, string>) => {
  if (requestText === undefined) return undefined;
  try {
    return Schema.decodeUnknownSync(schema)(requestText);
  } catch {
    return undefined;
  }
};

const headerPath = (line: string, side: string) => {
  const body = line.slice(4).split("\t", 1)[0] ?? "";
  if (body === "/dev/null") return "/dev/null";
  const prefix = `${side}/`;
  if (!body.startsWith(prefix)) return;
  const value = body.slice(prefix.length);
  return value.length > 0 ? value : undefined;
};

const provenPathsFromPatch = (patch: string) => {
  if (patch.trim() === "") return { ok: true as const, paths: [] as Array<string> };
  const paths = new Set<string>();
  let current: { source: string | undefined; dest: string | undefined } | undefined;
  let files = 0;
  const start = () => {
    current ??= { source: undefined, dest: undefined };
    return current;
  };
  const assign = (field: "source" | "dest", value: string) => {
    const slot = start();
    if (slot[field] !== undefined && slot[field] !== value) return false;
    slot[field] = value;
    return true;
  };
  const flush = () => {
    if (current === undefined) return true;
    if (current.source === undefined || current.dest === undefined) return false;
    if (current.source !== "/dev/null") paths.add(current.source);
    if (current.dest !== "/dev/null") paths.add(current.dest);
    files += 1;
    current = undefined;
    return true;
  };
  for (const line of patch.split("\n")) {
    if (line.startsWith("diff --git ")) {
      if (!flush()) return { ok: false as const };
      const git = /^diff --git a\/(.+) b\/(.+)$/.exec(line);
      if (git === null || git[1] === undefined || git[2] === undefined)
        return { ok: false as const };
      if (!assign("source", git[1]) || !assign("dest", git[2])) return { ok: false as const };
      continue;
    }
    if (line.startsWith("--- ")) {
      const path = headerPath(line, "a");
      if (path === undefined || !assign("source", path)) return { ok: false as const };
      continue;
    }
    if (line.startsWith("+++ ")) {
      const path = headerPath(line, "b");
      if (path === undefined || !assign("dest", path)) return { ok: false as const };
      continue;
    }
    if (line.startsWith("rename from ") || line.startsWith("copy from ")) {
      const path = line.slice(line.startsWith("rename from ") ? 12 : 10);
      if (path.length === 0 || !assign("source", path)) return { ok: false as const };
      continue;
    }
    if (line.startsWith("rename to ") || line.startsWith("copy to ")) {
      const path = line.slice(line.startsWith("rename to ") ? 10 : 8);
      if (path.length === 0 || !assign("dest", path)) return { ok: false as const };
    }
  }
  if (!flush() || files === 0) return { ok: false as const };
  return { ok: true as const, paths: [...paths] };
};

const sameStringArray = (left: ReadonlyArray<string>, right: ReadonlyArray<string>) =>
  left.length === right.length && left.every((value, index) => value === right[index]);

const sameChecks = (
  left: IsolatedPublishWorkerRequest["trust"]["requiredChecks"],
  right: IsolatedPublishWorkerRequest["trust"]["requiredChecks"],
) =>
  left.length === right.length &&
  left.every(
    (check, index) =>
      check.name === right[index]?.name &&
      check.status === right[index]?.status &&
      check.summary === right[index]?.summary,
  );

const sameIdentity = (
  trust: IsolatedPublishWorkerRequest["trust"],
  expected: IsolatedPublishWorkerRequest["expected"],
) =>
  trust.workOrderId === expected.workOrderId &&
  trust.workOrderDigest === expected.workOrderDigest &&
  trust.repository === expected.repository &&
  trust.pullRequestNumber === expected.pullRequestNumber &&
  trust.expectedHeadSha === expected.expectedHeadSha &&
  trust.patchDigest === expected.patchDigest &&
  sameStringArray(trust.allowedPaths, expected.allowedPaths) &&
  sameChecks(trust.requiredChecks, expected.requiredChecks);

if (role === "check") {
  const request =
    decodeRequest(Schema.fromJsonString(IsolatedCheckRequest)) ??
    fail({
      _tag: "IsolationViolation",
      process: "check",
      reason: "isolation request failed schema decode",
    });
  if (environment.hasWriteToken || environment.hasModelSecret) {
    fail({
      _tag: "IsolationViolation",
      process: "check",
      reason: "check process inherited a GitHub write token or model-provider secret",
    });
  }
  const results = [];
  for (const check of request.checks) {
    const spawned = spawnSync(check.command, check.args, {
      cwd: request.worktreeRoot,
      env: { PATH: "/usr/bin:/bin" },
      encoding: "utf8",
      timeout: 10_000,
    });
    const output = `${spawned.stdout ?? ""}${spawned.stderr ?? ""}`.slice(0, 2_000);
    results.push({
      name: check.name,
      status: spawned.status === 0 ? "passed" : "failed",
      summary: output.length > 0 ? output : spawned.status === 0 ? "passed" : "failed",
    });
  }
  process.stdout.write(
    JSON.stringify({
      _tag: "checked",
      environment: { process: "check", ...environment },
      results,
    }),
  );
  process.exit(0);
}

if (role === "publish") {
  const request =
    decodeRequest(Schema.fromJsonString(IsolatedPublishWorkerRequest)) ??
    fail({
      _tag: "IsolationViolation",
      process: "publish",
      reason: "isolation request failed schema decode",
    });
  if (environment.hasModelSecret) {
    fail({
      _tag: "IsolationViolation",
      process: "publish",
      reason: "publisher process inherited a model-provider secret",
    });
  }
  const { patch, trust, expected, stateDir } = request;
  if (!sameIdentity(trust, expected)) {
    fail({
      _tag: "PublisherVerificationFailure",
      reason: "identity-mismatch",
      detail:
        "publication request identity does not match independently owned publisher configuration",
    });
  }
  const digest = createHash("sha256").update(patch).digest("hex");
  if (digest !== trust.patchDigest) {
    fail({
      _tag: "PublisherVerificationFailure",
      reason: "digest-mismatch",
      detail: "publisher-computed patch digest differs from the host-validated digest",
    });
  }
  const proven = provenPathsFromPatch(patch);
  const paths = proven.ok
    ? proven.paths
    : fail({
        _tag: "PublisherVerificationFailure",
        reason: "path-not-allowed",
        detail: "publisher could not prove the complete source and destination path set",
      });
  if (paths.some((entry) => !trust.allowedPaths.includes(entry))) {
    fail({
      _tag: "PublisherVerificationFailure",
      reason: "path-not-allowed",
      detail: "publisher-derived patch paths are outside the trusted allowlist",
    });
  }
  if (trust.requiredChecks.some((check) => check.status !== "passed")) {
    fail({
      _tag: "PublisherVerificationFailure",
      reason: "check-evidence",
      detail: "trusted required-check evidence is not all passing",
    });
  }
  const lockPath = `${stateDir}/head.lock`;
  try {
    writeFileSync(lockPath, `${String(process.pid)}\n`, { flag: "wx" });
  } catch {
    fail({
      _tag: "PublicationUncertainty",
      reason: "lost exclusive publication lock",
    });
  }
  const releaseLock = () => {
    try {
      unlinkSync(lockPath);
    } catch {
      // lock already released
    }
  };
  let actual: string;
  try {
    actual = readFileSync(`${stateDir}/head`, "utf8").trim();
  } catch (cause) {
    releaseLock();
    actual = fail({
      _tag: "PublicationUncertainty",
      reason: `could not read current head: ${String(cause).slice(0, 1_000)}`,
    });
  }
  if (actual !== trust.expectedHeadSha) {
    releaseLock();
    fail({
      _tag: "StalePullRequestHead",
      expected: trust.expectedHeadSha,
      actual,
    });
  }
  const published = createHash("sha256").update(`${actual}\n${patch}`).digest("hex").slice(0, 40);
  try {
    writeFileSync(`${stateDir}/applied.patch`, patch);
    writeFileSync(`${stateDir}/head.next`, `${published}\n`);
    renameSync(`${stateDir}/head.next`, `${stateDir}/head`);
  } catch (cause) {
    releaseLock();
    fail({
      _tag: "PublicationUncertainty",
      reason: `compare-and-swap write failed: ${String(cause).slice(0, 1_000)}`,
      observedHeadSha: actual,
    });
  }
  releaseLock();
  process.stdout.write(
    JSON.stringify({
      _tag: "published",
      headSha: published,
      environment: { process: "publish", ...environment },
    }),
  );
  process.exit(0);
}

fail({
  _tag: "IsolationViolation",
  process: role === "publish" ? "publish" : "check",
  reason: "isolation worker received an unknown role",
});

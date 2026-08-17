import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { readFileSync, writeFileSync } from "node:fs";

const WRITE_TOKEN = "EFFECT_AGENT_GITHUB_WRITE_TOKEN";
const MODEL_SECRET = "EFFECT_AGENT_MODEL_SECRET";
const role = process.argv[2];
const requestPath = process.argv[3];

const fail = (payload) => {
  process.stdout.write(JSON.stringify(payload));
  process.exit(0);
};

const environment = {
  hasWriteToken: Object.prototype.hasOwnProperty.call(process.env, WRITE_TOKEN),
  hasModelSecret: Object.prototype.hasOwnProperty.call(process.env, MODEL_SECRET),
};

const request = JSON.parse(readFileSync(requestPath, "utf8"));

if (role === "check") {
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
  if (environment.hasModelSecret) {
    fail({
      _tag: "IsolationViolation",
      process: "publish",
      reason: "publisher process inherited a model-provider secret",
    });
  }
  const { patch, trust, stateDir } = request;
  const digest = createHash("sha256").update(patch).digest("hex");
  if (digest !== trust.patchDigest) {
    fail({
      _tag: "PublisherVerificationFailure",
      reason: "digest-mismatch",
      detail: "publisher-computed patch digest differs from the host-validated digest",
    });
  }
  const paths = new Set();
  for (const line of patch.split("\n")) {
    const git = /^diff --git a\/(.+) b\/(.+)$/.exec(line);
    if (git?.[2] !== undefined && git[2] !== "/dev/null") paths.add(git[2]);
  }
  if ([...paths].some((entry) => !trust.allowedPaths.includes(entry))) {
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
  if (
    trust.workOrderId.length === 0 ||
    trust.workOrderDigest.length === 0 ||
    trust.repository.length === 0 ||
    trust.pullRequestNumber <= 0
  ) {
    fail({
      _tag: "PublisherVerificationFailure",
      reason: "identity-mismatch",
      detail: "trusted publication record does not name a complete work-order identity",
    });
  }
  const actual = readFileSync(`${stateDir}/head`, "utf8").trim();
  if (actual !== trust.expectedHeadSha) {
    fail({
      _tag: "StalePullRequestHead",
      expected: trust.expectedHeadSha,
      actual,
    });
  }
  const published = createHash("sha256").update(`${actual}\n${patch}`).digest("hex").slice(0, 40);
  writeFileSync(`${stateDir}/head`, published);
  writeFileSync(`${stateDir}/applied.patch`, patch);
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

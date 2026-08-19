import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Effect, Layer } from "effect";
import { FetchHttpClient } from "effect/unstable/http";

import { workOrderActionProgram } from "./action.ts";

// GitHub exposes JavaScript Action inputs only as INPUT_* environment
// variables. This executable boundary maps them to the typed Config surface;
// phase logic itself never reads or mutates process.env.
const INPUT_TO_ENV: ReadonlyArray<readonly [string, string]> = [
  ["INPUT_PHASE", "EFFECT_AGENT_PHASE"],
  ["INPUT_ARTIFACT-DIRECTORY", "EFFECT_AGENT_ARTIFACT_DIRECTORY"],
  ["INPUT_GITHUB-TOKEN", "EFFECT_AGENT_GITHUB_TOKEN"],
  ["INPUT_STATE-SECRET", "EFFECT_AGENT_STATE_SECRET"],
  ["INPUT_STATE-AUTHOR-ID", "EFFECT_AGENT_STATE_AUTHOR_ID"],
  ["INPUT_AUTHORIZED-ACTOR-IDS", "EFFECT_AGENT_AUTHORIZED_ACTOR_IDS"],
  ["INPUT_PROVIDER", "EFFECT_AGENT_PROVIDER"],
  ["INPUT_MODEL", "EFFECT_AGENT_MODEL"],
  ["INPUT_OPENAI-API-KEY", "OPENAI_API_KEY"],
  ["INPUT_ANTHROPIC-API-KEY", "ANTHROPIC_API_KEY"],
  ["INPUT_REPOSITORY-PATH", "EFFECT_AGENT_REPOSITORY_PATH"],
  ["INPUT_SUPPORT-PATHS", "EFFECT_AGENT_SUPPORT_PATHS"],
  ["INPUT_CHECKS", "EFFECT_AGENT_CHECKS"],
  ["INPUT_CHECK-CONTAINER-IMAGE", "EFFECT_AGENT_CHECK_CONTAINER_IMAGE"],
  ["INPUT_MAX-DURATION-MINUTES", "EFFECT_AGENT_MAX_DURATION_MINUTES"],
  ["INPUT_COMMIT-MESSAGE", "EFFECT_AGENT_COMMIT_MESSAGE"],
  ["INPUT_PUBLICATION-ATTEMPTED", "EFFECT_AGENT_PUBLICATION_ATTEMPTED"],
];

const uid = process.getuid?.();
const gid = process.getgid?.();
if (uid !== undefined && gid !== undefined) {
  process.env.EFFECT_AGENT_RUNNER_USER = `${String(uid)}:${String(gid)}`;
}

for (const [input, target] of INPUT_TO_ENV) {
  const value = process.env[input];
  if (value !== undefined && value !== "" && (process.env[target] ?? "") === "") {
    process.env[target] = value;
  }
}

NodeRuntime.runMain(
  workOrderActionProgram.pipe(
    Effect.scoped,
    Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer)),
  ),
  { disableErrorReporting: true },
);

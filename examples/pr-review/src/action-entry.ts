import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Effect, Layer } from "effect";
import { FetchHttpClient } from "effect/unstable/http";

import { reviewActionProgram } from "./action.ts";

const INPUT_TO_ENV: ReadonlyArray<readonly [string, string]> = [
  ["INPUT_OPENAI-API-KEY", "OPENAI_API_KEY"],
  ["INPUT_GITHUB-TOKEN", "GITHUB_TOKEN"],
  ["INPUT_PULL-REQUEST", "PR_REVIEW_PULL_REQUEST"],
  ["INPUT_REVIEW-AUTHOR", "PR_REVIEW_AUTHOR"],
  ["INPUT_MODE", "PR_REVIEW_MODE"],
  ["INPUT_EXPECTED-HEAD", "PR_REVIEW_EXPECTED_HEAD"],
  ["INPUT_MODEL", "PR_REVIEW_MODEL"],
  ["INPUT_EFFORT", "PR_REVIEW_EFFORT"],
  ["INPUT_GUIDANCE-FILE", "PR_REVIEW_GUIDANCE_FILE"],
  ["INPUT_IGNORE", "PR_REVIEW_IGNORE"],
];

for (const [input, target] of INPUT_TO_ENV) {
  const value = process.env[input];
  if (value !== undefined && value !== "" && (process.env[target] ?? "") === "") {
    process.env[target] = value;
  }
}

NodeRuntime.runMain(
  reviewActionProgram.pipe(
    Effect.scoped,
    Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer)),
  ),
  { disableErrorReporting: true },
);

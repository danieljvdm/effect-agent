import { main } from "../action.ts";

// ---------------------------------------------------------------------------
// The bundled GitHub Action entrypoint (built by `scripts/build-action.ts`
// into `action/dist/index.mjs`). A node-runtime action exposes its manifest
// inputs only as `INPUT_<NAME>` environment variables, so this runtime
// adapter — the one place the package touches `process.env` directly — maps
// them onto the PR_REVIEW_* surface `resolveActionInputs` reads, plus the
// provider and GitHub credentials. Explicit environment variables win over
// manifest inputs.
// ---------------------------------------------------------------------------

const INPUT_TO_ENV: ReadonlyArray<readonly [input: string, env: string]> = [
  ["INPUT_PROVIDER", "PR_REVIEW_PROVIDER"],
  ["INPUT_MODEL", "PR_REVIEW_MODEL"],
  ["INPUT_EFFORT", "PR_REVIEW_EFFORT"],
  ["INPUT_MAX-DURATION-MINUTES", "PR_REVIEW_MAX_DURATION_MINUTES"],
  ["INPUT_POST", "PR_REVIEW_POST"],
  ["INPUT_APPLY-VERDICT", "PR_REVIEW_APPLY_VERDICT"],
  ["INPUT_FAN-OUT", "PR_REVIEW_FAN_OUT"],
  ["INPUT_GUIDANCE", "PR_REVIEW_GUIDANCE"],
  ["INPUT_GUIDANCE-FILE", "PR_REVIEW_GUIDANCE_FILE"],
  ["INPUT_IGNORE", "PR_REVIEW_IGNORE"],
  ["INPUT_MAX-FINDINGS", "PR_REVIEW_MAX_FINDINGS"],
  ["INPUT_REVIEW-MODE", "PR_REVIEW_MODE"],
  ["INPUT_FAIL-ON", "PR_REVIEW_FAIL_ON"],
  ["INPUT_SKIP-UNCHANGED", "PR_REVIEW_SKIP_UNCHANGED"],
  ["INPUT_RETIRE-STALE-REVIEWS", "PR_REVIEW_RETIRE_STALE_REVIEWS"],
  ["INPUT_STATE-SECRET", "PR_REVIEW_STATE_SECRET"],
  ["INPUT_REVIEW-AUTHOR", "PR_REVIEW_AUTHOR_LOGIN"],
  ["INPUT_OPENAI-API-KEY", "OPENAI_API_KEY"],
  ["INPUT_ANTHROPIC-API-KEY", "ANTHROPIC_API_KEY"],
  ["INPUT_GITHUB-TOKEN", "GITHUB_TOKEN"],
];

for (const [input, env] of INPUT_TO_ENV) {
  const value = process.env[input];
  if (value !== undefined && value !== "" && (process.env[env] ?? "") === "") {
    process.env[env] = value;
  }
}

main();

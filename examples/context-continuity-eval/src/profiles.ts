import { Schema } from "effect";

export const PROFILE_IDS = [
  "explicit-rollover-sqlite-v1",
  "pressure-restart-sqlite-v1",
  "pressure-cloudflare-v1",
  "production-capacity-v1",
] as const;

export const ProfileId = Schema.Literals(PROFILE_IDS);
export type ProfileId = typeof ProfileId.Type;

/** Automatic jobs run exactly one bounded profile. Larger capacity is planning-only. */
export const DEFAULT_PROFILE: ProfileId = "explicit-rollover-sqlite-v1";
export const REDUCED_CONTEXT_TOKENS = 16_000;
export const MAX_COST_MICROUSD = 10_000_000;

export const profilePlan = (profile: ProfileId, productionContextTokens: number) => ({
  profile,
  host: profile === "pressure-cloudflare-v1" ? "cloudflare" : "node-sqlite",
  trigger: profile === DEFAULT_PROFILE ? "requested" : "pressure",
  recovery:
    profile === DEFAULT_PROFILE
      ? "service-reacquisition"
      : profile === "pressure-cloudflare-v1"
        ? "durable-object-eviction"
        : "SIGKILL",
  contextTokenLimit:
    profile === "production-capacity-v1" ? productionContextTokens : REDUCED_CONTEXT_TOKENS,
  capacity: profile === "production-capacity-v1" ? "configured-production" : "reduced",
  liveEnabled: profile !== "production-capacity-v1",
  maxCostMicrousd: MAX_COST_MICROUSD,
  requiredCommittedWindows: 12,
  manifestPagesPerUpdate: profile === DEFAULT_PROFILE ? 0 : 2,
  manifestPageCharacters: profile === "production-capacity-v1" ? productionContextTokens * 2 : REDUCED_CONTEXT_TOKENS * 2,
  ...(profile === "production-capacity-v1" ? {
    prerequisites: ["Confirm actual application/model capacity", "Approve a model-specific budget including output and repeated history", "Enable a separate full-capacity input ceiling and verify the production host"],
  } : {}),
});

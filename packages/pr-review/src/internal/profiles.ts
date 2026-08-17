import { Schema } from "effect";

// ---------------------------------------------------------------------------
// Committed capability claims, schema-first: what this package's reviewers
// promise and — just as deliberately — what they never claim. Deployment
// class E (ephemeral): one bounded AgentRuntime.run per invocation, no
// durability, no exactly-once external effects (DUR-003).
// ---------------------------------------------------------------------------

/** The flat reviewer's committed capability claim. */
export class PullRequestReviewerProfile extends Schema.Class<PullRequestReviewerProfile>(
  "@effect-agent/pr-review/PullRequestReviewerProfile",
)({
  /** Ephemeral runtime: one bounded AgentRuntime.run per invocation. */
  deploymentClass: Schema.Literal("E"),
  /** Every model-callable tool is a read of the pull request; none mutate. */
  readOnlyToolSurface: Schema.Literal(true),
  /** The review is posted by the host AFTER the run settles, never by a tool. */
  publicationOutsideAgentLoop: Schema.Literal(true),
  /** Finding anchors are validated against the parsed diff before posting. */
  anchorsValidatedBeforePublication: Schema.Literal(true),
  /** The live profile is env-gated out of every ordinary test gate. */
  liveProfileOptIn: Schema.Literal(true),
  /** Never claimed at any phase (DUR-003). */
  exactlyOnceExternalEffects: Schema.Literal(false),
}) {}

export const pullRequestReviewerProfile = PullRequestReviewerProfile.make({
  deploymentClass: "E",
  readOnlyToolSurface: true,
  publicationOutsideAgentLoop: true,
  anchorsValidatedBeforePublication: true,
  liveProfileOptIn: true,
  exactlyOnceExternalEffects: false,
});

/** The fan-out reviewer's committed capability claim, schema-first. */
export class FanOutReviewerProfile extends Schema.Class<FanOutReviewerProfile>(
  "@effect-agent/pr-review/FanOutReviewerProfile",
)({
  /** Ephemeral runtime: one bounded AgentRuntime.run per invocation. */
  deploymentClass: Schema.Literal("E"),
  /** Every model-callable tool — parent and child — is a read; none mutate. */
  readOnlyToolSurface: Schema.Literal(true),
  /** The review is posted by the host AFTER the run settles, never by a tool. */
  publicationOutsideAgentLoop: Schema.Literal(true),
  /** Child findings are untrusted; anchors are validated against the parsed diff. */
  anchorsValidatedBeforePublication: Schema.Literal(true),
  /** S1 attached ephemeral delegation at depth 1; nested delegation is rejected. */
  attachedEphemeralDelegation: Schema.Literal(true),
  /** A failed unit surfaces to the coordinator as a typed failed result, never retried. */
  failedUnitsReportedNotRetried: Schema.Literal(true),
  /** Risk categories and required specialist passes are pure host policy. */
  hostOwnedRiskClassification: Schema.Literal(true),
  /** Every host-classified high-risk unit receives a fresh specialist pass. */
  redundantHighRiskDiscovery: Schema.Literal(true),
  /** Only exact candidates confirmed by a fresh verifier child may publish. */
  independentCandidateVerification: Schema.Literal(true),
  /** No bounded model pipeline proves that a pull request is defect-free. */
  defectAbsenceProven: Schema.Literal(false),
  /** The live profile is env-gated out of every ordinary test gate. */
  liveProfileOptIn: Schema.Literal(true),
  /** Never claimed at any phase (DUR-003). */
  exactlyOnceExternalEffects: Schema.Literal(false),
}) {}

export const fanOutReviewerProfile = FanOutReviewerProfile.make({
  deploymentClass: "E",
  readOnlyToolSurface: true,
  publicationOutsideAgentLoop: true,
  anchorsValidatedBeforePublication: true,
  attachedEphemeralDelegation: true,
  failedUnitsReportedNotRetried: true,
  hostOwnedRiskClassification: true,
  redundantHighRiskDiscovery: true,
  independentCandidateVerification: true,
  defectAbsenceProven: false,
  liveProfileOptIn: true,
  exactlyOnceExternalEffects: false,
});

export const LIVE_GATE_ENV = "EFFECT_AGENT_LIVE";

/**
 * `EFFECT_AGENT_LIVE=1` plus the named credential is the only enabling
 * combination for live (network, billed) profiles.
 */
export const liveProfileEnabled = (
  env: Record<string, string | undefined>,
  credentialEnv: string,
): boolean => env[LIVE_GATE_ENV] === "1" && (env[credentialEnv] ?? "") !== "";

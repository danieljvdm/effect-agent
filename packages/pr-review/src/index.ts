// The @effect-agent/pr-review public surface. Platform-free: host entrypoints
// live behind the "./action" and "./cli" subpaths, deterministic test helpers
// behind "./testing".
export * from "./internal/diff.ts";
export * from "./internal/coverage.ts";
export * from "./internal/effort.ts";
export * from "./internal/factory.ts";
export * from "./internal/fan-out.ts";
export * from "./internal/fingerprint.ts";
export * from "./internal/github.ts";
export * from "./internal/github-env.ts";
export * from "./internal/ignore.ts";
export * from "./internal/logging.ts";
export * from "./internal/profiles.ts";
export * from "./internal/progress.ts";
export * from "./internal/providers.ts";
export * from "./internal/render.ts";
export * from "./internal/retirement.ts";
export * from "./internal/review-agent.ts";
// Continuity wire schemas and authenticator are public. Range selection is a
// host-only authority: exposing its issuer would let an arbitrary consumer
// mint a trusted narrowed scope before `PrReview.run` signs new continuity
// state. The Action imports that internal selector after recovering the
// authenticated GitHub state; generic consumers can only run a full source.
export {
  GitCommitSha,
  ReviewMode,
  ReviewScopeMode,
  StoredReviewFinding,
  StoredReviewConcern,
  ReviewState,
  ReviewStateMarker,
  ReviewStateAuthenticationFailure,
  ReviewStateMarkerTooLarge,
  ReviewStateAuthenticator,
  webCryptoReviewStateAuthenticatorLayer,
  unavailableReviewStateAuthenticatorLayer,
  ReviewHeadComparison,
  computeProfileFingerprint,
  validateReviewState,
  buildProfileMission,
} from "./internal/review-state.ts";
export type {
  ReviewMode as ReviewModeType,
  ReviewScopeMode as ReviewScopeModeType,
  ReviewSelection,
  ReviewStateMarker as ReviewStateMarkerType,
} from "./internal/review-state.ts";
export * from "./internal/review-units.ts";
export * from "./internal/run.ts";
export * from "./internal/source.ts";

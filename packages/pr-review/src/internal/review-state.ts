import { Context, Effect, Encoding, Layer, Option, Redacted, Result, Schema } from "effect";

import { ChangedFile, ChangedPath } from "./diff.ts";
import { FindingSeverity, ReviewConcern, ReviewFinding, ReviewMission } from "./review-agent.ts";
import { PullRequestSource, ReviewInputViolation, type PullRequestMetadata } from "./source.ts";

// ---------------------------------------------------------------------------
// Bounded review continuity. The Action is still deployment class E, but every
// completed review can publish authenticated state inside its GitHub review body.
// A later Action run validates the state against the live PR/base lineage and
// uses a head-to-head comparison to select only newly affected scope.
// ---------------------------------------------------------------------------

export const ReviewMode = Schema.Literals(["incremental", "final"]);
export type ReviewMode = typeof ReviewMode.Type;

export const ReviewScopeMode = Schema.Literals(["incremental", "full"]);
export type ReviewScopeMode = typeof ReviewScopeMode.Type;

export const GitCommitSha = Schema.NonEmptyString.check(
  Schema.isMaxLength(64),
  Schema.isPattern(/^[0-9a-f]{40,64}$/),
);

const Fingerprint = Schema.String.check(Schema.isPattern(/^[0-9a-f]{64}$/));
const StoredText = Schema.NonEmptyString.check(Schema.isMaxLength(800));

/** A compact unresolved finding suitable for the bounded review-body marker. */
export class StoredReviewFinding extends Schema.Class<StoredReviewFinding>(
  "@effect-agent/pr-review/StoredReviewFinding",
)({
  path: ChangedPath,
  startLine: Schema.Int.check(Schema.isGreaterThan(0)),
  endLine: Schema.Int.check(Schema.isGreaterThan(0)),
  severity: FindingSeverity,
  title: Schema.NonEmptyString.check(Schema.isMaxLength(120)),
  body: StoredText,
}) {}

/** A compact unresolved non-anchored concern carried until a final audit. */
export class StoredReviewConcern extends Schema.Class<StoredReviewConcern>(
  "@effect-agent/pr-review/StoredReviewConcern",
)({
  severity: FindingSeverity,
  title: Schema.NonEmptyString.check(Schema.isMaxLength(120)),
  body: StoredText,
}) {}

/**
 * Versioned state embedded in one successfully covered review. The reviewed
 * head plus the full-scope fingerprint means every path not represented by an
 * unresolved item is accepted at that head; storing hundreds of path strings
 * separately would not fit GitHub's bounded review body in the worst case.
 */
export class ReviewState extends Schema.Class<ReviewState>("@effect-agent/pr-review/ReviewState")({
  version: Schema.Literal(1),
  repository: Schema.NonEmptyString.check(Schema.isMaxLength(200)),
  pullRequestNumber: Schema.Int.check(Schema.isGreaterThan(0)),
  baseRef: Schema.NonEmptyString.check(Schema.isMaxLength(300)),
  baseSha: GitCommitSha,
  headRef: Schema.NonEmptyString.check(Schema.isMaxLength(300)),
  reviewedHeadSha: GitCommitSha,
  profileFingerprint: Fingerprint,
  acceptedScopeFingerprint: Fingerprint,
  reviewedPathCount: Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 300 })),
  unresolvedFindings: Schema.Array(StoredReviewFinding).check(Schema.isMaxLength(20)),
  unresolvedConcerns: Schema.Array(StoredReviewConcern).check(Schema.isMaxLength(10)),
  lastReviewMode: ReviewScopeMode,
}) {}

export const toStoredFinding = (finding: ReviewFinding): StoredReviewFinding =>
  StoredReviewFinding.make({
    path: finding.path,
    startLine: finding.startLine,
    endLine: finding.endLine,
    severity: finding.severity,
    title: finding.title,
    body: finding.body.slice(0, 800),
  });

export const fromStoredFinding = (finding: StoredReviewFinding): ReviewFinding =>
  ReviewFinding.make({
    path: finding.path,
    startLine: finding.startLine,
    endLine: finding.endLine,
    severity: finding.severity,
    title: finding.title,
    body: finding.body,
  });

export const toStoredConcern = (concern: ReviewConcern): StoredReviewConcern =>
  StoredReviewConcern.make({
    severity: concern.severity,
    title: concern.title,
    body: concern.body.slice(0, 800),
  });

export const fromStoredConcern = (concern: StoredReviewConcern): ReviewConcern =>
  ReviewConcern.make({ severity: concern.severity, title: concern.title, body: concern.body });

const STATE_MARKER_PREFIX = "<!-- effect-agent-pr-review state-v1:";
const STATE_MARKER_SUFFIX = " -->";
const STATE_MARKER_PATTERN =
  /(?:^|\n)<!-- effect-agent-pr-review state-v1:([A-Za-z0-9+/]+={0,2})\.([0-9a-f]{64}) -->$/;
const STATE_SIGNATURE_DOMAIN = "effect-agent-pr-review/state-v1\u0000";

const encodeReviewState = (state: ReviewState): string => {
  const encoded = Schema.encodeSync(ReviewState)(state);
  return Encoding.encodeBase64(JSON.stringify(encoded));
};

const hmacKey = (secret: Redacted.Redacted<string>) =>
  globalThis.crypto.subtle.importKey(
    "raw",
    new TextEncoder().encode(Redacted.value(secret)),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign", "verify"],
  );

const signatureBytes = (signature: string): ArrayBuffer => {
  const pairs = signature.match(/../g) ?? [];
  const buffer = new ArrayBuffer(pairs.length);
  const bytes = new Uint8Array(buffer);
  for (let index = 0; index < pairs.length; index += 1) {
    bytes[index] = Number.parseInt(pairs[index] ?? "", 16);
  }
  return buffer;
};

/** Encode and HMAC-authenticate one schema-validated terminal state marker. */
export const renderReviewStateMarker = (
  state: ReviewState,
  secret: Redacted.Redacted<string>,
): Effect.Effect<string> => {
  const payload = encodeReviewState(state);
  const message = new TextEncoder().encode(`${STATE_SIGNATURE_DOMAIN}${payload}`);
  return Effect.promise(async () => {
    const signature = await globalThis.crypto.subtle.sign("HMAC", await hmacKey(secret), message);
    const hex = Array.from(new Uint8Array(signature))
      .map((byte) => byte.toString(16).padStart(2, "0"))
      .join("");
    return `${STATE_MARKER_PREFIX}${payload}.${hex}${STATE_MARKER_SUFFIX}`;
  });
};

/**
 * Verify and decode the terminal state marker. Invalid, injected, unsigned, or
 * differently keyed input is ignored so the caller safely performs a full review.
 */
export const extractReviewState = (
  body: string,
  secret: Redacted.Redacted<string>,
): Effect.Effect<ReviewState | undefined> => {
  const match = STATE_MARKER_PATTERN.exec(body);
  const payload = match?.[1];
  const signature = match?.[2];
  if (payload === undefined || signature === undefined) return Effect.succeed(undefined);
  const json = Result.getOrUndefined(Encoding.decodeBase64String(payload));
  if (json === undefined) return Effect.succeed(undefined);
  const decoded = Schema.decodeUnknownOption(Schema.fromJsonString(ReviewState))(json);
  if (Option.isNone(decoded)) return Effect.succeed(undefined);
  const message = new TextEncoder().encode(`${STATE_SIGNATURE_DOMAIN}${payload}`);
  return Effect.promise(async () => {
    const valid = await globalThis.crypto.subtle.verify(
      "HMAC",
      await hmacKey(secret),
      signatureBytes(signature),
      message,
    );
    return valid ? decoded.value : undefined;
  });
};

/** The bounded result of GitHub's previous-head...current-head comparison. */
export class ReviewHeadComparison extends Schema.Class<ReviewHeadComparison>(
  "@effect-agent/pr-review/ReviewHeadComparison",
)({
  status: Schema.Literals(["ahead", "behind", "diverged", "identical"]),
  baseSha: GitCommitSha,
  headSha: GitCommitSha,
  mergeBaseSha: GitCommitSha,
  files: Schema.Array(ChangedFile).check(Schema.isMaxLength(300)),
  /** GitHub caps compare-file payloads at 300; equality is conservatively truncated. */
  truncated: Schema.Boolean,
}) {}

/** Internal review selection applied as a decorator over the full PR source. */
export interface ReviewSelection {
  readonly mode: ReviewScopeMode;
  readonly reason: string;
  readonly files: ReadonlyArray<ChangedFile>;
  /** Paths whose changes invalidate prior findings, including paths reverted out of the PR. */
  readonly affectedPaths: ReadonlyArray<string>;
  readonly totalFiles: number;
  readonly baselineSha: string | undefined;
  readonly priorState: ReviewState | undefined;
  readonly profileFingerprint: string;
  /** Host-only key used to authenticate the next bounded continuity marker. */
  readonly stateSecret?: Redacted.Redacted<string> | undefined;
}

const fullSelection = (input: {
  readonly reason: string;
  readonly files: ReadonlyArray<ChangedFile>;
  readonly totalFiles: number;
  readonly profileFingerprint: string;
}): ReviewSelection => ({
  mode: "full",
  reason: input.reason,
  files: input.files,
  affectedPaths: input.files.flatMap((file) =>
    file.previousPath === undefined ? [file.path] : [file.path, file.previousPath],
  ),
  totalFiles: input.totalFiles,
  baselineSha: undefined,
  priorState: undefined,
  profileFingerprint: input.profileFingerprint,
});

/**
 * Validate that persisted state belongs to this exact PR/base lineage and the
 * same review profile. A mismatch is a full-review reason, never an error that
 * silently suppresses review work.
 */
export const validateReviewState = (
  state: ReviewState,
  current: PullRequestMetadata,
  profileFingerprint: string,
): string | undefined => {
  if (state.repository !== current.repository || state.pullRequestNumber !== current.number) {
    return "stored state belongs to a different pull request";
  }
  if (current.baseSha === undefined) return "the current base commit is unavailable";
  if (state.baseRef !== current.baseRef) return "the pull request base ref changed";
  if (state.headRef !== current.headRef) return "the pull request head ref changed";
  if (state.profileFingerprint !== profileFingerprint) {
    return "the reviewer profile or model configuration changed";
  }
  return undefined;
};

/** Pure, deterministic range selection with conservative full-review fallbacks. */
export const selectReviewRange = (input: {
  readonly requestedMode: ReviewMode;
  readonly current: PullRequestMetadata;
  readonly fullFiles: ReadonlyArray<ChangedFile>;
  readonly profileFingerprint: string;
  readonly priorState: ReviewState | undefined;
  readonly comparison: ReviewHeadComparison | undefined;
  readonly baseComparison?: ReviewHeadComparison | undefined;
  readonly lookupFailure?: string | undefined;
}): ReviewSelection => {
  const full = (reason: string) =>
    fullSelection({
      reason,
      files: input.fullFiles,
      totalFiles: input.current.totalChangedFiles,
      profileFingerprint: input.profileFingerprint,
    });
  if (input.requestedMode === "final") return full("explicit final full-diff audit requested");
  if (input.lookupFailure !== undefined) {
    return full(`stored review state could not be recovered: ${input.lookupFailure}`);
  }
  if (input.priorState === undefined) return full("no compatible stored review state was found");
  const invalid = validateReviewState(input.priorState, input.current, input.profileFingerprint);
  if (invalid !== undefined) return full(invalid);
  const comparison = input.comparison;
  if (comparison === undefined) return full("the incremental head comparison was unavailable");
  if (
    comparison.baseSha !== input.priorState.reviewedHeadSha ||
    comparison.headSha !== input.current.headSha ||
    comparison.mergeBaseSha !== input.priorState.reviewedHeadSha ||
    (comparison.status !== "ahead" && comparison.status !== "identical")
  ) {
    return full("the prior reviewed head is not an ancestor of the current head");
  }
  if (comparison.truncated) return full("the incremental comparison exceeded GitHub's file bound");
  const affectedPaths = new Set(
    comparison.files.flatMap((file) =>
      file.previousPath === undefined ? [file.path] : [file.path, file.previousPath],
    ),
  );
  let baseReason = "";
  if (input.priorState.baseSha !== input.current.baseSha) {
    const baseComparison = input.baseComparison;
    if (baseComparison === undefined) {
      return full("the pull request base changed and its lineage comparison was unavailable");
    }
    if (
      baseComparison.baseSha !== input.priorState.baseSha ||
      baseComparison.headSha !== input.current.baseSha ||
      baseComparison.mergeBaseSha !== input.priorState.baseSha ||
      (baseComparison.status !== "ahead" && baseComparison.status !== "identical") ||
      baseComparison.truncated
    ) {
      return full("the pull request base changed materially or exceeded the comparison bound");
    }
    for (const file of baseComparison.files) {
      affectedPaths.add(file.path);
      if (file.previousPath !== undefined) affectedPaths.add(file.previousPath);
    }
    baseReason = `; base advanced from ${input.priorState.baseSha.slice(0, 7)} and overlapping PR paths were included`;
  }
  const currentPaths = new Set(
    input.fullFiles.flatMap((file) =>
      file.previousPath === undefined ? [file.path] : [file.path, file.previousPath],
    ),
  );
  const selectedByPath = new Map<string, ChangedFile>();
  for (const file of comparison.files) {
    if (
      currentPaths.has(file.path) ||
      (file.previousPath !== undefined && currentPaths.has(file.previousPath))
    ) {
      selectedByPath.set(file.path, file);
    }
  }
  if (input.priorState.baseSha !== input.current.baseSha) {
    for (const file of input.fullFiles) {
      if (
        affectedPaths.has(file.path) ||
        (file.previousPath !== undefined && affectedPaths.has(file.previousPath))
      ) {
        selectedByPath.set(file.path, file);
      }
    }
  }
  const selectedFiles = [...selectedByPath.values()].sort((left, right) =>
    left.path < right.path ? -1 : left.path > right.path ? 1 : 0,
  );
  return {
    mode: "incremental",
    reason: `changes since successfully reviewed head ${input.priorState.reviewedHeadSha.slice(0, 7)}${baseReason}`,
    files: selectedFiles,
    affectedPaths: [...affectedPaths].sort(),
    totalFiles: selectedFiles.length,
    baselineSha: input.priorState.reviewedHeadSha,
    priorState: input.priorState,
    profileFingerprint: input.profileFingerprint,
  };
};

/** Per-run context consumed by orchestration and publication, not by the model. */
export class ReviewExecutionContext extends Context.Service<
  ReviewExecutionContext,
  ReviewSelection
>()("@effect-agent/pr-review/ReviewExecutionContext") {}

/**
 * Decorate the full source with the selected review range. Full anchor files
 * remain available to host-side publication validation; model tools see only
 * the selected delta and may read head context only for that delta's paths.
 */
export const selectedPullRequestSourceLayer = (
  selection: ReviewSelection,
): Layer.Layer<PullRequestSource, never, PullRequestSource> =>
  Layer.effect(PullRequestSource)(
    Effect.gen(function* () {
      const source = yield* PullRequestSource;
      const selectedPaths = new Set(selection.files.map((file) => file.path));
      return PullRequestSource.of({
        metadata: source.metadata,
        changedFiles: Effect.succeed(selection.files),
        anchorFiles: source.anchorFiles,
        readFile: (path) =>
          selectedPaths.has(path)
            ? source.readFile(path)
            : Effect.fail(
                ReviewInputViolation.make({
                  input: path,
                  reason: "Path is outside this incremental review range.",
                }),
              ),
      });
    }),
  );

/** Profile fingerprints are SHA-256 over configuration-only signatures. */
export const computeProfileFingerprint = (signature: string): Effect.Effect<string> =>
  Effect.promise(async () => {
    const digest = await globalThis.crypto.subtle.digest(
      "SHA-256",
      new TextEncoder().encode(signature),
    );
    return Array.from(new Uint8Array(digest))
      .map((byte) => byte.toString(16).padStart(2, "0"))
      .join("");
  });

/** Build the full-surface mission used only to resolve profile guidance. */
export const buildProfileMission = (
  metadata: PullRequestMetadata,
  files: ReadonlyArray<ChangedFile>,
): ReviewMission =>
  ReviewMission.make({
    repository: metadata.repository,
    number: metadata.number,
    title: metadata.title,
    body: metadata.body,
    baseRef: metadata.baseRef,
    headRef: metadata.headRef,
    changedFileCount: files.length,
  });

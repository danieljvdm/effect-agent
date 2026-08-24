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

/** A compact unresolved non-anchored concern with its invalidation paths. */
export class StoredReviewConcern extends Schema.Class<StoredReviewConcern>(
  "@effect-agent/pr-review/StoredReviewConcern",
)({
  /** Absent only on legacy state written before concern path binding. */
  evidencePaths: Schema.optionalKey(
    Schema.Array(ChangedPath).check(Schema.isMinLength(1)).check(Schema.isMaxLength(3)),
  ),
  severity: FindingSeverity,
  title: Schema.NonEmptyString.check(Schema.isMaxLength(120)),
  body: StoredText,
}) {}

/** How a maintainer settled a previously raised finding or concern. */
export const AdjudicationDisposition = Schema.Literals(["accepted-risk", "refuted", "obsolete"]);
export type AdjudicationDisposition = typeof AdjudicationDisposition.Type;

/** The adjudications bound carried by the ReviewState schema. */
export const MAX_STORED_ADJUDICATIONS = 20;

/**
 * One maintainer adjudication of a finding or concern identity. Anchored
 * findings carry their full location identity; unanchored concerns are
 * identified by title alone, so the location fields stay absent.
 */
const StoredAdjudicationFields = Schema.Struct({
  path: Schema.optionalKey(ChangedPath),
  startLine: Schema.optionalKey(Schema.Int.check(Schema.isGreaterThan(0))),
  endLine: Schema.optionalKey(Schema.Int.check(Schema.isGreaterThan(0))),
  title: Schema.NonEmptyString.check(Schema.isMaxLength(120)),
  disposition: AdjudicationDisposition,
  reason: Schema.optionalKey(Schema.NonEmptyString.check(Schema.isMaxLength(300))),
  /** GitHub login of the maintainer whose comment adjudicated the identity. */
  actor: Schema.NonEmptyString.check(Schema.isMaxLength(100)),
}).check(
  Schema.makeFilter(
    (adjudication) => {
      const locationParts = [
        adjudication.path,
        adjudication.startLine,
        adjudication.endLine,
      ].filter((part) => part !== undefined).length;
      return locationParts === 0 || locationParts === 3
        ? undefined
        : "path, startLine, and endLine must be either all present or all absent";
    },
    { title: "adjudication locations are complete or unanchored" },
  ),
);

export class StoredAdjudication extends Schema.Class<StoredAdjudication>(
  "@effect-agent/pr-review/StoredAdjudication",
)(StoredAdjudicationFields) {}

/**
 * The one finding-identity composition shared by retirement, adjudication,
 * and settlement. A tagged JSON tuple keeps anchored findings in a namespace
 * disjoint from title-only concerns and remains unambiguous even when
 * untrusted path or title text contains delimiter characters.
 */
export const findingIdentity = (finding: {
  readonly path: string;
  readonly startLine: number;
  readonly endLine: number;
  readonly title: string;
}): string =>
  JSON.stringify(["finding", finding.path, finding.startLine, finding.endLine, finding.title]);

/** The disjoint title-only identity namespace for unanchored concerns. */
export const concernIdentity = (concern: { readonly title: string }): string =>
  JSON.stringify(["concern", concern.title]);

/**
 * An adjudication's identity: the shared finding identity when anchored, the
 * disjoint concern identity when unanchored.
 */
export const adjudicationIdentity = (adjudication: StoredAdjudication): string =>
  adjudication.path !== undefined &&
  adjudication.startLine !== undefined &&
  adjudication.endLine !== undefined
    ? findingIdentity({
        path: adjudication.path,
        startLine: adjudication.startLine,
        endLine: adjudication.endLine,
        title: adjudication.title,
      })
    : concernIdentity(adjudication);

/** The carried-scope bound; a run that cannot fit its leftovers publishes no state. */
export const MAX_STORED_UNREVIEWED_PATHS = 100;

/** Failed-pass records stored beside the leftover paths; one per unit stage. */
export const MAX_STORED_UNREVIEWED_PASSES = 24;

/** Stages a leftover path may need retried on the next incremental run. */
export const UnreviewedStage = Schema.Literals(["discovery", "specialist", "verification"]);
export type UnreviewedStage = typeof UnreviewedStage.Type;

/** One failed fan-out pass whose stage remains attached to its exact paths. */
export class StoredUnreviewedPass extends Schema.Class<StoredUnreviewedPass>(
  "@effect-agent/pr-review/StoredUnreviewedPass",
)({
  stage: UnreviewedStage,
  paths: Schema.Array(ChangedPath).check(Schema.isMinLength(1)).check(Schema.isMaxLength(12)),
}) {}

/**
 * Versioned state embedded after EVERY completed run that can be signed. The
 * head plus full-scope fingerprint forms an incremental baseline; an absent
 * unresolved item never means the path is defect-free. `unreviewedPaths`
 * carries retryable review gaps (failed passes) forward so the next
 * incremental run re-reviews exactly them plus the new delta — the baseline
 * advances monotonically instead of freezing on one flaky pass and reopening
 * the whole post-baseline scope. Storing hundreds of path strings separately
 * would not fit GitHub's bounded review body in the worst case.
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
  settledScopeFingerprint: Fingerprint,
  reviewedPathCount: Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 300 })),
  unresolvedFindings: Schema.Array(StoredReviewFinding).check(Schema.isMaxLength(20)),
  unresolvedConcerns: Schema.Array(StoredReviewConcern).check(Schema.isMaxLength(10)),
  /** Retryable review gaps carried into the next incremental run's scope. */
  unreviewedPaths: Schema.Array(ChangedPath).check(Schema.isMaxLength(MAX_STORED_UNREVIEWED_PATHS)),
  /** Which failed pass produced those leftovers. */
  unreviewedPasses: Schema.Array(StoredUnreviewedPass).check(
    Schema.isMaxLength(MAX_STORED_UNREVIEWED_PASSES),
  ),
  /**
   * True only when the producing run had complete input coverage, no
   * unsettled pass, and nothing carried. Skip-unchanged authority: an
   * unchanged patch may skip re-review only over a settled state.
   */
  settled: Schema.Boolean,
  lastReviewMode: ReviewScopeMode,
  /**
   * Maintainer adjudications standing against this pull request. optionalKey
   * so state markers signed before the field existed still decode.
   */
  adjudications: Schema.optionalKey(
    Schema.Array(StoredAdjudication).check(Schema.isMaxLength(MAX_STORED_ADJUDICATIONS)),
  ),
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
    ...(concern.evidencePaths === undefined ? {} : { evidencePaths: concern.evidencePaths }),
    severity: concern.severity,
    title: concern.title,
    body: concern.body.slice(0, 800),
  });

export const fromStoredConcern = (concern: StoredReviewConcern): ReviewConcern =>
  ReviewConcern.make({
    ...(concern.evidencePaths === undefined ? {} : { evidencePaths: concern.evidencePaths }),
    severity: concern.severity,
    title: concern.title,
    body: concern.body,
  });

const STATE_MARKER_PREFIX = "<!-- effect-agent-pr-review state-v1:";
const STATE_MARKER_SUFFIX = " -->";
const STATE_MARKER_PATTERN =
  /(?:^|\n)<!-- effect-agent-pr-review state-v1:([A-Za-z0-9+/]+={0,2})\.([0-9a-f]{64}) -->$/;
const STATE_SIGNATURE_DOMAIN = "effect-agent-pr-review/state-v1\u0000";
export const MAX_REVIEW_STATE_MARKER_CHARS = 24_000;
export const ReviewStateMarker = Schema.NonEmptyString.check(
  Schema.isMaxLength(MAX_REVIEW_STATE_MARKER_CHARS),
  Schema.isPattern(/^<!-- effect-agent-pr-review state-v1:[A-Za-z0-9+/]+={0,2}\.[0-9a-f]{64} -->$/),
).pipe(Schema.brand("@effect-agent/pr-review/ReviewStateMarker"));
export type ReviewStateMarker = typeof ReviewStateMarker.Type;

export class ReviewStateAuthenticationFailure extends Schema.TaggedError<ReviewStateAuthenticationFailure>()(
  "ReviewStateAuthenticationFailure",
  {
    operation: Schema.Literals(["sign", "verify"]),
    reason: Schema.NonEmptyString.check(Schema.isMaxLength(2_048)),
  },
) {}

export class ReviewStateMarkerTooLarge extends Schema.TaggedError<ReviewStateMarkerTooLarge>()(
  "ReviewStateMarkerTooLarge",
  {
    observedChars: Schema.Int.check(Schema.isGreaterThan(0)),
    maximumChars: Schema.Int.check(Schema.isGreaterThan(0)),
  },
) {}

export class ReviewStateAuthenticator extends Context.Service<
  ReviewStateAuthenticator,
  {
    readonly status: "available" | "unavailable";
    readonly unavailableReason: string | undefined;
    readonly render: (
      state: ReviewState,
    ) => Effect.Effect<
      ReviewStateMarker,
      ReviewStateAuthenticationFailure | ReviewStateMarkerTooLarge
    >;
    readonly extract: (
      body: string,
    ) => Effect.Effect<Option.Option<ReviewState>, ReviewStateAuthenticationFailure>;
  }
>()("@effect-agent/pr-review/ReviewStateAuthenticator") {}

const authenticationFailure = (
  operation: "sign" | "verify",
  cause: unknown,
): ReviewStateAuthenticationFailure =>
  ReviewStateAuthenticationFailure.make({
    operation,
    reason: String(cause).slice(0, 2_048),
  });

const hmacKey = (secret: Redacted.Redacted<string>, operation: "sign" | "verify") =>
  Effect.tryPromise({
    try: () =>
      globalThis.crypto.subtle.importKey(
        "raw",
        new TextEncoder().encode(Redacted.value(secret)),
        { name: "HMAC", hash: "SHA-256" },
        false,
        ["sign", "verify"],
      ),
    catch: (cause) => authenticationFailure(operation, cause),
  });

const signatureBytes = (signature: string): ArrayBuffer => {
  const pairs = signature.match(/../g) ?? [];
  const buffer = new ArrayBuffer(pairs.length);
  const bytes = new Uint8Array(buffer);
  for (let index = 0; index < pairs.length; index += 1) {
    bytes[index] = Number.parseInt(pairs[index] ?? "", 16);
  }
  return buffer;
};

/** Validated WebCrypto adapter selected at the Action composition root. */
export const webCryptoReviewStateAuthenticatorLayer = (
  secret: Redacted.Redacted<string>,
): Layer.Layer<ReviewStateAuthenticator> =>
  Layer.succeed(ReviewStateAuthenticator)(
    ReviewStateAuthenticator.of({
      status: "available",
      unavailableReason: undefined,
      render: (state) =>
        Effect.gen(function* () {
          const json = yield* Schema.encodeUnknownEffect(Schema.fromJsonString(ReviewState))(
            state,
          ).pipe(Effect.mapError((cause) => authenticationFailure("sign", cause)));
          const payload = Encoding.encodeBase64(json);
          const message = new TextEncoder().encode(`${STATE_SIGNATURE_DOMAIN}${payload}`);
          const key = yield* hmacKey(secret, "sign");
          const signature = yield* Effect.tryPromise({
            try: () => globalThis.crypto.subtle.sign("HMAC", key, message),
            catch: (cause) => authenticationFailure("sign", cause),
          });
          const hex = Array.from(new Uint8Array(signature))
            .map((byte) => byte.toString(16).padStart(2, "0"))
            .join("");
          const marker = `${STATE_MARKER_PREFIX}${payload}.${hex}${STATE_MARKER_SUFFIX}`;
          if (marker.length > MAX_REVIEW_STATE_MARKER_CHARS) {
            return yield* ReviewStateMarkerTooLarge.make({
              observedChars: marker.length,
              maximumChars: MAX_REVIEW_STATE_MARKER_CHARS,
            });
          }
          return yield* Schema.decodeUnknownEffect(ReviewStateMarker)(marker).pipe(
            Effect.mapError((cause) => authenticationFailure("sign", cause)),
          );
        }),
      extract: (body) => {
        if (body.length > 60_000) return Effect.succeed(Option.none());
        const match = STATE_MARKER_PATTERN.exec(body);
        const payload = match?.[1];
        const signature = match?.[2];
        if (payload === undefined || signature === undefined) return Effect.succeed(Option.none());
        const marker = `${STATE_MARKER_PREFIX}${payload}.${signature}${STATE_MARKER_SUFFIX}`;
        if (!Schema.is(ReviewStateMarker)(marker)) return Effect.succeed(Option.none());
        const json = Result.getOrUndefined(Encoding.decodeBase64String(payload));
        if (json === undefined) return Effect.succeed(Option.none());
        const decoded = Schema.decodeUnknownOption(Schema.fromJsonString(ReviewState))(json);
        if (Option.isNone(decoded)) return Effect.succeed(Option.none());
        const message = new TextEncoder().encode(`${STATE_SIGNATURE_DOMAIN}${payload}`);
        return Effect.gen(function* () {
          const key = yield* hmacKey(secret, "verify");
          const valid = yield* Effect.tryPromise({
            try: () =>
              globalThis.crypto.subtle.verify("HMAC", key, signatureBytes(signature), message),
            catch: (cause) => authenticationFailure("verify", cause),
          });
          return valid ? Option.some(decoded.value) : Option.none();
        });
      },
    }),
  );

/** Explicit no-state implementation for hosts without a stable authentication secret. */
export const unavailableReviewStateAuthenticatorLayer = (
  reason: string,
): Layer.Layer<ReviewStateAuthenticator> => {
  const safeReason = reason === "" ? "review-state authentication is unavailable" : reason;
  return Layer.succeed(ReviewStateAuthenticator)(
    ReviewStateAuthenticator.of({
      status: "unavailable",
      unavailableReason: safeReason.slice(0, 1_000),
      render: () =>
        Effect.fail(
          ReviewStateAuthenticationFailure.make({
            operation: "sign",
            reason: safeReason.slice(0, 2_048),
          }),
        ),
      extract: () => Effect.succeed(Option.none()),
    }),
  );
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
  /**
   * Failed stages attached to the unchanged paths that own them. Verification
   * retries reopen discovery for only their paths because candidates are not
   * persisted in review state.
   */
  readonly retryPasses?: ReadonlyArray<StoredUnreviewedPass>;
  /** Flattened summaries retained for diagnostics and compatibility. */
  readonly retryPaths: ReadonlyArray<string>;
  readonly retryStages: ReadonlyArray<UnreviewedStage>;
  readonly totalFiles: number;
  readonly baselineSha: string | undefined;
  readonly priorState: ReviewState | undefined;
  /** Absent only for an explicit full review with no continuity profile. */
  readonly profileFingerprint: string | undefined;
  /** Action-owned authentication capability, constructed at the composition root. */
  readonly stateAuthenticator?: ReviewStateAuthenticator["Service"] | undefined;
}

export const fullReviewSelection = (input: {
  readonly reason: string;
  readonly files: ReadonlyArray<ChangedFile>;
  readonly totalFiles: number;
  readonly profileFingerprint?: string | undefined;
}): ReviewSelection => ({
  mode: "full",
  reason: input.reason,
  files: input.files,
  affectedPaths: input.files.flatMap((file) =>
    file.previousPath === undefined ? [file.path] : [file.path, file.previousPath],
  ),
  retryPasses: [],
  retryPaths: [],
  retryStages: [],
  totalFiles: input.totalFiles,
  baselineSha: undefined,
  priorState: undefined,
  profileFingerprint: input.profileFingerprint,
});

/** Three-dot lineage from the reviewed head to the current head is usable. */
export const isLineageAncestor = (
  comparison: ReviewHeadComparison,
  priorState: ReviewState,
  currentHeadSha: string,
): boolean =>
  comparison.baseSha === priorState.reviewedHeadSha &&
  comparison.headSha === currentHeadSha &&
  comparison.mergeBaseSha === priorState.reviewedHeadSha &&
  !comparison.truncated &&
  (comparison.status === "ahead" || comparison.status === "identical");

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
  if (state.unresolvedConcerns.some((concern) => concern.evidencePaths === undefined)) {
    return "stored concerns predate affected-path tracking";
  }
  return undefined;
};

const filePaths = (file: ChangedFile): ReadonlyArray<string> =>
  file.previousPath === undefined ? [file.path] : [file.path, file.previousPath];

const incrementalFromDelta = (input: {
  readonly current: PullRequestMetadata;
  readonly fullFiles: ReadonlyArray<ChangedFile>;
  readonly profileFingerprint: string;
  readonly priorState: ReviewState;
  readonly deltaFiles: ReadonlyArray<ChangedFile>;
  readonly extraAffectedPaths?: ReadonlyArray<string> | undefined;
  readonly reason: string;
}): ReviewSelection => {
  const currentPaths = new Set(input.fullFiles.flatMap(filePaths));
  const affectedPaths = new Set([
    ...input.deltaFiles.flatMap(filePaths),
    ...(input.extraAffectedPaths ?? []),
  ]);
  const initialAffectedCount = affectedPaths.size;
  // Reopen every current path needed to reassess a concern touched by this
  // delta. Repeat to a fixed point because two concerns may overlap on a path.
  let expanded = true;
  while (expanded) {
    expanded = false;
    for (const concern of input.priorState.unresolvedConcerns) {
      const paths = concern.evidencePaths ?? [];
      if (!paths.some((path) => affectedPaths.has(path))) continue;
      for (const path of paths) {
        if (!affectedPaths.has(path)) {
          affectedPaths.add(path);
          expanded = true;
        }
      }
    }
  }
  const selectedByPath = new Map<string, ChangedFile>();
  for (const file of input.deltaFiles) {
    if (
      currentPaths.has(file.path) ||
      (file.previousPath !== undefined && currentPaths.has(file.previousPath))
    ) {
      selectedByPath.set(file.path, file);
    }
  }
  const carriedPaths = input.priorState.unreviewedPaths.filter((path) => currentPaths.has(path));
  const retryOnly = new Set<string>();
  for (const path of carriedPaths) {
    if (affectedPaths.has(path)) continue;
    retryOnly.add(path);
  }

  const retryPathsByStage = new Map<UnreviewedStage, Set<string>>();
  const representedRetryPaths = new Set<string>();
  for (const pass of input.priorState.unreviewedPasses) {
    for (const path of pass.paths) {
      if (!retryOnly.has(path)) continue;
      const paths = retryPathsByStage.get(pass.stage) ?? new Set<string>();
      paths.add(path);
      retryPathsByStage.set(pass.stage, paths);
      representedRetryPaths.add(path);
    }
  }
  // Some continuity gaps (capacity overflow, partial evidence, legacy state)
  // have no failed-pass record. They conservatively re-enter fresh discovery
  // instead of inheriting another path's unrelated failed stage.
  for (const path of retryOnly) {
    if (representedRetryPaths.has(path)) continue;
    retryOnly.delete(path);
    affectedPaths.add(path);
  }
  const retryPasses = (["discovery", "specialist", "verification"] as const).flatMap((stage) => {
    const paths = [...(retryPathsByStage.get(stage) ?? [])]
      .filter((path) => retryOnly.has(path))
      .sort();
    return Array.from({ length: Math.ceil(paths.length / 12) }, (_, index) =>
      StoredUnreviewedPass.make({
        stage,
        paths: paths.slice(index * 12, (index + 1) * 12),
      }),
    );
  });
  const retryPaths = [...retryOnly].sort();
  const retryStages = [...new Set(retryPasses.map((pass) => pass.stage))];
  for (const file of input.fullFiles) {
    const needed =
      affectedPaths.has(file.path) ||
      (file.previousPath !== undefined && affectedPaths.has(file.previousPath)) ||
      retryOnly.has(file.path) ||
      (file.previousPath !== undefined && retryOnly.has(file.previousPath));
    if (needed) selectedByPath.set(file.path, file);
  }
  const selectedFiles = [...selectedByPath.values()].sort((left, right) =>
    left.path < right.path ? -1 : left.path > right.path ? 1 : 0,
  );
  const leftoverCount = retryPaths.length;
  const carriedReason =
    leftoverCount > 0
      ? `; retrying ${leftoverCount} unchanged leftover path(s) by recorded failed stage`
      : carriedPaths.length > 0
        ? `; retrying ${carriedPaths.length} carried unreviewed path(s)`
        : "";
  const concernPathCount = affectedPaths.size - initialAffectedCount;
  const concernReason =
    concernPathCount === 0
      ? ""
      : `; reopening ${concernPathCount} related concern path(s) for context`;
  return {
    mode: "incremental",
    reason: `${input.reason}${carriedReason}${concernReason}`,
    files: selectedFiles,
    affectedPaths: [...affectedPaths].sort(),
    retryPasses,
    retryPaths,
    retryStages,
    totalFiles: selectedFiles.length,
    baselineSha: input.priorState.reviewedHeadSha,
    priorState: input.priorState,
    profileFingerprint: input.profileFingerprint,
  };
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
  /**
   * Two-dot tree comparison used when the reviewed head is not a git ancestor
   * (rebase, amend, force-push). Intersected with the current PR path set so
   * main-drift outside the pull request never re-enters scope.
   */
  readonly contentComparison?: ReviewHeadComparison | undefined;
  readonly lookupFailure?: string | undefined;
}): ReviewSelection => {
  const full = (reason: string) =>
    fullReviewSelection({
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
  if (
    comparison !== undefined &&
    isLineageAncestor(comparison, input.priorState, input.current.headSha)
  ) {
    const extraAffected: Array<string> = [];
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
      for (const file of baseComparison.files) extraAffected.push(...filePaths(file));
      baseReason = `; base advanced from ${input.priorState.baseSha.slice(0, 7)} and overlapping PR paths were included`;
    }
    return incrementalFromDelta({
      current: input.current,
      fullFiles: input.fullFiles,
      profileFingerprint: input.profileFingerprint,
      priorState: input.priorState,
      deltaFiles: comparison.files,
      extraAffectedPaths: extraAffected,
      reason: `changes since reviewed head ${input.priorState.reviewedHeadSha.slice(0, 7)}${baseReason}`,
    });
  }
  const contentComparison = input.contentComparison;
  if (contentComparison !== undefined && !contentComparison.truncated) {
    return incrementalFromDelta({
      current: input.current,
      fullFiles: input.fullFiles,
      profileFingerprint: input.profileFingerprint,
      priorState: input.priorState,
      deltaFiles: contentComparison.files,
      reason: `rewritten history; contents changed since reviewed head ${input.priorState.reviewedHeadSha.slice(0, 7)}`,
    });
  }
  if (comparison === undefined) return full("the incremental head comparison was unavailable");
  if (comparison.truncated) return full("the incremental comparison exceeded GitHub's file bound");
  return full("the prior reviewed head is not an ancestor of the current head");
};

/** Per-run context consumed by orchestration and publication, not by the model. */
export class ReviewExecutionContext extends Context.Service<
  ReviewExecutionContext,
  ReviewSelection
>()("@effect-agent/pr-review/ReviewExecutionContext") {}

/**
 * Explicit direct-run adapter for callers that intentionally review the full
 * source without authenticated incremental continuity.
 */
export const fullReviewExecutionContextLayer = (reason: string) =>
  Layer.effect(
    ReviewExecutionContext,
    Effect.gen(function* () {
      const source = yield* PullRequestSource;
      const [metadata, files] = yield* Effect.all([source.metadata, source.changedFiles]);
      return fullReviewSelection({ reason, files, totalFiles: metadata.totalChangedFiles });
    }),
  );

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
      const selectedFiles = source.changedFiles.pipe(
        Effect.map((fullFiles) => {
          const fullByPath = new Map(fullFiles.map((file) => [file.path, file] as const));
          return selection.files.map((file) => {
            if (file.patch !== undefined) return file;
            const full = fullByPath.get(file.path);
            return full === undefined
              ? file
              : ChangedFile.make({
                  ...file,
                  ...(full.reviewBaseContent === undefined
                    ? {}
                    : { reviewBaseContent: full.reviewBaseContent }),
                  ...(full.reviewHeadContent === undefined
                    ? {}
                    : { reviewHeadContent: full.reviewHeadContent }),
                });
          });
        }),
      );
      return PullRequestSource.of({
        metadata: source.metadata,
        changedFiles: selectedFiles,
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

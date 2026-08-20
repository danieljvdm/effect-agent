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
export class StoredAdjudication extends Schema.Class<StoredAdjudication>(
  "@effect-agent/pr-review/StoredAdjudication",
)({
  path: Schema.optionalKey(ChangedPath),
  startLine: Schema.optionalKey(Schema.Int.check(Schema.isGreaterThan(0))),
  endLine: Schema.optionalKey(Schema.Int.check(Schema.isGreaterThan(0))),
  title: Schema.NonEmptyString.check(Schema.isMaxLength(120)),
  disposition: AdjudicationDisposition,
  reason: Schema.optionalKey(Schema.NonEmptyString.check(Schema.isMaxLength(300))),
  /** GitHub login of the maintainer whose comment adjudicated the identity. */
  actor: Schema.NonEmptyString.check(Schema.isMaxLength(100)),
}) {}

/**
 * The one finding-identity composition shared by retirement, adjudication,
 * and settlement: path, startLine, endLine, and title joined with NUL. An
 * adjudication suppresses exactly this identity — a materially different
 * finding (different title or anchor) at the same location is untouched.
 */
export const findingIdentity = (finding: {
  readonly path: string;
  readonly startLine: number;
  readonly endLine: number;
  readonly title: string;
}): string =>
  `${finding.path}\u0000${finding.startLine}\u0000${finding.endLine}\u0000${finding.title}`;

/**
 * An adjudication's identity: the shared finding identity when anchored, the
 * title alone for an unanchored concern.
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
    : adjudication.title;

/** The carried-scope bound; a run that cannot fit its leftovers publishes no state. */
export const MAX_STORED_UNREVIEWED_PATHS = 100;

/** Failed-pass records stored beside the leftover paths; one per unit stage. */
export const MAX_STORED_UNREVIEWED_PASSES = 24;

/** Stages a leftover path may need retried without a second general discovery. */
export const UnreviewedStage = Schema.Literals(["discovery", "specialist", "verification"]);
export type UnreviewedStage = typeof UnreviewedStage.Type;

/** One failed fan-out pass whose paths should be retried, not rediscovered. */
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
   * Leftover paths whose contents did not change. Fan-out retries only the
   * recorded failed stages on these paths and keeps their stored findings.
   */
  readonly retryPaths: ReadonlyArray<string>;
  readonly retryStages: ReadonlyArray<UnreviewedStage>;
  readonly totalFiles: number;
  readonly baselineSha: string | undefined;
  readonly priorState: ReviewState | undefined;
  readonly profileFingerprint: string;
  /** Action-owned authentication capability, constructed at the composition root. */
  readonly stateAuthenticator?: ReviewStateAuthenticator["Service"] | undefined;
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
  const retryStages = new Set<UnreviewedStage>();
  for (const path of carriedPaths) {
    if (affectedPaths.has(path)) continue;
    retryOnly.add(path);
    for (const pass of input.priorState.unreviewedPasses) {
      if (pass.paths.includes(path)) retryStages.add(pass.stage);
    }
  }
  if (
    retryStages.has("verification") &&
    !retryStages.has("discovery") &&
    !retryStages.has("specialist")
  ) {
    retryStages.add("discovery");
    retryStages.add("specialist");
  }
  if (retryOnly.size > 0 && retryStages.size === 0) {
    for (const path of retryOnly) affectedPaths.add(path);
    retryStages.add("discovery");
    retryStages.add("specialist");
    retryStages.add("verification");
  }
  if (carriedPaths.length > 0 || (input.extraAffectedPaths?.length ?? 0) > 0) {
    for (const file of input.fullFiles) {
      const needed =
        affectedPaths.has(file.path) ||
        (file.previousPath !== undefined && affectedPaths.has(file.previousPath)) ||
        retryOnly.has(file.path) ||
        (file.previousPath !== undefined && retryOnly.has(file.previousPath));
      if (needed) selectedByPath.set(file.path, file);
    }
  }
  const selectedFiles = [...selectedByPath.values()].sort((left, right) =>
    left.path < right.path ? -1 : left.path > right.path ? 1 : 0,
  );
  const leftoverCount = [...retryOnly].filter((path) => !affectedPaths.has(path)).length;
  const carriedReason =
    leftoverCount > 0
      ? `; retrying ${leftoverCount} unchanged leftover path(s) without rediscovery`
      : carriedPaths.length > 0
        ? `; retrying ${carriedPaths.length} carried unreviewed path(s)`
        : "";
  return {
    mode: "incremental",
    reason: `${input.reason}${carriedReason}`,
    files: selectedFiles,
    affectedPaths: [...affectedPaths].sort(),
    retryPaths: [...retryOnly].filter((path) => !affectedPaths.has(path)).sort(),
    retryStages: [...retryStages].sort(),
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

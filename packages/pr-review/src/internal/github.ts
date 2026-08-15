import type { Redacted } from "effect";
import { Context, Effect, Layer, Option, Schema } from "effect";
import { HttpClient, HttpClientRequest, HttpClientResponse } from "effect/unstable/http";

import { ChangedFile } from "./diff.ts";
import { extractFingerprint } from "./fingerprint.ts";
import type { ReviewPublicationPlan } from "./render.ts";
import { extractReviewState, ReviewHeadComparison, type ReviewState } from "./review-state.ts";
import {
  MAX_CHANGED_FILES,
  MAX_FILE_CHARS,
  normalizeRepoRelativePath,
  PullRequestMetadata,
  PullRequestSource,
  PullRequestSourceFailure,
  ReviewInputViolation,
} from "./source.ts";

// ---------------------------------------------------------------------------
// GitHub REST adapters for the PullRequestSource port and the ReviewPublisher.
// Wire payloads are decoded through minimal Schemas — never asserted — and
// every upstream fault becomes the typed PullRequestSourceFailure /
// GitHubApiFailure instead of an untyped defect.
// ---------------------------------------------------------------------------

/** Which pull request to review and how to reach the API. */
export class GitHubReviewTarget extends Context.Service<
  GitHubReviewTarget,
  {
    /** API root, e.g. `https://api.github.com` (no trailing slash). */
    readonly apiUrl: string;
    /** `owner/name`. */
    readonly repository: string;
    readonly number: number;
    /** Absent token means unauthenticated reads (public repositories only). */
    readonly token: Option.Option<Redacted.Redacted<string>>;
  }
>()("@effect-agent/pr-review/GitHubReviewTarget") {
  static layer(config: {
    readonly apiUrl: string;
    readonly repository: string;
    readonly number: number;
    readonly token: Option.Option<Redacted.Redacted<string>>;
  }): Layer.Layer<GitHubReviewTarget> {
    return Layer.succeed(this, GitHubReviewTarget.of(config));
  }
}

/** A GitHub API call failed: transport, status, or payload decode. */
export class GitHubApiFailure extends Schema.TaggedError<GitHubApiFailure>()("GitHubApiFailure", {
  operation: Schema.String,
  reason: Schema.String,
}) {
  override get message() {
    return `GitHub API operation '${this.operation}' failed: ${this.reason}`;
  }
}

// --- Wire schemas (decode-only, minimal fields) ------------------------------

const GitHubPullRequestWire = Schema.Struct({
  number: Schema.Int,
  title: Schema.String,
  body: Schema.NullOr(Schema.String),
  changed_files: Schema.Int,
  base: Schema.Struct({ ref: Schema.String, sha: Schema.String }),
  head: Schema.Struct({ ref: Schema.String, sha: Schema.String }),
});

const GitHubFileWire = Schema.Struct({
  filename: Schema.String,
  status: Schema.String,
  additions: Schema.Int,
  deletions: Schema.Int,
  patch: Schema.optionalKey(Schema.String),
  previous_filename: Schema.optionalKey(Schema.String),
});

const GitHubFilesPageWire = Schema.Array(GitHubFileWire);

const GitHubReviewWire = Schema.Struct({
  id: Schema.Int,
  html_url: Schema.String,
});

/** The publication receipt callers report back to the operator. */
export class PublishedReview extends Schema.Class<PublishedReview>(
  "@effect-agent/pr-review/PublishedReview",
)({
  reviewId: Schema.Int,
  url: Schema.String,
  event: Schema.String,
  inlineComments: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
}) {}

/** Posts one planned review; the ONLY mutating operation in this package. */
export class ReviewPublisher extends Context.Service<
  ReviewPublisher,
  {
    readonly publish: (
      plan: ReviewPublicationPlan,
    ) => Effect.Effect<PublishedReview, GitHubApiFailure>;
  }
>()("@effect-agent/pr-review/ReviewPublisher") {}

// --- Shared request plumbing -------------------------------------------------

const FILE_STATUSES = new Set([
  "added",
  "removed",
  "modified",
  "renamed",
  "copied",
  "changed",
  "unchanged",
]);

const withCommonHeaders = (
  request: HttpClientRequest.HttpClientRequest,
  token: Option.Option<Redacted.Redacted<string>>,
): HttpClientRequest.HttpClientRequest => {
  const base = request.pipe(
    HttpClientRequest.setHeaders({
      "X-GitHub-Api-Version": "2022-11-28",
      "User-Agent": "effect-agent-pr-review",
    }),
  );
  return Option.isSome(token) ? base.pipe(HttpClientRequest.bearerToken(token.value)) : base;
};

const failWith =
  (operation: string) =>
  (error: { readonly _tag: string; readonly message?: string }): PullRequestSourceFailure =>
    PullRequestSourceFailure.make({
      operation,
      reason: `${error._tag}: ${error.message ?? "request failed"}`.slice(0, 2_048),
    });

const decodeJsonBody = <S extends Schema.Top>(schema: S, operation: string) => {
  const decode = Schema.decodeUnknownEffect(schema);
  return (
    response: HttpClientResponse.HttpClientResponse,
  ): Effect.Effect<S["Type"], PullRequestSourceFailure, S["DecodingServices"]> =>
    response.json.pipe(
      Effect.mapError(failWith(operation)),
      Effect.flatMap((body) => decode(body).pipe(Effect.mapError(failWith(operation)))),
    );
};

const executeOk = (
  operation: string,
  request: HttpClientRequest.HttpClientRequest,
): Effect.Effect<
  HttpClientResponse.HttpClientResponse,
  PullRequestSourceFailure,
  HttpClient.HttpClient
> =>
  HttpClient.execute(request).pipe(
    Effect.flatMap(HttpClientResponse.filterStatusOk),
    Effect.mapError(failWith(operation)),
  );

const toChangedFile = (wire: typeof GitHubFileWire.Type): ChangedFile =>
  ChangedFile.make({
    path: wire.filename,
    status: FILE_STATUSES.has(wire.status) ? (wire.status as ChangedFile["status"]) : "changed",
    additions: wire.additions,
    deletions: wire.deletions,
    ...(wire.previous_filename !== undefined ? { previousPath: wire.previous_filename } : {}),
    ...(wire.patch !== undefined ? { patch: wire.patch } : {}),
  });

// --- Live PullRequestSource --------------------------------------------------

/**
 * GitHub-backed PullRequestSource. Metadata and the changeset are fetched
 * once per Layer build and cached: the pull request is reviewed as one
 * consistent snapshot even if the branch moves mid-run.
 */
export const gitHubPullRequestSourceLayer: Layer.Layer<
  PullRequestSource,
  never,
  GitHubReviewTarget | HttpClient.HttpClient
> = Layer.effect(PullRequestSource)(
  Effect.gen(function* () {
    const target = yield* GitHubReviewTarget;
    const client = yield* HttpClient.HttpClient;
    const prefix = `${target.apiUrl}/repos/${target.repository}/pulls/${target.number}`;

    const fetchMetadata = executeOk(
      "getPullRequest",
      withCommonHeaders(
        HttpClientRequest.get(prefix).pipe(HttpClientRequest.acceptJson),
        target.token,
      ),
    ).pipe(
      Effect.flatMap(decodeJsonBody(GitHubPullRequestWire, "getPullRequest")),
      Effect.map((wire) =>
        PullRequestMetadata.make({
          repository: target.repository,
          number: wire.number,
          title: wire.title.slice(0, 400),
          body: (wire.body ?? "").slice(0, 20_000),
          baseRef: wire.base.ref,
          baseSha: wire.base.sha,
          headRef: wire.head.ref,
          headSha: wire.head.sha,
          totalChangedFiles: wire.changed_files,
        }),
      ),
    );

    const fetchFiles = Effect.gen(function* () {
      const perPage = 100;
      const all: Array<ChangedFile> = [];
      for (let page = 1; page <= MAX_CHANGED_FILES / perPage; page += 1) {
        const response = yield* executeOk(
          "listChangedFiles",
          withCommonHeaders(
            HttpClientRequest.get(`${prefix}/files`).pipe(
              HttpClientRequest.acceptJson,
              HttpClientRequest.setUrlParams({
                per_page: String(perPage),
                page: String(page),
              }),
            ),
            target.token,
          ),
        );
        const wires = yield* decodeJsonBody(GitHubFilesPageWire, "listChangedFiles")(response);
        all.push(...wires.map(toChangedFile));
        if (wires.length < perPage) break;
      }
      return all as ReadonlyArray<ChangedFile>;
    });

    const metadata = yield* Effect.cached(
      fetchMetadata.pipe(Effect.provideService(HttpClient.HttpClient, client)),
    );
    const changedFiles = yield* Effect.cached(
      fetchFiles.pipe(Effect.provideService(HttpClient.HttpClient, client)),
    );

    const readFile = (path: string) =>
      Effect.gen(function* () {
        const relative = yield* normalizeRepoRelativePath(path);
        const files = yield* changedFiles;
        if (!files.some((file) => file.path === relative)) {
          return yield* ReviewInputViolation.make({
            input: relative,
            reason: "Path is not part of this pull request's changeset.",
          });
        }
        const head = yield* metadata;
        const encodedPath = relative.split("/").map(encodeURIComponent).join("/");
        const response = yield* executeOk(
          "readFile",
          withCommonHeaders(
            HttpClientRequest.get(
              `${target.apiUrl}/repos/${target.repository}/contents/${encodedPath}`,
            ).pipe(
              HttpClientRequest.accept("application/vnd.github.raw+json"),
              HttpClientRequest.setUrlParams({ ref: head.headSha }),
            ),
            target.token,
          ),
        ).pipe(Effect.provideService(HttpClient.HttpClient, client));
        const text = yield* response.text.pipe(Effect.mapError(failWith("readFile")));
        if (text.length > MAX_FILE_CHARS) {
          return yield* ReviewInputViolation.make({
            input: relative,
            reason: `File is larger than the ${MAX_FILE_CHARS}-character read bound.`,
          });
        }
        return text;
      });

    return PullRequestSource.of({ metadata, changedFiles, anchorFiles: changedFiles, readFile });
  }),
);

// --- Live ReviewPublisher ------------------------------------------------------

/** GitHub-backed publisher: one POST to the pull-request reviews endpoint. */
export const gitHubReviewPublisherLayer: Layer.Layer<
  ReviewPublisher,
  never,
  GitHubReviewTarget | HttpClient.HttpClient
> = Layer.effect(ReviewPublisher)(
  Effect.gen(function* () {
    const target = yield* GitHubReviewTarget;
    const client = yield* HttpClient.HttpClient;
    return ReviewPublisher.of({
      publish: (plan) =>
        Effect.gen(function* () {
          const payload = {
            event: plan.event,
            body: plan.body,
            commit_id: plan.commitSha,
            comments: plan.comments.map((comment) => ({
              path: comment.path,
              line: comment.line,
              side: "RIGHT",
              ...(comment.startLine !== undefined
                ? { start_line: comment.startLine, start_side: "RIGHT" }
                : {}),
              body: comment.body,
            })),
          };
          const request = withCommonHeaders(
            HttpClientRequest.post(
              `${target.apiUrl}/repos/${target.repository}/pulls/${target.number}/reviews`,
            ).pipe(HttpClientRequest.acceptJson, HttpClientRequest.bodyJsonUnsafe(payload)),
            target.token,
          );
          const wire = yield* HttpClient.execute(request).pipe(
            Effect.flatMap(HttpClientResponse.filterStatusOk),
            Effect.flatMap((response) =>
              response.json.pipe(Effect.flatMap(Schema.decodeUnknownEffect(GitHubReviewWire))),
            ),
            Effect.mapError((error) =>
              GitHubApiFailure.make({
                operation: "createReview",
                reason: `${error._tag}: ${error.message ?? "request failed"}`.slice(0, 2_048),
              }),
            ),
            Effect.provideService(HttpClient.HttpClient, client),
          );
          return PublishedReview.make({
            reviewId: wire.id,
            url: wire.html_url,
            event: plan.event,
            inlineComments: plan.comments.length,
          });
        }),
    });
  }),
);

// --- Prior reviews (fingerprint deduplication) ---------------------------------

/** Reading the pull request's previously posted reviews failed. */
export class PriorReviewLookupFailure extends Schema.TaggedError<PriorReviewLookupFailure>()(
  "PriorReviewLookupFailure",
  {
    reason: Schema.String,
  },
) {
  override get message() {
    return `Prior-review lookup failed: ${this.reason}`;
  }
}

/**
 * Read-only view of this package's previously posted reviews on the target
 * pull request — the deduplication state for unchanged-changeset skipping.
 */
export class PriorReviews extends Context.Service<
  PriorReviews,
  {
    /** The fingerprint embedded in the most recent marker-bearing review. */
    readonly latestFingerprint: Effect.Effect<Option.Option<string>, PriorReviewLookupFailure>;
    /** The latest authenticated, successfully covered review state marker. */
    readonly latestState: (
      secret: Redacted.Redacted<string>,
    ) => Effect.Effect<Option.Option<ReviewState>, PriorReviewLookupFailure>;
    /** Compare a previously reviewed head to the live current head. */
    readonly compareHeads: (
      baseSha: string,
      headSha: string,
    ) => Effect.Effect<ReviewHeadComparison, PriorReviewLookupFailure>;
  }
>()("@effect-agent/pr-review/PriorReviews") {}

const GitHubPriorReviewWire = Schema.Struct({
  body: Schema.NullOr(Schema.String),
  commit_id: Schema.String,
  user: Schema.optionalKey(
    Schema.NullOr(
      Schema.Struct({
        login: Schema.String,
        type: Schema.String,
      }),
    ),
  ),
});
const GitHubPriorReviewsPageWire = Schema.Array(GitHubPriorReviewWire);

const GitHubCompareWire = Schema.Struct({
  status: Schema.Literals(["ahead", "behind", "diverged", "identical"]),
  base_commit: Schema.Struct({ sha: Schema.String }),
  merge_base_commit: Schema.Struct({ sha: Schema.String }),
  files: GitHubFilesPageWire,
});

/** Reviews are paged chronologically; scanning stays bounded. */
const MAX_PRIOR_REVIEW_PAGES = 5;

/** GitHub-backed PriorReviews over the pull-request reviews endpoint. */
export const gitHubPriorReviewsLayer: Layer.Layer<
  PriorReviews,
  never,
  GitHubReviewTarget | HttpClient.HttpClient
> = Layer.effect(PriorReviews)(
  Effect.gen(function* () {
    const target = yield* GitHubReviewTarget;
    const client = yield* HttpClient.HttpClient;
    const prefix = `${target.apiUrl}/repos/${target.repository}/pulls/${target.number}`;
    const decodePage = Schema.decodeUnknownEffect(GitHubPriorReviewsPageWire);
    const asLookupFailure = (error: { readonly _tag: string; readonly message?: string }) =>
      PriorReviewLookupFailure.make({
        reason: `${error._tag}: ${error.message ?? "request failed"}`.slice(0, 2_048),
      });
    const readMarkers = (secret: Option.Option<Redacted.Redacted<string>>) =>
      Effect.gen(function* () {
        const perPage = 100;
        let latest = Option.none<string>();
        let latestState = Option.none<ReviewState>();
        for (let page = 1; page <= MAX_PRIOR_REVIEW_PAGES; page += 1) {
          const response = yield* HttpClient.execute(
            withCommonHeaders(
              HttpClientRequest.get(`${prefix}/reviews`).pipe(
                HttpClientRequest.acceptJson,
                HttpClientRequest.setUrlParams({
                  per_page: String(perPage),
                  page: String(page),
                }),
              ),
              target.token,
            ),
          ).pipe(
            Effect.flatMap(HttpClientResponse.filterStatusOk),
            Effect.mapError(asLookupFailure),
          );
          const wires = yield* response.json.pipe(
            Effect.mapError(asLookupFailure),
            Effect.flatMap((body) => decodePage(body).pipe(Effect.mapError(asLookupFailure))),
          );
          for (const wire of wires) {
            // State controls what required scope may be omitted. The author gate
            // rejects user prose; the terminal marker is additionally HMAC
            // authenticated so another bot workflow or model text cannot forge it.
            if (wire.user?.login !== "github-actions[bot]" || wire.user.type !== "Bot") continue;
            const fingerprint = extractFingerprint(wire.body ?? "");
            if (fingerprint !== undefined) latest = Option.some(fingerprint);
            if (Option.isSome(secret)) {
              const state = yield* extractReviewState(wire.body ?? "", secret.value);
              if (state !== undefined && state.reviewedHeadSha === wire.commit_id) {
                latestState = Option.some(state);
              }
            }
          }
          if (wires.length < perPage) break;
          if (page === MAX_PRIOR_REVIEW_PAGES) {
            return yield* PriorReviewLookupFailure.make({
              reason: `review history exceeds the bounded ${MAX_PRIOR_REVIEW_PAGES * perPage}-review lookup`,
            });
          }
        }
        return { latestFingerprint: latest, latestState };
      }).pipe(Effect.provideService(HttpClient.HttpClient, client));
    const compareHeads = (baseSha: string, headSha: string) =>
      Effect.gen(function* () {
        const response = yield* HttpClient.execute(
          withCommonHeaders(
            HttpClientRequest.get(
              `${target.apiUrl}/repos/${target.repository}/compare/${encodeURIComponent(baseSha)}...${encodeURIComponent(headSha)}`,
            ).pipe(HttpClientRequest.acceptJson),
            target.token,
          ),
        ).pipe(Effect.flatMap(HttpClientResponse.filterStatusOk), Effect.mapError(asLookupFailure));
        const wire = yield* response.json.pipe(
          Effect.mapError(asLookupFailure),
          Effect.flatMap((body) =>
            Schema.decodeUnknownEffect(GitHubCompareWire)(body).pipe(
              Effect.mapError(asLookupFailure),
            ),
          ),
        );
        const files = wire.files.map(toChangedFile);
        return ReviewHeadComparison.make({
          status: wire.status,
          baseSha: wire.base_commit.sha,
          headSha,
          mergeBaseSha: wire.merge_base_commit.sha,
          files,
          truncated: files.length >= MAX_CHANGED_FILES,
        });
      }).pipe(Effect.provideService(HttpClient.HttpClient, client));
    return PriorReviews.of({
      latestFingerprint: readMarkers(Option.none()).pipe(
        Effect.map((markers) => markers.latestFingerprint),
      ),
      latestState: (secret) =>
        readMarkers(Option.some(secret)).pipe(Effect.map((markers) => markers.latestState)),
      compareHeads,
    });
  }),
);

/**
 * Whether the current fingerprint matches the most recent posted review.
 * Fails OPEN: a lookup fault means "not unchanged" — the review proceeds,
 * which is the safe direction for a deduplication optimization.
 */
export const fingerprintUnchanged = (
  current: string,
): Effect.Effect<boolean, never, PriorReviews> =>
  Effect.gen(function* () {
    const priorReviews = yield* PriorReviews;
    const latest = yield* priorReviews.latestFingerprint.pipe(
      Effect.orElseSucceed(() => Option.none<string>()),
    );
    return Option.isSome(latest) && latest.value === current;
  });

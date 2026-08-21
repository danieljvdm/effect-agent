import type { Redacted } from "effect";
import { Context, DateTime, Effect, Layer, Option, Schema } from "effect";
import { HttpClient, HttpClientRequest, HttpClientResponse } from "effect/unstable/http";

import {
  AdjudicableThread,
  AdjudicationComment,
  AUTHORIZED_ADJUDICATION_ASSOCIATIONS,
  MAX_THREAD_ADJUDICATION_COMMANDS,
  parseThreadAdjudication,
  ReviewAdjudicationFailure,
  ReviewAdjudicationHost,
} from "./adjudication.ts";
import { ChangedFile } from "./diff.ts";
import { extractFingerprint } from "./fingerprint.ts";
import type { ReviewPublicationPlan } from "./render.ts";
import {
  RetirableReview,
  RetirableReviewComment,
  ReviewRetirementFailure,
  ReviewRetirementHost,
} from "./retirement.ts";
import {
  ReviewHeadComparison,
  ReviewStateAuthenticator,
  type ReviewState,
} from "./review-state.ts";
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

const defaultGraphqlUrl = (apiUrl: string): string =>
  apiUrl === "https://api.github.com"
    ? "https://api.github.com/graphql"
    : apiUrl.replace(/\/api\/v3$/, "/api/graphql");

/** Which pull request to review and how to reach the API. */
export const DEFAULT_GITHUB_REVIEW_AUTHOR_LOGIN = "github-actions[bot]";

export class GitHubReviewTarget extends Context.Service<
  GitHubReviewTarget,
  {
    /** API root, e.g. `https://api.github.com` (no trailing slash). */
    readonly apiUrl: string;
    /** GraphQL root, e.g. `https://api.github.com/graphql`. */
    readonly graphqlUrl: string;
    /** `owner/name`. */
    readonly repository: string;
    readonly number: number;
    /** Absent token means unauthenticated reads (public repositories only). */
    readonly token: Option.Option<Redacted.Redacted<string>>;
    /** Bot login expected to author reviews posted with this target's token. */
    readonly reviewAuthorLogin?: string | undefined;
  }
>()("@effect-agent/pr-review/GitHubReviewTarget") {
  static layer(config: {
    readonly apiUrl: string;
    readonly graphqlUrl?: string | undefined;
    readonly repository: string;
    readonly number: number;
    readonly token: Option.Option<Redacted.Redacted<string>>;
    readonly reviewAuthorLogin?: string | undefined;
  }): Layer.Layer<GitHubReviewTarget> {
    return Layer.succeed(
      this,
      GitHubReviewTarget.of({
        ...config,
        graphqlUrl: config.graphqlUrl ?? defaultGraphqlUrl(config.apiUrl),
        reviewAuthorLogin: config.reviewAuthorLogin ?? DEFAULT_GITHUB_REVIEW_AUTHOR_LOGIN,
      }),
    );
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

const GitHubActorWire = Schema.Struct({ node_id: Schema.String });

const GitHubReviewWire = Schema.Struct({
  id: Schema.Int,
  html_url: Schema.String,
  user: Schema.NullOr(GitHubActorWire),
  submitted_at: Schema.NullOr(Schema.String),
});

const GitHubRetirableReviewWire = Schema.Struct({
  id: Schema.Int,
  body: Schema.NullOr(Schema.String),
  commit_id: Schema.String,
  user: Schema.NullOr(GitHubActorWire),
  submitted_at: Schema.NullOr(Schema.String),
});
const GitHubRetirableReviewsPageWire = Schema.Array(GitHubRetirableReviewWire);

const GitHubReviewCommentWire = Schema.Struct({
  node_id: Schema.String,
  path: Schema.String,
  body: Schema.String,
  // Outdated comments omit `line` entirely instead of sending null.
  line: Schema.optionalKey(Schema.NullOr(Schema.Int)),
  original_line: Schema.optionalKey(Schema.NullOr(Schema.Int)),
  start_line: Schema.optionalKey(Schema.NullOr(Schema.Int)),
  original_start_line: Schema.optionalKey(Schema.NullOr(Schema.Int)),
});
const GitHubReviewCommentsPageWire = Schema.Array(GitHubReviewCommentWire);

const GitHubMinimizeCommentWire = Schema.Struct({
  data: Schema.optionalKey(
    Schema.NullOr(
      Schema.Struct({
        minimizeComment: Schema.NullOr(
          Schema.Struct({
            minimizedComment: Schema.NullOr(Schema.Struct({ isMinimized: Schema.Boolean })),
          }),
        ),
      }),
    ),
  ),
  errors: Schema.optionalKey(Schema.Array(Schema.Struct({ message: Schema.String }))),
});

/** Decode GitHub's external timestamp before it participates in mutation ordering. */
export const parseGitHubSubmittedAt = (value: string | null): DateTime.Utc | null =>
  value === null ? null : Option.getOrNull(DateTime.make(value));

/** The publication receipt callers report back to the operator. */
export class PublishedReview extends Schema.Class<PublishedReview>(
  "@effect-agent/pr-review/PublishedReview",
)({
  reviewId: Schema.Int,
  url: Schema.String,
  event: Schema.String,
  inlineComments: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  /** Actor and ordering boundary returned by the create-review response. */
  authorNodeId: Schema.NullOr(Schema.NonEmptyString.check(Schema.isMaxLength(200))),
  submittedAt: Schema.NullOr(Schema.DateTimeUtc),
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
    const rawFiles = yield* Effect.cached(
      fetchFiles.pipe(Effect.provideService(HttpClient.HttpClient, client)),
    );

    const readRepositoryFile = (path: string, ref: string) =>
      Effect.gen(function* () {
        const relative = yield* normalizeRepoRelativePath(path);
        const encodedPath = relative.split("/").map(encodeURIComponent).join("/");
        const response = yield* executeOk(
          "readFile",
          withCommonHeaders(
            HttpClientRequest.get(
              `${target.apiUrl}/repos/${target.repository}/contents/${encodedPath}`,
            ).pipe(
              HttpClientRequest.accept("application/vnd.github.raw+json"),
              HttpClientRequest.setUrlParams({ ref }),
            ),
            target.token,
          ),
        ).pipe(Effect.provideService(HttpClient.HttpClient, client));
        const buffer = yield* response.arrayBuffer.pipe(Effect.mapError(failWith("readFile")));
        if (buffer.byteLength > MAX_FILE_CHARS) {
          return yield* ReviewInputViolation.make({
            input: relative,
            reason: `File is larger than the ${MAX_FILE_CHARS}-byte read bound.`,
          });
        }
        const text = yield* Effect.try({
          try: () => new TextDecoder("utf-8", { fatal: true }).decode(buffer),
          catch: () =>
            ReviewInputViolation.make({
              input: relative,
              reason: "File is not valid UTF-8 text.",
            }),
        });
        if (text.includes("\u0000")) {
          return yield* ReviewInputViolation.make({
            input: relative,
            reason: "File contains binary NUL bytes.",
          });
        }
        if (text.length > MAX_FILE_CHARS) {
          return yield* ReviewInputViolation.make({
            input: relative,
            reason: `File is larger than the ${MAX_FILE_CHARS}-character read bound.`,
          });
        }
        return text;
      });

    const changedFiles = yield* Effect.cached(
      Effect.gen(function* () {
        const [files, pullRequest] = yield* Effect.all([rawFiles, metadata]);
        return yield* Effect.forEach(
          files,
          (file) => {
            if (file.patch !== undefined) return Effect.succeed(file);
            const basePath = file.previousPath ?? file.path;
            const base =
              file.status === "added"
                ? Effect.succeed(Option.none<string>())
                : readRepositoryFile(basePath, pullRequest.baseSha ?? pullRequest.baseRef).pipe(
                    Effect.option,
                  );
            const head =
              file.status === "removed"
                ? Effect.succeed(Option.none<string>())
                : readRepositoryFile(file.path, pullRequest.headSha).pipe(Effect.option);
            return Effect.all({ base, head }).pipe(
              Effect.map(({ base, head }) =>
                ChangedFile.make({
                  ...file,
                  ...(Option.isSome(base) ? { reviewBaseContent: base.value } : {}),
                  ...(Option.isSome(head) ? { reviewHeadContent: head.value } : {}),
                }),
              ),
            );
          },
          { concurrency: 4 },
        );
      }),
    );

    const readFile = (path: string) =>
      Effect.gen(function* () {
        const relative = yield* normalizeRepoRelativePath(path);
        const files = yield* changedFiles;
        const file = files.find((candidate) => candidate.path === relative);
        if (file === undefined) {
          return yield* ReviewInputViolation.make({
            input: relative,
            reason: "Path is not part of this pull request's changeset.",
          });
        }
        if (file.reviewHeadContent !== undefined) return file.reviewHeadContent;
        const head = yield* metadata;
        return yield* readRepositoryFile(relative, head.headSha);
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
            authorNodeId: wire.user?.node_id ?? null,
            submittedAt: parseGitHubSubmittedAt(wire.submitted_at),
          });
        }),
    });
  }),
);

// --- Live ReviewRetirementHost ----------------------------------------------

const MAX_RETIREMENT_PAGES = 5;
const MINIMIZE_REVIEW_COMMENT_MUTATION = `mutation MinimizeReviewComment($subjectId: ID!) {
  minimizeComment(input: { subjectId: $subjectId, classifier: OUTDATED }) {
    minimizedComment { isMinimized }
  }
}`;

/** GitHub-backed host operations for cosmetic retirement after publication. */
export const gitHubReviewRetirementHostLayer: Layer.Layer<
  ReviewRetirementHost,
  never,
  GitHubReviewTarget | HttpClient.HttpClient
> = Layer.effect(ReviewRetirementHost)(
  Effect.gen(function* () {
    const target = yield* GitHubReviewTarget;
    const client = yield* HttpClient.HttpClient;
    const prefix = `${target.apiUrl}/repos/${target.repository}/pulls/${target.number}`;
    const asRetirementFailure =
      (operation: string) =>
      (error: { readonly _tag: string; readonly message?: string }): ReviewRetirementFailure =>
        ReviewRetirementFailure.make({
          operation,
          reason: `${error._tag}: ${error.message ?? "request failed"}`.slice(0, 2_048),
        });
    const executeRetirement = (operation: string, request: HttpClientRequest.HttpClientRequest) =>
      HttpClient.execute(request).pipe(
        Effect.flatMap(HttpClientResponse.filterStatusOk),
        Effect.mapError(asRetirementFailure(operation)),
        Effect.provideService(HttpClient.HttpClient, client),
      );
    const decodeRetirement = <S extends Schema.Top>(schema: S, operation: string) => {
      const decode = Schema.decodeUnknownEffect(schema);
      return (response: HttpClientResponse.HttpClientResponse) =>
        response.json.pipe(
          Effect.mapError(asRetirementFailure(operation)),
          Effect.flatMap((body) =>
            decode(body).pipe(Effect.mapError(asRetirementFailure(operation))),
          ),
        );
    };
    const listPaged = <A>(input: {
      readonly operation: string;
      readonly url: string;
      readonly decode: (
        response: HttpClientResponse.HttpClientResponse,
      ) => Effect.Effect<ReadonlyArray<A>, ReviewRetirementFailure>;
    }) =>
      Effect.gen(function* () {
        const values: Array<A> = [];
        const perPage = 100;
        for (let page = 1; page <= MAX_RETIREMENT_PAGES; page += 1) {
          const response = yield* executeRetirement(
            input.operation,
            withCommonHeaders(
              HttpClientRequest.get(input.url).pipe(
                HttpClientRequest.acceptJson,
                HttpClientRequest.setUrlParams({
                  per_page: String(perPage),
                  page: String(page),
                }),
              ),
              target.token,
            ),
          );
          const pageValues = yield* input.decode(response);
          values.push(...pageValues);
          if (pageValues.length < perPage) return values;
        }
        return yield* ReviewRetirementFailure.make({
          operation: input.operation,
          reason: `history exceeds the bounded ${MAX_RETIREMENT_PAGES * 100}-item lookup`,
        });
      });

    return ReviewRetirementHost.of({
      listReviews: listPaged({
        operation: "listReviewsForRetirement",
        url: `${prefix}/reviews`,
        decode: decodeRetirement(GitHubRetirableReviewsPageWire, "listReviewsForRetirement"),
      }).pipe(
        Effect.map((reviews) =>
          reviews.map((review) =>
            RetirableReview.make({
              reviewId: review.id,
              body: review.body ?? "",
              commitSha: review.commit_id,
              authorNodeId: review.user?.node_id ?? null,
              submittedAt: parseGitHubSubmittedAt(review.submitted_at),
            }),
          ),
        ),
      ),
      listComments: (reviewId) =>
        listPaged({
          operation: "listReviewCommentsForRetirement",
          url: `${prefix}/reviews/${reviewId}/comments`,
          decode: decodeRetirement(GitHubReviewCommentsPageWire, "listReviewCommentsForRetirement"),
        }).pipe(
          Effect.map((comments) =>
            comments.map((comment) => {
              const positiveLine = (value: number | null | undefined): number | null =>
                value !== undefined && value !== null && value > 0 ? value : null;
              const endLine = positiveLine(comment.line ?? comment.original_line);
              const startLine =
                positiveLine(comment.start_line ?? comment.original_start_line) ?? endLine;
              return RetirableReviewComment.make({
                nodeId: comment.node_id,
                path: comment.path,
                startLine,
                endLine,
                body: comment.body,
              });
            }),
          ),
        ),
      updateBody: (reviewId, body) =>
        executeRetirement(
          "updateReview",
          withCommonHeaders(
            HttpClientRequest.put(`${prefix}/reviews/${reviewId}`).pipe(
              HttpClientRequest.acceptJson,
              HttpClientRequest.bodyJsonUnsafe({ body }),
            ),
            target.token,
          ),
        ).pipe(Effect.asVoid),
      minimizeComment: (nodeId) =>
        Effect.gen(function* () {
          const response = yield* executeRetirement(
            "minimizeComment",
            withCommonHeaders(
              HttpClientRequest.post(target.graphqlUrl).pipe(
                HttpClientRequest.acceptJson,
                HttpClientRequest.bodyJsonUnsafe({
                  query: MINIMIZE_REVIEW_COMMENT_MUTATION,
                  variables: { subjectId: nodeId },
                }),
              ),
              target.token,
            ),
          );
          const wire = yield* decodeRetirement(
            GitHubMinimizeCommentWire,
            "minimizeComment",
          )(response);
          if (
            (wire.errors?.length ?? 0) > 0 ||
            wire.data?.minimizeComment?.minimizedComment?.isMinimized !== true
          ) {
            return yield* ReviewRetirementFailure.make({
              operation: "minimizeComment",
              reason:
                wire.errors
                  ?.map((error) => error.message)
                  .join("; ")
                  .slice(0, 2_048) ?? "GitHub did not confirm comment minimization",
            });
          }
        }),
    });
  }),
);

// --- Live ReviewAdjudicationHost ----------------------------------------------

const MAX_ADJUDICATION_PAGES = 5;

const GitHubThreadCommentWire = Schema.Struct({
  id: Schema.Int,
  in_reply_to_id: Schema.optionalKey(Schema.NullOr(Schema.Int)),
  path: Schema.String,
  body: Schema.String,
  author_association: Schema.optionalKey(Schema.NullOr(Schema.String)),
  user: Schema.NullOr(Schema.Struct({ login: Schema.String })),
  created_at: Schema.optionalKey(Schema.NullOr(Schema.String)),
  line: Schema.optionalKey(Schema.NullOr(Schema.Int)),
  original_line: Schema.optionalKey(Schema.NullOr(Schema.Int)),
  start_line: Schema.optionalKey(Schema.NullOr(Schema.Int)),
  original_start_line: Schema.optionalKey(Schema.NullOr(Schema.Int)),
});
const GitHubThreadCommentsPageWire = Schema.Array(GitHubThreadCommentWire);

const GitHubIssueCommentWire = Schema.Struct({
  body: Schema.optionalKey(Schema.NullOr(Schema.String)),
  author_association: Schema.optionalKey(Schema.NullOr(Schema.String)),
  user: Schema.NullOr(Schema.Struct({ login: Schema.String })),
  created_at: Schema.optionalKey(Schema.NullOr(Schema.String)),
});
const GitHubIssueCommentsPageWire = Schema.Array(GitHubIssueCommentWire);

const toAdjudicationComment = (
  wire: {
    readonly body?: string | null | undefined;
    readonly author_association?: string | null | undefined;
    readonly user: { readonly login: string } | null;
    readonly created_at?: string | null | undefined;
  },
  sourceOrder: number,
): AdjudicationComment | undefined => {
  // A comment without an attributable author cannot authorize anything —
  // skip it fail-closed rather than inventing an actor.
  const login = wire.user?.login;
  if (login === undefined || login.length === 0) return undefined;
  return AdjudicationComment.make({
    body: (wire.body ?? "").slice(0, 65_536),
    authorAssociation: (wire.author_association ?? "NONE").slice(0, 40),
    authorLogin: login.slice(0, 100),
    createdAt: parseGitHubSubmittedAt(wire.created_at ?? null),
    sourceOrder,
  });
};

/**
 * GitHub-backed host reads for maintainer adjudication, installed by
 * `gitHubReviewLayers` in `github-env.ts` at the public composition root: this action's own
 * inline finding threads (roots authored by the configured review author)
 * with their replies, and the pull request's top-level conversation comments.
 * Both listings are creation-ordered.
 */
export const gitHubReviewAdjudicationHostLayer: Layer.Layer<
  ReviewAdjudicationHost,
  never,
  GitHubReviewTarget | HttpClient.HttpClient
> = Layer.effect(ReviewAdjudicationHost)(
  Effect.gen(function* () {
    const target = yield* GitHubReviewTarget;
    const client = yield* HttpClient.HttpClient;
    const reviewAuthorLogin = (
      target.reviewAuthorLogin ?? DEFAULT_GITHUB_REVIEW_AUTHOR_LOGIN
    ).toLowerCase();
    const asAdjudicationFailure =
      (operation: string) =>
      (error: { readonly _tag: string; readonly message?: string }): ReviewAdjudicationFailure =>
        ReviewAdjudicationFailure.make({
          operation,
          reason: `${error._tag}: ${error.message ?? "request failed"}`.slice(0, 2_048),
        });
    const decodeAdjudication = <S extends Schema.Top>(schema: S, operation: string) => {
      const decode = Schema.decodeUnknownEffect(schema);
      return (response: HttpClientResponse.HttpClientResponse) =>
        response.json.pipe(
          Effect.mapError(asAdjudicationFailure(operation)),
          Effect.flatMap((body) =>
            decode(body).pipe(Effect.mapError(asAdjudicationFailure(operation))),
          ),
        );
    };
    const listPaged = <A>(input: {
      readonly operation: string;
      readonly url: string;
      readonly decode: (
        response: HttpClientResponse.HttpClientResponse,
      ) => Effect.Effect<ReadonlyArray<A>, ReviewAdjudicationFailure>;
    }) =>
      Effect.gen(function* () {
        const values: Array<A> = [];
        const perPage = 100;
        for (let page = 1; page <= MAX_ADJUDICATION_PAGES; page += 1) {
          const response = yield* HttpClient.execute(
            withCommonHeaders(
              HttpClientRequest.get(input.url).pipe(
                HttpClientRequest.acceptJson,
                HttpClientRequest.setUrlParams({
                  per_page: String(perPage),
                  page: String(page),
                  sort: "created",
                  direction: "asc",
                }),
              ),
              target.token,
            ),
          ).pipe(
            Effect.flatMap(HttpClientResponse.filterStatusOk),
            Effect.mapError(asAdjudicationFailure(input.operation)),
          );
          const pageValues = yield* input.decode(response);
          values.push(...pageValues);
          if (pageValues.length < perPage) return values;
        }
        return yield* ReviewAdjudicationFailure.make({
          operation: input.operation,
          reason: `history exceeds the bounded ${MAX_ADJUDICATION_PAGES * 100}-item lookup`,
        });
      }).pipe(Effect.provideService(HttpClient.HttpClient, client));

    const listFindingThreads = Effect.gen(function* () {
      const wires = yield* listPaged({
        operation: "listReviewCommentsForAdjudication",
        url: `${target.apiUrl}/repos/${target.repository}/pulls/${target.number}/comments`,
        decode: decodeAdjudication(
          GitHubThreadCommentsPageWire,
          "listReviewCommentsForAdjudication",
        ),
      });
      const positiveLine = (value: number | null | undefined): number | null =>
        value !== undefined && value !== null && value > 0 ? value : null;
      const threads = new Map<
        number,
        { readonly root: (typeof wires)[number]; readonly replies: Array<AdjudicationComment> }
      >();
      for (const wire of wires) {
        if (wire.in_reply_to_id !== undefined && wire.in_reply_to_id !== null) continue;
        // Only this action's own finding threads can be adjudicated inline.
        if (wire.user?.login.toLowerCase() !== reviewAuthorLogin) continue;
        threads.set(wire.id, { root: wire, replies: [] });
      }
      for (const [sourceOrder, wire] of wires.entries()) {
        if (wire.in_reply_to_id === undefined || wire.in_reply_to_id === null) continue;
        const thread = threads.get(wire.in_reply_to_id);
        if (thread === undefined) continue;
        const reply = toAdjudicationComment(wire, sourceOrder);
        if (reply === undefined) continue;
        const command = parseThreadAdjudication(reply.body);
        if (command === undefined) continue;
        if (!AUTHORIZED_ADJUDICATION_ASSOCIATIONS.has(reply.authorAssociation)) {
          yield* Effect.logDebug(
            `Ignored inline adjudication command from @${reply.authorLogin} (${reply.authorAssociation}).`,
          );
          continue;
        }
        if (thread.replies.length >= MAX_THREAD_ADJUDICATION_COMMANDS) {
          return yield* ReviewAdjudicationFailure.make({
            operation: "listReviewCommentsForAdjudication",
            reason: `inline thread ${wire.in_reply_to_id} exceeds the bounded ${MAX_THREAD_ADJUDICATION_COMMANDS}-command adjudication lookup`,
          });
        }
        thread.replies.push(reply);
      }
      return [...threads.values()]
        .filter((thread) => thread.root.path.length > 0 && thread.root.path.length <= 500)
        .map(({ root, replies }) => {
          const endLine = positiveLine(root.line ?? root.original_line);
          const startLine = positiveLine(root.start_line ?? root.original_start_line) ?? endLine;
          return AdjudicableThread.make({
            path: root.path,
            startLine,
            endLine,
            rootBody: root.body.slice(0, 65_536),
            replies,
          });
        });
    });

    const listIssueComments = Effect.gen(function* () {
      const wires = yield* listPaged({
        operation: "listIssueCommentsForAdjudication",
        url: `${target.apiUrl}/repos/${target.repository}/issues/${target.number}/comments`,
        decode: decodeAdjudication(GitHubIssueCommentsPageWire, "listIssueCommentsForAdjudication"),
      });
      return wires.flatMap((wire, sourceOrder) => {
        const comment = toAdjudicationComment(wire, sourceOrder);
        return comment === undefined ? [] : [comment];
      });
    });

    return ReviewAdjudicationHost.of({ listFindingThreads, listIssueComments });
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
    readonly latestState: Effect.Effect<
      Option.Option<ReviewState>,
      PriorReviewLookupFailure,
      ReviewStateAuthenticator
    >;
    /** Compare a previously reviewed head to the live current head. */
    readonly compareHeads: (
      baseSha: string,
      headSha: string,
    ) => Effect.Effect<ReviewHeadComparison, PriorReviewLookupFailure>;
    /**
     * Two-dot tree comparison (`base..head`). Used when the reviewed head is
     * not a git ancestor so a rebase or amend can still name the paths whose
     * blob contents actually changed.
     */
    readonly compareTrees: (
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
    const reviewAuthorLogin = target.reviewAuthorLogin ?? DEFAULT_GITHUB_REVIEW_AUTHOR_LOGIN;
    const decodePage = Schema.decodeUnknownEffect(GitHubPriorReviewsPageWire);
    const asLookupFailure = (error: { readonly _tag: string; readonly message?: string }) =>
      PriorReviewLookupFailure.make({
        reason: `${error._tag}: ${error.message ?? "request failed"}`.slice(0, 2_048),
      });
    const readMarkers = (authenticator: Option.Option<ReviewStateAuthenticator["Service"]>) =>
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
            // State controls what required scope may be omitted. Match the bot
            // identity that posts with this target's token; the terminal marker
            // is additionally HMAC authenticated so another workflow or model
            // text cannot forge it.
            if (
              wire.user?.login.toLowerCase() !== reviewAuthorLogin.toLowerCase() ||
              wire.user.type !== "Bot"
            ) {
              continue;
            }
            const fingerprint = extractFingerprint(wire.body ?? "");
            if (fingerprint !== undefined) latest = Option.some(fingerprint);
            if (Option.isSome(authenticator)) {
              const state = yield* authenticator.value.extract(wire.body ?? "").pipe(
                Effect.mapError((error) =>
                  PriorReviewLookupFailure.make({
                    reason: `${error._tag}: ${error.reason}`.slice(0, 2_048),
                  }),
                ),
              );
              if (Option.isSome(state) && state.value.reviewedHeadSha === wire.commit_id) {
                latestState = state;
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
    const compareCommits = (baseSha: string, headSha: string, separator: "..." | "..") =>
      Effect.gen(function* () {
        const response = yield* HttpClient.execute(
          withCommonHeaders(
            HttpClientRequest.get(
              `${target.apiUrl}/repos/${target.repository}/compare/${encodeURIComponent(baseSha)}${separator}${encodeURIComponent(headSha)}`,
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
    const compareTrees = (baseSha: string, headSha: string) =>
      compareCommits(baseSha, headSha, "..").pipe(
        Effect.map((comparison) =>
          ReviewHeadComparison.make({
            // This is a content snapshot, not a lineage claim. Selection
            // intersects these files with the current PR path set.
            status: comparison.status === "identical" ? "identical" : "ahead",
            baseSha,
            headSha,
            mergeBaseSha: baseSha,
            files: comparison.files,
            truncated: comparison.truncated,
          }),
        ),
      );
    return PriorReviews.of({
      latestFingerprint: readMarkers(Option.none()).pipe(
        Effect.map((markers) => markers.latestFingerprint),
      ),
      latestState: Effect.gen(function* () {
        const authenticator = yield* ReviewStateAuthenticator;
        return yield* readMarkers(Option.some(authenticator)).pipe(
          Effect.map((markers) => markers.latestState),
        );
      }),
      compareHeads: (baseSha, headSha) => compareCommits(baseSha, headSha, "..."),
      compareTrees,
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

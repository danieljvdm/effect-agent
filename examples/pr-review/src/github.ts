import type { Redacted } from "effect";
import { Context, Effect, Layer, Option, Ref, Schema } from "effect";
import { HttpClient, HttpClientRequest, HttpClientResponse } from "effect/unstable/http";

import { ChangedFile } from "./diff.ts";
import type { ReviewPublicationPlan } from "./render.ts";
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
>()("@effect-agent/example-pr-review/GitHubReviewTarget") {
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
export class GitHubApiFailure extends Schema.TaggedErrorClass<GitHubApiFailure>()(
  "GitHubApiFailure",
  {
    operation: Schema.String,
    reason: Schema.String,
  },
) {
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
  base: Schema.Struct({ ref: Schema.String }),
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
  "@effect-agent/example-pr-review/PublishedReview",
)({
  reviewId: Schema.Int,
  url: Schema.String,
  event: Schema.String,
  inlineComments: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
}) {}

/** Posts one planned review; the ONLY mutating operation in this example. */
export class ReviewPublisher extends Context.Service<
  ReviewPublisher,
  {
    readonly publish: (
      plan: ReviewPublicationPlan,
    ) => Effect.Effect<PublishedReview, GitHubApiFailure>;
  }
>()("@effect-agent/example-pr-review/ReviewPublisher") {}

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
      "User-Agent": "effect-agent-example-pr-review",
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

    return PullRequestSource.of({ metadata, changedFiles, readFile });
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

// --- Collecting ReviewPublisher (tests and dry-run assertions) ----------------

/** In-memory publisher: records every plan and mints a deterministic receipt. */
export const collectingReviewPublisherLayer = (
  published: Ref.Ref<ReadonlyArray<ReviewPublicationPlan>>,
): Layer.Layer<ReviewPublisher> =>
  Layer.succeed(ReviewPublisher)(
    ReviewPublisher.of({
      publish: (plan) =>
        Ref.update(published, (plans) => [...plans, plan]).pipe(
          Effect.flatMap(() => Ref.get(published)),
          Effect.map((plans) =>
            PublishedReview.make({
              reviewId: plans.length,
              url: `memory://review/${plans.length}`,
              event: plan.event,
              inlineComments: plan.comments.length,
            }),
          ),
        ),
    }),
  );

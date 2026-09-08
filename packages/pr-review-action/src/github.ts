import { ReviewFollowUp } from "@effect-agent/pr-review/Review";
import { createTwoFilesPatch } from "diff";
import type { Redacted } from "effect";
import { Clock, DateTime, Effect, Encoding, Option, Result, Schema } from "effect";
import { HttpClient, HttpClientRequest, HttpClientResponse } from "effect/unstable/http";

import { unresolvedChangeRequests, type ReviewHistoryItem } from "./selection.ts";

const ShortString = Schema.String.check(Schema.isMaxLength(2_048));
const Revision = Schema.NonEmptyString.check(Schema.isMaxLength(128));

const PullRequestWire = Schema.Struct({
  number: Schema.Int.check(Schema.isGreaterThan(0)),
  title: Schema.String.check(Schema.isMaxLength(1_000)),
  body: Schema.NullOr(Schema.String.check(Schema.isMaxLength(100_000))),
  draft: Schema.Boolean,
  html_url: ShortString,
  base: Schema.Struct({ sha: Revision }),
  head: Schema.Struct({ sha: Revision }),
});

const ChangedFileWire = Schema.Struct({
  filename: Schema.NonEmptyString.check(Schema.isMaxLength(1_024)),
  previous_filename: Schema.optionalKey(Schema.NonEmptyString.check(Schema.isMaxLength(1_024))),
  status: Schema.NonEmptyString.check(Schema.isMaxLength(64)),
  additions: Schema.Natural,
  deletions: Schema.Natural,
  patch: Schema.optionalKey(Schema.String.check(Schema.isMaxLength(2_000_000))),
});

const ReviewWire = Schema.Struct({
  id: Schema.Natural,
  body: Schema.NullOr(Schema.String.check(Schema.isMaxLength(100_000))),
  commit_id: Schema.NullOr(Revision),
  submitted_at: Schema.NullOr(Schema.String.check(Schema.isMaxLength(128))),
  state: Schema.Literals(["APPROVED", "CHANGES_REQUESTED", "COMMENTED", "DISMISSED", "PENDING"]),
  user: Schema.Struct({
    login: Schema.NonEmptyString.check(Schema.isMaxLength(256)),
    type: Schema.NonEmptyString.check(Schema.isMaxLength(64)),
  }),
});

const ReviewCommentWire = Schema.Struct({
  pull_request_review_id: Schema.Natural,
  path: Schema.NonEmptyString.check(Schema.isMaxLength(1_024)),
  body: Schema.String.check(Schema.isMaxLength(100_000)),
  user: ReviewWire.fields.user,
});

const DismissReviewWire = Schema.Struct({
  message: Schema.NonEmptyString.check(Schema.isMaxLength(2_000)),
});

const DismissedReviewWire = Schema.Struct({
  id: Schema.Natural,
  state: Schema.Literal("DISMISSED"),
});

const reviewFromWire = (wire: typeof ReviewWire.Type): ReviewHistoryItem => ({
  id: wire.id,
  authorLogin: wire.user.login,
  authorType: wire.user.type,
  body: wire.body ?? "",
  commitId: wire.commit_id ?? undefined,
  submittedAt: wire.submitted_at ?? undefined,
  state: wire.state,
});

const GitCommitWire = Schema.Struct({
  sha: Revision,
  tree: Schema.Struct({ sha: Revision }),
});

const GitTreeEntryFields = {
  path: Schema.String.check(Schema.isMaxLength(4_096)),
  sha: Revision,
} as const;

const GitTreeEntryWire = Schema.Union([
  Schema.Struct({
    ...GitTreeEntryFields,
    mode: Schema.Literals(["100644", "100755", "120000"]),
    type: Schema.Literal("blob"),
    size: Schema.Natural,
  }),
  Schema.Struct({
    ...GitTreeEntryFields,
    mode: Schema.Literal("040000"),
    type: Schema.Literal("tree"),
  }),
  Schema.Struct({
    ...GitTreeEntryFields,
    mode: Schema.Literal("160000"),
    type: Schema.Literal("commit"),
  }),
]);

const GitTreeWire = Schema.Struct({
  sha: Revision,
  tree: Schema.Array(GitTreeEntryWire).check(Schema.isMaxLength(100_000)),
  truncated: Schema.Boolean,
});

const GitBlobWire = Schema.Struct({
  sha: Revision,
  size: Schema.Natural,
  encoding: Schema.Literal("base64"),
  content: Schema.String.check(Schema.isMaxLength(4_000_000)),
});

const CompareWire = Schema.Struct({
  merge_base_commit: Schema.Struct({ sha: Revision }),
});

const GeneratedFileQuery = Schema.Struct({
  query: Schema.String,
  variables: Schema.Struct({
    owner: Schema.NonEmptyString,
    name: Schema.NonEmptyString,
    revision: Revision,
    path: GitTreeEntryFields.path,
  }),
});

const GeneratedFileWire = Schema.Struct({
  data: Schema.Struct({
    repository: Schema.Struct({
      object: Schema.Struct({
        oid: Revision,
        file: Schema.Struct({
          path: GitTreeEntryFields.path,
          oid: Revision,
          isGenerated: Schema.Boolean,
        }),
      }),
    }),
  }),
  errors: Schema.optionalKey(Schema.Tuple([])),
});

const PublishedReviewWire = Schema.Struct({ html_url: ShortString });
const CreateReactionWire = Schema.Struct({ content: Schema.Literal("eyes") });

const ReactionWire = Schema.Struct({
  id: Schema.Natural,
  content: Schema.Literal("eyes"),
});

const PublishReviewWire = Schema.Struct({
  commit_id: Revision,
  event: Schema.Literals(["COMMENT", "REQUEST_CHANGES"]),
  body: Schema.String.check(Schema.isMaxLength(100_000)),
  comments: Schema.Array(
    Schema.Struct({
      path: Schema.NonEmptyString.check(Schema.isMaxLength(512)),
      line: Schema.Int.check(Schema.isGreaterThan(0)),
      side: Schema.Literal("RIGHT"),
      body: Schema.NonEmptyString.check(Schema.isMaxLength(4_096)),
    }),
  ).check(Schema.isMaxLength(24)),
});

const PublishAttemptWire = Schema.Struct({
  commit_id: Revision,
  event: Schema.Literal("COMMENT"),
  body: Schema.String.check(Schema.isMaxLength(100_000)),
  comments: Schema.Tuple([]),
});

export interface PullRequestView {
  readonly number: number;
  readonly title: string;
  readonly description: string;
  readonly draft: boolean;
  readonly url: string;
  readonly baseRevision: string;
  readonly headRevision: string;
}

export interface ChangedFile {
  readonly path: string;
  readonly previousPath?: string | undefined;
  readonly status: string;
  readonly additions: number;
  readonly deletions: number;
  readonly patch: string | undefined;
}

export interface TreeComparisonView {
  readonly changedPaths: ReadonlyArray<string>;
  readonly base: RepositorySnapshot;
  readonly head: RepositorySnapshot;
}

export interface RepositorySnapshot {
  readonly revision: string;
  readonly paths: ReadonlyArray<string>;
  readonly readTextFile: (path: string) => Effect.Effect<string, GitHubApiFailure | BinaryBlob>;
  readonly entry: (path: string) =>
    | {
        readonly sha: string;
        readonly mode: "100644" | "100755" | "120000" | "040000" | "160000";
        readonly type: "blob" | "tree" | "commit";
        readonly size?: number | undefined;
      }
    | undefined;
}

export class GitHubApiFailure extends Schema.TaggedError<GitHubApiFailure>()("GitHubApiFailure", {
  operation: Schema.String,
  reason: Schema.String,
  attempts: Schema.optionalKey(Schema.Natural),
  status: Schema.optionalKey(Schema.Natural),
  requestId: Schema.optionalKey(Schema.String),
}) {}

/** A verified blob containing NUL bytes, not a failed GitHub read. */
export class BinaryBlob extends Schema.TaggedError<BinaryBlob>()("BinaryBlob", {
  sha: Revision,
}) {}

// These formats are outside the text review scope even when their bytes happen
// to decode as UTF-8. Keep textual assets such as SVG, JSON, and XML reviewable.
export const isBinaryAssetPath = (path: string): boolean =>
  /\.(png|jpe?g|gif|webp|avif|heic|heif|ico|icns|bmp|tiff?|psd|woff2?|ttf|otf|eot|mp3|mp4|m4[av]|wav|ogg|flac|aac|aiff|mov|webm|avi|mkv|pdf|zip|gz|bz2|xz|7z|rar|tar|jar|wasm|exe|dll|so|dylib|class|pyc|sqlite3?|db)$/i.test(
    path,
  );

export class StaleReviewHead extends Schema.TaggedError<StaleReviewHead>()("StaleReviewHead", {
  inspectedHead: Revision,
  currentHead: Revision,
}) {}

const MAX_TEXT_BLOB_BYTES = 2_000_000;

/** Build a unified patch from the exact two committed file contents. */
export const makeExactPatch = (input: {
  readonly path: string;
  readonly basePath?: string | undefined;
  readonly headPath?: string | undefined;
  readonly baseRevision: string;
  readonly headRevision: string;
  readonly before: string;
  readonly after: string;
}): string | undefined =>
  createTwoFilesPatch(
    `a/${input.basePath ?? input.path}`,
    `b/${input.headPath ?? input.path}`,
    input.before,
    input.after,
    input.baseRevision,
    input.headRevision,
    { context: 3, timeout: 1_000, maxEditLength: 200_000 },
  );

const changedFileFromWire = (wire: typeof ChangedFileWire.Type): ChangedFile => ({
  path: wire.filename,
  previousPath: wire.previous_filename,
  status: wire.status,
  additions: wire.additions,
  deletions: wire.deletions,
  patch: wire.patch,
});

export const makeGitHubClient = Effect.fn("makeGitHubClient")(function* (options: {
  readonly repository: string;
  readonly pullRequest: number;
  readonly token: Redacted.Redacted<string>;
  readonly apiUrl?: string | undefined;
  readonly graphqlUrl?: string | undefined;
}) {
  const client = yield* HttpClient.HttpClient;
  const apiUrl = (options.apiUrl ?? "https://api.github.com").replace(/\/$/, "");
  const graphqlUrl = options.graphqlUrl ?? `${apiUrl.replace(/\/v3$/, "")}/graphql`;
  const pullUrl = `${apiUrl}/repos/${options.repository}/pulls/${String(options.pullRequest)}`;

  const failure = (operation: string, cause: unknown) =>
    GitHubApiFailure.make({ operation, reason: String(cause).slice(0, 4_096) });

  const request = (value: HttpClientRequest.HttpClientRequest) =>
    value.pipe(
      HttpClientRequest.setHeaders({
        Accept: "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "effect-agent-pr-review",
      }),
      HttpClientRequest.bearerToken(options.token),
    );

  const execute = (operation: string, value: HttpClientRequest.HttpClientRequest) =>
    HttpClient.execute(request(value)).pipe(
      Effect.flatMap(HttpClientResponse.filterStatusOk),
      Effect.mapError((cause) => failure(operation, cause)),
      Effect.provideService(HttpClient.HttpClient, client),
    );

  const decode =
    <S extends Schema.Top>(schema: S, operation: string) =>
    (response: HttpClientResponse.HttpClientResponse) =>
      response.json.pipe(
        Effect.flatMap(Schema.decodeUnknownEffect(schema)),
        Effect.mapError((cause) => failure(operation, cause)),
      );

  // Only explicitly read-only operations enter this retry boundary. Include body
  // consumption in each attempt: retrying headers alone cannot repair a cut-off body.
  const readJson = Effect.fn("GitHubClient.readJson")(function* <S extends Schema.Top>(
    operation: string,
    value: HttpClientRequest.HttpClientRequest,
    schema: S,
  ) {
    const deadline = (yield* Clock.currentTimeMillis) + 90_000;

    for (let attempt = 1; ; attempt += 1) {
      const remaining = deadline - (yield* Clock.currentTimeMillis);

      if (remaining <= 0) {
        return yield* GitHubApiFailure.make({
          operation,
          reason: "GitHub read deadline exceeded",
          attempts: attempt - 1,
        });
      }

      const result = yield* client.execute(request(value)).pipe(
        Effect.flatMap(HttpClientResponse.filterStatusOk),
        Effect.flatMap((response) => response.json),
        Effect.timeout(Math.min(15_000, remaining)),
        Effect.result,
      );

      if (Result.isSuccess(result)) {
        return yield* Schema.decodeUnknownEffect(schema)(result.success).pipe(
          Effect.mapError(() =>
            GitHubApiFailure.make({
              operation,
              reason: "Response does not match the GitHub schema",
              attempts: attempt,
            }),
          ),
        );
      }

      const error = result.failure;
      const response = error._tag === "HttpClientError" ? error.response : undefined;
      const status = response?.status;
      const category = error._tag === "TimeoutError" ? "TimeoutError" : error.reason._tag;
      const headers = response?.headers;
      const now = yield* Clock.currentTimeMillis;
      const retryAfter = headers?.["retry-after"];

      const retryDate =
        retryAfter === undefined ? undefined : Option.getOrUndefined(DateTime.make(retryAfter));

      const retryAfterMs =
        retryAfter === undefined
          ? undefined
          : Number.isFinite(Number(retryAfter))
            ? Math.max(0, Number(retryAfter) * 1_000)
            : retryDate === undefined
              ? undefined
              : Math.max(0, DateTime.toEpochMillis(retryDate) - now);

      const rateLimited =
        status === 429 ||
        (status === 403 &&
          (retryAfterMs !== undefined || headers?.["x-ratelimit-remaining"] === "0"));

      const reset = Number(headers?.["x-ratelimit-reset"]);

      const rateDelay =
        retryAfterMs ??
        (rateLimited ? Math.max(60_000, Number.isFinite(reset) ? reset * 1_000 - now : 0) : 0);

      const delay = Math.max(1_000 * 2 ** (attempt - 1), rateDelay);

      const retryable =
        category === "TimeoutError" ||
        category === "TransportError" ||
        category === "DecodeError" ||
        status === 408 ||
        rateLimited ||
        (status !== undefined && status >= 500 && status <= 599);

      const retry = retryable && attempt < 4 && now + delay < deadline;
      const requestId = headers?.["x-github-request-id"]?.slice(0, 256);

      const diagnostic = {
        operation,
        attempt,
        category,
        status,
        requestId,
        ...(retry ? { retryInMs: delay } : {}),
      };

      yield* Effect.logWarning(retry ? "Retrying GitHub read" : "GitHub read failed", diagnostic);
      if (!retry) {
        return yield* GitHubApiFailure.make({
          operation,
          reason: `${category}${status === undefined ? "" : ` (HTTP ${String(status)})`} after ${String(attempt)} attempt(s)`,
          attempts: attempt,
          ...(status === undefined ? {} : { status }),
          ...(requestId === undefined ? {} : { requestId }),
        });
      }
      yield* Effect.sleep(delay);
    }
  });

  const getPullRequest = readJson(
    "get pull request",
    HttpClientRequest.get(pullUrl),
    PullRequestWire,
  ).pipe(
    Effect.map((wire): PullRequestView => ({
      number: wire.number,
      title: wire.title,
      description: wire.body ?? "",
      draft: wire.draft,
      url: wire.html_url,
      baseRevision: wire.base.sha,
      headRevision: wire.head.sha,
    })),
  );

  const listFiles = Effect.gen(function* () {
    const all: Array<ChangedFile> = [];

    for (let page = 1; page <= 30; page += 1) {
      const wires = yield* readJson(
        "list pull request files",
        HttpClientRequest.get(`${pullUrl}/files?per_page=100&page=${String(page)}`),
        Schema.Array(ChangedFileWire).check(Schema.isMaxLength(100)),
      );

      all.push(...wires.map(changedFileFromWire));
      if (wires.length < 100) return all;
    }

    return yield* GitHubApiFailure.make({
      operation: "list pull request files",
      reason: "pull request exceeds GitHub's 3,000-file review bound",
    });
  });

  const listReviews = Effect.gen(function* () {
    const all: Array<ReviewHistoryItem> = [];

    for (let page = 1; page <= 10; page += 1) {
      const wires = yield* readJson(
        "list pull request reviews",
        HttpClientRequest.get(`${pullUrl}/reviews?per_page=100&page=${String(page)}`),
        Schema.Array(ReviewWire).check(Schema.isMaxLength(100)),
      );

      all.push(...wires.map(reviewFromWire));
      if (wires.length < 100) return all;
    }

    return yield* GitHubApiFailure.make({
      operation: "list pull request reviews",
      reason: "review history exceeds the 1,000-review admission bound",
    });
  });

  /** Revisit unresolved feedback independently of the delta; never truncate its blockers. */
  const loadReviewFollowUps = Effect.fn("GitHubClient.loadReviewFollowUps")(function* (input: {
    readonly reviewAuthor: string;
    readonly history: ReadonlyArray<ReviewHistoryItem>;
  }) {
    const followUps: Array<ReviewFollowUp> = [];

    for (const review of unresolvedChangeRequests(input).slice(0, 8)) {
      const comments: Array<typeof ReviewCommentWire.Type> = [];

      for (let page = 1; ; page += 1) {
        if (page > 3) {
          return yield* GitHubApiFailure.make({
            operation: "load review follow-ups",
            reason: "Review comments exceed the 300-comment admission bound",
          });
        }

        const batch = yield* readJson(
          "list review comments",
          HttpClientRequest.get(
            `${pullUrl}/reviews/${String(review.id)}/comments?per_page=100&page=${String(page)}`,
          ),
          Schema.Array(ReviewCommentWire).check(Schema.isMaxLength(100)),
        );

        if (batch.some((comment) => comment.pull_request_review_id !== review.id)) {
          return yield* GitHubApiFailure.make({
            operation: "load review follow-ups",
            reason: "GitHub returned comments for a different review",
          });
        }
        comments.push(
          ...batch.filter(
            (comment) =>
              comment.user.type === "Bot" &&
              comment.user.login.toLowerCase() === input.reviewAuthor.toLowerCase(),
          ),
        );
        if (batch.length < 100) break;
      }

      const candidate = Schema.decodeUnknownOption(ReviewFollowUp)({
        id: String(review.id),
        description: JSON.stringify({
          reviewedCommit: review.commitId,
          review: review.body,
          comments: comments.map(({ path, body }) => ({ path, body })),
        }),
      });

      if (candidate._tag === "Some") followUps.push(candidate.value);
      else
        yield* Effect.logInfo(
          "Prior review exceeds follow-up input bound; retaining change request",
          {
            reviewId: review.id,
          },
        );
    }

    return followUps;
  });

  /** Recheck ownership, feedback, and head before each dismissal. GitHub has no conditional PUT. */
  const dismissReview = Effect.fn("GitHubClient.dismissReview")(function* (input: {
    readonly review: ReviewHistoryItem;
    readonly followUp: ReviewFollowUp;
    readonly reviewAuthor: string;
    readonly commitId: string;
    readonly evidence: string;
  }) {
    if (
      input.followUp.id !== String(input.review.id) ||
      unresolvedChangeRequests({ reviewAuthor: input.reviewAuthor, history: [input.review] })
        .length !== 1
    ) {
      return yield* GitHubApiFailure.make({
        operation: "dismiss review",
        reason: "Review is not an owned change request",
      });
    }
    const reviewUrl = `${pullUrl}/reviews/${String(input.review.id)}`;

    const currentReview = yield* readJson(
      "get review before dismissal",
      HttpClientRequest.get(reviewUrl),
      ReviewWire,
    ).pipe(Effect.map(reviewFromWire));

    if (
      currentReview.id !== input.review.id ||
      currentReview.authorLogin !== input.review.authorLogin ||
      currentReview.authorType !== "Bot" ||
      currentReview.body !== input.review.body ||
      currentReview.commitId !== input.review.commitId
    ) {
      return yield* GitHubApiFailure.make({
        operation: "dismiss review",
        reason: "Review changed after verification",
      });
    }
    if (currentReview.state === "DISMISSED") return;
    if (currentReview.state !== "CHANGES_REQUESTED") {
      return yield* GitHubApiFailure.make({
        operation: "dismiss review",
        reason: "Review is no longer a change request",
      });
    }

    const [currentFollowUp] = yield* loadReviewFollowUps({
      reviewAuthor: input.reviewAuthor,
      history: [currentReview],
    });

    if (currentFollowUp?.description !== input.followUp.description) {
      return yield* GitHubApiFailure.make({
        operation: "dismiss review",
        reason: "Review comments changed after verification",
      });
    }
    const current = yield* getPullRequest;

    if (current.headRevision !== input.commitId) {
      return yield* StaleReviewHead.make({
        inspectedHead: input.commitId,
        currentHead: current.headRevision,
      });
    }

    const body = yield* Schema.encodeEffect(DismissReviewWire)({
      message: `Verified addressed at ${input.commitId}.\n\n${input.evidence}`,
    }).pipe(Effect.mapError((cause) => failure("encode review dismissal", cause)));

    const request = yield* HttpClientRequest.put(`${reviewUrl}/dismissals`).pipe(
      HttpClientRequest.bodyJson(body),
      Effect.mapError((cause) => failure("encode review dismissal", cause)),
    );

    const dismissed = yield* execute("dismiss addressed review", request).pipe(
      Effect.flatMap(decode(DismissedReviewWire, "dismiss addressed review")),
    );

    if (dismissed.id !== input.review.id) {
      return yield* GitHubApiFailure.make({
        operation: "dismiss review",
        reason: "GitHub returned a different dismissed review",
      });
    }
  });

  const textBlobs = new Map<string, string>();

  const readTextBlob = Effect.fn("GitHubClient.readTextBlob")(function* (sha: string) {
    const cached = textBlobs.get(sha);

    if (cached !== undefined) return cached;

    const blob = yield* readJson(
      "get Git blob",
      HttpClientRequest.get(
        `${apiUrl}/repos/${options.repository}/git/blobs/${encodeURIComponent(sha)}`,
      ),
      GitBlobWire,
    );

    if (blob.sha !== sha) {
      return yield* GitHubApiFailure.make({
        operation: "get Git blob",
        reason: `GitHub returned blob ${blob.sha} for requested blob ${sha}`,
      });
    }
    if (blob.size > MAX_TEXT_BLOB_BYTES) {
      return yield* GitHubApiFailure.make({
        operation: "get Git blob",
        reason: `blob ${sha} exceeds the ${String(MAX_TEXT_BLOB_BYTES)}-byte text bound`,
      });
    }

    const bytes = yield* Effect.fromResult(
      Encoding.decodeBase64(blob.content.replaceAll("\n", "")),
    ).pipe(Effect.mapError((cause) => failure("decode Git blob", cause)));

    if (bytes.length !== blob.size) {
      return yield* GitHubApiFailure.make({
        operation: "decode Git blob",
        reason: `decoded blob ${sha} has ${String(bytes.length)} bytes, expected ${String(blob.size)}`,
      });
    }
    if (bytes.includes(0)) {
      return yield* BinaryBlob.make({ sha });
    }

    const content = yield* Effect.try({
      try: () => new TextDecoder("utf-8", { fatal: true }).decode(bytes),
      catch: (cause) => failure("decode Git blob", cause),
    });

    // Source search can visit the whole tree. Retain at most 16 verified blobs
    // (each bounded to 2 MB) instead of accumulating every file for the run.
    textBlobs.set(sha, content);
    if (textBlobs.size > 16) {
      const oldest = textBlobs.keys().next().value;

      if (oldest !== undefined) textBlobs.delete(oldest);
    }

    return content;
  });

  const readTreeSnapshot = Effect.fn("GitHubClient.readTreeSnapshot")(function* (
    revision: string,
  ): Effect.fn.Return<RepositorySnapshot, GitHubApiFailure> {
    const commit = yield* readJson(
      "get Git commit",
      HttpClientRequest.get(
        `${apiUrl}/repos/${options.repository}/git/commits/${encodeURIComponent(revision)}`,
      ),
      GitCommitWire,
    );

    if (commit.sha !== revision) {
      return yield* GitHubApiFailure.make({
        operation: "get Git commit",
        reason: `GitHub returned commit ${commit.sha} for requested revision ${revision}`,
      });
    }

    const tree = yield* readJson(
      "get recursive Git tree",
      HttpClientRequest.get(
        `${apiUrl}/repos/${options.repository}/git/trees/${encodeURIComponent(commit.tree.sha)}?recursive=1`,
      ),
      GitTreeWire,
    );

    if (tree.sha !== commit.tree.sha) {
      return yield* GitHubApiFailure.make({
        operation: "get recursive Git tree",
        reason: `GitHub returned tree ${tree.sha} for requested tree ${commit.tree.sha}`,
      });
    }
    if (tree.truncated) {
      return yield* GitHubApiFailure.make({
        operation: "get recursive Git tree",
        reason: `GitHub truncated tree ${tree.sha}`,
      });
    }
    const entries = new Map<string, typeof GitTreeEntryWire.Type>();

    for (const entry of tree.tree) {
      if (entries.has(entry.path)) {
        return yield* GitHubApiFailure.make({
          operation: "get recursive Git tree",
          reason: `GitHub returned duplicate path '${entry.path}' in tree ${tree.sha}`,
        });
      }
      entries.set(entry.path, entry);
    }

    const paths = [...entries.values()]
      .filter((entry) => entry.type !== "tree")
      .map((entry) => entry.path)
      .sort();

    const entry = (path: string) => entries.get(path);

    const readTextFile = Effect.fn("GitHubClient.RepositorySnapshot.readTextFile")(function* (
      path: string,
    ) {
      const value = entry(path);

      if (value?.type !== "blob") {
        return yield* GitHubApiFailure.make({
          operation: "read repository file",
          reason: `path '${path}' is unavailable at revision ${revision}`,
        });
      }

      return yield* readTextBlob(value.sha);
    });

    return { revision, paths, entry, readTextFile } satisfies RepositorySnapshot;
  });

  const getMergeBase = Effect.fn("GitHubClient.getMergeBase")(function* (
    base: string,
    head: string,
  ) {
    const comparison = yield* readJson(
      "get pull request merge base",
      HttpClientRequest.get(
        `${apiUrl}/repos/${options.repository}/compare/${encodeURIComponent(base)}...${encodeURIComponent(head)}`,
      ),
      CompareWire,
    );

    return comparison.merge_base_commit.sha;
  });

  const compareTrees = Effect.fn("GitHubClient.compareTrees")(function* (
    baseRevision: string,
    headRevision: string,
  ) {
    const { base: baseSnapshot, head: headSnapshot } = yield* Effect.all(
      {
        base: readTreeSnapshot(baseRevision),
        head: readTreeSnapshot(headRevision),
      },
      { concurrency: 2 },
    );

    const candidates = [...baseSnapshot.paths, ...headSnapshot.paths];

    const changedPaths = [...new Set(candidates)].sort().filter((path) => {
      const before = baseSnapshot.entry(path);
      const after = headSnapshot.entry(path);

      if (before === undefined || after === undefined) return before !== after;

      return before.sha !== after.sha || before.mode !== after.mode || before.type !== after.type;
    });

    return { changedPaths, base: baseSnapshot, head: headSnapshot } satisfies TreeComparisonView;
  });

  /** Classify only an existing regular file in a caller-selected trusted snapshot. */
  const isGenerated = Effect.fn("GitHubClient.isGenerated")(function* (
    snapshot: RepositorySnapshot,
    path: string,
  ) {
    const entry = snapshot.entry(path);

    if (entry?.type !== "blob" || entry.mode === "120000") return false;
    const [owner = "", name = ""] = options.repository.split("/");

    const body = yield* Schema.encodeEffect(GeneratedFileQuery)({
      query: `query GeneratedFile($owner: String!, $name: String!, $revision: GitObjectID!, $path: String!) {
        repository(owner: $owner, name: $name) {
          object(oid: $revision) {
            ... on Commit { oid file(path: $path) { path oid isGenerated } }
          }
        }
      }`,
      variables: { owner, name, revision: snapshot.revision, path },
    }).pipe(Effect.mapError((cause) => failure("encode generated file query", cause)));

    const query = yield* HttpClientRequest.post(graphqlUrl).pipe(
      HttpClientRequest.bodyJson(body),
      Effect.mapError((cause) => failure("encode generated file query", cause)),
    );

    const result = yield* readJson("classify generated file", query, GeneratedFileWire).pipe(
      Effect.timeout("10 seconds"),
      Effect.catchTag("TimeoutError", () =>
        Effect.fail(
          GitHubApiFailure.make({
            operation: "classify generated file",
            reason: "Generated-file classification timed out",
          }),
        ),
      ),
    );

    const commit = result.data.repository.object;

    if (
      commit.oid !== snapshot.revision ||
      commit.file.path !== path ||
      commit.file.oid !== entry.sha
    ) {
      return yield* GitHubApiFailure.make({
        operation: "classify generated file",
        reason: "Generated-file classification does not match the frozen repository entry",
      });
    }

    return commit.file.isGenerated;
  });

  const acknowledgeComment = Effect.fn("GitHubClient.acknowledgeComment")(function* (
    commentId: number,
  ) {
    const body = yield* Schema.encodeEffect(CreateReactionWire)({ content: "eyes" }).pipe(
      Effect.mapError((cause) => failure("encode issue comment reaction", cause)),
    );

    const reactionRequest = yield* HttpClientRequest.post(
      `${apiUrl}/repos/${options.repository}/issues/comments/${String(commentId)}/reactions`,
    ).pipe(
      HttpClientRequest.bodyJson(body),
      Effect.mapError((cause) => failure("encode issue comment reaction", cause)),
    );

    yield* execute("acknowledge review command", reactionRequest).pipe(
      Effect.flatMap(decode(ReactionWire, "acknowledge review command")),
    );
  });

  const publishReview = (input: {
    readonly commitId: string;
    readonly event: "COMMENT" | "REQUEST_CHANGES";
    readonly body: string;
    readonly comments: ReadonlyArray<{
      readonly path: string;
      readonly line: number;
      readonly body: string;
    }>;
  }) =>
    Effect.gen(function* () {
      const current = yield* getPullRequest;

      if (current.headRevision !== input.commitId) {
        return yield* StaleReviewHead.make({
          inspectedHead: input.commitId,
          currentHead: current.headRevision,
        });
      }

      const body = yield* Schema.encodeEffect(PublishReviewWire)({
        commit_id: input.commitId,
        event: input.event,
        body: input.body,
        comments: input.comments.map((comment) => ({
          path: comment.path,
          line: comment.line,
          side: "RIGHT" as const,
          body: comment.body,
        })),
      }).pipe(Effect.mapError((cause) => failure("encode pull request review", cause)));

      const request = yield* HttpClientRequest.post(`${pullUrl}/reviews`).pipe(
        HttpClientRequest.bodyJson(body),
        Effect.mapError((cause) => failure("encode pull request review", cause)),
      );

      return yield* execute("publish pull request review", request).pipe(
        Effect.flatMap(decode(PublishedReviewWire, "publish pull request review")),
        Effect.map((wire) => wire.html_url),
      );
    });

  /** Record a host-authored incomplete attempt on the inspected commit, even after a push. */
  const publishAttemptMarker = Effect.fn("GitHubClient.publishAttemptMarker")(function* (input: {
    readonly commitId: string;
    readonly body: string;
  }) {
    const body = yield* Schema.encodeEffect(PublishAttemptWire)({
      commit_id: input.commitId,
      event: "COMMENT",
      body: input.body,
      comments: [],
    }).pipe(Effect.mapError((cause) => failure("encode stale review marker", cause)));

    const reviewRequest = yield* HttpClientRequest.post(`${pullUrl}/reviews`).pipe(
      HttpClientRequest.bodyJson(body),
      Effect.mapError((cause) => failure("encode stale review marker", cause)),
    );

    return yield* execute("publish stale review marker", reviewRequest).pipe(
      Effect.flatMap(decode(PublishedReviewWire, "publish stale review marker")),
      Effect.map((wire) => wire.html_url),
    );
  });

  return {
    getPullRequest,
    listFiles,
    listReviews,
    loadReviewFollowUps,
    dismissReview,
    getMergeBase,
    compareTrees,
    readTreeSnapshot,
    isGenerated,
    acknowledgeComment,
    publishReview,
    publishAttemptMarker,
  } as const;
});

import { createTwoFilesPatch } from "diff";
import type { Redacted } from "effect";
import { Effect, Encoding, Schema } from "effect";
import { HttpClient, HttpClientRequest, HttpClientResponse } from "effect/unstable/http";

import type { ReviewHistoryItem } from "./selection.ts";

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
const PublishCurrentHeadCommentWire = Schema.Struct({
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
  readonly readTextFile: (path: string) => Effect.Effect<string, GitHubApiFailure>;
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
}) {}

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
}) {
  const client = yield* HttpClient.HttpClient;
  const apiUrl = (options.apiUrl ?? "https://api.github.com").replace(/\/$/, "");
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

  const getPullRequest = execute("get pull request", HttpClientRequest.get(pullUrl)).pipe(
    Effect.flatMap(decode(PullRequestWire, "get pull request")),
    Effect.map(
      (wire): PullRequestView => ({
        number: wire.number,
        title: wire.title,
        description: wire.body ?? "",
        draft: wire.draft,
        url: wire.html_url,
        baseRevision: wire.base.sha,
        headRevision: wire.head.sha,
      }),
    ),
  );

  const listFiles = Effect.gen(function* () {
    const all: Array<ChangedFile> = [];
    for (let page = 1; page <= 30; page += 1) {
      const wires = yield* execute(
        "list pull request files",
        HttpClientRequest.get(`${pullUrl}/files?per_page=100&page=${String(page)}`),
      ).pipe(
        Effect.flatMap(
          decode(
            Schema.Array(ChangedFileWire).check(Schema.isMaxLength(100)),
            "list pull request files",
          ),
        ),
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
      const wires = yield* execute(
        "list pull request reviews",
        HttpClientRequest.get(`${pullUrl}/reviews?per_page=100&page=${String(page)}`),
      ).pipe(
        Effect.flatMap(
          decode(
            Schema.Array(ReviewWire).check(Schema.isMaxLength(100)),
            "list pull request reviews",
          ),
        ),
      );
      all.push(
        ...wires.map((wire) => ({
          id: wire.id,
          authorLogin: wire.user.login,
          authorType: wire.user.type,
          body: wire.body ?? "",
          commitId: wire.commit_id ?? undefined,
          submittedAt: wire.submitted_at ?? undefined,
          state: wire.state,
        })),
      );
      if (wires.length < 100) return all;
    }
    return yield* GitHubApiFailure.make({
      operation: "list pull request reviews",
      reason: "review history exceeds the 1,000-review admission bound",
    });
  });

  const textBlobs = new Map<string, string>();

  const readTextBlob = Effect.fn("GitHubClient.readTextBlob")(function* (sha: string) {
    const cached = textBlobs.get(sha);
    if (cached !== undefined) return cached;
    const blob = yield* execute(
      "get Git blob",
      HttpClientRequest.get(
        `${apiUrl}/repos/${options.repository}/git/blobs/${encodeURIComponent(sha)}`,
      ),
    ).pipe(Effect.flatMap(decode(GitBlobWire, "get Git blob")));
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
      return yield* GitHubApiFailure.make({
        operation: "decode Git blob",
        reason: `blob ${sha} is not textual`,
      });
    }
    const content = yield* Effect.try({
      try: () => new TextDecoder("utf-8", { fatal: true }).decode(bytes),
      catch: (cause) => failure("decode Git blob", cause),
    });
    textBlobs.set(sha, content);
    return content;
  });

  const readTreeSnapshot = Effect.fn("GitHubClient.readTreeSnapshot")(function* (
    revision: string,
  ): Effect.fn.Return<RepositorySnapshot, GitHubApiFailure> {
    const commit = yield* execute(
      "get Git commit",
      HttpClientRequest.get(
        `${apiUrl}/repos/${options.repository}/git/commits/${encodeURIComponent(revision)}`,
      ),
    ).pipe(Effect.flatMap(decode(GitCommitWire, "get Git commit")));
    if (commit.sha !== revision) {
      return yield* GitHubApiFailure.make({
        operation: "get Git commit",
        reason: `GitHub returned commit ${commit.sha} for requested revision ${revision}`,
      });
    }
    const tree = yield* execute(
      "get recursive Git tree",
      HttpClientRequest.get(
        `${apiUrl}/repos/${options.repository}/git/trees/${encodeURIComponent(commit.tree.sha)}?recursive=1`,
      ),
    ).pipe(Effect.flatMap(decode(GitTreeWire, "get recursive Git tree")));
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
    const comparison = yield* execute(
      "get pull request merge base",
      HttpClientRequest.get(
        `${apiUrl}/repos/${options.repository}/compare/${encodeURIComponent(base)}...${encodeURIComponent(head)}`,
      ),
    ).pipe(Effect.flatMap(decode(CompareWire, "get pull request merge base")));
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

  /** Publish only a host-authored incomplete-attempt marker on GitHub's current head. */
  const publishCurrentHeadAttemptMarker = Effect.fn("GitHubClient.publishCurrentHeadAttemptMarker")(
    function* (bodyText: string) {
      const body = yield* Schema.encodeEffect(PublishCurrentHeadCommentWire)({
        event: "COMMENT",
        body: bodyText,
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
    },
  );

  return {
    getPullRequest,
    listFiles,
    listReviews,
    getMergeBase,
    compareTrees,
    acknowledgeComment,
    publishReview,
    publishCurrentHeadAttemptMarker,
  } as const;
});

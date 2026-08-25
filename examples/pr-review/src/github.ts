import type { Redacted } from "effect";
import { Effect, Schema } from "effect";
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
  ).check(Schema.isMaxLength(12)),
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
  readonly status: string;
  readonly additions: number;
  readonly deletions: number;
  readonly patch: string | undefined;
}

export interface TreeComparisonView {
  readonly changedPaths: ReadonlyArray<string>;
}

export class GitHubApiFailure extends Schema.TaggedError<GitHubApiFailure>()("GitHubApiFailure", {
  operation: Schema.String,
  reason: Schema.String,
}) {}

const changedFileFromWire = (wire: typeof ChangedFileWire.Type): ChangedFile => ({
  path: wire.filename,
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
        })),
      );
      if (wires.length < 100) return all;
    }
    return yield* GitHubApiFailure.make({
      operation: "list pull request reviews",
      reason: "review history exceeds the 1,000-review admission bound",
    });
  });

  const readTreeSnapshot = Effect.fn("GitHubClient.readTreeSnapshot")(function* (revision: string) {
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
    return entries;
  });

  const compareTrees = Effect.fn("GitHubClient.compareTrees")(function* (
    base: string,
    head: string,
    paths: ReadonlyArray<string>,
  ) {
    const { baseEntries, headEntries } = yield* Effect.all(
      {
        baseEntries: readTreeSnapshot(base),
        headEntries: readTreeSnapshot(head),
      },
      { concurrency: 2 },
    );
    const changedPaths = [...new Set(paths)].sort().filter((path) => {
      const before = baseEntries.get(path);
      const after = headEntries.get(path);
      if (before === undefined || after === undefined) return before !== after;
      return before.sha !== after.sha || before.mode !== after.mode || before.type !== after.type;
    });
    return { changedPaths } satisfies TreeComparisonView;
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

  return {
    getPullRequest,
    listFiles,
    listReviews,
    compareTrees,
    acknowledgeComment,
    publishReview,
  } as const;
});

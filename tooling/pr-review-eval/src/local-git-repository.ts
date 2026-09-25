import { execFile } from "node:child_process";
import { isAbsolute } from "node:path";

import {
  BinaryBlob,
  GitHubApiFailure,
  makeReviewRepository,
  type RepositorySnapshot,
} from "@effect-agent/pr-review-action/review-repository";
import { type ReviewRequest } from "@effect-agent/pr-review/review";
import { type ReviewRepository } from "@effect-agent/pr-review/review-repository";
import { Effect, Result, Schema } from "effect";

import { EvalConfigurationError, type EvalInputDigest } from "./contracts.ts";
import { digestText } from "./corpus.ts";

const MAX_TREE_BYTES = 16_000_000;
const MAX_TEXT_BLOB_BYTES = 2_000_000;
const MAX_TREE_ENTRIES = 100_000;
const GitSha = Schema.String.check(Schema.isPattern(/^(?:[a-f0-9]{40}|[a-f0-9]{64})$/));

const GitEntry = Schema.Struct({
  path: Schema.NonEmptyString.check(Schema.isMaxLength(4_096)),
  sha: GitSha,
  type: Schema.Literals(["blob", "commit"]),
  mode: Schema.Literals(["100644", "100755", "120000", "160000"]),
  size: Schema.optionalKey(Schema.Natural),
}).check(
  Schema.makeFilter(
    (entry) =>
      entry.type === "blob"
        ? entry.mode !== "160000" && entry.size !== undefined
        : entry.mode === "160000" && entry.size === undefined,
    { title: "Git tree entry mode, type, and size agree" },
  ),
);

type GitEntry = typeof GitEntry.Type;

export interface LocalGitRepository {
  readonly service: ReviewRepository["Service"];
  /** Binds the exact pair of Git trees, ignore patterns, and source exclusions. */
  readonly digest: EvalInputDigest;
}

const gitEnvironment = {
  PATH: process.env.PATH ?? "/usr/bin:/bin",
  GIT_CONFIG_GLOBAL: "/dev/null",
  GIT_CONFIG_NOSYSTEM: "1",
  GIT_NO_LAZY_FETCH: "1",
  GIT_NO_REPLACE_OBJECTS: "1",
  GIT_OPTIONAL_LOCKS: "0",
  GIT_TERMINAL_PROMPT: "0",
};

/** No shell, checkout, index mutation, Git hooks, credentials, or source in diagnostics. */
const gitBytes = Effect.fn("PrReviewEval.gitBytes")(function* (
  root: string,
  args: ReadonlyArray<string>,
  operation: string,
  maxBuffer: number,
) {
  return yield* Effect.tryPromise({
    try: (signal) =>
      new Promise<Buffer>((resolve, reject) => {
        execFile(
          "git",
          [...args],
          {
            cwd: root,
            encoding: "buffer",
            env: gitEnvironment,
            maxBuffer,
            signal,
            timeout: 20_000,
          },
          (error, stdout) => {
            if (error !== null) reject(error);
            else resolve(stdout);
          },
        );
      }),
    catch: () =>
      EvalConfigurationError.make({
        message: `Could not ${operation} in the configured local Git repository`,
      }),
  });
});

const decodeUtf8 = (bytes: Uint8Array, operation: string) =>
  Effect.try({
    try: () => new TextDecoder("utf-8", { fatal: true }).decode(bytes),
    catch: () => EvalConfigurationError.make({ message: `Invalid UTF-8 while ${operation}` }),
  });

const commitTree = Effect.fn("PrReviewEval.commitTree")(function* (root: string, revision: string) {
  const sha = yield* Schema.decodeEffect(GitSha)(revision).pipe(
    Effect.mapError(() =>
      EvalConfigurationError.make({ message: "Local Git source requires exact commit object IDs" }),
    ),
  );

  const kind = yield* gitBytes(root, ["cat-file", "-t", sha], "verify pinned commit", 100);

  if ((yield* decodeUtf8(kind, "verifying a pinned commit")).trim() !== "commit") {
    return yield* EvalConfigurationError.make({
      message: "A pinned local Git revision is not a commit",
    });
  }

  const treeBytes = yield* gitBytes(
    root,
    ["rev-parse", "--verify", `${sha}^{tree}`],
    "resolve pinned tree",
    100,
  );

  const tree = yield* Schema.decodeEffect(GitSha)(
    (yield* decodeUtf8(treeBytes, "resolving a pinned tree")).trim(),
  ).pipe(
    Effect.mapError(() =>
      EvalConfigurationError.make({ message: "Local Git returned an invalid tree object ID" }),
    ),
  );

  return { sha, tree };
});

const readTreeEntries = Effect.fn("PrReviewEval.readTreeEntries")(function* (
  root: string,
  revision: string,
) {
  const bytes = yield* gitBytes(
    root,
    ["ls-tree", "-r", "-l", "-z", revision],
    "list pinned Git tree",
    MAX_TREE_BYTES,
  );

  const text = yield* decodeUtf8(bytes, "listing a pinned Git tree");

  if (text.length > 0 && !text.endsWith("\0")) {
    return yield* EvalConfigurationError.make({ message: "Local Git returned an incomplete tree" });
  }

  const rows = text.length === 0 ? [] : text.slice(0, -1).split("\0");

  if (rows.length > MAX_TREE_ENTRIES) {
    return yield* EvalConfigurationError.make({
      message: "Pinned Git tree exceeds 100,000 entries",
    });
  }

  const entries = new Map<string, GitEntry>();

  for (const row of rows) {
    const separator = row.indexOf("\t");

    const fields =
      separator < 0
        ? undefined
        : row
            .slice(0, separator)
            .match(/^([0-7]{6}) (blob|commit) ([a-f0-9]{40}|[a-f0-9]{64}) +([0-9]+|-)$/);

    if (separator < 0 || fields === null || fields === undefined) {
      return yield* EvalConfigurationError.make({
        message: "Local Git returned a malformed tree entry",
      });
    }

    const size = fields[4] === "-" ? undefined : Number(fields[4]);

    const entry = yield* Schema.decodeUnknownEffect(GitEntry)({
      path: row.slice(separator + 1),
      mode: fields[1],
      type: fields[2],
      sha: fields[3],
      ...(size === undefined ? {} : { size }),
    }).pipe(
      Effect.mapError(() =>
        EvalConfigurationError.make({ message: "Local Git returned an unsupported tree entry" }),
      ),
    );

    if (entries.has(entry.path)) {
      return yield* EvalConfigurationError.make({
        message: "Local Git returned a duplicate tree path",
      });
    }
    entries.set(entry.path, entry);
  }

  return entries;
});

const makeSnapshot = (
  root: string,
  revision: string,
  entries: ReadonlyMap<string, GitEntry>,
  textBlobs: Map<string, string>,
): RepositorySnapshot => {
  const readTextFile = Effect.fn("PrReviewEval.LocalGit.readTextFile")(function* (path: string) {
    const entry = entries.get(path);

    if (entry?.type !== "blob" || entry.size === undefined) {
      return yield* GitHubApiFailure.make({
        operation: "read repository file",
        reason: "Text source is unavailable at the pinned Git revision",
      });
    }
    if (entry.size > MAX_TEXT_BLOB_BYTES) {
      return yield* GitHubApiFailure.make({
        operation: "read Git blob",
        reason: "Pinned Git blob exceeds the text-source byte bound",
      });
    }

    const cached = textBlobs.get(entry.sha);

    if (cached !== undefined) return cached;

    const bytes = yield* gitBytes(
      root,
      ["cat-file", "blob", entry.sha],
      "read pinned Git blob",
      MAX_TEXT_BLOB_BYTES,
    ).pipe(
      Effect.mapError(() =>
        GitHubApiFailure.make({
          operation: "read Git blob",
          reason: "Pinned Git blob could not be read",
        }),
      ),
    );

    if (bytes.length !== entry.size) {
      return yield* GitHubApiFailure.make({
        operation: "read Git blob",
        reason: "Pinned Git blob size differs from its tree entry",
      });
    }
    if (bytes.includes(0)) return yield* BinaryBlob.make({ sha: entry.sha });

    const content = yield* Effect.try({
      try: () => new TextDecoder("utf-8", { fatal: true }).decode(bytes),
      catch: () =>
        GitHubApiFailure.make({
          operation: "decode Git blob",
          reason: "Pinned Git blob is not UTF-8 text",
        }),
    });

    textBlobs.set(entry.sha, content);
    if (textBlobs.size > 16) {
      const oldest = textBlobs.keys().next().value;

      if (oldest !== undefined) textBlobs.delete(oldest);
    }

    return content;
  });

  return {
    revision,
    paths: [...entries.keys()].sort(),
    entry: (path) => entries.get(path),
    readTextFile,
  };
};

/** Use only exact committed blobs; the checkout and ignored working files are never read. */
export const openLocalGitRepository = Effect.fn("PrReviewEval.openLocalGitRepository")(
  function* (input: {
    readonly root: string;
    readonly request: ReviewRequest;
    readonly ignore: ReadonlyArray<string>;
    /** The Action's source exclusions, distinct from capacity-excluded unreviewed paths. */
    readonly unavailablePaths?: ReadonlySet<string>;
  }) {
    if (!isAbsolute(input.root)) {
      return yield* EvalConfigurationError.make({
        message: "PR_REVIEW_LOCAL_GIT_REPOSITORY must be an absolute path",
      });
    }

    const baseCommit = yield* commitTree(input.root, input.request.baseRevision);
    const headCommit = yield* commitTree(input.root, input.request.headRevision);
    const baseEntries = yield* readTreeEntries(input.root, baseCommit.sha);
    const headEntries = yield* readTreeEntries(input.root, headCommit.sha);
    const textBlobs = new Map<string, string>();
    const unavailablePaths = new Set(input.unavailablePaths ?? []);

    const service = makeReviewRepository({
      base: makeSnapshot(input.root, baseCommit.sha, baseEntries, textBlobs),
      head: makeSnapshot(input.root, headCommit.sha, headEntries, textBlobs),
      ignore: input.ignore,
      unavailablePaths,
    });

    for (const change of input.request.changes) {
      const revision = headEntries.has(change.path) ? "head" : "base";

      const source = yield* service
        .readFile({ path: change.path, revision, startLine: 1, lineCount: 1 })
        .pipe(Effect.result);

      // An overlong first line still belongs to the review's source scope and can be searched.
      if (
        Result.isFailure(source) &&
        source.failure.message !==
          "The requested line range exceeds 20,000 characters; request fewer lines."
      ) {
        return yield* EvalConfigurationError.make({
          message: `Changed path ${change.path} is missing or excluded from local Git source`,
        });
      }
    }

    const digest = yield* digestText(
      JSON.stringify({
        version: 1,
        base: baseCommit,
        head: headCommit,
        ignore: input.ignore,
        unavailablePaths: [...unavailablePaths].sort(),
      }),
    );

    return { service, digest } satisfies LocalGitRepository;
  },
);

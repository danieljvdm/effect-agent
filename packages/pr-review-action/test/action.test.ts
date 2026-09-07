import { ReviewRepository } from "@effect-agent/pr-review/ReviewRepository";
import { NodeServices } from "@effect/platform-node";
import { describe, expect, expectTypeOf, it, layer } from "@effect/vitest";
import { parsePatch } from "diff";
import {
  Cause,
  Config,
  ConfigProvider,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Layer,
  Logger,
  Option,
  Ref,
  Schema,
} from "effect";
import { HttpClient, HttpClientResponse } from "effect/unstable/http";

import {
  GeneratedFileClassification,
  hydrateExactChanges,
  IncrementalScopeUnavailable,
  makeReviewRepository,
  publishHeadBoundReview,
  reviewPublicationFailure,
  reviewActionProgram,
  reviewEventFor,
  withActionInputs,
} from "../src/action.ts";
import {
  BinaryBlob,
  type ChangedFile,
  GitHubApiFailure,
  makeExactPatch,
  type RepositorySnapshot,
} from "../src/github.ts";
import { reviewPriority, reviewMaxCostUsd, reviewCostLimitMicrousd } from "../src/review-openai.ts";
import { reviewMarker } from "../src/selection.ts";

const file = (path: string, patch: string | undefined): ChangedFile => ({
  path,
  status: "modified",
  additions: 1,
  deletions: 0,
  patch,
});

const PublishedReviewBody = Schema.Struct({
  commit_id: Schema.String,
  event: Schema.String,
  body: Schema.String,
  comments: Schema.Array(Schema.Unknown),
});

type TestHttpRequest = Parameters<typeof HttpClientResponse.fromWeb>[0];

const jsonResponse = (request: TestHttpRequest, body: unknown) =>
  HttpClientResponse.fromWeb(
    request,
    new globalThis.Response(JSON.stringify(body), {
      status: 200,
      headers: { "content-type": "application/json" },
    }),
  );

const decodePublishedReview = (request: TestHttpRequest) => {
  const encoded =
    request.body._tag === "Uint8Array" ? new TextDecoder().decode(request.body.body) : "{}";

  return Schema.decodeUnknownSync(PublishedReviewBody)(JSON.parse(encoded));
};

const actionConfig = (overrides: Record<string, string | undefined> = {}) =>
  ConfigProvider.fromEnv({
    env: {
      GITHUB_REPOSITORY: "reve-ai/example",
      GITHUB_TOKEN: "github-token",
      GITHUB_API_URL: "https://api.github.test",
      PR_REVIEW_PULL_REQUEST: "12",
      PR_REVIEW_AUTHOR: "effect-agent[bot]",
      PR_REVIEW_MODEL: "gpt-6-astra",
      ...overrides,
    },
  });

const runReviewAction = (
  client: HttpClient.HttpClient,
  overrides?: Record<string, string | undefined>,
) =>
  reviewActionProgram.pipe(
    Effect.provideService(ConfigProvider.ConfigProvider, actionConfig(overrides)),
    Effect.provideService(HttpClient.HttpClient, client),
    Effect.provide(NodeServices.layer),
  );

const recordJsonResponse = (
  requests: Ref.Ref<ReadonlyArray<string>>,
  request: TestHttpRequest,
  url: URL,
  body: unknown,
) =>
  Ref.update(requests, (current) => [...current, `${request.method} ${url.pathname}`]).pipe(
    Effect.as(jsonResponse(request, body)),
  );

const pullRequestWire = (title: string, base: string, head: string, draft = false) => ({
  number: 12,
  title,
  body: null,
  draft,
  html_url: "https://github.test/reve-ai/example/pull/12",
  base: { sha: base },
  head: { sha: head },
});

const reviewHistoryWire = (id: number, body: string, commitId: string, submittedAt: string) => ({
  id,
  body,
  commit_id: commitId,
  submitted_at: submittedAt,
  state: "COMMENTED",
  user: { login: "effect-agent[bot]", type: "Bot" },
});

describe("immutable review source", () => {
  const snapshot = (
    revision: string,
    files: Readonly<Record<string, string>>,
    symlinks: ReadonlySet<string> = new Set(),
  ): RepositorySnapshot => ({
    revision,
    paths: [...Object.keys(files), ...symlinks].sort(),
    entry: (path) =>
      symlinks.has(path)
        ? { sha: `link-${path}`, mode: "120000", type: "blob" }
        : path in files
          ? { sha: `${revision}-${path}`, mode: "100644", type: "blob" }
          : undefined,
    readTextFile: (path) =>
      path in files
        ? Effect.succeed(files[path] ?? "")
        : Effect.die(`unexpected source read for ${path}`),
  });

  it.effect("reads the requested range from the exact revision through the Effect service", () =>
    Effect.gen(function* () {
      const repository = makeReviewRepository({
        base: snapshot("base-sha", { "src/value.ts": "base one\nbase two\nbase three\n" }),
        head: snapshot("head-sha", { "src/value.ts": "head one\nhead two\n" }),
        ignore: [],
        unavailablePaths: new Set(),
      });

      const source = yield* Effect.gen(function* () {
        const bound = yield* ReviewRepository;

        return yield* bound.readFile({
          path: "src/value.ts",
          revision: "base",
          startLine: 2,
          lineCount: 1,
        });
      }).pipe(Effect.provideService(ReviewRepository, repository));

      expect(source).toMatchObject({
        path: "src/value.ts",
        revision: "base",
        startLine: 2,
        totalLines: 3,
        content: "base two",
      });
    }),
  );

  it.effect("finds case-sensitive plain substrings with stable bounds and scope filtering", () =>
    Effect.gen(function* () {
      const matching = Object.fromEntries(
        Array.from({ length: 102 }, (_, index) => [
          `src/Test-${String(index).padStart(3, "0")}.ts`,
          "source",
        ]),
      );

      const repository = makeReviewRepository({
        base: snapshot("base", {}),
        head: snapshot(
          "head",
          { ...matching, "src/test-lower.ts": "source", "ignored/Test.ts": "source" },
          new Set(["src/Test-link.ts"]),
        ),
        ignore: ["ignored/**"],
        unavailablePaths: new Set(["src/Test-001.ts"]),
      });

      const found = yield* repository.findFiles({ query: "Test", revision: "head" });

      expect(found.paths).toHaveLength(100);
      expect(found.truncated).toBe(true);
      expect(found.paths).not.toContain("src/test-lower.ts");
      expect(found.paths).not.toContain("ignored/Test.ts");
      expect(found.paths).not.toContain("src/Test-001.ts");
      expect(found.paths).not.toContain("src/Test-link.ts");
      expect(found.paths).toEqual([...found.paths].sort());
    }),
  );

  it.effect("fails closed for ignored paths and source ranges above the return bound", () =>
    Effect.gen(function* () {
      const repository = makeReviewRepository({
        base: snapshot("base", {}),
        head: snapshot("head", {
          "ignored/secret.ts": "secret",
          "src/wide.ts": "x".repeat(20_001),
        }),
        ignore: ["ignored/**"],
        unavailablePaths: new Set(),
      });

      const ignored = yield* repository
        .readFile({
          path: "ignored/secret.ts",
          revision: "head",
          startLine: 1,
          lineCount: 1,
        })
        .pipe(Effect.flip);

      const wide = yield* repository
        .readFile({
          path: "src/wide.ts",
          revision: "head",
          startLine: 1,
          lineCount: 1,
        })
        .pipe(Effect.flip);

      expect(ignored.message).toBe("The requested path is outside this review's source scope.");
      expect(wide.message).toContain("request fewer lines");
    }),
  );

  it.effect("searches literal source at the exact revision within authorized regular files", () =>
    Effect.gen(function* () {
      const repository = makeReviewRepository({
        base: snapshot("base", { "src/caller.ts": "handler[old]\n" }),
        head: snapshot(
          "head",
          {
            "src/caller.ts": "unchanged\nhandler[new]\nHandler[other]\n",
            "src/ignored.ts": "handler[ignored]",
            "src/unavailable.ts": "handler[unavailable]",
            "src/image.png": "handler[binary]",
            "other/caller.ts": "handler[other]",
          },
          new Set(["src/link.ts"]),
        ),
        ignore: ["src/ignored.ts"],
        unavailablePaths: new Set(["src/unavailable.ts"]),
      });

      const base = yield* repository.searchCode({
        query: "handler[",
        path: "src/",
        revision: "base",
        cursor: 0,
      });

      const head = yield* repository.searchCode({
        query: "handler[",
        path: "src/",
        revision: "head",
        cursor: 0,
      });

      expect(base.matches).toEqual([{ path: "src/caller.ts", line: 1, content: "handler[old]" }]);
      expect(head).toEqual({
        matches: [{ path: "src/caller.ts", line: 2, content: "handler[new]" }],
        truncated: false,
        unreadablePaths: [],
      });
    }),
  );

  it.effect("paginates source search and discloses bounded matches and unreadable files", () =>
    Effect.gen(function* () {
      const query = "needle";

      const paths = Array.from(
        { length: 22 },
        (_, index) => `src/${String(index).padStart(2, "0")}.ts`,
      );

      const selected = snapshot("head", Object.fromEntries(paths.map((path) => [path, query])));

      const repository = makeReviewRepository({
        base: snapshot("base", {}),
        head: {
          ...selected,
          readTextFile: (path) =>
            path === "src/00.ts"
              ? Effect.succeed(`${"x".repeat(300)}${query}${"x".repeat(300)}\n`.repeat(6))
              : path === "src/01.ts"
                ? Effect.fail(GitHubApiFailure.make({ operation: "read", reason: "missing" }))
                : path === "src/02.ts"
                  ? Effect.fail(BinaryBlob.make({ sha: "binary-blob" }))
                  : selected.readTextFile(path),
        },
        ignore: [],
        unavailablePaths: new Set(),
      });

      const first = yield* repository.searchCode({
        query,
        path: "src/",
        revision: "head",
        cursor: 0,
      });

      expect(first.nextCursor).toBe(20);
      expect(first.truncated).toBe(true);
      expect(first.unreadablePaths).toEqual(["src/01.ts", "src/02.ts"]);
      expect(first.matches.filter(({ path }) => path === "src/00.ts")).toHaveLength(5);
      expect(
        first.matches.every(({ content }) => content.includes(query) && content.length <= 200),
      ).toBe(true);
      expect(first.matches.at(-1)?.path).toBe("src/19.ts");

      const last = yield* repository.searchCode({
        query,
        path: "src/",
        revision: "head",
        cursor: 20,
      });

      expect(last.matches.map(({ path }) => path)).toEqual(["src/20.ts", "src/21.ts"]);
      expect(last.nextCursor).toBeUndefined();
      expect(last.truncated).toBe(false);
      expect(last.unreadablePaths).toEqual([]);
    }),
  );

  it.effect("retains complete search terms at the Unicode path and query bounds", () =>
    Effect.gen(function* () {
      const query = "界".repeat(200);

      const contents = Object.fromEntries(
        Array.from({ length: 20 }, (_, index) => [
          `src/${"界".repeat(490)}-${String(index).padStart(2, "0")}.ts`,
          `${query}\n`.repeat(5),
        ]),
      );

      const repository = makeReviewRepository({
        base: snapshot("base", {}),
        head: snapshot("head", contents),
        ignore: [],
        unavailablePaths: new Set(),
      });

      const result = yield* repository.searchCode({ query, path: "", revision: "head", cursor: 0 });

      expect(result.truncated).toBe(false);
      expect(result.matches).toHaveLength(100);
      expect(result.matches.every(({ content }) => content === query)).toBe(true);
      expect(new TextEncoder().encode(JSON.stringify(result)).length).toBeLessThan(512 * 1_024);
    }),
  );

  it.effect(
    "interrupts every active source search read within the four-read concurrency bound",
    () =>
      Effect.gen(function* () {
        const started = yield* Ref.make(0);
        const finalized = yield* Ref.make(0);
        const ready = yield* Deferred.make<void>();

        const head = snapshot(
          "head",
          Object.fromEntries(
            Array.from({ length: 20 }, (_, index) => [`src/${String(index)}.ts`, "source"]),
          ),
        );

        const repository = makeReviewRepository({
          base: snapshot("base", {}),
          head: {
            ...head,
            readTextFile: () =>
              Effect.gen(function* () {
                const count = yield* Ref.updateAndGet(started, (value) => value + 1);

                if (count === 4) yield* Deferred.succeed(ready, undefined);

                return yield* Effect.never;
              }).pipe(Effect.ensuring(Ref.update(finalized, (value) => value + 1))),
          },
          ignore: [],
          unavailablePaths: new Set(),
        });

        const fiber = yield* Effect.forkChild(
          repository.searchCode({
            query: "source",
            path: "",
            revision: "head",
            cursor: 0,
          }),
        );

        yield* Deferred.await(ready);
        yield* Fiber.interrupt(fiber);
        expect(yield* Ref.get(started)).toBe(4);
        expect(yield* Ref.get(finalized)).toBe(4);
      }),
  );
});

describe("Action configuration", () => {
  it.effect.each([undefined, "", "   "])("requires an explicit nonblank model: %s", (model) =>
    Effect.gen(function* () {
      const client = HttpClient.make(() => Effect.die("Missing model must fail before HTTP"));
      const exit = yield* runReviewAction(client, { PR_REVIEW_MODEL: model }).pipe(Effect.exit);

      expect(Exit.isFailure(exit)).toBe(true);
      if (Exit.isFailure(exit)) {
        expect(Cause.hasDies(exit.cause)).toBe(false);
        expect(Cause.pretty(exit.cause)).toContain("PR_REVIEW_MODEL");
      }
    }),
  );

  it.effect("rejects an unsupported priority before HTTP", () =>
    Effect.gen(function* () {
      const client = HttpClient.make(() => Effect.die("Invalid priority must fail before HTTP"));

      const exit = yield* runReviewAction(client, { PR_REVIEW_PRIORITY: "ultrafast" }).pipe(
        Effect.exit,
      );

      expect(Exit.isFailure(exit)).toBe(true);
      if (Exit.isFailure(exit)) {
        expect(Cause.hasDies(exit.cause)).toBe(false);
        expect(Cause.pretty(exit.cause)).toContain("PR_REVIEW_PRIORITY");
      }
    }),
  );

  it.effect("opts into Fast through the Action input with an explicit environment override", () =>
    Effect.gen(function* () {
      for (const { env, expected } of [
        { env: {}, expected: "" },
        { env: { INPUT_PRIORITY: "fast" }, expected: "fast" },
        { env: { INPUT_PRIORITY: "fast", PR_REVIEW_PRIORITY: "default" }, expected: "default" },
      ]) {
        expect(yield* reviewPriority.parse(withActionInputs(ConfigProvider.fromEnv({ env })))).toBe(
          expected,
        );
      }
      yield* reviewPriority
        .parse(withActionInputs(ConfigProvider.fromEnv({ env: { INPUT_PRIORITY: "typo" } })))
        .pipe(Effect.flip);
    }),
  );

  it.effect("defaults to $2.50 and lets environment configuration override the Action cap", () =>
    Effect.gen(function* () {
      expect(yield* reviewMaxCostUsd.parse(ConfigProvider.fromEnv({ env: {} }))).toBe(2.5);
      const inputs = { "INPUT_MAX-COST-USD": "4.25" };

      expect(
        yield* reviewMaxCostUsd.parse(withActionInputs(ConfigProvider.fromEnv({ env: inputs }))),
      ).toBe(4.25);
      expect(
        yield* reviewMaxCostUsd.parse(
          withActionInputs(
            ConfigProvider.fromEnv({
              env: {
                ...inputs,
                PR_REVIEW_MAX_COST_USD: "0.75",
              },
            }),
          ),
        ),
      ).toBe(0.75);
      for (const invalid of ["0", "-1", "NaN", "Infinity", "101", "oops"]) {
        const exit = yield* reviewMaxCostUsd
          .parse(
            withActionInputs(
              ConfigProvider.fromEnv({
                env: {
                  "INPUT_MAX-COST-USD": invalid,
                },
              }),
            ),
          )
          .pipe(Effect.exit);

        expect(Exit.isFailure(exit)).toBe(true);
      }
    }),
  );

  it.effect("reads a GitHub Action input without mutating the environment", () =>
    Effect.gen(function* () {
      const provider = withActionInputs(
        ConfigProvider.fromEnv({ env: { "INPUT_AUTOMATIC-REVIEW-LIMIT": "7" } }),
      );

      expect(yield* Config.string("PR_REVIEW_AUTOMATIC_LIMIT").parse(provider)).toBe("7");
    }),
  );

  it.effect("preserves the raw manual review command for validation", () =>
    Effect.gen(function* () {
      const provider = withActionInputs(
        ConfigProvider.fromEnv({ env: { INPUT_COMMAND: "@effect-agent review\r\n" } }),
      );

      expect(yield* Config.string("PR_REVIEW_COMMAND").parse(provider)).toBe(
        "@effect-agent review\r\n",
      );
    }),
  );

  it.effect("falls back to an Action input when the environment value is empty", () =>
    Effect.gen(function* () {
      const provider = withActionInputs(
        ConfigProvider.fromEnv({
          env: { OPENAI_API_KEY: "", "INPUT_OPENAI-API-KEY": "action-key" },
        }),
      );

      expect(yield* Config.nonEmptyString("OPENAI_API_KEY").parse(provider)).toBe("action-key");
    }),
  );
});

describe("stale-head publication", () => {
  it.effect("preserves a generic publication failure without retrying", () =>
    Effect.gen(function* () {
      const attempts = yield* Ref.make(0);

      const failure = GitHubApiFailure.make({
        operation: "publish pull request review",
        reason: "POST failed after an uncertain transport outcome",
      });

      const exit = yield* publishHeadBoundReview(
        Ref.update(attempts, (count) => count + 1).pipe(Effect.andThen(Effect.fail(failure))),
        { publish: () => Effect.die("Must not retry uncertain publication"), automatic: true },
      ).pipe(Effect.exit);

      if (Exit.isSuccess(exit)) throw new Error("Expected publication to fail");
      expect(Option.getOrUndefined(Cause.findErrorOption(exit.cause))).toEqual(failure);
      expect(yield* Ref.get(attempts)).toBe(1);
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("leaves a stale run failed without blocking the queued review of the new head", () =>
    Effect.gen(function* () {
      const pullReads = yield* Ref.make(0);
      const postBodies = yield* Ref.make<ReadonlyArray<typeof PublishedReviewBody.Type>>([]);

      const client = HttpClient.make((request, url) => {
        if (request.method === "GET" && url.pathname.endsWith("/pulls/12")) {
          return Ref.getAndUpdate(pullReads, (count) => count + 1).pipe(
            Effect.map((count) =>
              jsonResponse(
                request,
                pullRequestWire(
                  "Move during publication",
                  "base",
                  count === 0 ? "inspected-head" : "current-head",
                ),
              ),
            ),
          );
        }
        if (request.method === "GET" && url.pathname.endsWith("/reviews")) {
          return Ref.get(postBodies).pipe(
            Effect.map((posts) =>
              jsonResponse(
                request,
                posts.map((post, index) =>
                  reviewHistoryWire(index + 1, post.body, post.commit_id, "2026-08-30T21:18:06Z"),
                ),
              ),
            ),
          );
        }
        if (request.method === "GET" && url.pathname.endsWith("/files")) {
          return Effect.succeed(jsonResponse(request, []));
        }
        if (request.method === "GET" && url.pathname.includes("/compare/")) {
          return Effect.succeed(jsonResponse(request, { merge_base_commit: { sha: "base" } }));
        }
        if (request.method === "GET" && url.pathname.endsWith("/git/commits/base")) {
          return Effect.succeed(jsonResponse(request, { sha: "base", tree: { sha: "base-tree" } }));
        }
        if (
          request.method === "GET" &&
          (url.pathname.endsWith("/git/commits/inspected-head") ||
            url.pathname.endsWith("/git/commits/current-head"))
        ) {
          return Effect.succeed(
            jsonResponse(request, {
              sha: url.pathname.split("/").at(-1),
              tree: { sha: "head-tree" },
            }),
          );
        }
        if (
          request.method === "GET" &&
          (url.pathname.endsWith("/git/trees/base-tree") ||
            url.pathname.endsWith("/git/trees/head-tree"))
        ) {
          const sha = url.pathname.endsWith("base-tree") ? "base-tree" : "head-tree";

          return Effect.succeed(jsonResponse(request, { sha, tree: [], truncated: false }));
        }
        if (request.method === "POST" && url.pathname.endsWith("/reviews")) {
          return Ref.update(postBodies, (current) => [
            ...current,
            decodePublishedReview(request),
          ]).pipe(Effect.as(jsonResponse(request, { html_url: "https://github.test/review" })));
        }

        return Effect.die(`unexpected request ${request.method} ${url.href}`);
      });

      const exit = yield* runReviewAction(client, { PR_REVIEW_AUTOMATIC_LIMIT: "2" }).pipe(
        Effect.exit,
      );

      if (Exit.isSuccess(exit)) throw new Error("Expected stale publication to fail");
      expect(Option.getOrUndefined(Cause.findErrorOption(exit.cause))).toMatchObject({
        _tag: "StaleReviewHead",
        inspectedHead: "inspected-head",
        currentHead: "current-head",
      });
      expect(yield* Ref.get(pullReads)).toBe(2);
      expect(yield* Ref.get(postBodies)).toEqual([
        {
          commit_id: "inspected-head",
          event: "COMMENT",
          body: expect.stringContaining(reviewMarker(true, false)),
          comments: [],
        },
      ]);

      yield* runReviewAction(client, { PR_REVIEW_AUTOMATIC_LIMIT: "2" });
      const posts = yield* Ref.get(postBodies);

      expect(posts).toHaveLength(2);
      expect(posts[1]).toEqual({
        commit_id: "current-head",
        event: "COMMENT",
        body: expect.stringContaining(
          "<!-- effect-agent-review:v3 automatic=true completed=true -->",
        ),
        comments: [],
      });
      expect(posts[1]?.body).toContain("Automatic reviews are paused");
    }),
  );
});

describe("Manual command acknowledgement", () => {
  it.effect("PRR-007 reacts before reading pull-request state", () =>
    Effect.gen(function* () {
      const requests = yield* Ref.make<ReadonlyArray<string>>([]);

      const client = HttpClient.make((request, url) => {
        const wire = url.pathname.endsWith("/reactions")
          ? { id: 1, content: "eyes" }
          : pullRequestWire("Draft pull request", "base", "head", true);

        return recordJsonResponse(requests, request, url, wire);
      });

      yield* runReviewAction(client, {
        PR_REVIEW_COMMAND: "@effect-agent review full",
        PR_REVIEW_COMMENT_ID: "42",
      });

      expect(yield* Ref.get(requests)).toEqual([
        "POST /repos/reve-ai/example/issues/comments/42/reactions",
        "GET /repos/reve-ai/example/pulls/12",
      ]);
    }),
  );
});

describe("Incremental review scope", () => {
  it.effect.each([
    { mode: "full", unavailable: false },
    { mode: "incremental", unavailable: false },
    { mode: "incremental", unavailable: true },
  ] as const)(
    "classifies deleted artifacts at the trusted merge base: $mode, unavailable=$unavailable",
    ({ mode, unavailable }) =>
      Effect.gen(function* () {
        const path = "retired-action/dist/index.mjs";
        const published: Array<typeof PublishedReviewBody.Type> = [];
        const classified: Array<string> = [];

        const client = HttpClient.make((request, url) => {
          let body: unknown;

          if (url.pathname.endsWith("/pulls/12")) {
            body = pullRequestWire("Delete generated output", "target", "head");
          } else if (request.method === "GET" && url.pathname.endsWith("/reviews")) {
            body = [
              reviewHistoryWire(1, reviewMarker(false), "reviewed-head", "2026-08-25T00:00:00Z"),
            ];
          } else if (url.pathname.endsWith("/files")) {
            body = [{ filename: path, status: "removed", additions: 0, deletions: 58_601 }];
          } else if (url.pathname.includes("/compare/")) {
            body = { merge_base_commit: { sha: "trusted-base" } };
          } else if (url.pathname.includes("/git/commits/")) {
            const revision = url.pathname.split("/").at(-1);

            body = { sha: revision, tree: { sha: `${revision}-tree` } };
          } else if (url.pathname.includes("/git/trees/")) {
            const tree = url.pathname.split("/").at(-1);

            body = {
              sha: tree,
              truncated: false,
              tree:
                tree === "head-tree"
                  ? []
                  : [{ path, mode: "100644", type: "blob", sha: `${tree}-blob`, size: 2_138_832 }],
            };
          } else if (url.pathname === "/graphql") {
            if (request.body._tag !== "Uint8Array") throw new Error("Expected GraphQL JSON");

            const query = Schema.decodeUnknownSync(
              Schema.fromJsonString(
                Schema.Struct({
                  variables: Schema.Struct({ revision: Schema.String, path: Schema.String }),
                }),
              ),
            )(new TextDecoder().decode(request.body.body));

            expect(query.variables.path).toBe(path);
            classified.push(query.variables.revision);
            body = unavailable
              ? { errors: [{ message: "classification unavailable" }] }
              : {
                  data: {
                    repository: {
                      object: {
                        oid: "trusted-base",
                        file: { path, oid: "trusted-base-tree-blob", isGenerated: true },
                      },
                    },
                  },
                };
          } else if (request.method === "POST" && url.pathname.endsWith("/reviews")) {
            published.push(decodePublishedReview(request));
            body = { html_url: "https://github.test/review" };
          } else {
            return Effect.die(`must not read bundle contents or call a model: ${url.href}`);
          }

          return Effect.succeed(jsonResponse(request, body));
        });

        const exit = yield* runReviewAction(client, { PR_REVIEW_MODE: mode }).pipe(Effect.exit);

        expect(classified).toEqual(["trusted-base"]);
        expect(Exit.isSuccess(exit)).toBe(!unavailable);
        expect(published).toHaveLength(1);
        expect(published[0]?.body).toContain(reviewMarker(false, !unavailable));
        if (!unavailable) expect(published[0]?.body).toContain("1 ignored");
      }),
  );

  it.effect.each([false, true])(
    "completes a binary-only PR, including an incomplete same-head retry: %s",
    (retry) =>
      Effect.gen(function* () {
        const published = yield* Ref.make<ReadonlyArray<typeof PublishedReviewBody.Type>>([]);

        const client = HttpClient.make((request, url) => {
          let response: unknown;

          if (request.method === "POST" && url.pathname.endsWith("/reviews")) {
            return Ref.update(published, (values) => [
              ...values,
              decodePublishedReview(request),
            ]).pipe(
              Effect.as(
                jsonResponse(request, {
                  html_url: "https://github.test/reve-ai/example/pull/12#review",
                }),
              ),
            );
          }
          if (url.pathname.endsWith("/pulls/12")) {
            response = pullRequestWire("Update icons", "base", "head");
          } else if (url.pathname.endsWith("/reviews") || url.pathname.endsWith("/files")) {
            response =
              retry && url.pathname.endsWith("/reviews")
                ? [reviewHistoryWire(1, reviewMarker(true, false), "head", "2026-08-25T00:00:00Z")]
                : [];
          } else if (url.pathname.includes("/compare/")) {
            response = { merge_base_commit: { sha: "base" } };
          } else if (url.pathname.includes("/git/commits/")) {
            const sha = url.pathname.endsWith("/base") ? "base" : "head";

            response = { sha, tree: { sha: `${sha}-tree` } };
          } else if (url.pathname.includes("/git/trees/")) {
            const sha = url.pathname.endsWith("/base-tree") ? "base-tree" : "head-tree";

            response = {
              sha,
              truncated: false,
              tree:
                sha === "base-tree"
                  ? []
                  : [
                      {
                        path: "assets/icon.png",
                        sha: "image",
                        mode: "100644",
                        type: "blob",
                        size: 20_000_000,
                      },
                    ],
            };
          } else {
            return Effect.die(`unexpected request ${request.method} ${url.href}`);
          }

          return Effect.succeed(jsonResponse(request, response));
        });

        yield* runReviewAction(client);
        const reviews = yield* Ref.get(published);

        expect(reviews).toHaveLength(1);
        expect(reviews[0]?.body).toContain("1 ignored");
        expect(reviews[0]?.body).toContain(reviewMarker(true, true));
      }),
  );

  it("publishes blocking findings as a head-bound change request", () => {
    expect(reviewEventFor(1)).toBe("REQUEST_CHANGES");
    expect(reviewEventFor(0)).toBe("COMMENT");
  });

  it("keeps incomplete coverage and prior change requests failing", () => {
    expect(
      reviewPublicationFailure({
        blockingFindings: 0,
        unreviewedPaths: 0,
        unresolvedChangeRequests: 0,
        exhausted: "tokens",
      })?._tag,
    ).toBe("ReviewAttemptIncomplete");
    expect(
      reviewPublicationFailure({
        blockingFindings: 0,
        unreviewedPaths: 1,
        unresolvedChangeRequests: 0,
      })?._tag,
    ).toBe("IncompleteReview");
    expect(
      reviewPublicationFailure({
        blockingFindings: 0,
        unreviewedPaths: 0,
        unresolvedChangeRequests: 2,
      })?._tag,
    ).toBe("UnresolvedChangeRequests");
    expect(
      reviewPublicationFailure({
        blockingFindings: 0,
        unreviewedPaths: 0,
        unresolvedChangeRequests: 0,
      }),
    ).toBeUndefined();
  });

  it.effect(
    "keeps incomplete same-head attempts failing when the automatic allowance is exhausted",
    () =>
      Effect.gen(function* () {
        for (const body of [
          `Review execution failed.\n\n${reviewMarker(true, false)}`,
          `Review coverage is incomplete.\n\n${reviewMarker(true, false)}`,
        ]) {
          const requests = yield* Ref.make<ReadonlyArray<string>>([]);

          const client = HttpClient.make((request, url) => {
            const response = url.pathname.endsWith("/reviews")
              ? [reviewHistoryWire(1, body, "head", "2026-08-25T00:00:00Z")]
              : url.pathname.endsWith("/pulls/12")
                ? pullRequestWire("Keep a failed check red", "base", "head")
                : undefined;

            if (response === undefined) {
              return Effect.die(`unexpected request ${request.method} ${url.href}`);
            }

            return recordJsonResponse(requests, request, url, response);
          });

          const exit = yield* runReviewAction(client, { PR_REVIEW_AUTOMATIC_LIMIT: "1" }).pipe(
            Effect.exit,
          );

          expect(Exit.isFailure(exit)).toBe(true);
          expect(yield* Ref.get(requests)).toEqual([
            "GET /repos/reve-ai/example/pulls/12",
            "GET /repos/reve-ai/example/pulls/12/reviews",
          ]);
        }
      }),
  );

  it.effect(
    "preserves preparation failures and publishes an attempt only on the inspected head",
    () =>
      Effect.gen(function* () {
        for (const failureKind of ["truncated-tree", "missing-guidance"] as const) {
          const publishedWires = yield* Ref.make<ReadonlyArray<typeof PublishedReviewBody.Type>>(
            [],
          );

          const requests = yield* Ref.make<ReadonlyArray<string>>([]);
          let pullReads = 0;

          const client = HttpClient.make((request, url) => {
            let response: unknown;

            if (request.method === "GET" && url.pathname.endsWith("/pulls/12")) {
              response = pullRequestWire(
                "Record preparation failures",
                "base",
                failureKind === "truncated-tree" && pullReads > 0 ? "moved-head" : "head",
              );
              pullReads += 1;
            } else if (
              request.method === "GET" &&
              (url.pathname.endsWith("/reviews") || url.pathname.endsWith("/files"))
            ) {
              response = [];
            } else if (request.method === "GET" && url.pathname.includes("/compare/")) {
              response = { merge_base_commit: { sha: "base" } };
            } else if (request.method === "GET" && url.pathname.endsWith("/git/commits/base")) {
              response = { sha: "base", tree: { sha: "base-tree" } };
            } else if (request.method === "GET" && url.pathname.endsWith("/git/commits/head")) {
              response = { sha: "head", tree: { sha: "head-tree" } };
            } else if (request.method === "GET" && url.pathname.endsWith("/git/trees/base-tree")) {
              response = { sha: "base-tree", tree: [], truncated: false };
            } else if (request.method === "GET" && url.pathname.endsWith("/git/trees/head-tree")) {
              response = {
                sha: "head-tree",
                tree: [],
                truncated: failureKind === "truncated-tree",
              };
            } else if (request.method === "POST" && url.pathname.endsWith("/reviews")) {
              const publishedWire = decodePublishedReview(request);

              response = { html_url: "https://github.test/reve-ai/example/pull/12#review" };

              return Ref.update(publishedWires, (current) => [...current, publishedWire]).pipe(
                Effect.andThen(recordJsonResponse(requests, request, url, response)),
              );
            } else {
              return Effect.die(`unexpected request ${request.method} ${url.href}`);
            }

            return recordJsonResponse(requests, request, url, response);
          });

          const exit = yield* runReviewAction(
            client,
            failureKind === "missing-guidance"
              ? { PR_REVIEW_GUIDANCE_FILE: "/missing/effect-agent-review-guidance.md" }
              : {},
          ).pipe(Effect.exit);

          expect(Exit.isFailure(exit)).toBe(true);
          const wires = yield* Ref.get(publishedWires);

          if (failureKind === "truncated-tree") {
            if (!Exit.isFailure(exit)) return;
            expect(Option.getOrUndefined(Cause.findErrorOption(exit.cause))).toMatchObject({
              _tag: "GitHubApiFailure",
              operation: "get recursive Git tree",
              reason: "GitHub truncated tree head-tree",
            });
            expect(wires).toEqual([
              {
                commit_id: "head",
                event: "COMMENT",
                body: expect.stringContaining("A GitHub repository request failed."),
                comments: [],
              },
            ]);
          } else {
            expect(wires).toHaveLength(1);
            expect(wires[0]).toEqual({
              commit_id: "head",
              event: "COMMENT",
              body: expect.stringContaining(
                "<!-- effect-agent-review:v3 automatic=true completed=false -->",
              ),
              comments: [],
            });
          }
        }
      }),
  );

  it.effect("keeps explicit incremental review strict when the merge base moved", () =>
    Effect.gen(function* () {
      const targetHead = "target-head";
      const reviewedHead = "reviewed-head";
      const currentHead = "current-head";
      const priorMergeBase = "prior-merge-base";
      const currentMergeBase = "current-merge-base";
      const requests = yield* Ref.make<ReadonlyArray<string>>([]);
      const publishedWires = yield* Ref.make<ReadonlyArray<typeof PublishedReviewBody.Type>>([]);

      const client = HttpClient.make((request, url) => {
        if (request.method === "GET" && url.pathname.endsWith("/pulls/12")) {
          return recordJsonResponse(
            requests,
            request,
            url,
            pullRequestWire("Rebase onto an updated target", targetHead, currentHead),
          );
        }
        if (request.method === "GET" && url.pathname.endsWith("/pulls/12/reviews")) {
          return recordJsonResponse(requests, request, url, [
            reviewHistoryWire(1, reviewMarker(false), reviewedHead, "2026-08-25T00:00:00Z"),
          ]);
        }
        if (request.method === "GET" && url.pathname.endsWith("/pulls/12/files")) {
          return recordJsonResponse(requests, request, url, [
            {
              filename: "src/shared.ts",
              status: "modified",
              additions: 1,
              deletions: 1,
              patch: "@@ -1 +1 @@\n-target\n+pull",
            },
          ]);
        }
        if (
          request.method === "GET" &&
          url.pathname.endsWith(`/compare/${targetHead}...${currentHead}`)
        ) {
          return recordJsonResponse(requests, request, url, {
            merge_base_commit: { sha: currentMergeBase },
          });
        }
        if (
          request.method === "GET" &&
          url.pathname.endsWith(`/compare/${targetHead}...${reviewedHead}`)
        ) {
          return recordJsonResponse(requests, request, url, {
            merge_base_commit: { sha: priorMergeBase },
          });
        }
        if (request.method === "POST" && url.pathname.endsWith("/pulls/12/reviews")) {
          const wire = decodePublishedReview(request);

          return Ref.update(publishedWires, (current) => [...current, wire]).pipe(
            Effect.andThen(
              recordJsonResponse(requests, request, url, {
                html_url: "https://github.test/reve-ai/example/pull/12#review",
              }),
            ),
          );
        }

        return Effect.die(`unexpected request ${request.method} ${url.href}`);
      });

      const exit = yield* runReviewAction(client, { PR_REVIEW_MODE: "incremental" }).pipe(
        Effect.exit,
      );

      expect(Exit.isFailure(exit)).toBe(true);
      if (!Exit.isFailure(exit)) return;
      expect(Option.getOrUndefined(Cause.findErrorOption(exit.cause))).toEqual(
        IncrementalScopeUnavailable.make({ priorMergeBase, currentMergeBase }),
      );
      const observed = yield* Ref.get(requests);

      expect(observed.filter((value) => value.includes("/compare/"))).toHaveLength(2);
      expect(observed.some((value) => value.includes("/git/trees/"))).toBe(false);
      expect(observed.some((value) => value.includes("/git/blobs/"))).toBe(false);
      const published = yield* Ref.get(publishedWires);

      expect(published[0]?.body).toContain("The merge base changed.");
      expect(published).toEqual([
        {
          commit_id: currentHead,
          event: "COMMENT",
          body: expect.stringContaining(reviewMarker(false, false)),
          comments: [],
        },
      ]);
    }),
  );

  it.effect.each(["auto", "full"] as const)(
    "establishes a full baseline after merge-base movement in %s mode",
    (mode) =>
      Effect.gen(function* () {
        const targetHead = "target-head";
        const reviewedHead = "reviewed-head";
        const currentHead = "current-head";
        const currentMergeBase = "current-merge-base";
        const requests = yield* Ref.make<ReadonlyArray<string>>([]);
        const publishedWires = yield* Ref.make<ReadonlyArray<typeof PublishedReviewBody.Type>>([]);

        const client = HttpClient.make((request, url) => {
          if (request.method === "GET" && url.pathname.endsWith("/pulls/12")) {
            return recordJsonResponse(
              requests,
              request,
              url,
              pullRequestWire("Review the rebased pull request in full", targetHead, currentHead),
            );
          }
          if (request.method === "GET" && url.pathname.endsWith("/pulls/12/reviews")) {
            return recordJsonResponse(requests, request, url, [
              reviewHistoryWire(1, reviewMarker(true), reviewedHead, "2026-08-25T00:00:00Z"),
              ...(mode === "full"
                ? [
                    reviewHistoryWire(
                      2,
                      reviewMarker(true, false),
                      currentHead,
                      "2026-08-26T00:00:00Z",
                    ),
                  ]
                : []),
            ]);
          }
          if (request.method === "GET" && url.pathname.endsWith("/pulls/12/files")) {
            return recordJsonResponse(requests, request, url, []);
          }
          if (
            request.method === "GET" &&
            url.pathname.endsWith(`/compare/${targetHead}...${currentHead}`)
          ) {
            return recordJsonResponse(requests, request, url, {
              merge_base_commit: { sha: currentMergeBase },
            });
          }
          if (
            request.method === "GET" &&
            url.pathname.endsWith(`/compare/${targetHead}...${reviewedHead}`)
          ) {
            return recordJsonResponse(requests, request, url, {
              merge_base_commit: { sha: "prior-merge-base" },
            });
          }
          if (
            request.method === "GET" &&
            url.pathname.endsWith(`/git/commits/${currentMergeBase}`)
          ) {
            return recordJsonResponse(requests, request, url, {
              sha: currentMergeBase,
              tree: { sha: "base-tree" },
            });
          }
          if (request.method === "GET" && url.pathname.endsWith(`/git/commits/${currentHead}`)) {
            return recordJsonResponse(requests, request, url, {
              sha: currentHead,
              tree: { sha: "head-tree" },
            });
          }
          if (
            request.method === "GET" &&
            (url.pathname.endsWith("/git/trees/base-tree") ||
              url.pathname.endsWith("/git/trees/head-tree"))
          ) {
            const sha = url.pathname.endsWith("base-tree") ? "base-tree" : "head-tree";

            return recordJsonResponse(requests, request, url, { sha, tree: [], truncated: false });
          }
          if (request.method === "POST" && url.pathname.endsWith("/pulls/12/reviews")) {
            const wire = decodePublishedReview(request);

            return Ref.update(publishedWires, (current) => [...current, wire]).pipe(
              Effect.andThen(
                recordJsonResponse(requests, request, url, {
                  html_url: "https://github.test/reve-ai/example/pull/12#review",
                }),
              ),
            );
          }

          return Effect.die(`unexpected request ${request.method} ${url.href}`);
        });

        const exit = yield* runReviewAction(client, { PR_REVIEW_MODE: mode }).pipe(Effect.exit);

        expect(Exit.isSuccess(exit)).toBe(true);
        const observed = yield* Ref.get(requests);

        expect(observed.filter((value) => value.includes("/compare/"))).toEqual([
          `GET /repos/reve-ai/example/compare/${targetHead}...${currentHead}`,
          ...(mode === "auto"
            ? [`GET /repos/reve-ai/example/compare/${targetHead}...${reviewedHead}`]
            : []),
        ]);
        expect(observed.some((value) => value.includes(`/git/commits/${reviewedHead}`))).toBe(
          false,
        );
        expect(yield* Ref.get(publishedWires)).toEqual([
          {
            commit_id: currentHead,
            event: "COMMENT",
            body: expect.stringContaining(reviewMarker(mode === "auto", true)),
            comments: [],
          },
        ]);
        expect((yield* Ref.get(publishedWires))[0]?.body).toContain("**Full diff**");
      }),
  );

  it.effect("PRR-007 keeps a manual review incremental after a force-push", () =>
    Effect.gen(function* () {
      const reviewedHead = "1".repeat(40);
      const currentHead = "2".repeat(40);
      const reviewedTree = "3".repeat(40);
      const currentTree = "4".repeat(40);
      const unchangedBlob = "5".repeat(40);
      const targetHead = "0".repeat(40);
      const targetTree = "6".repeat(40);
      const targetBlob = "7".repeat(40);
      const requests = yield* Ref.make<ReadonlyArray<string>>([]);
      const publishedBodies = yield* Ref.make<ReadonlyArray<string>>([]);

      const treeBlobs = new Map([
        [targetTree, targetBlob],
        [reviewedTree, unchangedBlob],
        [currentTree, unchangedBlob],
      ]);

      const client = HttpClient.make((request, url) => {
        const requestedTree = [...treeBlobs].find(([tree]) => url.pathname.endsWith(tree));
        let body: unknown;

        if (request.method === "POST" && url.pathname.endsWith("/reactions")) {
          body = { id: 1, content: "eyes" };
        } else if (request.method === "GET" && url.pathname.endsWith("/pulls/12")) {
          body = pullRequestWire("Rewrite the branch", targetHead, currentHead);
        } else if (request.method === "GET" && url.pathname.endsWith("/pulls/12/reviews")) {
          body = [reviewHistoryWire(1, reviewMarker(false), reviewedHead, "2026-08-25T00:00:00Z")];
        } else if (request.method === "GET" && url.pathname.endsWith("/pulls/12/files")) {
          body = [
            {
              filename: "src/unchanged.ts",
              status: "modified",
              additions: 1,
              deletions: 1,
              patch: "@@ -1 +1 @@\n-old\n+new",
            },
          ];
        } else if (request.method === "GET" && url.pathname.includes("/compare/")) {
          body = { merge_base_commit: { sha: targetHead } };
        } else if (request.method === "GET" && url.pathname.endsWith(targetHead)) {
          body = { sha: targetHead, tree: { sha: targetTree } };
        } else if (request.method === "GET" && url.pathname.endsWith(reviewedHead)) {
          body = { sha: reviewedHead, tree: { sha: reviewedTree } };
        } else if (request.method === "GET" && url.pathname.endsWith(currentHead)) {
          body = { sha: currentHead, tree: { sha: currentTree } };
        } else if (request.method === "GET" && requestedTree !== undefined) {
          const [tree, blob] = requestedTree;

          body = {
            sha: tree,
            truncated: false,
            tree: [
              {
                path: "src",
                mode: "040000",
                type: "tree",
                sha: `${tree}-src`,
              },
              {
                path: "src/unchanged.ts",
                mode: "100644",
                type: "blob",
                sha: blob,
                size: 1,
              },
            ],
          };
        } else if (request.method === "POST" && url.pathname.endsWith("/pulls/12/reviews")) {
          const published = decodePublishedReview(request);

          body = { html_url: "https://github.test/reve-ai/example/pull/12#review" };

          return Ref.update(publishedBodies, (current) => [...current, published.body]).pipe(
            Effect.andThen(recordJsonResponse(requests, request, url, body)),
          );
        } else {
          return Effect.die(new Error(`unexpected request ${request.method} ${url.href}`));
        }

        return recordJsonResponse(requests, request, url, body);
      });

      yield* runReviewAction(client, {
        PR_REVIEW_COMMAND: "@effect-agent review",
        PR_REVIEW_COMMENT_ID: "42",
      });

      const requested = yield* Ref.get(requests);
      const published = yield* Ref.get(publishedBodies);

      expect(requested.some((request) => request.includes("/compare/"))).toBe(true);
      expect(requested.filter((request) => request.includes("/git/trees/"))).toHaveLength(2);
      expect(published).toHaveLength(1);
      expect(published[0]).toContain("| **Incremental** | 0 reviewed | ✅ None |");
      expect(published[0]).toContain(
        "No pull-request files changed since the last completed review.",
      );
    }),
  );
});

const noGeneratedFiles = Layer.succeed(GeneratedFileClassification, {
  isGenerated: () => Effect.succeed(false),
});

layer(noGeneratedFiles)("exact review delta", (it) => {
  const treeSnapshot = (
    revision: string,
    files: Readonly<Record<string, string>>,
    forbiddenReads: ReadonlySet<string> = new Set(),
  ): RepositorySnapshot => ({
    revision,
    paths: Object.keys(files).sort(),
    entry: (path) =>
      path in files
        ? {
            sha: `${revision}-${path}`,
            mode: "100644",
            type: "blob",
            size: (files[path] ?? "").length,
          }
        : undefined,
    readTextFile: (path) =>
      forbiddenReads.has(path)
        ? Effect.die(`must not hydrate ${path}`)
        : Effect.succeed(files[path] ?? ""),
  });

  // Regression: deleted dependency patches exhausted review spend on embedded source maps.
  it.effect(
    "omits embedded source maps before spending admission without shifting source anchors",
    () =>
      Effect.gen(function* () {
        const path = "patches/dependency.patch";

        const map = JSON.stringify({
          version: 3,
          sources: ["../src/value.ts"],
          names: [],
          mappings: "AAAA;".repeat(70_000),
        });

        const dependencyPatch = [
          "diff --git a/dist/value.js.map b/dist/value.js.map",
          "--- a/dist/value.js.map",
          "+++ b/dist/value.js.map",
          "@@ -1 +1 @@",
          `-${map}`,
          `+${map}`,
          "diff --git a/src/value.ts b/src/value.ts",
          "--- a/src/value.ts",
          "+++ b/src/value.ts",
          "@@ -1 +1 @@",
          "-export const value = 1;",
          "+export const value = 2;",
          "",
        ].join("\n");

        for (const [before, after, omittedLines] of [
          [dependencyPatch, "", 2],
          ["", dependencyPatch, 2],
          [dependencyPatch, dependencyPatch.replace("value = 2", "value = 3"), 0],
          [
            dependencyPatch,
            dependencyPatch.replaceAll("AAAA;", "CAAA;").replace("value = 2", "value = 3"),
            4,
          ],
        ] as const) {
          const surface = yield* hydrateExactChanges({
            files: [file(path, undefined)],
            changedPaths: [path],
            ignore: [],
            base: treeSnapshot("base", before === "" ? {} : { [path]: before }),
            head: treeSnapshot("head", after === "" ? {} : { [path]: after }),
          });

          const patch = surface.changes[0]?.patch ?? "";

          const original =
            makeExactPatch({ path, baseRevision: "base", headRevision: "head", before, after }) ??
            "";

          const coordinates = (text: string) =>
            parsePatch(text).flatMap((item) =>
              item.hunks.map(({ oldStart, oldLines, newStart, newLines }) => ({
                oldStart,
                oldLines,
                newStart,
                newLines,
              })),
            );

          expect(coordinates(patch)).toEqual(coordinates(original));
          expect(patch).toContain("export const value =");
          expect(patch).not.toContain(map);
          expect(patch.length).toBeLessThan(2_000);
          expect(surface.unreviewedPaths).toEqual([]);
          expect(surface.ignoredPaths).toEqual([]);
          expect(surface.generatedContent).toEqual(
            omittedLines === 0
              ? []
              : [{ path, lines: omittedLines, characters: map.length * omittedLines }],
          );
          expect(reviewCostLimitMicrousd({ changes: surface.changes }, 20, 4)).toBeLessThan(
            4_020_000,
          );
        }
      }),
  );

  it.effect("retains malformed maps and patches, non-map JSON, and generated-marker source", () =>
    Effect.gen(function* () {
      const validMap = '{"version":3,"sources":[],"mappings":"AAAA"}';

      for (const [path, nestedPath, payload, hunk] of [
        [
          "patches/dependency.patch",
          "dist/value.js.map",
          '{"version":3,"mappings":42}',
          "@@ -0,0 +1 @@",
        ],
        ["patches/dependency.patch", "dist/value.js.map", validMap, "@@ -0,0 +1,2 @@"],
        ["patches/dependency.patch", "src/value.json", validMap, "@@ -0,0 +1 @@"],
        ["docs/example.txt", "dist/value.js.map", validMap, "@@ -0,0 +1 @@"],
      ]) {
        const after = `diff --git a/${nestedPath} b/${nestedPath}\n--- /dev/null\n+++ b/${nestedPath}\n${hunk}\n+${payload}\n`;

        const surface = yield* hydrateExactChanges({
          files: [file(path, undefined)],
          changedPaths: [path],
          ignore: [],
          base: treeSnapshot("base", {}),
          head: treeSnapshot("head", { [path]: after }),
        });

        expect(surface.changes[0]?.patch).toContain(payload);
        expect(surface.generatedContent).toEqual([]);
      }
      const source = "// @generated do not edit\nexport const bypassAuthorization = true;\n";

      const surface = yield* hydrateExactChanges({
        files: [file("src/auth.ts", undefined)],
        changedPaths: ["src/auth.ts"],
        ignore: [],
        base: treeSnapshot("base", {}),
        head: treeSnapshot("head", { "src/auth.ts": source }),
      });

      expect(surface.changes[0]?.patch).toContain("bypassAuthorization");
      expect(surface.generatedContent).toEqual([]);
    }),
  );

  it.effect("keeps review capacity after exhausting generated classification requests", () =>
    Effect.gen(function* () {
      const generated = Array.from(
        { length: 100 },
        (_, index) => `generated/${String(index).padStart(3, "0")}.ts`,
      );

      const source = Array.from(
        { length: 101 },
        (_, index) => `src/${String(index).padStart(3, "0")}.ts`,
      );

      const paths = [...generated, ...source];

      const base = treeSnapshot(
        "base",
        Object.fromEntries(paths.map((path) => [path, "export const value = 1;\n"])),
        new Set(generated),
      );

      const head = treeSnapshot(
        "head",
        Object.fromEntries(
          [...generated.slice(50), ...source].map((path) => [path, "export const value = 2;\n"]),
        ),
        new Set(generated),
      );

      const hydration = hydrateExactChanges({
        files: paths.map((path) => file(path, undefined)),
        changedPaths: paths,
        base,
        head,
        ignore: [],
      });

      expectTypeOf<
        Effect.Services<typeof hydration>
      >().toEqualTypeOf<GeneratedFileClassification>();
      expectTypeOf<Effect.Error<typeof hydration>>().toEqualTypeOf<GitHubApiFailure>();
      const classified: Array<string> = [];

      const surface = yield* hydration.pipe(
        Effect.provideService(GeneratedFileClassification, {
          isGenerated: (path) =>
            Effect.sync(() => {
              classified.push(path);

              return path.startsWith("generated/");
            }),
        }),
      );

      expect(classified).toEqual(generated);
      expect(surface.ignoredPaths).toEqual(generated);
      expect(surface.changes.map((change) => change.path)).toEqual(source);
      expect(surface.unreviewedPaths).toEqual([]);
      expect(surface.exclusions).toEqual([]);
    }),
  );

  it.effect("ignores a deleted generated bundle before reading it, even after policy removal", () =>
    Effect.gen(function* () {
      const path = "retired-action/dist/index.mjs";

      const base = treeSnapshot(
        "trusted-base",
        {
          [path]: "x".repeat(2_138_832),
          ".gitattributes": "retired-action/dist/** linguist-generated=true\n",
        },
        new Set([path]),
      );

      const head = treeSnapshot("head", {});

      const surface = yield* hydrateExactChanges({
        files: base.paths.map((path) => ({ ...file(path, undefined), status: "removed" })),
        changedPaths: base.paths,
        base,
        head,
        ignore: [],
      }).pipe(
        Effect.provideService(GeneratedFileClassification, {
          isGenerated: (candidate) => Effect.succeed(candidate === path),
        }),
      );

      expect(surface.ignoredPaths).toEqual([path]);
      expect(surface.unreviewedPaths).toEqual([]);
      expect(surface.changes.map((change) => change.path)).toEqual([".gitattributes"]);

      const repository = makeReviewRepository({
        base,
        head,
        ignore: [],
        unavailablePaths: surface.unavailablePaths,
      });

      expect(
        yield* repository
          .readFile({ path, revision: "base", startLine: 1, lineCount: 1 })
          .pipe(Effect.flip),
      ).toMatchObject({ _tag: "ReviewContextError" });
    }),
  );

  it.effect("reviews new paths, renames, and mode changes despite generated classifications", () =>
    Effect.gen(function* () {
      const renamed: ChangedFile = {
        ...file("src/new.ts", undefined),
        previousPath: "generated/old.ts",
        status: "renamed",
      };

      const before = treeSnapshot("base", {
        "generated/old.ts": "export const value = 1;\n",
        "generated/mode.ts": "export const mode = 1;\n",
      });

      const after = treeSnapshot("head", {
        "src/new.ts": "export const value = 1;\n",
        "generated/added.ts": "export const added = 1;\n",
        "generated/mode.ts": "export const mode = 1;\n",
      });

      const surface = yield* hydrateExactChanges({
        files: [
          renamed,
          file("generated/added.ts", undefined),
          file("generated/mode.ts", undefined),
        ],
        changedPaths: [...before.paths, ...after.paths],
        base: before,
        head: {
          ...after,
          entry: (path) => {
            const entry = after.entry(path);

            return path === "generated/mode.ts" && entry !== undefined
              ? { ...entry, mode: "100755" }
              : entry;
          },
        },
        ignore: [],
      }).pipe(
        Effect.provideService(GeneratedFileClassification, {
          isGenerated: () => Effect.die("structural changes must remain reviewable"),
        }),
      );

      expect(surface.ignoredPaths).toEqual([]);
      expect(surface.changes.map((change) => change.path)).toEqual([
        "generated/added.ts",
        "generated/mode.ts",
        "src/new.ts",
      ]);
    }),
  );

  it.effect("hydrates a provider-declared pure rename as one bounded old-to-new patch", () =>
    Effect.gen(function* () {
      const previousPath = "src/old-name.ts";
      const path = "src/new-name.ts";
      const unchanged = `export const value = "${"x".repeat(90_000)}";\n`;

      const renamed: ChangedFile = {
        ...file(path, undefined),
        previousPath,
        status: "renamed",
      };

      const surface = yield* hydrateExactChanges({
        files: [renamed],
        changedPaths: [previousPath, path],
        base: treeSnapshot("merge-base", { [previousPath]: unchanged }),
        head: treeSnapshot("head", { [path]: unchanged }),
        ignore: [],
      });

      expect(surface.changes).toHaveLength(1);
      expect(surface.unreviewedPaths).toEqual([]);
      expect(surface.changes[0]?.path).toBe(path);
      expect(surface.changes[0]?.patch).toContain(`rename from ${previousPath}`);
      expect(surface.changes[0]?.patch).toContain(`rename to ${path}`);
      expect(surface.changes[0]?.patch.length).toBeLessThan(1_000);
      expect(surface.changes[0]?.patch).not.toContain("x".repeat(1_000));
    }),
  );

  it.effect("ignores binary assets before hydration and preserves capacity for text assets", () =>
    Effect.gen(function* () {
      const binaries = [
        ...Array.from({ length: 101 }, (_, index) => `assets/${index}.PNG`),
        "assets/font.woff2",
        "assets/video.mp4",
        "assets/archive.zip",
        "assets/manual.pdf",
      ];

      const text = { "src/main.ts": "export const value = 1;", "assets/icon.svg": "<svg/>" };
      const base = treeSnapshot("base", {});

      const head = treeSnapshot(
        "head",
        {
          ...Object.fromEntries(binaries.map((path) => [path, "not text"])),
          ...text,
          "assets/unchanged.jpg": "not text",
        },
        new Set([...binaries, "assets/unchanged.jpg"]),
      );

      const surface = yield* hydrateExactChanges({
        files: [],
        changedPaths: [...binaries, ...Object.keys(text)],
        base,
        head,
        ignore: [],
      });

      expect(surface.ignoredPaths).toEqual([...binaries].sort());
      expect(surface.unreviewedPaths).toEqual([]);
      expect(surface.changes.map(({ path }) => path)).toEqual(Object.keys(text).sort());

      const repository = makeReviewRepository({
        base,
        head,
        ignore: [],
        unavailablePaths: surface.unavailablePaths,
      });

      expect((yield* repository.findFiles({ revision: "head", query: "assets/" })).paths).toEqual([
        "assets/icon.svg",
      ]);
      expect(
        yield* repository
          .readFile({ revision: "head", path: "assets/unchanged.jpg", startLine: 1, lineCount: 1 })
          .pipe(Effect.flip),
      ).toMatchObject({ _tag: "ReviewContextError" });
    }),
  );

  it.effect.each([false, true])("preserves the textual side of a binary rename: %s", (reverse) =>
    Effect.gen(function* () {
      const previousPath = reverse ? "src/icon.ts" : "assets/icon.png";
      const path = reverse ? "assets/icon.png" : "src/icon.ts";
      const content = "export const icon = true;\n";
      const base = treeSnapshot("base", { [previousPath]: content }, new Set(["assets/icon.png"]));
      const head = treeSnapshot("head", { [path]: content }, new Set(["assets/icon.png"]));

      const surface = yield* hydrateExactChanges({
        files: [{ ...file(path, undefined), previousPath, status: "renamed" }],
        changedPaths: [previousPath, path],
        base,
        head,
        ignore: [],
      });

      expect(surface.ignoredPaths).toEqual(["assets/icon.png"]);
      expect(surface.unreviewedPaths).toEqual([]);
      expect(surface.changes).toHaveLength(1);
      expect(surface.changes[0]?.path).toBe("src/icon.ts");
      expect(surface.changes[0]?.patch).toContain(`${reverse ? "-" : "+"}${content}`);
      expect(surface.changes[0]?.patch).not.toContain("rename from");

      const repository = makeReviewRepository({
        base,
        head,
        ignore: [],
        unavailablePaths: surface.unavailablePaths,
      });

      expect(
        (yield* repository.findFiles({ revision: reverse ? "base" : "head", query: "src/" })).paths,
      ).toEqual(["src/icon.ts"]);
      yield* repository.readFile({
        revision: reverse ? "base" : "head",
        path: "src/icon.ts",
        startLine: 1,
        lineCount: 1,
      });
    }),
  );

  it.effect.each(
    (["addition", "deletion", "binary", "failure"] as const).flatMap((mode) =>
      [false, true].map((renamed) => ({ mode, renamed })),
    ),
  )(
    "preserves text and read failures beside content-detected binaries: $mode, renamed=$renamed",
    ({ mode, renamed }) =>
      Effect.gen(function* () {
        const path = "assets/unknown";
        const previousPath = renamed ? "assets/old" : path;
        const textPath = mode === "deletion" ? previousPath : path;

        const surface = yield* hydrateExactChanges({
          files: renamed ? [{ ...file(path, undefined), previousPath, status: "renamed" }] : [],
          changedPaths: renamed ? [previousPath, path] : [path],
          ignore: [],
          base: {
            ...treeSnapshot("base", { [previousPath]: "binary" }),
            readTextFile: () =>
              mode === "deletion"
                ? Effect.succeed("text")
                : Effect.fail(BinaryBlob.make({ sha: "binary" })),
          },
          head: {
            ...treeSnapshot("head", { [path]: "text" }),
            readTextFile: () =>
              mode === "failure"
                ? Effect.fail(
                    GitHubApiFailure.make({ operation: "get Git blob", reason: "unavailable" }),
                  )
                : mode === "addition"
                  ? Effect.succeed("text")
                  : Effect.fail(BinaryBlob.make({ sha: "binary" })),
          },
        });

        expect(surface.ignoredPaths).toEqual(mode === "binary" ? [path] : []);
        expect(surface.unreviewedPaths).toEqual(mode === "failure" ? [path] : []);
        if (mode === "addition" || mode === "deletion") {
          expect(surface.changes).toHaveLength(1);
          expect(surface.changes[0]?.path).toBe(textPath);
          expect(surface.changes[0]?.patch).toContain(mode === "addition" ? "+text" : "-text");
          expect(surface.changes[0]?.patch).not.toContain("rename from");
          expect(surface.unavailablePaths.has(textPath)).toBe(false);
        } else {
          expect(surface.changes).toEqual([]);
          expect(surface.unavailablePaths.has(path)).toBe(true);
        }
      }),
  );

  it.effect("keeps expected blob read failures as coverage gaps and admits other files", () =>
    Effect.gen(function* () {
      const logs: Array<unknown> = [];
      const paths = ["src/a.ts", "src/b.ts"];
      const head = treeSnapshot("head", { "src/a.ts": "a", "src/b.ts": "b" });

      const surface = yield* hydrateExactChanges({
        files: paths.map((path) => file(path, undefined)),
        changedPaths: paths,
        base: treeSnapshot("base", {}),
        head: {
          ...head,
          readTextFile: (path) =>
            path === "src/a.ts"
              ? Effect.fail(
                  GitHubApiFailure.make({ operation: "readTextFile", reason: "blob unavailable" }),
                )
              : head.readTextFile(path),
        },
        ignore: [],
      }).pipe(
        Effect.provide(
          Logger.layer([Logger.make<unknown, void>(({ message }) => logs.push(message))]),
        ),
      );

      expect(logs).toContainEqual([
        "Review source read failed",
        expect.objectContaining({
          path: "src/a.ts",
          baseRevision: "base",
          headRevision: "head",
          operation: "readTextFile",
          reason: "blob unavailable",
        }),
      ]);
      expect(surface.unreviewedPaths).toEqual(["src/a.ts"]);
      expect(surface.exclusions).toEqual([{ path: "src/a.ts", reason: "source-read-failed" }]);
      expect([...surface.unavailablePaths]).toEqual(["src/a.ts"]);
      expect(surface.changes.map((change) => change.path)).toEqual(["src/b.ts"]);
    }),
  );

  it.effect.each([
    { cause: Cause.die("adapter defect"), expected: { _tag: "Die", defect: "adapter defect" } },
    { cause: Cause.interrupt(7335), expected: { _tag: "Interrupt", fiberId: 7335 } },
  ])("preserves a blob read cause and stops hydration: $expected._tag", ({ cause, expected }) =>
    Effect.gen(function* () {
      const paths = ["src/a.ts", "src/b.ts"];
      const reads: Array<string> = [];

      const exit = yield* hydrateExactChanges({
        files: paths.map((path) => file(path, undefined)),
        changedPaths: paths,
        base: treeSnapshot("base", {}),
        head: {
          ...treeSnapshot("head", { "src/a.ts": "a", "src/b.ts": "b" }),
          readTextFile: (path) =>
            Effect.sync(() => reads.push(path)).pipe(Effect.andThen(Effect.failCause(cause))),
        },
        ignore: [],
      }).pipe(Effect.exit);

      if (Exit.isSuccess(exit)) throw new Error("Expected hydration to fail");
      expect(exit.cause.reasons).toMatchObject([expected]);
      expect(reads).toEqual(["src/a.ts"]);
    }),
  );

  it.effect(
    "treats either ignored rename alias as one ignored change and denies both sources",
    () =>
      Effect.gen(function* () {
        const previousPath = "generated/old.ts";
        const path = "src/new.ts";
        const content = "export const generated = true;\n";

        const renamed: ChangedFile = {
          ...file(path, undefined),
          previousPath,
          status: "renamed",
        };

        for (const ignoredAlias of [previousPath, path]) {
          const base = treeSnapshot("base", { [previousPath]: content });
          const head = treeSnapshot("head", { [path]: content });

          const surface = yield* hydrateExactChanges({
            files: [renamed],
            changedPaths: [previousPath, path],
            base,
            head,
            ignore: [ignoredAlias],
          });

          const repository = makeReviewRepository({
            base,
            head,
            ignore: [ignoredAlias],
            unavailablePaths: surface.unavailablePaths,
          });

          expect(surface.ignoredPaths).toEqual([path]);
          expect(surface.changes).toEqual([]);
          expect(
            yield* repository
              .readFile({ path: previousPath, revision: "base", startLine: 1, lineCount: 1 })
              .pipe(Effect.flip),
          ).toMatchObject({ _tag: "ReviewContextError" });
          expect(
            yield* repository
              .readFile({ path, revision: "head", startLine: 1, lineCount: 1 })
              .pipe(Effect.flip),
          ).toMatchObject({ _tag: "ReviewContextError" });
        }
      }),
  );

  it.effect("uses the current name after an incremental baseline already contains the rename", () =>
    Effect.gen(function* () {
      const previousPath = "src/old-name.ts";
      const path = "src/new-name.ts";

      const renamed: ChangedFile = {
        ...file(path, undefined),
        previousPath,
        status: "renamed",
      };

      const surface = yield* hydrateExactChanges({
        files: [renamed],
        changedPaths: [path],
        base: treeSnapshot("reviewed-head", { [path]: "export const value = 1;\n" }),
        head: treeSnapshot("current-head", { [path]: "export const value = 2;\n" }),
        ignore: [],
      });

      expect(surface.changes[0]?.patch).toContain(`--- a/${path}`);
      expect(surface.changes[0]?.patch).not.toContain("rename from");
      expect(surface.changes[0]?.patch).toContain("+export const value = 2;");
    }),
  );

  it.effect("does not read ignored paths or candidates beyond the 1,000-file admission bound", () =>
    Effect.gen(function* () {
      const paths = Array.from(
        { length: 1_002 },
        (_, index) => `src/file-${String(index).padStart(4, "0")}.ts`,
      );

      const ignored = paths[0] ?? "";
      const afterCap = paths.at(-1) ?? "";

      const contents = Object.fromEntries(
        paths.map((path) => [path, `export const p = '${path}';\n`]),
      );

      const surface = yield* hydrateExactChanges({
        files: paths.map((path) => file(path, undefined)),
        changedPaths: paths,
        base: treeSnapshot("base", {}),
        head: treeSnapshot("head", contents, new Set([ignored, afterCap])),
        ignore: [ignored],
      });

      expect(surface.changes).toHaveLength(1_000);
      expect(surface.ignoredPaths).toEqual([ignored]);
      expect(surface.unreviewedPaths).toEqual([afterCap]);
      expect(surface.exclusions).toEqual([{ path: afterCap, reason: "file-limit" }]);
    }),
  );

  it.effect("keeps usable file slots after unsupported candidates", () =>
    Effect.gen(function* () {
      const unsupported = Array.from({ length: 1_000 }, (_, index) => `links/${String(index)}.ts`);
      const path = "src/valid.ts";
      const head = treeSnapshot("head", { [path]: "export const valid = true;\n" });

      const surface = yield* hydrateExactChanges({
        files: [file(path, undefined)],
        changedPaths: [...unsupported, path],
        base: treeSnapshot("base", {}),
        head: {
          ...head,
          entry: (candidate) =>
            candidate.startsWith("links/")
              ? { sha: candidate, mode: "120000", type: "blob", size: 0 }
              : head.entry(candidate),
        },
        ignore: [],
      });

      expect(surface.changes.map((change) => change.path)).toEqual([path]);
      expect(surface.exclusions).toHaveLength(1_000);
      expect(surface.exclusions.every(({ reason }) => reason === "unsupported-entry")).toBe(true);
    }),
  );

  it.effect("bounds expanded patch text while retaining smaller later changes", () =>
    Effect.gen(function* () {
      const largePaths = Array.from({ length: 5 }, (_, index) => `src/large-${String(index)}.ts`);
      const smallPath = "src/z-small.ts";
      const source = `${"x".repeat(39)}\n`.repeat(39_500);

      const contents = Object.fromEntries([
        ...largePaths.map((path) => [path, source]),
        [smallPath, "export const valid = true;\n"],
      ]);

      const surface = yield* hydrateExactChanges({
        files: [...largePaths, smallPath].map((path) => file(path, undefined)),
        changedPaths: [...largePaths, smallPath],
        base: treeSnapshot("base", {}),
        head: treeSnapshot("head", contents),
        ignore: [],
      });

      expect(surface.changes.map((change) => change.path)).toEqual([
        ...largePaths.slice(0, 4),
        smallPath,
      ]);
      expect(surface.exclusions).toEqual([{ path: "src/large-4.ts", reason: "patch-total-limit" }]);
      expect(surface.unavailablePaths.has("src/large-4.ts")).toBe(false);
    }),
  );

  it.effect(
    "admits large documentation changes without excluding implementation or later patches",
    () =>
      Effect.gen(function* () {
        const paths = ["docs/a.md", "docs/b.md", "docs/c.md", "docs/d.md", "src/e.ts"];

        const surface = yield* hydrateExactChanges({
          files: paths.map((path) => file(path, undefined)),
          changedPaths: paths,
          base: treeSnapshot("base", {}),
          head: treeSnapshot("head", {
            "docs/a.md": "a".repeat(70_000),
            "docs/b.md": "b".repeat(70_000),
            "docs/c.md": "c".repeat(70_000),
            "docs/d.md": "d".repeat(70_000),
            "src/e.ts": "export const corrected = true;",
          }),
          ignore: [],
        });

        expect(surface.changes.map((change) => change.path)).toEqual([
          "src/e.ts",
          "docs/a.md",
          "docs/b.md",
          "docs/c.md",
          "docs/d.md",
        ]);
        expect(surface.unreviewedPaths).toEqual([]);
        expect(surface.exclusions).toEqual([]);
      }),
  );

  it.effect.each(["added", "removed"] as const)(
    "admits complete large %s files beyond the former batch size",
    (status) =>
      Effect.gen(function* () {
        const contents: Record<string, string> = {
          "formal/DurableSubmission.tla": "state transition and invariant definition\n".repeat(
            7_000,
          ),
          "formal/SubagentEstablishment.tla":
            "child establishment and ownership invariant\n".repeat(6_000),
        };

        const paths = Object.keys(contents);

        const surface = yield* hydrateExactChanges({
          files: paths.map((path) => ({ ...file(path, undefined), status })),
          changedPaths: paths,
          base: treeSnapshot("base", status === "removed" ? contents : {}),
          head: treeSnapshot("head", status === "added" ? contents : {}),
          ignore: [],
        });

        expect(surface.exclusions).toEqual([]);
        expect(surface.unreviewedPaths).toEqual([]);
        expect(surface.changes.map((change) => change.path)).toEqual(paths);
        for (const change of surface.changes) {
          const source = contents[change.path] ?? "";
          const prefix = status === "added" ? "+" : "-";

          expect(change.patch.length).toBeGreaterThan(256_000);
          expect(change.patch).toContain(
            source
              .trimEnd()
              .split("\n")
              .map((line) => `${prefix}${line}`)
              .join("\n"),
          );
        }
      }),
  );

  it.effect("keeps oversized patches readable as context without claiming they were reviewed", () =>
    Effect.gen(function* () {
      const path = "src/large.ts";
      const base = treeSnapshot("base", {});

      const head = treeSnapshot("head", {
        [path]: `export const large = true;\n${"x".repeat(1_999_960)}`,
      });

      const surface = yield* hydrateExactChanges({
        files: [file(path, undefined)],
        changedPaths: [path],
        base,
        head,
        ignore: [],
      });

      const repository = makeReviewRepository({
        base,
        head,
        ignore: [],
        unavailablePaths: surface.unavailablePaths,
      });

      expect(surface.changes).toEqual([]);
      expect(surface.exclusions).toEqual([{ path, reason: "patch-limit" }]);
      expect((yield* repository.findFiles({ revision: "head", query: "large" })).paths).toEqual([
        path,
      ]);
      expect(
        (yield* repository.readFile({ path, revision: "head", startLine: 1, lineCount: 1 }))
          .content,
      ).toBe("export const large = true;");
    }),
  );

  it.effect("excludes an earlier same-file change from the incremental patch", () =>
    Effect.gen(function* () {
      const path = "src/example.ts";

      const reviewed = [
        "const defect = 'already reviewed';",
        ...Array.from(
          { length: 8 },
          (_, index) => `const line${String(index)} = ${String(index)};`,
        ),
        "export const tail = 1;",
      ].join("\n");

      const current = reviewed.replace("export const tail = 1;", "export const tail = 2;");

      const snapshot = (revision: string, sha: string, content: string): RepositorySnapshot => ({
        revision,
        paths: [path],
        entry: (candidate) =>
          candidate === path ? { sha, mode: "100644", type: "blob" } : undefined,
        readTextFile: () => Effect.succeed(content),
      });

      const files = yield* hydrateExactChanges({
        files: [file(path, "GitHub's base-to-head patch must not be reused")],
        changedPaths: [path],
        base: snapshot("reviewed-head", "a".repeat(40), reviewed),
        head: snapshot("current-head", "b".repeat(40), current),
        ignore: [],
      });

      expect(files.changes).toHaveLength(1);
      expect(files.changes[0]?.patch).toContain("+export const tail = 2;");
      expect(files.changes[0]?.patch).not.toContain("+const defect");
      expect(files.changes[0]?.patch).not.toContain("GitHub's base-to-head patch");
    }),
  );

  it.effect("keeps an unsupported changed tree entry unavailable", () =>
    Effect.gen(function* () {
      const path = "vendor/dependency";

      const base: RepositorySnapshot = {
        revision: "base",
        paths: [path],
        entry: () => ({ sha: "a".repeat(40), mode: "160000", type: "commit" }),
        readTextFile: () => Effect.die("must not read a submodule as text"),
      };

      const head: RepositorySnapshot = {
        ...base,
        revision: "head",
        entry: () => ({ sha: "b".repeat(40), mode: "160000", type: "commit" }),
      };

      const files = yield* hydrateExactChanges({
        files: [file(path, undefined)],
        changedPaths: [path],
        base,
        head,
        ignore: [],
      });

      expect(files.unreviewedPaths).toEqual([path]);
    }),
  );

  it.effect("hydrates a path reverted out of the current pull-request file list", () =>
    Effect.gen(function* () {
      const path = "src/reverted.ts";

      const snapshot = (revision: string, sha: string, content: string): RepositorySnapshot => ({
        revision,
        paths: [path],
        entry: () => ({ sha, mode: "100644", type: "blob" }),
        readTextFile: () => Effect.succeed(content),
      });

      const files = yield* hydrateExactChanges({
        files: [],
        changedPaths: [path],
        base: snapshot("reviewed-head", "a".repeat(40), "export const value = 'pr-change';\n"),
        head: snapshot("current-head", "b".repeat(40), "export const value = 'target';\n"),
        ignore: [],
      });

      expect(files.changes[0]?.path).toBe(path);
      expect(files.changes[0]?.patch).toContain("-export const value = 'pr-change';");
      expect(files.changes[0]?.patch).toContain("+export const value = 'target';");
      expect(files.changes).toHaveLength(1);
    }),
  );
});

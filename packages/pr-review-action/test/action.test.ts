import { ReviewRepository } from "@effect-agent/pr-review";
import { NodeServices } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import { Cause, Config, ConfigProvider, Effect, Exit, Option, Ref, Schema } from "effect";
import { Response } from "effect/unstable/ai";
import { HttpClient, HttpClientResponse } from "effect/unstable/http";

import {
  estimateGpt56CostMicrousd,
  hydrateExactChanges,
  IncrementalScopeUnavailable,
  makeReviewRepository,
  publishHeadBoundReview,
  reviewCandidatePaths,
  reviewUnavailablePaths,
  reviewPublicationFailure,
  reviewActionProgram,
  reviewEventFor,
  withActionInputs,
} from "../src/action.ts";
import {
  type ChangedFile,
  GitHubApiFailure,
  type RepositorySnapshot,
  StaleReviewHead,
} from "../src/github.ts";
import { reviewMarker, selectReview } from "../src/selection.ts";

const file = (path: string, patch: string | undefined): ChangedFile => ({
  path,
  status: "modified",
  additions: 1,
  deletions: 0,
  patch,
});

const PublishedReviewBody = Schema.Struct({
  commit_id: Schema.optionalKey(Schema.String),
  event: Schema.String,
  body: Schema.String,
  comments: Schema.Array(Schema.Unknown),
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
});

describe("Action configuration", () => {
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
  it.effect(
    "replaces successful and failed stale result bodies with one neutral attempt marker",
    () =>
      Effect.gen(function* () {
        for (const originalBody of [
          "MODEL FINDING: reachable secret exposure",
          "MODEL FAILURE: provider diagnostics",
        ]) {
          const attemptedBodies = yield* Ref.make<ReadonlyArray<string>>([]);
          const markerBodies = yield* Ref.make<ReadonlyArray<string>>([]);
          const stale = StaleReviewHead.make({
            inspectedHead: "old-head",
            currentHead: "new-head",
          });
          const publication = yield* publishHeadBoundReview({
            publish: Ref.update(attemptedBodies, (current) => [...current, originalBody]).pipe(
              Effect.andThen(Effect.fail(stale)),
            ),
            publishCurrentHeadAttemptMarker: (body) =>
              Ref.update(markerBodies, (current) => [...current, body]).pipe(
                Effect.as("https://github.test/review"),
              ),
            automatic: true,
          });

          expect(publication).toMatchObject({
            _tag: "stale",
            reviewUrl: "https://github.test/review",
            failure: stale,
          });
          expect(yield* Ref.get(attemptedBodies)).toEqual([originalBody]);
          const markers = yield* Ref.get(markerBodies);
          expect(markers).toHaveLength(1);
          expect(markers[0]).toContain(
            "<!-- effect-agent-review:v3 automatic=true completed=false -->",
          );
          expect(markers[0]).not.toContain(originalBody);
          expect(markers[0]).not.toContain("finding");
          const history = [
            {
              id: 1,
              authorLogin: "effect-agent[bot]",
              authorType: "Bot",
              body: markers[0] ?? "",
              commitId: "new-head",
              submittedAt: "2026-08-26T00:00:00Z",
              state: "COMMENTED" as const,
            },
          ];
          expect(
            selectReview({
              mode: "auto",
              currentHead: "new-head",
              reviewAuthor: "effect-agent[bot]",
              automaticReviewLimit: 1,
              history,
            }),
          ).toEqual({ _tag: "skip", reason: "head-review-incomplete" });
          expect(
            selectReview({
              mode: "auto",
              currentHead: "later-head",
              reviewAuthor: "effect-agent[bot]",
              automaticReviewLimit: 1,
              history,
            }),
          ).toMatchObject({ _tag: "pause", automaticAttempts: 1 });
        }
      }),
  );

  it.effect("does not retry or post a marker after a generic publication failure", () =>
    Effect.gen(function* () {
      const markerPosts = yield* Ref.make(0);
      const failure = GitHubApiFailure.make({
        operation: "publish pull request review",
        reason: "POST failed after an uncertain transport outcome",
      });
      const exit = yield* publishHeadBoundReview({
        publish: Effect.fail(failure),
        publishCurrentHeadAttemptMarker: () =>
          Ref.update(markerPosts, (count) => count + 1).pipe(
            Effect.andThen(Effect.succeed("https://github.test/extra-review")),
          ),
        automatic: true,
      }).pipe(Effect.exit);

      expect(Exit.isFailure(exit)).toBe(true);
      expect(yield* Ref.get(markerPosts)).toBe(0);
    }),
  );

  it.effect(
    "posts one marker-only review and leaves the action failed when preflight sees a push",
    () =>
      Effect.gen(function* () {
        const pullReads = yield* Ref.make(0);
        const postBodies = yield* Ref.make<ReadonlyArray<unknown>>([]);
        const client = HttpClient.make((request, url) => {
          const response = (body: unknown, status = 200) =>
            HttpClientResponse.fromWeb(
              request,
              new globalThis.Response(JSON.stringify(body), {
                status,
                headers: { "content-type": "application/json" },
              }),
            );
          if (request.method === "GET" && url.pathname.endsWith("/pulls/12")) {
            return Ref.getAndUpdate(pullReads, (count) => count + 1).pipe(
              Effect.map((count) =>
                response({
                  number: 12,
                  title: "Move during publication",
                  body: null,
                  draft: false,
                  html_url: "https://github.test/reve-ai/example/pull/12",
                  base: { sha: "base" },
                  head: { sha: count === 0 ? "inspected-head" : "current-head" },
                }),
              ),
            );
          }
          if (
            request.method === "GET" &&
            (url.pathname.endsWith("/reviews") || url.pathname.endsWith("/files"))
          ) {
            return Effect.succeed(response([]));
          }
          if (request.method === "GET" && url.pathname.includes("/compare/")) {
            return Effect.succeed(response({ merge_base_commit: { sha: "base" } }));
          }
          if (request.method === "GET" && url.pathname.endsWith("/git/commits/base")) {
            return Effect.succeed(response({ sha: "base", tree: { sha: "base-tree" } }));
          }
          if (request.method === "GET" && url.pathname.endsWith("/git/commits/inspected-head")) {
            return Effect.succeed(response({ sha: "inspected-head", tree: { sha: "head-tree" } }));
          }
          if (
            request.method === "GET" &&
            (url.pathname.endsWith("/git/trees/base-tree") ||
              url.pathname.endsWith("/git/trees/head-tree"))
          ) {
            const sha = url.pathname.endsWith("base-tree") ? "base-tree" : "head-tree";
            return Effect.succeed(response({ sha, tree: [], truncated: false }));
          }
          if (request.method === "POST" && url.pathname.endsWith("/reviews")) {
            const encoded =
              request.body._tag === "Uint8Array"
                ? new TextDecoder().decode(request.body.body)
                : "{}";
            return Ref.update(postBodies, (current) => [...current, JSON.parse(encoded)]).pipe(
              Effect.as(response({ html_url: "https://github.test/review" })),
            );
          }
          return Effect.die(`unexpected request ${request.method} ${url.href}`);
        });
        const provider = ConfigProvider.fromEnv({
          env: {
            GITHUB_REPOSITORY: "reve-ai/example",
            GITHUB_TOKEN: "github-token",
            GITHUB_API_URL: "https://api.github.test",
            PR_REVIEW_PULL_REQUEST: "12",
            PR_REVIEW_AUTHOR: "effect-agent[bot]",
          },
        });
        const exit = yield* reviewActionProgram.pipe(
          Effect.provideService(ConfigProvider.ConfigProvider, provider),
          Effect.provideService(HttpClient.HttpClient, client),
          Effect.provide(NodeServices.layer),
          Effect.exit,
        );

        expect(Exit.isFailure(exit)).toBe(true);
        expect(yield* Ref.get(pullReads)).toBe(2);
        const posts = yield* Ref.get(postBodies);
        expect(posts).toHaveLength(1);
        expect(posts[0]).toEqual({
          event: "COMMENT",
          body: expect.stringContaining(
            "<!-- effect-agent-review:v3 automatic=true completed=false -->",
          ),
          comments: [],
        });
        expect(posts[0]).not.toHaveProperty("commit_id");
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
          : {
              number: 12,
              title: "Draft pull request",
              body: null,
              draft: true,
              html_url: "https://github.test/reve-ai/example/pull/12",
              base: { sha: "base" },
              head: { sha: "head" },
            };
        return Ref.update(requests, (current) => [
          ...current,
          `${request.method} ${url.pathname}`,
        ]).pipe(
          Effect.as(
            HttpClientResponse.fromWeb(
              request,
              new globalThis.Response(JSON.stringify(wire), {
                status: 200,
                headers: { "content-type": "application/json" },
              }),
            ),
          ),
        );
      });
      const provider = ConfigProvider.fromEnv({
        env: {
          GITHUB_REPOSITORY: "reve-ai/example",
          GITHUB_TOKEN: "github-token",
          GITHUB_API_URL: "https://api.github.test",
          PR_REVIEW_PULL_REQUEST: "12",
          PR_REVIEW_COMMAND: "@effect-agent review full",
          PR_REVIEW_COMMENT_ID: "42",
        },
      });

      yield* reviewActionProgram.pipe(
        Effect.provideService(ConfigProvider.ConfigProvider, provider),
        Effect.provideService(HttpClient.HttpClient, client),
        Effect.provide(NodeServices.layer),
      );

      expect(yield* Ref.get(requests)).toEqual([
        "POST /repos/reve-ai/example/issues/comments/42/reactions",
        "GET /repos/reve-ai/example/pulls/12",
      ]);
    }),
  );
});

describe("Incremental review scope", () => {
  it("publishes blocking findings as a head-bound change request", () => {
    expect(reviewEventFor(1)).toBe("REQUEST_CHANGES");
    expect(reviewEventFor(0)).toBe("COMMENT");
  });

  it("keeps incomplete coverage and prior change requests failing", () => {
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
    "keeps failed and incomplete same-head automatic attempts failing at the action seam",
    () =>
      Effect.gen(function* () {
        for (const body of [
          `Review execution failed.\n\n${reviewMarker(true, false)}`,
          `Review coverage is incomplete.\n\n${reviewMarker(true, false)}`,
        ]) {
          const requests = yield* Ref.make<ReadonlyArray<string>>([]);
          const client = HttpClient.make((request, url) => {
            const response = url.pathname.endsWith("/reviews")
              ? [
                  {
                    id: 1,
                    body,
                    commit_id: "head",
                    submitted_at: "2026-08-25T00:00:00Z",
                    state: "COMMENTED",
                    user: { login: "effect-agent[bot]", type: "Bot" },
                  },
                ]
              : url.pathname.endsWith("/pulls/12")
                ? {
                    number: 12,
                    title: "Keep a failed check red",
                    body: null,
                    draft: false,
                    html_url: "https://github.test/reve-ai/example/pull/12",
                    base: { sha: "base" },
                    head: { sha: "head" },
                  }
                : undefined;
            if (response === undefined) {
              return Effect.die(`unexpected request ${request.method} ${url.href}`);
            }
            return Ref.update(requests, (current) => [...current, url.pathname]).pipe(
              Effect.as(
                HttpClientResponse.fromWeb(
                  request,
                  new globalThis.Response(JSON.stringify(response), {
                    status: 200,
                    headers: { "content-type": "application/json" },
                  }),
                ),
              ),
            );
          });
          const provider = ConfigProvider.fromEnv({
            env: {
              GITHUB_REPOSITORY: "reve-ai/example",
              GITHUB_TOKEN: "github-token",
              GITHUB_API_URL: "https://api.github.test",
              PR_REVIEW_PULL_REQUEST: "12",
              PR_REVIEW_AUTHOR: "effect-agent[bot]",
            },
          });
          const exit = yield* reviewActionProgram.pipe(
            Effect.provideService(ConfigProvider.ConfigProvider, provider),
            Effect.provideService(HttpClient.HttpClient, client),
            Effect.provide(NodeServices.layer),
            Effect.exit,
          );

          expect(Exit.isFailure(exit)).toBe(true);
          expect(yield* Ref.get(requests)).toEqual([
            "/repos/reve-ai/example/pulls/12",
            "/repos/reve-ai/example/pulls/12/reviews",
          ]);
        }
      }),
  );

  it.effect("publishes one incomplete attempt for tree and guidance preparation failures", () =>
    Effect.gen(function* () {
      for (const failureKind of ["truncated-tree", "missing-guidance"] as const) {
        const publishedBodies = yield* Ref.make<ReadonlyArray<string>>([]);
        const publishedWires = yield* Ref.make<ReadonlyArray<unknown>>([]);
        const requests = yield* Ref.make<ReadonlyArray<string>>([]);
        let pullReads = 0;
        const client = HttpClient.make((request, url) => {
          let response: unknown;
          if (request.method === "GET" && url.pathname.endsWith("/pulls/12")) {
            response = {
              number: 12,
              title: "Record preparation failures",
              body: null,
              draft: false,
              html_url: "https://github.test/reve-ai/example/pull/12",
              base: { sha: "base" },
              head: {
                sha: failureKind === "truncated-tree" && pullReads > 0 ? "moved-head" : "head",
              },
            };
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
            const encoded =
              request.body._tag === "Uint8Array"
                ? new TextDecoder().decode(request.body.body)
                : "{}";
            const publishedWire: unknown = JSON.parse(encoded);
            const published = Schema.decodeUnknownSync(Schema.Struct({ body: Schema.String }))(
              publishedWire,
            );
            response = { html_url: "https://github.test/reve-ai/example/pull/12#review" };
            return Ref.update(requests, (current) => [
              ...current,
              `${request.method} ${url.pathname}`,
            ]).pipe(
              Effect.andThen(
                Ref.update(publishedBodies, (current) => [...current, published.body]),
              ),
              Effect.andThen(Ref.update(publishedWires, (current) => [...current, publishedWire])),
              Effect.as(
                HttpClientResponse.fromWeb(
                  request,
                  new globalThis.Response(JSON.stringify(response), {
                    status: 200,
                    headers: { "content-type": "application/json" },
                  }),
                ),
              ),
            );
          } else {
            return Effect.die(`unexpected request ${request.method} ${url.href}`);
          }
          return Ref.update(requests, (current) => [
            ...current,
            `${request.method} ${url.pathname}`,
          ]).pipe(
            Effect.as(
              HttpClientResponse.fromWeb(
                request,
                new globalThis.Response(JSON.stringify(response), {
                  status: 200,
                  headers: { "content-type": "application/json" },
                }),
              ),
            ),
          );
        });
        const provider = ConfigProvider.fromEnv({
          env: {
            GITHUB_REPOSITORY: "reve-ai/example",
            GITHUB_TOKEN: "github-token",
            GITHUB_API_URL: "https://api.github.test",
            PR_REVIEW_PULL_REQUEST: "12",
            PR_REVIEW_AUTHOR: "effect-agent[bot]",
            ...(failureKind === "missing-guidance"
              ? { PR_REVIEW_GUIDANCE_FILE: "/missing/effect-agent-review-guidance.md" }
              : {}),
          },
        });
        const exit = yield* reviewActionProgram.pipe(
          Effect.provideService(ConfigProvider.ConfigProvider, provider),
          Effect.provideService(HttpClient.HttpClient, client),
          Effect.provide(NodeServices.layer),
          Effect.exit,
        );

        expect(Exit.isFailure(exit)).toBe(true);
        expect(yield* Ref.get(publishedBodies)).toEqual([
          expect.stringContaining("<!-- effect-agent-review:v3 automatic=true completed=false -->"),
        ]);
        expect(
          (yield* Ref.get(requests)).filter((value) => value.startsWith("POST ")),
        ).toHaveLength(1);
        if (failureKind === "truncated-tree") {
          if (!Exit.isFailure(exit)) return;
          expect(Option.getOrUndefined(Cause.findErrorOption(exit.cause))).toMatchObject({
            _tag: "GitHubApiFailure",
            operation: "get recursive Git tree",
            reason: "GitHub truncated tree head-tree",
          });
          const wires = yield* Ref.get(publishedWires);
          expect(wires[0]).toEqual({
            event: "COMMENT",
            body: expect.stringContaining(
              "<!-- effect-agent-review:v3 automatic=true completed=false -->",
            ),
            comments: [],
          });
          expect(wires[0]).not.toHaveProperty("commit_id");
        }
      }
    }),
  );

  it.effect("fails closed before repository hydration when the incremental merge base moved", () =>
    Effect.gen(function* () {
      const targetHead = "target-head";
      const reviewedHead = "reviewed-head";
      const currentHead = "current-head";
      const priorMergeBase = "prior-merge-base";
      const currentMergeBase = "current-merge-base";
      const requests = yield* Ref.make<ReadonlyArray<string>>([]);
      const publishedWires = yield* Ref.make<ReadonlyArray<typeof PublishedReviewBody.Type>>([]);
      const response = (request: Parameters<typeof HttpClientResponse.fromWeb>[0], body: unknown) =>
        HttpClientResponse.fromWeb(
          request,
          new globalThis.Response(JSON.stringify(body), {
            status: 200,
            headers: { "content-type": "application/json" },
          }),
        );
      const client = HttpClient.make((request, url) => {
        const record = Ref.update(requests, (current) => [
          ...current,
          `${request.method} ${url.pathname}`,
        ]);
        if (request.method === "GET" && url.pathname.endsWith("/pulls/12")) {
          return record.pipe(
            Effect.as(
              response(request, {
                number: 12,
                title: "Rebase onto an updated target",
                body: null,
                draft: false,
                html_url: "https://github.test/reve-ai/example/pull/12",
                base: { sha: targetHead },
                head: { sha: currentHead },
              }),
            ),
          );
        }
        if (request.method === "GET" && url.pathname.endsWith("/pulls/12/reviews")) {
          return record.pipe(
            Effect.as(
              response(request, [
                {
                  id: 1,
                  body: reviewMarker(false),
                  commit_id: reviewedHead,
                  submitted_at: "2026-08-25T00:00:00Z",
                  state: "COMMENTED",
                  user: { login: "effect-agent[bot]", type: "Bot" },
                },
              ]),
            ),
          );
        }
        if (request.method === "GET" && url.pathname.endsWith("/pulls/12/files")) {
          return record.pipe(
            Effect.as(
              response(request, [
                {
                  filename: "src/shared.ts",
                  status: "modified",
                  additions: 1,
                  deletions: 1,
                  patch: "@@ -1 +1 @@\n-target\n+pull",
                },
              ]),
            ),
          );
        }
        if (
          request.method === "GET" &&
          url.pathname.endsWith(`/compare/${targetHead}...${currentHead}`)
        ) {
          return record.pipe(
            Effect.as(response(request, { merge_base_commit: { sha: currentMergeBase } })),
          );
        }
        if (
          request.method === "GET" &&
          url.pathname.endsWith(`/compare/${targetHead}...${reviewedHead}`)
        ) {
          return record.pipe(
            Effect.as(response(request, { merge_base_commit: { sha: priorMergeBase } })),
          );
        }
        if (request.method === "POST" && url.pathname.endsWith("/pulls/12/reviews")) {
          const encoded =
            request.body._tag === "Uint8Array" ? new TextDecoder().decode(request.body.body) : "{}";
          const wire = Schema.decodeUnknownSync(PublishedReviewBody)(JSON.parse(encoded));
          return record.pipe(
            Effect.andThen(Ref.update(publishedWires, (current) => [...current, wire])),
            Effect.as(
              response(request, {
                html_url: "https://github.test/reve-ai/example/pull/12#review",
              }),
            ),
          );
        }
        return Effect.die(`unexpected request ${request.method} ${url.href}`);
      });
      const provider = ConfigProvider.fromEnv({
        env: {
          GITHUB_REPOSITORY: "reve-ai/example",
          GITHUB_TOKEN: "github-token",
          GITHUB_API_URL: "https://api.github.test",
          PR_REVIEW_PULL_REQUEST: "12",
          PR_REVIEW_AUTHOR: "effect-agent[bot]",
        },
      });
      const exit = yield* reviewActionProgram.pipe(
        Effect.provideService(ConfigProvider.ConfigProvider, provider),
        Effect.provideService(HttpClient.HttpClient, client),
        Effect.provide(NodeServices.layer),
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
      expect(published).toEqual([
        {
          commit_id: currentHead,
          event: "COMMENT",
          body: expect.stringContaining(reviewMarker(true, false)),
          comments: [],
        },
      ]);
      const failedAttempt = {
        id: 2,
        authorLogin: "effect-agent[bot]",
        authorType: "Bot",
        body: String(published[0]?.body),
        commitId: currentHead,
        submittedAt: "2026-08-26T00:00:00Z",
        state: "COMMENTED" as const,
      };
      const completedAttempt = {
        id: 1,
        authorLogin: "effect-agent[bot]",
        authorType: "Bot",
        body: reviewMarker(false),
        commitId: reviewedHead,
        submittedAt: "2026-08-25T00:00:00Z",
        state: "COMMENTED" as const,
      };
      expect(
        selectReview({
          mode: "auto",
          currentHead,
          reviewAuthor: "effect-agent[bot]",
          automaticReviewLimit: 2,
          history: [completedAttempt, failedAttempt],
        }),
      ).toEqual({ _tag: "skip", reason: "head-review-incomplete" });
      expect(
        selectReview({
          mode: "auto",
          currentHead: "later-head",
          reviewAuthor: "effect-agent[bot]",
          automaticReviewLimit: 2,
          history: [completedAttempt, failedAttempt],
        }),
      ).toMatchObject({
        _tag: "review",
        scope: "incremental",
        baseRevision: reviewedHead,
      });
    }),
  );

  it.effect("allows a manual full review to recover after incremental merge-base movement", () =>
    Effect.gen(function* () {
      const targetHead = "target-head";
      const reviewedHead = "reviewed-head";
      const currentHead = "current-head";
      const currentMergeBase = "current-merge-base";
      const requests = yield* Ref.make<ReadonlyArray<string>>([]);
      const publishedWires = yield* Ref.make<ReadonlyArray<typeof PublishedReviewBody.Type>>([]);
      const response = (request: Parameters<typeof HttpClientResponse.fromWeb>[0], body: unknown) =>
        HttpClientResponse.fromWeb(
          request,
          new globalThis.Response(JSON.stringify(body), {
            status: 200,
            headers: { "content-type": "application/json" },
          }),
        );
      const client = HttpClient.make((request, url) => {
        const record = Ref.update(requests, (current) => [
          ...current,
          `${request.method} ${url.pathname}`,
        ]);
        if (request.method === "GET" && url.pathname.endsWith("/pulls/12")) {
          return record.pipe(
            Effect.as(
              response(request, {
                number: 12,
                title: "Review the rebased pull request in full",
                body: null,
                draft: false,
                html_url: "https://github.test/reve-ai/example/pull/12",
                base: { sha: targetHead },
                head: { sha: currentHead },
              }),
            ),
          );
        }
        if (request.method === "GET" && url.pathname.endsWith("/pulls/12/reviews")) {
          return record.pipe(
            Effect.as(
              response(request, [
                {
                  id: 1,
                  body: reviewMarker(false),
                  commit_id: reviewedHead,
                  submitted_at: "2026-08-25T00:00:00Z",
                  state: "COMMENTED",
                  user: { login: "effect-agent[bot]", type: "Bot" },
                },
                {
                  id: 2,
                  body: reviewMarker(true, false),
                  commit_id: currentHead,
                  submitted_at: "2026-08-26T00:00:00Z",
                  state: "COMMENTED",
                  user: { login: "effect-agent[bot]", type: "Bot" },
                },
              ]),
            ),
          );
        }
        if (request.method === "GET" && url.pathname.endsWith("/pulls/12/files")) {
          return record.pipe(Effect.as(response(request, [])));
        }
        if (
          request.method === "GET" &&
          url.pathname.endsWith(`/compare/${targetHead}...${currentHead}`)
        ) {
          return record.pipe(
            Effect.as(response(request, { merge_base_commit: { sha: currentMergeBase } })),
          );
        }
        if (request.method === "GET" && url.pathname.endsWith(`/git/commits/${currentMergeBase}`)) {
          return record.pipe(
            Effect.as(response(request, { sha: currentMergeBase, tree: { sha: "base-tree" } })),
          );
        }
        if (request.method === "GET" && url.pathname.endsWith(`/git/commits/${currentHead}`)) {
          return record.pipe(
            Effect.as(response(request, { sha: currentHead, tree: { sha: "head-tree" } })),
          );
        }
        if (
          request.method === "GET" &&
          (url.pathname.endsWith("/git/trees/base-tree") ||
            url.pathname.endsWith("/git/trees/head-tree"))
        ) {
          const sha = url.pathname.endsWith("base-tree") ? "base-tree" : "head-tree";
          return record.pipe(Effect.as(response(request, { sha, tree: [], truncated: false })));
        }
        if (request.method === "POST" && url.pathname.endsWith("/pulls/12/reviews")) {
          const encoded =
            request.body._tag === "Uint8Array" ? new TextDecoder().decode(request.body.body) : "{}";
          const wire = Schema.decodeUnknownSync(PublishedReviewBody)(JSON.parse(encoded));
          return record.pipe(
            Effect.andThen(Ref.update(publishedWires, (current) => [...current, wire])),
            Effect.as(
              response(request, {
                html_url: "https://github.test/reve-ai/example/pull/12#review",
              }),
            ),
          );
        }
        return Effect.die(`unexpected request ${request.method} ${url.href}`);
      });
      const provider = ConfigProvider.fromEnv({
        env: {
          GITHUB_REPOSITORY: "reve-ai/example",
          GITHUB_TOKEN: "github-token",
          GITHUB_API_URL: "https://api.github.test",
          PR_REVIEW_PULL_REQUEST: "12",
          PR_REVIEW_AUTHOR: "effect-agent[bot]",
          PR_REVIEW_MODE: "full",
        },
      });

      const exit = yield* reviewActionProgram.pipe(
        Effect.provideService(ConfigProvider.ConfigProvider, provider),
        Effect.provideService(HttpClient.HttpClient, client),
        Effect.provide(NodeServices.layer),
        Effect.exit,
      );

      expect(Exit.isSuccess(exit)).toBe(true);
      const observed = yield* Ref.get(requests);
      expect(observed.filter((value) => value.includes("/compare/"))).toEqual([
        `GET /repos/reve-ai/example/compare/${targetHead}...${currentHead}`,
      ]);
      expect(observed.some((value) => value.includes(reviewedHead))).toBe(false);
      expect(yield* Ref.get(publishedWires)).toEqual([
        {
          commit_id: currentHead,
          event: "COMMENT",
          body: expect.stringContaining(reviewMarker(false, true)),
          comments: [],
        },
      ]);
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
      const client = HttpClient.make((request, url) => {
        let body: unknown;
        if (request.method === "POST" && url.pathname.endsWith("/reactions")) {
          body = { id: 1, content: "eyes" };
        } else if (request.method === "GET" && url.pathname.endsWith("/pulls/12")) {
          body = {
            number: 12,
            title: "Rewrite the branch",
            body: null,
            draft: false,
            html_url: "https://github.test/reve-ai/example/pull/12",
            base: { sha: targetHead },
            head: { sha: currentHead },
          };
        } else if (request.method === "GET" && url.pathname.endsWith("/pulls/12/reviews")) {
          body = [
            {
              id: 1,
              body: reviewMarker(false),
              commit_id: reviewedHead,
              submitted_at: "2026-08-25T00:00:00Z",
              state: "COMMENTED",
              user: { login: "effect-agent[bot]", type: "Bot" },
            },
          ];
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
        } else if (request.method === "GET" && url.pathname.endsWith(targetTree)) {
          body = {
            sha: targetTree,
            truncated: false,
            tree: [
              {
                path: "src/unchanged.ts",
                mode: "100644",
                type: "blob",
                sha: targetBlob,
                size: 1,
              },
            ],
          };
        } else if (request.method === "GET" && url.pathname.endsWith(reviewedTree)) {
          body = {
            sha: reviewedTree,
            truncated: false,
            tree: [
              {
                path: "src/unchanged.ts",
                mode: "100644",
                type: "blob",
                sha: unchangedBlob,
                size: 1,
              },
            ],
          };
        } else if (request.method === "GET" && url.pathname.endsWith(currentTree)) {
          body = {
            sha: currentTree,
            truncated: false,
            tree: [
              {
                path: "src/unchanged.ts",
                mode: "100644",
                type: "blob",
                sha: unchangedBlob,
                size: 1,
              },
            ],
          };
        } else if (request.method === "POST" && url.pathname.endsWith("/pulls/12/reviews")) {
          const encoded =
            request.body._tag === "Uint8Array" ? new TextDecoder().decode(request.body.body) : "{}";
          const published = Schema.decodeUnknownSync(Schema.Struct({ body: Schema.String }))(
            JSON.parse(encoded),
          );
          body = { html_url: "https://github.test/reve-ai/example/pull/12#review" };
          return Ref.update(requests, (current) => [
            ...current,
            `${request.method} ${url.pathname}`,
          ]).pipe(
            Effect.andThen(Ref.update(publishedBodies, (current) => [...current, published.body])),
            Effect.as(
              HttpClientResponse.fromWeb(
                request,
                new globalThis.Response(JSON.stringify(body), {
                  status: 200,
                  headers: { "content-type": "application/json" },
                }),
              ),
            ),
          );
        } else {
          return Effect.die(new Error(`unexpected request ${request.method} ${url.href}`));
        }
        return Ref.update(requests, (current) => [
          ...current,
          `${request.method} ${url.pathname}`,
        ]).pipe(
          Effect.as(
            HttpClientResponse.fromWeb(
              request,
              new globalThis.Response(JSON.stringify(body), {
                status: 200,
                headers: { "content-type": "application/json" },
              }),
            ),
          ),
        );
      });
      const provider = ConfigProvider.fromEnv({
        env: {
          GITHUB_REPOSITORY: "reve-ai/example",
          GITHUB_TOKEN: "github-token",
          GITHUB_API_URL: "https://api.github.test",
          PR_REVIEW_PULL_REQUEST: "12",
          PR_REVIEW_AUTHOR: "effect-agent[bot]",
          PR_REVIEW_COMMAND: "@effect-agent review",
          PR_REVIEW_COMMENT_ID: "42",
        },
      });

      yield* reviewActionProgram.pipe(
        Effect.provideService(ConfigProvider.ConfigProvider, provider),
        Effect.provideService(HttpClient.HttpClient, client),
        Effect.provide(NodeServices.layer),
      );

      const requested = yield* Ref.get(requests);
      const published = yield* Ref.get(publishedBodies);
      expect(requested.some((request) => request.includes("/compare/"))).toBe(true);
      expect(requested.filter((request) => request.includes("/git/trees/"))).toHaveLength(3);
      expect(published).toHaveLength(1);
      expect(published[0]).toContain("| **Incremental** | 0 reviewed | ✅ None |");
      expect(published[0]).toContain(
        "No pull-request files changed since the last completed review.",
      );
    }),
  );
});

describe("GPT-5.6 cost estimation", () => {
  it("prices uncached, cached, cache-write, and output tokens at current family rates", () => {
    const usage = Response.Usage.make({
      inputTokens: { total: 10_000, uncached: 8_000, cacheRead: 2_000, cacheWrite: 1_000 },
      outputTokens: { total: 500, text: 400, reasoning: 100 },
    });

    expect(estimateGpt56CostMicrousd("gpt-5.6-sol", usage)).toBe(43_800);
    expect(estimateGpt56CostMicrousd("gpt-5.6-terra", usage)).toBe(22_900);
    expect(estimateGpt56CostMicrousd("gpt-5.6-luna", usage)).toBe(2_290);
    expect(estimateGpt56CostMicrousd("custom-model", usage)).toBeUndefined();
  });
});

describe("exact review delta", () => {
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
      const candidates = reviewCandidatePaths([renamed]);
      const surface = yield* hydrateExactChanges({
        files: [renamed],
        changedPaths: candidates,
        base: treeSnapshot("merge-base", { [previousPath]: unchanged }),
        head: treeSnapshot("head", { [path]: unchanged }),
        ignore: [],
      });

      expect(candidates).toEqual([path, previousPath].sort());
      expect(surface.changes).toHaveLength(1);
      expect(surface.unreviewedPaths).toEqual([]);
      expect(surface.changes[0]?.path).toBe(path);
      expect(surface.changes[0]?.patch).toContain(`rename from ${previousPath}`);
      expect(surface.changes[0]?.patch).toContain(`rename to ${path}`);
      expect(surface.changes[0]?.patch.length).toBeLessThan(1_000);
      expect(surface.changes[0]?.patch).not.toContain("x".repeat(1_000));
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
            unavailablePaths: reviewUnavailablePaths([renamed], surface),
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

  it.effect("does not read ignored paths or candidates beyond the 100-file admission bound", () =>
    Effect.gen(function* () {
      const paths = Array.from(
        { length: 102 },
        (_, index) => `src/file-${String(index).padStart(3, "0")}.ts`,
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

      expect(surface.changes).toHaveLength(100);
      expect(surface.ignoredPaths).toEqual([ignored]);
      expect(surface.unreviewedPaths).toEqual([afterCap]);
    }),
  );

  it.effect("stops blob hydration after the aggregate patch budget is exhausted", () =>
    Effect.gen(function* () {
      const paths = ["src/a.ts", "src/b.ts", "src/c.ts"];
      const surface = yield* hydrateExactChanges({
        files: paths.map((path) => file(path, undefined)),
        changedPaths: paths,
        base: treeSnapshot("base", {}),
        head: treeSnapshot(
          "head",
          {
            "src/a.ts": "a".repeat(70_000),
            "src/b.ts": "b".repeat(60_000),
            "src/c.ts": "must not be read",
          },
          new Set(["src/c.ts"]),
        ),
        ignore: [],
      });

      expect(surface.changes.map((change) => change.path)).toEqual(["src/a.ts"]);
      expect(surface.unreviewedPaths).toEqual(["src/b.ts", "src/c.ts"]);
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

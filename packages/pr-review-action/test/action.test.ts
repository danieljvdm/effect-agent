import { NodeServices } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import { Cause, ConfigProvider, Deferred, Effect, Exit, Fiber, Option, Ref, Schema } from "effect";
import { TestClock } from "effect/testing";
import { HttpClient, HttpClientResponse } from "effect/unstable/http";

import {
  GeneratedFileClassification,
  hydrateExactChanges,
  reviewActionProgram,
} from "../src/action.ts";
import type { ChangedFile, RepositorySnapshot } from "../src/github.ts";
import { reviewMarker } from "../src/selection.ts";

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

const decodeCheckBody = (request: TestHttpRequest) => {
  if (request.body._tag !== "Uint8Array") throw new Error("Expected check JSON");

  return Schema.decodeSync(Schema.fromJsonString(Schema.Json))(
    new TextDecoder().decode(request.body.body),
  );
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

const pullRequestWire = (title: string, base: string, head: string, draft = false) => ({
  number: 12,
  title,
  body: null,
  draft,
  html_url: "https://github.test/reve-ai/example/pull/12",
  base: { sha: base },
  head: { sha: head },
});

const reviewHistoryWire = (
  id: number,
  body: string,
  commitId: string,
  submittedAt: string | null,
) => ({
  id,
  body,
  commit_id: commitId,
  submitted_at: submittedAt,
  state: "COMMENTED",
  user: { login: "effect-agent[bot]", type: "Bot" },
});

describe("review input admission", () => {
  // Regression from b3be98955: the configured nested snapshot pattern admitted a generated file.
  it.effect("ignores a nested snapshot without ignoring other snapshots", () =>
    Effect.gen(function* () {
      const ignoredPath = "packages/db/migrations/postgres/20260924/snapshot.json";
      const includedPath = "packages/db/fixtures/snapshot.json";

      const content = new Map([
        [ignoredPath, '{"generated":true}\n'],
        [includedPath, '{"fixture":true}\n'],
      ]);

      const base: RepositorySnapshot = {
        revision: "base",
        paths: [],
        entry: () => undefined,
        readTextFile: () => Effect.succeed(""),
      };

      const head: RepositorySnapshot = {
        revision: "head",
        paths: [ignoredPath, includedPath],
        entry: (path) => {
          const text = content.get(path);

          return text === undefined
            ? undefined
            : { sha: path, mode: "100644", type: "blob", size: text.length };
        },
        readTextFile: (path) => Effect.succeed(content.get(path) ?? ""),
      };

      const files: ReadonlyArray<ChangedFile> = [ignoredPath, includedPath].map((path) => ({
        path,
        status: "added",
        additions: 1,
        deletions: 0,
        patch: undefined,
      }));

      const surface = yield* hydrateExactChanges({
        files,
        changedPaths: [ignoredPath, includedPath],
        base,
        head,
        ignore: ["packages/db/migrations/**/snapshot.json"],
      }).pipe(
        Effect.provideService(GeneratedFileClassification, {
          isGenerated: () => Effect.succeed(false),
        }),
      );

      expect(surface.ignoredPaths).toEqual([ignoredPath]);
      expect(surface.changes.map(({ path }) => path)).toEqual([includedPath]);
    }),
  );
});

describe("stale-head publication", () => {
  it.effect("leaves a stale run failed without blocking the queued review of the new head", () =>
    Effect.gen(function* () {
      const pullReads = yield* Ref.make(0);
      const postBodies = yield* Ref.make<ReadonlyArray<typeof PublishedReviewBody.Type>>([]);

      const checkWrites: Array<Schema.Json> = [];

      const check = {
        id: 100,
        name: "Effect Agent review",
        head_sha: "inspected-head",
        external_id: "effect-agent-pr-review:v1:12",
      };

      const options = { PR_REVIEW_AUTOMATIC_LIMIT: "2", PR_REVIEW_CHECK_NAME: check.name };

      const client = HttpClient.make((request, url) => {
        if (url.pathname.includes("/check-runs")) {
          checkWrites.push(decodeCheckBody(request));

          return Effect.succeed(jsonResponse(request, check));
        }
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

      const exit = yield* runReviewAction(client, options).pipe(Effect.exit);

      if (Exit.isSuccess(exit)) throw new Error("Expected stale publication to fail");
      expect(Option.getOrUndefined(Cause.findErrorOption(exit.cause))).toMatchObject({
        _tag: "StaleReviewHead",
        inspectedHead: "inspected-head",
        currentHead: "current-head",
      });
      expect(yield* Ref.get(postBodies)).toEqual([
        {
          commit_id: "inspected-head",
          event: "COMMENT",
          body: expect.stringContaining(reviewMarker(true, false)),
          comments: [],
        },
      ]);

      expect(checkWrites).toEqual([
        expect.objectContaining({ head_sha: "inspected-head", status: "in_progress" }),
        expect.objectContaining({ conclusion: "cancelled" }),
      ]);
      check.id = 101;
      check.head_sha = "current-head";
      yield* runReviewAction(client, options);
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
      expect(checkWrites.at(-1)).toMatchObject({ conclusion: "success" });
    }),
  );
});

describe("PR commit review checks", () => {
  const check = {
    name: "Effect Agent review",
    head_sha: "head",
    external_id: "effect-agent-pr-review:v1:12",
  };

  const blocked = {
    ...reviewHistoryWire(1, reviewMarker(true), "old-head", "2026-09-01T00:00:00Z"),
    state: "CHANGES_REQUESTED",
  };

  // Stop at source acquisition: existing reviewer tests exercise completed passes.
  const fixture = (
    history = [blocked],
    intercept?: (
      request: TestHttpRequest,
      url: URL,
    ) => Effect.Effect<HttpClientResponse.HttpClientResponse> | undefined,
  ) => {
    const writes: Array<{ path: string; body: Schema.Json }> = [];
    const checks: Array<typeof check & { id: number }> = [];

    const client = HttpClient.make((request, url) =>
      Effect.suspend(() => {
        if (request.method !== "GET" && url.pathname.includes("/check-runs"))
          writes.push({ path: url.pathname, body: decodeCheckBody(request) });
        const intercepted = intercept?.(request, url);

        if (intercepted !== undefined) return intercepted;
        let body: unknown;

        if (url.pathname.includes("/check-runs")) {
          expect(request.headers.authorization).toBe("Bearer checks-token");
          if (request.method === "GET") body = { total_count: checks.length, check_runs: checks };
          else if (request.method === "POST") {
            body = { ...check, id: 100 + checks.length };
            checks.push({ ...check, id: 100 + checks.length });
          } else body = checks.find(({ id }) => url.pathname.endsWith(`/${String(id)}`));
        } else if (url.pathname.endsWith("/pulls/12")) {
          body = pullRequestWire("Review status", "base", "head");
        } else if (url.pathname.endsWith("/reviews")) {
          body = request.method === "GET" ? history : { html_url: "https://github.test/review" };
        } else if (url.pathname.endsWith("/reactions")) body = { id: 42, content: "eyes" };
        else return Effect.die(`Unexpected request ${request.method} ${url.pathname}`);

        return Effect.succeed(jsonResponse(request, body));
      }),
    );

    return {
      writes,
      run: (overrides: Record<string, string> = {}) =>
        runReviewAction(client, {
          PR_REVIEW_AUTOMATIC_LIMIT: "0",
          PR_REVIEW_CHECK_NAME: check.name,
          PR_REVIEW_CHECKS_TOKEN: "checks-token",
          GITHUB_SHA: "default-branch-head",
          ...overrides,
        }),
    };
  };

  // PR #679: fixed/refuted bot reviews stayed blocking. Exercise dismissal and
  // reconciliation together; live GitHub cannot reliably reproduce command edits
  // and permission revocation between event admission and execution.
  it.effect.each(["authorized", "read-only", "edited", "different-pr"] as const)(
    "reconciles a maintainer dismissal using current authority: %s",
    (mode) =>
      Effect.gen(function* () {
        const history = [{ ...blocked }, { ...blocked, id: 2, commit_id: "head" }];

        const dismissed: Array<Schema.Json> = [];
        let command = "";
        let pullUrl = "https://github.test/reve-ai/example/pull/12";

        const test = fixture(history, (request, url) => {
          if (url.pathname.endsWith("/pulls/12"))
            return Effect.succeed(
              jsonResponse(request, {
                ...pullRequestWire("Review status", "base", "head"),
                html_url: pullUrl,
              }),
            );
          if (url.pathname.endsWith("/issues/comments/42"))
            return Effect.succeed(
              jsonResponse(request, {
                id: 42,
                body: mode === "edited" ? `${command}\nEdited` : command,
                issue_url: `https://api.github.test/repos/reve-ai/example/issues/${mode === "different-pr" ? "13" : "12"}`,
                html_url: "https://github.test/reve-ai/example/pull/12#issuecomment-42",
                user: { login: "maintainer", type: "User" },
              }),
            );
          if (url.pathname.endsWith("/collaborators/maintainer/permission"))
            return Effect.succeed(
              jsonResponse(request, {
                permission: mode === "read-only" ? "read" : "write",
                user: { login: "maintainer" },
              }),
            );

          const review = history.find(
            ({ id }) =>
              url.pathname === `/repos/reve-ai/example/pulls/12/reviews/${String(id)}` ||
              url.pathname === `/repos/reve-ai/example/pulls/12/reviews/${String(id)}/dismissals`,
          );

          if (review === undefined) return undefined;
          if (request.method === "PUT") {
            dismissed.push(decodeCheckBody(request));
            review.state = "DISMISSED";
          }

          return Effect.succeed(jsonResponse(request, review));
        });

        const dismiss = (
          id: number,
          reason = "The finding does not apply.",
          overrides: Record<string, string> = {},
        ) => {
          const target =
            id === 1
              ? `https://github.test/reve-ai/example/pull/12#pullrequestreview-${String(id)}`
              : String(id);

          command = `@effect-agent dismiss ${target}\n${reason}`;

          return test.run({ PR_REVIEW_COMMAND: command, PR_REVIEW_COMMENT_ID: "42", ...overrides });
        };

        if (mode !== "authorized") {
          const exit = yield* dismiss(1).pipe(Effect.exit);

          expect(Exit.isFailure(exit)).toBe(true);
          expect(dismissed).toEqual([]);
          expect(test.writes).toEqual([]);

          return;
        }

        yield* dismiss(1, "Fixed by individually keyed recipients.");
        expect(dismissed).toEqual([
          { message: expect.stringContaining("Fixed by individually keyed recipients.") },
        ]);
        expect(dismissed[0]).toEqual({ message: expect.stringContaining("maintainer") });
        expect(dismissed[0]).toEqual({ message: expect.stringContaining("#issuecomment-42") });
        expect(test.writes.at(-1)?.body).toMatchObject({ conclusion: "failure" });

        yield* dismiss(2, "The intermediate schema was never released.");
        expect(dismissed).toHaveLength(2);
        expect(test.writes.at(-1)?.body).toMatchObject({ conclusion: "success" });

        yield* dismiss(2);
        expect(dismissed).toHaveLength(2);
        expect(test.writes.at(-1)?.body).toMatchObject({ conclusion: "success" });

        history.push({ ...blocked, id: 3, commit_id: "head", body: reviewMarker(false, false) });
        yield* dismiss(3);
        expect(test.writes.at(-1)?.body).toMatchObject({
          conclusion: "failure",
          output: { title: "Review incomplete" },
        });

        for (const review of history) review.commit_id = "old-head";
        yield* dismiss(3);
        expect(test.writes.at(-1)?.body).toMatchObject({ conclusion: "action_required" });

        // Regression in 98280c38: coverage was enforced only by the optional check.
        const withoutCheck = yield* dismiss(3, "Already dismissed.", {
          PR_REVIEW_CHECK_NAME: "",
        }).pipe(Effect.exit);

        // The same commit admitted these inputs but could not publish their record.
        pullUrl = `https://github.test/${"p".repeat(2_000)}/pull/12`;
        const reason = "Evidence. ".repeat(100);

        history.push({ ...blocked, id: 4, commit_id: "head" });
        yield* dismiss(4, reason);
        expect(dismissed.at(-1)).toEqual({ message: expect.stringContaining(reason.trim()) });
        expect(dismissed.at(-1)).toEqual({
          message: expect.stringContaining(`${pullUrl}#issuecomment-42`),
        });
        expect(test.writes.at(-1)?.body).toMatchObject({ conclusion: "success" });
        expect(Exit.isFailure(withoutCheck)).toBe(true);
      }),
  );

  // Regression in https://github.com/danieljvdm/effect-agent/commit/b66bdf89726c48f108c10172500c992e4ddf7686:
  // preserving every skipped check forced a fresh full audit after a blocker was dismissed.
  it.effect("reconciles a completed head after dismissal without another review", () =>
    Effect.gen(function* () {
      const prior = { ...blocked };

      const history = [
        prior,
        reviewHistoryWire(2, reviewMarker(false), "head", "2026-09-02T00:00:00Z"),
      ];

      const test = fixture(history);

      const manual = {
        PR_REVIEW_COMMAND: "@effect-agent review",
        PR_REVIEW_COMMENT_ID: "42",
      };

      yield* test.run(manual);
      expect(test.writes.at(-1)?.body).toMatchObject({ conclusion: "failure" });

      prior.state = "DISMISSED";
      yield* test.run();
      expect(test.writes).toHaveLength(2);

      yield* test.run(manual);
      expect(test.writes).toHaveLength(4);
      expect(test.writes.at(-1)?.body).toMatchObject({ conclusion: "success" });

      yield* test.run();
      expect(test.writes).toHaveLength(4);

      history.push(
        reviewHistoryWire(
          3,
          "<!-- effect-agent-review:v3 automatic=false completed=false -->",
          "head",
          "2026-09-03T00:00:00Z",
        ),
      );
      yield* test.run(manual);
      // Regression in https://github.com/danieljvdm/effect-agent/commit/e2c70b01952088acaf73fbf4a5de86bf22d548ad:
      // preserving the existing check left the earlier success visible.
      expect(test.writes.at(-1)?.body).toMatchObject({
        conclusion: "failure",
        output: { title: "Review incomplete" },
      });

      const withoutCheck = fixture(history);

      yield* withoutCheck.run(manual);
      expect(withoutCheck.writes.at(-1)?.body).toMatchObject({ conclusion: "failure" });
    }),
  );

  // Regression in https://github.com/danieljvdm/effect-agent/commit/e2c70b01952088acaf73fbf4a5de86bf22d548ad:
  // sorting an undated incomplete attempt first allowed a successful status refresh.
  it.effect("blocks a status refresh when an incomplete attempt has no timestamp", () =>
    Effect.gen(function* () {
      const test = fixture([
        reviewHistoryWire(1, reviewMarker(false), "head", "2026-09-02T00:00:00Z"),
        reviewHistoryWire(2, reviewMarker(false, false), "head", null),
      ]);

      yield* test.run({
        PR_REVIEW_COMMAND: "@effect-agent review",
        PR_REVIEW_COMMENT_ID: "42",
      });
      expect(test.writes.at(-1)?.body).toMatchObject({
        conclusion: "failure",
        output: { title: "Review incomplete" },
      });
    }),
  );

  it.effect("pauses after five automatic attempts without blocking a manual full review", () =>
    Effect.gen(function* () {
      const history = Array.from({ length: 5 }, (_, index) => ({
        ...reviewHistoryWire(
          index + 1,
          `Prior review ${String(index + 1)}\n${index < 4 ? "<!-- effect-agent-review:v3 automatic=true completed=true -->" : "<!-- effect-agent-review:v3 automatic=true completed=false -->"}`,
          `prior-head-${String(index + 1)}`,
          `2026-09-01T00:00:0${String(index + 1)}Z`,
        ),
        state: index < 4 ? "CHANGES_REQUESTED" : "COMMENTED",
      }));

      const published: Array<typeof PublishedReviewBody.Type> = [];
      let sourceReads = 0;

      const test = fixture(history, (request, url) => {
        if (request.method === "POST" && url.pathname.endsWith("/reviews")) {
          const review = decodePublishedReview(request);

          published.push(review);
          history.push(
            reviewHistoryWire(
              100 + published.length,
              review.body,
              review.commit_id,
              `2026-09-02T00:00:0${String(published.length)}Z`,
            ),
          );

          return Effect.succeed(jsonResponse(request, { html_url: "https://github.test/review" }));
        }
        if (request.method !== "GET") return undefined;
        if (url.pathname.endsWith("/files")) {
          sourceReads += 1;

          return Effect.succeed(jsonResponse(request, []));
        }
        if (url.pathname.includes("/compare/")) {
          sourceReads += 1;

          return Effect.succeed(jsonResponse(request, { merge_base_commit: { sha: "base" } }));
        }
        if (url.pathname.includes("/git/commits/")) {
          sourceReads += 1;
          const revision = url.pathname.split("/").at(-1) ?? "";

          return Effect.succeed(
            jsonResponse(request, { sha: revision, tree: { sha: `${revision}-tree` } }),
          );
        }
        if (url.pathname.includes("/git/trees/")) {
          sourceReads += 1;
          const tree = url.pathname.split("/").at(-1) ?? "";

          return Effect.succeed(jsonResponse(request, { sha: tree, tree: [], truncated: false }));
        }

        return undefined;
      });

      const options = { PR_REVIEW_AUTOMATIC_LIMIT: "5" };

      yield* test.run(options);
      expect(published).toEqual([
        expect.objectContaining({
          commit_id: "head",
          event: "COMMENT",
          body: expect.stringContaining("Automatic reviews are paused for this pull request"),
          comments: [],
        }),
      ]);
      expect(sourceReads).toBe(0);
      expect(test.writes.at(-1)?.body).toMatchObject({ conclusion: "failure" });

      yield* test.run(options);
      expect(published).toHaveLength(1);
      expect(sourceReads).toBe(0);
      expect(test.writes).toHaveLength(2);

      yield* test.run({
        ...options,
        PR_REVIEW_COMMAND: "@effect-agent review full",
        PR_REVIEW_COMMENT_ID: "42",
      });
      expect(sourceReads).toBeGreaterThan(0);
      expect(published[1]).toEqual({
        commit_id: "head",
        event: "COMMENT",
        body: expect.stringContaining(reviewMarker(false)),
        comments: [],
      });
      expect(published).toHaveLength(2);
      expect(test.writes.at(-1)?.body).toMatchObject({ conclusion: "failure" });
    }),
  );

  it.effect("keeps an uncertain completion write failing the job without replaying it", () =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>();

      const test = fixture(undefined, (request) =>
        request.method === "PATCH"
          ? Deferred.succeed(started, undefined).pipe(Effect.andThen(Effect.never))
          : undefined,
      );

      const fiber = yield* Effect.forkChild(Effect.exit(test.run()));

      yield* Deferred.await(started);
      yield* TestClock.adjust("10 seconds");
      const exit = yield* Fiber.join(fiber);

      expect(Exit.isFailure(exit)).toBe(true);
      if (Exit.isFailure(exit))
        expect(Option.getOrUndefined(Cause.findErrorOption(exit.cause))).toMatchObject({
          _tag: "GitHubApiFailure",
          reason: expect.stringContaining("outcome is unknown"),
        });
      expect(test.writes).toHaveLength(2);
    }),
  );
});

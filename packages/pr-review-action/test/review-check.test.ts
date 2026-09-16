import { NodeServices } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import { Cause, ConfigProvider, Deferred, Effect, Exit, Fiber, Option, Schema } from "effect";
import { TestClock } from "effect/testing";
import { HttpClient, HttpClientResponse } from "effect/unstable/http";

import { reviewActionProgram, withActionInputs } from "../src/action.ts";
import { reviewMarker, reviewPauseMarker } from "../src/selection.ts";

type Request = Parameters<typeof HttpClientResponse.fromWeb>[0];

const json = (request: Request, body: unknown, status = 200) =>
  HttpClientResponse.fromWeb(request, new globalThis.Response(JSON.stringify(body), { status }));

const historyItem = (body: string, commit = "old-head", state = "COMMENTED") => ({
  id: 1,
  body,
  commit_id: commit,
  state,
  submitted_at: "2026-09-01T00:00:00Z",
  user: { login: "effect-agent[bot]", type: "Bot" },
});

const fixture = (
  options: {
    readonly history?: ReadonlyArray<ReturnType<typeof historyItem>>;
    readonly intercept?: (
      request: Request,
      url: URL,
    ) => Effect.Effect<HttpClientResponse.HttpClientResponse> | undefined;
  } = {},
) => {
  const requests: Array<string> = [];
  const writes: Array<{ path: string; body: Schema.Json }> = [];
  const checks: Array<{ id: number; name: string; head_sha: string; external_id: string }> = [];
  let currentHead = "head";

  const client = HttpClient.make((request, url) =>
    Effect.suspend(() => {
      requests.push(`${request.method} ${url.pathname}`);
      if (request.method === "POST" || request.method === "PATCH") {
        if (request.body._tag !== "Uint8Array") return Effect.die("Expected JSON request");
        writes.push({
          path: url.pathname,
          body: Schema.decodeSync(Schema.fromJsonString(Schema.Json))(
            new TextDecoder().decode(request.body.body),
          ),
        });
      }
      const intercepted = options.intercept?.(request, url);

      if (intercepted !== undefined) return intercepted;

      if (url.pathname.includes("/check-runs")) {
        expect(request.headers.authorization).toBe("Bearer checks-token");
        if (request.method === "GET") {
          expect(url.pathname).toContain(`/commits/${currentHead}/check-runs`);
          expect(url.searchParams.get("check_name")).toBe("Effect Agent review");

          return Effect.succeed(json(request, { total_count: checks.length, check_runs: checks }));
        }
        if (request.method === "POST") {
          const check = {
            id: 100 + checks.length,
            name: "Effect Agent review",
            head_sha: currentHead,
            external_id: "effect-agent-pr-review:v1:12",
          };

          checks.push(check);

          return Effect.succeed(json(request, check));
        }

        return Effect.succeed(
          json(
            request,
            checks.find((check) => url.pathname.endsWith(`/${String(check.id)}`)),
          ),
        );
      }

      expect(request.headers.authorization).toBe("Bearer review-token");
      if (url.pathname.endsWith("/reactions"))
        return Effect.succeed(json(request, { id: 42, content: "eyes" }));
      if (url.pathname.endsWith("/pulls/12"))
        return Effect.succeed(
          json(request, {
            number: 12,
            title: "Review fixture",
            body: null,
            draft: false,
            html_url: "https://github.test/fixtures/example/pull/12",
            base: { sha: "base" },
            head: { sha: currentHead },
          }),
        );
      if (url.pathname.endsWith("/reviews"))
        return Effect.succeed(
          json(
            request,
            request.method === "GET"
              ? (options.history ?? [])
              : { html_url: "https://github.test/fixtures/example/pull/12#review" },
          ),
        );
      if (url.pathname.endsWith("/files")) return Effect.succeed(json(request, []));
      if (url.pathname.includes("/compare/"))
        return Effect.succeed(json(request, { merge_base_commit: { sha: "base" } }));
      if (url.pathname.includes("/git/commits/")) {
        const sha = url.pathname.split("/").at(-1);

        return Effect.succeed(json(request, { sha, tree: { sha: `${sha}-tree` } }));
      }
      if (url.pathname.includes("/git/trees/"))
        return Effect.succeed(
          json(request, { sha: url.pathname.split("/").at(-1), tree: [], truncated: false }),
        );

      return Effect.die(`Unexpected request ${request.method} ${url.pathname}`);
    }),
  );

  const run = (env: Record<string, string> = {}) =>
    reviewActionProgram.pipe(
      Effect.provideService(
        ConfigProvider.ConfigProvider,
        withActionInputs(
          ConfigProvider.fromEnv({
            env: {
              GITHUB_REPOSITORY: "fixtures/example",
              "INPUT_GITHUB-TOKEN": "review-token",
              "INPUT_CHECKS-TOKEN": "checks-token",
              "INPUT_CHECK-NAME": "Effect Agent review",
              GITHUB_API_URL: "https://api.github.test",
              GITHUB_SERVER_URL: "https://github.test",
              GITHUB_RUN_ID: "1234",
              GITHUB_SHA: "default-branch-head",
              PR_REVIEW_PULL_REQUEST: "12",
              PR_REVIEW_AUTHOR: "effect-agent[bot]",
              PR_REVIEW_MODEL: "gpt-6-astra",
              PR_REVIEW_AUTOMATIC_LIMIT: "0",
              ...env,
            },
          }),
        ),
      ),
      Effect.provideService(HttpClient.HttpClient, client),
      Effect.provide(NodeServices.layer),
    );

  return {
    run,
    requests,
    writes,
    checks,
    moveHead: () => {
      currentHead = "new-head";
    },
  };
};

const full = { PR_REVIEW_COMMAND: "@effect-agent review full", PR_REVIEW_COMMENT_ID: "42" };

describe("PR commit review checks", () => {
  it.effect(
    "replaces a paused failure with manual progress on the PR commit and preserves it on skipped pushes",
    () =>
      Effect.gen(function* () {
        const test = fixture({
          history: [historyItem(reviewMarker(true), "old-head", "CHANGES_REQUESTED")],
        });

        yield* test.run();
        expect(test.writes.at(-1)?.body).toMatchObject({
          status: "completed",
          conclusion: "failure",
          output: { title: "1 earlier change request(s) unresolved" },
        });

        const offset = test.writes.length;

        yield* test.run(full);
        expect(test.writes.slice(offset)).toEqual([
          {
            path: "/repos/fixtures/example/issues/comments/42/reactions",
            body: { content: "eyes" },
          },
          {
            path: "/repos/fixtures/example/check-runs",
            body: {
              name: "Effect Agent review",
              head_sha: "head",
              external_id: "effect-agent-pr-review:v1:12",
              status: "in_progress",
              details_url: "https://github.test/fixtures/example/actions/runs/1234",
              output: {
                title: "Review in progress",
                summary: "Reviewing the pull request at the attached commit.",
              },
            },
          },
          {
            path: "/repos/fixtures/example/pulls/12/reviews",
            body: expect.objectContaining({
              commit_id: "head",
              body: expect.stringContaining(reviewMarker(false)),
            }),
          },
          {
            path: "/repos/fixtures/example/check-runs/101",
            body: expect.objectContaining({
              status: "completed",
              conclusion: "failure",
              details_url: "https://github.test/fixtures/example/pull/12#review",
            }),
          },
        ]);
        expect(test.requests.indexOf("POST /repos/fixtures/example/check-runs")).toBeLessThan(
          test.requests.indexOf("GET /repos/fixtures/example/pulls/12/files"),
        );

        const afterManual = test.writes.length;

        yield* test.run();
        expect(test.writes).toHaveLength(afterManual);
      }),
  );

  it.effect.each([
    {
      label: "completed head",
      history: [historyItem(reviewMarker(false), "head")],
      conclusion: "success",
    },
    {
      label: "paused new head",
      history: [historyItem(reviewPauseMarker(0))],
      conclusion: "action_required",
    },
    {
      label: "incomplete head",
      history: [historyItem(reviewMarker(true, false), "head")],
      conclusion: "failure",
    },
  ])("reports $label honestly without reading source", ({ history, conclusion }) =>
    Effect.gen(function* () {
      const test = fixture({ history });

      yield* test.run();
      expect(test.writes.at(-1)?.body).toMatchObject({ status: "completed", conclusion });
      expect(test.requests.some((request) => request.endsWith("/files"))).toBe(false);
    }),
  );

  it.effect(
    "completes a manual full pass successfully even when the same head was already reviewed",
    () =>
      Effect.gen(function* () {
        const test = fixture({ history: [historyItem(reviewMarker(true), "head")] });

        yield* test.run(full);
        expect(test.writes.at(-1)?.body).toMatchObject({
          conclusion: "success",
          details_url: "https://github.test/fixtures/example/pull/12#review",
        });
        expect(test.requests).toContain("GET /repos/fixtures/example/pulls/12/files");
      }),
  );

  it.effect.each(["start-denied", "complete-denied", "wrong-identity", "defect"] as const)(
    "keeps infrastructure failures failing the job: %s",
    (mode) =>
      Effect.gen(function* () {
        const test = fixture({
          history: [historyItem(reviewMarker(true), "old-head", "CHANGES_REQUESTED")],
          intercept: (request, url) => {
            if (
              (mode === "start-denied" &&
                request.method === "POST" &&
                url.pathname.endsWith("/check-runs")) ||
              (mode === "complete-denied" && request.method === "PATCH")
            )
              return Effect.succeed(json(request, {}, 403));
            if (
              mode === "wrong-identity" &&
              request.method === "POST" &&
              url.pathname.endsWith("/check-runs")
            )
              return Effect.succeed(
                json(request, {
                  id: 100,
                  name: "Effect Agent review",
                  head_sha: "other-head",
                  external_id: "effect-agent-pr-review:v1:12",
                }),
              );
            if (mode === "defect" && url.pathname.endsWith("/files"))
              return Effect.die("private defect details");
          },
        });

        const exit = yield* Effect.exit(test.run(full));

        expect(Exit.isFailure(exit)).toBe(true);
        expect(
          test.requests.filter((request) => request === "POST /repos/fixtures/example/check-runs"),
        ).toHaveLength(1);
        if (mode === "start-denied" || mode === "wrong-identity")
          expect(test.requests.some((request) => request.endsWith("/files"))).toBe(false);
        else expect(test.writes.at(-1)?.body).toMatchObject({ conclusion: "failure" });
        expect(
          JSON.stringify(test.writes.filter((write) => write.path.includes("check-runs"))),
        ).not.toContain("private defect details");
      }),
  );

  it.effect("cancels the inspected commit's check when a push races publication", () =>
    Effect.gen(function* () {
      const test = fixture({
        intercept: (_request, url) => {
          if (url.pathname.endsWith("/files")) test.moveHead();
        },
      });

      const exit = yield* Effect.exit(test.run(full));

      expect(Exit.isFailure(exit)).toBe(true);
      expect(test.checks).toEqual([
        {
          id: 100,
          name: "Effect Agent review",
          head_sha: "head",
          external_id: "effect-agent-pr-review:v1:12",
        },
      ]);
      expect(test.writes.at(-1)?.body).toMatchObject({
        conclusion: "cancelled",
        output: { title: "Pull request changed during review" },
      });
    }),
  );

  it.effect("closes an interrupted check before returning control", () =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>();

      const test = fixture({
        intercept: (_request, url) =>
          url.pathname.endsWith("/files")
            ? Deferred.succeed(started, undefined).pipe(Effect.andThen(Effect.never))
            : undefined,
      });

      const fiber = yield* Effect.forkChild(test.run(full));

      yield* Deferred.await(started);
      yield* Fiber.interrupt(fiber);
      expect(test.writes.at(-1)?.body).toMatchObject({
        status: "completed",
        conclusion: "cancelled",
      });
    }),
  );

  it.effect.each(["POST", "PATCH"] as const)(
    "bounds an uncertain %s check write without replaying it",
    (method) =>
      Effect.gen(function* () {
        const started = yield* Deferred.make<void>();

        const test = fixture({
          intercept: (request, url) =>
            request.method === method && url.pathname.includes("/check-runs")
              ? Deferred.succeed(started, undefined).pipe(Effect.andThen(Effect.never))
              : undefined,
        });

        const fiber = yield* Effect.forkChild(Effect.exit(test.run(full)));

        yield* Deferred.await(started);
        yield* TestClock.adjust("10 seconds");
        const exit = yield* Fiber.join(fiber);

        expect(Exit.isFailure(exit)).toBe(true);
        if (Exit.isFailure(exit))
          expect(Option.getOrUndefined(Cause.findErrorOption(exit.cause))).toMatchObject({
            _tag: "GitHubApiFailure",
            reason: expect.stringContaining("outcome is unknown"),
          });
        expect(
          test.requests.filter(
            (request) => request.startsWith(method) && request.includes("/check-runs"),
          ),
        ).toHaveLength(1);
      }),
  );
});

import {
  ReviewChange,
  ReviewFinding,
  ReviewOutcome,
  ReviewReport,
  ReviewUsage,
  type ReviewSeverity,
} from "@effect-agent/pr-review";
import { NodeServices } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import { Config, ConfigProvider, Deferred, Effect, Fiber, Ref, Schema } from "effect";
import { Response } from "effect/unstable/ai";
import { HttpClient, HttpClientResponse } from "effect/unstable/http";

import {
  MAX_REVIEW_SHARDS,
  estimateGpt56CostMicrousd,
  mergeReviewOutcomes,
  prepareReviewSurface,
  reviewActionProgram,
  reviewEventFor,
  runReviewWave,
  shardReviewChanges,
  withActionInputs,
} from "../src/action.ts";
import type { ChangedFile } from "../src/github.ts";
import { reviewMarker } from "../src/selection.ts";

const file = (path: string, patch: string | undefined): ChangedFile => ({
  path,
  status: "modified",
  additions: 1,
  deletions: 0,
  patch,
});

describe("GitHub diff admission", () => {
  it("PRR-003 bounds admitted files and reports the remainder", () => {
    const files = Array.from({ length: 102 }, (_, index) =>
      file(`src/file-${String(index)}.ts`, "@@ -0,0 +1 @@\n+export const value = 1;"),
    );
    const surface = prepareReviewSurface(files, []);

    expect(surface.changes).toHaveLength(100);
    expect(surface.unreviewedPaths).toEqual(["src/file-100.ts", "src/file-101.ts"]);
  });

  it("PRR-003 distinguishes ignored and unavailable files", () => {
    const surface = prepareReviewSurface(
      [file("bun.lock", "@@ -1 +1 @@\n-old\n+new"), file("assets/image.png", undefined)],
      ["**/*.lock"],
    );

    expect(surface.changes).toHaveLength(0);
    expect(surface.ignoredPaths).toEqual(["bun.lock"]);
    expect(surface.unreviewedPaths).toEqual(["assets/image.png"]);
  });
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
          PR_REVIEW_COMMAND: "/effect-agent review full",
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

  it.effect("PRR-007 keeps a manual review incremental after a force-push", () =>
    Effect.gen(function* () {
      const reviewedHead = "1".repeat(40);
      const currentHead = "2".repeat(40);
      const reviewedTree = "3".repeat(40);
      const currentTree = "4".repeat(40);
      const unchangedBlob = "5".repeat(40);
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
            base: { sha: "0".repeat(40) },
            head: { sha: currentHead },
          };
        } else if (request.method === "GET" && url.pathname.endsWith("/pulls/12/reviews")) {
          body = [
            {
              id: 1,
              body: reviewMarker(false),
              commit_id: reviewedHead,
              submitted_at: "2026-08-25T00:00:00Z",
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
        } else if (request.method === "GET" && url.pathname.endsWith(reviewedHead)) {
          body = { sha: reviewedHead, tree: { sha: reviewedTree } };
        } else if (request.method === "GET" && url.pathname.endsWith(currentHead)) {
          body = { sha: currentHead, tree: { sha: currentTree } };
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
          PR_REVIEW_COMMAND: "/effect-agent review",
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
      expect(requested.some((request) => request.includes("/compare/"))).toBe(false);
      expect(requested.filter((request) => request.includes("/git/trees/"))).toHaveLength(2);
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
    const usage = new Response.Usage({
      inputTokens: { total: 10_000, uncached: 8_000, cacheRead: 2_000, cacheWrite: 1_000 },
      outputTokens: { total: 500, text: 400, reasoning: 100 },
    });

    expect(estimateGpt56CostMicrousd("gpt-5.6-sol", usage)).toBe(43_800);
    expect(estimateGpt56CostMicrousd("gpt-5.6-terra", usage)).toBe(22_900);
    expect(estimateGpt56CostMicrousd("gpt-5.6-luna", usage)).toBe(2_290);
    expect(estimateGpt56CostMicrousd("custom-model", usage)).toBeUndefined();
  });
});

describe("bounded review wave", () => {
  it("PRR-005 partitions a large diff into at most four balanced shards", () => {
    const changes = [
      ["a.ts", 75_000],
      ["b.ts", 65_000],
      ["c.ts", 55_000],
      ["d.ts", 45_000],
      ["e.ts", 35_000],
      ["f.ts", 25_000],
    ].map(([path, chars]) =>
      ReviewChange.make({ path: String(path), patch: "x".repeat(Number(chars)) }),
    );

    const shards = shardReviewChanges(changes);
    expect(shards).toHaveLength(MAX_REVIEW_SHARDS);
    expect(shards.map((shard) => shard.map((change) => change.path))).toEqual([
      ["a.ts"],
      ["b.ts"],
      ["c.ts", "f.ts"],
      ["d.ts", "e.ts"],
    ]);
    expect(
      shardReviewChanges([
        ReviewChange.make({ path: "small-a.ts", patch: "x".repeat(10_000) }),
        ReviewChange.make({ path: "small-b.ts", patch: "x".repeat(10_000) }),
      ]),
    ).toHaveLength(1);
  });

  it.effect("PRR-005 starts independent shards concurrently", () =>
    Effect.gen(function* () {
      const firstStarted = yield* Deferred.make<void>();
      const secondStarted = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const review = (started: Deferred.Deferred<void>, label: string) =>
        Deferred.succeed(started, undefined).pipe(
          Effect.andThen(Deferred.await(release)),
          Effect.as(label),
        );
      const fiber = yield* Effect.forkChild(
        runReviewWave([review(firstStarted, "first"), review(secondStarted, "second")]),
      );

      yield* Effect.all([Deferred.await(firstStarted), Deferred.await(secondStarted)], {
        concurrency: "unbounded",
      });
      yield* Deferred.succeed(release, undefined);
      expect(yield* Fiber.join(fiber)).toEqual(["first", "second"]);
    }),
  );

  it("PRR-005 merges usage and keeps the twelve highest-severity findings", () => {
    const outcome = (index: number, severity: ReviewSeverity): ReviewOutcome =>
      ReviewOutcome.make({
        turns: 1,
        usage: ReviewUsage.make({
          inputTokens: 100,
          uncachedInputTokens: 70,
          cachedInputTokens: 20,
          cacheWriteInputTokens: 10,
          outputTokens: 10,
          estimatedCostMicrousd: 25,
        }),
        report: ReviewReport.make({
          summary: `Summary ${String(index)}`,
          findings: Array.from({ length: 4 }, (_, findingIndex) =>
            ReviewFinding.make({
              path: `src/${String(index)}-${String(findingIndex)}.ts`,
              severity,
              category: "correctness",
              title: `Finding ${String(index)}-${String(findingIndex)}`,
              body: "Actionable defect.",
            }),
          ),
        }),
      });
    const merged = mergeReviewOutcomes([
      outcome(1, "nit"),
      outcome(2, "important"),
      outcome(3, "blocking"),
      outcome(4, "nit"),
    ]);

    expect(merged.inputTokens).toBe(400);
    expect(merged.uncachedInputTokens).toBe(280);
    expect(merged.cachedInputTokens).toBe(80);
    expect(merged.cacheWriteInputTokens).toBe(40);
    expect(merged.outputTokens).toBe(40);
    expect(merged.estimatedCostMicrousd).toBe(100);
    expect(merged.report.summary).toContain("Shard 4");
    expect(merged.report.findings).toHaveLength(12);
    expect(merged.report.findings.slice(0, 4).map((finding) => finding.severity)).toEqual([
      "blocking",
      "blocking",
      "blocking",
      "blocking",
    ]);
  });
});

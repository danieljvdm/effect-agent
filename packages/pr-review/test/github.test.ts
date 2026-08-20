import { describe, expect, it } from "@effect/vitest";
import { Effect, Layer, Option, Redacted } from "effect";
import { HttpClient, HttpClientResponse } from "effect/unstable/http";

import {
  GitHubReviewTarget,
  gitHubPullRequestSourceLayer,
  gitHubPriorReviewsLayer,
  gitHubReviewRetirementHostLayer,
  PriorReviews,
  PullRequestSource,
  renderFingerprintMarker,
  ReviewRetirementHost,
  ReviewState,
  ReviewStateAuthenticator,
  webCryptoReviewStateAuthenticatorLayer,
} from "../src/index.ts";

const REVIEWED_HEAD_SHA = "2".repeat(40);
const FINGERPRINT = "a".repeat(64);

const reviewState = ReviewState.make({
  version: 2,
  repository: "acme/widgets",
  pullRequestNumber: 30,
  baseRef: "main",
  baseSha: "1".repeat(40),
  headRef: "fix/review",
  reviewedHeadSha: REVIEWED_HEAD_SHA,
  profileFingerprint: "b".repeat(64),
  acceptedScopeFingerprint: "c".repeat(64),
  reviewedPathCount: 1,
  unresolvedFindings: [],
  unresolvedConcerns: [],
  unreviewedPaths: [],
  settled: true,
  lastReviewMode: "full",
});

describe("GitHub prior reviews", () => {
  it.effect("matches continuity state and fingerprints to the configured posting bot", () =>
    Effect.gen(function* () {
      const authenticatorLayer = webCryptoReviewStateAuthenticatorLayer(
        Redacted.make("stable-state-secret"),
      );
      const stateMarker = yield* Effect.gen(function* () {
        const authenticator = yield* ReviewStateAuthenticator;
        return yield* authenticator.render(reviewState);
      }).pipe(Effect.provide(authenticatorLayer));
      const body = ["Review body", renderFingerprintMarker(FINGERPRINT), stateMarker].join("\n");
      const client = HttpClient.make((request, url) => {
        if (url.pathname !== "/repos/acme/widgets/pulls/30/reviews") {
          return Effect.die(new Error(`Unexpected request: ${url}`));
        }
        return Effect.succeed(
          HttpClientResponse.fromWeb(
            request,
            new Response(
              JSON.stringify([
                {
                  body,
                  commit_id: REVIEWED_HEAD_SHA,
                  user: { login: "kommunikasie[bot]", type: "Bot" },
                },
                {
                  body: body.replace(FINGERPRINT, "d".repeat(64)),
                  commit_id: REVIEWED_HEAD_SHA,
                  user: { login: "other-app[bot]", type: "Bot" },
                },
                {
                  body: body.replace(FINGERPRINT, "e".repeat(64)),
                  commit_id: REVIEWED_HEAD_SHA,
                  user: { login: "kommunikasie[bot]", type: "User" },
                },
              ]),
              { status: 200, headers: { "Content-Type": "application/json" } },
            ),
          ),
        );
      });
      const dependencies = Layer.merge(
        GitHubReviewTarget.layer({
          apiUrl: "https://api.github.test",
          repository: "acme/widgets",
          number: 30,
          token: Option.some(Redacted.make("github-app-token")),
          reviewAuthorLogin: "Kommunikasie[bot]",
        }),
        Layer.succeed(HttpClient.HttpClient)(client),
      );
      const priorReviewsLayer = gitHubPriorReviewsLayer.pipe(Layer.provide(dependencies));
      const testLayer = Layer.merge(priorReviewsLayer, authenticatorLayer);

      const result = yield* Effect.gen(function* () {
        const priorReviews = yield* PriorReviews;
        return {
          fingerprint: yield* priorReviews.latestFingerprint,
          state: yield* priorReviews.latestState,
        };
      }).pipe(Effect.provide(testLayer));

      expect(Option.getOrUndefined(result.fingerprint)).toBe(FINGERPRINT);
      expect(Option.getOrUndefined(result.state)).toEqual(reviewState);
    }),
  );

  it.effect("compares rewritten heads with a two-dot tree request", () =>
    Effect.gen(function* () {
      const requested: Array<string> = [];
      const client = HttpClient.make((request, url) => {
        requested.push(url.pathname);
        return Effect.succeed(
          HttpClientResponse.fromWeb(
            request,
            new Response(
              JSON.stringify({
                status: "diverged",
                base_commit: { sha: REVIEWED_HEAD_SHA },
                merge_base_commit: { sha: "1".repeat(40) },
                files: [
                  {
                    filename: "src/fix.ts",
                    status: "modified",
                    additions: 1,
                    deletions: 1,
                    patch: "@@ -1 +1 @@\n-before\n+after",
                  },
                ],
              }),
              { status: 200, headers: { "Content-Type": "application/json" } },
            ),
          ),
        );
      });
      const comparison = yield* Effect.gen(function* () {
        const priorReviews = yield* PriorReviews;
        return yield* priorReviews.compareTrees(REVIEWED_HEAD_SHA, "3".repeat(40));
      }).pipe(
        Effect.provide(
          gitHubPriorReviewsLayer.pipe(
            Layer.provide(
              Layer.merge(
                GitHubReviewTarget.layer({
                  apiUrl: "https://api.github.test",
                  repository: "acme/widgets",
                  number: 30,
                  token: Option.some(Redacted.make("github-app-token")),
                }),
                Layer.succeed(HttpClient.HttpClient)(client),
              ),
            ),
          ),
        ),
      );

      expect(requested).toEqual([
        `/repos/acme/widgets/compare/${REVIEWED_HEAD_SHA}..${"3".repeat(40)}`,
      ]);
      expect(comparison.status).toBe("ahead");
      expect(comparison.mergeBaseSha).toBe(REVIEWED_HEAD_SHA);
      expect(comparison.files.map((file) => file.path)).toEqual(["src/fix.ts"]);
    }),
  );
});

describe("GitHub review retirement comments", () => {
  it.effect("decodes outdated inline comments that omit the current line", () =>
    Effect.gen(function* () {
      const client = HttpClient.make((request, url) => {
        if (url.pathname === "/repos/acme/widgets/pulls/30/reviews") {
          return Effect.succeed(
            HttpClientResponse.fromWeb(
              request,
              new Response(JSON.stringify([]), {
                status: 200,
                headers: { "Content-Type": "application/json" },
              }),
            ),
          );
        }
        expect(url.pathname).toBe("/repos/acme/widgets/pulls/30/reviews/9/comments");
        return Effect.succeed(
          HttpClientResponse.fromWeb(
            request,
            new Response(
              JSON.stringify([
                {
                  node_id: "PRRC_outdated",
                  path: "src/a.ts",
                  body: "**[🛑 blocking] Gone stale**\n\nThe line moved.",
                  original_line: 12,
                },
              ]),
              { status: 200, headers: { "Content-Type": "application/json" } },
            ),
          ),
        );
      });
      const comments = yield* Effect.gen(function* () {
        const host = yield* ReviewRetirementHost;
        return yield* host.listComments(9);
      }).pipe(
        Effect.provide(
          gitHubReviewRetirementHostLayer.pipe(
            Layer.provide(
              Layer.merge(
                GitHubReviewTarget.layer({
                  apiUrl: "https://api.github.test",
                  repository: "acme/widgets",
                  number: 30,
                  token: Option.some(Redacted.make("github-app-token")),
                }),
                Layer.succeed(HttpClient.HttpClient)(client),
              ),
            ),
          ),
        ),
      );

      expect(comments).toEqual([
        {
          nodeId: "PRRC_outdated",
          path: "src/a.ts",
          startLine: 12,
          endLine: 12,
          body: "**[🛑 blocking] Gone stale**\n\nThe line moved.",
        },
      ]);
    }),
  );
});

describe("GitHub pull-request source", () => {
  it.effect("recovers omitted text patches without treating binary content as reviewable", () =>
    Effect.gen(function* () {
      const baseSha = "1".repeat(40);
      const headSha = "2".repeat(40);
      const snapshot = '{"version":"7","tables":{"widgets":{"columns":{"id":"uuid"}}}}';
      const client = HttpClient.make((request, url) => {
        if (url.pathname === "/repos/acme/widgets/pulls/30") {
          return Effect.succeed(
            HttpClientResponse.fromWeb(
              request,
              new Response(
                JSON.stringify({
                  number: 30,
                  title: "Add generated schema state",
                  body: "",
                  changed_files: 2,
                  base: { ref: "main", sha: baseSha },
                  head: { ref: "feature/schema", sha: headSha },
                }),
                { status: 200, headers: { "Content-Type": "application/json" } },
              ),
            ),
          );
        }
        if (url.pathname === "/repos/acme/widgets/pulls/30/files") {
          return Effect.succeed(
            HttpClientResponse.fromWeb(
              request,
              new Response(
                JSON.stringify([
                  {
                    filename: "db/snapshot.json",
                    status: "added",
                    additions: 1,
                    deletions: 0,
                  },
                  {
                    filename: "assets/logo.png",
                    status: "added",
                    additions: 0,
                    deletions: 0,
                  },
                ]),
                { status: 200, headers: { "Content-Type": "application/json" } },
              ),
            ),
          );
        }
        if (url.pathname === "/repos/acme/widgets/contents/db/snapshot.json") {
          expect(url.searchParams.get("ref")).toBe(headSha);
          return Effect.succeed(
            HttpClientResponse.fromWeb(request, new Response(snapshot, { status: 200 })),
          );
        }
        if (url.pathname === "/repos/acme/widgets/contents/assets/logo.png") {
          return Effect.succeed(
            HttpClientResponse.fromWeb(
              request,
              new Response(new Uint8Array([0x89, 0x50, 0x4e, 0x47, 0x00]), { status: 200 }),
            ),
          );
        }
        return Effect.die(new Error(`Unexpected request: ${url}`));
      });
      const dependencies = Layer.merge(
        GitHubReviewTarget.layer({
          apiUrl: "https://api.github.test",
          repository: "acme/widgets",
          number: 30,
          token: Option.none(),
        }),
        Layer.succeed(HttpClient.HttpClient)(client),
      );
      const sourceLayer = gitHubPullRequestSourceLayer.pipe(Layer.provide(dependencies));
      const files = yield* Effect.gen(function* () {
        const source = yield* PullRequestSource;
        return yield* source.changedFiles;
      }).pipe(Effect.provide(sourceLayer));

      expect(files.find((file) => file.path === "db/snapshot.json")?.reviewHeadContent).toBe(
        snapshot,
      );
      expect(
        files.find((file) => file.path === "assets/logo.png")?.reviewHeadContent,
      ).toBeUndefined();
    }),
  );
});

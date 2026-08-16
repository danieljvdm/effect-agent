import { describe, expect, it } from "@effect/vitest";
import { Effect, Layer, Option, Redacted } from "effect";
import { HttpClient, HttpClientResponse } from "effect/unstable/http";

import {
  GitHubReviewTarget,
  gitHubPriorReviewsLayer,
  PriorReviews,
  renderFingerprintMarker,
  ReviewState,
  ReviewStateAuthenticator,
  webCryptoReviewStateAuthenticatorLayer,
} from "../src/index.ts";

const REVIEWED_HEAD_SHA = "2".repeat(40);
const FINGERPRINT = "a".repeat(64);

const reviewState = ReviewState.make({
  version: 1,
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
});

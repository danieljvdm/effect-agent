import { ReviewFollowUp } from "@effect-agent/pr-review/review";
import { describe, expect, it } from "@effect/vitest";
import { Effect, Encoding, Exit, Fiber, Logger, Redacted, Schema } from "effect";
import { TestClock } from "effect/testing";
import { HttpClient, HttpClientResponse } from "effect/unstable/http";

import { makeGitHubClient } from "../src/github.ts";
import { reviewMarker, type ReviewHistoryItem } from "../src/selection.ts";

const repository = "reve-ai/example";
const baseRevision = "1".repeat(40);
const headRevision = "2".repeat(40);
const headTree = "4".repeat(40);

const priorReview: ReviewHistoryItem = {
  id: 42,
  authorLogin: "effect-agent[bot]",
  authorType: "Bot",
  body: `A prior blocking review.\n${reviewMarker(true)}`,
  commitId: baseRevision,
  submittedAt: "2026-09-01T00:00:00Z",
  state: "CHANGES_REQUESTED",
};

const priorReviewWire = {
  id: priorReview.id,
  body: priorReview.body,
  commit_id: priorReview.commitId,
  submitted_at: priorReview.submittedAt,
  state: priorReview.state,
  user: { login: priorReview.authorLogin, type: priorReview.authorType },
};

const priorFollowUp = ReviewFollowUp.make({
  id: "42",
  description: JSON.stringify({
    reviewedCommit: priorReview.commitId,
    review: priorReview.body,
    comments: [],
  }),
});

describe("addressed review verification", () => {
  it.effect.each(["success", "edited-comment", "untrusted"] as const)(
    "dismisses only an unchanged owned review on the inspected head: %s",
    (mode) =>
      Effect.gen(function* () {
        const writes: Array<string> = [];

        const client = HttpClient.make((request, url) => {
          if (url.pathname.endsWith("/comments"))
            return Effect.succeed(
              HttpClientResponse.fromWeb(
                request,
                new globalThis.Response(
                  JSON.stringify(
                    mode === "edited-comment"
                      ? [
                          {
                            pull_request_review_id: 42,
                            path: "src/changed.ts",
                            body: "A changed blocker",
                            user: priorReviewWire.user,
                          },
                        ]
                      : [],
                  ),
                ),
              ),
            );
          if (request.method === "PUT") {
            const encoded =
              request.body._tag === "Uint8Array"
                ? new TextDecoder().decode(request.body.body)
                : "{}";

            const body = Schema.decodeSync(
              Schema.fromJsonString(Schema.Struct({ message: Schema.String })),
            )(encoded);

            writes.push(body.message);
          }

          const body =
            request.method === "PUT"
              ? { id: 42, state: "DISMISSED" }
              : url.pathname.endsWith("/reviews/42")
                ? {
                    ...priorReviewWire,
                    body: priorReview.body,
                    state: priorReview.state,
                  }
                : {
                    number: 12,
                    title: "Fix",
                    body: null,
                    draft: false,
                    html_url: "https://github.test/pr/12",
                    base: { sha: baseRevision },
                    head: { sha: headRevision },
                  };

          return Effect.succeed(
            HttpClientResponse.fromWeb(
              request,
              new globalThis.Response(JSON.stringify(body), {
                status: 200,
              }),
            ),
          );
        });

        const github = yield* makeGitHubClient({
          repository,
          pullRequest: 12,
          token: Redacted.make("token"),
        }).pipe(Effect.provideService(HttpClient.HttpClient, client));

        const result = yield* github
          .dismissReview({
            review: mode === "untrusted" ? { ...priorReview, authorType: "User" } : priorReview,
            followUp: priorFollowUp,
            reviewAuthor: priorReview.authorLogin,
            commitId: headRevision,
            evidence: "All candidate retention is now bounded.",
          })
          .pipe(Effect.exit);

        expect(Exit.isSuccess(result)).toBe(mode === "success" || false);
        expect(writes).toHaveLength(["success", "denied", "wrong-response"].includes(mode) ? 1 : 0);
        if (mode === "success") expect(writes[0]).toContain(headRevision);
      }),
  );
});

const entry = (
  path: string,
  sha: string,
  mode: "100644" | "100755" | "120000" | "040000" | "160000" = "100644",
  type: "blob" | "tree" | "commit" = "blob",
) => ({ path, sha, mode, type, ...(type === "blob" ? { size: 1 } : {}) });

describe("GitHub read recovery", () => {
  it.effect("keeps exhausted blob-read diagnostics free of credentials and private source", () =>
    Effect.gen(function* () {
      let reads = 0;
      const logs: Array<unknown> = [];
      const blob = "a".repeat(40);
      const path = "src/example.ts";

      const client = HttpClient.make((request, url) => {
        let body: unknown;
        let status = 200;
        let headers: Record<string, string> = {};

        if (url.pathname.includes("/git/commits/")) {
          body = { sha: headRevision, tree: { sha: headTree } };
        } else if (url.pathname.includes("/git/trees/")) {
          body = { sha: headTree, truncated: false, tree: [entry(path, blob)] };
        } else {
          reads += 1;
          body = {
            sha: blob,
            size: 23,
            encoding: "base64",
            content: Encoding.encodeBase64("private-source-sentinel"),
          };
          {
            status = 503;
            headers = {};
          }
        }

        return Effect.succeed(
          HttpClientResponse.fromWeb(
            request,
            new globalThis.Response(JSON.stringify(body), {
              status,
              headers: { ...headers, "x-github-request-id": "request-123" },
            }),
          ),
        );
      });

      const github = yield* makeGitHubClient({
        repository,
        pullRequest: 12,
        token: Redacted.make("credential-sentinel"),
      }).pipe(Effect.provideService(HttpClient.HttpClient, client));

      const snapshot = yield* github.readTreeSnapshot(headRevision);

      const fiber = yield* snapshot
        .readTextFile(path)
        .pipe(
          Effect.provide(
            Logger.layer([Logger.make<unknown, void>(({ message }) => logs.push(message))]),
          ),
          Effect.result,
          Effect.forkChild,
        );

      yield* TestClock.adjust("90 seconds");
      const result = yield* Fiber.join(fiber);

      expect(result._tag).toBe("Failure");
      if (result._tag === "Failure") {
        expect(result.failure).toMatchObject({
          operation: "get Git blob",
          attempts: 4,
          status: 503,
          requestId: "request-123",
        });
      }
      expect(JSON.stringify(logs)).not.toContain("credential-sentinel");
      expect(JSON.stringify(logs)).not.toContain("private-source-sentinel");
    }),
  );

  it.effect("does not replay an uncertain GitHub write", () =>
    Effect.gen(function* () {
      let writes = 0;

      const client = HttpClient.make((request) => {
        writes += 1;
        expect(request.method).toBe("POST");

        return Effect.succeed(
          HttpClientResponse.fromWeb(request, new globalThis.Response("", { status: 503 })),
        );
      });

      const github = yield* makeGitHubClient({
        repository,
        pullRequest: 12,
        token: Redacted.make("token"),
      }).pipe(Effect.provideService(HttpClient.HttpClient, client));

      const failure = yield* github.acknowledgeComment(123).pipe(Effect.flip);

      expect(failure._tag).toBe("GitHubApiFailure");
      expect(writes).toBe(1);
    }),
  );
});

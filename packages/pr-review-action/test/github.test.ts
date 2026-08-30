import { ReviewFinding, ReviewReport } from "@effect-agent/pr-review";
import { describe, expect, it } from "@effect/vitest";
import { Effect, Encoding, Redacted, Ref } from "effect";
import { HttpClient, HttpClientResponse } from "effect/unstable/http";

import { makeGitHubClient } from "../src/github.ts";
import { defaultReviewPresentation } from "../src/presentation.ts";

const repository = "reve-ai/example";
const baseRevision = "1".repeat(40);
const headRevision = "2".repeat(40);
const baseTree = "3".repeat(40);
const headTree = "4".repeat(40);

const entry = (
  path: string,
  sha: string,
  mode: "100644" | "100755" | "120000" | "040000" | "160000" = "100644",
  type: "blob" | "tree" | "commit" = "blob",
) => ({ path, sha, mode, type, ...(type === "blob" ? { size: 1 } : {}) });

it.effect("PRR-009 publishes twenty-four blocking findings at the review field bounds", () =>
  Effect.gen(function* () {
    const findings = Array.from({ length: 24 }, (_, index) =>
      ReviewFinding.make({
        path: `${"src/".repeat(124)}${String(index).padStart(2, "0")}.txt`,
        line: 1,
        severity: "blocking",
        category: "security",
        title: `${String(index).padStart(2, "0")}${"T".repeat(198)}`,
        body: "B".repeat(2_000),
      }),
    );
    const published = yield* Ref.make<ReadonlyArray<string>>([]);
    const reviewUrl = "https://github.test/reve-ai/example/pull/12#pullrequestreview-1";
    const client = HttpClient.make((request, url) =>
      Ref.update(published, (requests) => [...requests, `${request.method} ${url.pathname}`]).pipe(
        Effect.as(
          HttpClientResponse.fromWeb(
            request,
            new globalThis.Response(
              JSON.stringify(
                request.method === "GET"
                  ? {
                      number: 12,
                      title: "Change",
                      body: null,
                      draft: false,
                      html_url: "https://github.test/reve-ai/example/pull/12",
                      base: { sha: baseRevision },
                      head: { sha: headRevision },
                    }
                  : { html_url: reviewUrl },
              ),
              {
                status: 200,
                headers: { "content-type": "application/json" },
              },
            ),
          ),
        ),
      ),
    );
    const github = yield* makeGitHubClient({
      repository,
      pullRequest: 12,
      token: Redacted.make("github-token"),
      apiUrl: "https://api.github.test",
    }).pipe(Effect.provideService(HttpClient.HttpClient, client));

    const reviewBody = defaultReviewPresentation.renderReview({
      report: ReviewReport.make({ summary: "Blocking defects were found.", findings }),
      automaticReviewsRemaining: 1,
      scope: "full",
      reviewedFiles: 1,
      unreviewedFiles: 0,
      ignoredFiles: 0,
      modelTurns: 3,
      complete: true,
      unresolvedChangeRequests: 0,
      inputTokens: 10,
      uncachedInputTokens: 10,
      cachedInputTokens: 0,
      cacheWriteInputTokens: 0,
      outputTokens: 4,
      headRevision,
    });
    const result = yield* github.publishReview({
      commitId: headRevision,
      event: "REQUEST_CHANGES",
      body: reviewBody,
      comments: findings.map((finding) => ({
        path: finding.path,
        line: 1,
        body: defaultReviewPresentation.renderFinding(finding, headRevision),
      })),
    });

    expect(result).toBe(reviewUrl);
    expect(reviewBody).not.toContain("Prompt for all");
    expect(yield* Ref.get(published)).toEqual([
      "GET /repos/reve-ai/example/pulls/12",
      "POST /repos/reve-ai/example/pulls/12/reviews",
    ]);
  }),
);

it.effect("refuses publication after the pull-request head moves", () =>
  Effect.gen(function* () {
    const requests = yield* Ref.make<ReadonlyArray<string>>([]);
    const movedHead = "9".repeat(40);
    const client = HttpClient.make((request, url) =>
      Ref.update(requests, (current) => [...current, `${request.method} ${url.pathname}`]).pipe(
        Effect.as(
          HttpClientResponse.fromWeb(
            request,
            new globalThis.Response(
              JSON.stringify({
                number: 12,
                title: "Moved",
                body: null,
                draft: false,
                html_url: "https://github.test/reve-ai/example/pull/12",
                base: { sha: baseRevision },
                head: { sha: movedHead },
              }),
              { status: 200, headers: { "content-type": "application/json" } },
            ),
          ),
        ),
      ),
    );
    const github = yield* makeGitHubClient({
      repository,
      pullRequest: 12,
      token: Redacted.make("github-token"),
      apiUrl: "https://api.github.test",
    }).pipe(Effect.provideService(HttpClient.HttpClient, client));

    const failure = yield* github
      .publishReview({
        commitId: headRevision,
        event: "COMMENT",
        body: "Review",
        comments: [],
      })
      .pipe(Effect.flip);

    expect(failure).toMatchObject({
      _tag: "StaleReviewHead",
      inspectedHead: headRevision,
      currentHead: movedHead,
    });
    expect(yield* Ref.get(requests)).toEqual(["GET /repos/reve-ai/example/pulls/12"]);
  }),
);

it.effect("retains GitHub's exact previous filename for declared renames", () =>
  Effect.gen(function* () {
    const client = HttpClient.make((request) =>
      Effect.succeed(
        HttpClientResponse.fromWeb(
          request,
          new globalThis.Response(
            JSON.stringify([
              {
                filename: "src/new-name.ts",
                previous_filename: "src/old-name.ts",
                status: "renamed",
                additions: 0,
                deletions: 0,
              },
            ]),
            { status: 200, headers: { "content-type": "application/json" } },
          ),
        ),
      ),
    );
    const github = yield* makeGitHubClient({
      repository,
      pullRequest: 12,
      token: Redacted.make("github-token"),
      apiUrl: "https://api.github.test",
    }).pipe(Effect.provideService(HttpClient.HttpClient, client));

    expect(yield* github.listFiles).toEqual([
      expect.objectContaining({
        path: "src/new-name.ts",
        previousPath: "src/old-name.ts",
        status: "renamed",
      }),
    ]);
  }),
);

describe("GitHub tree comparison", () => {
  it.effect("PRR-008 compares exact contents after rewritten history", () =>
    Effect.gen(function* () {
      const requests = yield* Ref.make<ReadonlyArray<string>>([]);
      const client = HttpClient.make((request, url) => {
        const body = url.pathname.endsWith(`/git/commits/${baseRevision}`)
          ? { sha: baseRevision, tree: { sha: baseTree } }
          : url.pathname.endsWith(`/git/commits/${headRevision}`)
            ? { sha: headRevision, tree: { sha: headTree } }
            : url.pathname.endsWith(`/git/trees/${baseTree}`)
              ? {
                  sha: baseTree,
                  truncated: false,
                  tree: [
                    entry("src", "a".repeat(40), "040000", "tree"),
                    entry("src/unchanged.ts", "a".repeat(40)),
                    entry("src/changed.ts", "b".repeat(40)),
                    entry("src/mode.ts", "c".repeat(40)),
                    entry("src/removed.ts", "d".repeat(40)),
                    entry("src/type.ts", "e".repeat(40)),
                    entry("src/was-file", "f".repeat(40)),
                    entry("src/was-directory", "f".repeat(40), "040000", "tree"),
                  ],
                }
              : {
                  sha: headTree,
                  truncated: false,
                  tree: [
                    entry("src", "b".repeat(40), "040000", "tree"),
                    entry("src/unchanged.ts", "a".repeat(40)),
                    entry("src/changed.ts", "8".repeat(40)),
                    entry("src/mode.ts", "c".repeat(40), "100755"),
                    entry("src/added.ts", "9".repeat(40)),
                    entry("src/type.ts", "e".repeat(40), "160000", "commit"),
                    entry("src/was-file", "0".repeat(40), "040000", "tree"),
                    entry("src/was-directory", "0".repeat(40)),
                  ],
                };
        return Ref.update(requests, (current) => [...current, url.href]).pipe(
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
      const github = yield* makeGitHubClient({
        repository,
        pullRequest: 12,
        token: Redacted.make("github-token"),
        apiUrl: "https://api.github.test",
      }).pipe(Effect.provideService(HttpClient.HttpClient, client));

      const comparison = yield* github.compareTrees(baseRevision, headRevision);

      expect(comparison.changedPaths).toEqual([
        "src/added.ts",
        "src/changed.ts",
        "src/mode.ts",
        "src/removed.ts",
        "src/type.ts",
        "src/was-directory",
        "src/was-file",
      ]);
      expect(yield* Ref.get(requests)).toHaveLength(4);
      expect((yield* Ref.get(requests)).filter((url) => url.endsWith("?recursive=1"))).toHaveLength(
        2,
      );
    }),
  );

  it.effect("PRR-008 refuses a truncated tree instead of widening review scope", () =>
    Effect.gen(function* () {
      const client = HttpClient.make((request, url) => {
        const body = url.pathname.endsWith(`/git/commits/${baseRevision}`)
          ? { sha: baseRevision, tree: { sha: baseTree } }
          : url.pathname.endsWith(`/git/commits/${headRevision}`)
            ? { sha: headRevision, tree: { sha: headTree } }
            : {
                sha: url.pathname.endsWith(baseTree) ? baseTree : headTree,
                truncated: url.pathname.endsWith(headTree),
                tree: [],
              };
        return Effect.succeed(
          HttpClientResponse.fromWeb(
            request,
            new globalThis.Response(JSON.stringify(body), {
              status: 200,
              headers: { "content-type": "application/json" },
            }),
          ),
        );
      });
      const github = yield* makeGitHubClient({
        repository,
        pullRequest: 12,
        token: Redacted.make("github-token"),
        apiUrl: "https://api.github.test",
      }).pipe(Effect.provideService(HttpClient.HttpClient, client));

      const failure = yield* github.compareTrees(baseRevision, headRevision).pipe(Effect.flip);

      expect(failure._tag).toBe("GitHubApiFailure");
      expect(failure.reason).toContain("truncated tree");
    }),
  );

  it.effect("rejects blob content whose identity does not match the frozen tree", () =>
    Effect.gen(function* () {
      const path = "src/changed.ts";
      const baseBlob = "a".repeat(40);
      const headBlob = "b".repeat(40);
      const client = HttpClient.make((request, url) => {
        const body = url.pathname.endsWith(`/git/commits/${baseRevision}`)
          ? { sha: baseRevision, tree: { sha: baseTree } }
          : url.pathname.endsWith(`/git/commits/${headRevision}`)
            ? { sha: headRevision, tree: { sha: headTree } }
            : url.pathname.endsWith(`/git/trees/${baseTree}`)
              ? { sha: baseTree, truncated: false, tree: [entry(path, baseBlob)] }
              : url.pathname.endsWith(`/git/trees/${headTree}`)
                ? { sha: headTree, truncated: false, tree: [entry(path, headBlob)] }
                : {
                    sha: "c".repeat(40),
                    size: 4,
                    encoding: "base64",
                    content: Encoding.encodeBase64("head"),
                  };
        return Effect.succeed(
          HttpClientResponse.fromWeb(
            request,
            new globalThis.Response(JSON.stringify(body), {
              status: 200,
              headers: { "content-type": "application/json" },
            }),
          ),
        );
      });
      const github = yield* makeGitHubClient({
        repository,
        pullRequest: 12,
        token: Redacted.make("github-token"),
        apiUrl: "https://api.github.test",
      }).pipe(Effect.provideService(HttpClient.HttpClient, client));
      const comparison = yield* github.compareTrees(baseRevision, headRevision);

      const failure = yield* comparison.head.readTextFile(path).pipe(Effect.flip);

      expect(failure.reason).toContain(`returned blob ${"c".repeat(40)}`);
      expect(failure.reason).toContain(`requested blob ${headBlob}`);
    }),
  );
});

import { describe, expect, it } from "@effect/vitest";
import { Effect, Redacted, Ref } from "effect";
import { HttpClient, HttpClientResponse } from "effect/unstable/http";

import { makeGitHubClient } from "../src/github.ts";

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
) => ({ path, sha, mode, type });

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
                    entry("src/unchanged.ts", "a".repeat(40)),
                    entry("src/changed.ts", "b".repeat(40)),
                    entry("src/mode.ts", "c".repeat(40)),
                    entry("src/removed.ts", "d".repeat(40)),
                    entry("src/type.ts", "e".repeat(40)),
                    entry("src/not-in-pr.ts", "f".repeat(40)),
                  ],
                }
              : {
                  sha: headTree,
                  truncated: false,
                  tree: [
                    entry("src/unchanged.ts", "a".repeat(40)),
                    entry("src/changed.ts", "8".repeat(40)),
                    entry("src/mode.ts", "c".repeat(40), "100755"),
                    entry("src/added.ts", "9".repeat(40)),
                    entry("src/type.ts", "e".repeat(40), "160000", "commit"),
                    entry("src/not-in-pr.ts", "0".repeat(40)),
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

      const comparison = yield* github.compareTrees(baseRevision, headRevision, [
        "src/unchanged.ts",
        "src/changed.ts",
        "src/mode.ts",
        "src/added.ts",
        "src/removed.ts",
        "src/type.ts",
        "src/changed.ts",
      ]);

      expect(comparison.changedPaths).toEqual([
        "src/added.ts",
        "src/changed.ts",
        "src/mode.ts",
        "src/removed.ts",
        "src/type.ts",
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

      const failure = yield* github
        .compareTrees(baseRevision, headRevision, ["src/changed.ts"])
        .pipe(Effect.flip);

      expect(failure._tag).toBe("GitHubApiFailure");
      expect(failure.reason).toContain("truncated tree");
    }),
  );
});

import { execFileSync, spawnSync } from "node:child_process";
import { mkdtempSync, mkdirSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";

import {
  ReviewChange,
  ReviewOutcome,
  ReviewReport,
  ReviewRequest,
} from "@effect-agent/pr-review/review";
import { NodeServices } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import { Effect, Result, Schema, Stream } from "effect";

import { EvalCase, EvalCaseId, EvalSuite, EvalVariantConfiguration } from "../src/contracts.ts";
import { digestReviewRequest } from "../src/corpus.ts";
import { openLocalGitRepository } from "../src/local-git-repository.ts";
import { runEvalSuite } from "../src/runner.ts";

const makeGitFixture = () => {
  const root = mkdtempSync(join(tmpdir(), "pr-review-pinned-git-"));

  const git = (...args: ReadonlyArray<string>) =>
    execFileSync("git", [...args], { cwd: root, encoding: "utf8" }).trim();

  git("init", "-q", "-b", "main");
  git("config", "user.name", "Review Test");
  git("config", "user.email", "review@example.invalid");
  git("config", "commit.gpgsign", "false");
  mkdirSync(join(root, "src"));
  writeFileSync(join(root, "src/changed.ts"), "export const stage = 'before';\n");
  writeFileSync(join(root, "src/routeTree.gen.ts"), "needle ignored\n");
  writeFileSync(join(root, "src/image.png"), "needle image\n");
  writeFileSync(join(root, "src/binary.ts"), Buffer.from("needle\0binary"));
  symlinkSync("changed.ts", join(root, "src/linked.ts"));
  git("add", ".");
  git("commit", "-qm", "base");
  const baseRevision = git("rev-parse", "HEAD");

  writeFileSync(join(root, "src/changed.ts"), "export const stage = 'after';\n");
  writeFileSync(join(root, "src/caller.ts"), "export const caller = 'needle caller';\n");
  git("add", ".");
  git("commit", "-qm", "head");
  const headRevision = git("rev-parse", "HEAD");

  // Source must come from Git objects, never this mutable working file.
  writeFileSync(join(root, "src/changed.ts"), "export const stage = 'uncommitted';\n");

  const request = ReviewRequest.make({
    title: "Pinned source",
    description: "",
    baseRevision,
    headRevision,
    changes: [
      ReviewChange.make({
        path: "src/changed.ts",
        patch: "@@ -1 +1 @@\n-export const stage = 'before';\n+export const stage = 'after';",
      }),
    ],
    unreviewedPaths: [],
  });

  return { root, request };
};

const makePartialGitFixture = () => {
  const { root, request } = makeGitFixture();
  const partialParent = mkdtempSync(join(tmpdir(), "pr-review-partial-git-"));
  const partialRoot = join(partialParent, "clone");

  try {
    execFileSync("git", ["config", "uploadpack.allowFilter", "true"], { cwd: root });
    execFileSync(
      "git",
      ["clone", "-q", "--filter=blob:none", "--no-checkout", pathToFileURL(root).href, partialRoot],
      { stdio: "ignore" },
    );

    const blobSha = execFileSync("git", ["rev-parse", `${request.headRevision}:src/changed.ts`], {
      cwd: root,
      encoding: "utf8",
    }).trim();

    return { root, request, partialParent, partialRoot, blobSha };
  } catch (error) {
    rmSync(partialParent, { recursive: true, force: true });
    rmSync(root, { recursive: true, force: true });
    throw error;
  }
};

const makeCrowdedGitFixture = () => {
  const { root, request } = makeGitFixture();

  try {
    const decoys = Array.from({ length: 100 }, (_, index) => `a${String(index).padStart(3, "0")}`);

    for (const directory of decoys) {
      const path = join(root, directory, "src");

      mkdirSync(path, { recursive: true });
      writeFileSync(join(path, "changed.ts"), "export const decoy = true;\n");
    }

    execFileSync("git", ["add", "--", ...decoys], { cwd: root });
    execFileSync("git", ["commit", "-qm", "crowd source listing"], { cwd: root });

    const headRevision = execFileSync("git", ["rev-parse", "HEAD"], {
      cwd: root,
      encoding: "utf8",
    }).trim();

    return { root, request: ReviewRequest.make({ ...request, headRevision }) };
  } catch (error) {
    rmSync(root, { recursive: true, force: true });
    throw error;
  }
};

const makeLongLineGitFixture = () => {
  const { root, request } = makeGitFixture();

  try {
    writeFileSync(join(root, "src/long.ts"), `${"x".repeat(20_001)}long-line-needle\n`);
    execFileSync("git", ["add", "--", "src/long.ts"], { cwd: root });
    execFileSync("git", ["commit", "-qm", "add long source line"], { cwd: root });

    const headRevision = execFileSync("git", ["rev-parse", "HEAD"], {
      cwd: root,
      encoding: "utf8",
    }).trim();

    const patch = execFileSync(
      "git",
      ["diff", request.baseRevision, headRevision, "--", "src/long.ts"],
      { cwd: root, encoding: "utf8" },
    ).trim();

    return {
      root,
      request: ReviewRequest.make({
        ...request,
        headRevision,
        changes: [...request.changes, ReviewChange.make({ path: "src/long.ts", patch })],
      }),
    };
  } catch (error) {
    rmSync(root, { recursive: true, force: true });
    throw error;
  }
};

describe("pinned local Git review source", () => {
  it.effect("validates an exact changed path beyond the 100-result file listing", () =>
    Effect.acquireUseRelease(
      Effect.sync(makeCrowdedGitFixture),
      ({ root, request }) =>
        Effect.gen(function* () {
          const source = yield* openLocalGitRepository({ root, request, ignore: [] });

          const listed = yield* source.service.findFiles({
            query: "src/changed.ts",
            revision: "head",
          });

          expect(listed.truncated).toBe(true);
          expect(listed.paths).not.toContain("src/changed.ts");

          const exact = yield* source.service.readFile({
            path: "src/changed.ts",
            revision: "head",
            startLine: 1,
            lineCount: 1,
          });

          expect(exact.content).toBe("export const stage = 'after';");
        }),
      ({ root }) => Effect.sync(() => rmSync(root, { recursive: true, force: true })),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("accepts changed source searchable despite the read-file line bound", () =>
    Effect.acquireUseRelease(
      Effect.sync(makeLongLineGitFixture),
      ({ root, request }) =>
        Effect.gen(function* () {
          const source = yield* openLocalGitRepository({ root, request, ignore: [] });

          const longLine = yield* source.service
            .readFile({ path: "src/long.ts", revision: "head", startLine: 1, lineCount: 1 })
            .pipe(Effect.result);

          expect(Result.isFailure(longLine)).toBe(true);

          const searched = yield* source.service.searchCode({
            query: "long-line-needle",
            path: "src/long.ts",
            revision: "head",
            cursor: 0,
          });

          expect(searched.matches.map((match) => match.path)).toContain("src/long.ts");
        }),
      ({ root }) => Effect.sync(() => rmSync(root, { recursive: true, force: true })),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect(
    "keeps capacity-excluded paths readable while honoring explicit source exclusions",
    () =>
      Effect.acquireUseRelease(
        Effect.sync(makeGitFixture),
        ({ root, request }) =>
          Effect.gen(function* () {
            const capacityExcludedRequest = ReviewRequest.make({
              ...request,
              unreviewedPaths: ["src/caller.ts"],
            });

            const ordinary = yield* openLocalGitRepository({ root, request, ignore: [] });

            const capacityExcluded = yield* openLocalGitRepository({
              root,
              request: capacityExcludedRequest,
              ignore: [],
            });

            const visible = yield* capacityExcluded.service.findFiles({
              query: "src/caller.ts",
              revision: "head",
            });

            expect(visible.paths).toContain("src/caller.ts");
            expect(capacityExcluded.digest).toBe(ordinary.digest);

            const withUnavailablePath = {
              root,
              request: capacityExcludedRequest,
              ignore: [],
              unavailablePaths: new Set(["src/caller.ts"]),
            };

            const unavailable = yield* openLocalGitRepository(withUnavailablePath);

            const excluded = yield* unavailable.service.findFiles({
              query: "src/caller.ts",
              revision: "head",
            });

            expect(excluded.paths).not.toContain("src/caller.ts");
            expect(unavailable.digest).not.toBe(capacityExcluded.digest);
          }),
        ({ root }) => Effect.sync(() => rmSync(root, { recursive: true, force: true })),
      ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect("fails closed when a pinned blob is missing from a partial clone", () =>
    Effect.acquireUseRelease(
      Effect.sync(makePartialGitFixture),
      ({ partialRoot, request, blobSha }) =>
        Effect.gen(function* () {
          const localBlobExists = () =>
            spawnSync("git", ["cat-file", "-e", blobSha], {
              cwd: partialRoot,
              env: { ...process.env, GIT_NO_LAZY_FETCH: "1" },
              stdio: "ignore",
            }).status === 0;

          expect(localBlobExists()).toBe(false);

          const opened = yield* openLocalGitRepository({
            root: partialRoot,
            request,
            ignore: [],
          }).pipe(Effect.result);

          expect(Result.isFailure(opened)).toBe(true);
          expect(localBlobExists()).toBe(false);
        }),
      ({ root, partialParent }) =>
        Effect.sync(() => {
          rmSync(partialParent, { recursive: true, force: true });
          rmSync(root, { recursive: true, force: true });
        }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect(
    "exposes committed callers and keeps the Action's ignore, binary, and symlink scope",
    () =>
      Effect.acquireUseRelease(
        Effect.sync(makeGitFixture),
        ({ root, request }) =>
          Effect.gen(function* () {
            const source = yield* openLocalGitRepository({
              root,
              request,
              ignore: ["**/routeTree.gen.ts"],
            });

            const repository = source.service;
            const files = yield* repository.findFiles({ query: "src/", revision: "head" });

            expect(files.paths).toContain("src/caller.ts");
            expect(files.paths).not.toContain("src/routeTree.gen.ts");
            expect(files.paths).not.toContain("src/image.png");
            expect(files.paths).not.toContain("src/linked.ts");

            const base = yield* repository.readFile({
              path: "src/changed.ts",
              revision: "base",
              startLine: 1,
              lineCount: 1,
            });

            const head = yield* repository.readFile({
              path: "src/changed.ts",
              revision: "head",
              startLine: 1,
              lineCount: 1,
            });

            expect(base.content).toBe("export const stage = 'before';");
            expect(head.content).toBe("export const stage = 'after';");

            const search = yield* repository.searchCode({
              query: "needle",
              path: "src/",
              revision: "head",
              cursor: 0,
            });

            expect(search.matches.map((match) => match.path)).toEqual(["src/caller.ts"]);
            expect(search.unreadablePaths).toEqual(["src/binary.ts"]);

            const linked = yield* repository
              .readFile({ path: "src/linked.ts", revision: "head", startLine: 1, lineCount: 1 })
              .pipe(Effect.result);

            expect(Result.isFailure(linked)).toBe(true);

            const invalidRevision = yield* openLocalGitRepository({
              root,
              request: ReviewRequest.make({ ...request, headRevision: "main" }),
              ignore: [],
            }).pipe(Effect.result);

            expect(Result.isFailure(invalidRevision)).toBe(true);
            if (Result.isFailure(invalidRevision)) {
              expect(invalidRevision.failure._tag).toBe("EvalConfigurationError");
            }
          }),
        ({ root }) => Effect.sync(() => rmSync(root, { recursive: true, force: true })),
      ).pipe(Effect.provide(NodeServices.layer)),
  );

  it.effect(
    "stamps local source identity on observations without claiming frozen fixture source",
    () =>
      Effect.acquireUseRelease(
        Effect.sync(makeGitFixture),
        ({ root, request }) =>
          Effect.gen(function* () {
            const source = yield* openLocalGitRepository({ root, request, ignore: [] });
            const id = Schema.decodeSync(EvalCaseId)("pinned-source");

            const suite = EvalSuite.make({
              version: 1,
              cases: [
                EvalCase.make({
                  version: 1,
                  id,
                  kind: "unadjudicated",
                  provenance: "Local Git source identity test.",
                  inputDigest: yield* digestReviewRequest(request),
                  request,
                  expectedDefects: [],
                }),
              ],
            });

            const observations = yield* runEvalSuite(
              suite,
              [
                {
                  configuration: EvalVariantConfiguration.make({
                    id: "scripted",
                    reviewerProfile: "repository-review",
                    provider: "openai",
                    model: "scripted",
                    reasoningEffort: "medium",
                    serviceTier: "default",
                    compaction: "rollover",
                    contextTokenLimit: 48_000,
                    maxOutputTokens: 1_000,
                    strictJsonSchema: true,
                    store: false,
                    maxCostMicrousd: 2_500_000,
                    budgetPolicy: "input-size-v1",
                  }),
                  review: () =>
                    Effect.succeed(
                      ReviewOutcome.make({
                        report: ReviewReport.make({ summary: "Reviewed.", findings: [] }),
                        turns: 1,
                        usage: {
                          inputTokens: 1,
                          uncachedInputTokens: 1,
                          cachedInputTokens: 0,
                          cacheWriteInputTokens: 0,
                          outputTokens: 1,
                        },
                      }),
                    ),
                },
              ],
              {
                trials: 1,
                concurrency: 1,
                caseIds: [id],
                localGitRepositories: new Map([[id, source]]),
              },
            ).pipe(Stream.runCollect);

            expect(observations[0]?.repositorySource).toEqual({
              mode: "pinned-git",
              digest: source.digest,
            });
            expect(observations[0]?.repositoryDigest).toBeUndefined();
          }),
        ({ root }) => Effect.sync(() => rmSync(root, { recursive: true, force: true })),
      ).pipe(Effect.provide(NodeServices.layer)),
  );
});

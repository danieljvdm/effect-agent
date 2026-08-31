import { ReviewRepository, ReviewRequest } from "@effect-agent/pr-review";
import { Config, Console, Effect, FileSystem, Schema } from "effect";
import { FetchHttpClient } from "effect/unstable/http";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

import {
  hydrateExactChanges,
  makeReviewRepository,
} from "../../../packages/pr-review-action/src/action.ts";
import { makeGitHubClient } from "../../../packages/pr-review-action/src/github.ts";
import { EvalCase, EvalCaseId, EvalSuite } from "./contracts.ts";
import { digestReviewRequest, digestText, loadEvalSuite, writeObservations } from "./corpus.ts";
import { makeCurrentOpenAiVariant, openAiClientLayer } from "./openai-variant.ts";
import { runEvalSuite } from "./runner.ts";

const Metadata = Schema.Array(
  Schema.Struct({
    id: EvalCaseId,
    number: Schema.Int,
    base: Schema.String,
    head: Schema.String,
    title: Schema.String,
    description: Schema.String,
    sourceUrl: Schema.String,
  }),
);
const ignore = [
  "bun.lock",
  "action/dist/**",
  "work-order-action/dist/**",
  "**/CHANGELOG.md",
  ".agents/skills/**",
  ".claude/skills/**",
];
const root = "examples/pr-review-eval";
const directory = `${root}/results/efficient-261/candidate-2`;

// Temporary fixed acceptance runner. Historical revisions are only GitHub blob data.
// The final integration PR removes this one-shot execution machinery after archiving its outputs.
export const acceptance259 = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const reviewerCommit = (yield* spawner.string(
    ChildProcess.make("git", ["rev-parse", "HEAD"]),
  )).trim();
  const live = (yield* Config.string("EFFECT_AGENT_LIVE").pipe(Config.withDefault(""))) === "1";
  const hosted = (yield* Config.string("CI").pipe(Config.withDefault(""))) === "true";
  if (hosted && !live) return yield* Effect.die("Hosted acceptance requires EFFECT_AGENT_LIVE=1");
  const metadata = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Metadata))(
    yield* fs.readFileString(`${root}/fixtures/acceptance-259-metadata.json`),
  );
  const guidance = yield* fs.readFileString(".github/review-guidance.md");
  if (
    (yield* digestText(guidance)) !==
    "7ede36771127dcfeec948df15a01b5acf1dfb4f73bed55c8234f470908f1f1c0"
  ) {
    return yield* Effect.die("Historical guidance changed");
  }
  yield* fs.makeDirectory(directory, { recursive: true });
  const prepared = yield* Effect.forEach(
    metadata,
    Effect.fn(function* (item) {
      const github = yield* makeGitHubClient({
        repository: "danieljvdm/effect-agent",
        pullRequest: item.number,
        token: yield* Config.redacted("GITHUB_TOKEN"),
      });
      const comparison = yield* github.compareTrees(item.base, item.head);
      // None of these three immutable diffs has a renamed path (verified offline).
      const surface = yield* hydrateExactChanges({ ...comparison, files: [], ignore });
      const request = ReviewRequest.make({
        title: item.title,
        description: item.description,
        baseRevision: item.base,
        headRevision: item.head,
        scope: "full",
        changes: surface.changes,
        unreviewedPaths: surface.unreviewedPaths,
      });
      const evalCase = EvalCase.make({
        version: 1,
        id: item.id,
        kind: "unadjudicated",
        sourceUrl: item.sourceUrl,
        provenance:
          "Historical title/body recovered from GitHub edit history before inference. Exact full diff and context tools use the shipping immutable GitHub source adapter. No renamed paths. No independently source-verified defect oracle established; completion does not establish defect-detection quality or clearance.",
        inputDigest: yield* digestReviewRequest(request),
        request,
        expectedDefects: [],
      });
      yield* fs.writeFileString(
        `${directory}/${item.id}-source.json`,
        JSON.stringify(
          {
            reviewerCommit,
            reviewerSourceCommit: reviewerCommit,
            inputDigest: evalCase.inputDigest,
            base: item.base,
            head: item.head,
            admittedPaths: surface.changes.map((change) => change.path),
            ignoredPaths: surface.ignoredPaths,
            unreviewedPaths: surface.unreviewedPaths,
            unavailablePaths: [...surface.unavailablePaths],
            baseEntries: comparison.base.paths.map((path) => ({
              path,
              ...comparison.base.entry(path),
            })),
            headEntries: comparison.head.paths.map((path) => ({
              path,
              ...comparison.head.entry(path),
            })),
          },
          null,
          2,
        ),
      );
      yield* Console.log(
        `${item.id}: ${evalCase.inputDigest}; ${surface.changes.length} supplied, ${surface.unreviewedPaths.length} unavailable, ${surface.ignoredPaths.length} ignored`,
      );
      return {
        evalCase,
        repository: makeReviewRepository({
          ...comparison,
          ignore,
          unavailablePaths: surface.unavailablePaths,
        }),
      };
    }),
    { concurrency: 1 },
  );
  const historical = EvalSuite.make({ version: 1, cases: prepared.map((item) => item.evalCase) });
  const frozen = yield* loadEvalSuite(`${root}/fixtures/acceptance-259.json`);
  if (JSON.stringify(frozen) !== JSON.stringify(historical))
    return yield* Effect.die("Acceptance inputs differ from the frozen baseline");
  // Coordinator-adjudicated before candidate outputs: detect the newly exposed
  // unanchored publication overflow; do not report it on the comment-only control.
  const quality = yield* loadEvalSuite(`${root}/fixtures/reviewer-quality.json`);
  const qualityCases = quality.cases.filter(
    (item) =>
      item.id === "incremental-publication-exposure" || item.id === "incremental-unrelated-control",
  );
  if (qualityCases.length !== 2) return yield* Effect.die("Missing fixed quality cases");
  const suite = EvalSuite.make({ version: 1, cases: [...historical.cases, ...qualityCases] });
  yield* fs.writeFileString(
    `${directory}/cases.json`,
    yield* Schema.encodeEffect(Schema.fromJsonString(EvalSuite))(suite),
  );
  if (!live) return;
  const variant = yield* makeCurrentOpenAiVariant({
    id: `efficient-261-${reviewerCommit.slice(0, 12)}`,
    guidance,
  });
  const boundVariant = {
    ...variant,
    review: (request: ReviewRequest) => {
      const item = prepared.find(
        (candidate) => candidate.evalCase.request.headRevision === request.headRevision,
      );
      if (item === undefined) return variant.review(request);
      return variant.review(request).pipe(Effect.provideService(ReviewRepository, item.repository));
    },
  };
  yield* writeObservations(
    `${directory}/observations.jsonl`,
    runEvalSuite(suite, [boundVariant], {
      trials: 1,
      concurrency: 1,
      caseIds: [],
    }),
  ).pipe(Effect.provide(openAiClientLayer));
}).pipe(Effect.provide(FetchHttpClient.layer));

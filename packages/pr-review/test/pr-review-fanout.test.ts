import { describe, expect, it } from "@effect/vitest";
import { Duration, Effect, Layer, Ref, Schema, Stream } from "effect";
import {
  Agent,
  AgentSpawner,
  IdGenerator,
  RunEventSink,
  SubagentBudgetExhausted,
  SubagentDurability,
  SubagentDurabilityError,
  SubagentExecutionFailure,
  SubagentReservations,
  SubagentReservationsMemoryLive,
  ToolCallWaiting,
} from "effect-agent";
import { type AiError, LanguageModel, Model, Tool, Toolkit } from "effect/unstable/ai";
import { toCodecOpenAI } from "effect/unstable/ai/OpenAiStructuredOutput";

import {
  ChangedFile,
  CodeReview,
  DelegateFileReview,
  executeReview,
  FanOutCoordinatorToolkitLayer,
  fanOutHandlersLayer,
  FanOutReviewer,
  fanOutReviewBudgetLimits,
  fanOutReviewerProfile,
  FanOutReviewerProfile,
  FileReviewer,
  FileReviewReport,
  FileReviewToolkit,
  FileReviewToolkitLayer,
  FileReviewUnitFailed,
  FileReviewBrief,
  ListReviewUnits,
  makeFanOutReviewInstructions,
  makeFanOutReviewSuite,
  MAX_FILE_REVIEW_TOOL_CALLS,
  MAX_REVIEW_UNITS,
  MAX_UNIT_FILES,
  planReviewUnits,
  PullRequestMetadata,
  PullRequestSource,
  rankAndDedupeFindings,
  ReviewConcern,
  ReviewFinding,
  ReviewMission,
  ReviewPublicationPlan,
  defaultFileReviewerPolicy,
  fileReviewPolicy,
} from "../src/index.ts";
import {
  collectingReviewPublisherLayer,
  FixtureFile,
  FixturePullRequest,
  fixturePullRequestSourceLayer,
  makeOfflineFanOutCoordinatorModel,
  makeOfflineFileReviewerModel,
  type OfflineUnitCall,
  type OfflineUnitScript,
} from "../src/testing.ts";

type Equal<Left, Right> =
  (<Value>() => Value extends Left ? 1 : 2) extends <Value>() => Value extends Right ? 1 : 2
    ? (<Value>() => Value extends Right ? 1 : 2) extends <Value>() => Value extends Left ? 1 : 2
      ? true
      : false
    : false;
type Assert<Value extends true> = Value;

const requiresNothing = <A, E>(effect: Effect.Effect<A, E, never>): Effect.Effect<A, E, never> =>
  effect;

// ---------------------------------------------------------------------------
// Deterministic fixture pull request: four diffable TypeScript files across
// two directories, sized so the pure planner yields exactly two review units,
// plus one binary file no unit can cover. Declared line counts drive the
// planner; the small real patches drive anchor validation.
// ---------------------------------------------------------------------------

const FIXTURE_SHA = "fedcba9876543210fedcba9876543210fedcba98";

/** A parent-only secret: must never appear in any child prompt (SUB-006/015). */
const MISSION_MARKER = "coordinator-dossier-91x";

const patchFor = (marker: string): string =>
  [
    "@@ -1,3 +1,4 @@",
    ` const ${marker}Base = 1;`,
    `-const ${marker}Old = 3;`,
    `+const ${marker}New = 2;`,
    `+const ${marker}Extra = 3;`,
    ` export const ${marker}Sum = ${marker}Base + ${marker}New;`,
  ].join("\n");

const headFor = (marker: string): string =>
  [
    `const ${marker}Base = 1;`,
    `const ${marker}New = 2;`,
    `const ${marker}Extra = 3;`,
    `export const ${marker}Sum = ${marker}Base + ${marker}New;`,
  ].join("\n");

const fixtureSource = (marker: string, path: string, additions: number, deletions: number) =>
  FixtureFile.make({
    file: ChangedFile.make({
      path,
      status: "modified",
      additions,
      deletions,
      patch: patchFor(marker),
    }),
    headContent: headFor(marker),
  });

const fixture = FixturePullRequest.make({
  metadata: PullRequestMetadata.make({
    repository: "acme/widgets",
    number: 202,
    title: "Refactor the api and core constants",
    body: `Large refactor; ${MISSION_MARKER} stays with the coordinator.`,
    baseRef: "main",
    headRef: "refactor/constants",
    headSha: FIXTURE_SHA,
    totalChangedFiles: 5,
  }),
  files: [
    fixtureSource("alpha", "src/api/alpha.ts", 350, 20),
    fixtureSource("beta", "src/api/beta.ts", 330, 20),
    fixtureSource("delta", "src/core/delta.ts", 90, 10),
    fixtureSource("gamma", "src/core/gamma.ts", 190, 10),
    FixtureFile.make({
      file: ChangedFile.make({
        path: "assets/logo.png",
        status: "added",
        additions: 0,
        deletions: 0,
      }),
    }),
  ],
});

const fixtureFiles = fixture.files.map((entry) => entry.file);

const UNIT_ONE = { unitId: "unit-001", paths: ["src/api/alpha.ts", "src/api/beta.ts"] } as const;
const UNIT_TWO = { unitId: "unit-002", paths: ["src/core/delta.ts", "src/core/gamma.ts"] } as const;

// The scripted children's findings: one valid anchor with a suggestion, one
// ghost anchor (demoted by the host), one valid nit from the second unit.
const alphaFinding = ReviewFinding.make({
  path: "src/api/alpha.ts",
  startLine: 2,
  endLine: 2,
  severity: "important",
  title: "Magic number replaces the named constant",
  body: "The literal duplicates the meaning of the extra constant.",
  suggestion: "const alphaNew = TWO;",
});
const betaGhostFinding = ReviewFinding.make({
  path: "src/api/beta.ts",
  startLine: 99,
  endLine: 99,
  severity: "nit",
  title: "Ghost anchor",
  body: "This line does not exist in the diff.",
});
const gammaFinding = ReviewFinding.make({
  path: "src/core/gamma.ts",
  startLine: 3,
  endLine: 3,
  severity: "nit",
  title: "Name the gamma literal",
  body: "The literal deserves a named constant.",
});

const unitOneReport = FileReviewReport.make({
  unitId: "unit-001",
  findings: [alphaFinding, betaGhostFinding],
});
const unitTwoReport = FileReviewReport.make({
  unitId: "unit-002",
  findings: [gammaFinding],
});

// ---------------------------------------------------------------------------
// Shared offline wiring: the exact host fan-out composition, minus GitHub and
// live models.
// ---------------------------------------------------------------------------

const runOfflineFanOut = (script: {
  readonly children: ReadonlyArray<OfflineUnitScript>;
  readonly review: CodeReview;
  readonly unitCalls?: ReadonlyArray<OfflineUnitCall> | undefined;
  readonly sourceFixture?: FixturePullRequest | undefined;
}) =>
  Effect.gen(function* () {
    const coordinator = yield* makeOfflineFanOutCoordinatorModel({
      unitCalls: script.unitCalls ?? [UNIT_ONE, UNIT_TWO],
      review: script.review,
    });
    const children = yield* makeOfflineFileReviewerModel(script.children);
    const parentBinding = Agent.withModel(FanOutReviewer, coordinator.model);
    const childBinding = Agent.withModel(FileReviewer, children.model);
    const published = yield* Ref.make<ReadonlyArray<ReviewPublicationPlan>>([]);
    const sourceLayer = fixturePullRequestSourceLayer(script.sourceFixture ?? fixture);
    const childSupportLayer = Layer.mergeAll(
      FileReviewToolkitLayer,
      SubagentReservationsMemoryLive,
      IdGenerator.layer,
    ).pipe(Layer.provideMerge(sourceLayer));
    const program = executeReview(parentBinding, {
      post: true,
      applyVerdict: false,
      limits: fanOutReviewBudgetLimits,
      reviewShape: "fan-out",
    }).pipe(
      Effect.provide(
        Layer.mergeAll(
          FanOutCoordinatorToolkitLayer.pipe(Layer.provideMerge(sourceLayer)),
          fanOutHandlersLayer(childBinding).pipe(Layer.provide(childSupportLayer)),
          collectingReviewPublisherLayer(published),
          IdGenerator.layer,
        ),
      ),
      Effect.scoped,
    );
    const outcome = yield* requiresNothing(program);
    return {
      outcome,
      published: yield* Ref.get(published),
      coordinatorCalls: yield* coordinator.calls,
      coordinatorPrompts: yield* coordinator.prompts,
      childCalls: yield* children.calls,
      childPrompts: yield* children.prompts,
    };
  });

// ---------------------------------------------------------------------------
// Coordinator instructions: the shared guidance and the configured findings
// bound reach the coordinator, not only the children (the dogfooded gap).
// ---------------------------------------------------------------------------

describe("coordinator instructions", () => {
  const mission = ReviewMission.make({
    repository: "acme/widgets",
    number: 202,
    title: "Refactor",
    body: "",
    baseRef: "main",
    headRef: "refactor/constants",
    changedFileCount: 5,
  });

  it("carry the shared guidance and the configured findings bound", () => {
    const instructions = makeFanOutReviewInstructions({
      guidance: ["Review architecture first, correctness second."],
      maxFindings: 7,
    })(mission);
    expect(instructions).toContain("Review architecture first, correctness second.");
    expect(instructions).toContain("keep at most 7 findings");
    expect(instructions).toContain('"concerns"');
  });

  it("reach both the coordinator and the children through the suite", () => {
    const suite = makeFanOutReviewSuite({ guidance: "Architecture first.", maxFindings: 7 });
    expect(suite.parent.instructions(mission)).toContain("Architecture first.");
    expect(suite.parent.instructions(mission)).toContain("keep at most 7 findings");
    const brief = FileReviewBrief.make({
      unitId: "unit-001",
      paths: ["src/api/alpha.ts"],
      focus: "defects-first",
    });
    expect(suite.child.instructions(brief)).toContain("Architecture first.");
  });
});

describe("file-reviewer policy", () => {
  it("budgets one diff and one context read for every path in a maximum-size unit", () => {
    expect(MAX_FILE_REVIEW_TOOL_CALLS).toBe(MAX_UNIT_FILES * 2);
    expect(defaultFileReviewerPolicy.maxToolCalls).toBe(MAX_UNIT_FILES * 2);
    expect(fileReviewPolicy.maxToolCalls).toBe(MAX_UNIT_FILES * 2);
  });

  it("keeps the child and delegation deadlines aligned with reasoning-model headroom", () => {
    expect(Duration.toMillis(defaultFileReviewerPolicy.maxDuration)).toBe(6 * 60_000);
    expect(Duration.toMillis(fileReviewPolicy.maxDuration)).toBe(6 * 60_000);
  });
});

// ---------------------------------------------------------------------------
// Profile claim.
// ---------------------------------------------------------------------------

describe("fan-out profile", () => {
  it("pins the profile claim and its schema round-trip", () => {
    const decoded = Schema.decodeUnknownSync(FanOutReviewerProfile)(
      Schema.encodeSync(FanOutReviewerProfile)(fanOutReviewerProfile),
    );
    expect(decoded).toEqual(fanOutReviewerProfile);
    expect(fanOutReviewerProfile).toEqual({
      deploymentClass: "E",
      readOnlyToolSurface: true,
      publicationOutsideAgentLoop: true,
      anchorsValidatedBeforePublication: true,
      attachedEphemeralDelegation: true,
      failedUnitsReportedNotRetried: true,
      liveProfileOptIn: true,
      exactlyOnceExternalEffects: false,
    });
  });
});

// ---------------------------------------------------------------------------
// OpenAI strict schema compatibility for the fan-out tool surface.
// ---------------------------------------------------------------------------

describe("fan-out OpenAI tool schema compatibility", () => {
  it("encodes the coordinator tools as strict OpenAI object schemas", () => {
    for (const tool of [ListReviewUnits, DelegateFileReview]) {
      const jsonSchema = Tool.getJsonSchema(tool, { transformer: toCodecOpenAI });
      expect(jsonSchema.type).toBe("object");
      expect(jsonSchema.anyOf).toBeUndefined();
    }
  });
});

// ---------------------------------------------------------------------------
// Deterministic unit planning.
// ---------------------------------------------------------------------------

describe("planReviewUnits", () => {
  it("packs directory-affine, size-budgeted units and reports undiffable files", () => {
    const plan = planReviewUnits(fixtureFiles, { totalChangedFiles: 5 });
    expect(plan.totalFiles).toBe(5);
    expect(plan.truncated).toBe(false);
    expect(plan.units.map((unit) => ({ unitId: unit.unitId, paths: [...unit.paths] }))).toEqual([
      { unitId: "unit-001", paths: [...UNIT_ONE.paths] },
      { unitId: "unit-002", paths: [...UNIT_TWO.paths] },
    ]);
    expect(plan.units[0]?.changedLines).toBe(720);
    expect(plan.units[1]?.changedLines).toBe(300);
    expect([...plan.undiffablePaths]).toEqual(["assets/logo.png"]);
    expect(plan.unassignedPaths).toHaveLength(0);
  });

  it("assigns patchless files recovered as bounded text and keeps binaries unreviewed", () => {
    const snapshot = ChangedFile.make({
      path: "db/snapshot.json",
      status: "added",
      additions: 1,
      deletions: 0,
      reviewHeadContent: '{"version":"7","tables":{}}',
    });
    const binary = ChangedFile.make({
      path: "assets/logo.png",
      status: "added",
      additions: 0,
      deletions: 0,
    });
    const plan = planReviewUnits([snapshot, binary], { totalChangedFiles: 2 });

    expect(plan.units.flatMap((unit) => [...unit.paths])).toEqual([snapshot.path]);
    expect([...plan.undiffablePaths]).toEqual([binary.path]);
  });

  it("keeps fallback evidence beyond the complete render bound unreviewed", () => {
    const lineHeavyAddition = ChangedFile.make({
      path: "db/line-heavy.json",
      status: "added",
      additions: 100_000,
      deletions: 0,
      reviewHeadContent: "x\n".repeat(100_000),
    });
    const largeModification = ChangedFile.make({
      path: "db/large-modified.json",
      status: "modified",
      additions: 1,
      deletions: 1,
      reviewBaseContent: "b".repeat(120_000),
      reviewHeadContent: "h".repeat(120_000),
    });
    const plan = planReviewUnits([lineHeavyAddition, largeModification], {
      totalChangedFiles: 2,
    });

    expect(plan.units).toEqual([]);
    expect([...plan.undiffablePaths]).toEqual(["db/large-modified.json", "db/line-heavy.json"]);
  });

  it("is deterministic regardless of input order", () => {
    const shuffled = [...fixtureFiles].reverse();
    expect(planReviewUnits(shuffled, { totalChangedFiles: 5 })).toEqual(
      planReviewUnits(fixtureFiles, { totalChangedFiles: 5 }),
    );
  });

  it("bounds the fan-out and reports overflow files instead of dropping them", () => {
    const many = Array.from({ length: 120 }, (_, index) =>
      ChangedFile.make({
        path: `src/wide/file-${String(index + 1).padStart(3, "0")}.ts`,
        status: "modified",
        additions: 1,
        deletions: 0,
        patch: patchFor("wide"),
      }),
    );
    const plan = planReviewUnits(many, { totalChangedFiles: 310 });
    expect(plan.truncated).toBe(true);
    expect(plan.units).toHaveLength(MAX_REVIEW_UNITS);
    for (const unit of plan.units) {
      expect(unit.paths.length).toBeLessThanOrEqual(MAX_UNIT_FILES);
    }
    const assigned = plan.units.flatMap((unit) => [...unit.paths]);
    expect(assigned).toHaveLength(MAX_REVIEW_UNITS * MAX_UNIT_FILES);
    // Every input file is either assigned or reported unassigned — no file
    // silently disappears.
    expect([...assigned, ...plan.unassignedPaths].sort()).toEqual(
      many.map((file) => file.path).sort(),
    );
  });
});

// ---------------------------------------------------------------------------
// Deterministic merge policy.
// ---------------------------------------------------------------------------

describe("rankAndDedupeFindings", () => {
  it("dedupes shared anchors keeping the most severe and ranks the rest", () => {
    const duplicateNit = ReviewFinding.make({ ...alphaFinding, severity: "nit", title: "Dup" });
    const blocking = ReviewFinding.make({ ...gammaFinding, severity: "blocking" });
    const merged = rankAndDedupeFindings([duplicateNit, gammaFinding, blocking, alphaFinding]);
    // The alpha anchor collapsed to its most severe finding; the gamma anchor
    // collapsed to the blocking duplicate, which then ranks first.
    expect(merged.map((finding) => [finding.path, finding.severity])).toEqual([
      ["src/core/gamma.ts", "blocking"],
      ["src/api/alpha.ts", "important"],
    ]);
    expect(merged[1]?.title).toBe(alphaFinding.title);
  });

  it("caps the merged list at the CodeReview findings bound", () => {
    const many = Array.from({ length: 25 }, (_, index) =>
      ReviewFinding.make({
        path: "src/api/alpha.ts",
        startLine: index + 1,
        endLine: index + 1,
        severity: index % 2 === 0 ? "nit" : "important",
        title: `Finding ${index + 1}`,
        body: "Bounded merge test.",
      }),
    );
    const merged = rankAndDedupeFindings(many);
    expect(merged).toHaveLength(20);
    // All 12 "important" findings out-rank the nits; the cap trims nits only.
    expect(merged.filter((finding) => finding.severity === "important")).toHaveLength(12);
  });
});

// ---------------------------------------------------------------------------
// Offline end-to-end fan-out: scripted coordinator + scripted children over
// the real S1 delegation, toolkits, source port, and publication path.
// ---------------------------------------------------------------------------

describe("offline fan-out review run", () => {
  const happyChildren: ReadonlyArray<OfflineUnitScript> = [
    {
      unitId: "unit-001",
      diffPath: "src/api/alpha.ts",
      outcome: { _tag: "findings", report: unitOneReport },
    },
    {
      unitId: "unit-002",
      diffPath: "src/core/gamma.ts",
      outcome: { _tag: "findings", report: unitTwoReport },
    },
  ];

  const mergedFindings = rankAndDedupeFindings([
    ...unitOneReport.findings,
    ...unitTwoReport.findings,
  ]);

  const happyReview = CodeReview.make({
    summary:
      "Reviewed 2 units across 4 files by delegation. assets/logo.png has no textual diff and was not reviewed.",
    verdict: "comment",
    findings: mergedFindings,
  });

  it.effect(
    "fans out to two children, merges their findings, and publishes the validated plan",
    () =>
      Effect.gen(function* () {
        const result = yield* runOfflineFanOut({ children: happyChildren, review: happyReview });

        // list -> delegate batch -> final: three coordinator turns.
        expect(result.outcome.turns).toBe(3);
        expect(result.coordinatorCalls).toBe(3);
        // Two children, two turns each: real fan-out, not sequential re-use.
        expect(result.childCalls).toBe(4);

        // The merged review round-tripped through the run unchanged.
        expect(result.outcome.review).toEqual(happyReview);

        // Anchor validation is unchanged host-side: the ghost anchor demoted,
        // the two real anchors published inline.
        expect(result.outcome.plan.comments.map((comment) => comment.path)).toEqual([
          "src/api/alpha.ts",
          "src/core/gamma.ts",
        ]);
        expect(result.outcome.plan.demoted).toEqual([betaGhostFinding]);
        expect(result.outcome.plan.body).toContain("`src/api/beta.ts:99`");
        expect(result.outcome.plan.commitSha).toBe(FIXTURE_SHA);

        // Publication went through the collecting publisher exactly once.
        expect(result.published).toHaveLength(1);
        expect(result.outcome.published?.inlineComments).toBe(2);
        expect(result.outcome.coverage.status).toBe("incomplete");
        expect(result.outcome.coverage.unreviewedPaths).toContain("assets/logo.png");

        // Context isolation: each child saw exactly its briefed unit — never
        // the other unit's paths and never the parent-only mission marker.
        const unitOnePrompt = result.childPrompts.find((prompt) => prompt.includes("unit-001"));
        const unitTwoPrompt = result.childPrompts.find((prompt) => prompt.includes("unit-002"));
        expect(unitOnePrompt).toBeDefined();
        expect(unitTwoPrompt).toBeDefined();
        expect(unitOnePrompt).toContain("src/api/alpha.ts");
        expect(unitOnePrompt).not.toContain("src/core/gamma.ts");
        expect(unitTwoPrompt).toContain("src/core/delta.ts");
        expect(unitTwoPrompt).not.toContain("src/api/alpha.ts");
        for (const prompt of result.childPrompts) {
          expect(prompt).not.toContain(MISSION_MARKER);
        }

        // Declassification: the diffs the children read never reach the
        // coordinator — only the projected findings do.
        for (const prompt of result.coordinatorPrompts) {
          expect(prompt).not.toContain("alphaBase");
          expect(prompt).not.toContain("gammaBase");
        }
        const finalPrompt = result.coordinatorPrompts[2] ?? "";
        expect(finalPrompt).toContain(alphaFinding.title);
        expect(finalPrompt).toContain(gammaFinding.title);
      }),
  );

  it.effect("reviews a patchless generated text file through bounded head content", () =>
    Effect.gen(function* () {
      const snapshotPath = "packages/db/migrations/next/snapshot.json";
      const snapshotContent = `{"version":"7","payload":"${"x".repeat(142_900)}","tail":"snapshot-tail"}`;
      const snapshotFixture = FixturePullRequest.make({
        metadata: PullRequestMetadata.make({
          repository: "acme/widgets",
          number: 203,
          title: "Add generated migration snapshot",
          body: "",
          baseRef: "main",
          headRef: "feature/migration",
          headSha: FIXTURE_SHA,
          totalChangedFiles: 1,
        }),
        files: [
          FixtureFile.make({
            file: ChangedFile.make({
              path: snapshotPath,
              status: "added",
              additions: 1,
              deletions: 0,
            }),
            headContent: snapshotContent,
          }),
        ],
      });
      const cleanReport = FileReviewReport.make({ unitId: "unit-001", findings: [] });
      const cleanReview = CodeReview.make({
        summary: "Reviewed the generated snapshot through bounded head content.",
        verdict: "approve",
        findings: [],
      });

      const result = yield* runOfflineFanOut({
        sourceFixture: snapshotFixture,
        unitCalls: [{ unitId: "unit-001", paths: [snapshotPath] }],
        children: [
          {
            unitId: "unit-001",
            diffPath: snapshotPath,
            outcome: { _tag: "findings", report: cleanReport },
          },
        ],
        review: cleanReview,
      });

      expect(result.outcome.coverage.status).toBe("complete");
      expect(result.outcome.coverage.unreviewedPaths).toEqual([]);
      expect(result.childPrompts.join("\n")).toContain("snapshot-tail");
      expect(result.childPrompts.join("\n")).toContain("not valid inline-comment anchors");
      expect(result.childPrompts.join("\n")).toContain('"truncated":false');
      expect(result.outcome.plan.body).not.toContain("Incomplete coverage");
    }),
  );

  it.effect("projects child concerns to the coordinator and renders them as body sections", () =>
    Effect.gen(function* () {
      const gammaConcern = ReviewConcern.make({
        severity: "important",
        title: "Replaced gamma path is never deleted",
        body: "The old constant path stays reachable after this refactor.",
      });
      const concernedChildren: ReadonlyArray<OfflineUnitScript> = [
        happyChildren[0] as OfflineUnitScript,
        {
          unitId: "unit-002",
          diffPath: "src/core/gamma.ts",
          outcome: {
            _tag: "findings",
            report: FileReviewReport.make({
              unitId: "unit-002",
              findings: [gammaFinding],
              concerns: [gammaConcern],
            }),
          },
        },
      ];
      const concernedReview = CodeReview.make({
        summary: "Merged 2 units; one non-anchored concern carried through.",
        verdict: "comment",
        findings: mergedFindings,
        concerns: [gammaConcern],
      });

      const result = yield* runOfflineFanOut({
        children: concernedChildren,
        review: concernedReview,
      });

      // The projection carried the concern across the declassification
      // boundary: the coordinator's merge turn saw it.
      const finalPrompt = result.coordinatorPrompts[2] ?? "";
      expect(finalPrompt).toContain(gammaConcern.title);

      // The published body renders it as a severity-tagged section.
      expect(result.outcome.plan.body).toContain(`### ⚠️ ${gammaConcern.title}`);
      expect(result.outcome.plan.body).toContain(gammaConcern.body);
    }),
  );

  it.effect("reports a failed unit honestly in the summary instead of failing the run", () =>
    Effect.gen(function* () {
      const failingChildren: ReadonlyArray<OfflineUnitScript> = [
        happyChildren[0] as OfflineUnitScript,
        {
          unitId: "unit-002",
          diffPath: "src/core/gamma.ts",
          outcome: { _tag: "malformed-output" },
        },
      ];
      const honestReview = CodeReview.make({
        summary:
          "Reviewed unit-001 (src/api). unit-002 unreviewed: AgentOutputError — its findings are missing from this review. assets/logo.png has no textual diff.",
        verdict: "comment",
        findings: rankAndDedupeFindings([...unitOneReport.findings]),
      });

      const result = yield* runOfflineFanOut({ children: failingChildren, review: honestReview });

      // The run COMPLETED: the typed unit failure surfaced to the model as a
      // failed tool result instead of aborting the fan-out (containment via
      // the parent-facing return-mode Tool view).
      expect(result.outcome.turns).toBe(3);
      expect(result.outcome.review.summary).toContain("unit-002 unreviewed: AgentOutputError");

      // The typed failure — tag and child error tag — was model-visible.
      const finalPrompt = result.coordinatorPrompts[2] ?? "";
      expect(finalPrompt).toContain("FileReviewUnitFailed");
      expect(finalPrompt).toContain("AgentOutputError");

      // No retry: the coordinator declared exactly one delegation per unit
      // and the failed child ran its two scripted turns exactly once.
      expect(result.coordinatorCalls).toBe(3);
      expect(result.childCalls).toBe(4);

      // The published review carries the honest summary and the surviving
      // unit's validated findings.
      expect(result.published).toHaveLength(1);
      expect(result.outcome.plan.body).toContain("unit-002 unreviewed");
      expect(result.outcome.plan.comments.map((comment) => comment.path)).toEqual([
        "src/api/alpha.ts",
      ]);
      expect(result.outcome.coverage.status).toBe("incomplete");
      // The contained failure carries the child error tag through the
      // coverage classification (first-party return mode, SUB-033).
      expect(result.outcome.coverage.failedUnits).toContainEqual({
        unitId: "unit-002",
        errorTag: "FileReviewUnitFailed:AgentOutputError",
      });
    }),
  );

  it.effect("reports a whole batch of failed units before the repeated-failure stop", () =>
    Effect.gen(function* () {
      const failedUnits: ReadonlyArray<OfflineUnitScript> = [
        {
          unitId: "unit-001",
          diffPath: "src/api/alpha.ts",
          outcome: { _tag: "malformed-output" },
        },
        {
          unitId: "unit-002",
          diffPath: "src/core/gamma.ts",
          outcome: { _tag: "malformed-output" },
        },
        {
          unitId: "unit-003",
          diffPath: "src/core/delta.ts",
          outcome: { _tag: "malformed-output" },
        },
      ];
      const honestReview = CodeReview.make({
        summary:
          "unit-001, unit-002, and unit-003 were unreviewed after AgentOutputError failures.",
        verdict: "comment",
        findings: [],
      });

      const result = yield* runOfflineFanOut({
        children: failedUnits,
        review: honestReview,
        unitCalls: failedUnits.map((unit) => ({
          unitId: unit.unitId,
          paths: [unit.diffPath],
        })),
      });

      expect(result.outcome.turns).toBe(3);
      expect(result.coordinatorCalls).toBe(3);
      expect(result.childCalls).toBe(6);
      expect(result.outcome.review).toEqual(honestReview);
      expect(result.published).toHaveLength(1);
      const finalPrompt = result.coordinatorPrompts[2] ?? "";
      expect(finalPrompt.match(/FileReviewUnitFailed/g)).toHaveLength(3);
    }),
  );

  it.effect(
    "SUB-033 a runaway child fails typed and its unit is reported honestly, never retried",
    () =>
      Effect.gen(function* () {
        const runawayChildren: ReadonlyArray<OfflineUnitScript> = [
          happyChildren[0] as OfflineUnitScript,
          {
            unitId: "unit-002",
            diffPath: "src/core/gamma.ts",
            // One more declared call than the child AgentPolicy allows: the
            // reviewer pins typed exhaustion (a review is a coverage claim —
            // schema-valid findings from a child whose reads never executed
            // would launder budget exhaustion into coverage), so the child
            // fails typed and first-party containment turns that into result
            // data instead of failing the run.
            outcome: {
              _tag: "budget-runaway",
              declaredCalls: MAX_FILE_REVIEW_TOOL_CALLS + 1,
            },
          },
        ];
        const honestReview = CodeReview.make({
          summary: "unit-002 unreviewed: AgentPolicyError (exceeded its Tool Call budget).",
          verdict: "comment",
          findings: rankAndDedupeFindings([...unitOneReport.findings]),
        });

        const result = yield* runOfflineFanOut({
          children: runawayChildren,
          review: honestReview,
        });

        // The child failed typed on its Tool Call bound BEFORE any runaway
        // call executed: one child model call, no second chance, run intact.
        expect(result.childCalls).toBe(3);
        expect(result.coordinatorCalls).toBe(3);

        // The typed policy failure crossed the delegation boundary bounded
        // and model-visible; the unit stays honestly unreviewed.
        const finalPrompt = result.coordinatorPrompts[2] ?? "";
        expect(finalPrompt).toContain("FileReviewUnitFailed");
        expect(finalPrompt).toContain("AgentPolicyError");
        expect(result.outcome.review.summary).toContain("unit-002 unreviewed: AgentPolicyError");
        expect(result.published).toHaveLength(1);
        expect(result.outcome.coverage.status).toBe("incomplete");
        expect(result.outcome.coverage.failedUnits).toContainEqual({
          unitId: "unit-002",
          errorTag: "FileReviewUnitFailed:AgentPolicyError",
        });
      }),
  );
});

// ---------------------------------------------------------------------------
// E/R compile proofs (AGENTS.md change discipline): the parent-facing
// return-mode Tool view contains expected unit failures — they are results,
// not Run failures — while the delegation Layer's construction requirements
// stay honestly visible.
// ---------------------------------------------------------------------------

const typesModel = Model.make(
  "scripted",
  "fan-out-types",
  Layer.effect(
    LanguageModel.LanguageModel,
    LanguageModel.make({
      generateText: () => Effect.succeed([]),
      streamText: () => Stream.empty,
    }),
  ),
);

const typedChildBinding = Agent.withModel(FileReviewer, typesModel);
const typedFanOutLayer = fanOutHandlersLayer(typedChildBinding);
const typedFanOutProgram = executeReview(Agent.withModel(FanOutReviewer, typesModel), {
  post: false,
  applyVerdict: false,
}).pipe(Effect.provide(typedFanOutLayer));

type LayerContext<L> = L extends Layer.Layer<infer _ROut, infer _E, infer RIn> ? RIn : never;
type EffectError<T> = T extends Effect.Effect<infer _A, infer E, infer _R> ? E : never;

type FanOutLayerRequirements = LayerContext<typeof typedFanOutLayer>;
type FanOutProgramFailure = EffectError<typeof typedFanOutProgram>;
type FanOutProgramServices = Effect.Services<typeof typedFanOutProgram>;
type DelegateHandlerError = Tool.HandlerError<typeof DelegateFileReview>;

// The first-party contained delegation Tool (SUB-033): every expected unit
// failure is result data, so only the engine signals — the durable waiting
// suspension and coordinator-seam error, neither of which can occur on this
// class-E path — remain in the Run's `E`; one failed unit cannot abort the
// fan-out review.
type ContainedHandlerErrorProof = Assert<
  Equal<DelegateHandlerError, AiError.AiError | ToolCallWaiting | SubagentDurabilityError>
>;
type ContainedUnitFailureProof = Assert<
  Equal<Extract<FanOutProgramFailure, FileReviewUnitFailed>, never>
>;
type ContainedBudgetFailureProof = Assert<
  Equal<Extract<FanOutProgramFailure, SubagentBudgetExhausted>, never>
>;
type ContainedExecutionFailureProof = Assert<
  Equal<Extract<FanOutProgramFailure, SubagentExecutionFailure>, never>
>;
// Layer construction carries the child's runtime needs visibly: its tool
// handlers' source port and the parent-owned reservation service.
type LayerSourceProof = Assert<
  Equal<Extract<FanOutLayerRequirements, PullRequestSource>, PullRequestSource>
>;
type LayerReservationsProof = Assert<
  Equal<Extract<FanOutLayerRequirements, SubagentReservations>, SubagentReservations>
>;
type LayerChildHandlersProof = Assert<
  Equal<
    Extract<FanOutLayerRequirements, Tool.HandlersFor<Toolkit.Tools<typeof FileReviewToolkit>>>,
    Tool.HandlersFor<Toolkit.Tools<typeof FileReviewToolkit>>
  >
>;
// Engine-provided per-batch services never surface as program requirements.
type ProgramSpawnerExcludedProof = Assert<
  Equal<Extract<FanOutProgramServices, AgentSpawner>, never>
>;
type ProgramSinkExcludedProof = Assert<Equal<Extract<FanOutProgramServices, RunEventSink>, never>>;
type ProgramDurabilityExcludedProof = Assert<
  Equal<Extract<FanOutProgramServices, SubagentDurability>, never>
>;
// The provided program still needs the coordinator-side source port,
// identifiers, and the publisher — visible, not hidden.
type ProgramSourceProof = Assert<
  Equal<Extract<FanOutProgramServices, PullRequestSource>, PullRequestSource>
>;
type ProgramIdGeneratorProof = Assert<
  Equal<Extract<FanOutProgramServices, IdGenerator>, IdGenerator>
>;

describe("fan-out type proofs", () => {
  it("contains unit failures as results and keeps construction requirements visible", () => {
    const containedHandlerErrorProof: ContainedHandlerErrorProof = true;
    const containedUnitFailureProof: ContainedUnitFailureProof = true;
    const containedBudgetFailureProof: ContainedBudgetFailureProof = true;
    const containedExecutionFailureProof: ContainedExecutionFailureProof = true;
    const layerSourceProof: LayerSourceProof = true;
    const layerReservationsProof: LayerReservationsProof = true;
    const layerChildHandlersProof: LayerChildHandlersProof = true;
    const programSpawnerExcludedProof: ProgramSpawnerExcludedProof = true;
    const programSinkExcludedProof: ProgramSinkExcludedProof = true;
    const programDurabilityExcludedProof: ProgramDurabilityExcludedProof = true;
    const programSourceProof: ProgramSourceProof = true;
    const programIdGeneratorProof: ProgramIdGeneratorProof = true;
    expect([
      containedHandlerErrorProof,
      containedUnitFailureProof,
      containedBudgetFailureProof,
      containedExecutionFailureProof,
      layerSourceProof,
      layerReservationsProof,
      layerChildHandlersProof,
      programSpawnerExcludedProof,
      programSinkExcludedProof,
      programDurabilityExcludedProof,
      programSourceProof,
      programIdGeneratorProof,
    ]).toEqual([true, true, true, true, true, true, true, true, true, true, true, true]);
  });
});

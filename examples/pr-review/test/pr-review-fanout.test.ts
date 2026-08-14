import { describe, expect, it } from "@effect/vitest";
import { Deferred, Effect, Exit, Fiber, Layer, Ref, Schema, Stream } from "effect";
import {
  Agent,
  AgentSpawner,
  IdGenerator,
  RunEventSink,
  SubagentBudgetExhausted,
  SubagentDurability,
  SubagentExecutionFailure,
  SubagentReservations,
  SubagentReservationsMemoryLive,
} from "effect-agent";
import { LanguageModel, Model, type Response, Tool, Toolkit } from "effect/unstable/ai";
import { toCodecOpenAI } from "effect/unstable/ai/OpenAiStructuredOutput";

import {
  ChangedFile,
  CodeReview,
  collectingReviewPublisherLayer,
  DelegateFileReview,
  executeFanOutReview,
  fanOutHandlersLayer,
  FanOutReviewer,
  fanOutReviewerProfile,
  FanOutReviewerProfile,
  fanOutScopedSourceLayer,
  FanOutUnitOutcomes,
  FileReviewer,
  FileReviewReport,
  FileReviewToolkit,
  FileReviewUnitFailed,
  finalizeFanOutReview,
  FixtureFile,
  FixturePullRequest,
  fixturePullRequestSourceLayer,
  ListReviewUnits,
  makeOfflineFanOutCoordinatorModel,
  makeOfflineFileReviewerModel,
  MAX_REVIEW_UNITS,
  MAX_UNIT_FILES,
  offlineChildDiffCallId,
  type OfflineUnitScript,
  planReviewUnits,
  PullRequestMetadata,
  PullRequestSource,
  rankAndDedupeFindings,
  ReviewFinding,
  ReviewPublicationPlan,
} from "../src/index.ts";

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

const binaryFixtureFile = FixtureFile.make({
  file: ChangedFile.make({
    path: "assets/logo.png",
    status: "added",
    additions: 0,
    deletions: 0,
  }),
});

const fixtureMetadata = (totalChangedFiles: number) =>
  PullRequestMetadata.make({
    repository: "acme/widgets",
    number: 202,
    title: "Refactor the api and core constants",
    body: `Large refactor; ${MISSION_MARKER} stays with the coordinator.`,
    baseRef: "main",
    headRef: "refactor/constants",
    headSha: FIXTURE_SHA,
    totalChangedFiles,
  });

const fixture = FixturePullRequest.make({
  metadata: fixtureMetadata(5),
  files: [
    fixtureSource("alpha", "src/api/alpha.ts", 350, 20),
    fixtureSource("beta", "src/api/beta.ts", 330, 20),
    fixtureSource("delta", "src/core/delta.ts", 90, 10),
    fixtureSource("gamma", "src/core/gamma.ts", 190, 10),
    binaryFixtureFile,
  ],
});

/** A changeset with no textual diff at all: the planner yields zero units. */
const undiffableFixture = FixturePullRequest.make({
  metadata: fixtureMetadata(1),
  files: [binaryFixtureFile],
});

const fixtureFiles = fixture.files.map((entry) => entry.file);

const UNIT_ONE = { unitId: "unit-001", paths: ["src/api/alpha.ts", "src/api/beta.ts"] } as const;
const UNIT_TWO = { unitId: "unit-002", paths: ["src/core/delta.ts", "src/core/gamma.ts"] } as const;
const DIFF_MARKERS = ["alphaBase", "betaBase", "deltaBase", "gammaBase"] as const;

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
// Shared offline wiring: the exact CLI fan-out composition, minus GitHub and
// live models.
// ---------------------------------------------------------------------------

const runOfflineFanOut = (script: {
  readonly children: ReadonlyArray<OfflineUnitScript>;
  readonly review: CodeReview;
  readonly unitCalls?: ReadonlyArray<{
    readonly unitId: string;
    readonly paths: ReadonlyArray<string>;
  }>;
  readonly pullRequest?: FixturePullRequest;
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
    const program = executeFanOutReview(parentBinding, childBinding, {
      post: true,
      applyVerdict: false,
    }).pipe(
      Effect.provide(
        Layer.mergeAll(
          fixturePullRequestSourceLayer(script.pullRequest ?? fixture),
          collectingReviewPublisherLayer(published),
          IdGenerator.layer,
          SubagentReservationsMemoryLive,
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

const promptsForUnit = (prompts: ReadonlyArray<string>, unitId: string): ReadonlyArray<string> =>
  prompts.filter((prompt) => prompt.includes(`[review-unit:${unitId}]`));

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
// OpenAI strict schema compatibility for the new tool surface: every object
// node in every parameter schema must forbid additional properties and
// require every declared property, recursively.
// ---------------------------------------------------------------------------

const expectStrictObjectNodes = (node: unknown): void => {
  if (Array.isArray(node)) {
    for (const item of node) expectStrictObjectNodes(item);
    return;
  }
  if (typeof node !== "object" || node === null) return;
  const record = node as Record<string, unknown>;
  if (record["type"] === "object") {
    expect(record["additionalProperties"]).toBe(false);
    const properties = Object.keys(
      (record["properties"] as Record<string, unknown> | undefined) ?? {},
    );
    const required = [...((record["required"] as ReadonlyArray<string> | undefined) ?? [])];
    expect([...required].sort()).toEqual([...properties].sort());
  }
  for (const value of Object.values(record)) expectStrictObjectNodes(value);
};

describe("fan-out OpenAI tool schema compatibility", () => {
  it("encodes the coordinator tools as recursively strict OpenAI object schemas", () => {
    for (const tool of [ListReviewUnits, DelegateFileReview]) {
      const jsonSchema = Tool.getJsonSchema(tool, { transformer: toCodecOpenAI });
      expect(jsonSchema.type).toBe("object");
      expect(jsonSchema.anyOf).toBeUndefined();
      expectStrictObjectNodes(jsonSchema);
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
    // silently disappears (the kommunikasie#202 lesson).
    expect([...assigned, ...plan.unassignedPaths].sort()).toEqual(
      many.map((file) => file.path).sort(),
    );
  });
});

// ---------------------------------------------------------------------------
// Deterministic merge policy and host-side finalization.
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

describe("finalizeFanOutReview", () => {
  const proposal = CodeReview.make({
    summary: "Proposal.",
    verdict: "approve",
    findings: [betaGhostFinding],
  });

  it("replaces the model's findings with the merged collection", () => {
    const finalized = finalizeFanOutReview(proposal, [alphaFinding, gammaFinding]);
    expect(finalized.findings).toEqual(rankAndDedupeFindings([alphaFinding, gammaFinding]));
    expect(finalized.verdict).toBe("approve");
    expect(finalized.summary).toBe("Proposal.");
  });

  it("derives the verdict from the merged findings, both directions", () => {
    const blocking = ReviewFinding.make({ ...gammaFinding, severity: "blocking" });
    // A blocking collected finding forces request-changes past an approval.
    expect(finalizeFanOutReview(proposal, [blocking]).verdict).toBe("request-changes");
    // request-changes without any blocking finding downgrades to comment.
    const harsh = CodeReview.make({ ...proposal, verdict: "request-changes" });
    expect(finalizeFanOutReview(harsh, [gammaFinding]).verdict).toBe("comment");
  });
});

// ---------------------------------------------------------------------------
// The children's scoped source view.
// ---------------------------------------------------------------------------

describe("fanOutScopedSourceLayer", () => {
  it.effect("restricts children to the planned, diffable files fail-closed", () =>
    Effect.gen(function* () {
      const program = Effect.gen(function* () {
        const source = yield* PullRequestSource;
        const files = yield* source.changedFiles;
        expect(files.map((file) => file.path)).toEqual([...UNIT_ONE.paths, ...UNIT_TWO.paths]);
        expect(yield* source.readFile("src/api/alpha.ts")).toBe(headFor("alpha"));
        // The binary file is in the changeset but outside every planned unit.
        const refused = yield* Effect.exit(source.readFile("assets/logo.png"));
        expect(Exit.isFailure(refused)).toBe(true);
      });
      yield* program.pipe(
        Effect.provide(
          fanOutScopedSourceLayer.pipe(Layer.provide(fixturePullRequestSourceLayer(fixture))),
        ),
      );
    }),
  );
});

// ---------------------------------------------------------------------------
// Offline end-to-end fan-out: scripted coordinator + scripted children over
// the real S1 delegation, toolkits, source port, and publication path.
// ---------------------------------------------------------------------------

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

describe("offline fan-out review run", () => {
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

        // The merged review round-tripped through the run unchanged (the
        // host merge agrees with the coordinator's honest proposal).
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

        // Context isolation: EVERY prompt of each child is free of the other
        // unit's paths, the other unit's diff content, and the parent-only
        // mission marker.
        const unitOnePrompts = promptsForUnit(result.childPrompts, "unit-001");
        const unitTwoPrompts = promptsForUnit(result.childPrompts, "unit-002");
        expect(unitOnePrompts.length).toBeGreaterThan(0);
        expect(unitTwoPrompts.length).toBeGreaterThan(0);
        expect(unitOnePrompts.length + unitTwoPrompts.length).toBe(result.childPrompts.length);
        for (const prompt of unitOnePrompts) {
          for (const leaked of [...UNIT_TWO.paths, "deltaBase", "gammaBase", MISSION_MARKER]) {
            expect(prompt).not.toContain(leaked);
          }
        }
        for (const prompt of unitTwoPrompts) {
          for (const leaked of [...UNIT_ONE.paths, "alphaBase", "betaBase", MISSION_MARKER]) {
            expect(prompt).not.toContain(leaked);
          }
        }

        // Declassification: no diff content from ANY unit ever reaches the
        // coordinator — only the projected findings do.
        for (const prompt of result.coordinatorPrompts) {
          for (const marker of DIFF_MARKERS) {
            expect(prompt).not.toContain(marker);
          }
        }
        const finalPrompt = result.coordinatorPrompts[2] ?? "";
        expect(finalPrompt).toContain(alphaFinding.title);
        expect(finalPrompt).toContain(gammaFinding.title);
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
    }),
  );

  it.effect("bounds a runaway child by its policy: typed failure, reported, never retried", () =>
    Effect.gen(function* () {
      const runawayChildren: ReadonlyArray<OfflineUnitScript> = [
        happyChildren[0] as OfflineUnitScript,
        {
          unitId: "unit-002",
          diffPath: "src/core/gamma.ts",
          // One more declared call than the child AgentPolicy allows.
          outcome: { _tag: "budget-runaway", declaredCalls: 17 },
        },
      ];
      const honestReview = CodeReview.make({
        summary: "unit-002 unreviewed: AgentPolicyError (exceeded its Tool Call budget).",
        verdict: "comment",
        findings: rankAndDedupeFindings([...unitOneReport.findings]),
      });

      const result = yield* runOfflineFanOut({ children: runawayChildren, review: honestReview });

      // The child failed typed on its Tool Call bound BEFORE any of the
      // runaway calls executed: one model call, no second chance.
      expect(result.childCalls).toBe(3);
      expect(result.coordinatorCalls).toBe(3);

      // The typed policy failure crossed the delegation boundary bounded and
      // was reported, not retried.
      const finalPrompt = result.coordinatorPrompts[2] ?? "";
      expect(finalPrompt).toContain("FileReviewUnitFailed");
      expect(finalPrompt).toContain("AgentPolicyError");
      expect(finalPrompt).toContain("16 Tool Call limit");
      expect(result.outcome.review.summary).toContain("unit-002 unreviewed: AgentPolicyError");
      expect(result.published).toHaveLength(1);
    }),
  );

  it.effect("strips invented findings and restores dropped ones host-side", () =>
    Effect.gen(function* () {
      const gammaBlocking = ReviewFinding.make({ ...gammaFinding, severity: "blocking" });
      const children: ReadonlyArray<OfflineUnitScript> = [
        happyChildren[0] as OfflineUnitScript,
        {
          unitId: "unit-002",
          diffPath: "src/core/gamma.ts",
          outcome: {
            _tag: "findings",
            report: FileReviewReport.make({ unitId: "unit-002", findings: [gammaBlocking] }),
          },
        },
      ];
      // A dishonest coordinator: it drops the blocking gamma finding, invents
      // a delta finding no child reported, and approves.
      const invented = ReviewFinding.make({
        path: "src/core/delta.ts",
        startLine: 2,
        endLine: 2,
        severity: "nit",
        title: "Invented by the coordinator",
        body: "No child ever reported this.",
      });
      const dishonestReview = CodeReview.make({
        summary: "Everything is fine.",
        verdict: "approve",
        findings: [alphaFinding, invented],
      });

      const result = yield* runOfflineFanOut({ children, review: dishonestReview });

      // The host merged EXACTLY the collected child findings: the invented
      // finding is gone, the dropped blocking finding is back, and the
      // verdict is re-derived from what the children actually found.
      expect(result.outcome.review.findings).toEqual(
        rankAndDedupeFindings([...unitOneReport.findings, gammaBlocking]),
      );
      expect(
        result.outcome.review.findings.some((finding) => finding.title === invented.title),
      ).toBe(false);
      expect(result.outcome.review.verdict).toBe("request-changes");
      expect(result.outcome.plan.comments.map((comment) => comment.path)).toEqual([
        "src/core/gamma.ts",
        "src/api/alpha.ts",
      ]);
    }),
  );

  it.effect("fails a delegation that deviates from the host unit plan, typed and unretried", () =>
    Effect.gen(function* () {
      const honestReview = CodeReview.make({
        summary: "unit-002 unreviewed: UnitPlanViolation (delegation did not match the plan).",
        verdict: "comment",
        findings: rankAndDedupeFindings([...unitOneReport.findings]),
      });
      const result = yield* runOfflineFanOut({
        children: [happyChildren[0] as OfflineUnitScript],
        review: honestReview,
        // The coordinator invents a unit-002 with only half its planned paths.
        unitCalls: [UNIT_ONE, { unitId: "unit-002", paths: ["src/core/delta.ts"] }],
      });

      // The off-plan delegation failed typed BEFORE any child spawned: only
      // the valid unit's child ran (two turns).
      expect(result.childCalls).toBe(2);
      const finalPrompt = result.coordinatorPrompts[2] ?? "";
      expect(finalPrompt).toContain("FileReviewUnitFailed");
      expect(finalPrompt).toContain("UnitPlanViolation");
      expect(result.outcome.review.findings).toEqual(
        rankAndDedupeFindings([...unitOneReport.findings]),
      );
      expect(result.published).toHaveLength(1);
    }),
  );

  it.effect("rejects a child report that escapes its unit's planned paths", () =>
    Effect.gen(function* () {
      const children: ReadonlyArray<OfflineUnitScript> = [
        happyChildren[0] as OfflineUnitScript,
        {
          unitId: "unit-002",
          diffPath: "src/core/gamma.ts",
          outcome: {
            _tag: "findings",
            // The unit-002 child claims a finding anchored in unit-001.
            report: FileReviewReport.make({ unitId: "unit-002", findings: [alphaFinding] }),
          },
        },
      ];
      const honestReview = CodeReview.make({
        summary: "unit-002 unreviewed: UnitScopeViolation (report escaped its unit).",
        verdict: "comment",
        findings: rankAndDedupeFindings([...unitOneReport.findings]),
      });

      const result = yield* runOfflineFanOut({ children, review: honestReview });

      // The out-of-unit report failed typed at the declassification boundary
      // and contributed NOTHING to the merged findings.
      const finalPrompt = result.coordinatorPrompts[2] ?? "";
      expect(finalPrompt).toContain("UnitScopeViolation");
      expect(result.outcome.review.findings).toEqual(
        rankAndDedupeFindings([...unitOneReport.findings]),
      );
      expect(result.published).toHaveLength(1);
    }),
  );

  it.effect("reviews an all-binary changeset without delegating at all", () =>
    Effect.gen(function* () {
      const emptyReview = CodeReview.make({
        summary: "Nothing reviewable: assets/logo.png has no textual diff.",
        verdict: "approve",
        findings: [],
      });
      const result = yield* runOfflineFanOut({
        children: [],
        review: emptyReview,
        unitCalls: [],
        pullRequest: undiffableFixture,
      });

      // list -> final: the empty plan finalizes without one delegation call.
      expect(result.outcome.turns).toBe(2);
      expect(result.childCalls).toBe(0);
      expect(result.outcome.review).toEqual(emptyReview);
      expect(result.outcome.plan.comments).toHaveLength(0);
      expect(result.published).toHaveLength(1);
    }),
  );
});

// ---------------------------------------------------------------------------
// Concurrency evidence: both children are live at the same time. Each gated
// child signals on its first model turn and withholds its report until the
// test releases it — a sequential implementation would never let the second
// child start while the first is held, so awaiting both starts proves the
// delegations overlap.
// ---------------------------------------------------------------------------

const scriptedUsage = { inputTokens: { total: 64 }, outputTokens: { total: 48 } };

const makeGatedFileReviewerModel = (
  scripts: ReadonlyArray<{ readonly unitId: string; readonly diffPath: string }>,
) =>
  Effect.gen(function* () {
    const gates = new Map<
      string,
      { readonly started: Deferred.Deferred<void>; readonly release: Deferred.Deferred<void> }
    >();
    for (const script of scripts) {
      gates.set(script.unitId, {
        started: yield* Deferred.make<void>(),
        release: yield* Deferred.make<void>(),
      });
    }
    const gatesFor = (unitId: string) =>
      Effect.suspend(() => {
        const entry = gates.get(unitId);
        return entry === undefined
          ? Effect.die(new Error(`No gates exist for unit ${unitId}`))
          : Effect.succeed(entry);
      });
    const controls = {
      awaitStarted: (unitId: string) =>
        gatesFor(unitId).pipe(Effect.flatMap((entry) => Deferred.await(entry.started))),
      release: (unitId: string) =>
        gatesFor(unitId).pipe(
          Effect.flatMap((entry) => Deferred.succeed(entry.release, undefined)),
          Effect.asVoid,
        ),
    };
    const model = Model.make(
      "scripted",
      "pr-fanout-gated-child",
      Layer.effect(
        LanguageModel.LanguageModel,
        LanguageModel.make({
          generateText: () => Effect.succeed([]),
          streamText: (request) =>
            Stream.unwrap(
              Effect.gen(function* () {
                const promptJson = JSON.stringify(request.prompt);
                const script = scripts.find((candidate) =>
                  promptJson.includes(`[review-unit:${candidate.unitId}]`),
                );
                if (script === undefined) {
                  return yield* Effect.die(new Error("The child prompt names no gated unit"));
                }
                const entry = yield* gatesFor(script.unitId);
                if (promptJson.includes(offlineChildDiffCallId(script.unitId))) {
                  yield* Deferred.await(entry.release);
                  return Stream.fromIterable<Response.StreamPartEncoded>([
                    { type: "text-start", id: "report" },
                    {
                      type: "text-delta",
                      id: "report",
                      delta: JSON.stringify(
                        Schema.encodeSync(FileReviewReport)(
                          FileReviewReport.make({ unitId: script.unitId, findings: [] }),
                        ),
                      ),
                    },
                    { type: "text-end", id: "report" },
                    { type: "finish", reason: "stop", usage: scriptedUsage },
                  ]);
                }
                yield* Deferred.succeed(entry.started, undefined);
                return Stream.fromIterable<Response.StreamPartEncoded>([
                  {
                    type: "tool-call",
                    id: offlineChildDiffCallId(script.unitId),
                    name: "read_file_diff",
                    params: { path: script.diffPath },
                    providerExecuted: false,
                  },
                  { type: "finish", reason: "tool-calls", usage: scriptedUsage },
                ]);
              }),
            ),
        }),
      ),
    );
    return { controls, model };
  });

describe("fan-out concurrency", () => {
  it.effect("runs the delegated children concurrently, not sequentially", () =>
    Effect.gen(function* () {
      const coordinator = yield* makeOfflineFanOutCoordinatorModel({
        unitCalls: [UNIT_ONE, UNIT_TWO],
        review: CodeReview.make({
          summary: "Both units reviewed concurrently; nothing found.",
          verdict: "approve",
          findings: [],
        }),
      });
      const gated = yield* makeGatedFileReviewerModel([
        { unitId: "unit-001", diffPath: "src/api/alpha.ts" },
        { unitId: "unit-002", diffPath: "src/core/gamma.ts" },
      ]);
      const published = yield* Ref.make<ReadonlyArray<ReviewPublicationPlan>>([]);
      const program = executeFanOutReview(
        Agent.withModel(FanOutReviewer, coordinator.model),
        Agent.withModel(FileReviewer, gated.model),
        { post: false, applyVerdict: false },
      ).pipe(
        Effect.provide(
          Layer.mergeAll(
            fixturePullRequestSourceLayer(fixture),
            collectingReviewPublisherLayer(published),
            IdGenerator.layer,
            SubagentReservationsMemoryLive,
          ),
        ),
        Effect.scoped,
      );

      const fiber = yield* requiresNothing(program).pipe(Effect.forkChild);
      // Both children reached their first model turn while both reports are
      // still withheld: the delegations overlap in time.
      yield* gated.controls.awaitStarted("unit-001");
      yield* gated.controls.awaitStarted("unit-002");
      yield* gated.controls.release("unit-001");
      yield* gated.controls.release("unit-002");
      const outcome = yield* Fiber.join(fiber);
      expect(outcome.review.findings).toHaveLength(0);
    }),
  );
});

// ---------------------------------------------------------------------------
// E/R compile proofs (AGENTS.md change discipline): the parent-facing
// return-mode Tool view contains expected unit failures — they are results,
// not Run failures — while the delegation Layer's construction requirements
// stay honestly visible and the findings collector never leaks to callers.
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
const typedFanOutProgram = executeFanOutReview(
  Agent.withModel(FanOutReviewer, typesModel),
  typedChildBinding,
  { post: false, applyVerdict: false },
);

type LayerContext<L> = L extends Layer.Layer<infer _ROut, infer _E, infer RIn> ? RIn : never;
type EffectError<T> = T extends Effect.Effect<infer _A, infer E, infer _R> ? E : never;

type FanOutLayerRequirements = LayerContext<typeof typedFanOutLayer>;
type FanOutProgramFailure = EffectError<typeof typedFanOutProgram>;
type FanOutProgramServices = Effect.Services<typeof typedFanOutProgram>;
type DelegateHandlerError = Tool.HandlerError<typeof DelegateFileReview>;

// The parent-facing Tool contains failures: nothing reaches the Run's `E`
// through it (failureMode "return"), so one failed unit cannot abort the
// fan-out review.
type ContainedHandlerErrorProof = Assert<Equal<DelegateHandlerError, never>>;
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
// handlers' source port, the projection services (source port + findings
// collector), and the parent-owned reservation service.
type LayerSourceProof = Assert<
  Equal<Extract<FanOutLayerRequirements, PullRequestSource>, PullRequestSource>
>;
type LayerOutcomesProof = Assert<
  Equal<Extract<FanOutLayerRequirements, FanOutUnitOutcomes>, FanOutUnitOutcomes>
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
// Engine-provided per-batch services never surface as program requirements,
// and the per-run findings collector is owned inside `executeFanOutReview`.
type ProgramSpawnerExcludedProof = Assert<
  Equal<Extract<FanOutProgramServices, AgentSpawner>, never>
>;
type ProgramSinkExcludedProof = Assert<Equal<Extract<FanOutProgramServices, RunEventSink>, never>>;
type ProgramDurabilityExcludedProof = Assert<
  Equal<Extract<FanOutProgramServices, SubagentDurability>, never>
>;
type ProgramOutcomesExcludedProof = Assert<
  Equal<Extract<FanOutProgramServices, FanOutUnitOutcomes>, never>
>;
// The provided program still needs the coordinator-side source port,
// identifiers, and the reservation service — visible, not hidden.
type ProgramSourceProof = Assert<
  Equal<Extract<FanOutProgramServices, PullRequestSource>, PullRequestSource>
>;
type ProgramIdGeneratorProof = Assert<
  Equal<Extract<FanOutProgramServices, IdGenerator>, IdGenerator>
>;
type ProgramReservationsProof = Assert<
  Equal<Extract<FanOutProgramServices, SubagentReservations>, SubagentReservations>
>;

describe("fan-out type proofs", () => {
  it("contains unit failures as results and keeps construction requirements visible", () => {
    const containedHandlerErrorProof: ContainedHandlerErrorProof = true;
    const containedUnitFailureProof: ContainedUnitFailureProof = true;
    const containedBudgetFailureProof: ContainedBudgetFailureProof = true;
    const containedExecutionFailureProof: ContainedExecutionFailureProof = true;
    const layerSourceProof: LayerSourceProof = true;
    const layerOutcomesProof: LayerOutcomesProof = true;
    const layerReservationsProof: LayerReservationsProof = true;
    const layerChildHandlersProof: LayerChildHandlersProof = true;
    const programSpawnerExcludedProof: ProgramSpawnerExcludedProof = true;
    const programSinkExcludedProof: ProgramSinkExcludedProof = true;
    const programDurabilityExcludedProof: ProgramDurabilityExcludedProof = true;
    const programOutcomesExcludedProof: ProgramOutcomesExcludedProof = true;
    const programSourceProof: ProgramSourceProof = true;
    const programIdGeneratorProof: ProgramIdGeneratorProof = true;
    const programReservationsProof: ProgramReservationsProof = true;
    expect([
      containedHandlerErrorProof,
      containedUnitFailureProof,
      containedBudgetFailureProof,
      containedExecutionFailureProof,
      layerSourceProof,
      layerOutcomesProof,
      layerReservationsProof,
      layerChildHandlersProof,
      programSpawnerExcludedProof,
      programSinkExcludedProof,
      programDurabilityExcludedProof,
      programOutcomesExcludedProof,
      programSourceProof,
      programIdGeneratorProof,
      programReservationsProof,
    ]).toEqual([
      true,
      true,
      true,
      true,
      true,
      true,
      true,
      true,
      true,
      true,
      true,
      true,
      true,
      true,
      true,
    ]);
  });
});

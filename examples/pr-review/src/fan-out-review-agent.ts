import { Context, Effect, Layer, Ref, Schema } from "effect";
import {
  Agent,
  AgentPolicy,
  AgentSpawner,
  IdGenerator,
  RunEventSink,
  Subagent,
  SubagentBudgetExhausted,
  SubagentDurability,
  SubagentDurabilityError,
  SubagentExecutionFailure,
  SubagentPolicy,
  SubagentPrestartDenied,
  SubagentProjectionFailure,
  SubagentRuntime,
  ToolCallWaiting,
  ToolExecutionClass,
  type RuntimeBinding,
} from "effect-agent";
import { Tool, Toolkit } from "effect/unstable/ai";

import { ChangedPath } from "./diff.ts";
import {
  CodeReview,
  ReadFile,
  ReadFileDiff,
  readFileDiffHandler,
  readFileHandler,
  ReviewFinding,
  ReviewMission,
  type ReviewVerdict,
} from "./review-agent.ts";
import {
  MAX_REVIEW_UNITS,
  MAX_UNIT_FILES,
  planReviewUnits,
  rankAndDedupeFindings,
  type ReviewUnit,
  ReviewUnitId,
  ReviewUnitPlan,
} from "./review-units.ts";
import {
  executeReview,
  type ExecuteReviewOptions,
  fanOutReviewBudgetLimits,
} from "./run-review.ts";
import {
  normalizeRepoRelativePath,
  PullRequestSource,
  PullRequestSourceFailure,
  ReviewInputViolation,
} from "./source.ts";

// ---------------------------------------------------------------------------
// The fan-out reviewer: the same review contract as the flat reviewer, but
// the diff reading happens in bounded delegated children (S1 attached
// ephemeral delegation) so no single context window has to hold every diff.
// A coordinator lists the changeset as deterministic review units, delegates
// one `delegate_file_review` call per unit, then merges the children's
// bounded findings into one `CodeReview`. Publication and anchor validation
// are unchanged: child output is untrusted input like everything else and
// crosses to the host only through the same fail-closed planPublication path.
// ---------------------------------------------------------------------------

/** One child returns at most this many findings; the merge caps the total. */
export const MAX_CHILD_FINDINGS = 8;

// ---------------------------------------------------------------------------
// The child: a file reviewer over one unit. Its toolkit is intentionally
// smaller than the flat reviewer's — diff and head-file reads only, no
// changeset listing — so a child can never roam beyond its briefed unit's
// observation surface being the whole changeset port.
// ---------------------------------------------------------------------------

export const FileReviewToolkit = Toolkit.make(ReadFileDiff, ReadFile);

export const FileReviewToolkitLayer = FileReviewToolkit.toLayer({
  read_file_diff: readFileDiffHandler,
  read_file: readFileHandler,
});

/**
 * The children's view of the pull request: only files belonging to some
 * planned review unit are observable. S1 cannot scope a service per
 * invocation (child requirements are fixed at handler-Layer construction —
 * FRICTION.md item 12), so per-UNIT scoping is enforced host-side at the
 * projection boundary instead; this Layer still shrinks every child's
 * observation surface from "the whole changeset" to "the planned, diffable
 * files", fail-closed like the rest of the source port.
 */
export const fanOutScopedSourceLayer: Layer.Layer<PullRequestSource, never, PullRequestSource> =
  Layer.effect(
    PullRequestSource,
    Effect.gen(function* () {
      const source = yield* PullRequestSource;
      const plannedPaths = source.changedFiles.pipe(
        Effect.map((files) => {
          const plan = planReviewUnits(files, { totalChangedFiles: files.length });
          return new Set(plan.units.flatMap((unit) => [...unit.paths]));
        }),
      );
      return PullRequestSource.of({
        metadata: source.metadata,
        changedFiles: Effect.gen(function* () {
          const files = yield* source.changedFiles;
          const allowed = yield* plannedPaths;
          return files.filter((file) => allowed.has(file.path));
        }),
        readFile: (path) =>
          Effect.gen(function* () {
            const relative = yield* normalizeRepoRelativePath(path);
            const allowed = yield* plannedPaths;
            if (!allowed.has(relative)) {
              return yield* ReviewInputViolation.make({
                input: relative,
                reason: "Path is outside the planned review units.",
              });
            }
            return yield* source.readFile(relative);
          }),
      });
    }),
  );

const UnitPaths = Schema.Array(ChangedPath)
  .check(Schema.isMinLength(1))
  .check(Schema.isMaxLength(MAX_UNIT_FILES));

/** The child Agent input: one briefed unit of the changeset. */
export class FileReviewBrief extends Schema.Class<FileReviewBrief>(
  "@effect-agent/example-pr-review/FileReviewBrief",
)({
  unitId: ReviewUnitId,
  paths: UnitPaths,
  focus: Schema.NonEmptyString.check(Schema.isMaxLength(200)),
}) {}

/** The child Agent output: the briefed unit's bounded findings. */
export class FileReviewReport extends Schema.Class<FileReviewReport>(
  "@effect-agent/example-pr-review/FileReviewReport",
)({
  unitId: ReviewUnitId,
  findings: Schema.Array(ReviewFinding).check(Schema.isMaxLength(MAX_CHILD_FINDINGS)),
}) {}

export const fileReviewerInstructions = (brief: FileReviewBrief): string =>
  [
    `You are a code reviewer for one unit of a pull request: [review-unit:${brief.unitId}], covering exactly these changed files: ${brief.paths.join(", ")}. Focus: ${brief.focus}.`,
    "Work in this order:",
    "1. Call read_file_diff for every file in your unit. In its output, only lines marked R<number> exist in the new version; those numbers are the only valid values for startLine and endLine. Never anchor a finding to a removed (-) line.",
    "2. Call read_file when you need surrounding context the diff does not show. Only changed files are readable.",
    "3. Review for real defects first: correctness, security, concurrency, resource leaks, error handling, API misuse. Style nits are least important. Do not praise; do not restate the diff.",
    `4. Then return ONLY a JSON object — no Markdown fences, no prose before or after — exactly this shape: {"unitId": ${JSON.stringify(brief.unitId)}, "findings": [{"path": <string, a file in your unit>, "startLine": <integer, an R-marked new-file line>, "endLine": <integer, >= startLine, same file, R-marked>, "severity": <"blocking" | "important" | "nit">, "title": <string, <= 120 chars>, "body": <string, why it matters and what to do>, "suggestion": <string, OPTIONAL: replacement text for exactly lines startLine..endLine, ready to commit>}]}.`,
    `Report at most ${MAX_CHILD_FINDINGS} findings; prefer the most important ones. An empty findings array is a valid report. Never report on files outside your unit. Line anchors you invent will be discarded, so copy R-numbers from read_file_diff output.`,
  ].join("\n");

/**
 * The child Agent Definition. Bounds are tight and per-unit: a unit that
 * cannot be reviewed inside them fails typed and is reported by the
 * coordinator, never silently absorbed or retried.
 */
export const FileReviewer = Agent.define("pr-file-reviewer", {
  input: FileReviewBrief,
  output: FileReviewReport,
  instructions: fileReviewerInstructions,
  toolkit: FileReviewToolkit,
  policy: AgentPolicy.make({
    maxTurns: 8,
    maxToolCalls: 16,
    maxDuration: "4 minutes",
    toolConcurrency: 2,
    tokenBudget: 200_000,
  }),
  description:
    "Review one bounded unit of a pull request's changeset read-only and return line-anchored findings for exactly those files.",
  metadata: { deploymentClass: "E", surface: "read-only" },
});

// ---------------------------------------------------------------------------
// The delegation: one Effect AI Tool per review unit, with explicit
// projections and finite bounds (SUB-009). `projectResult` is the
// declassification boundary — the parent sees the child's bounded findings,
// never its transcript or the diffs it read.
// ---------------------------------------------------------------------------

/** The model-decoded delegation parameters: which unit to review. */
export class FileReviewRequest extends Schema.Class<FileReviewRequest>(
  "@effect-agent/example-pr-review/FileReviewRequest",
)({
  unitId: ReviewUnitId,
  paths: UnitPaths,
}) {}

/** The bounded parent-visible result of one delegated unit review. */
export class FileReviewUnitResult extends Schema.Class<FileReviewUnitResult>(
  "@effect-agent/example-pr-review/FileReviewUnitResult",
)({
  unitId: ReviewUnitId,
  findings: Schema.Array(ReviewFinding).check(Schema.isMaxLength(MAX_CHILD_FINDINGS)),
}) {}

/**
 * One unit's review failed: the child Run ended in a typed failure (policy
 * bound, output violation, model fault). The marker is bounded and carries no
 * child transcript content beyond the failure tag and message.
 */
export class FileReviewUnitFailed extends Schema.TaggedErrorClass<FileReviewUnitFailed>()(
  "FileReviewUnitFailed",
  {
    childErrorTag: Schema.NonEmptyString.check(Schema.isMaxLength(256)),
    message: Schema.String.check(Schema.isMaxLength(400)),
  },
) {}

/**
 * Finite per-invocation bounds (SUB-009), aligned with the child's own
 * AgentPolicy: the child's policy is the limit that trips typed; the
 * reservation mirrors it so parent-side accounting stays honest. The token
 * and cost caps make the AGGREGATE child budget explicit — scaled by
 * `maxChildren`, a fan-out run's children are bounded to 8 × these values,
 * enforced as honestly as provider usage reporting allows.
 */
export const fileReviewPolicy = SubagentPolicy.make({
  maxChildren: MAX_REVIEW_UNITS,
  maxConcurrency: 3,
  maxTurns: 8,
  maxToolCalls: 16,
  maxDuration: "4 minutes",
  maxInputTokens: 200_000,
  maxOutputTokens: 16_000,
  maxCostMicrousd: 250_000,
});

const delegationDescription =
  "Delegate the review of one planned unit to a bounded file-reviewer child and return its line-anchored findings. Call it exactly once per unit from list_review_units; never retry a failed unit.";

/**
 * Host-side collector of validated child findings. `projectResult` records
 * every finding that survived unit-scope validation, and the run's
 * `finalizeReview` hook merges EXACTLY this collection mechanically — the
 * coordinator model can therefore neither invent findings nor silently drop
 * a delegated one.
 */
export class FanOutUnitOutcomes extends Context.Service<
  FanOutUnitOutcomes,
  {
    readonly record: (findings: ReadonlyArray<ReviewFinding>) => Effect.Effect<void>;
    readonly collected: Effect.Effect<ReadonlyArray<ReviewFinding>>;
  }
>()("@effect-agent/example-pr-review/FanOutUnitOutcomes") {}

/** One collector per review run; the instance is shared with `finalizeFanOutReview`. */
export const makeFanOutUnitOutcomes = Effect.gen(function* () {
  const findings = yield* Ref.make<ReadonlyArray<ReviewFinding>>([]);
  return FanOutUnitOutcomes.of({
    record: (unitFindings) => Ref.update(findings, (previous) => [...previous, ...unitFindings]),
    collected: Ref.get(findings),
  });
});

const boundedFailureMessage = (message: string): string => message.slice(0, 400);

/**
 * The host-owned unit plan, recomputed deterministically from the source.
 * Both projections consult it: coordinator-supplied delegation parameters
 * and child-claimed unit identities are untrusted model output and must
 * match the plan exactly (fail-closed, SEC-007 style).
 */
const plannedUnits: Effect.Effect<
  ReadonlyArray<ReviewUnit>,
  FileReviewUnitFailed,
  PullRequestSource
> = Effect.gen(function* () {
  const source = yield* PullRequestSource;
  const files = yield* source.changedFiles.pipe(
    Effect.mapError((failure) =>
      FileReviewUnitFailed.make({
        childErrorTag: failure._tag,
        message: boundedFailureMessage(failure.message),
      }),
    ),
  );
  return planReviewUnits(files, { totalChangedFiles: files.length }).units;
});

export const fileReviewDelegation = Subagent.define("delegate_file_review", {
  description: delegationDescription,
  target: FileReviewer,
  parameters: FileReviewRequest,
  success: FileReviewUnitResult,
  failure: FileReviewUnitFailed,
  // Coordinator output is untrusted: a delegation request must name a
  // planned unit with exactly its planned paths, or it fails typed before
  // any child budget is reserved. The brief is built from the HOST plan.
  prepareInput: (request) =>
    Effect.gen(function* () {
      const units = yield* plannedUnits;
      const unit = units.find((candidate) => candidate.unitId === request.unitId);
      const matchesPlan =
        unit !== undefined &&
        unit.paths.length === request.paths.length &&
        unit.paths.every((path, index) => path === request.paths[index]);
      if (unit === undefined || !matchesPlan) {
        return yield* FileReviewUnitFailed.make({
          childErrorTag: "UnitPlanViolation",
          message: boundedFailureMessage(
            `Delegation request for ${request.unitId} does not match the host review-unit plan.`,
          ),
        });
      }
      return FileReviewBrief.make({
        unitId: unit.unitId,
        paths: unit.paths,
        focus: "defects-first: correctness, security, concurrency, resources, error handling",
      });
    }),
  // The explicit declassification boundary (SUB-015): exactly the bounded,
  // unit-scoped findings cross to the parent. The claimed unit must be
  // planned and every finding must anchor inside that unit's planned paths;
  // whether an anchor names a real diff line stays a host publication check.
  projectResult: (report) =>
    Effect.gen(function* () {
      const units = yield* plannedUnits;
      const unit = units.find((candidate) => candidate.unitId === report.unitId);
      if (unit === undefined) {
        return yield* FileReviewUnitFailed.make({
          childErrorTag: "UnitScopeViolation",
          message: boundedFailureMessage(`Child report claims unplanned unit ${report.unitId}.`),
        });
      }
      for (const finding of report.findings) {
        if (!unit.paths.includes(finding.path)) {
          return yield* FileReviewUnitFailed.make({
            childErrorTag: "UnitScopeViolation",
            message: boundedFailureMessage(
              `Child report for ${report.unitId} includes a finding outside its unit.`,
            ),
          });
        }
      }
      const outcomes = yield* FanOutUnitOutcomes;
      yield* outcomes.record(report.findings);
      return FileReviewUnitResult.make({
        unitId: report.unitId,
        findings: report.findings,
      });
    }),
  policy: fileReviewPolicy,
});

/**
 * Total mapping from every expected child Run failure to the declared unit
 * failure (SUB-028): the tag plus a bounded message, nothing else crosses.
 */
export const mapFileReviewChildFailure = (failure: {
  readonly _tag: string;
  readonly message?: string;
}): FileReviewUnitFailed =>
  FileReviewUnitFailed.make({
    childErrorTag: failure._tag,
    message: (failure.message ?? "").slice(0, 400),
  });

/** Runtime wiring: the immutable delegation plus one explicit child Binding. */
export const fanOutHandlersLayer = <Provider, ModelProvides, ModelRequires>(
  childBinding: RuntimeBinding<
    typeof FileReviewBrief,
    typeof FileReviewReport,
    typeof fileReviewerInstructions,
    Toolkit.Tools<typeof FileReviewToolkit>,
    Provider,
    ModelProvides,
    ModelRequires
  >,
) =>
  SubagentRuntime.layer(fileReviewDelegation, childBinding, {
    mapChildFailure: mapFileReviewChildFailure,
  });

// ---------------------------------------------------------------------------
// The parent-facing view of the delegation Tool.
//
// `Subagent.define` fixes `failureMode: "error"`, so a failed child would
// fail the WHOLE parent Run typed — one unreviewable unit would abort the
// entire fan-out review. This coordinator wants partial results with honest
// reporting instead, so its Toolkit carries a same-name Tool value with the
// identical parameter/success/failure Schemas but `failureMode: "return"`:
// Effect AI resolves handlers by Tool NAME, so the real S1 delegation handler
// from `SubagentRuntime.layer` still executes, and a typed unit failure
// reaches the model as a failed tool result (bounded, encoded through the
// declared failure union) instead of aborting the Run. Recorded as authoring
// friction: `Subagent.define` should accept a containment policy directly.
// ---------------------------------------------------------------------------

/** Exactly the failure union `Subagent.define` declares for this delegation. */
export const FileReviewDelegationFailure = Schema.Union([
  FileReviewUnitFailed,
  SubagentPrestartDenied,
  SubagentBudgetExhausted,
  SubagentProjectionFailure,
  SubagentExecutionFailure,
  ToolCallWaiting,
  SubagentDurabilityError,
]);

export const DelegateFileReview = Tool.make("delegate_file_review", {
  description: delegationDescription,
  parameters: FileReviewRequest,
  success: FileReviewUnitResult,
  failure: FileReviewDelegationFailure,
  failureMode: "return",
})
  .addDependency(AgentSpawner)
  .addDependency(RunEventSink)
  .addDependency(SubagentDurability)
  .addDependency(IdGenerator)
  // The delegated child's whole tool surface is read-only.
  .annotate(ToolExecutionClass, "readonly");

// ---------------------------------------------------------------------------
// The coordinator's own tool: the deterministic unit plan over the changeset.
// Grouping is host code (review-units.ts), not model prose, so fan-out shape
// and budget honesty stay pinnable in tests.
// ---------------------------------------------------------------------------

export class ListReviewUnitsQuery extends Schema.Class<ListReviewUnitsQuery>(
  "@effect-agent/example-pr-review/ListReviewUnitsQuery",
)({
  /** Explicit constant keeps the zero-choice operation compatible with strict provider schemas. */
  scope: Schema.Literal("all"),
}) {}

export const ListReviewUnits = Tool.make("list_review_units", {
  description:
    "List this pull request's changeset grouped into bounded review units (size-budgeted, directory-affine), plus the files no unit can cover.",
  parameters: ListReviewUnitsQuery,
  success: ReviewUnitPlan,
  failure: PullRequestSourceFailure,
  failureMode: "error",
  dependencies: [PullRequestSource],
}).annotate(ToolExecutionClass, "readonly");

export const FanOutCoordinatorToolkit = Toolkit.make(ListReviewUnits);

export const FanOutCoordinatorToolkitLayer = FanOutCoordinatorToolkit.toLayer({
  list_review_units: () =>
    Effect.gen(function* () {
      const source = yield* PullRequestSource;
      const files = yield* source.changedFiles;
      const metadata = yield* source.metadata;
      return planReviewUnits(files, { totalChangedFiles: metadata.totalChangedFiles });
    }),
});

// ---------------------------------------------------------------------------
// The coordinator Agent Definition: same mission input and CodeReview output
// contract as the flat reviewer, so planPublication and anchor validation
// apply unchanged.
// ---------------------------------------------------------------------------

export const FanOutReviewToolkit = Toolkit.make(ListReviewUnits, DelegateFileReview);

export const fanOutReviewInstructions = (mission: ReviewMission): string =>
  [
    `You coordinate the review of pull request #${mission.number} ("${mission.title}") in ${mission.repository}, merging ${mission.headRef} into ${mission.baseRef}. It changes ${mission.changedFileCount} file(s).`,
    mission.body.length > 0
      ? `Author description:\n${mission.body}`
      : "The author provided no description.",
    "Work in this order:",
    "1. Call list_review_units once to get the planned review units.",
    "2. Call delegate_file_review EXACTLY once per unit, passing each unit's unitId and paths verbatim — requests that deviate from the plan fail typed. Prefer declaring all delegation calls in one batch. Never review files yourself and never invent units.",
    '3. A delegation result with "_tag" is a FAILED unit. Never retry it; instead your summary MUST name it honestly, e.g. "unit-002 unreviewed: AgentPolicyError". The plan\'s undiffablePaths and unassignedPaths must also be named as not reviewed when present, and if the plan reports truncated: true your summary must state the review is incomplete and your verdict must not be "approve".',
    "4. Copy the successful units' findings into your findings array verbatim. The host merges the delegated findings mechanically (deduplicating shared anchors keeping the most severe, ranking blocking > important > nit, capping at 20) — findings you invent or edit are discarded, and findings you drop are restored.",
    '5. Then return ONLY a JSON object — no Markdown fences, no prose before or after — exactly this shape: {"summary": <string, 1-3 paragraphs of overall assessment, including every unreviewed unit or file>, "verdict": <"approve" | "comment" | "request-changes">, "findings": [{"path": <string>, "startLine": <integer>, "endLine": <integer>, "severity": <"blocking" | "important" | "nit">, "title": <string, <= 120 chars>, "body": <string>, "suggestion": <string, OPTIONAL>}]}.',
    'Use verdict "request-changes" only when at least one finding is "blocking" (the host enforces this rule on the merged findings). An empty findings array with verdict "approve" is a valid review when every unit succeeded and found nothing.',
  ].join("\n");

export const FanOutReviewer = Agent.define("pr-fanout-reviewer", {
  input: ReviewMission,
  output: CodeReview,
  instructions: fanOutReviewInstructions,
  toolkit: FanOutReviewToolkit,
  policy: AgentPolicy.make({
    maxTurns: 6,
    maxToolCalls: 1 + MAX_REVIEW_UNITS,
    maxDuration: "15 minutes",
    toolConcurrency: 3,
    tokenBudget: 300_000,
  }),
  description:
    "Coordinate one pull-request review by fanning bounded per-unit file reviews out to delegated children and merging their findings into one structured review.",
  metadata: { deploymentClass: "E", surface: "read-only", delegation: "S1-attached" },
});

// ---------------------------------------------------------------------------
// Host-side finalization: the coordinator model's findings array is a
// proposal only. The published review carries exactly the mechanically
// merged, host-collected child findings, and the verdict is re-derived from
// them — a coordinator can neither invent a finding, drop a blocking one,
// nor approve past one.
// ---------------------------------------------------------------------------

/** Deterministic merge of the collected child findings into the final review. */
export const finalizeFanOutReview = (
  review: CodeReview,
  collected: ReadonlyArray<ReviewFinding>,
): CodeReview => {
  const findings = rankAndDedupeFindings(collected);
  const hasBlocking = findings.some((finding) => finding.severity === "blocking");
  const verdict: ReviewVerdict = hasBlocking
    ? "request-changes"
    : review.verdict === "request-changes"
      ? "comment"
      : review.verdict;
  return CodeReview.make({ summary: review.summary, verdict, findings });
};

/**
 * One fan-out review, end to end: the shared wiring the CLI and the offline
 * tests both use. It owns the per-run findings collector, provides the
 * coordinator toolkit and the delegation handler Layer (children observing
 * the source only through `fanOutScopedSourceLayer`), and finalizes the
 * decoded review against the collected child findings before publication
 * planning. Source, publisher, model Layers, identifiers, and the
 * reservation service stay caller-provided, exactly like `executeReview`.
 */
export const executeFanOutReview = <
  ParentInstructions,
  ParentTools extends Record<string, Tool.Any>,
  ParentProvider,
  ParentModelProvides,
  ParentModelRequires,
  ChildProvider,
  ChildModelProvides,
  ChildModelRequires,
>(
  parentBinding: RuntimeBinding<
    typeof ReviewMission,
    typeof CodeReview,
    ParentInstructions,
    ParentTools,
    ParentProvider,
    ParentModelProvides,
    ParentModelRequires
  >,
  childBinding: RuntimeBinding<
    typeof FileReviewBrief,
    typeof FileReviewReport,
    typeof fileReviewerInstructions,
    Toolkit.Tools<typeof FileReviewToolkit>,
    ChildProvider,
    ChildModelProvides,
    ChildModelRequires
  >,
  options: Omit<ExecuteReviewOptions, "finalizeReview">,
) =>
  Effect.gen(function* () {
    const outcomes = yield* makeFanOutUnitOutcomes;
    return yield* executeReview(parentBinding, {
      ...options,
      limits: options.limits ?? fanOutReviewBudgetLimits,
      finalizeReview: (review) =>
        outcomes.collected.pipe(Effect.map((collected) => finalizeFanOutReview(review, collected))),
    }).pipe(
      Effect.provide(
        Layer.mergeAll(
          FanOutCoordinatorToolkitLayer,
          fanOutHandlersLayer(childBinding).pipe(
            Layer.provide(
              Layer.mergeAll(
                FileReviewToolkitLayer.pipe(Layer.provide(fanOutScopedSourceLayer)),
                Layer.succeed(FanOutUnitOutcomes)(outcomes),
              ),
            ),
          ),
        ),
      ),
    );
  });

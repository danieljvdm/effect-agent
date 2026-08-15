import { Effect, Schema } from "effect";
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
} from "./review-agent.ts";
import {
  MAX_REVIEW_UNITS,
  MAX_UNIT_FILES,
  planReviewUnits,
  ReviewUnitId,
  ReviewUnitPlan,
} from "./review-units.ts";
import { PullRequestSource, PullRequestSourceFailure } from "./source.ts";

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
// changeset listing — so a child can never roam beyond its briefed unit
// despite its observation surface being the whole changeset port.
// ---------------------------------------------------------------------------

export const FileReviewToolkit = Toolkit.make(ReadFileDiff, ReadFile);

export const FileReviewToolkitLayer = FileReviewToolkit.toLayer({
  read_file_diff: readFileDiffHandler,
  read_file: readFileHandler,
});

const UnitPaths = Schema.Array(ChangedPath)
  .check(Schema.isMinLength(1))
  .check(Schema.isMaxLength(MAX_UNIT_FILES));

/** The child Agent input: one briefed unit of the changeset. */
export class FileReviewBrief extends Schema.Class<FileReviewBrief>(
  "@effect-agent/pr-review/FileReviewBrief",
)({
  unitId: ReviewUnitId,
  paths: UnitPaths,
  focus: Schema.NonEmptyString.check(Schema.isMaxLength(200)),
}) {}

/** The child Agent output: the briefed unit's bounded findings. */
export class FileReviewReport extends Schema.Class<FileReviewReport>(
  "@effect-agent/pr-review/FileReviewReport",
)({
  unitId: ReviewUnitId,
  findings: Schema.Array(ReviewFinding).check(Schema.isMaxLength(MAX_CHILD_FINDINGS)),
}) {}

/**
 * Guidance for delegated children must be static: child instructions are a
 * pure function of the brief, and the coordinator's mission never crosses the
 * delegation boundary (context isolation), so mission-dependent guidance
 * cannot be resolved for a child.
 */
export interface FanOutInstructionOptions {
  readonly guidance?: string | ReadonlyArray<string> | undefined;
}

const staticGuidanceLines = (
  guidance: string | ReadonlyArray<string> | undefined,
): ReadonlyArray<string> => {
  if (guidance === undefined) return [];
  const lines = typeof guidance === "string" ? [guidance] : guidance;
  return lines.filter((line) => line.length > 0);
};

/** Build the child file-reviewer instructions with optional static guidance. */
export const makeFileReviewerInstructions =
  (options: FanOutInstructionOptions = {}) =>
  (brief: FileReviewBrief): string =>
    [
      `You are a code reviewer for one unit of a pull request: unit ${brief.unitId}, covering exactly these changed files: ${brief.paths.join(", ")}. Focus: ${brief.focus}.`,
      ...staticGuidanceLines(options.guidance),
      "Work in this order:",
      "1. Call read_file_diff for every file in your unit. In its output, only lines marked R<number> exist in the new version; those numbers are the only valid values for startLine and endLine. Never anchor a finding to a removed (-) line.",
      "2. Call read_file when you need surrounding context the diff does not show. ONLY files in the changeset are readable: a request for any other path (an import, a neighbor, a config) returns a failed result — do not retry it; reason from the diff instead and note the gap in your report when it matters.",
      "3. Review for real defects first: correctness, security, concurrency, resource leaks, error handling, API misuse. Style nits are least important. Do not praise; do not restate the diff.",
      `4. Then return ONLY a JSON object — no Markdown fences, no prose before or after — exactly this shape: {"unitId": ${JSON.stringify(brief.unitId)}, "findings": [{"path": <string, a file in your unit>, "startLine": <integer, an R-marked new-file line>, "endLine": <integer, >= startLine, same file, R-marked>, "severity": <"blocking" | "important" | "nit">, "title": <string, <= 120 chars>, "body": <string, why it matters and what to do>, "suggestion": <string, OPTIONAL: replacement text for exactly lines startLine..endLine, ready to commit>}]}.`,
      `Report at most ${MAX_CHILD_FINDINGS} findings; prefer the most important ones. An empty findings array is a valid report. Never report on files outside your unit. Line anchors you invent will be discarded, so copy R-numbers from read_file_diff output.`,
    ].join("\n");

export const fileReviewerInstructions = makeFileReviewerInstructions();

/** The default per-unit child execution bounds. */
export const defaultFileReviewerPolicy = AgentPolicy.make({
  maxTurns: 8,
  maxToolCalls: 16,
  maxDuration: "4 minutes",
  toolConcurrency: 2,
  tokenBudget: 200_000,
});

// ---------------------------------------------------------------------------
// The delegation: one Effect AI Tool per review unit, with explicit
// projections and finite bounds (SUB-009). `projectResult` is the
// declassification boundary — the parent sees the child's bounded findings,
// never its transcript or the diffs it read.
// ---------------------------------------------------------------------------

/** The model-decoded delegation parameters: which unit to review. */
export class FileReviewRequest extends Schema.Class<FileReviewRequest>(
  "@effect-agent/pr-review/FileReviewRequest",
)({
  unitId: ReviewUnitId,
  paths: UnitPaths,
}) {}

/** The bounded parent-visible result of one delegated unit review. */
export class FileReviewUnitResult extends Schema.Class<FileReviewUnitResult>(
  "@effect-agent/pr-review/FileReviewUnitResult",
)({
  unitId: ReviewUnitId,
  findings: Schema.Array(ReviewFinding).check(Schema.isMaxLength(MAX_CHILD_FINDINGS)),
}) {}

/**
 * One unit's review failed: the child Run ended in a typed failure (policy
 * bound, output violation, model fault). The marker is bounded and carries no
 * child transcript content beyond the failure tag and message.
 */
export class FileReviewUnitFailed extends Schema.TaggedError<FileReviewUnitFailed>()(
  "FileReviewUnitFailed",
  {
    childErrorTag: Schema.NonEmptyString.check(Schema.isMaxLength(256)),
    message: Schema.String.check(Schema.isMaxLength(400)),
  },
) {}

/**
 * Finite per-invocation bounds (SUB-009), aligned with the child's own
 * AgentPolicy: the child's policy is the limit that trips typed; the
 * reservation mirrors it so parent-side accounting stays honest.
 */
export const fileReviewPolicy = SubagentPolicy.make({
  maxChildren: MAX_REVIEW_UNITS,
  maxConcurrency: 3,
  maxTurns: 8,
  maxToolCalls: 16,
  maxDuration: "4 minutes",
});

const delegationDescription =
  "Delegate the review of one planned unit to a bounded file-reviewer child and return its line-anchored findings. Call it exactly once per unit from list_review_units; never retry a failed unit.";

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
// declared failure union) instead of aborting the Run.
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
  "@effect-agent/pr-review/ListReviewUnitsQuery",
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
    "2. Call delegate_file_review EXACTLY once per unit, passing each unit's unitId and paths verbatim. Prefer declaring all delegation calls in one batch. Never review files yourself and never invent units.",
    '3. A delegation result with "_tag" is a FAILED unit. Never retry it; instead your summary MUST name it honestly, e.g. "unit-002 unreviewed: AgentPolicyError". The plan\'s undiffablePaths and unassignedPaths must also be named as not reviewed when present.',
    "4. Merge the successful units' findings: drop duplicates sharing the same path and line range keeping the most severe, rank blocking > important > nit, and keep at most 20 findings.",
    '5. Then return ONLY a JSON object — no Markdown fences, no prose before or after — exactly this shape: {"summary": <string, 1-3 paragraphs of overall assessment, including every unreviewed unit or file>, "verdict": <"approve" | "comment" | "request-changes">, "findings": [{"path": <string>, "startLine": <integer>, "endLine": <integer>, "severity": <"blocking" | "important" | "nit">, "title": <string, <= 120 chars>, "body": <string>, "suggestion": <string, OPTIONAL>}]}. Copy findings verbatim from the delegation results; never invent or edit anchors.',
    'Use verdict "request-changes" only when at least one finding is "blocking". An empty findings array with verdict "approve" is a valid review when every unit succeeded and found nothing.',
  ].join("\n");

/** The default fan-out coordinator execution bounds. */
export const defaultFanOutPolicy = AgentPolicy.make({
  maxTurns: 6,
  maxToolCalls: 1 + MAX_REVIEW_UNITS,
  maxDuration: "15 minutes",
  toolConcurrency: 3,
  // One declared batch may legitimately contain a failed result for every
  // review unit. Leave the coordinator one turn to report all of them, while
  // still stopping a model that declares another failed delegation.
  repeatedFailureLimit: MAX_REVIEW_UNITS + 1,
  tokenBudget: 300_000,
});

/** Everything one fan-out configuration is made of, built as one unit so the
 * delegation always targets exactly the child definition that will run. */
export interface FanOutReviewSuite {
  readonly child: ReturnType<typeof makeFileReviewerDefinition>;
  readonly parent: ReturnType<typeof makeFanOutReviewerDefinition>;
  readonly delegation: ReturnType<typeof makeFileReviewDelegation>;
}

const makeFileReviewerDefinition = (options: FanOutInstructionOptions = {}) =>
  Agent.define("pr-file-reviewer", {
    input: FileReviewBrief,
    output: FileReviewReport,
    instructions: makeFileReviewerInstructions(options),
    toolkit: FileReviewToolkit,
    policy: defaultFileReviewerPolicy,
    description:
      "Review one bounded unit of a pull request's changeset read-only and return line-anchored findings for exactly those files.",
    metadata: { deploymentClass: "E", surface: "read-only" },
  });

const makeFanOutReviewerDefinition = () =>
  Agent.define("pr-fanout-reviewer", {
    input: ReviewMission,
    output: CodeReview,
    instructions: fanOutReviewInstructions,
    toolkit: FanOutReviewToolkit,
    policy: defaultFanOutPolicy,
    description:
      "Coordinate one pull-request review by fanning bounded per-unit file reviews out to delegated children and merging their findings into one structured review.",
    metadata: { deploymentClass: "E", surface: "read-only", delegation: "S1-attached" },
  });

const makeFileReviewDelegation = (child: ReturnType<typeof makeFileReviewerDefinition>) =>
  Subagent.define("delegate_file_review", {
    description: delegationDescription,
    target: child,
    parameters: FileReviewRequest,
    success: FileReviewUnitResult,
    failure: FileReviewUnitFailed,
    prepareInput: (request) =>
      Effect.succeed(
        FileReviewBrief.make({
          unitId: request.unitId,
          paths: request.paths,
          focus: "defects-first: correctness, security, concurrency, resources, error handling",
        }),
      ),
    // The explicit declassification boundary (SUB-015): exactly the bounded
    // findings cross to the parent. Whether they may anchor anywhere is
    // decided host-side by planPublication against the real diff, not here.
    projectResult: (report) =>
      Effect.succeed(
        FileReviewUnitResult.make({
          unitId: report.unitId,
          findings: report.findings,
        }),
      ),
    policy: fileReviewPolicy,
  });

/** Build one coherent fan-out suite: child, coordinator, and delegation. */
export const makeFanOutReviewSuite = (
  options: FanOutInstructionOptions = {},
): FanOutReviewSuite => {
  const child = makeFileReviewerDefinition(options);
  return {
    child,
    parent: makeFanOutReviewerDefinition(),
    delegation: makeFileReviewDelegation(child),
  };
};

const defaultSuite = makeFanOutReviewSuite();

/** The default child Agent Definition. */
export const FileReviewer = defaultSuite.child;

/** The default coordinator Agent Definition. */
export const FanOutReviewer = defaultSuite.parent;

/** The default delegation over the default child. */
export const fileReviewDelegation = defaultSuite.delegation;

/** Runtime wiring: one delegation plus one explicit child Binding. */
export const fanOutHandlersLayerFor =
  (delegation: ReturnType<typeof makeFileReviewDelegation>) =>
  <Provider, ModelProvides, ModelRequires>(
    childBinding: RuntimeBinding<
      typeof FileReviewBrief,
      typeof FileReviewReport,
      ReturnType<typeof makeFileReviewerInstructions>,
      Toolkit.Tools<typeof FileReviewToolkit>,
      Provider,
      ModelProvides,
      ModelRequires
    >,
  ) =>
    SubagentRuntime.layer(delegation, childBinding, {
      mapChildFailure: mapFileReviewChildFailure,
    });

/** Runtime wiring over the default delegation, mirroring the leaf example. */
export const fanOutHandlersLayer = fanOutHandlersLayerFor(fileReviewDelegation);

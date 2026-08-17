import { Effect, Schema } from "effect";
import {
  Agent,
  AgentPolicy,
  PolicyLimit,
  Subagent,
  SubagentPolicy,
  SubagentRuntime,
  ToolExecutionClass,
  ToolResultBounds,
  type RuntimeBinding,
} from "effect-agent";
import { Tool, Toolkit } from "effect/unstable/ai";

import { ChangedPath } from "./diff.ts";
import {
  clampMaxFindings,
  CodeReview,
  MAX_CONCERNS,
  MAX_WALKTHROUGH_SUMMARY_CHARS,
  ReadFile,
  ReadFileDiff,
  readFileDiffHandler,
  readFileHandler,
  REVIEW_EVENT_REPLAY_LIMIT,
  REVIEW_TOOL_RESULT_MAX_BYTES,
  ReviewConcern,
  ReviewFinding,
  ReviewMission,
  WalkthroughEntry,
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
// one initial `delegate_file_review` call per unit, optionally retries one
// bounded wave of failed read-only units, then merges the children's bounded
// findings into one `CodeReview`. Publication and anchor validation are
// unchanged: child output is untrusted input like everything else and crosses
// to the host only through the same fail-closed planPublication path.
// ---------------------------------------------------------------------------

/** One child returns at most this many findings; the merge caps the total. */
export const MAX_CHILD_FINDINGS = 8;

/** One child returns at most this many non-anchored concerns. */
export const MAX_CHILD_CONCERNS = 3;

/**
 * One mandatory diff read plus at most three bounded context reads for every
 * path in a maximum-size unit. The reviewer can need distinct focused reads
 * for the producer and consumer sides of a changed contract; keep the child
 * and delegation reservation aligned rather than making that ordinary work
 * look like a policy failure.
 */
export const MAX_FILE_REVIEW_TOOL_CALLS = MAX_UNIT_FILES * 4;

/**
 * A child may serialize every permitted read across model turns, then needs
 * one final turn to return its structured report. Keep this independent bound
 * aligned with the full read budget: a valid maximum-size review must not
 * fail merely because the provider did not batch calls.
 */
export const MAX_FILE_REVIEW_TURNS = MAX_FILE_REVIEW_TOOL_CALLS + 1;

/** Parent-side concurrent child permits. */
export const FILE_REVIEW_MAX_CONCURRENCY = 5;

/** One bounded retry wave for failed read-only review units. */
export const MAX_FILE_REVIEW_RETRIES = FILE_REVIEW_MAX_CONCURRENCY;

/** Initial unit attempts plus the single bounded retry wave. */
export const MAX_FILE_REVIEW_ATTEMPTS = MAX_REVIEW_UNITS + MAX_FILE_REVIEW_RETRIES;

/** Maximum wall-clock allowance for one attached file-review child. */
export const FILE_REVIEW_MAX_DURATION_MINUTES = 8;

/**
 * A maximum-size audit schedules every initial unit plus one bounded retry
 * wave. The coordinator must remain alive for all waves, then have time to
 * merge the reports into its terminal review.
 */
export const MAX_FILE_REVIEW_WAVES = Math.ceil(
  MAX_FILE_REVIEW_ATTEMPTS / FILE_REVIEW_MAX_CONCURRENCY,
);
export const FILE_REVIEW_WAVE_DURATION_MINUTES =
  MAX_FILE_REVIEW_WAVES * FILE_REVIEW_MAX_DURATION_MINUTES;
export const FAN_OUT_COORDINATOR_MERGE_HEADROOM_MINUTES = 10;
export const FAN_OUT_MAX_DURATION_MINUTES =
  FILE_REVIEW_WAVE_DURATION_MINUTES + FAN_OUT_COORDINATOR_MERGE_HEADROOM_MINUTES;
/** GitHub Actions job timeout, including a bounded runner-cleanup allowance. */
export const FAN_OUT_WORKFLOW_TIMEOUT_MINUTES = FAN_OUT_MAX_DURATION_MINUTES + 5;

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

/** The child Agent output: the briefed unit's bounded findings and concerns. */
export class FileReviewReport extends Schema.Class<FileReviewReport>(
  "@effect-agent/pr-review/FileReviewReport",
)({
  unitId: ReviewUnitId,
  findings: Schema.Array(ReviewFinding).check(Schema.isMaxLength(MAX_CHILD_FINDINGS)),
  /** Unit-scoped concerns with no diff line to anchor to. */
  concerns: Schema.optionalKey(
    Schema.Array(ReviewConcern).check(Schema.isMaxLength(MAX_CHILD_CONCERNS)),
  ),
  /** One-sentence per-file change summaries for the merged walkthrough. */
  fileSummaries: Schema.optionalKey(
    Schema.Array(WalkthroughEntry).check(Schema.isMaxLength(MAX_UNIT_FILES)),
  ),
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
      "1. Call read_file_diff for every file in your unit. A normal diff marks new-version anchors as R<number>; only those numbers are valid startLine/endLine values. When GitHub omitted a diff, the tool may return bounded base/head content marked B/H instead. Review that content, but report its defects as non-anchored concerns because B/H lines cannot anchor GitHub comments. Never anchor a finding to a removed (-), B, or H line.",
      "2. Call read_file when you need surrounding context the diff does not show, at most three times per file. ONLY files in the changeset are readable: a request for any other path (an import, a neighbor, a config) returns a failed result — do not retry it; reason from the diff instead and note the gap in your report when it matters.",
      "3. Review for real defects first: correctness, security, concurrency, resource leaks, error handling, API misuse. Style nits are least important. Do not praise; do not restate the diff.",
      "When the diff adds or changes a test, check that it can actually fail: a test that would still pass with the bug present is theatre, not coverage. The usual tell is a loose assertion standing where an exact one belongs — >= or a truthiness check over an expected value, or a snapshot that absorbs whatever it is handed.",
      "Drop bloat-shaped findings before reporting: defensive checks for cases that cannot happen, abstractions used once, comments restating obvious code, tests asserting tautologies, just-in-case guards. A finding must be sound, correct, and worth acting on; prefer an explicit keep over an invented finding.",
      `4. For every file in your unit, write one factual sentence (<= ${MAX_WALKTHROUGH_SUMMARY_CHARS} chars) describing what changed in that file — for a reader scanning the pull request, never a line-by-line restatement.`,
      `5. Then return ONLY a JSON object — no Markdown fences, no prose before or after — exactly this shape: {"unitId": ${JSON.stringify(brief.unitId)}, "findings": [{"path": <string, a file in your unit>, "startLine": <integer, an R-marked new-file line>, "endLine": <integer, >= startLine, same file, R-marked>, "severity": <"blocking" | "important" | "nit">, "category": <OPTIONAL: "correctness" | "security" | "concurrency" | "performance" | "resources" | "error-handling" | "testing" | "maintainability" | "style" | "docs">, "title": <string, <= 120 chars>, "body": <string, why it matters and what to do>, "suggestion": <string, OPTIONAL: replacement text for exactly lines startLine..endLine, ready to commit>}], "concerns": <array, OPTIONAL: [{"severity": <"blocking" | "important" | "nit">, "title": <string, <= 120 chars>, "body": <string>}], only for concerns about YOUR unit's files with no valid line anchor (a missing cleanup, a coverage gap the diff implies, sequencing the diff leaves open) — never duplicate a finding here>, "fileSummaries": <array, OPTIONAL: [{"path": <string, a file in your unit>, "summary": <string, the step-4 sentence>}], one entry per file in your unit>}.`,
      `Report at most ${MAX_CHILD_FINDINGS} findings and at most ${MAX_CHILD_CONCERNS} concerns; prefer the most important ones. An empty findings array is a valid report. Never report on files outside your unit. Line anchors you invent will be discarded, so copy R-numbers from read_file_diff output.`,
    ].join("\n");

export const fileReviewerInstructions = makeFileReviewerInstructions();

/** The default per-unit child execution bounds. */
export const defaultFileReviewerPolicy = AgentPolicy.make({
  maxTurns: MAX_FILE_REVIEW_TURNS,
  maxToolCalls: MAX_FILE_REVIEW_TOOL_CALLS,
  maxDuration: `${FILE_REVIEW_MAX_DURATION_MINUTES} minutes`,
  toolConcurrency: 2,
  // Same rationale as the flat reviewer's bound: read refusals are
  // model-visible results, and one parallel batch of out-of-unit probes must
  // not kill the child before it has seen a single refusal.
  repeatedFailureLimit: 12,
  tokenBudget: 200_000,
  // Bound one live prompt independently from cumulative usage. The engine
  // prunes old diff/file results before paying for a summary.
  contextTokenLimit: 150_000,
  toolResultBounds: ToolResultBounds.make({ maxBytes: REVIEW_TOOL_RESULT_MAX_BYTES }),
  // Typed exhaustion, deliberately NOT the final-answer soft landing: a
  // review is a coverage claim, and a child whose reads were rejected could
  // still emit schema-valid findings — laundering budget exhaustion into
  // "reviewed". Until host-owned evidence proves every mandatory
  // read_file_diff completed, an exhausted child fails typed and its unit
  // stays honestly unreviewed (containment turns that into result data
  // without failing the run).
  onExhaustion: "fail",
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
  /** Unit-scoped concerns with no diff line to anchor to. */
  concerns: Schema.optionalKey(
    Schema.Array(ReviewConcern).check(Schema.isMaxLength(MAX_CHILD_CONCERNS)),
  ),
  /** One-sentence per-file change summaries for the merged walkthrough. */
  fileSummaries: Schema.optionalKey(
    Schema.Array(WalkthroughEntry).check(Schema.isMaxLength(MAX_UNIT_FILES)),
  ),
}) {}

/**
 * One unit's review failed: the child Run ended in a typed failure (policy
 * bound, output violation, model fault). The marker is bounded and carries no
 * child transcript content beyond the failure tag, its finite policy dimension
 * when applicable, and a bounded message.
 */
export class FileReviewUnitFailed extends Schema.TaggedError<FileReviewUnitFailed>()(
  "FileReviewUnitFailed",
  {
    childErrorTag: Schema.NonEmptyString.check(Schema.isMaxLength(256)),
    /** A finite, non-sensitive exhaustion dimension when the child hit policy. */
    childPolicyLimit: Schema.optionalKey(PolicyLimit),
    message: Schema.String.check(Schema.isMaxLength(400)),
  },
) {}

/**
 * Finite per-invocation bounds (SUB-009), aligned with the child's own
 * AgentPolicy: the child's policy is the limit that trips typed; the
 * reservation mirrors it so parent-side accounting stays honest.
 */
export const fileReviewPolicy = SubagentPolicy.make({
  maxChildren: MAX_FILE_REVIEW_ATTEMPTS,
  maxConcurrency: FILE_REVIEW_MAX_CONCURRENCY,
  maxTurns: MAX_FILE_REVIEW_TURNS,
  maxToolCalls: MAX_FILE_REVIEW_TOOL_CALLS,
  maxDuration: `${FILE_REVIEW_MAX_DURATION_MINUTES} minutes`,
});

const delegationDescription = `Delegate one planned unit to a bounded read-only file reviewer. Call every unit once; after those settle, at most ${MAX_FILE_REVIEW_RETRIES} failed units may each be retried once with the exact same paths.`;

/**
 * Total mapping from every expected child Run failure to the declared unit
 * failure (SUB-028): the tag plus a bounded message cross. Policy exhaustion
 * additionally retains only its finite dimension, never child trace data.
 */
export const mapFileReviewChildFailure = (failure: {
  readonly _tag: string;
  readonly limit?: PolicyLimit;
  readonly message?: string;
}): FileReviewUnitFailed =>
  FileReviewUnitFailed.make({
    childErrorTag: failure._tag,
    ...(failure._tag === "AgentPolicyError" && failure.limit !== undefined
      ? { childPolicyLimit: failure.limit }
      : {}),
    message: (failure.message ?? "").slice(0, 400),
  });

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

/**
 * Build the coordinator's instructions. The same consumer guidance the
 * children receive is injected between the mission framing and the procedure
 * so the merged summary and verdict are shaped by the same review profile,
 * and the configured findings bound reaches the merge step instead of only
 * the host-side trim.
 */
export const makeFanOutReviewInstructions =
  (options: FanOutInstructionOptions & { readonly maxFindings?: number | undefined } = {}) =>
  (mission: ReviewMission): string => {
    const maxFindings = clampMaxFindings(options.maxFindings);
    return [
      `You coordinate the review of pull request #${mission.number} ("${mission.title}") in ${mission.repository}, merging ${mission.headRef} into ${mission.baseRef}. It changes ${mission.changedFileCount} file(s).`,
      mission.body.length > 0
        ? `Author description:\n${mission.body}`
        : "The author provided no description.",
      ...staticGuidanceLines(options.guidance),
      "Work in this order:",
      "1. Call list_review_units once to get the planned review units.",
      "2. Call delegate_file_review EXACTLY once per unit, passing each unit's unitId and paths verbatim. Prefer declaring all initial delegation calls in one batch. Never review files yourself and never invent units.",
      `3. After every initial call settles, you MUST retry the first ${MAX_FILE_REVIEW_RETRIES} FAILED units once, in unit order, with the exact same unitId and paths (or every failed unit when fewer than ${MAX_FILE_REVIEW_RETRIES} failed). Retrying is allowed only because this child surface is read-only. Never retry a successful unit or retry any unit more than once.`,
      '4. A unit whose retry also returns an "_tag", or which was not eligible for the bounded retry wave, remains FAILED. Your summary MUST name it honestly, e.g. "unit-002 unreviewed: AgentPolicyError". The plan\'s undiffablePaths and unassignedPaths must also be named as not reviewed when present.',
      `5. Merge the successful units' findings: drop duplicates sharing the same path and line range keeping the most severe, rank blocking > important > nit, and keep at most ${maxFindings} findings. Drop bloat-shaped findings during the merge — defensive checks for cases that cannot happen, abstractions used once, comments restating obvious code, tests asserting tautologies; children bias toward recommending changes, and a finding must be sound, correct, and worth acting on to survive.`,
      `6. Merge the units' concerns the same way: drop duplicates keeping the most severe, and keep at most ${MAX_CONCERNS}.`,
      "7. Merge the units' fileSummaries into one walkthrough: copy each entry verbatim, one entry per file, dropping duplicate paths.",
      '8. Then return ONLY a JSON object — no Markdown fences, no prose before or after — exactly this shape: {"summary": <string, 1-3 paragraphs of overall assessment, including every unreviewed unit or file>, "verdict": <"approve" | "comment" | "request-changes">, "findings": [{"path": <string>, "startLine": <integer>, "endLine": <integer>, "severity": <"blocking" | "important" | "nit">, "category": <string, OPTIONAL>, "title": <string, <= 120 chars>, "body": <string>, "suggestion": <string, OPTIONAL>}], "concerns": <array, OPTIONAL: [{"severity": <"blocking" | "important" | "nit">, "title": <string, <= 120 chars>, "body": <string>}], the merged unit concerns>, "walkthrough": <array, OPTIONAL: [{"path": <string>, "summary": <string>}], the merged fileSummaries>}. Copy findings (including "category" and "suggestion" when present), concerns, and walkthrough entries verbatim from the delegation results; never invent or edit anchors.',
      'Use verdict "request-changes" only when at least one finding or concern is "blocking". An empty findings array with verdict "approve" is a valid review when every unit succeeded and found nothing.',
    ].join("\n");
  };

export const fanOutReviewInstructions = makeFanOutReviewInstructions();

/** The default fan-out coordinator execution bounds. */
export const defaultFanOutPolicy = AgentPolicy.make({
  maxTurns: 6,
  maxToolCalls: 1 + MAX_FILE_REVIEW_ATTEMPTS,
  // 20 initial units plus one five-unit retry wave, followed by the
  // coordinator's bounded merge and final response window.
  maxDuration: `${FAN_OUT_MAX_DURATION_MINUTES} minutes`,
  toolConcurrency: FILE_REVIEW_MAX_CONCURRENCY,
  // Contained unit failures (SUB-033) are ordinary successful Tool results,
  // so they no longer fold into the repeated-failure counter; the default
  // bound suffices.
  repeatedFailureLimit: 3,
  tokenBudget: 300_000,
  // Child reports can amplify the merge prompt; compact before the provider's
  // 200k-class window becomes the failure boundary.
  contextTokenLimit: 150_000,
  // Budget soft landing (RUN-018): an exhausted coordinator merges what it
  // has into one best-effort review instead of discarding every child report.
  onExhaustion: "final-answer",
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

/** Options for one coherent fan-out suite: shared guidance plus the merge bound. */
export interface FanOutSuiteOptions extends FanOutInstructionOptions {
  readonly maxFindings?: number | undefined;
}

const makeFileReviewDelegation = (child: ReturnType<typeof makeFileReviewerDefinition>) =>
  Subagent.define("delegate_file_review", {
    description: delegationDescription,
    target: child,
    parameters: FileReviewRequest,
    success: FileReviewUnitResult,
    failure: FileReviewUnitFailed,
    // First-party containment (SUB-033): a failed unit is model-visible
    // result data instead of a parent-Run-fatal error, so the coordinator
    // reports it honestly and keeps reviewing the other units. This retires
    // the former same-name shadow-Tool workaround (FRICTION #7).
    failureMode: "return",
    prepareInput: (request) =>
      Effect.succeed(
        FileReviewBrief.make({
          unitId: request.unitId,
          paths: request.paths,
          focus: "defects-first: correctness, security, concurrency, resources, error handling",
        }),
      ),
    // The explicit declassification boundary (SUB-015): exactly the bounded
    // findings and concerns cross to the parent. Whether findings may anchor
    // anywhere is decided host-side by planPublication against the real diff.
    projectResult: (report) =>
      Effect.succeed(
        FileReviewUnitResult.make({
          unitId: report.unitId,
          findings: report.findings,
          ...(report.concerns !== undefined ? { concerns: report.concerns } : {}),
          ...(report.fileSummaries !== undefined ? { fileSummaries: report.fileSummaries } : {}),
        }),
      ),
    policy: fileReviewPolicy,
  });

/**
 * The coordinator-facing delegation Tool: the delegation's own first-party
 * contained Tool plus the read-only execution class (the delegated child's
 * whole tool surface is read-only). Effect AI resolves handlers by Tool name,
 * so `SubagentRuntime.layer`'s handler serves this annotated copy unchanged.
 */
const delegationToolFor = (delegation: ReturnType<typeof makeFileReviewDelegation>) =>
  delegation.tool.annotate(ToolExecutionClass, "readonly");

const makeFanOutReviewerDefinition = (
  options: FanOutSuiteOptions,
  delegation: ReturnType<typeof makeFileReviewDelegation>,
) =>
  Agent.define("pr-fanout-reviewer", {
    input: ReviewMission,
    output: CodeReview,
    instructions: makeFanOutReviewInstructions(options),
    toolkit: Toolkit.make(ListReviewUnits, delegationToolFor(delegation)),
    policy: defaultFanOutPolicy,
    description:
      "Coordinate one pull-request review by fanning bounded per-unit file reviews out to delegated children and merging their findings into one structured review.",
    metadata: { deploymentClass: "E", surface: "read-only", delegation: "S1-attached" },
  });

/** Build one coherent fan-out suite: child, coordinator, and delegation. */
export const makeFanOutReviewSuite = (options: FanOutSuiteOptions = {}): FanOutReviewSuite => {
  const child = makeFileReviewerDefinition({ guidance: options.guidance });
  const delegation = makeFileReviewDelegation(child);
  return {
    child,
    parent: makeFanOutReviewerDefinition(options, delegation),
    delegation,
  };
};

const defaultSuite = makeFanOutReviewSuite();

/** The default child Agent Definition. */
export const FileReviewer = defaultSuite.child;

/** The default coordinator Agent Definition. */
export const FanOutReviewer = defaultSuite.parent;

/** The default delegation over the default child. */
export const fileReviewDelegation = defaultSuite.delegation;

/** The default coordinator-facing delegation Tool (first-party contained mode). */
export const DelegateFileReview = delegationToolFor(fileReviewDelegation);

/** The default coordinator Toolkit. */
export const FanOutReviewToolkit = FanOutReviewer.toolkit;

/**
 * The contained failure family the delegation can surface as result data
 * (SUB-033), derived from the delegation itself so the coverage decoder can
 * never diverge from what the runtime actually contains.
 */
export const FileReviewDelegationFailure = fileReviewDelegation.containedFailure;

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
      child: { maxReplayEvents: REVIEW_EVENT_REPLAY_LIMIT },
    });

/** Runtime wiring over the default delegation, mirroring the leaf example. */
export const fanOutHandlersLayer = fanOutHandlersLayerFor(fileReviewDelegation);

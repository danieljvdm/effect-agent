import { Effect, Schema } from "effect";
import { Agent, AgentPolicy, ToolExecutionClass } from "effect-agent";
import { Tool, Toolkit } from "effect/unstable/ai";

import { annotatePatch, ChangedFileStatus, ChangedPath } from "./diff.ts";
import {
  normalizeRepoRelativePath,
  PullRequestSource,
  PullRequestSourceFailure,
  ReviewInputViolation,
} from "./source.ts";

// ---------------------------------------------------------------------------
// The pull-request reviewer: a bounded, read-only agent. Every tool observes
// the pull request through the PullRequestSource port; nothing the model can
// call mutates anything. Publishing the review happens OUTSIDE the agent
// loop, after the finding anchors have been validated against the real diff
// (model output is untrusted input, AGENTS.md rule 11).
// ---------------------------------------------------------------------------

/** The hard findings bound carried by the CodeReview schema. */
export const MAX_FINDINGS = 20;

/** The hard non-anchored-concerns bound carried by the CodeReview schema. */
export const MAX_CONCERNS = 10;

/** Annotated patches larger than this are truncated with an explicit marker. */
const MAX_PATCH_CHARS = 60_000;

/** One `read_file` slice never exceeds this many lines. */
const MAX_SLICE_LINES = 1_000;
const DEFAULT_SLICE_LINES = 400;

// ---------------------------------------------------------------------------
// Tool surface.
// ---------------------------------------------------------------------------

export class ChangedFileSummary extends Schema.Class<ChangedFileSummary>(
  "@effect-agent/pr-review/ChangedFileSummary",
)({
  path: ChangedPath,
  status: ChangedFileStatus,
  additions: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  deletions: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  hasTextualDiff: Schema.Boolean,
}) {}

export class ChangedFilesView extends Schema.Class<ChangedFilesView>(
  "@effect-agent/pr-review/ChangedFilesView",
)({
  totalFiles: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  /** True when the pull request has more changed files than are listed here. */
  truncated: Schema.Boolean,
  files: Schema.Array(ChangedFileSummary).check(Schema.isMaxLength(300)),
}) {}

export class ListChangedFilesQuery extends Schema.Class<ListChangedFilesQuery>(
  "@effect-agent/pr-review/ListChangedFilesQuery",
)({
  /** Explicit constant keeps the zero-choice operation compatible with strict provider schemas. */
  scope: Schema.Literal("all"),
}) {}

export const ListChangedFiles = Tool.make("list_changed_files", {
  description:
    "List every file changed by this pull request with its status, line counts, and whether a textual diff is available.",
  parameters: ListChangedFilesQuery,
  success: ChangedFilesView,
  failure: PullRequestSourceFailure,
  failureMode: "error",
  dependencies: [PullRequestSource],
}).annotate(ToolExecutionClass, "readonly");

export class FileDiffQuery extends Schema.Class<FileDiffQuery>(
  "@effect-agent/pr-review/FileDiffQuery",
)({
  path: ChangedPath,
}) {}

export class FileDiffView extends Schema.Class<FileDiffView>(
  "@effect-agent/pr-review/FileDiffView",
)({
  path: ChangedPath,
  status: ChangedFileStatus,
  /**
   * The unified diff with explicit RIGHT-side line numbers: `R<n>` marks a
   * line present in the new file version (only those may anchor findings);
   * `-` marks removed lines. Empty when no textual diff exists.
   */
  annotatedPatch: Schema.String,
  truncated: Schema.Boolean,
}) {}

// Read failures stay model-visible results ("return"), never run-killers:
// a model asking for an out-of-changeset path is expected untrusted-input
// behavior, and the fail-closed answer is a typed refusal it can correct —
// aborting the whole review on one bad path guess would be fragility, not
// security (the run stays bounded by AgentPolicy regardless).
export const ReadFileDiff = Tool.make("read_file_diff", {
  description:
    "Read the annotated unified diff of one changed file. Lines marked R<number> exist in the new version and are the only valid finding anchors.",
  parameters: FileDiffQuery,
  success: FileDiffView,
  failure: Schema.Union([PullRequestSourceFailure, ReviewInputViolation]),
  failureMode: "return",
  dependencies: [PullRequestSource],
}).annotate(ToolExecutionClass, "readonly");

export class FileSliceQuery extends Schema.Class<FileSliceQuery>(
  "@effect-agent/pr-review/FileSliceQuery",
)({
  path: ChangedPath,
  /** 1-based first line to read; defaults to 1. */
  startLine: Schema.optionalKey(Schema.Int.check(Schema.isGreaterThan(0))),
  /** Number of lines to read; defaults to 400, capped at 1000. */
  maxLines: Schema.optionalKey(
    Schema.Int.check(Schema.isGreaterThan(0)).check(Schema.isLessThanOrEqualTo(MAX_SLICE_LINES)),
  ),
}) {}

export class FileSlice extends Schema.Class<FileSlice>("@effect-agent/pr-review/FileSlice")({
  path: ChangedPath,
  startLine: Schema.Int.check(Schema.isGreaterThan(0)),
  endLine: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  totalLines: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  /** Slice content with each line prefixed by its 1-based line number. */
  content: Schema.String,
}) {}

export const ReadFile = Tool.make("read_file", {
  description:
    "Read a numbered slice of the NEW (head) version of one changed file, for context around the diff. Only files in the changeset are readable.",
  parameters: FileSliceQuery,
  success: FileSlice,
  failure: Schema.Union([PullRequestSourceFailure, ReviewInputViolation]),
  failureMode: "return",
  dependencies: [PullRequestSource],
}).annotate(ToolExecutionClass, "readonly");

export const ReviewToolkit = Toolkit.make(ListChangedFiles, ReadFileDiff, ReadFile);

/**
 * The `list_changed_files` handler, shared by the flat reviewer's toolkit and
 * any extended toolkit built by the configuration factory.
 */
export const listChangedFilesHandler = (_query: ListChangedFilesQuery) =>
  Effect.gen(function* () {
    const source = yield* PullRequestSource;
    const files = yield* source.changedFiles;
    const metadata = yield* source.metadata;
    return ChangedFilesView.make({
      totalFiles: metadata.totalChangedFiles,
      truncated: files.length < metadata.totalChangedFiles,
      files: files.map((file) =>
        ChangedFileSummary.make({
          path: file.path,
          status: file.status,
          additions: file.additions,
          deletions: file.deletions,
          hasTextualDiff: file.patch !== undefined,
        }),
      ),
    });
  });

/**
 * The `read_file_diff` handler, shared verbatim by the flat reviewer's
 * toolkit and the fan-out child's toolkit (fan-out.ts).
 */
export const readFileDiffHandler = (query: FileDiffQuery) =>
  Effect.gen(function* () {
    const source = yield* PullRequestSource;
    const relative = yield* normalizeRepoRelativePath(query.path);
    const files = yield* source.changedFiles;
    const file = files.find((candidate) => candidate.path === relative);
    if (file === undefined) {
      return yield* ReviewInputViolation.make({
        input: relative,
        reason: "Path is not part of this pull request's changeset.",
      });
    }
    const annotated = file.patch === undefined ? "" : annotatePatch(file.patch);
    const truncated = annotated.length > MAX_PATCH_CHARS;
    return FileDiffView.make({
      path: file.path,
      status: file.status,
      annotatedPatch: truncated
        ? `${annotated.slice(0, MAX_PATCH_CHARS)}\n[diff truncated]`
        : annotated,
      truncated,
    });
  });

/**
 * The `read_file` handler, shared verbatim by the flat reviewer's toolkit
 * and the fan-out child's toolkit (fan-out.ts).
 */
export const readFileHandler = (query: FileSliceQuery) =>
  Effect.gen(function* () {
    const source = yield* PullRequestSource;
    const relative = yield* normalizeRepoRelativePath(query.path);
    const content = yield* source.readFile(relative);
    const lines = content.split("\n");
    const startLine = query.startLine ?? 1;
    const maxLines = query.maxLines ?? DEFAULT_SLICE_LINES;
    if (startLine > lines.length) {
      return yield* ReviewInputViolation.make({
        input: `${relative}:${startLine}`,
        reason: `startLine is beyond the end of the file (${lines.length} lines).`,
      });
    }
    const slice = lines.slice(startLine - 1, startLine - 1 + maxLines);
    const endLine = startLine + slice.length - 1;
    return FileSlice.make({
      path: relative,
      startLine,
      endLine,
      totalLines: lines.length,
      content: slice
        .map((text, index) => `${String(startLine + index).padStart(5)}  ${text}`)
        .join("\n"),
    });
  });

export const ReviewToolkitLayer = ReviewToolkit.toLayer({
  list_changed_files: listChangedFilesHandler,
  read_file_diff: readFileDiffHandler,
  read_file: readFileHandler,
});

// ---------------------------------------------------------------------------
// Mission input and review output contracts.
// ---------------------------------------------------------------------------

export class ReviewMission extends Schema.Class<ReviewMission>(
  "@effect-agent/pr-review/ReviewMission",
)({
  repository: Schema.NonEmptyString.check(Schema.isMaxLength(200)),
  number: Schema.Int.check(Schema.isGreaterThan(0)),
  title: Schema.String.check(Schema.isMaxLength(400)),
  body: Schema.String.check(Schema.isMaxLength(20_000)),
  baseRef: Schema.NonEmptyString.check(Schema.isMaxLength(300)),
  headRef: Schema.NonEmptyString.check(Schema.isMaxLength(300)),
  changedFileCount: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
}) {}

export const FindingSeverity = Schema.Literals(["blocking", "important", "nit"]);
export type FindingSeverity = typeof FindingSeverity.Type;

export class ReviewFinding extends Schema.Class<ReviewFinding>(
  "@effect-agent/pr-review/ReviewFinding",
)({
  path: ChangedPath,
  /** 1-based line numbers in the NEW file version; must appear in the diff. */
  startLine: Schema.Int.check(Schema.isGreaterThan(0)),
  endLine: Schema.Int.check(Schema.isGreaterThan(0)),
  severity: FindingSeverity,
  title: Schema.NonEmptyString.check(Schema.isMaxLength(120)),
  body: Schema.NonEmptyString.check(Schema.isMaxLength(2_000)),
  /** Replacement for exactly lines startLine..endLine; omit when unsure. */
  suggestion: Schema.optionalKey(Schema.String.check(Schema.isMaxLength(2_000))),
}) {}

export const ReviewVerdict = Schema.Literals(["approve", "comment", "request-changes"]);
export type ReviewVerdict = typeof ReviewVerdict.Type;

/**
 * A concern with no diff line to anchor to: a missing deletion or cleanup,
 * rollout or migration sequencing, a coverage gap the diff implies but does
 * not add, or a scope question only the author can answer. Rendered as a
 * review-body section — never as an inline comment, so it needs no anchor and
 * is never demoted.
 */
export class ReviewConcern extends Schema.Class<ReviewConcern>(
  "@effect-agent/pr-review/ReviewConcern",
)({
  severity: FindingSeverity,
  title: Schema.NonEmptyString.check(Schema.isMaxLength(120)),
  body: Schema.NonEmptyString.check(Schema.isMaxLength(2_000)),
}) {}

export class CodeReview extends Schema.Class<CodeReview>("@effect-agent/pr-review/CodeReview")({
  summary: Schema.NonEmptyString.check(Schema.isMaxLength(4_000)),
  verdict: ReviewVerdict,
  findings: Schema.Array(ReviewFinding).check(Schema.isMaxLength(MAX_FINDINGS)),
  /** Non-anchorable concerns; absent when the review raises none. */
  concerns: Schema.optionalKey(Schema.Array(ReviewConcern).check(Schema.isMaxLength(MAX_CONCERNS))),
}) {}

// ---------------------------------------------------------------------------
// Instructions. Live models diverge wherever the contract is implicit, so the
// exact JSON shape, the anchor rule, and the suggestion rule are all spelled
// out with types. Consumer guidance is injected BETWEEN the mission framing
// and the procedure — it can widen what the reviewer pays attention to, but
// the machine contract (anchor rule, JSON shape, bounds) is always appended
// by this builder and cannot be edited out.
// ---------------------------------------------------------------------------

/** Consumer-supplied domain guidance: static lines or a function of the mission. */
export type ReviewGuidance =
  | string
  | ReadonlyArray<string>
  | ((mission: ReviewMission) => string | ReadonlyArray<string>);

export const resolveGuidance = (
  guidance: ReviewGuidance | undefined,
  mission: ReviewMission,
): ReadonlyArray<string> => {
  if (guidance === undefined) return [];
  const value = typeof guidance === "function" ? guidance(mission) : guidance;
  const lines = typeof value === "string" ? [value] : value;
  return lines.filter((line) => line.length > 0);
};

export interface ReviewInstructionOptions {
  readonly guidance?: ReviewGuidance | undefined;
  /** Advertised findings bound; clamped to the CodeReview schema cap. */
  readonly maxFindings?: number | undefined;
}

/** Clamp a configured findings bound into the schema-supported range. */
export const clampMaxFindings = (maxFindings: number | undefined): number =>
  maxFindings === undefined
    ? MAX_FINDINGS
    : Math.min(MAX_FINDINGS, Math.max(1, Math.trunc(maxFindings)));

/** Build the flat reviewer's instructions with optional consumer guidance. */
export const makeReviewInstructions =
  (options: ReviewInstructionOptions = {}) =>
  (mission: ReviewMission): string => {
    const maxFindings = clampMaxFindings(options.maxFindings);
    return [
      `You are a senior code reviewer for pull request #${mission.number} ("${mission.title}") in ${mission.repository}, merging ${mission.headRef} into ${mission.baseRef}. It changes ${mission.changedFileCount} file(s).`,
      mission.body.length > 0
        ? `Author description:\n${mission.body}`
        : "The author provided no description.",
      ...resolveGuidance(options.guidance, mission),
      "Work in this order:",
      "1. Call list_changed_files once to see the changeset.",
      "2. Call read_file_diff for every file you review. In its output, only lines marked R<number> exist in the new version; those numbers are the only valid values for startLine and endLine. Never anchor a finding to a removed (-) line.",
      "3. Call read_file when you need surrounding context the diff does not show. ONLY files in the changeset are readable: a request for any other path (an import, a neighbor, a config) returns a failed result — do not retry it; reason from the diff instead and note the gap honestly in your summary when it matters.",
      "4. Review for real defects first: correctness, security, concurrency, resource leaks, error handling, API misuse. Style nits are least important. Do not praise; do not restate the diff.",
      "When the diff adds or changes a test, check that it can actually fail: a test that would still pass with the bug present is theatre, not coverage. The usual tell is a loose assertion standing where an exact one belongs — >= or a truthiness check over an expected value, or a snapshot that absorbs whatever it is handed.",
      "Go shallow only when the diff has no behavioral surface at all: doc typos, formatting, lockfile or generated-code regeneration, a mechanical rename. Line count is not the signal — a one-line change to auth, money, SQL, a comparison operator, or a config default is not trivial.",
      "Drop bloat-shaped findings before reporting: defensive checks for cases that cannot happen, abstractions used once, comments restating obvious code, tests asserting tautologies, just-in-case guards. A finding must be sound, correct, and worth acting on; prefer an explicit keep over an invented finding.",
      '5. After collecting anchored findings, deliberately scan for concerns with NO line to point at: deletion or cleanup plans for code the diff replaces, rollout or migration sequencing, coverage gaps the diff implies but does not add, scope questions only the author can answer. Report each as a "concern", never as a finding with an invented anchor; report none when none exist.',
      '6. Then return ONLY a JSON object — no Markdown fences, no prose before or after — exactly this shape: {"summary": <string, 1-3 paragraphs of overall assessment>, "verdict": <"approve" | "comment" | "request-changes">, "findings": [{"path": <string, a changed file path>, "startLine": <integer, an R-marked new-file line>, "endLine": <integer, >= startLine, same file, R-marked>, "severity": <"blocking" | "important" | "nit">, "title": <string, <= 120 chars>, "body": <string, why it matters and what to do>, "suggestion": <string, OPTIONAL: replacement text for exactly lines startLine..endLine, ready to commit>}], "concerns": <array, OPTIONAL: [{"severity": <"blocking" | "important" | "nit">, "title": <string, <= 120 chars>, "body": <string>}], only for step-5 concerns with no valid anchor — never duplicate a finding here>}.',
      `Report at most ${maxFindings} findings and at most ${MAX_CONCERNS} concerns; prefer the most important ones. An empty findings array with verdict "approve" is a valid review. Include "suggestion" only when you are confident the replacement compiles and preserves intent; its text must contain the full replacement for every line in the range and nothing else.`,
      'Use verdict "request-changes" only when at least one finding or concern is "blocking". Line anchors you invent will be discarded, so copy R-numbers from read_file_diff output.',
    ].join("\n");
  };

/** The default flat-reviewer instructions: no guidance, schema-cap findings. */
export const reviewInstructions = makeReviewInstructions();

/** The default flat-reviewer execution bounds. */
export const defaultReviewPolicy = AgentPolicy.make({
  maxTurns: 12,
  maxToolCalls: 24,
  maxDuration: "8 minutes",
  toolConcurrency: 2,
  tokenBudget: 300_000,
  // Budget soft landing (RUN-018): an exhausted reviewer returns its partial
  // review on one final tool-free turn instead of failing the whole run.
  onExhaustion: "final-answer",
});

// ---------------------------------------------------------------------------
// Agent Definition: model-agnostic (D-027); bindings are created by callers
// or by the configuration factory.
// ---------------------------------------------------------------------------

export const PullRequestReviewer = Agent.define("pr-reviewer", {
  input: ReviewMission,
  output: CodeReview,
  instructions: reviewInstructions,
  toolkit: ReviewToolkit,
  policy: defaultReviewPolicy,
  description:
    "Review one pull request read-only: list the changeset, read annotated diffs and head-file context, and return a structured, line-anchored code review.",
  metadata: { deploymentClass: "E", surface: "read-only" },
});

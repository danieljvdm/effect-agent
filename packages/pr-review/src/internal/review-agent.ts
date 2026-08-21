import { Effect, Schema } from "effect";
import { Agent, AgentPolicy, ToolExecutionClass, ToolResultBounds } from "effect-agent";
import { Tool, Toolkit } from "effect/unstable/ai";

import {
  annotatePatch,
  ChangedFileStatus,
  ChangedPath,
  hasReviewableContent,
  renderReviewContent,
} from "./diff.ts";
import type { ChangedFile } from "./diff.ts";
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

/** Maximum characters in one deterministic model-visible evidence chunk. */
export const MAX_PATCH_CHARS = 60_000;

/** The encoded Tool result must retain one complete bounded content fallback. */
export const REVIEW_TOOL_RESULT_MAX_BYTES = 2 * 1024 * 1024;

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
  /** True when a missing patch was recovered as bounded UTF-8 base/head content. */
  hasReviewableContent: Schema.Boolean,
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
  reviewMode: Schema.Literals(["diff", "content", "unavailable"]),
  /**
   * The unified diff with explicit RIGHT-side line numbers: `R<n>` marks a
   * line present in the new file version (only those may anchor findings);
   * `-` marks removed lines. For content fallback, `B<n>` and `H<n>`
   * identify base/head lines for reading only; they are never valid anchors.
   * Empty only when neither a patch nor bounded textual content exists.
   */
  annotatedPatch: Schema.String,
  truncated: Schema.Boolean,
}) {}

export interface FileReviewEvidenceChunk {
  readonly reviewMode: "diff" | "content" | "unavailable";
  readonly annotatedPatch: string;
}

/**
 * Split complete model-visible evidence at deterministic line boundaries.
 * A pathological single line is hard-sliced so every character is still
 * assigned and every chunk remains within the provider-independent bound.
 */
const boundedEvidenceChunks = (evidence: string): ReadonlyArray<string> => {
  if (evidence.length <= MAX_PATCH_CHARS) return [evidence];
  const chunks: Array<string> = [];
  let offset = 0;
  while (offset < evidence.length) {
    let end = Math.min(offset + MAX_PATCH_CHARS, evidence.length);
    if (end < evidence.length) {
      const boundary = evidence.lastIndexOf("\n", end - 1);
      if (boundary >= offset) end = boundary + 1;
    }
    // No newline exists inside the bound: preserve complete input with a
    // deterministic hard slice instead of silently truncating the line.
    if (end === offset) end = Math.min(offset + MAX_PATCH_CHARS, evidence.length);
    chunks.push(evidence.slice(offset, end));
    offset = end;
  }
  return chunks;
};

/** Complete bounded evidence chunks used by deterministic fan-out planning. */
export const fileReviewEvidenceChunks = (
  file: ChangedFile,
): ReadonlyArray<FileReviewEvidenceChunk> => {
  const contentEvidence = renderReviewContent(file);
  const reviewMode =
    file.patch !== undefined
      ? ("diff" as const)
      : contentEvidence !== undefined
        ? ("content" as const)
        : ("unavailable" as const);
  const annotated = file.patch === undefined ? (contentEvidence ?? "") : annotatePatch(file.patch);
  return boundedEvidenceChunks(annotated).map((annotatedPatch) => ({
    reviewMode,
    annotatedPatch,
  }));
};

/** Host-owned rendering of one changed file's bounded review evidence. */
export const fileDiffView = (file: ChangedFile): FileDiffView => {
  const chunks = fileReviewEvidenceChunks(file);
  const first = chunks[0] ?? { reviewMode: "unavailable" as const, annotatedPatch: "" };
  const truncated = first.reviewMode === "diff" && chunks.length > 1;
  return FileDiffView.make({
    path: file.path,
    status: file.status,
    reviewMode: first.reviewMode,
    annotatedPatch: truncated ? `${first.annotatedPatch}\n[diff truncated]` : first.annotatedPatch,
    truncated,
  });
};

// Read failures stay model-visible results ("return"), never run-killers:
// a model asking for an out-of-changeset path is expected untrusted-input
// behavior, and the fail-closed answer is a typed refusal it can correct —
// aborting the whole review on one bad path guess would be fragility, not
// security (the run stays bounded by AgentPolicy regardless).
export const ReadFileDiff = Tool.make("read_file_diff", {
  description:
    "Read one changed file's review evidence. A normal unified diff marks valid anchors as R<number>. When GitHub omitted the diff, bounded base/head content is returned with B/H line labels for review but no valid inline anchors.",
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
          hasReviewableContent: hasReviewableContent(file),
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
    return fileDiffView(file);
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

/** Bounded prior-review context lines injected into reviewer instructions. */
const ReviewContextLines = Schema.Array(Schema.String.check(Schema.isMaxLength(1_200))).check(
  Schema.isMaxLength(20),
);

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
  /**
   * Maintainer-adjudicated identities rendered as bounded context lines; the
   * reviewer must not re-raise them without materially new evidence. Absent
   * from fingerprint missions so an adjudication never invalidates the
   * skip-unchanged authority.
   */
  adjudicatedContext: Schema.optionalKey(ReviewContextLines),
  /**
   * Prior-round findings on re-reviewed scope, rendered as bounded context
   * lines; each must be confirmed, declared fixed, or explicitly withdrawn.
   */
  priorFindingContext: Schema.optionalKey(ReviewContextLines),
}) {}

export const FindingSeverity = Schema.Literals(["blocking", "important", "nit"]);
export type FindingSeverity = typeof FindingSeverity.Type;

/**
 * What kind of problem a finding names. Model-claimed like severity — it is a
 * label for scanning a busy review, never an input to the check conclusion.
 */
export const FindingCategory = Schema.Literals([
  "correctness",
  "security",
  "concurrency",
  "performance",
  "resources",
  "error-handling",
  "testing",
  "maintainability",
  "style",
  "docs",
]);
export type FindingCategory = typeof FindingCategory.Type;

export class ReviewFinding extends Schema.Class<ReviewFinding>(
  "@effect-agent/pr-review/ReviewFinding",
)({
  path: ChangedPath,
  /** 1-based line numbers in the NEW file version; must appear in the diff. */
  startLine: Schema.Int.check(Schema.isGreaterThan(0)),
  endLine: Schema.Int.check(Schema.isGreaterThan(0)),
  severity: FindingSeverity,
  /** Optional problem-kind label rendered next to the severity. */
  category: Schema.optionalKey(FindingCategory),
  title: Schema.NonEmptyString.check(Schema.isMaxLength(120)),
  body: Schema.NonEmptyString.check(Schema.isMaxLength(2_000)),
  /** Replacement for exactly lines startLine..endLine; omit when unsure. */
  suggestion: Schema.optionalKey(
    Schema.String.annotate({
      description:
        "Committable replacement source code for exactly lines startLine..endLine: the full replacement for every line in the range and nothing else — never prose describing the change, which belongs in body.",
    }).check(Schema.isMaxLength(2_000)),
  ),
}) {}

export const ReviewVerdict = Schema.Literals(["approve", "comment", "request-changes"]);
export type ReviewVerdict = typeof ReviewVerdict.Type;

/**
 * A concern with no diff line to anchor to: a missing deletion or cleanup,
 * rollout or migration sequencing, a coverage gap the diff implies but does
 * not add, or a scope question only the author can answer. Rendered as a
 * review-body section instead of an inline comment. `evidencePaths` binds the
 * concern to changed files so a later incremental review can invalidate and
 * recheck it when any supporting path changes. It remains optional only for
 * decoding review output and continuity state written before path binding was
 * introduced; a pathless concern cannot authorize incremental continuity.
 */
export class ReviewConcern extends Schema.Class<ReviewConcern>(
  "@effect-agent/pr-review/ReviewConcern",
)({
  evidencePaths: Schema.optionalKey(
    Schema.Array(ChangedPath).check(Schema.isMinLength(1)).check(Schema.isMaxLength(3)),
  ),
  severity: FindingSeverity,
  title: Schema.NonEmptyString.check(Schema.isMaxLength(120)),
  body: Schema.NonEmptyString.check(Schema.isMaxLength(2_000)),
}) {}

/** The per-entry walkthrough summary bound, and the entries bound (the changeset cap). */
export const MAX_WALKTHROUGH_SUMMARY_CHARS = 240;
export const MAX_WALKTHROUGH_ENTRIES = 300;

/**
 * One reviewed file's one-sentence change summary. Rendered only when the
 * path is actually part of the changeset — like finding anchors, walkthrough
 * paths are validated host-side and invented ones are dropped.
 */
export class WalkthroughEntry extends Schema.Class<WalkthroughEntry>(
  "@effect-agent/pr-review/WalkthroughEntry",
)({
  path: ChangedPath,
  summary: Schema.NonEmptyString.check(Schema.isMaxLength(MAX_WALKTHROUGH_SUMMARY_CHARS)),
}) {}

export class CodeReview extends Schema.Class<CodeReview>("@effect-agent/pr-review/CodeReview")({
  summary: Schema.NonEmptyString.check(Schema.isMaxLength(4_000)),
  verdict: ReviewVerdict,
  findings: Schema.Array(ReviewFinding).check(Schema.isMaxLength(MAX_FINDINGS)),
  /** Non-anchorable concerns; absent when the review raises none. */
  concerns: Schema.optionalKey(Schema.Array(ReviewConcern).check(Schema.isMaxLength(MAX_CONCERNS))),
  /** Per-file change summaries; absent when the model provides none. */
  walkthrough: Schema.optionalKey(
    Schema.Array(WalkthroughEntry).check(Schema.isMaxLength(MAX_WALKTHROUGH_ENTRIES)),
  ),
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
      ...(mission.adjudicatedContext === undefined || mission.adjudicatedContext.length === 0
        ? []
        : [
            "A maintainer has adjudicated these previously raised review items (disposition, reason). Do not re-raise them unless you have materially new evidence, and if you do, say explicitly what changed since the adjudication:",
            ...mission.adjudicatedContext.map((line) => `- ${line}`),
          ]),
      ...(mission.priorFindingContext === undefined || mission.priorFindingContext.length === 0
        ? []
        : [
            "Your previous review raised these findings on the scope you are re-reviewing. For each, either confirm it still holds, state that it is fixed, or withdraw it; do not demand the opposite of your own prior guidance without explicitly acknowledging the reversal:",
            ...mission.priorFindingContext.map((line) => `- ${line}`),
          ]),
      "Work in this order:",
      "1. Call list_changed_files once to see the selected input scope. In incremental reviews it is deliberately a subset of the pull request's full diff (totalFiles counts the whole pull request); omitted paths belong to settled prior scope or explicit host exclusions, not to this run.",
      "2. Call read_file_diff for every listed file. A normal diff marks new-version anchors as R<number>; only those numbers are valid startLine/endLine values. When GitHub omitted a diff, the tool may return bounded base/head content marked B/H instead. Review that content, but report its defects as non-anchored concerns because B/H lines cannot anchor GitHub comments. Never anchor a finding to a removed (-), B, or H line.",
      "3. Call read_file when you need surrounding context the diff does not show. ONLY listed files are readable — read_file_diff and read_file both return a failed result for any other path (an import, a neighbor, a file named in the description). Do not request or retry unlisted paths; reason from the visible diffs instead and note the gap honestly in your summary when it matters.",
      "4. Review for real defects first: correctness, security, concurrency, resource leaks, error handling, API misuse. Style nits are least important. Do not praise; do not restate the diff.",
      "When the diff adds or changes a test, check that it can actually fail: a test that would still pass with the bug present is theatre, not coverage. The usual tell is a loose assertion standing where an exact one belongs — >= or a truthiness check over an expected value, or a snapshot that absorbs whatever it is handed.",
      "Go shallow only when the diff has no behavioral surface at all: doc typos, formatting, lockfile or generated-code regeneration, a mechanical rename. Line count is not the signal — a one-line change to auth, money, SQL, a comparison operator, or a config default is not trivial.",
      "Drop bloat-shaped findings before reporting: defensive checks for cases that cannot happen, abstractions used once, comments restating obvious code, tests asserting tautologies, just-in-case guards. A finding must be sound, correct, and worth acting on; prefer an explicit keep over an invented finding.",
      '5. After collecting anchored findings, deliberately scan for concerns with NO line to point at: deletion or cleanup plans for code the diff replaces, rollout or migration sequencing, coverage gaps the diff implies but does not add, scope questions only the author can answer. Report each as a "concern", never as a finding with an invented anchor. Every concern must list 1-3 exact changed evidencePaths that support it so later incremental reviews can recheck it when those files change. Report none when none exist, and never split one root concern into differently worded restatements.',
      `6. Write a walkthrough: for every file whose evidence you examined, one factual sentence (<= ${MAX_WALKTHROUGH_SUMMARY_CHARS} chars) describing what changed in that file — written for a reader scanning the pull request, never restating the diff line by line. Use only paths from list_changed_files; invented paths are dropped.`,
      '7. Then return ONLY a JSON object — no Markdown fences, no prose before or after — exactly this shape: {"summary": <string, 1-3 paragraphs of overall assessment>, "verdict": <"approve" | "comment" | "request-changes">, "findings": [{"path": <string, a changed file path>, "startLine": <integer, an R-marked new-file line>, "endLine": <integer, >= startLine, same file, R-marked>, "severity": <"blocking" | "important" | "nit">, "category": <OPTIONAL: "correctness" | "security" | "concurrency" | "performance" | "resources" | "error-handling" | "testing" | "maintainability" | "style" | "docs">, "title": <string, <= 120 chars>, "body": <string, why it matters and what to do>, "suggestion": <string, OPTIONAL: replacement text for exactly lines startLine..endLine, ready to commit>}], "concerns": <array, OPTIONAL: [{"evidencePaths": <array of 1-3 exact changed file paths>, "severity": <"blocking" | "important" | "nit">, "title": <string, <= 120 chars>, "body": <string>}], only for step-5 concerns with no valid anchor — never duplicate a finding here>, "walkthrough": <array, OPTIONAL: [{"path": <string, a changed file path>, "summary": <string, the step-6 sentence>}], one entry per reviewed file>}.',
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
  // The read tools return refusals as model-visible results (failureMode
  // "return"), and a model may probe several out-of-scope paths in ONE
  // parallel batch — e.g. files the PR description names outside an
  // incremental delta — before it has seen a single refusal. The engine's
  // default limit of 3 made that exploration fatal; half the tool-call
  // budget keeps the genuinely-stuck brake while maxToolCalls and
  // maxDuration bound the run regardless.
  repeatedFailureLimit: 12,
  tokenBudget: 300_000,
  // Keep enough output/summary headroom for the 200k-class provider window;
  // tool-heavy histories prune before the engine spends a summarization call.
  contextTokenLimit: 150_000,
  toolResultBounds: ToolResultBounds.make({ maxBytes: REVIEW_TOOL_RESULT_MAX_BYTES }),
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

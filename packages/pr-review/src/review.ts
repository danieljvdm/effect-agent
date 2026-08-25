import { Effect, Schema } from "effect";
import {
  Agent,
  AgentPolicy,
  AgentRuntime,
  IdGenerator,
  makeUsageBudget,
  toRunBudgetHook,
  UsageBudgetLimits,
} from "effect-agent";
import { type LanguageModel, type Model, Toolkit } from "effect/unstable/ai";

const ReviewPath = Schema.NonEmptyString.check(Schema.isMaxLength(512));
const Revision = Schema.NonEmptyString.check(Schema.isMaxLength(128));

/** One complete textual patch supplied by the host. */
export class ReviewChange extends Schema.Class<ReviewChange>(
  "@effect-agent/pr-review/ReviewChange",
)({
  path: ReviewPath,
  patch: Schema.NonEmptyString.check(Schema.isMaxLength(80_000)),
}) {}

/** The provider-neutral input to one review pass. */
export class ReviewRequest extends Schema.Class<ReviewRequest>(
  "@effect-agent/pr-review/ReviewRequest",
)({
  title: Schema.String.check(Schema.isMaxLength(1_000)),
  description: Schema.String.check(Schema.isMaxLength(20_000)),
  baseRevision: Revision,
  headRevision: Revision,
  changes: Schema.Array(ReviewChange).check(Schema.isMaxLength(100)),
  unreviewedPaths: Schema.Array(ReviewPath).check(Schema.isMaxLength(300)),
}) {}

export const ReviewSeverity = Schema.Literals(["blocking", "important", "nit"]);
export type ReviewSeverity = typeof ReviewSeverity.Type;

/** One actionable defect. `line` is a RIGHT-side line in the supplied patch. */
export class ReviewFinding extends Schema.Class<ReviewFinding>(
  "@effect-agent/pr-review/ReviewFinding",
)({
  path: ReviewPath,
  line: Schema.optionalKey(Schema.Int.check(Schema.isGreaterThan(0))),
  severity: ReviewSeverity,
  title: Schema.NonEmptyString.check(Schema.isMaxLength(200)),
  body: Schema.NonEmptyString.check(Schema.isMaxLength(2_000)),
}) {}

/** The only model-authored output. An empty findings array is a successful review. */
export class ReviewReport extends Schema.Class<ReviewReport>(
  "@effect-agent/pr-review/ReviewReport",
)({
  summary: Schema.NonEmptyString.check(Schema.isMaxLength(6_000)),
  findings: Schema.Array(ReviewFinding).check(Schema.isMaxLength(12)),
}) {}

export class ReviewUsage extends Schema.Class<ReviewUsage>("@effect-agent/pr-review/ReviewUsage")({
  inputTokens: Schema.Natural,
  outputTokens: Schema.Natural,
}) {}

export class ReviewOutcome extends Schema.Class<ReviewOutcome>(
  "@effect-agent/pr-review/ReviewOutcome",
)({
  report: ReviewReport,
  turns: Schema.Natural,
  usage: ReviewUsage,
}) {}

const BASE_INSTRUCTIONS = `Review the supplied pull-request diff once.

Report only concrete correctness, security, reliability, or maintainability defects that the author should act on. Do not praise, restate the change, invent missing repository context, or ask for speculative cleanup. An empty findings array is valid.

Every finding must use an exact supplied path. Set line only to a RIGHT-side added or context line visible in that path's unified patch; otherwise omit line. Use blocking only for a defect that should prevent shipping. Treat unreviewedPaths as unavailable scope and never imply that you inspected it.`;

export const reviewPolicy = AgentPolicy.make({
  maxTurns: 1,
  maxToolCalls: 1,
  maxDuration: "5 minutes",
  toolConcurrency: 1,
  tokenBudget: 56_000,
  completionReserveTokens: 8_000,
  contextTokenLimit: 48_000,
  onExhaustion: "fail",
});

const makeDefinition = (guidance?: string) =>
  Agent.define("pr-review", {
    input: ReviewRequest,
    output: ReviewReport,
    instructions:
      guidance === undefined || guidance.trim().length === 0
        ? BASE_INSTRUCTIONS
        : `${BASE_INSTRUCTIONS}\n\nRepository guidance:\n${guidance.trim()}`,
    toolkit: Toolkit.empty,
    policy: reviewPolicy,
    description: "Review one supplied pull-request diff in a single model call.",
    metadata: { deploymentClass: "E", surface: "read-only" },
  });

export const reviewBudgetLimits = UsageBudgetLimits.make({
  maxInputTokens: 48_000,
  maxOutputTokens: 8_000,
  maxToolCalls: 0,
  maxDurationMillis: 300_000,
});

/** Return every RIGHT-side line on which GitHub can place a diff comment. */
export const commentableLines = (patch: string): ReadonlySet<number> => {
  const lines = new Set<number>();
  let right: number | undefined;
  for (const text of patch.split("\n")) {
    const hunk = /^@@ -\d+(?:,\d+)? \+(\d+)(?:,\d+)? @@/.exec(text);
    if (hunk !== null) {
      right = Number(hunk[1]);
      continue;
    }
    if (right === undefined || text.startsWith("\\")) continue;
    if (text.startsWith("-")) continue;
    if (text.startsWith("+") || text.startsWith(" ")) {
      lines.add(right);
      right += 1;
    }
  }
  return lines;
};

export const isCommentableLine = (patch: string, line: number): boolean =>
  commentableLines(patch).has(line);

/**
 * Treat model output as untrusted: remove unknown paths, demote invalid line
 * anchors to top-level findings, and collapse exact duplicates.
 */
export const sanitizeReviewReport = (
  request: Pick<ReviewRequest, "changes">,
  report: ReviewReport,
): ReviewReport => {
  const patches = new Map(request.changes.map((change) => [change.path, change.patch] as const));
  const seen = new Set<string>();
  const findings: Array<ReviewFinding> = [];
  for (const finding of report.findings) {
    const patch = patches.get(finding.path);
    if (patch === undefined) continue;
    const line =
      finding.line !== undefined && isCommentableLine(patch, finding.line)
        ? finding.line
        : undefined;
    const key = `${finding.path}\u0000${String(line ?? "")}\u0000${finding.title.trim().toLowerCase()}`;
    if (seen.has(key)) continue;
    seen.add(key);
    findings.push(
      ReviewFinding.make({
        path: finding.path,
        ...(line === undefined ? {} : { line }),
        severity: finding.severity,
        title: finding.title,
        body: finding.body,
      }),
    );
  }
  return ReviewReport.make({ summary: report.summary, findings });
};

export interface ReviewerOptions<Provider, ModelProvides, ModelRequires> {
  readonly model: Model.Model<Provider, LanguageModel.LanguageModel | ModelProvides, ModelRequires>;
  readonly guidance?: string | undefined;
}

/** Build a provider-neutral reviewer. The returned `review` performs exactly one Run. */
export const makeReviewer = <Provider, ModelProvides, ModelRequires>(
  options: ReviewerOptions<Provider, ModelProvides, ModelRequires>,
) => {
  const definition = makeDefinition(options.guidance);
  const binding = Agent.withModel(definition, options.model);
  const review = (request: ReviewRequest) =>
    Effect.gen(function* () {
      const budget = yield* makeUsageBudget(reviewBudgetLimits);
      const result = yield* AgentRuntime.run(binding, request, {
        budget: toRunBudgetHook(budget),
      });
      const usage = yield* budget.snapshot;
      return ReviewOutcome.make({
        report: sanitizeReviewReport(request, result.output),
        turns: result.turns,
        usage: ReviewUsage.make({
          inputTokens: usage.inputTokens,
          outputTokens: usage.outputTokens,
        }),
      });
    }).pipe(Effect.provide(IdGenerator.layer), Effect.scoped);
  return { definition, binding, review } as const;
};

import { Option, Schema } from "effect";
import type { RunEvent } from "effect-agent";

import type { ChangedFile } from "./diff.ts";
import { isReviewableFile } from "./diff.ts";
import { FileReviewDelegationFailure, FileReviewRequest, FileReviewUnitResult } from "./fan-out.ts";
import { FileDiffQuery, type WalkthroughEntry } from "./review-agent.ts";
import { planReviewUnits } from "./review-units.ts";

// ---------------------------------------------------------------------------
// Host-owned coverage. Model summaries are untrusted prose; the check result
// is based on deterministic unit planning plus the semantic Tool events that
// prove which required review operations actually settled successfully.
// ---------------------------------------------------------------------------

export const ReviewShape = Schema.Literals(["flat", "fan-out"]);
export type ReviewShape = typeof ReviewShape.Type;

export class FailedReviewUnit extends Schema.Class<FailedReviewUnit>(
  "@effect-agent/pr-review/FailedReviewUnit",
)({
  unitId: Schema.NonEmptyString.check(Schema.isMaxLength(32)),
  errorTag: Schema.NonEmptyString.check(Schema.isMaxLength(256)),
}) {}

export class ReviewCoverage extends Schema.Class<ReviewCoverage>(
  "@effect-agent/pr-review/ReviewCoverage",
)({
  status: Schema.Literals(["complete", "incomplete"]),
  requiredPaths: Schema.Array(Schema.NonEmptyString.check(Schema.isMaxLength(512))).check(
    Schema.isMaxLength(300),
  ),
  reviewedPaths: Schema.Array(Schema.NonEmptyString.check(Schema.isMaxLength(512))).check(
    Schema.isMaxLength(300),
  ),
  unreviewedPaths: Schema.Array(Schema.NonEmptyString.check(Schema.isMaxLength(512))).check(
    Schema.isMaxLength(300),
  ),
  failedUnits: Schema.Array(FailedReviewUnit).check(Schema.isMaxLength(8)),
  reasons: Schema.Array(Schema.NonEmptyString.check(Schema.isMaxLength(1_000))).check(
    Schema.isMaxLength(20),
  ),
}) {}

interface ToolTrace {
  readonly declared: Map<string, Extract<RunEvent, { readonly _tag: "ToolCallDeclared" }>>;
  readonly succeeded: Map<string, Extract<RunEvent, { readonly _tag: "ToolCallSucceeded" }>>;
  readonly failed: Map<string, Extract<RunEvent, { readonly _tag: "ToolCallFailed" }>>;
}

const toolTrace = (events: ReadonlyArray<RunEvent>): ToolTrace => {
  const declared = new Map<string, Extract<RunEvent, { readonly _tag: "ToolCallDeclared" }>>();
  const succeeded = new Map<string, Extract<RunEvent, { readonly _tag: "ToolCallSucceeded" }>>();
  const failed = new Map<string, Extract<RunEvent, { readonly _tag: "ToolCallFailed" }>>();
  for (const event of events) {
    if (event._tag === "ToolCallDeclared") declared.set(event.toolCallId, event);
    if (event._tag === "ToolCallSucceeded") succeeded.set(event.toolCallId, event);
    if (event._tag === "ToolCallFailed") failed.set(event.toolCallId, event);
  }
  return { declared, succeeded, failed };
};

const sortedUnique = (values: Iterable<string>): ReadonlyArray<string> =>
  [...new Set(values)].sort((left, right) => (left < right ? -1 : left > right ? 1 : 0));

const boundedListReason = (label: string, values: Iterable<string>): string => {
  const items = sortedUnique(values);
  const prefix = `${label} (${items.length}): `;
  let rendered = prefix;
  for (let index = 0; index < items.length; index += 1) {
    const item = items[index] ?? "";
    const separator = index === 0 ? "" : ", ";
    const omitted = items.length - index - 1;
    const suffix = omitted === 0 ? "" : ` … (+${omitted} more)`;
    if (`${rendered}${separator}${item}${suffix}`.length > 1_000) {
      const omission = `… (+${items.length - index} more)`;
      return `${rendered.slice(0, 1_000 - omission.length)}${omission}`;
    }
    rendered = `${rendered}${separator}${item}`;
  }
  return rendered;
};

const flatCoverage = (
  files: ReadonlyArray<ChangedFile>,
  totalFiles: number,
  trace: ToolTrace,
): ReviewCoverage => {
  const requiredPaths = sortedUnique(files.map((file) => file.path));
  const reviewed = new Set<string>();
  const failedPaths = new Set<string>();
  for (const [toolCallId, declaration] of trace.declared) {
    if (declaration.toolName !== "read_file_diff") continue;
    const query = Schema.decodeUnknownOption(FileDiffQuery)(declaration.parameters);
    if (Option.isNone(query)) continue;
    if (trace.succeeded.has(toolCallId)) reviewed.add(query.value.path);
    if (trace.failed.has(toolCallId)) failedPaths.add(query.value.path);
  }
  const undiffable = files.filter((file) => !isReviewableFile(file)).map((file) => file.path);
  const unreviewed = requiredPaths.filter(
    (path) => !reviewed.has(path) || undiffable.includes(path) || failedPaths.has(path),
  );
  const reasons: Array<string> = [];
  if (files.length < totalFiles) {
    reasons.push(`review range exposed ${files.length} of ${totalFiles} required files`);
  }
  if (undiffable.length > 0) {
    reasons.push(
      boundedListReason("required paths have no reviewable diff or bounded text", undiffable),
    );
  }
  if (failedPaths.size > 0) {
    reasons.push(boundedListReason("diff reads failed", failedPaths));
  }
  if (unreviewed.length > 0) {
    reasons.push(boundedListReason("required paths were not successfully reviewed", unreviewed));
  }
  return ReviewCoverage.make({
    status: reasons.length === 0 ? "complete" : "incomplete",
    requiredPaths,
    reviewedPaths: sortedUnique(reviewed),
    unreviewedPaths: sortedUnique(unreviewed),
    failedUnits: [],
    reasons,
  });
};

const fanOutCoverage = (
  files: ReadonlyArray<ChangedFile>,
  totalFiles: number,
  trace: ToolTrace,
): ReviewCoverage => {
  const plan = planReviewUnits(files, { totalChangedFiles: totalFiles });
  const declarationsByUnit = new Map<
    string,
    Array<{ readonly id: string; readonly paths: ReadonlyArray<string> }>
  >();
  for (const [toolCallId, declaration] of trace.declared) {
    if (declaration.toolName !== "delegate_file_review") continue;
    const request = Schema.decodeUnknownOption(FileReviewRequest)(declaration.parameters);
    if (Option.isNone(request)) continue;
    const declarations = declarationsByUnit.get(request.value.unitId) ?? [];
    declarations.push({ id: toolCallId, paths: request.value.paths });
    declarationsByUnit.set(request.value.unitId, declarations);
  }

  const reviewed = new Set<string>();
  const unreviewed = new Set<string>([...plan.undiffablePaths, ...plan.unassignedPaths]);
  const failedUnits: Array<FailedReviewUnit> = [];
  const reasons: Array<string> = [];
  for (const unit of plan.units) {
    const declarations = declarationsByUnit.get(unit.unitId) ?? [];
    const expectedPaths = [...unit.paths];
    const exact = declarations.filter(
      (declaration) =>
        declaration.paths.length === expectedPaths.length &&
        declaration.paths.every((path, index) => path === expectedPaths[index]),
    );
    const successful = exact.filter((declaration) => {
      const event = trace.succeeded.get(declaration.id);
      if (event === undefined || trace.failed.has(declaration.id)) return false;
      const result = Schema.decodeUnknownOption(FileReviewUnitResult)(event.result);
      return Option.isSome(result) && result.value.unitId === unit.unitId;
    });
    if (declarations.length === 1 && exact.length === 1 && successful.length === 1) {
      for (const path of unit.paths) reviewed.add(path);
      continue;
    }
    for (const path of unit.paths) unreviewed.add(path);
    const failure = declarations
      .map((declaration) => trace.failed.get(declaration.id))
      .find((event) => event !== undefined);
    const returnedFailure = declarations
      .map((declaration) => trace.succeeded.get(declaration.id))
      .filter((event) => event !== undefined)
      .map((event) => Schema.decodeUnknownOption(FileReviewDelegationFailure)(event.result))
      .find(Option.isSome);
    failedUnits.push(
      FailedReviewUnit.make({
        unitId: unit.unitId,
        errorTag:
          failure?.errorTag ??
          (returnedFailure !== undefined
            ? returnedFailure.value._tag === "FileReviewUnitFailed"
              ? `${returnedFailure.value._tag}:${returnedFailure.value.childErrorTag}`
              : returnedFailure.value._tag
            : undefined) ??
          (declarations.length === 0
            ? "UnitNotAssigned"
            : declarations.length > 1
              ? "UnitAssignedMultipleTimes"
              : exact.length === 0
                ? "UnitAssignmentMismatch"
                : "UnitDidNotSettleSuccessfully"),
      }),
    );
  }
  if (plan.truncated) {
    reasons.push(`review range exposed ${files.length} of ${totalFiles} required files`);
  }
  if (plan.undiffablePaths.length > 0) {
    reasons.push(
      boundedListReason(
        "required paths have no reviewable diff or bounded text",
        plan.undiffablePaths,
      ),
    );
  }
  if (plan.unassignedPaths.length > 0) {
    reasons.push(boundedListReason("fan-out capacity left paths unassigned", plan.unassignedPaths));
  }
  if (failedUnits.length > 0) {
    reasons.push(
      boundedListReason(
        "review units did not complete",
        failedUnits.map((unit) => `${unit.unitId} (${unit.errorTag})`),
      ),
    );
  }
  return ReviewCoverage.make({
    status: reasons.length === 0 ? "complete" : "incomplete",
    requiredPaths: sortedUnique(files.map((file) => file.path)),
    reviewedPaths: sortedUnique(reviewed),
    unreviewedPaths: sortedUnique(unreviewed),
    failedUnits,
    reasons,
  });
};

/**
 * Host-verified per-file summaries from the fan-out run's Tool events: for
 * every successfully settled delegation, the child-reported `fileSummaries`
 * whose paths belong to that invocation's requested unit. This is the
 * declassification check `projectResult` cannot perform itself (it never sees
 * the request): a child assigned file A cannot smuggle a summary for changed
 * file B into the merged walkthrough, and a coordinator cannot invent or edit
 * entries — only exact child-reported, in-unit summaries survive.
 */
export const collectUnitFileSummaries = (
  events: ReadonlyArray<RunEvent>,
): ReadonlyArray<WalkthroughEntry> => {
  const trace = toolTrace(events);
  const entries: Array<WalkthroughEntry> = [];
  for (const [toolCallId, declaration] of trace.declared) {
    if (declaration.toolName !== "delegate_file_review") continue;
    const request = Schema.decodeUnknownOption(FileReviewRequest)(declaration.parameters);
    if (Option.isNone(request)) continue;
    const success = trace.succeeded.get(toolCallId);
    if (success === undefined || trace.failed.has(toolCallId)) continue;
    const result = Schema.decodeUnknownOption(FileReviewUnitResult)(success.result);
    if (Option.isNone(result) || result.value.unitId !== request.value.unitId) continue;
    const assigned = new Set(request.value.paths);
    for (const entry of result.value.fileSummaries ?? []) {
      if (assigned.has(entry.path)) entries.push(entry);
    }
  }
  return entries;
};

/** Assess one settled run without trusting its prose summary or verdict. */
export const assessReviewCoverage = (input: {
  readonly shape: ReviewShape;
  readonly files: ReadonlyArray<ChangedFile>;
  readonly totalFiles: number;
  readonly anchorFiles: ReadonlyArray<ChangedFile>;
  readonly totalAnchorFiles: number;
  readonly events: ReadonlyArray<RunEvent>;
}): ReviewCoverage => {
  const trace = toolTrace(input.events);
  const coverage =
    input.shape === "fan-out"
      ? fanOutCoverage(input.files, input.totalFiles, trace)
      : flatCoverage(input.files, input.totalFiles, trace);
  if (input.anchorFiles.length >= input.totalAnchorFiles) return coverage;
  return ReviewCoverage.make({
    ...coverage,
    status: "incomplete",
    reasons: [
      ...coverage.reasons,
      `full pull-request anchor surface exposed ${input.anchorFiles.length} of ${input.totalAnchorFiles} required files`,
    ],
  });
};

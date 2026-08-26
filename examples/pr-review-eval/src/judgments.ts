import { Schema } from "effect";

import {
  EvalCaseId,
  EvalDefectId,
  EvalInputDigest,
  EvalObservationSetDigest,
  EvalVariantId,
} from "./contracts.ts";

const BoundedRationale = Schema.NonEmptyString.check(Schema.isMaxLength(4_096));
const Adjudicator = Schema.NonEmptyString.check(
  Schema.isMaxLength(128),
  Schema.isPattern(/^[a-z0-9][a-z0-9._-]*$/),
);

export const EvalAdjudicationLabel = Schema.Literals([
  "matches-expected",
  "new-valid",
  "invalid",
  "unclear",
]);
export type EvalAdjudicationLabel = typeof EvalAdjudicationLabel.Type;

const FindingJudgmentFields = Schema.Struct({
  version: Schema.Literal(1),
  caseId: EvalCaseId,
  caseVersion: Schema.Literal(1),
  inputDigest: EvalInputDigest,
  variantId: EvalVariantId,
  trial: Schema.Int.check(Schema.isGreaterThan(0)),
  findingIndex: Schema.Natural,
  label: EvalAdjudicationLabel,
  matchedDefectIds: Schema.Array(EvalDefectId).check(Schema.isMaxLength(12)),
  rationale: BoundedRationale,
  adjudicator: Adjudicator,
}).check(
  Schema.makeFilter(
    (judgment) => {
      const ids = judgment.matchedDefectIds;
      return (
        new Set(ids).size === ids.length &&
        (judgment.label === "matches-expected" ? ids.length > 0 : ids.length === 0)
      );
    },
    {
      title: "Matched defect IDs are unique and present only for matches-expected judgments",
    },
  ),
);

export class EvalFindingJudgment extends Schema.Class<EvalFindingJudgment>(
  "@effect-agent/example-pr-review-eval/EvalFindingJudgment",
)(FindingJudgmentFields) {}

const JudgmentSetFields = Schema.Struct({
  version: Schema.Literal(1),
  observationSetDigest: EvalObservationSetDigest,
  judgments: Schema.Array(EvalFindingJudgment).check(Schema.isMaxLength(100_000)),
}).check(
  Schema.makeFilter(
    (set) => {
      const keys = set.judgments.map(
        (judgment) =>
          `${judgment.caseId}\0${judgment.variantId}\0${judgment.trial}\0${judgment.findingIndex}`,
      );
      return new Set(keys).size === keys.length;
    },
    { title: "Finding judgment keys are unique" },
  ),
);

export class EvalJudgmentSet extends Schema.Class<EvalJudgmentSet>(
  "@effect-agent/example-pr-review-eval/EvalJudgmentSet",
)(JudgmentSetFields) {}

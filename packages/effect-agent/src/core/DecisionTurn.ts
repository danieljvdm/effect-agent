import { Schema } from "effect";

const identity = Schema.NonEmptyString.check(Schema.isMaxLength(256));
const digest = Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/));
const probability = Schema.Number.check(Schema.isBetween({ minimum: 0, maximum: 1 }));

/** Bound provider/model identity, independent of the Agent's LanguageModel. */
export class DecisionTurnModel extends Schema.Class<DecisionTurnModel>(
  "@effect-agent/core/DecisionTurnModel",
)({
  provider: identity,
  model: identity,
  purpose: Schema.Literal("decision"),
}) {}

/** Provider answers and actual aggregate usage retained with the bound model identity. */
export class DecisionTurnResult extends Schema.Class<DecisionTurnResult>(
  "@effect-agent/core/DecisionTurnResult",
)({
  ...DecisionTurnModel.fields,
  contractDigest: digest,
  stateDigest: digest,
  toolName: identity,
  answer: Schema.Struct({
    label: identity,
    probabilities: Schema.Record(identity, probability),
    confidence: Schema.optionalKey(probability),
  }),
  rawUsage: Schema.Struct({
    inputTokens: Schema.optionalKey(Schema.Natural),
    outputTokens: Schema.optionalKey(Schema.Natural),
  }),
}) {}

/** Native classification evidence; projected Tool arguments are host-authored, not model output. */
export class DecisionTurnEvidence extends Schema.Class<DecisionTurnEvidence>(
  "@effect-agent/core/DecisionTurnEvidence",
)({ ...DecisionTurnResult.fields, projection: Schema.Literals(["tool", "continue"]) }) {}

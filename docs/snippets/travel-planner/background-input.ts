import { Schema } from "effect";

export const CoordinatorInput = Schema.Union([
  Schema.Struct({ _tag: Schema.Literal("Message"), text: Schema.String }),
  Schema.Struct({
    _tag: Schema.Literal("ResearchFinished"),
    activities: Schema.Array(Schema.String),
    partial: Schema.Boolean,
  }),
  Schema.Struct({ _tag: Schema.Literal("ResearchFailed"), reason: Schema.String }),
]);

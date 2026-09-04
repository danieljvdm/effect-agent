import { Schema } from "effect";

/** Application-owned source/proposal/profile shapes, intentionally outside framework source. */
export const Source = Schema.Struct({
  id: Schema.NonEmptyString.check(Schema.isMaxLength(64)),
  text: Schema.NonEmptyString.check(Schema.isMaxLength(4096)),
  revision: Schema.NonEmptyString.check(Schema.isMaxLength(64)),
  sequence: Schema.Natural,
  author: Schema.Literals(["human", "assistant"]),
});

export type Source = typeof Source.Type;

export const Proposal = Schema.Struct({
  originId: Schema.NonEmptyString,
  revision: Schema.NonEmptyString,
  text: Schema.NonEmptyString.check(Schema.isMaxLength(4096)),
  quote: Schema.NonEmptyString.check(Schema.isMaxLength(4096)),
});

export const Fact = Schema.Struct({
  originId: Schema.NonEmptyString,
  revision: Schema.NonEmptyString,
  text: Schema.NonEmptyString,
  human: Schema.Boolean,
});

export const Profile = Schema.Struct({ facts: Schema.Array(Fact) });
export type Profile = typeof Profile.Type;

export const Request = Schema.Union([
  Schema.TaggedStruct("Commit", { source: Source, automatic: Schema.Boolean }),
  Schema.TaggedStruct("Remember", { id: Schema.String, sourceId: Schema.String }),
  Schema.TaggedStruct("Foreground", {
    source: Source,
    learning: Schema.Literals(["off", "automatic", "explicit"]),
  }),
  Schema.TaggedStruct("Work", {}),
  Schema.TaggedStruct("Status", {}),
  Schema.TaggedStruct("CorruptHeader", { kind: Schema.Literals(["missing", "incompatible"]) }),
  Schema.TaggedStruct("Read", { authorized: Schema.Boolean }),
  Schema.TaggedStruct("Correct", { text: Schema.String }),
  Schema.TaggedStruct("Forget", { sourceId: Schema.String, sequence: Schema.Natural }),
  Schema.TaggedStruct("Configure", {
    extractionBlocked: Schema.optionalKey(Schema.Boolean),
    writeBlocked: Schema.optionalKey(Schema.Boolean),
    automaticWake: Schema.optionalKey(Schema.Boolean),
    failAt: Schema.optionalKey(Schema.String),
    extractionFailure: Schema.optionalKey(Schema.Literals(["retry", "defect", "none"])),
    workerTimeoutMillis: Schema.optionalKey(Schema.Number),
  }),
]);

export type Request = typeof Request.Type;

export const Status = Schema.Struct({
  active: Schema.Natural,
  archived: Schema.Natural,
  outbox: Schema.Natural,
  sources: Schema.Natural,
  extractionCalls: Schema.Natural,
  mergeCalls: Schema.Natural,
  writeCalls: Schema.Natural,
  workerStarts: Schema.Natural,
  workerFinalizers: Schema.Natural,
  extractionFinalizers: Schema.Natural,
  foregroundFinalizers: Schema.Natural,
  extractionWaiting: Schema.Natural,
  writeWaiting: Schema.Natural,
  running: Schema.Boolean,
  checkpoints: Schema.Array(Schema.Struct({ id: Schema.String, tag: Schema.String })),
});

export type Status = typeof Status.Type;

export const ForegroundSample = Schema.Struct({
  environment: Schema.Literal("local workerd/Miniflare; scripted native Effect AI"),
  learning: Schema.Literals(["off", "automatic", "explicit"]),
  firstTokenMillis: Schema.Number,
  completedResponseMillis: Schema.Number,
  admissionMillis: Schema.Number,
  output: Schema.String,
  recalled: Schema.String,
  promptIncludesRecall: Schema.Boolean,
});

export type ForegroundSample = typeof ForegroundSample.Type;

import { Option, Schema } from "effect";

const ErrorDetails = Schema.Struct({
  _tag: Schema.optionalKey(Schema.NonEmptyString),
  message: Schema.optionalKey(Schema.String),
});

export type ErrorDetails = typeof ErrorDetails.Type;

/** Tolerantly decode the safe string fields exposed by an unknown failure. */
export const decodeErrorDetails = (error: unknown): ErrorDetails =>
  Option.getOrElse(Schema.decodeUnknownOption(ErrorDetails)(error), () => ({}));

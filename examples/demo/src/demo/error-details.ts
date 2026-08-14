import { Option, Schema } from "effect";

import { DemoRunFailure } from "./operational-contracts";

const ErrorDetails = Schema.Struct({
  _tag: Schema.optionalKey(Schema.NonEmptyString),
  message: Schema.optionalKey(Schema.String),
});

export type ErrorDetails = typeof ErrorDetails.Type;

/** Tolerantly decode the safe string fields exposed by an unknown failure. */
export const decodeErrorDetails = (error: unknown): ErrorDetails =>
  Option.getOrElse(Schema.decodeUnknownOption(ErrorDetails)(error), () => ({}));

const describeUnknown = (error: unknown): string => {
  if (typeof error === "string") return error;
  if (error instanceof Error) return error.message;
  try {
    return JSON.stringify(error) ?? "Unknown demo error";
  } catch {
    return "Unserializable demo error";
  }
};

/**
 * The single mapper from an arbitrary failure or defect onto the wire
 * `DemoRunFailure`. An already-typed `DemoRunFailure` passes through unchanged
 * so its specific `errorTag` survives transport adapters, and every other
 * message is secret-redacted and bounded before it leaves the server.
 */
export const toDemoRunFailure = (error: unknown): DemoRunFailure => {
  if (Schema.is(DemoRunFailure)(error)) {
    return error;
  }
  const details = decodeErrorDetails(error);
  return DemoRunFailure.make({
    errorTag: details._tag ?? "DemoRunError",
    message: (details.message ?? describeUnknown(error))
      .replace(/\bsk-[A-Za-z0-9_-]+\b/g, "[redacted]")
      .slice(0, 1_000),
  });
};

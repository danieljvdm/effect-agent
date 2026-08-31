import { Option, Schema } from "effect";

const ErrorMessage = Schema.Struct({ message: Schema.String });
const ErrorTag = Schema.Struct({ _tag: Schema.NonEmptyString });
const decodeErrorMessage = Schema.decodeUnknownOption(ErrorMessage);
const decodeErrorTag = Schema.decodeUnknownOption(ErrorTag);

export interface ForeignDiagnostic {
  readonly tag?: string | undefined;
  readonly message?: string | undefined;
}

/**
 * Inspect a foreign failure without trusting its property accessors. Schema owns the accepted
 * fields, while the guards around each decode keep hostile proxies and getters from escaping as
 * defects during diagnostic projection.
 */
export const inspectForeignDiagnostic = (value: unknown): ForeignDiagnostic => {
  let tag: string | undefined;
  let message: string | undefined;

  try {
    tag = Option.getOrUndefined(decodeErrorTag(value))?._tag;
  } catch {
    // A diagnostic is best effort. The caller supplies the stable fallback classification.
  }

  try {
    message = Option.getOrUndefined(decodeErrorMessage(value))?.message;
  } catch {
    // A hostile message accessor must not replace the original failure with a defect.
  }

  return {
    ...(tag === undefined ? {} : { tag }),
    ...(message === undefined ? {} : { message }),
  };
};

/** Render an opaque foreign value without allowing its coercion hooks to escape. */
export const safeUnknownString = (value: unknown, fallback: string): string => {
  try {
    return String(value);
  } catch {
    return fallback;
  }
};

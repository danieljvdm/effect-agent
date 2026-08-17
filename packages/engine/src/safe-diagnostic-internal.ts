import { Option, Predicate, Schema } from "effect";

/** Maximum diagnostic text retained by the engine at an untrusted boundary. */
export const SAFE_DIAGNOSTIC_MESSAGE_LIMIT = 2_048;
const SAFE_DIAGNOSTIC_TAG_LIMIT = 128;

export interface SafeDiagnostic {
  readonly errorTag: string;
  readonly message: string;
}

const MessageProjection = Schema.Struct({ message: Schema.String });
const TagProjection = Schema.Struct({
  _tag: Schema.NonEmptyString.check(Schema.isMaxLength(SAFE_DIAGNOSTIC_TAG_LIMIT)),
});

const decodeMessageTotal = (value: unknown): { readonly message: string } | undefined => {
  try {
    return Option.getOrUndefined(Schema.decodeUnknownOption(MessageProjection)(value));
  } catch {
    // Hostile proxies and throwing accessors are untrusted input too.
    return undefined;
  }
};

const decodeTagTotal = (value: unknown): { readonly _tag: string } | undefined => {
  try {
    return Option.getOrUndefined(Schema.decodeUnknownOption(TagProjection)(value));
  } catch {
    return undefined;
  }
};

const primitiveMessage = (value: unknown): string => {
  if (value === null) return "null";
  if (value === undefined) return "undefined";
  if (Predicate.isString(value)) return value;
  if (Predicate.isNumber(value) || Predicate.isBoolean(value) || Predicate.isBigInt(value)) {
    return `${value}`;
  }
  if (Predicate.isSymbol(value)) {
    try {
      return String(value);
    } catch {
      return "Unknown failure";
    }
  }
  return "Unknown failure";
};

/**
 * Total, bounded projection of any untrusted failure value. It never invokes
 * object stringification and guards Schema reads so proxies, getters, and
 * unusual thrown values cannot replace the original failure with a new defect.
 */
export const safeDiagnostic = (value: unknown): SafeDiagnostic => {
  const projectedMessage = decodeMessageTotal(value)?.message;
  const projectedTag = decodeTagTotal(value)?._tag;
  return {
    errorTag: projectedTag ?? "UnknownError",
    message: (projectedMessage ?? primitiveMessage(value)).slice(0, SAFE_DIAGNOSTIC_MESSAGE_LIMIT),
  };
};

const DIAGNOSTIC_MESSAGE_LIMIT = 4_096;
const DIAGNOSTIC_TAG_LIMIT = 128;

const boundedDiagnostic = (value: string, maxCharacters: number): string =>
  value.length <= maxCharacters ? value : value.slice(0, maxCharacters);

const ownStringDiagnostic = (
  input: unknown,
  key: "message" | "_tag",
  maxCharacters: number,
): string | undefined => {
  if (input === null || (typeof input !== "object" && typeof input !== "function")) {
    return undefined;
  }
  try {
    const descriptor = Object.getOwnPropertyDescriptor(input, key);

    return descriptor !== undefined && "value" in descriptor && typeof descriptor.value === "string"
      ? boundedDiagnostic(descriptor.value, maxCharacters)
      : undefined;
  } catch {
    return undefined;
  }
};

/** Total, bounded projection for opaque failures. It never invokes arbitrary coercion hooks. */
export const errorMessage = (error: unknown): string => {
  const message = ownStringDiagnostic(error, "message", DIAGNOSTIC_MESSAGE_LIMIT);

  if (message !== undefined) return message;

  return typeof error === "string"
    ? boundedDiagnostic(error, DIAGNOSTIC_MESSAGE_LIMIT)
    : "Unknown error";
};

/** Total, bounded tag projection for opaque failures. */
export const errorTag = (error: unknown): string =>
  ownStringDiagnostic(error, "_tag", DIAGNOSTIC_TAG_LIMIT) ?? "UnknownError";

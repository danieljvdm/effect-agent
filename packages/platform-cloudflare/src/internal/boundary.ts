import { Predicate } from "effect";

const MAX_FOREIGN_DIAGNOSTIC_LENGTH = 8_192;

const boundForeignDiagnostic = (message: string): string =>
  message.slice(0, MAX_FOREIGN_DIAGNOSTIC_LENGTH);

/** Render a foreign failure without trusting accessors or coercion hooks on the value. */
export const safeCauseMessage = (cause: unknown, fallback: string): string => {
  try {
    const message = cause instanceof Error ? cause.message : cause;

    return boundForeignDiagnostic(typeof message === "string" ? message : String(message));
  } catch {
    return boundForeignDiagnostic(fallback);
  }
};

/** Include an Error name when worker-failure classification needs it. */
export const safeCauseDiagnostic = (cause: unknown, fallback: string): string => {
  try {
    return cause instanceof Error
      ? boundForeignDiagnostic(`${cause.name}: ${cause.message}`)
      : safeCauseMessage(cause, fallback);
  } catch {
    return boundForeignDiagnostic(fallback);
  }
};

export interface CloudflareFailureSignals {
  readonly retryable?: boolean | undefined;
  readonly overloaded?: boolean | undefined;
}

/** Read Cloudflare RPC classifications without letting a hostile proxy defect the client. */
export const cloudflareFailureSignals = (cause: unknown): CloudflareFailureSignals => {
  if (!Predicate.isObjectKeyword(cause)) return {};
  try {
    const retryableValue = Reflect.get(cause, "retryable");
    const overloadedValue = Reflect.get(cause, "overloaded");
    const resetValue = Reflect.get(cause, "durableObjectReset");

    const retryable =
      typeof retryableValue === "boolean" ? retryableValue : resetValue === true ? true : undefined;

    const overloaded = typeof overloadedValue === "boolean" ? overloadedValue : undefined;

    return {
      ...(retryable === undefined ? {} : { retryable }),
      ...(overloaded === undefined ? {} : { overloaded }),
    };
  } catch {
    return {};
  }
};

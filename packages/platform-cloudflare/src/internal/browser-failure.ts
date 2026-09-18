import type { Effect } from "effect";
import { Cause, Data, ErrorReporter, Predicate, Schema } from "effect";

/** Source-authored stages and classifications only; never retain SDK text or session capabilities. */
export class BrowserRunFailure extends Data.TaggedError("BrowserRunFailure")<{
  readonly operation: string;
  readonly reason:
    | "provider"
    | "timeout"
    | "malformed"
    | "configuration"
    | "authorization"
    | "rate-limited"
    | "pending";
  readonly status?: number;
  readonly cause?: BrowserRunFailure | { readonly name: string };
}> {
  readonly [ErrorReporter.severity] = "Error" as const;
}

/** A public projection of an already reported private failure must not produce another issue. */
export const reportedBrowserError = <A extends object>(error: A): A =>
  Object.assign(error, { [ErrorReporter.ignore]: true });

export const inheritBrowserReport = <A extends object>(original: unknown, error: A): A =>
  ErrorReporter.isIgnored(original) ? reportedBrowserError(error) : error;

const reasonSchema = Schema.Literals([
  "provider",
  "timeout",
  "malformed",
  "configuration",
  "authorization",
  "rate-limited",
  "pending",
]);

const statusSchema = Schema.Int.check(Schema.isBetween({ minimum: 100, maximum: 599 }));

const failureNames = new Set([
  "TypeError",
  "RangeError",
  "SyntaxError",
  "TimeoutError",
  "SchemaError",
]);

export const browserFailure = (operation: string, cause?: unknown): BrowserRunFailure =>
  cause instanceof BrowserRunFailure
    ? cause
    : new BrowserRunFailure({
        operation,
        reason:
          Predicate.isObject(cause) && "reason" in cause && Schema.is(reasonSchema)(cause.reason)
            ? cause.reason
            : "provider",
        ...(Predicate.isObject(cause) && "status" in cause && Schema.is(statusSchema)(cause.status)
          ? { status: cause.status }
          : {}),
        ...(Predicate.isObject(cause) &&
        "cause" in cause &&
        cause.cause instanceof BrowserRunFailure
          ? { cause: cause.cause }
          : Predicate.isObject(cause) &&
              "name" in cause &&
              typeof cause.name === "string" &&
              failureNames.has(cause.name)
            ? { cause: { name: cause.name } }
            : {}),
      });

/** Keep mixed defects and interruptions distinct while discarding all foreign diagnostic content. */
export const reportBrowserCause = <E>(
  operation: string,
  cause: Cause.Cause<E>,
): Effect.Effect<void> =>
  ErrorReporter.report(
    Cause.fromReasons(
      cause.reasons
        .filter((reason) => !Cause.isFailReason(reason) || !ErrorReporter.isIgnored(reason.error))
        .map((reason) => {
          switch (reason._tag) {
            case "Fail":
              return Cause.makeFailReason(browserFailure(operation, reason.error));
            case "Die":
              return Cause.makeDieReason(browserFailure(operation, reason.defect));
            case "Interrupt":
              return reason;
          }
        }),
    ),
  );

import {
  Cause as EffectCause,
  Exit,
  Predicate,
  Redacted,
  Schema,
  SchemaTransformation,
} from "effect";

import { strictSchema } from "./internal/strict-schema.ts";

const Text = Schema.String.check(Schema.isMaxLength(16 * 1024));
const Name = Schema.String.check(Schema.isMaxLength(1_024));
const Scalar = Schema.Union([Text, Schema.Finite, Schema.Boolean, Schema.Null]);

/** Correlation only: never include inputs, tool parameters, request bodies, or credentials. */
export const Context = Schema.Struct({
  agentId: Schema.optionalKey(Name),
  threadId: Schema.optionalKey(Name),
  runId: Schema.optionalKey(Name),
  submissionId: Schema.optionalKey(Name),
  attemptId: Schema.optionalKey(Name),
  turnId: Schema.optionalKey(Name),
  toolCallId: Schema.optionalKey(Name),
  toolName: Schema.optionalKey(Name),
  callId: Schema.optionalKey(Name),
  check: Schema.optionalKey(Name),
  operation: Schema.optionalKey(Name),
}).pipe(strictSchema);

export type Context = typeof Context.Type;

type CauseReason =
  | { readonly _tag: "Fail"; readonly error: Diagnostic }
  | { readonly _tag: "Die"; readonly defect: Diagnostic }
  | { readonly _tag: "Interrupt"; readonly fiberId?: number };

/** A private diagnostic projection, not a JavaScript error reconstruction or model response. */
export type Diagnostic =
  | {
      readonly _tag: "Error";
      readonly errorTag?: string;
      readonly name?: string;
      readonly message?: string;
      readonly stack?: string;
      readonly reason?: Diagnostic;
      readonly code?: string | number;
      readonly cause?: Diagnostic;
      readonly errors?: ReadonlyArray<Diagnostic>;
      readonly context?: Context;
      readonly omittedFields?: ReadonlyArray<string>;
      readonly truncated?: true;
    }
  | {
      readonly _tag: "Cause";
      readonly reasons: ReadonlyArray<CauseReason>;
      readonly truncated?: true;
    }
  | { readonly _tag: "Value"; readonly value: string | number | boolean | null }
  | { readonly _tag: "Omitted"; readonly reason: "redacted" | "cycle" | "limit" | "unsupported" };

const diagnosticSchema = <Nested extends Schema.Top>(nested: Nested) =>
  Schema.Union([
    Schema.Struct({
      _tag: Schema.Literal("Error"),
      errorTag: Schema.optionalKey(Name),
      name: Schema.optionalKey(Name),
      message: Schema.optionalKey(Text),
      stack: Schema.optionalKey(Text),
      reason: Schema.optionalKey(nested),
      code: Schema.optionalKey(Schema.Union([Name, Schema.Finite])),
      cause: Schema.optionalKey(nested),
      errors: Schema.optionalKey(Schema.Array(nested).check(Schema.isMaxLength(128))),
      context: Schema.optionalKey(Context),
      omittedFields: Schema.optionalKey(Schema.Array(Name).check(Schema.isMaxLength(32))),
      truncated: Schema.optionalKey(Schema.Literal(true)),
    }),
    Schema.Struct({
      _tag: Schema.Literal("Cause"),
      reasons: Schema.Array(
        Schema.Union([
          Schema.Struct({ _tag: Schema.Literal("Fail"), error: nested }),
          Schema.Struct({ _tag: Schema.Literal("Die"), defect: nested }),
          Schema.Struct({
            _tag: Schema.Literal("Interrupt"),
            fiberId: Schema.optionalKey(Schema.Finite),
          }),
        ]),
      ).check(Schema.isMaxLength(128)),
      truncated: Schema.optionalKey(Schema.Literal(true)),
    }),
    Schema.Struct({ _tag: Schema.Literal("Value"), value: Scalar }),
    Schema.Struct({
      _tag: Schema.Literal("Omitted"),
      reason: Schema.Literals(["redacted", "cycle", "limit", "unsupported"]),
    }),
  ]);

/** Structured, allowlisted Error fields and ordered Effect Fail/Die/Interrupt reasons. */
export const Diagnostic: Schema.Codec<Diagnostic> = Schema.suspend(() =>
  diagnosticSchema(Diagnostic),
).pipe(strictSchema);

// Inspect one encoded node only; recursion must share capture's depth/node/text budgets.
const decodeDiagnosticNode = Schema.decodeUnknownExit(
  diagnosticSchema(Schema.Unknown).pipe(strictSchema),
);

/** Remove common credential forms; hosts must keep arbitrary private payloads out of error text. */
export const redactText = (text: string): string =>
  text
    .replace(/\b(Bearer|Basic)\s+[A-Za-z0-9._~+/=-]+/gi, "$1 [REDACTED]")
    .replace(/(https?:\/\/)[^\s/@]+:[^\s/@]+@/gi, "$1[REDACTED]@")
    .replace(
      /([?&](?:access_token|api_key|apikey|token|key|secret|password|signature)=)[^\s&#]*/gi,
      "$1[REDACTED]",
    )
    .replace(
      /\b((?:api[_-]?key|access[_-]?token|authorization|password|secret)\s*[=:]\s*)[^\s,;}]+/gi,
      "$1[REDACTED]",
    );

/**
 * Capture only diagnostic fields. Local values are never mutated. Cycles and exhausted limits
 * are explicit, not silently dropped; capture visits at most 1,024 nodes, with 56 JSON levels
 * and 128 KiB text. Inner causes have priority over outer stacks when the text budget is exhausted.
 * Text remains operator-private. No generic serializer can identify secrets embedded in arbitrary
 * prose: application error messages must exclude payloads, and hosts may supply stricter redaction.
 */
export const capture = (
  value: unknown,
  options?: { readonly redactText?: (text: string) => string },
): Diagnostic => {
  const seen = new WeakSet<object>();
  let remainingNodes = 1_024;
  let remainingText = 128 * 1024;

  const text = (value: string, limit = 16 * 1024): string => {
    const safe = (options?.redactText ?? redactText)(value);
    const length = Math.max(0, Math.min(limit, remainingText));

    const marker = "[truncated]".slice(0, length);

    const result =
      safe.length <= length ? safe : `${safe.slice(0, length - marker.length)}${marker}`;

    remainingText -= result.length;

    return result;
  };

  const visit = (value: unknown, depth: number): Diagnostic => {
    if (remainingNodes <= 0 || depth >= 56) return { _tag: "Omitted", reason: "limit" };
    remainingNodes--;
    if (Redacted.isRedacted(value)) return { _tag: "Omitted", reason: "redacted" };
    if (Predicate.isString(value)) return { _tag: "Value", value: text(value) };
    if (
      value === null ||
      Predicate.isBoolean(value) ||
      (Predicate.isNumber(value) && Number.isFinite(value))
    )
      return { _tag: "Value", value };
    if (!Predicate.isObject(value)) return { _tag: "Omitted", reason: "unsupported" };
    if (seen.has(value)) return { _tag: "Omitted", reason: "cycle" };
    seen.add(value);
    const nested = (value: unknown) => visit(value, depth + 1);
    // Local errors retain their own tag, even when it happens to be "Error".
    const decoded = value instanceof Error ? undefined : decodeDiagnosticNode(value);
    const node = decoded !== undefined && Exit.isSuccess(decoded) ? decoded.value : undefined;

    if (node?._tag === "Omitted" || node?._tag === "Value") {
      seen.delete(value);

      return node._tag === "Value" && typeof node.value === "string"
        ? { _tag: "Value", value: text(node.value) }
        : node;
    }

    const originalReasons = EffectCause.isCause(value)
      ? value.reasons
      : node?._tag === "Cause"
        ? node.reasons
        : undefined;

    if (originalReasons !== undefined) {
      const reasons: Array<CauseReason> = [];

      for (const reason of originalReasons) {
        if (reasons.length >= 128 || remainingNodes <= 0) break;
        remainingNodes--;

        // reasons -> array item -> error/defect contributes three JSON levels.
        switch (reason._tag) {
          case "Fail":
            reasons.push({ _tag: "Fail", error: visit(reason.error, depth + 3) });
            break;
          case "Die":
            reasons.push({ _tag: "Die", defect: visit(reason.defect, depth + 3) });
            break;
          case "Interrupt":
            reasons.push({
              _tag: "Interrupt",
              ...(reason.fiberId === undefined ? {} : { fiberId: reason.fiberId }),
            });
            break;
        }
      }

      const result: Diagnostic = {
        _tag: "Cause",
        ...(originalReasons.length > reasons.length || (node?._tag === "Cause" && node.truncated)
          ? { truncated: true }
          : {}),
        reasons,
      };

      seen.delete(value);

      return result;
    }
    const retained = node?._tag === "Error" ? node : undefined;
    const omittedFields: Array<string> = [];

    const read = (key: string): unknown => {
      // A throwing accessor must not replace the original failure during diagnostic capture.
      try {
        return Reflect.get(value, key);
      } catch {
        omittedFields.push(key);

        return undefined;
      }
    };

    const tag = retained === undefined ? read("_tag") : retained.errorTag;
    const name = read("name");
    const message = read("message");
    const stack = read("stack");
    const reason = read("reason");
    const code = read("code");
    const cause = read("cause");
    const errors = read("errors");
    const capturedCause = cause === undefined ? undefined : nested(cause);
    const capturedReason = reason === undefined ? undefined : nested(reason);
    const capturedErrors: Array<Diagnostic> = [];

    if (Array.isArray(errors))
      for (const error of errors) {
        if (capturedErrors.length >= 128 || remainingNodes <= 0) break;
        capturedErrors.push(visit(error, depth + 2));
      }

    for (const field of retained?.omittedFields ?? []) omittedFields.push(text(field, 1_024));

    const context: Record<string, string> = {};

    for (const key of [
      "agentId",
      "threadId",
      "runId",
      "submissionId",
      "attemptId",
      "turnId",
      "toolCallId",
      "toolName",
      "callId",
      "check",
      "operation",
    ] as const) {
      const field = retained === undefined ? read(key) : retained.context?.[key];

      if (Predicate.isString(field)) context[key] = text(field, 1_024);
    }

    const result: Diagnostic = {
      _tag: "Error",
      ...(Predicate.isString(tag) ? { errorTag: text(tag, 1_024) } : {}),
      ...(Predicate.isString(name) ? { name: text(name, 1_024) } : {}),
      ...(Predicate.isString(message) ? { message: text(message) } : {}),
      ...(Predicate.isString(stack) ? { stack: text(stack) } : {}),
      ...(capturedReason === undefined ? {} : { reason: capturedReason }),
      ...(Predicate.isString(code)
        ? { code: text(code, 1_024) }
        : Predicate.isNumber(code) && Number.isFinite(code)
          ? { code }
          : {}),
      ...(capturedCause === undefined ? {} : { cause: capturedCause }),
      ...(Array.isArray(errors) ? { errors: capturedErrors } : {}),
      ...(Object.keys(context).length === 0 ? {} : { context }),
      ...(omittedFields.length === 0 ? {} : { omittedFields }),
      ...(retained?.truncated ||
      (Array.isArray(errors) && errors.length > capturedErrors.length) ||
      remainingText === 0
        ? { truncated: true }
        : {}),
    };

    seen.delete(value);

    return result;
  };

  return visit(value, 0);
};

/**
 * Preserve the original value in-process; across JSON retain its structured diagnostic projection.
 * Decoding returns Diagnostic data, never claims to reconstruct an application error class.
 */
export const Value = Diagnostic.pipe(
  Schema.decodeTo(
    Schema.Unknown,
    SchemaTransformation.transform({
      decode: (diagnostic): unknown => diagnostic,
      encode: (value: unknown) => capture(value),
    }),
  ),
);

/** Original local Effect Cause; its JSON codec preserves ordered typed, defect and interrupt reasons. */
export const Cause = Schema.Cause(Value, Value).pipe(
  Schema.decodeTo(
    Schema.declare<EffectCause.Cause<unknown>>(EffectCause.isCause),
    SchemaTransformation.transform({ decode: (cause) => cause, encode: (cause) => cause }),
  ),
);

/** Safe summary plus optional operator-private causal evidence retained at a terminal boundary. */
export const Failure = Schema.Struct({
  errorTag: Schema.NonEmptyString.check(Schema.isMaxLength(256)),
  message: Text,
  diagnostic: Schema.optionalKey(Diagnostic),
  context: Schema.optionalKey(Context),
}).pipe(strictSchema);

export type Failure = typeof Failure.Type;

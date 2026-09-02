import { Context, Duration, Schema, type Effect, type Scope } from "effect";

import {
  SANDBOX_DIAGNOSTIC_MAX_LENGTH,
  SandboxImplementation,
  SandboxNetworkPolicy,
} from "./sandbox.ts";

// Schema `isMaxLength` counts UTF-16 string elements: these caps are
// transport sanity bounds, not byte budgets. The authoritative byte budgets
// are the `CodeExecutionLimits` fields, which every adapter enforces on the
// UTF-8 encoded values (CAP-015); a multibyte payload therefore hits its
// configured byte limit well before any of these element-count caps.
const BoundedLogLine = Schema.String.check(Schema.isMaxLength(16 * 1024));
const BoundedMessage = Schema.String.check(Schema.isMaxLength(SANDBOX_DIAGNOSTIC_MAX_LENGTH));
const BoundedLogs = Schema.Array(BoundedLogLine).check(Schema.isMaxLength(4_096));
const BoundedSourceText = Schema.String.check(Schema.isMaxLength(4 * 1024 * 1024));
const PositiveInt = Schema.Int.check(Schema.isGreaterThan(0));

const FinitePositiveDuration = Schema.Duration.pipe(
  Schema.refine(
    (duration): duration is Duration.Duration =>
      Duration.isFinite(duration) && Duration.isPositive(duration),
    { expected: "a finite positive duration" },
  ),
);

const FiniteNonNegativeDuration = Schema.Duration.pipe(
  Schema.refine(
    (duration): duration is Duration.Duration =>
      Duration.isFinite(duration) && !Duration.isNegative(duration),
    { expected: "a finite non-negative duration" },
  ),
);

/**
 * ECMAScript reserved words are rejected up front: a namespace or method that
 * needs one would make every harness that binds identifiers fail at runtime
 * instead of failing closed at the Schema boundary.
 */
const reservedIdentifiers = new Set([
  "arguments",
  "await",
  "break",
  "case",
  "catch",
  "class",
  "const",
  "continue",
  "debugger",
  "default",
  "delete",
  "do",
  "else",
  "enum",
  "eval",
  "export",
  "extends",
  "false",
  "finally",
  "for",
  "function",
  "if",
  "implements",
  "import",
  "in",
  "instanceof",
  "interface",
  "let",
  "new",
  "null",
  "package",
  "private",
  "protected",
  "public",
  "return",
  "static",
  "super",
  "switch",
  "this",
  "throw",
  "true",
  "try",
  "typeof",
  "var",
  "void",
  "while",
  "with",
  "yield",
]);

/** A JavaScript identifier safe to bind as a sandbox global or method name. */
export const JsIdentifier = Schema.NonEmptyString.check(
  Schema.isMaxLength(128),
  Schema.isPattern(/^[A-Za-z_$][A-Za-z0-9_$]*$/),
).pipe(
  Schema.refine((value): value is string => !reservedIdentifiers.has(value), {
    expected: "a JavaScript identifier that is not a reserved word",
  }),
);

export type JsIdentifier = typeof JsIdentifier.Type;

/** One callable namespace exposed to the generated program as a typed global. */
export class CodeExecutionNamespace extends Schema.Class<CodeExecutionNamespace>(
  "CodeExecutionNamespace",
)({
  name: JsIdentifier,
  methods: Schema.Array(JsIdentifier).check(Schema.isMaxLength(64), Schema.isMinLength(1)),
}) {}

/**
 * Limits an executor must either enforce or reject (CAP-015). Byte bounds are
 * measured on UTF-8 encoded JSON at the sandbox boundary.
 */
export class CodeExecutionLimits extends Schema.Class<CodeExecutionLimits>("CodeExecutionLimits")({
  maxSourceBytes: PositiveInt.check(Schema.isLessThanOrEqualTo(4 * 1024 * 1024)),
  maxWallTime: FinitePositiveDuration,
  cpuMillis: Schema.optionalKey(PositiveInt.check(Schema.isLessThanOrEqualTo(5 * 60 * 1000))),
  maxLogBytes: PositiveInt.check(Schema.isLessThanOrEqualTo(1024 * 1024)),
  maxResultBytes: PositiveInt.check(Schema.isLessThanOrEqualTo(4 * 1024 * 1024)),
  maxHostCalls: Schema.Natural.check(Schema.isLessThanOrEqualTo(10_000)),
  maxHostCallArgumentBytes: PositiveInt.check(Schema.isLessThanOrEqualTo(1024 * 1024)),
  maxHostCallResultBytes: PositiveInt.check(Schema.isLessThanOrEqualTo(4 * 1024 * 1024)),
}) {}

/**
 * One isolated execution pass over bounded generated JavaScript. The source
 * must be a single expression evaluating to one async function; the executor
 * harness invokes it exactly once with no arguments (ADR-0017 decision 5).
 */
export class CodeExecutionRequest extends Schema.Class<CodeExecutionRequest>(
  "CodeExecutionRequest",
)({
  language: Schema.Literals(["javascript"]),
  source: BoundedSourceText,
  namespaces: Schema.Array(CodeExecutionNamespace).check(Schema.isMaxLength(32)),
  network: SandboxNetworkPolicy,
  limits: CodeExecutionLimits,
}) {}

/** One host invocation from generated code. Data only: the broker owns identity. */
export class CodeHostCall extends Schema.Class<CodeHostCall>("CodeHostCall")({
  namespace: JsIdentifier,
  method: JsIdentifier,
  argument: Schema.Json,
}) {}

/** A host call settled with the encoded Tool success value. */
export class CodeHostCallSuccess extends Schema.TaggedClass<CodeHostCallSuccess>()(
  "CodeHostCallSuccess",
  {
    value: Schema.Json,
  },
) {}

/**
 * A host call settled with a Schema-encoded typed failure envelope. Inside the
 * generated program the call's Promise rejects with the envelope value, so a
 * program can catch and branch on expected failures.
 */
export class CodeHostCallFailure extends Schema.TaggedClass<CodeHostCallFailure>()(
  "CodeHostCallFailure",
  {
    error: Schema.Json,
  },
) {}

/** Outcome of one host call as data; transport failures stay executor errors. */
export const CodeHostCallResult = Schema.Union([CodeHostCallSuccess, CodeHostCallFailure]);
export type CodeHostCallResult = typeof CodeHostCallResult.Type;

/**
 * The per-pass host capability: the only authority generated code has over the
 * host. It is provided at the pass edge by the capability layer, its live
 * bindings are scoped resources, and it is never persisted (ADR-0017
 * decision 2). Broker-level failures return as `CodeHostCallFailure` data;
 * an unexpected host defect remains a defect.
 */
export class CodeExecutionHost extends Context.Service<
  CodeExecutionHost,
  {
    readonly call: (call: CodeHostCall) => Effect.Effect<CodeHostCallResult>;
  }
>()("@effect-agent/sandbox/CodeExecutionHost") {}

/** Accounting one executor observed for one pass. */
export class CodeExecutionResourceUse extends Schema.Class<CodeExecutionResourceUse>(
  "CodeExecutionResourceUse",
)({
  wallTime: FiniteNonNegativeDuration,
  hostCalls: Schema.Natural,
  logBytes: Schema.Natural,
  resultBytes: Schema.Natural,
  cpuMillis: Schema.optionalKey(Schema.Natural),
}) {}

/**
 * The bounded outcome of one successful pass. `value` and `logs` are the raw
 * executor capture; the aggregate model-visible egress budget and redaction
 * pass are owned by the Code Mode capability, not the executor (CAP-016).
 */
export class CodeExecutionResult extends Schema.Class<CodeExecutionResult>("CodeExecutionResult")({
  implementation: SandboxImplementation,
  value: Schema.Json,
  logs: BoundedLogs,
  resourceUse: CodeExecutionResourceUse,
}) {}

/** The source is not one async function expression the executor can run. */
export class CodeSourceError extends Schema.TaggedError<CodeSourceError>()("CodeSourceError", {
  implementation: SandboxImplementation,
  reason: Schema.Literals(["invalid", "oversized", "not-a-function"]),
  message: BoundedMessage,
}) {}

/** The executor cannot honestly enforce one requested feature, limit, or policy. */
export class CodeExecutorUnsupportedError extends Schema.TaggedError<CodeExecutorUnsupportedError>()(
  "CodeExecutorUnsupportedError",
  {
    implementation: SandboxImplementation,
    feature: Schema.Literals([
      "language",
      "network",
      "cpu-limit",
      "namespaces",
      "wall-clock",
      "host-calls",
    ]),
    message: BoundedMessage,
  },
) {}

/** The executor could not start the isolated pass. */
export class CodeExecutorStartError extends Schema.TaggedError<CodeExecutorStartError>()(
  "CodeExecutorStartError",
  {
    implementation: SandboxImplementation,
    message: BoundedMessage,
    cause: Schema.optionalKey(Schema.Defect()),
  },
) {}

/** The pass exceeded its CPU or wall-clock budget and was terminated. */
export class CodeExecutionTimeoutError extends Schema.TaggedError<CodeExecutionTimeoutError>()(
  "CodeExecutionTimeoutError",
  {
    implementation: SandboxImplementation,
    kind: Schema.Literals(["cpu", "wall-clock"]),
    maxWallTime: FinitePositiveDuration,
    logs: BoundedLogs,
  },
) {}

/** A bounded surface (logs, result, or a per-call byte bound) was exceeded. */
export class CodeOutputLimitError extends Schema.TaggedError<CodeOutputLimitError>()(
  "CodeOutputLimitError",
  {
    implementation: SandboxImplementation,
    surface: Schema.Literals(["logs", "result", "host-call-argument", "host-call-result"]),
    limit: PositiveInt,
    observed: Schema.Natural,
    logs: BoundedLogs,
  },
) {}

/** The pass issued more host calls than its executor-owned cap allows. */
export class CodeHostCallLimitError extends Schema.TaggedError<CodeHostCallLimitError>()(
  "CodeHostCallLimitError",
  {
    implementation: SandboxImplementation,
    limit: Schema.Natural,
    logs: BoundedLogs,
  },
) {}

/**
 * The generated program finished by failing: it threw, its promise rejected,
 * or it returned a value that is not JSON. The bounded thrown value and log
 * capture flow back so a model can correct the program without a blind retry.
 */
export class CodeProgramFailedError extends Schema.TaggedError<CodeProgramFailedError>()(
  "CodeProgramFailedError",
  {
    implementation: SandboxImplementation,
    reason: Schema.Literals(["threw", "rejected", "non-json-result"]),
    thrown: Schema.Json,
    message: BoundedMessage,
    logs: BoundedLogs,
  },
) {}

/** The executor or its transport produced a value outside the protocol Schemas. */
export class CodeExecutionProtocolError extends Schema.TaggedError<CodeExecutionProtocolError>()(
  "CodeExecutionProtocolError",
  {
    implementation: SandboxImplementation,
    message: BoundedMessage,
  },
) {}

/**
 * The pass was terminated externally — isolate eviction or a platform kill —
 * with no terminal program result. No result is fabricated.
 */
export class CodeExecutorTerminatedError extends Schema.TaggedError<CodeExecutorTerminatedError>()(
  "CodeExecutorTerminatedError",
  {
    implementation: SandboxImplementation,
    message: BoundedMessage,
  },
) {}

/** Expected Code Mode execution failures. Interruption stays an Effect interruption. */
export const CodeExecutionError = Schema.Union([
  CodeSourceError,
  CodeExecutorUnsupportedError,
  CodeExecutorStartError,
  CodeExecutionTimeoutError,
  CodeOutputLimitError,
  CodeHostCallLimitError,
  CodeProgramFailedError,
  CodeExecutionProtocolError,
  CodeExecutorTerminatedError,
]);

export type CodeExecutionError = typeof CodeExecutionError.Type;

/**
 * Callback-capable isolated execution port for Code Mode (ADR-0017), a sibling
 * of the command-shaped `Sandbox` service. One `execute` is one stateless pass:
 * the executor owns no approval, Tool, replay, or Thread semantics, holds
 * every pass resource in the given Scope, and reaches the host only through the
 * `CodeExecutionHost` service in its requirement channel. Implementations must
 * identify their isolation posture honestly; an `unisolated` executor is never
 * a security boundary (CAP-010, CAP-015).
 */
export class CodeExecutor extends Context.Service<
  CodeExecutor,
  {
    readonly execute: (
      request: CodeExecutionRequest,
    ) => Effect.Effect<CodeExecutionResult, CodeExecutionError, Scope.Scope | CodeExecutionHost>;
  }
>()("@effect-agent/sandbox/CodeExecutor") {}

/** Type helper for implementations that preserve the public CodeExecutor contract. */
export type CodeExecutorExecute = (
  request: CodeExecutionRequest,
) => Effect.Effect<CodeExecutionResult, CodeExecutionError, Scope.Scope | CodeExecutionHost>;

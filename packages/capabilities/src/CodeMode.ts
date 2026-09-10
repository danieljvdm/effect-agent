import {
  AdditionalToolCatalog,
  IncludesCatalogDocumentation,
} from "@effect-agent/core/ToolExposure";
import {
  type ToolBrokerConfigurationError,
  type ToolBrokerUnavailableError,
  ToolBroker,
  type ProgrammaticCallOutcome,
  type ToolBrokerPass,
  ProgrammaticCallRecord,
} from "@effect-agent/engine/ToolBroker";
import { CurrentToolCatalog } from "@effect-agent/engine/ToolExposure";
import {
  type CodeExecutionError,
  CodeExecutionHost,
  CodeExecutionLimits,
  CodeExecutionNamespace,
  CodeExecutionRequest,
  CodeExecutor,
  JsIdentifier,
  type CodeExecutionResult,
  type CodeHostCall,
  type CodeHostCallResult,
} from "@effect-agent/sandbox/CodeExecutor";
import { NetworkDisabled } from "@effect-agent/sandbox/Sandbox";
import {
  type Layer,
  Cause,
  Context,
  Duration,
  Effect,
  Exit,
  Option,
  Schema,
  type Scope,
} from "effect";
import { Tool, Toolkit } from "effect/unstable/ai";

import { utf8ByteLength } from "./internal/utf8.ts";

/**
 * Code Mode (D-035, ADR-0017; capability spec §9.1): one native Effect AI
 * Tool whose input is bounded JavaScript source, executed in one isolated
 * `CodeExecutor` pass that may call an explicit construction-time allowlist
 * of existing Tools through typed sandbox globals and the engine-owned
 * `ToolBroker`. The builder follows the Delegation pattern: an explicit
 * record of selected Tools plus namespace mapping fixed at construction,
 * returning an ordinary Tool and a handler Layer, with no ambient registry
 * (CAP-014). Deployment class `E` only.
 */

const maxFailureTextLength = 4 * 1024;
const BoundedFailureText = Schema.String.check(Schema.isMaxLength(maxFailureTextLength));
const BoundedErrorTag = Schema.NonEmptyString.check(Schema.isMaxLength(256));
const BoundedLogLine = Schema.String.check(Schema.isMaxLength(16 * 1024));
const BoundedLogs = Schema.Array(BoundedLogLine).check(Schema.isMaxLength(4_096));
const BoundedCode = Schema.NonEmptyString.check(Schema.isMaxLength(512 * 1024));

/** A pass-local report, available to the host even when the caller interrupts execution. */
export const CodeModePassReport = Schema.Struct({
  status: Schema.Literals(["completed", "failed", "interrupted", "defect"]),
  calls: Schema.Array(ProgrammaticCallRecord),
});

export type CodeModePassReport = typeof CodeModePassReport.Type;

const encodedJsonByteLength = (value: unknown): number | undefined => {
  try {
    const encoded = JSON.stringify(value);

    return encoded === undefined ? undefined : utf8ByteLength(encoded);
  } catch {
    return undefined;
  }
};

/** Model-decoded Code Mode parameters: one async function expression. */
export const CodeModeParameters = Schema.Struct({
  code: BoundedCode,
});

/**
 * The bounded model-visible success: the program's JSON result plus captured
 * logs, both already passed through the aggregate egress budget (CAP-016).
 */
export class CodeModeSuccess extends Schema.Class<CodeModeSuccess>(
  "@effect-agent/capabilities/CodeModeSuccess",
)({
  result: Schema.Json,
  logs: BoundedLogs,
}) {}

/**
 * The bounded model-visible failure envelope. `failureMode: "return"` turns
 * it into a failed Tool result, so a model can correct a failing program
 * without a blind retry; it carries the same bounded log capture as success
 * plus the bounded thrown value where one exists, all inside the same
 * aggregate egress budget (CAP-016).
 */
export class CodeModeFailure extends Schema.TaggedError<CodeModeFailure>()("CodeModeFailure", {
  errorTag: BoundedErrorTag,
  message: BoundedFailureText,
  logs: BoundedLogs,
  thrown: Schema.optionalKey(Schema.Json),
  /** Invocation-ordered evidence that fits the egress budget. This is not a replay plan. */
  calls: Schema.optionalKey(Schema.Array(ProgrammaticCallRecord)),
  omittedCalls: Schema.optionalKey(Schema.Natural),
}) {}

/** A selective documentation request is invalid or cannot fit its declared byte budget. */
export class CodeModeDescriptionError extends Schema.TaggedError<CodeModeDescriptionError>()(
  "CodeModeDescriptionError",
  {
    reason: Schema.Literals([
      "invalid-selection",
      "unknown-method",
      "invalid-bound",
      "limit-exceeded",
    ]),
    message: BoundedFailureText,
  },
) {}

const DescriptionMethods = Schema.Array(Schema.NonEmptyString.check(Schema.isMaxLength(257))).check(
  Schema.isMinLength(1),
  Schema.isMaxLength(64),
  Schema.isUnique(),
);

const DescriptionByteLimit = Schema.Int.check(
  Schema.isBetween({ minimum: 1, maximum: 256 * 1024 }),
);

/** The namespace-record shape accepted by `CodeMode.make`. */
export type CodeModeNamespaces = Record<string, Record<string, Tool.Any>>;

/**
 * Union of every Tool selected across all namespaces, computed distributively
 * per namespace: indexing the namespace union with the INTERSECTION of method
 * keys would erase every Tool once two namespaces have disjoint methods.
 */
export type CodeModeSelectedTool<Namespaces extends CodeModeNamespaces> = {
  [Namespace in keyof Namespaces]: Namespaces[Namespace][keyof Namespaces[Namespace]];
}[keyof Namespaces];

/** The selected Tools re-keyed by their own Tool names. */
export type CodeModeSelectedRecord<Namespaces extends CodeModeNamespaces> = {
  readonly [T in CodeModeSelectedTool<Namespaces> as T["name"]]: T;
};

/**
 * The native Effect AI Tool created by `CodeMode.make` (CAP-014). Its only
 * per-call dependency is the engine-provided `ToolBroker`; the `CodeExecutor`
 * and every selected handler and redaction service are construction requirements of the handler
 * Layer instead, so they stay visible in the composed `R`.
 */
export type CodeModeTool<Name extends string> = Tool.Tool<
  Name,
  {
    readonly parameters: typeof CodeModeParameters;
    readonly success: typeof CodeModeSuccess;
    readonly failure: typeof CodeModeFailure;
    readonly failureMode: "return";
  },
  ToolBroker
>;

/** Singleton Tool record provided by one Code Mode handler Layer. */
export type CodeModeTools<Name extends string> = {
  readonly [Key in Name]: CodeModeTool<Name>;
};

/** Construction requirements of the Code Mode handler Layer. */
export type CodeModeLayerRequirements<
  Namespaces extends CodeModeNamespaces,
  RedactionRequirements = never,
> =
  | CodeExecutor
  | Exclude<RedactionRequirements, Scope.Scope>
  | Tool.HandlersFor<CodeModeSelectedRecord<Namespaces>>
  | Tool.HandlerServices<CodeModeSelectedTool<Namespaces>>;

export interface CodeModeOptions<
  Namespaces extends CodeModeNamespaces,
  RedactionRequirements = never,
> {
  /** Model-visible description; the builder appends the sandbox contract. */
  readonly description: string;
  /** Include all sandbox declarations in the model-facing description. Defaults to true. */
  readonly includeDeclarations?: boolean | undefined;
  /**
   * Explicit allowlist: namespace name → method name → native Effect AI
   * Tool. Reads and mutations are allowed; calls requiring additional approval fail closed.
   */
  readonly tools: Namespaces;
  /** Executor limits for one pass; a bounded default applies when omitted. */
  readonly limits?: CodeExecutionLimits | undefined;
  /**
   * Aggregate model-visible egress budget in UTF-8 bytes across the final
   * result, captured logs, and any thrown value (CAP-016). Default 65536.
   */
  readonly maxEgressBytes?: number | undefined;
  /**
   * Host-only ephemeral report after executor resources and invocation fibers close, including
   * failure, defect, and interruption. It contains no arguments/results and is never a checkpoint.
   * Keep this total callback bounded; its services are captured with the handler Layer.
   */
  readonly onPassExit?:
    | ((report: CodeModePassReport) => Effect.Effect<void, never, RedactionRequirements>)
    | undefined;
  /**
   * Optional aggregate redaction pass applied to the model-visible egress
   * before the byte budget. Its services are acquired with the handler Layer;
   * temporary resources close with each redaction invocation.
   * It must be total; a defect stays a defect.
   */
  readonly redactEgress?:
    | ((egress: {
        readonly result: Schema.Json;
        readonly logs: ReadonlyArray<string>;
      }) => Effect.Effect<
        {
          readonly result: Schema.Json;
          readonly logs: ReadonlyArray<string>;
        },
        never,
        RedactionRequirements
      >)
    | undefined;
}

/**
 * An immutable Code Mode definition: one model-facing Tool over an explicit
 * allowlist, plus the handler Layer that runs generated programs through the
 * `CodeExecutor` port and the engine-owned broker. It owns no acquired
 * resources.
 */
export interface CodeModeDefinition<
  Name extends string,
  Namespaces extends CodeModeNamespaces,
  RedactionRequirements = never,
> {
  readonly name: Name;
  /** The assembled model-facing description, optionally including all declarations. */
  readonly description: string;
  /** Rendered TypeScript declarations of the sandbox globals (documentation only). */
  readonly declarations: string;
  /**
   * Render complete encoded-schema declarations for 1–64 unique, exact namespace.method names.
   * The UTF-8 byte limit defaults to 16384 and must be an integer from 1 through 262144.
   * Oversized documentation fails rather than truncating a declaration. This host operation
   * does not authorize a model to see the selected methods; filter selections by current
   * visibility and inherited grants before returning its output to a model.
   */
  readonly describe: (
    methods: ReadonlyArray<string>,
    options?: { readonly maxBytes?: number | undefined },
  ) => Effect.Effect<string, CodeModeDescriptionError>;
  /** The executor-facing namespace catalog derived from the allowlist. */
  readonly namespaces: ReadonlyArray<CodeExecutionNamespace>;
  readonly limits: CodeExecutionLimits;
  readonly maxEgressBytes: number;
  /** The ordinary Effect AI Tool to include in the model-facing Toolkit. */
  readonly tool: CodeModeTool<Name>;
  /** Handler Layer; selected handler requirements stay visible in `R`. */
  readonly handlers: Layer.Layer<
    Tool.HandlersFor<CodeModeTools<Name>>,
    never,
    CodeModeLayerRequirements<Namespaces, RedactionRequirements>
  >;
}

const defaultLimits = CodeExecutionLimits.make({
  maxSourceBytes: 256 * 1024,
  maxWallTime: Duration.seconds(30),
  maxLogBytes: 256 * 1024,
  maxResultBytes: 1024 * 1024,
  maxHostCalls: 64,
  maxHostCallArgumentBytes: 256 * 1024,
  maxHostCallResultBytes: 1024 * 1024,
});

const defaultMaxEgressBytes = 64 * 1024;

// ---------------------------------------------------------------------------
// Declaration rendering (capability spec §9.1): the encoded side of each
// Schema — the JSON that actually crosses the sandbox boundary — rendered as
// TypeScript documentation via the same JSON-schema derivation Effect AI
// applies to Tool parameters. A schema the renderer cannot express fails Tool
// construction closed rather than degrading to `unknown`.
// ---------------------------------------------------------------------------

const MAX_RENDER_DEPTH = 24;

const isJsonSchemaRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const renderJsonSchemaType = (
  schema: unknown,
  defs: Record<string, unknown>,
  depth: number,
  indent: string,
): string => {
  if (depth > MAX_RENDER_DEPTH) {
    throw new Error("Code Mode declaration rendering exceeded its depth bound");
  }
  if (!isJsonSchemaRecord(schema)) {
    throw new Error(`Code Mode cannot render the JSON schema fragment ${JSON.stringify(schema)}`);
  }
  const reference = schema.$ref;

  if (typeof reference === "string") {
    const match = /^#\/\$defs\/(.+)$/.exec(reference);
    // JSON-pointer tokens escape `/` as `~1` and `~` as `~0`.
    const key = match === null ? undefined : match[1].replaceAll("~1", "/").replaceAll("~0", "~");
    const resolved = key === undefined ? undefined : defs[key];

    if (resolved === undefined) {
      throw new Error(`Code Mode cannot resolve the JSON schema reference ${reference}`);
    }

    return renderJsonSchemaType(resolved, defs, depth + 1, indent);
  }
  if (Array.isArray(schema.enum)) {
    return schema.enum.map((value) => JSON.stringify(value)).join(" | ");
  }
  if ("const" in schema) {
    return JSON.stringify(schema.const);
  }
  const union = schema.anyOf ?? schema.oneOf;

  if (Array.isArray(union)) {
    return union.map((member) => renderJsonSchemaType(member, defs, depth + 1, indent)).join(" | ");
  }
  const type = schema.type;

  if (Array.isArray(type)) {
    return type
      .map((member) => renderJsonSchemaType({ ...schema, type: member }, defs, depth + 1, indent))
      .join(" | ");
  }
  switch (type) {
    case "string": {
      return "string";
    }
    case "number":
    case "integer": {
      return "number";
    }
    case "boolean": {
      return "boolean";
    }
    case "null": {
      return "null";
    }
    case "array": {
      if (!("items" in schema)) {
        throw new Error("Code Mode cannot render an array schema without items");
      }

      return `ReadonlyArray<${renderJsonSchemaType(schema.items, defs, depth + 1, indent)}>`;
    }
    case "object":
    case undefined: {
      if (isJsonSchemaRecord(schema.properties)) {
        const required = Array.isArray(schema.required) ? schema.required : [];
        const inner = `${indent}  `;

        const fields = Object.entries(schema.properties).map(([key, property]) => {
          const optional = required.includes(key) ? "" : "?";
          // A JSON property name need not be a TypeScript identifier.
          const rendered = /^[A-Za-z_$][A-Za-z0-9_$]*$/.test(key) ? key : JSON.stringify(key);

          return `${inner}readonly ${rendered}${optional}: ${renderJsonSchemaType(property, defs, depth + 1, inner)};`;
        });

        return fields.length === 0 ? "{}" : `{\n${fields.join("\n")}\n${indent}}`;
      }
      if (isJsonSchemaRecord(schema.additionalProperties)) {
        return `Record<string, ${renderJsonSchemaType(schema.additionalProperties, defs, depth + 1, indent)}>`;
      }
      // A bare `{ "type": "object" }` states "any JSON object" (Schema.Json's
      // object member derives to exactly this); rendering it as an
      // unconstrained record is faithful, not a deriver degradation.
      if (type === "object" && !("properties" in schema) && !("additionalProperties" in schema)) {
        return "Record<string, unknown>";
      }
      break;
    }
    default: {
      break;
    }
  }
  throw new Error(
    `Code Mode cannot render the JSON schema fragment ${JSON.stringify(schema).slice(0, 200)}; fix or simplify the Tool's Schema`,
  );
};

const renderTopLevel = (jsonSchema: unknown, indent: string): string => {
  const defs =
    isJsonSchemaRecord(jsonSchema) && isJsonSchemaRecord(jsonSchema.$defs)
      ? jsonSchema.$defs
      : ({} as Record<string, unknown>);

  return renderJsonSchemaType(jsonSchema, defs, 0, indent);
};

const decodeIdentifier = Schema.decodeUnknownOption(JsIdentifier);

interface ResolvedMethod {
  readonly namespace: string;
  readonly method: string;
  readonly tool: Tool.Any;
}

const renderDeclarations = (methods: ReadonlyArray<ResolvedMethod>): string => {
  const namespaces = new Map<string, Array<ResolvedMethod>>();

  for (const method of methods) {
    const existing = namespaces.get(method.namespace) ?? [];

    existing.push(method);
    namespaces.set(method.namespace, existing);
  }

  const blocks = [...namespaces.entries()].map(([namespace, members]) => {
    const lines = members.map((member) => {
      const parameters = renderTopLevel(Tool.getJsonSchema(member.tool), "  ");
      const success = renderTopLevel(Tool.getJsonSchemaFromSchema(member.tool.successSchema), "  ");

      // A description is arbitrary text: newlines and comment terminators
      // must not be able to break out of the documentation comment.
      const safeDescription = member.tool.description
        ?.replaceAll("*/", "*\\/")
        .replaceAll(/\s*\n\s*/g, " ");

      const description = safeDescription === undefined ? "" : `  /** ${safeDescription} */\n`;

      return `${description}  ${member.method}(input: ${parameters}): Promise<${success}>;`;
    });

    return `declare const ${namespace}: {\n${lines.join("\n")}\n};`;
  });

  return blocks.join("\n\n");
};

// ---------------------------------------------------------------------------
// Aggregate model-visible egress (CAP-016): the final result, captured logs,
// and any thrown value share one byte budget. Logs are truncated line-by-line
// with an explicit marker; a result that alone exceeds the budget is a typed
// failure rather than silent truncation.
// ---------------------------------------------------------------------------

const truncationMarker = "… logs truncated by the egress budget";
const MAX_EGRESS_LOG_LINE_CHARACTERS = 16_000;

/**
 * Budget charge of one log line as it actually crosses to the model: the
 * JSON-encoded string (quotes, escapes) plus one array separator. Charging
 * raw UTF-8 would undercount model-visible bytes for escape-heavy content.
 */
const encodedLogLineBytes = (line: string): number => {
  try {
    return utf8ByteLength(JSON.stringify(line)) + 1;
  } catch {
    return Number.MAX_SAFE_INTEGER;
  }
};

const budgetedLogs = (
  logs: ReadonlyArray<string>,
  remainingBytes: number,
): ReadonlyArray<string> => {
  const kept: Array<string> = [];
  let used = 0;
  let truncated = false;

  for (const raw of logs) {
    // Per-line cap keeps every kept line inside the BoundedLogLine schema.
    const line =
      raw.length > MAX_EGRESS_LOG_LINE_CHARACTERS
        ? `${raw.slice(0, MAX_EGRESS_LOG_LINE_CHARACTERS - 1)}…`
        : raw;

    const bytes = encodedLogLineBytes(line);

    if (kept.length >= 4_096 || used + bytes > remainingBytes) {
      truncated = true;
      break;
    }
    kept.push(line);
    used += bytes;
  }
  if (truncated) {
    // Truncation is never silent: drop kept lines from the end until the
    // marker itself fits inside the budget.
    const markerBytes = encodedLogLineBytes(truncationMarker);

    while (kept.length > 0 && used + markerBytes > remainingBytes) {
      used -= encodedLogLineBytes(kept.pop() ?? "");
    }
    if (markerBytes <= remainingBytes) {
      kept.push(truncationMarker);
    }
  }

  return kept;
};

const boundedMessage = (message: string): string => message.slice(0, maxFailureTextLength);

const executionFailureMessage = (error: CodeExecutionError): string => {
  switch (error._tag) {
    case "CodeExecutionTimeoutError": {
      return `The program exceeded its ${Duration.format(error.maxWallTime)} ${error.kind} budget`;
    }
    case "CodeOutputLimitError": {
      return `The ${error.surface} limit of ${error.limit} bytes was exceeded (${error.observed} bytes observed)`;
    }
    case "CodeHostCallLimitError": {
      return `The pass exceeded its executor cap of ${error.limit} host calls`;
    }
    default: {
      return error.message;
    }
  }
};

/** UTF-8-aware truncation so a message can never exceed the aggregate budget. */
const truncateToUtf8Bytes = (value: string, maxBytes: number): string => {
  if (utf8ByteLength(value) <= maxBytes) {
    return value;
  }
  let output = "";
  let used = 0;

  for (const character of value) {
    const bytes = utf8ByteLength(character);

    if (used + bytes + 3 > maxBytes) {
      break;
    }
    output += character;
    used += bytes;
  }

  return `${output}…`;
};

type EgressRedactor<Requirements = never> = NonNullable<
  CodeModeOptions<CodeModeNamespaces, Requirements>["redactEgress"]
>;

// ---------------------------------------------------------------------------
// Builder
// ---------------------------------------------------------------------------

const make = <
  const Name extends string,
  Namespaces extends CodeModeNamespaces,
  RedactionRequirements = never,
>(
  name: Name,
  options: CodeModeOptions<Namespaces, RedactionRequirements>,
): CodeModeDefinition<Name, Namespaces, RedactionRequirements> => {
  const limits = options.limits ?? defaultLimits;
  const maxEgressBytes = options.maxEgressBytes ?? defaultMaxEgressBytes;

  // Fail closed on an invalid egress bound: NaN would make every size
  // comparison false and Infinity would remove the bound entirely.
  if (
    !Number.isSafeInteger(maxEgressBytes) ||
    maxEgressBytes < 256 ||
    maxEgressBytes > 4 * 1024 * 1024
  ) {
    throw new Error(
      `Code Mode maxEgressBytes must be an integer between 256 and ${4 * 1024 * 1024}; received ${String(maxEgressBytes)}`,
    );
  }

  // Construction-time fail-closed validation (CAP-014).
  const methods: Array<ResolvedMethod> = [];
  const toolsByName = new Map<string, Tool.Any>();
  const methodToTool = new Map<string, string>();

  for (const [namespace, namespaceMethods] of Object.entries(options.tools)) {
    if (Option.isNone(decodeIdentifier(namespace))) {
      throw new Error(`Code Mode namespace ${namespace} is not a valid JavaScript identifier`);
    }
    const entries = Object.entries(namespaceMethods);

    if (entries.length === 0) {
      throw new Error(`Code Mode namespace ${namespace} declares no methods`);
    }
    for (const [method, tool] of entries) {
      if (Option.isNone(decodeIdentifier(method))) {
        throw new Error(
          `Code Mode method ${namespace}.${method} is not a valid JavaScript identifier`,
        );
      }
      const approval = tool.needsApproval;

      if (approval !== undefined && approval !== false) {
        throw new Error(
          `Code Mode rejects Tool ${tool.name} (${namespace}.${method}): approval-requiring Tools cannot be invoked programmatically in the ephemeral slice`,
        );
      }
      const existing = toolsByName.get(tool.name);

      if (existing !== undefined && existing !== tool) {
        throw new Error(
          `Code Mode selected two different Tools named ${tool.name}; Tool names must identify one Tool`,
        );
      }
      toolsByName.set(tool.name, tool);
      methodToTool.set(`${namespace}.${method}`, tool.name);
      methods.push({ namespace, method, tool });
    }
  }
  if (methods.length === 0) {
    throw new Error("Code Mode requires at least one allowlisted Tool");
  }

  // Declarations derive from the encoded Schemas and fail construction closed
  // on anything the renderer cannot express.
  const declarations = renderDeclarations(methods);

  const methodsByPath = new Map(
    methods.map((method) => [`${method.namespace}.${method.method}`, method]),
  );

  const describe = Effect.fn("CodeMode.describe")(function* (
    selected: ReadonlyArray<string>,
    options?: { readonly maxBytes?: number | undefined },
  ) {
    const paths = yield* Schema.decodeUnknownEffect(DescriptionMethods)(selected).pipe(
      Effect.mapError(() =>
        CodeModeDescriptionError.make({
          reason: "invalid-selection",
          message:
            "Select between 1 and 64 unique namespace.method names, each at most 257 characters",
        }),
      ),
    );

    const maxBytes = yield* Schema.decodeUnknownEffect(DescriptionByteLimit)(
      options?.maxBytes ?? 16 * 1024,
    ).pipe(
      Effect.mapError(() =>
        CodeModeDescriptionError.make({
          reason: "invalid-bound",
          message: "The documentation byte limit must be an integer between 1 and 262144",
        }),
      ),
    );

    const selectedMethods: Array<ResolvedMethod> = [];

    for (const path of paths) {
      const method = methodsByPath.get(path);

      if (method === undefined) {
        return yield* CodeModeDescriptionError.make({
          reason: "unknown-method",
          message: "The selection contains a method outside this Code Mode allowlist",
        });
      }
      selectedMethods.push(method);
    }

    const documentation = renderDeclarations(selectedMethods);

    if (utf8ByteLength(documentation) > maxBytes) {
      return yield* CodeModeDescriptionError.make({
        reason: "limit-exceeded",
        message: `The selected declarations exceed the ${maxBytes}-byte documentation limit; select fewer methods or raise the limit`,
      });
    }

    return documentation;
  });

  const namespaces = [...new Set(methods.map((method) => method.namespace))].map((namespace) =>
    CodeExecutionNamespace.make({
      name: namespace,
      methods: methods
        .filter((method) => method.namespace === namespace)
        .map((method) => method.method),
    }),
  );

  const description = [
    options.description,
    "",
    "The `code` argument must be one JavaScript async function expression; the sandbox invokes it exactly once with no arguments. It runs isolated with no ambient network, filesystem, environment, or secrets. Return one JSON value. `console.log` output is captured within a bounded budget and returned alongside the result.",
    "Namespace methods return Promises. An expected Tool failure rejects with a JSON envelope carrying a stable `_tag`; catch it to branch. Use Promise.all for independent calls; the host bounds concurrency. Await every call you need. Writes may complete before a failure: inspect call outcomes and never blindly retry a program.",
    ...(options.includeDeclarations === false
      ? []
      : ["", "Sandbox globals:", "```ts", declarations, "```"]),
  ].join("\n");

  const tool = Tool.make(name, {
    description,
    parameters: CodeModeParameters,
    success: CodeModeSuccess,
    failure: CodeModeFailure,
    failureMode: "return",
  })
    .annotate(Tool.Readonly, false)
    .annotate(ToolExecutionClassAnnotation, "uncertain")
    .annotate(IncludesCatalogDocumentation, options.includeDeclarations !== false)
    .annotate(
      AdditionalToolCatalog,
      Object.freeze(methods.map((method) => Object.freeze({ ...method }))),
    )
    .addDependency(ToolBroker) as CodeModeTool<Name>;

  const outerToolkit = Toolkit.make(tool);

  /**
   * The nested namespace record collapses into one Toolkit keyed by exact
   * Tool names. The assertion restores the name-keyed record type TypeScript
   * cannot compute from `Map` iteration; construction above guarantees the
   * name uniqueness the type states.
   */
  const selectedToolkit = Toolkit.make(...toolsByName.values()) as unknown as Toolkit.Toolkit<
    CodeModeSelectedRecord<Namespaces>
  >;

  const executionRequest = (
    code: string,
    visibleNamespaces: ReadonlyArray<CodeExecutionNamespace>,
  ): CodeExecutionRequest =>
    CodeExecutionRequest.make({
      language: "javascript",
      source: code,
      namespaces: visibleNamespaces,
      network: NetworkDisabled.make({}),
      limits,
    });

  const routeHostCall = (
    pass: ToolBrokerPass,
    hostCall: CodeHostCall,
    visiblePaths: ReadonlySet<string> | undefined,
  ): Effect.Effect<CodeHostCallResult> =>
    Effect.gen(function* () {
      const path = `${hostCall.namespace}.${hostCall.method}`;

      const toolName =
        visiblePaths === undefined || visiblePaths.has(path) ? methodToTool.get(path) : undefined;

      if (toolName === undefined) {
        return {
          _tag: "CodeHostCallFailure",
          error: {
            _tag: "UnknownCodeModeMethod",
            message: "The requested method is not available in this pass",
          },
        } as const;
      }

      const outcome: ProgrammaticCallOutcome = yield* pass.invoke({
        toolName,
        encodedArguments: hostCall.argument,
      });

      switch (outcome._tag) {
        case "ProgrammaticCallSuccess": {
          const value = decodeBrokerJson(outcome.encodedResult);

          return Option.isSome(value)
            ? ({ _tag: "CodeHostCallSuccess", value: value.value } as const)
            : ({ _tag: "CodeHostCallFailure", error: brokerProtocolEnvelope } as const);
        }
        case "ProgrammaticCallFailure": {
          const value = decodeBrokerJson(outcome.encodedResult);

          return {
            _tag: "CodeHostCallFailure",
            error: Option.isSome(value) ? value.value : brokerProtocolEnvelope,
          } as const;
        }
        case "ProgrammaticCallError": {
          return {
            _tag: "CodeHostCallFailure",
            error: { _tag: outcome.errorTag, message: outcome.message },
          } as const;
        }
      }
    });

  const build = Effect.gen(function* () {
    const captured = yield* Effect.context<never>();
    const redactionServices = yield* Effect.context<Exclude<RedactionRequirements, Scope.Scope>>();
    const configuredRedactor = options.redactEgress;

    const redact: EgressRedactor | undefined =
      configuredRedactor === undefined
        ? undefined
        : (egress) =>
            Effect.scoped(configuredRedactor(egress)).pipe(
              // Merge invocation-local services before opening the redaction Scope.
              // The inner Scope shadows any Scope retained in the construction context.
              Effect.updateContext((current: Context.Context<never>) =>
                Context.merge(current, redactionServices),
              ),
            );

    const withHandler = yield* selectedToolkit;
    const executor = yield* CodeExecutor;

    const successEgress = (
      execution: CodeExecutionResult,
    ): Effect.Effect<CodeModeSuccess, CodeModeFailure> =>
      Effect.gen(function* () {
        let egress: { readonly result: Schema.Json; readonly logs: ReadonlyArray<string> } = {
          result: execution.value,
          logs: execution.logs,
        };

        if (redact !== undefined) {
          egress = yield* redact(egress);
        }
        const resultBytes = encodedJsonByteLength(egress.result);

        if (resultBytes === undefined || resultBytes > maxEgressBytes) {
          return yield* CodeModeFailure.make({
            errorTag: "CodeModeEgressExceeded",
            message: `The program result of ${resultBytes ?? "unencodable"} bytes exceeds the ${maxEgressBytes}-byte model-visible egress budget; return a smaller value`,
            logs: budgetedLogs(egress.logs, Math.max(0, maxEgressBytes - 256)),
          });
        }

        return CodeModeSuccess.make({
          result: egress.result,
          logs: budgetedLogs(egress.logs, maxEgressBytes - resultBytes),
        });
      });

    /**
     * The failure half of the aggregate egress policy (CAP-016): the configured
     * redaction pass covers failure logs and thrown values exactly like success
     * egress — a program cannot leak by logging and then throwing — and the
     * message itself is bounded by the budget, not only by its own schema cap.
     */
    const failureEgress = (
      error: CodeExecutionError | ToolBrokerUnavailableError | ToolBrokerConfigurationError,
    ): Effect.Effect<CodeModeFailure> =>
      Effect.gen(function* () {
        if (
          error._tag === "ToolBrokerUnavailableError" ||
          error._tag === "ToolBrokerConfigurationError"
        ) {
          return CodeModeFailure.make({
            errorTag: error._tag,
            message: truncateToUtf8Bytes(boundedMessage(error.message), maxEgressBytes),
            logs: [],
          });
        }
        let logs: ReadonlyArray<string> = "logs" in error ? error.logs : [];
        let candidateThrown = error._tag === "CodeProgramFailedError" ? error.thrown : undefined;

        if (redact !== undefined) {
          const redacted = yield* redact({ result: candidateThrown ?? null, logs });

          logs = redacted.logs;
          candidateThrown = candidateThrown === undefined ? undefined : redacted.result;
        }

        const message = truncateToUtf8Bytes(
          boundedMessage(executionFailureMessage(error)),
          maxEgressBytes,
        );

        const messageBytes = utf8ByteLength(message);

        // `thrown` is included only when it fits TOGETHER with the message inside
        // the aggregate budget, and it reduces the log allowance only when it is
        // actually included.
        const candidateBytes =
          candidateThrown === undefined ? undefined : encodedJsonByteLength(candidateThrown);

        const includeThrown =
          candidateThrown !== undefined &&
          candidateBytes !== undefined &&
          messageBytes + candidateBytes <= maxEgressBytes;

        const remaining = Math.max(
          0,
          maxEgressBytes - messageBytes - (includeThrown ? (candidateBytes ?? 0) : 0),
        );

        return CodeModeFailure.make({
          errorTag: error._tag,
          message,
          logs: budgetedLogs(logs, remaining),
          ...(includeThrown ? { thrown: candidateThrown } : {}),
        });
      });

    const invoke = Effect.fn(`CodeMode.${name}`)(function* (parameters: { readonly code: string }) {
      const broker = yield* ToolBroker;
      // Resolve invocation authority before restoring captured construction services. A Layer
      // built under an older Run must not restore that Run's catalogue or hidden method names.
      const catalogue = yield* Effect.serviceOption(CurrentToolCatalog);

      const visiblePaths = Option.isNone(catalogue)
        ? undefined
        : new Set(
            catalogue.value.entries.flatMap((entry) =>
              entry.kind === "code-mode" &&
              entry.nativeToolName === name &&
              methodsByPath.get(`${entry.namespace}.${entry.method}`)?.tool.name === entry.tool.name
                ? [`${entry.namespace}.${entry.method}`]
                : [],
            ),
          );

      const visibleNamespaces =
        visiblePaths === undefined
          ? namespaces
          : namespaces.flatMap((namespace) => {
              const allowed = namespace.methods.filter((method) =>
                visiblePaths.has(`${namespace.name}.${method}`),
              );

              return allowed.length === 0
                ? []
                : [CodeExecutionNamespace.make({ name: namespace.name, methods: allowed })];
            });

      let calls: ReadonlyArray<ProgrammaticCallRecord> = [];

      const execution = Effect.gen(function* () {
        const pass = yield* broker.openPass(withHandler, {
          maxResultBytes: limits.maxHostCallResultBytes,
          concurrency: limits.maxHostCallConcurrency ?? 4,
        });

        const host = CodeExecutionHost.of({
          call: (hostCall) => routeHostCall(pass, hostCall, visiblePaths),
        });

        return yield* executor.execute(executionRequest(parameters.code, visibleNamespaces)).pipe(
          Effect.provideService(CodeExecutionHost, host),
          Effect.scoped,
          Effect.onExit((exit) =>
            Effect.gen(function* () {
              calls = yield* pass.snapshot;
              if (options.onPassExit !== undefined) {
                yield* Effect.scoped(
                  options.onPassExit({
                    status: Exit.isSuccess(exit)
                      ? "completed"
                      : Cause.hasInterrupts(exit.cause)
                        ? "interrupted"
                        : Cause.hasDies(exit.cause)
                          ? "defect"
                          : "failed",
                    calls,
                  }),
                ).pipe(Effect.provideContext(redactionServices));
              }
            }),
          ),
        );
      }).pipe(
        Effect.scoped,
        // The live engine broker is re-provided innermost so a Layer built
        // inside another Run can never shadow it; the captured construction
        // context supplies the selected handlers' services (same idiom as
        // SubagentRuntime). TypeScript cannot reduce the deferred Exclude
        // over the generic namespace record, so the same private-assertion
        // contract as the engine's provideHookServices pins the identity
        // that providing the captured Context leaves no requirements; it
        // never bypasses validation.
        Effect.provideService(ToolBroker, broker),
        Effect.provideContext(captured),
      ) as Effect.Effect<
        CodeExecutionResult,
        CodeExecutionError | ToolBrokerUnavailableError | ToolBrokerConfigurationError
      >;

      return yield* execution.pipe(
        Effect.catch((error) => failureEgress(error).pipe(Effect.flatMap(Effect.fail))),
        Effect.flatMap(successEgress),
        Effect.mapError((failure) => {
          if (calls.length === 0) return failure;

          const complete = CodeModeFailure.make({
            errorTag: failure.errorTag,
            message: failure.message,
            logs: failure.logs,
            ...(failure.thrown === undefined ? {} : { thrown: failure.thrown }),
            calls,
            omittedCalls: 0,
          });

          if ((encodedJsonByteLength(complete) ?? Infinity) <= maxEgressBytes) return complete;

          // Evidence takes priority over logs and thrown values. Never silently omit calls.
          const base = {
            errorTag: failure.errorTag,
            message: truncateToUtf8Bytes(failure.message, Math.min(256, maxEgressBytes / 4)),
            logs: [],
          };

          const kept: Array<ProgrammaticCallRecord> = [];

          for (const call of calls) {
            const candidate = CodeModeFailure.make({
              ...base,
              calls: [...kept, call],
              omittedCalls: calls.length - kept.length - 1,
            });

            if ((encodedJsonByteLength(candidate) ?? Infinity) > maxEgressBytes) break;
            kept.push(call);
          }

          return CodeModeFailure.make({
            ...base,
            calls: kept,
            omittedCalls: calls.length - kept.length,
          });
        }),
      );
    });

    return { [name]: invoke } as unknown as Toolkit.HandlersFrom<CodeModeTools<Name>>;
  });

  /**
   * TypeScript cannot unify the two spellings of the singleton Tool record
   * (`CodeModeTools<Name>` versus the toolkit's name-remapped form) over a
   * generic `Name`, nor reduce the deferred `Exclude` when `toLayer`
   * subtracts what `build` consumed; the assertions pin the layer to its
   * documented requirement surface and never bypass validation.
   */
  const handlers = outerToolkit.toLayer(
    build as unknown as Effect.Effect<
      Toolkit.HandlersFrom<Toolkit.ToolsByName<readonly [CodeModeTool<Name>]>>,
      never,
      | CodeExecutor
      | Tool.HandlersFor<CodeModeSelectedRecord<Namespaces>>
      | Exclude<RedactionRequirements, Scope.Scope>
    >,
  ) as unknown as Layer.Layer<
    Tool.HandlersFor<CodeModeTools<Name>>,
    never,
    CodeModeLayerRequirements<Namespaces, RedactionRequirements>
  >;

  return Object.freeze({
    name,
    description,
    declarations,
    describe,
    namespaces,
    limits,
    maxEgressBytes,
    tool,
    handlers,
  });
};

/**
 * Fail-closed JSON boundary for broker outcomes: hostile values can throw
 * from trap getters during decode, and a value outside the JSON surface must
 * become a typed failure envelope — never a fabricated success.
 */
const decodeBrokerJson = (value: unknown): Option.Option<Schema.Json> => {
  try {
    return Schema.decodeUnknownOption(Schema.Json)(value);
  } catch {
    return Option.none();
  }
};

const brokerProtocolEnvelope: Schema.Json = {
  _tag: "CodeModeProtocolError",
  message: "The broker returned a value outside the JSON surface",
};

// The engine annotation is imported under a local alias to keep the builder
// readable next to Effect AI's own `Tool.Readonly` annotation.
import { ToolExecutionClass as ToolExecutionClassAnnotation } from "@effect-agent/engine/DurableStep";

/** Code Mode builder namespace (capability spec §9.1). */
export { make };

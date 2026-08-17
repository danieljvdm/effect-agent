import {
  ToolBroker,
  ToolBrokerConfigurationError,
  ToolBrokerUnavailableError,
  getToolExecutionClass,
  type ProgrammaticCallOutcome,
  type ToolBrokerPass,
} from "@effect-agent/engine";
import {
  CodeExecutionError,
  CodeExecutionHost,
  CodeExecutionLimits,
  CodeExecutionNamespace,
  CodeExecutionRequest,
  CodeExecutor,
  JsIdentifier,
  NetworkDisabled,
  type CodeExecutionResult,
  type CodeHostCall,
  type CodeHostCallResult,
} from "@effect-agent/sandbox";
import { Duration, Effect, Layer, Option, Schema } from "effect";
import { Tool, Toolkit } from "effect/unstable/ai";

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

const utf8ByteLength = (value: string): number => {
  let total = 0;
  for (const character of value) {
    const codePoint = character.codePointAt(0) ?? 0;
    total += codePoint <= 0x7f ? 1 : codePoint <= 0x7ff ? 2 : codePoint <= 0xffff ? 3 : 4;
  }
  return total;
};

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
}) {}

const encodedSuccessByteLength = (value: CodeModeSuccess): number | undefined => {
  try {
    return encodedJsonByteLength(Schema.encodeSync(CodeModeSuccess)(value));
  } catch {
    return undefined;
  }
};

const encodedFailureByteLength = (value: CodeModeFailure): number | undefined => {
  try {
    return encodedJsonByteLength(Schema.encodeSync(CodeModeFailure)(value));
  } catch {
    return undefined;
  }
};

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
 * and every selected handler are construction requirements of the handler
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
export type CodeModeLayerRequirements<Namespaces extends CodeModeNamespaces> =
  | CodeExecutor
  | Tool.HandlersFor<CodeModeSelectedRecord<Namespaces>>
  | Tool.HandlerServices<CodeModeSelectedTool<Namespaces>>;

export interface CodeModeOptions<Namespaces extends CodeModeNamespaces> {
  /** Model-visible description; the builder appends the sandbox contract and declarations. */
  readonly description: string;
  /**
   * Explicit allowlist: namespace name → method name → native Effect AI
   * Tool. Every Tool must be annotated `readonly` (`ToolExecutionClass`) and
   * must not require approval; construction fails closed otherwise.
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
   * Optional aggregate redaction pass applied to the model-visible egress
   * before the byte budget. It must be total; a defect stays a defect.
   */
  readonly redactEgress?:
    | ((egress: {
        readonly result: Schema.Json;
        readonly logs: ReadonlyArray<string>;
      }) => Effect.Effect<{
        readonly result: Schema.Json;
        readonly logs: ReadonlyArray<string>;
      }>)
    | undefined;
}

/**
 * An immutable Code Mode definition: one model-facing Tool over an explicit
 * allowlist, plus the handler Layer that runs generated programs through the
 * `CodeExecutor` port and the engine-owned broker. It owns no acquired
 * resources.
 */
export interface CodeModeDefinition<Name extends string, Namespaces extends CodeModeNamespaces> {
  readonly name: Name;
  /** The assembled model-facing description including the TypeScript declarations. */
  readonly description: string;
  /** Rendered TypeScript declarations of the sandbox globals (documentation only). */
  readonly declarations: string;
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
    CodeModeLayerRequirements<Namespaces>
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

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

const MAX_RENDER_DEPTH = 24;

const renderJsonSchemaType = (
  schema: unknown,
  defs: Record<string, unknown>,
  depth: number,
  indent: string,
): string => {
  if (depth > MAX_RENDER_DEPTH) {
    throw new Error("Code Mode declaration rendering exceeded its depth bound");
  }
  if (!isRecord(schema)) {
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
      if (isRecord(schema.properties)) {
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
      if (isRecord(schema.additionalProperties)) {
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
    isRecord(jsonSchema) && isRecord(jsonSchema.$defs)
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

const boundedLogs = (logs: ReadonlyArray<string>): ReadonlyArray<string> =>
  logs
    .slice(0, 4_096)
    .map((raw) =>
      raw.length > MAX_EGRESS_LOG_LINE_CHARACTERS
        ? `${raw.slice(0, MAX_EGRESS_LOG_LINE_CHARACTERS - 1)}…`
        : raw,
    );

/** Fit logs against the fully Schema-encoded envelope, including tags and JSON escaping. */
const budgetedEnvelopeLogs = <A>(
  logs: ReadonlyArray<string>,
  maxEgressBytes: number,
  makeEnvelope: (logs: ReadonlyArray<string>) => A,
  encodedEnvelopeByteLength: (value: A) => number | undefined,
): ReadonlyArray<string> => {
  const normalized = boundedLogs(logs);
  if (
    logs.length <= 4_096 &&
    (encodedEnvelopeByteLength(makeEnvelope(normalized)) ?? Number.MAX_SAFE_INTEGER) <=
      maxEgressBytes
  ) {
    return normalized;
  }

  // At most 4,095 source lines can accompany the marker while satisfying the
  // public BoundedLogs schema. Binary search keeps envelope encoding O(log n).
  let lower = 0;
  let upper = Math.min(normalized.length, 4_095);
  let best: ReadonlyArray<string> = [];
  while (lower <= upper) {
    const count = Math.floor((lower + upper) / 2);
    const candidate = [...normalized.slice(0, count), truncationMarker];
    const bytes = encodedEnvelopeByteLength(makeEnvelope(candidate));
    if (bytes !== undefined && bytes <= maxEgressBytes) {
      best = candidate;
      lower = count + 1;
    } else {
      upper = count - 1;
    }
  }
  return best;
};

const boundedMessage = (message: string): string => {
  let bounded = "";
  for (const character of message) {
    if (bounded.length + character.length > maxFailureTextLength) break;
    bounded += character;
  }
  return bounded;
};

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

type EgressRedactor = NonNullable<CodeModeOptions<CodeModeNamespaces>["redactEgress"]>;

const budgetedFailure = (
  errorTag: string,
  rawMessage: string,
  logs: ReadonlyArray<string>,
  candidateThrown: Schema.Json | undefined,
  maxEgressBytes: number,
): CodeModeFailure => {
  const characters = Array.from(boundedMessage(rawMessage));
  const makeMessage = (count: number): string =>
    count >= characters.length ? characters.join("") : `${characters.slice(0, count).join("")}…`;
  const makeEnvelope = (
    message: string,
    envelopeLogs: ReadonlyArray<string>,
    thrown: Schema.Json | undefined,
  ): CodeModeFailure =>
    CodeModeFailure.make({
      errorTag,
      message,
      logs: envelopeLogs,
      ...(thrown === undefined ? {} : { thrown }),
    });

  const fullMessage = makeMessage(characters.length);
  let message = fullMessage;
  if (
    (encodedFailureByteLength(makeEnvelope(fullMessage, [], undefined)) ??
      Number.MAX_SAFE_INTEGER) > maxEgressBytes
  ) {
    let lower = 0;
    let upper = Math.max(0, characters.length - 1);
    message = "";
    while (lower <= upper) {
      const count = Math.floor((lower + upper) / 2);
      const candidateMessage = makeMessage(count);
      const bytes = encodedFailureByteLength(makeEnvelope(candidateMessage, [], undefined));
      if (bytes !== undefined && bytes <= maxEgressBytes) {
        message = candidateMessage;
        lower = count + 1;
      } else {
        upper = count - 1;
      }
    }
  }

  const thrown =
    candidateThrown !== undefined &&
    (encodedFailureByteLength(makeEnvelope(message, [], candidateThrown)) ??
      Number.MAX_SAFE_INTEGER) <= maxEgressBytes
      ? candidateThrown
      : undefined;
  const envelopeWithLogs = (envelopeLogs: ReadonlyArray<string>) =>
    makeEnvelope(message, envelopeLogs, thrown);
  const fittedLogs = budgetedEnvelopeLogs(
    logs,
    maxEgressBytes,
    envelopeWithLogs,
    encodedFailureByteLength,
  );
  return envelopeWithLogs(fittedLogs);
};

/**
 * The failure half of the aggregate egress policy (CAP-016): the configured
 * redaction pass covers failure logs and thrown values exactly like success
 * egress — a program cannot leak by logging and then throwing — and the
 * message itself is bounded by the budget, not only by its own schema cap.
 */
const failureEgress = (
  error: CodeExecutionError | ToolBrokerUnavailableError | ToolBrokerConfigurationError,
  maxEgressBytes: number,
  redact: EgressRedactor | undefined,
): Effect.Effect<CodeModeFailure> =>
  Effect.gen(function* () {
    if (
      error._tag === "ToolBrokerUnavailableError" ||
      error._tag === "ToolBrokerConfigurationError"
    ) {
      return budgetedFailure(error._tag, error.message, [], undefined, maxEgressBytes);
    }
    let logs: ReadonlyArray<string> = "logs" in error ? error.logs : [];
    let candidateThrown = error._tag === "CodeProgramFailedError" ? error.thrown : undefined;
    if (redact !== undefined) {
      const redacted = yield* redact({ result: candidateThrown ?? null, logs });
      logs = redacted.logs;
      candidateThrown = candidateThrown === undefined ? undefined : redacted.result;
    }
    return budgetedFailure(
      error._tag,
      executionFailureMessage(error),
      logs,
      candidateThrown,
      maxEgressBytes,
    );
  });

// ---------------------------------------------------------------------------
// Builder
// ---------------------------------------------------------------------------

const make = <const Name extends string, Namespaces extends CodeModeNamespaces>(
  name: Name,
  options: CodeModeOptions<Namespaces>,
): CodeModeDefinition<Name, Namespaces> => {
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
      const executionClass = getToolExecutionClass(tool);
      if (executionClass !== "readonly") {
        throw new Error(
          `Code Mode rejects Tool ${tool.name} (${namespace}.${method}): its execution class is ${executionClass}; the first slice accepts only Tools annotated readonly (an unannotated Tool reads as uncertain)`,
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
    "Namespace methods return Promises. An expected Tool failure rejects with a JSON envelope carrying a stable `_tag`; catch it to branch. Calls are strictly sequential — issue one host call at a time.",
    "",
    "Sandbox globals:",
    "```ts",
    declarations,
    "```",
  ].join("\n");

  const tool = Tool.make(name, {
    description,
    parameters: CodeModeParameters,
    success: CodeModeSuccess,
    failure: CodeModeFailure,
    failureMode: "return",
  })
    .annotate(Tool.Readonly, true)
    .annotate(ToolExecutionClassAnnotation, "readonly")
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

  const executionRequest = (code: string): CodeExecutionRequest =>
    CodeExecutionRequest.make({
      language: "javascript",
      source: code,
      namespaces,
      network: NetworkDisabled.make({}),
      limits,
    });

  const routeHostCall = (
    pass: ToolBrokerPass,
    hostCall: CodeHostCall,
  ): Effect.Effect<CodeHostCallResult> =>
    Effect.gen(function* () {
      const toolName = methodToTool.get(`${hostCall.namespace}.${hostCall.method}`);
      if (toolName === undefined) {
        return {
          _tag: "CodeHostCallFailure",
          error: {
            _tag: "UnknownCodeModeMethod",
            message: `${hostCall.namespace}.${hostCall.method} is not an allowlisted method`,
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

  const successEgress = (
    execution: CodeExecutionResult,
  ): Effect.Effect<CodeModeSuccess, CodeModeFailure> =>
    Effect.gen(function* () {
      let egress: { readonly result: Schema.Json; readonly logs: ReadonlyArray<string> } = {
        result: execution.value,
        logs: execution.logs,
      };
      if (options.redactEgress !== undefined) {
        egress = yield* options.redactEgress(egress);
      }
      const withoutLogs = CodeModeSuccess.make({ result: egress.result, logs: [] });
      const envelopeBytes = encodedSuccessByteLength(withoutLogs);
      if (envelopeBytes === undefined || envelopeBytes > maxEgressBytes) {
        const resultBytes = encodedJsonByteLength(egress.result);
        return yield* budgetedFailure(
          "CodeModeEgressExceeded",
          `The program result of ${resultBytes ?? "unencodable"} bytes exceeds the ${maxEgressBytes}-byte model-visible egress budget; return a smaller value`,
          egress.logs,
          undefined,
          maxEgressBytes,
        );
      }
      const makeEnvelope = (logs: ReadonlyArray<string>) =>
        CodeModeSuccess.make({ result: egress.result, logs });
      return makeEnvelope(
        budgetedEnvelopeLogs(egress.logs, maxEgressBytes, makeEnvelope, encodedSuccessByteLength),
      );
    });

  const build = Effect.gen(function* () {
    const captured = yield* Effect.context<never>();
    const withHandler = yield* selectedToolkit;
    const executor = yield* CodeExecutor;

    const invoke = Effect.fn(`CodeMode.${name}`)(function* (parameters: { readonly code: string }) {
      const broker = yield* ToolBroker;
      const execution = Effect.gen(function* () {
        const pass = yield* broker.openPass(withHandler, {
          maxResultBytes: limits.maxHostCallResultBytes,
        });
        const host = CodeExecutionHost.of({
          call: (hostCall) => routeHostCall(pass, hostCall),
        });
        return yield* executor
          .execute(executionRequest(parameters.code))
          .pipe(Effect.provideService(CodeExecutionHost, host));
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
      const result = yield* execution.pipe(
        Effect.catch((error) =>
          failureEgress(error, maxEgressBytes, options.redactEgress).pipe(
            Effect.flatMap((failure) => Effect.fail(failure)),
          ),
        ),
      );
      return yield* successEgress(result);
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
      CodeExecutor | Tool.HandlersFor<CodeModeSelectedRecord<Namespaces>>
    >,
  ) as unknown as Layer.Layer<
    Tool.HandlersFor<CodeModeTools<Name>>,
    never,
    CodeModeLayerRequirements<Namespaces>
  >;

  return Object.freeze({
    name,
    description,
    declarations,
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
import { ToolExecutionClass as ToolExecutionClassAnnotation } from "@effect-agent/engine";

/** Code Mode builder namespace (capability spec §9.1). */
export const CodeMode = { make } as const;

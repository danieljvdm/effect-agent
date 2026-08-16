import type { LogLevel } from "effect";
import { Cause, Config, Effect, Layer, Logger, Predicate, References } from "effect";

// ---------------------------------------------------------------------------
// Compact host logging for CI runs. The engine's telemetry logs carry full
// OTel-style annotation sets — correct for an exporter, unreadable as an
// Actions console. This logger renders each record as ONE line: known engine
// telemetry messages become short progress lines, and everything else keeps
// its message with annotations compacted (warnings and errors only). It is
// presentation only — record filtering stays with MinimumLogLevel, and
// nothing here feeds back into the run.
// ---------------------------------------------------------------------------

/** Everything one compact line is rendered from; pure and directly testable. */
export interface CompactLogRecord {
  readonly date: Date;
  readonly logLevel: LogLevel.LogLevel;
  readonly message: unknown;
  readonly annotations: Readonly<Record<string, unknown>>;
  /** Pretty-rendered Cause, present only when the record carries one. */
  readonly cause?: string | undefined;
}

const levelTag: Partial<Record<LogLevel.LogLevel, string>> = {
  Trace: "trace",
  Debug: "debug",
  Warn: "WARN",
  Error: "ERROR",
  Fatal: "FATAL",
};

const asText = (value: unknown): string => {
  if (Predicate.isString(value)) return value;
  try {
    return JSON.stringify(value) ?? String(value);
  } catch {
    return String(value);
  }
};

const annotationText = (annotations: Readonly<Record<string, unknown>>): string =>
  Object.entries(annotations)
    // Dotted keys are the OTel-convention duplicates of the camelCase
    // annotations; one copy per line is enough for a human console.
    .filter(([key]) => !key.includes("."))
    .map(([key, value]) => `${key}=${asText(value).slice(0, 120)}`)
    .join(" ");

/** Known engine telemetry messages, rendered as short progress lines. */
const telemetryLine = (
  message: string,
  annotations: Readonly<Record<string, unknown>>,
): string | undefined => {
  const toolName = asText(annotations["toolName"] ?? "tool");
  switch (message) {
    case "agent tool execution completed":
      return `✓ tool ${toolName}`;
    case "agent tool execution failed":
      return `✗ tool ${toolName} failed`;
    case "agent tool handler started":
    case "agent programmatic tool handler started":
      return `→ tool ${toolName}`;
    case "agent model call started":
      return `→ model call`;
    case "agent run started":
      return `→ agent ${asText(annotations["agentId"] ?? "run")} started`;
    default:
      return undefined;
  }
};

/** Render one log record as a single compact console line. */
export const formatCompactLogLine = (record: CompactLogRecord): string => {
  const time = record.date.toISOString().slice(11, 19);
  const messages = Array.isArray(record.message) ? record.message : [record.message];
  const messageText = messages.map(asText).join(" ");
  const mapped =
    messages.length === 1 && Predicate.isString(messages[0])
      ? telemetryLine(messages[0], record.annotations)
      : undefined;
  const tag = levelTag[record.logLevel];
  let line: string;
  if (mapped !== undefined) {
    line = `[${time}] ${tag === undefined ? "" : `${tag} `}${mapped}`;
  } else {
    const isSevere =
      record.logLevel === "Warn" || record.logLevel === "Error" || record.logLevel === "Fatal";
    const annotations = isSevere ? annotationText(record.annotations) : "";
    line = `[${time}] ${tag === undefined ? "" : `${tag} `}${messageText}${
      annotations === "" ? "" : ` · ${annotations}`
    }`;
  }
  return record.cause === undefined ? line : `${line}\n${record.cause}`;
};

/** The compact Logger; annotations come from the emitting fiber. */
export const compactReviewLogger: Logger.Logger<unknown, string> = Logger.make((options) =>
  formatCompactLogLine({
    date: options.date,
    logLevel: options.logLevel,
    message: options.message,
    annotations: options.fiber.getRef(References.CurrentLogAnnotations),
    cause: options.cause.reasons.length > 0 ? Cause.pretty(options.cause) : undefined,
  }),
);

/**
 * Install the compact console logger and the minimum level for one host run.
 * PR_REVIEW_LOG_LEVEL widens visibility (e.g. "Debug" shows the engine's
 * per-turn and per-handler telemetry); an unknown value fails loudly like
 * every other configuration fault.
 */
export const compactReviewLoggingLayer: Layer.Layer<never, Config.ConfigError> = Layer.unwrap(
  Effect.gen(function* () {
    const level = yield* Config.literals(
      ["All", "Trace", "Debug", "Info", "Warn", "Error"],
      "PR_REVIEW_LOG_LEVEL",
    ).pipe(Config.withDefault<LogLevel.LogLevel>("Info"));
    return Layer.merge(
      Logger.layer([Logger.withLeveledConsole(compactReviewLogger)]),
      Layer.succeed(References.MinimumLogLevel, level),
    );
  }),
);

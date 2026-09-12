import { toolFailureObserverLayer } from "@effect-agent/engine/RunOptions";
import { Context, DateTime, Effect, Layer, Option, Predicate, Redacted, Schema } from "effect";
import { SqlClient } from "effect/unstable/sql/SqlClient";

import { PlannerError } from "../domain.ts";

export const diagnosticTextLimit = 65_536;

const secretKey =
  /authorization|cookie|password|secret|credential|api[_-]?key|(?:access|refresh|session|security|id)[_-]?token$|^token$|^jwt$|assertion|signature|encrypted[_-]?content|private[_-]?reasoning|reasoning[_-]?(?:text|content|details)/i;

export const redactDiagnosticText = (text: string): string =>
  text
    .replace(/\bsk-[a-zA-Z0-9_-]{12,}/g, "[redacted]")
    .replace(/\b(Bearer|Basic)\s+[a-zA-Z0-9._~+/=-]+/gi, "$1 [redacted]")
    .replace(
      /("(?:authorization|cookie|set-cookie|password|secret|api[_-]?key|access[_-]?token|refresh[_-]?token|token)"\s*:\s*)"(?:[^"\\]|\\.)*"/gi,
      '$1"[redacted]"',
    )
    .replace(
      /((?:authorization|cookie|set-cookie|password|secret|api[_-]?key|access[_-]?token|refresh[_-]?token|token)\s*[=:]\s*)[^\s,;]+/gi,
      "$1[redacted]",
    )
    .replace(/https?:\/\/[^\s"'<>\\]+/gi, (raw) => {
      if (!URL.canParse(raw)) return "[unparseable URL]";
      const url = new URL(raw);

      if (url.username || url.password) {
        url.username = "redacted";
        url.password = "";
      }
      for (const key of new Set(url.searchParams.keys()))
        if (secretKey.test(key) || /token|signature|credential|key|auth|policy/i.test(key))
          url.searchParams.set(key, "[redacted]");

      return url.href;
    });

/** Inspect data properties only: Error.toJSON, getters, and provider objects never execute here. */
export const diagnosticDetail = (value: unknown, limit = diagnosticTextLimit) => {
  const seen = new WeakSet<object>();
  let nodes = 0;
  let truncated = false;

  const bounded = () => {
    truncated = true;

    return "[diagnostic limit reached]";
  };

  const visit = (input: unknown, depth: number): Schema.Json => {
    if (++nodes > 2_048 || depth > 12) return bounded();
    if (Redacted.isRedacted(input)) return "[redacted]";
    if (input === null || input === undefined) return null;
    if (typeof input === "boolean") return input;
    if (typeof input === "number") return Number.isFinite(input) ? input : String(input);
    if (typeof input === "string") {
      // Provider bodies frequently contain JSON inside an Error.message or a body field.
      if (input.length < diagnosticTextLimit && /^[\s]*[[{]/.test(input)) {
        const decoded = Schema.decodeOption(Schema.fromJsonString(Schema.Json))(input);

        if (Option.isSome(decoded)) return visit(decoded.value, depth + 1);
      }
      const text = redactDiagnosticText(input);

      if (text.length > limit) {
        truncated = true;

        return text.slice(0, limit) + "[truncated]";
      }

      return text;
    }
    if (!Predicate.isObjectOrArray(input)) return `[${typeof input}]`;
    if (seen.has(input)) return "[circular reference]";
    seen.add(input);
    try {
      if (Array.isArray(input)) {
        if (input.length > 256) truncated = true;

        return Array.from({ length: Math.min(input.length, 256) }, (_, index) => {
          const descriptor = Object.getOwnPropertyDescriptor(input, index);

          return descriptor === undefined
            ? null
            : "value" in descriptor
              ? visit(descriptor.value, depth + 1)
              : "[accessor omitted]";
        });
      }
      const descriptors = Object.getOwnPropertyDescriptors(input);

      if (descriptors.type?.value === "reasoning") return "[private reasoning excluded]";
      const entries = Object.entries(descriptors);

      if (entries.length > 256) truncated = true;
      const result: Record<string, Schema.Json> = {};

      if (input instanceof Error) result.name = "Error";
      for (const [key, descriptor] of entries.slice(0, 256))
        if (key !== "__proto__")
          result[key] = secretKey.test(key)
            ? "[redacted]"
            : "value" in descriptor
              ? visit(descriptor.value, depth + 1)
              : "[accessor omitted]";

      return result;
    } catch {
      return "[uninspectable diagnostic]";
    }
  };

  let text: string;

  try {
    text = JSON.stringify(visit(value, 0), null, 2);
  } catch {
    text = '"[uninspectable diagnostic]"';
    truncated = true;
  }

  return {
    text: text.length <= limit ? text : text.slice(0, limit - 40) + "\n[diagnostic truncated]",
    truncated: truncated || text.length > limit,
  };
};

const Identity = Schema.String.check(Schema.isMaxLength(512));

export const FailureDiagnostic = Schema.Struct({
  version: Schema.Literal(1),
  timestamp: Schema.String,
  operation: Schema.String.check(Schema.isMaxLength(240)),
  submissionId: Schema.optionalKey(Identity),
  attemptId: Schema.optionalKey(Identity),
  runId: Schema.optionalKey(Identity),
  toolCallId: Schema.optionalKey(Identity),
  durationMs: Schema.optionalKey(Schema.Natural),
  text: Schema.String.check(Schema.isMaxLength(diagnosticTextLimit)),
  truncated: Schema.Boolean,
});

export type FailureDiagnostic = typeof FailureDiagnostic.Type;

export const RecordedDiagnostic = Schema.Struct({
  id: Schema.Natural,
  ...FailureDiagnostic.fields,
});

export const RecordedDiagnostics = Schema.Array(RecordedDiagnostic);

export const DiagnosticContext = Context.Reference<{
  readonly submissionId?: string;
  readonly attemptId?: string;
}>("travel-planner/DiagnosticContext", { defaultValue: () => ({}) });

export const DiagnosticFailpoint = Context.Reference<{
  readonly hit: (
    point: "schema:before" | "schema:after" | "append:before" | "append:after",
  ) => Effect.Effect<void, PlannerError>;
}>("travel-planner/DiagnosticFailpoint", { defaultValue: () => ({ hit: () => Effect.void }) });

export class FailureDiagnostics extends Context.Service<
  FailureDiagnostics,
  {
    readonly append: (diagnostic: FailureDiagnostic) => Effect.Effect<void, PlannerError>;
    readonly list: Effect.Effect<typeof RecordedDiagnostics.Type, PlannerError>;
  }
>()("travel-planner/FailureDiagnostics") {}

const unavailable = () =>
  new PlannerError({
    code: "storage",
    message: "Failure diagnostics could not be stored or read.",
  });

/** Separate, append-only per-Thread operator data; never prompt history or recovery authority. */
export const FailureDiagnosticsLive = Layer.effect(
  FailureDiagnostics,
  Effect.gen(function* () {
    const sql = yield* SqlClient;
    const failpoint = yield* DiagnosticFailpoint;

    yield* failpoint.hit("schema:before");
    yield* sql`CREATE TABLE IF NOT EXISTS travel_failure_diagnostics (id INTEGER PRIMARY KEY AUTOINCREMENT, value TEXT NOT NULL)`.pipe(
      Effect.mapError(unavailable),
    );
    yield* failpoint.hit("schema:after");

    return FailureDiagnostics.of({
      append: Effect.fn("FailureDiagnostics.append")(function* (diagnostic) {
        const value = yield* Schema.encodeEffect(Schema.fromJsonString(FailureDiagnostic))(
          diagnostic,
        ).pipe(Effect.mapError(unavailable));

        yield* failpoint.hit("append:before");
        yield* sql`INSERT INTO travel_failure_diagnostics (value) VALUES (${value})`.pipe(
          Effect.mapError(unavailable),
        );
        yield* failpoint.hit("append:after");
      }),
      list: sql`SELECT id, value FROM travel_failure_diagnostics ORDER BY id DESC LIMIT 100`.pipe(
        Effect.flatMap(
          Schema.decodeUnknownEffect(
            Schema.Array(Schema.Struct({ id: Schema.Natural, value: Schema.String })),
          ),
        ),
        Effect.flatMap((rows) =>
          Effect.forEach(rows, (row) =>
            Schema.decodeEffect(Schema.fromJsonString(FailureDiagnostic))(row.value).pipe(
              Effect.map((value) => ({ id: row.id, ...value })),
            ),
          ),
        ),
        Effect.mapError(unavailable),
      ),
    });
  }),
);

/** Diagnostic failure must not change a Tool/Model outcome or recursively report itself. */
export const recordDiagnostic = Effect.fn("recordDiagnostic")(function* (
  operation: string,
  value: unknown,
  identity: {
    readonly toolCallId?: string;
    readonly runId?: string;
    readonly durationMs?: number;
  } = {},
) {
  const store = yield* Effect.serviceOption(FailureDiagnostics);

  if (Option.isNone(store)) return;
  const context = yield* DiagnosticContext;

  const diagnostic = {
    version: 1 as const,
    timestamp: DateTime.formatIso(yield* DateTime.now),
    operation: operation.slice(0, 240),
    ...context,
    ...identity,
    ...diagnosticDetail({ ...context, ...identity, data: value }),
  };

  yield* store.value.append(diagnostic).pipe(
    Effect.timeout("2 seconds"),
    Effect.catchCause(() => Effect.logWarning("Could not persist failure diagnostics", diagnostic)),
  );
});

export const readDiagnostics = Effect.serviceOption(FailureDiagnostics).pipe(
  Effect.flatMap((store) => (Option.isSome(store) ? store.value.list : Effect.succeed([]))),
  Effect.catchCause(() => Effect.succeed([])),
);

/** The engine supplies correlation for returned failures, and original causes for broker failures. */
export const DiagnosticObserverLive = Layer.unwrap(
  Effect.gen(function* () {
    const context = yield* Effect.context<FailureDiagnostics>();

    return toolFailureObserverLayer({
      observe: (observation) =>
        recordDiagnostic(`${observation.toolName}: ${observation.kind}`, observation, {
          runId: observation.runId,
          ...("toolCallId" in observation ? { toolCallId: observation.toolCallId } : {}),
        }).pipe(Effect.provide(context)),
    });
  }),
);

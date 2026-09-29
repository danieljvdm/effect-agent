import { OpenAiClient } from "@effect/ai-openai";
import {
  Cause,
  Clock,
  Context,
  DateTime,
  Effect,
  Exit,
  Option,
  Schema,
  Stream,
  Tracer,
} from "effect";
import { Telemetry } from "effect/unstable/ai";

import {
  browserCommandTimeoutMillis,
  LabError,
  type Phase,
  type Report,
  type RunInput,
  Span,
} from "./contract.ts";

type Details = Partial<
  Pick<
    Span,
    | "inputTokens"
    | "outputTokens"
    | "reasoningTokens"
    | "reasoningSummary"
    | "serviceTier"
    | "bytes"
    | "candidateCount"
    | "questionCount"
    | "model"
    | "choices"
    | "decisionDistributions"
  >
>;

export const makeTrace = Effect.fnUntraced(function* (input: RunInput, model: string) {
  const clock = yield* Clock.Clock;
  const origin = clock.monotonicTimeNanosUnsafe();
  const startedAt = DateTime.formatIso(yield* DateTime.now);
  const spans: Array<Span> = [];
  let turn = 0;
  const now = () => Number(clock.monotonicTimeNanosUnsafe() - origin) / 1_000_000;

  let report: Report = {
    version: 1,
    fixture: input.scenario === "wikipedia" ? "wikipedia-race-v1" : "task-board-v1",
    input,
    model,
    commandTimeoutMillis: browserCommandTimeoutMillis,
    startedAt,
    status: "running",
    message: "Opening browser…",
    spans,
    elapsed: 0,
    timing: "page-ready-v1",
    readyAt: null,
    finishedAt: null,
    verifiedAt: null,
    firstActionAt: null,
    cleanup: "pending",
    board: [],
  };

  const begin = (phase: Phase, name: string) => {
    if (phase === "model") turn++;
    const id = spans.length;
    const start = now();
    const spanTurn = turn;

    spans.push({ id, phase, name, start, duration: null, outcome: "running", turn: spanTurn });

    const end = (exit: Exit.Exit<unknown, unknown>, details: Details = {}) => {
      const failure = Exit.isFailure(exit)
        ? Option.flatMap(Cause.findErrorOption(exit.cause), Schema.decodeUnknownOption(LabError))
        : Option.none();

      spans[id] = {
        ...spans[id],
        ...details,
        ...(Option.isSome(failure) ? { error: failure.value.message } : {}),
        duration: now() - start,
        outcome: Exit.isSuccess(exit)
          ? "success"
          : Cause.hasInterrupts(exit.cause)
            ? "interrupted"
            : Cause.hasDies(exit.cause)
              ? "defect"
              : "failure",
      };
      if (phase === "action" && Exit.isSuccess(exit) && report.firstActionAt === null)
        report = { ...report, firstActionAt: now() };
    };

    return {
      end,
      annotate: (details: Details) => {
        const span = spans[id];

        if (span !== undefined) spans[id] = { ...span, ...details };
      },
    };
  };

  return {
    now,
    begin,
    // Called only after the initial page and observation are ready, before any decision or action.
    // Never reset this boundary: a flow failure must not become a preparation retry.
    ready: () => {
      if (report.readyAt === null) report = { ...report, readyAt: now() };
    },
    // The lab permits one model call at a time; provider evidence belongs to its active span.
    annotateModel: (details: Details) => {
      const index = spans.findLastIndex(
        (span) => span.phase === "model" && span.outcome === "running",
      );

      if (index >= 0) spans[index] = { ...spans[index], ...details };
    },
    annotateDecision: (details: Details) => {
      const index = spans.findLastIndex(
        (span) => span.phase === "decision" && span.outcome === "running",
      );

      if (index >= 0) spans[index] = { ...spans[index], ...details };
    },
    snapshot: (): Report => ({
      ...report,
      spans: [...spans],
      elapsed: report.status === "running" ? now() : report.elapsed,
    }),
    update: (patch: Partial<Report>) => {
      report = { ...report, ...patch };
    },
    measure: Effect.fnUntraced(function* <A, E, R>(
      phase: Phase,
      name: string,
      effect: Effect.Effect<A, E, R>,
      details?: (value: A) => Details,
    ) {
      const { end } = begin(phase, name);

      return yield* effect.pipe(
        Effect.onExit((exit) =>
          Effect.sync(() => end(exit, Exit.isSuccess(exit) ? details?.(exit.value) : undefined)),
        ),
      );
    }),
  };
});

export class Trace extends Context.Service<Trace, Effect.Success<ReturnType<typeof makeTrace>>>()(
  "browser-speed/Trace",
) {}

const decodeServedTier = Schema.decodeUnknownOption(
  Schema.Struct({
    type: Schema.Literals(["response.completed", "response.incomplete", "response.failed"]),
    response: Schema.Struct({ service_tier: Schema.String }),
  }),
);

/** Preserve the raw served tier: the pinned provider omits the new "fast" alias from finish metadata. */
export const traceOpenAiClient = Effect.gen(function* () {
  const client = yield* OpenAiClient.OpenAiClient;
  const trace = yield* Trace;

  return OpenAiClient.OpenAiClient.of({
    ...client,
    createResponse: (input) =>
      client.createResponse(input).pipe(
        Effect.tap(([response]) =>
          Effect.sync(() => {
            if (response.service_tier !== undefined)
              trace.annotateModel({ serviceTier: response.service_tier });
          }),
        ),
      ),
    createResponseStream: (input) =>
      client.createResponseStream(input).pipe(
        Effect.map(
          ([response, events]) =>
            [
              response,
              events.pipe(
                Stream.tap((event) =>
                  Effect.sync(() => {
                    const evidence = decodeServedTier(event);

                    if (Option.isSome(evidence))
                      trace.annotateModel({ serviceTier: evidence.value.response.service_tier });
                  }),
                ),
              ),
            ] as const,
        ),
      ),
  });
});

const decodeSelection = Schema.decodeUnknownOption(
  Schema.Struct({
    choices: Span.fields.choices,
    inputTokens: Span.fields.inputTokens,
    outputTokens: Span.fields.outputTokens,
  }),
);

/** Tap native model and browser selection spans without replacing either model service. */
export const traceModels = Effect.fnUntraced(function* <A, E, R>(effect: Effect.Effect<A, E, R>) {
  const trace = yield* Trace;
  const delegate = yield* Tracer.Tracer;
  const parentTransformer = yield* Effect.serviceOption(Telemetry.CurrentSpanTransformer);
  const handles = new Map<string, ReturnType<typeof trace.begin>>();

  const tracer = Tracer.make({
    span(options) {
      const span = delegate.span(options);
      const selection = options.name === "BrowserUse.selectTargets";

      if (
        !selection &&
        !options.name.startsWith("chat ") &&
        !options.name.startsWith("LanguageModel.")
      )
        return span;

      const finish = trace.begin(
        selection ? "decision" : "model",
        selection ? "Jev · select controls" : options.name,
      );

      if (!selection) handles.set(span.spanId, finish);

      return {
        _tag: span._tag,
        name: span.name,
        spanId: span.spanId,
        traceId: span.traceId,
        parent: span.parent,
        annotations: span.annotations,
        attributes: span.attributes,
        links: span.links,
        sampled: span.sampled,
        kind: span.kind,
        attribute: span.attribute.bind(span),
        event: span.event.bind(span),
        addLinks: span.addLinks.bind(span),
        get status() {
          return span.status;
        },
        end(time, exit) {
          const details = selection
            ? decodeSelection(span.attributes.get("browser.selection"))
            : Option.none();

          finish.end(
            exit,
            selection
              ? {
                  model: "jev-latest",
                  ...(Option.isSome(details) ? details.value : {}),
                }
              : {},
          );
          span.end(time, exit);
        },
      };
    },
    ...(delegate.context === undefined ? {} : { context: delegate.context.bind(delegate) }),
  });

  return yield* effect.pipe(
    Effect.provideService(Tracer.Tracer, tracer),
    Effect.provideService(Telemetry.CurrentSpanTransformer, (options) => {
      if (Option.isSome(parentTransformer)) parentTransformer.value(options);
      const { span, response } = options;
      let value: Details = {};

      for (const part of response) {
        if (part.type === "response-metadata" && part.modelId !== undefined)
          value = { ...value, model: part.modelId };
        if (part.type === "reasoning")
          value = {
            ...value,
            reasoningSummary: `${value.reasoningSummary ?? ""}${part.text}`.slice(0, 16_000),
          };
        if (part.type === "reasoning-delta")
          value = {
            ...value,
            reasoningSummary: `${value.reasoningSummary ?? ""}${part.delta}`.slice(0, 16_000),
          };
        if (part.type === "finish")
          value = {
            ...value,
            ...(part.usage.inputTokens.total === undefined
              ? {}
              : { inputTokens: part.usage.inputTokens.total }),
            ...(part.usage.outputTokens.total === undefined
              ? {}
              : { outputTokens: part.usage.outputTokens.total }),
            ...(part.usage.outputTokens.reasoning === undefined
              ? {}
              : { reasoningTokens: part.usage.outputTokens.reasoning }),
          };
      }
      // Streaming response metadata can finalize after the native timing span.
      handles.get(span.spanId)?.annotate(value);
    }),
  );
});

import { Cause, Clock, Context, Effect, Exit, Option, Tracer } from "effect";
import { Telemetry } from "effect/unstable/ai";

import type { CheckoutSpan } from "./checkout-contract.ts";

type Span = typeof CheckoutSpan.Type;
type Details = Partial<
  Pick<
    Span,
    | "requestedModel"
    | "resolvedModel"
    | "tools"
    | "inputTokens"
    | "outputTokens"
    | "observationBytes"
  >
>;

/** Synchronous storage is required by native tracer callbacks and atomic request allocation. */
export class CheckoutTelemetryStore extends Context.Service<
  CheckoutTelemetryStore,
  {
    readonly allocateRequestUnsafe: () => number;
    readonly recordUnsafe: (span: Span) => void;
  }
>()("checkout/TelemetryStore") {}

/** A request owns its clock and turn counter. Never serialize absolute monotonic timestamps. */
export const makeTelemetry = Effect.fnUntraced(function* () {
  const store = yield* CheckoutTelemetryStore;
  const clock = yield* Clock.Clock;
  const origin = clock.monotonicTimeNanosUnsafe();
  let request: number | undefined;
  let sequence = 0;
  let turn = 0;
  const millis = () => Number(clock.monotonicTimeNanosUnsafe() - origin) / 1_000_000;

  return {
    begin(phase: Span["phase"], operation: string, details: Details = {}) {
      // Allocate beside the first synchronous write, after any request-body suspension.
      request ??= store.allocateRequestUnsafe();
      if (phase === "model" || phase === "decision" || phase === "text") turn++;

      const span: Span = {
        id: `${request}:${sequence++}`,
        request,
        ...(turn === 0 ? {} : { turn }),
        phase,
        operation,
        offsetMillis: millis(),
        outcome: "running",
        ...details,
      };

      store.recordUnsafe(span);

      return (exit: Exit.Exit<unknown, unknown>, extra: Details = {}) => {
        const outcome = Exit.isSuccess(exit)
          ? "completed"
          : Cause.hasInterrupts(exit.cause)
            ? "interrupted"
            : Cause.hasDies(exit.cause)
              ? "defect"
              : "failure";

        store.recordUnsafe({
          ...span,
          ...extra,
          elapsedMillis: Math.max(0, millis() - span.offsetMillis),
          outcome,
          ...(Exit.isFailure(exit) ? { error: outcome } : {}),
        });
      };
    },
  };
});

export const CheckoutTelemetry = Context.Reference<
  Effect.Success<ReturnType<typeof makeTelemetry>> | undefined
>("checkout/Telemetry", { defaultValue: () => undefined });

export const measured = Effect.fnUntraced(function* <A, E, R>(
  phase: Span["phase"],
  operation: string,
  effect: Effect.Effect<A, E, R>,
  details: Details = {},
  resultDetails?: (value: A) => Details,
) {
  const telemetry = yield* CheckoutTelemetry;

  if (telemetry === undefined) return yield* effect;
  const finish = telemetry.begin(phase, operation, details);

  return yield* effect.pipe(
    Effect.onExit((exit) =>
      Effect.sync(() =>
        finish(exit, Exit.isSuccess(exit) ? resultDetails?.(exit.value) : undefined),
      ),
    ),
  );
});

/** Use native Effect AI spans and response metadata without replacing its service or validation. */
export const instrumentModels = Effect.fnUntraced(function* <A, E, R>(
  effect: Effect.Effect<A, E, R>,
  requestedModel: string,
) {
  const delegate = yield* Tracer.Tracer;
  const telemetry = yield* CheckoutTelemetry;
  const parentTransformer = yield* Effect.serviceOption(Telemetry.CurrentSpanTransformer);
  const responses = new Map<string, Details>();

  const tracer = Tracer.make({
    span(options) {
      const span = delegate.span(options);

      if (!options.name.startsWith("chat ") && !options.name.startsWith("LanguageModel."))
        return span;
      const finish = telemetry?.begin("model", options.name, { requestedModel });

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
          finish?.(exit, responses.get(span.spanId));
          responses.delete(span.spanId);
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
      const tools: Array<string> = [];
      let details: Details = {};

      for (const part of options.response) {
        if (part.type === "tool-call") tools.push(part.name);
        if (part.type === "response-metadata" && part.modelId !== undefined)
          details = { ...details, resolvedModel: part.modelId };
        if (part.type === "finish")
          details = {
            ...details,
            ...(part.usage.inputTokens.total === undefined
              ? {}
              : { inputTokens: part.usage.inputTokens.total }),
            ...(part.usage.outputTokens.total === undefined
              ? {}
              : { outputTokens: part.usage.outputTokens.total }),
          };
      }
      responses.set(options.span.spanId, { ...details, tools });
    }),
  );
});

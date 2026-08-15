import {
  Cause,
  Context,
  Effect,
  ErrorReporter,
  Exit,
  type Fiber,
  Layer,
  Option,
  Stream,
  Tracer,
} from "effect";
import { AiError } from "effect/unstable/ai";

/**
 * @internal Payload-free control signal used only to close the canonical Tool span with a failed
 * Exit. It is removed immediately outside that span and never enters the public error channel.
 */
export class ToolSpanFailure extends AiError.AiError.extend<ToolSpanFailure>(
  "@effect-agent/engine/ToolSpanFailure",
)({}) {
  override readonly name = "ToolCallFailed";

  /** Schema-safe factory retaining the generated constructor required by Effect Schema. */
  static marker(): ToolSpanFailure {
    return ToolSpanFailure.make({
      module: "effect-agent",
      method: "execute_tool",
      reason: AiError.UnknownError.make({
        description: "Tool execution reached a failed terminal state",
      }),
    });
  }
}

const TOOL_SPAN_FAILURE_MARKER_ATTRIBUTE = "@effect-agent/engine/ToolSpanFailureMarker";
const terminalOutcomeTokens = new WeakMap<
  object,
  {
    readonly outcome: "success" | "failure";
    readonly failureMarker: ToolSpanFailure | undefined;
  }
>();

/** @internal Annotate one completed attempt and hand its existing marker to the isolated span. */
export const annotateToolSpanTerminalOutcome = (
  outcome: "success" | "failure",
  failureMarker?: ToolSpanFailure,
): Effect.Effect<void> => {
  const token = {};
  terminalOutcomeTokens.set(token, { outcome, failureMarker });
  return Effect.annotateCurrentSpan({
    "effect_agent.tool.outcome": outcome,
    [TOOL_SPAN_FAILURE_MARKER_ATTRIBUTE]: token,
  });
};

/**
 * @internal Emit one canonical value from the first pull, then run derivative work from either the
 * next pull or structured stream finalization when the downstream owner stops after that value.
 * The synchronous phase transition gives the action one owner across the pull/finalizer race, and
 * a started action is never retried after interruption.
 */
export const emitThenAfter = <A, E, R, E2, R2>(
  event: Effect.Effect<A, E, R>,
  after: Effect.Effect<void, E2, R2>,
): Stream.Stream<A, E | E2, R | R2> =>
  Stream.unwrap(
    Effect.sync(() => {
      let firstPull = true;
      let afterPhase: "unarmed" | "pending" | "running" | "completed" = "unarmed";
      const runAfter = Effect.suspend((): Effect.Effect<void, E2, R2> => {
        if (afterPhase !== "pending") return Effect.void;
        afterPhase = "running";
        return after.pipe(
          Effect.onExit(() =>
            Effect.sync(() => {
              afterPhase = "completed";
            }),
          ),
        );
      });
      const pull = Effect.suspend(
        (): Effect.Effect<readonly [A], E | E2 | Cause.Done<void>, R | R2> => {
          if (firstPull) {
            return Effect.map(event, (value) => {
              firstPull = false;
              afterPhase = "pending";
              return [value] as const;
            });
          }
          return Effect.andThen(runAfter, Cause.done());
        },
      );
      return Stream.fromPull(Effect.succeed(pull)).pipe(
        Stream.ensuring(
          // A typed `after` failure remains visible to an ordinary second pull. Cleanup only owns
          // the otherwise-unobservable early-close path, so capture its Exit after the action has
          // annotated the canonical span. `IsolatedToolSpan.end` derives a failed export status
          // from that bounded outcome when cleanup consumes the private span marker. Only
          // derivative failure/defect Reasons are suppressed; external interruption is restored.
          Effect.exit(Effect.interruptible(runAfter)).pipe(
            Effect.flatMap((exit) => {
              if (Exit.isSuccess(exit)) return Effect.void;
              return reportDerivativeCause(exit.cause);
            }),
          ),
        ),
      );
    }),
  );

export const stripToolSpanFailures = <E>(
  cause: Cause.Cause<E | ToolSpanFailure>,
  marker: ToolSpanFailure,
): { readonly found: boolean; readonly residual: Cause.Cause<E | ToolSpanFailure> } => {
  const found = cause.reasons.some(
    (reason) => Cause.isFailReason(reason) && reason.error === marker,
  );
  if (!found) {
    // Effect v4 Cause is a flat collection of Reasons. Object-identity absence proves only that
    // this invocation's marker is absent: another ToolSpanFailure may still be a genuine handler
    // failure, so retain both the exact Cause object and its complete error union.
    return { found: false, residual: cause };
  }
  const reasons = cause.reasons.filter((reason) => {
    if (Cause.isFailReason(reason) && reason.error === marker) {
      return false;
    }
    return true;
  });
  return { found: true, residual: Cause.fromReasons(reasons) };
};

/** @internal Remove only the private marker and restore the saved Cause plus every residual. */
export const restoreToolSpanFailureCause = <E, Original>(
  cause: Cause.Cause<E | ToolSpanFailure>,
  marker: ToolSpanFailure,
  original: Cause.Cause<Original> | undefined,
): {
  readonly found: boolean;
  readonly restored: Cause.Cause<E | Original | ToolSpanFailure>;
} => {
  const { found, residual } = stripToolSpanFailures(cause, marker);
  return {
    found,
    restored: original === undefined ? residual : Cause.combine(original, residual),
  };
};

/**
 * @internal Report derivative terminal-telemetry failures without consuming external
 * interruption. Reporter defects are isolated, while interruption of either the measurement or
 * reporter is restored as interruption instead of allowing Tool settlement to continue.
 */
const reportDerivativeCause = <E>(cause: Cause.Cause<E>): Effect.Effect<void> => {
  const reportableReasons: Array<Cause.Reason<E>> = [];
  const interruptionReasons: Array<Cause.Interrupt> = [];
  for (const reason of cause.reasons) {
    if (Cause.isInterruptReason(reason)) interruptionReasons.push(reason);
    else reportableReasons.push(reason);
  }

  const reportExit =
    reportableReasons.length === 0
      ? Effect.succeed(Exit.succeed(undefined))
      : Effect.exit(ErrorReporter.report(Cause.fromReasons(reportableReasons)));
  return Effect.flatMap(reportExit, (exit) => {
    if (Exit.isFailure(exit)) {
      for (const reason of exit.cause.reasons) {
        if (Cause.isInterruptReason(reason)) interruptionReasons.push(reason);
      }
    }
    return interruptionReasons.length === 0
      ? Effect.void
      : Effect.failCause(Cause.fromReasons<never>(interruptionReasons));
  });
};

export const isolateToolTerminalTelemetry = <R>(
  telemetry: Effect.Effect<void, never, R>,
): Effect.Effect<void, never, R> => telemetry.pipe(Effect.catchCause(reportDerivativeCause));

const MAX_REPORTED_SPAN_LIFECYCLE_DEFECTS = 16;

/** Content-free marker used only to close a delegate that cannot be wrapped safely. */
class ToolSpanLifecycleFailure extends Error {
  override readonly name = "ToolSpanLifecycleFailure";

  constructor() {
    super("Tool span lifecycle failed");
  }
}

/**
 * @internal Guard one host span so synchronous exporter defects cannot alter Tool execution.
 * The wrapper retains the span contract for Effect while forwarding every measurement to the host.
 */
class IsolatedToolSpan implements Tracer.Span {
  readonly _tag = "Span";
  readonly name: string;
  readonly spanId: string;
  readonly traceId: string;
  readonly parent: Tracer.Span["parent"];
  readonly annotations: Tracer.Span["annotations"];
  readonly attributes = new Map<string, unknown>();
  readonly links: Array<Tracer.SpanLink>;
  readonly sampled: boolean;
  readonly kind: Tracer.SpanKind;
  status: Tracer.SpanStatus;

  readonly #delegate: Tracer.Span;
  readonly #recordDefect: (defect: unknown) => void;
  #terminalOutcome: "success" | "failure" | undefined;
  #terminalFailure: ToolSpanFailure | undefined;

  constructor(
    delegate: Tracer.Span,
    options: Parameters<Tracer.Tracer["span"]>[0],
    recordDefect: (defect: unknown) => void,
  ) {
    this.#delegate = delegate;
    this.#recordDefect = recordDefect;
    this.name = options.name;
    this.spanId = delegate.spanId;
    this.traceId = delegate.traceId;
    this.parent = options.parent;
    this.annotations = options.annotations;
    this.links = [...options.links];
    // The host tracer owns the final sampling decision. Reading it here keeps that access inside
    // the caller's existing creation try/fallback together with span allocation and identifiers.
    this.sampled = delegate.sampled;
    this.kind = options.kind;
    this.status = { _tag: "Started", startTime: options.startTime };
  }

  end(endTime: bigint, exit: Exit.Exit<unknown, unknown>): void {
    if (this.status._tag === "Ended") return;
    // Early downstream closure can own terminal telemetry from a no-fail stream finalizer, where
    // the private typed marker is intentionally captured because there is no consumer left to
    // observe it. Once present, the bounded outcome annotation is authoritative for this completed
    // attempt even if downstream cancellation or telemetry interruption determines the channel Exit.
    const exportedExit =
      this.#terminalOutcome === "failure"
        ? Exit.fail(this.#terminalFailure ?? new ToolSpanLifecycleFailure())
        : this.#terminalOutcome === "success"
          ? Exit.succeed(undefined)
          : exit;
    this.status = {
      _tag: "Ended",
      startTime: this.status.startTime,
      endTime,
      exit: exportedExit,
    };
    if (this.#terminalOutcome !== undefined) {
      // The public attribute map is intentionally mutable for ordinary instrumentation. Restore
      // this framework-owned value from the authenticated private token at the export boundary so
      // handler or telemetry code cannot forge the canonical terminal classification.
      this.attributes.set("effect_agent.tool.outcome", this.#terminalOutcome);
      try {
        this.#delegate.attribute("effect_agent.tool.outcome", this.#terminalOutcome);
      } catch (defect) {
        this.#recordDefect(defect);
      }
    }
    try {
      this.#delegate.end(endTime, exportedExit);
    } catch (defect) {
      this.#recordDefect(defect);
    }
  }

  attribute(key: string, value: unknown): void {
    if (key === TOOL_SPAN_FAILURE_MARKER_ATTRIBUTE) {
      if (typeof value === "object" && value !== null) {
        const terminal = terminalOutcomeTokens.get(value);
        if (terminal !== undefined) {
          this.#terminalOutcome = terminal.outcome;
          this.#terminalFailure = terminal.failureMarker;
        }
      }
      return;
    }
    this.attributes.set(key, value);
    try {
      this.#delegate.attribute(key, value);
    } catch (defect) {
      this.#recordDefect(defect);
    }
  }

  event(name: string, startTime: bigint, attributes?: Record<string, unknown>): void {
    try {
      this.#delegate.event(name, startTime, attributes);
    } catch (defect) {
      this.#recordDefect(defect);
    }
  }

  addLinks(links: ReadonlyArray<Tracer.SpanLink>): void {
    this.links.push(...links);
    try {
      this.#delegate.addLinks(links);
    } catch (defect) {
      this.#recordDefect(defect);
    }
  }
}

interface IsolatedToolTracer {
  readonly tracer: Tracer.Tracer;
  readonly reportLifecycleDefects: Effect.Effect<void>;
}

/**
 * @internal Isolate the complete canonical span lifecycle. A creation defect receives a local,
 * non-exported span so the handler still runs; delegate method/finalization defects are retained
 * as bounded reporter diagnostics and never enter the handler stream Cause.
 */
export const makeIsolatedToolTracer = (delegate: Tracer.Tracer): IsolatedToolTracer => {
  const defects: Array<unknown> = [];
  const recordDefect = (defect: unknown): void => {
    if (defects.length < MAX_REPORTED_SPAN_LIFECYCLE_DEFECTS) defects.push(defect);
  };
  let delegateContext: Tracer.Tracer["context"];
  try {
    delegateContext = delegate.context?.bind(delegate);
  } catch (defect) {
    recordDefect(defect);
    delegateContext = undefined;
  }
  const context: Tracer.Tracer["context"] =
    delegateContext === undefined
      ? undefined
      : <X>(primitive: Tracer.EffectPrimitive<X>, fiber: Fiber.Fiber<unknown, unknown>): X => {
          type Evaluation =
            | { readonly _tag: "Pending" }
            | { readonly _tag: "Succeeded"; readonly value: X }
            | { readonly _tag: "Failed"; readonly error: unknown };
          let evaluation: Evaluation = { _tag: "Pending" };
          const currentEvaluation = (): Evaluation => evaluation;
          const evaluate = (currentFiber: Fiber.Fiber<unknown, unknown>): X => {
            switch (evaluation._tag) {
              case "Succeeded":
                return evaluation.value;
              case "Failed":
                throw evaluation.error;
              case "Pending": {
                try {
                  const value = primitive["~effect/Effect/evaluate"](currentFiber);
                  evaluation = { _tag: "Succeeded", value };
                  return value;
                } catch (error) {
                  evaluation = { _tag: "Failed", error };
                  throw error;
                }
              }
            }
          };
          const guardedPrimitive: Tracer.EffectPrimitive<X> = {
            ["~effect/Effect/evaluate"]: evaluate,
          };

          let delegated: X;
          try {
            delegated = delegateContext(guardedPrimitive, fiber);
          } catch (defect) {
            const observed = currentEvaluation();
            switch (observed._tag) {
              case "Failed":
                throw observed.error;
              case "Succeeded":
                recordDefect(defect);
                return observed.value;
              case "Pending":
                recordDefect(defect);
                return evaluate(fiber);
            }
          }
          const observed = currentEvaluation();
          switch (observed._tag) {
            case "Failed":
              throw observed.error;
            case "Succeeded":
              return delegated;
            case "Pending":
              return evaluate(fiber);
          }
        };
  const tracer = Tracer.make({
    context,
    span(options) {
      let delegateSpan: Tracer.Span;
      try {
        delegateSpan = delegate.span(options);
      } catch (defect) {
        recordDefect(defect);
        // This native span is deliberately not handed to the host exporter. It only preserves
        // Effect's current-span contract so Tool behavior continues without observable tracing.
        return new Tracer.NativeSpan(options);
      }
      try {
        return new IsolatedToolSpan(delegateSpan, options, recordDefect);
      } catch (defect) {
        recordDefect(defect);
        try {
          // Allocation succeeded, so best-effort close the host span without attaching the raw
          // wrapper defect. The start time gives this abandoned measurement a bounded duration.
          delegateSpan.end(options.startTime, Exit.fail(new ToolSpanLifecycleFailure()));
        } catch (closeDefect) {
          recordDefect(closeDefect);
        }
        return new Tracer.NativeSpan(options);
      }
    },
  });
  const reportLifecycleDefects = Effect.suspend(() => {
    const pending = defects.splice(0);
    return Effect.forEach(
      pending,
      (defect) => isolateToolTerminalTelemetry(ErrorReporter.report(Cause.die(defect))),
      { discard: true },
    );
  });
  return { tracer, reportLifecycleDefects };
};

export interface ToolSpanTelemetryService {
  /** Measure one Tool handler attempt while isolating every host span-lifecycle defect. */
  readonly isolateSpanLifecycle: <A, E, R>(
    stream: Stream.Stream<A, E, R>,
  ) => Stream.Stream<A, E, R>;
  /** Keep Effect AI implementation annotations local while preserving host-owned handler spans. */
  readonly isolateToolkitHandle: <A, E, R>(
    effect: Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E, R>;
}

/**
 * @internal Engine-owned span-lifecycle policy. The AgentRuntime composition boundary builds this
 * capability from the host's ambient Tracer; Tool execution consumes it without selecting,
 * decorating, or locally providing a Tracer implementation.
 */
export class ToolSpanTelemetry extends Context.Service<
  ToolSpanTelemetry,
  ToolSpanTelemetryService
>()("@effect-agent/engine/ToolSpanTelemetry") {
  static readonly layer: Layer.Layer<ToolSpanTelemetry> = Layer.effect(
    ToolSpanTelemetry,
    Effect.map(Tracer.Tracer, (delegate) =>
      ToolSpanTelemetry.of({
        isolateToolkitHandle: (effect) =>
          Effect.currentSpan.pipe(
            Effect.option,
            Effect.flatMap((parentOption) => {
              if (Option.isNone(parentOption)) return effect;
              const parent = parentOption.value;
              // Effect AI annotates its current span with decoded Tool parameters before forking
              // the handler. Keep those annotations on a local span that is never handed to the
              // host tracer. DisablePropagation makes explicit handler spans filter past this
              // implementation parent and attach to the canonical execute_tool span instead.
              const local = new Tracer.NativeSpan({
                name: "AgentRuntime.toolkit.handle",
                parent: Option.some(parent),
                annotations: Context.add(Context.empty(), Tracer.DisablePropagation, true),
                links: [],
                startTime: 0n,
                kind: "internal",
                sampled: parent.sampled,
              });
              return effect.pipe(
                Effect.withParentSpan(local),
                Effect.onExit((exit) =>
                  Effect.sync(() => {
                    local.end(0n, exit);
                  }),
                ),
              );
            }),
          ),
        isolateSpanLifecycle: (stream) =>
          Stream.unwrap(
            Effect.sync(() => {
              const isolated = makeIsolatedToolTracer(delegate);
              return stream.pipe(
                Stream.provideService(Tracer.Tracer, isolated.tracer),
                // `Stream.ensuring` runs after the canonical span's own finalizer, so creation,
                // annotation, and close defects are reported outside this execution's measurement.
                Stream.ensuring(isolated.reportLifecycleDefects),
              );
            }),
          ),
      }),
    ),
  );
}

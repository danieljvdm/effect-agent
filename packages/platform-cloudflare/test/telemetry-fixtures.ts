import { Cause, Effect, Exit, Layer, Option, Schema, type Scope, Tracer } from "effect";

import { DurableObjectContext } from "../src/bindings.ts";
import { CloudflareRuntimeTelemetry, CloudflareTelemetryExportError } from "../src/telemetry.ts";

/** Typed test-only failure proving host telemetry acquisition remains in the runtime Layer error. */
export class TelemetryLayerAcquisitionError extends Schema.TaggedError<TelemetryLayerAcquisitionError>()(
  "TelemetryLayerAcquisitionError",
  {
    envHasTelemetryBinding: Schema.Boolean,
  },
) {}

export interface TelemetrySpanObservation {
  readonly phase: "span-ended";
  readonly entrypoint: string;
  readonly conversationId: string;
  readonly spanName: string;
  readonly spanId: string;
  readonly traceId: string;
  readonly parentSpanId: string | undefined;
  readonly sampled: boolean;
  readonly kind: Tracer.SpanKind;
  readonly startTime: bigint;
  readonly endTime: bigint;
  readonly failed: boolean;
  readonly failureText: string | undefined;
  readonly attributes: Readonly<Record<string, unknown>>;
  readonly events: ReadonlyArray<Tracer.NativeSpan["events"][number]>;
  readonly links: ReadonlyArray<{
    readonly traceId: string;
    readonly spanId: string;
    readonly attributes: Readonly<Record<string, unknown>>;
  }>;
}

export interface TelemetryFlushObservation {
  readonly phase: "flush-started" | "flush-completed" | "flush-failed";
}

export type TelemetryObservation = TelemetrySpanObservation | TelemetryFlushObservation;

interface PendingFlushGate {
  active: boolean;
  completed: boolean;
  released: boolean;
  readonly release: () => void;
}

interface PendingFlushAttempt {
  active: boolean;
  completed: boolean;
}

/** Hold one exporter attempt until the test explicitly releases it or its owner is interrupted. */
export interface TelemetryFlushBlock {
  readonly release: () => void;
  readonly isActive: () => boolean;
  readonly isCompleted: () => boolean;
}

/** Causal token claimed and completed by the next real exporter attempt in this fixture. */
export interface TelemetryFlushAttempt {
  readonly isActive: () => boolean;
  readonly isCompleted: () => boolean;
}

class TelemetryProbeSynchronizationError extends Error {
  override readonly name = "TelemetryProbeSynchronizationError";
}

const MAX_SYNCHRONIZATION_YIELDS = 10_000;
const ENTRYPOINT_SPAN_PREFIX = "effect_agent.cloudflare.conversation_object.";

const requiredStringAttribute = (
  attributes: Readonly<Record<string, unknown>>,
  name: string,
): string => {
  const value = attributes[name];
  if (typeof value !== "string") {
    throw new TypeError(
      `Telemetry probe expected ${name} to be a string, received ${typeof value}`,
    );
  }
  return value;
};

const takeFirst = <A>(values: Array<A>): A | undefined => values.shift();

export interface TelemetryProbeFixture {
  readonly observations: Array<TelemetryObservation>;
  readonly exportErrors: Array<CloudflareTelemetryExportError>;
  readonly acquisitionErrors: Array<TelemetryLayerAcquisitionError>;
  readonly layer: Layer.Layer<CloudflareRuntimeTelemetry>;
  readonly failNextFlush: (cause: unknown) => void;
  readonly expectFlushAttempt: () => TelemetryFlushAttempt;
  readonly withFlushBlock: <A, E, R>(
    use: (block: TelemetryFlushBlock) => Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E, R>;
  readonly awaitFlushBlock: (
    block: TelemetryFlushBlock,
    remainingYields?: number,
  ) => Effect.Effect<void, TelemetryProbeSynchronizationError>;
  readonly awaitFlushBlockCompleted: (
    block: TelemetryFlushBlock,
    remainingYields?: number,
  ) => Effect.Effect<void, TelemetryProbeSynchronizationError>;
  readonly awaitFlushAttempt: (
    attempt: TelemetryFlushAttempt,
    remainingYields?: number,
  ) => Effect.Effect<void, TelemetryProbeSynchronizationError>;
  readonly awaitObservationCount: (
    count: number,
    remainingYields?: number,
  ) => Effect.Effect<void, TelemetryProbeSynchronizationError>;
  readonly registerConversation: (
    conversationId: string,
  ) => Effect.Effect<void, never, Scope.Scope>;
  readonly dispose: () => void;
}

// Workerd requires its Durable Object class to be exported statically. This registry routes that
// class to fixture-owned state by the unique per-test Conversation identity; it contains no probe
// controls or observations itself, and a late old-incarnation completion retains its old fixture.
interface WorkerdFixtureRegistration {
  readonly fixture: TelemetryProbeFixture;
  readonly owner: object;
}

const workerdFixtures = new Map<string, WorkerdFixtureRegistration>();

const fixtureForContext = Effect.map(DurableObjectContext, ({ ctx }) => {
  const conversationId = ctx.id.name;
  if (conversationId === undefined) {
    throw new Error("Telemetry probe requires a named Durable Object identity");
  }
  const registration = workerdFixtures.get(conversationId);
  if (registration === undefined) {
    throw new Error(`No telemetry probe fixture registered for ${conversationId}`);
  }
  return registration.fixture;
});

/** Static Worker bridge selecting the fixture registered for this Conversation Object. */
export const telemetryProbeRouterLayer: Layer.Layer<
  CloudflareRuntimeTelemetry,
  never,
  DurableObjectContext
> = Layer.unwrap(Effect.map(fixtureForContext, ({ layer }) => layer));

/** Static typed-acquisition bridge selecting the fixture registered for this Object. */
export const failingTelemetryAcquisitionRouterLayer: Layer.Layer<
  CloudflareRuntimeTelemetry,
  TelemetryLayerAcquisitionError,
  DurableObjectContext
> = Layer.effect(
  CloudflareRuntimeTelemetry,
  Effect.gen(function* () {
    const fixture = yield* fixtureForContext;
    const { env } = yield* DurableObjectContext;
    const error = TelemetryLayerAcquisitionError.make({
      envHasTelemetryBinding:
        typeof env === "object" && env !== null && "TELEMETRY_ACQUISITION" in env,
    });
    fixture.acquisitionErrors.push(error);
    return yield* error;
  }),
);

/** Create one independent exporter/tracer probe and its complete control surface for one test. */
export const makeTelemetryProbeFixture = (): TelemetryProbeFixture => {
  const observations: Array<TelemetryObservation> = [];
  const exportErrors: Array<CloudflareTelemetryExportError> = [];
  const acquisitionErrors: Array<TelemetryLayerAcquisitionError> = [];
  const pendingFailures: Array<unknown> = [];
  const pendingGates: Array<PendingFlushGate> = [];
  const unreleasedGates = new Set<PendingFlushGate>();
  const pendingAttempts: Array<PendingFlushAttempt> = [];

  const blockNextFlush = (): TelemetryFlushBlock => {
    const gate: PendingFlushGate = {
      active: false,
      completed: false,
      released: false,
      release: () => {
        if (gate.released) return;
        gate.released = true;
        const pendingIndex = pendingGates.indexOf(gate);
        if (pendingIndex !== -1) pendingGates.splice(pendingIndex, 1);
        if (!gate.active) unreleasedGates.delete(gate);
      },
    };
    pendingGates.push(gate);
    unreleasedGates.add(gate);
    return {
      release: gate.release,
      isActive: () => gate.active,
      isCompleted: () => gate.completed,
    };
  };

  const awaitFlushGate = (gate: PendingFlushGate): Effect.Effect<void> =>
    Effect.whileLoop({
      while: () => !gate.released,
      body: () => Effect.yieldNow,
      step: () => undefined,
    });

  const awaitCondition = (
    condition: () => boolean,
    exhaustedMessage: () => string,
    remainingYields: number,
  ): Effect.Effect<void, TelemetryProbeSynchronizationError> =>
    Effect.suspend(() => {
      if (condition()) return Effect.void;
      if (remainingYields <= 0) {
        return Effect.fail(new TelemetryProbeSynchronizationError(exhaustedMessage()));
      }
      return Effect.yieldNow.pipe(
        Effect.andThen(awaitCondition(condition, exhaustedMessage, remainingYields - 1)),
      );
    });

  const layer: Layer.Layer<CloudflareRuntimeTelemetry> = Layer.unwrap(
    Effect.sync(() => {
      class ProbeSpan extends Tracer.NativeSpan {
        override end(endTime: bigint, exit: Exit.Exit<unknown, unknown>): void {
          super.end(endTime, exit);
          if (!this.name.startsWith(ENTRYPOINT_SPAN_PREFIX)) return;
          const attributes = Object.fromEntries(this.attributes);
          const entrypoint = requiredStringAttribute(
            attributes,
            "effect_agent.cloudflare.entrypoint",
          );
          const conversationId = requiredStringAttribute(attributes, "conversationId");
          observations.push({
            phase: "span-ended",
            entrypoint,
            conversationId,
            spanName: this.name,
            spanId: this.spanId,
            traceId: this.traceId,
            parentSpanId: Option.getOrUndefined(this.parent)?.spanId,
            sampled: this.sampled,
            kind: this.kind,
            startTime: this.startTime,
            endTime,
            failed: Exit.isFailure(exit),
            failureText: Exit.isFailure(exit) ? Cause.pretty(exit.cause) : undefined,
            attributes,
            events: this.events.map(([name, startTime, eventAttributes]) => [
              name,
              startTime,
              { ...eventAttributes },
            ]),
            links: this.links.map(({ span, attributes: linkAttributes }) => ({
              traceId: span.traceId,
              spanId: span.spanId,
              attributes: { ...linkAttributes },
            })),
          });
        }
      }

      const tracerLayer = Layer.effect(
        Tracer.Tracer,
        Effect.succeed(
          Tracer.make({
            span: (options) => new ProbeSpan(options),
          }),
        ),
      );
      const flushLayer = Layer.succeed(CloudflareRuntimeTelemetry)({
        flush: Effect.suspend(() => {
          const gate = takeFirst(pendingGates);
          const failure = takeFirst(pendingFailures);
          const expectedAttempt = takeFirst(pendingAttempts);
          if (expectedAttempt !== undefined) expectedAttempt.active = true;
          observations.push({ phase: "flush-started" });
          return Effect.gen(function* () {
            yield* Effect.yieldNow;
            if (gate !== undefined && !gate.released) {
              gate.active = true;
              yield* awaitFlushGate(gate).pipe(Effect.interruptible);
            }
            if (failure !== undefined) {
              const error = CloudflareTelemetryExportError.make({ cause: failure });
              exportErrors.push(error);
              observations.push({ phase: "flush-failed" });
              return yield* error;
            }
            observations.push({ phase: "flush-completed" });
          }).pipe(
            Effect.ensuring(
              Effect.sync(() => {
                if (gate !== undefined) gate.completed = true;
                if (gate !== undefined) unreleasedGates.delete(gate);
                if (expectedAttempt !== undefined) expectedAttempt.completed = true;
              }),
            ),
          );
        }),
      });
      return flushLayer.pipe(Layer.provideMerge(tracerLayer));
    }),
  );

  const fixture: TelemetryProbeFixture = {
    observations,
    exportErrors,
    acquisitionErrors,
    layer,
    failNextFlush: (cause) => pendingFailures.push(cause),
    expectFlushAttempt: () => {
      const attempt: PendingFlushAttempt = { active: false, completed: false };
      pendingAttempts.push(attempt);
      return {
        isActive: () => attempt.active,
        isCompleted: () => attempt.completed,
      };
    },
    withFlushBlock: (use) =>
      Effect.acquireUseRelease(Effect.sync(blockNextFlush), use, (block) =>
        Effect.sync(block.release),
      ),
    awaitFlushBlock: (block, remainingYields = MAX_SYNCHRONIZATION_YIELDS) =>
      awaitCondition(
        block.isActive,
        () => "Telemetry flush did not become active",
        remainingYields,
      ),
    awaitFlushBlockCompleted: (block, remainingYields = MAX_SYNCHRONIZATION_YIELDS) =>
      awaitCondition(block.isCompleted, () => "Telemetry flush did not complete", remainingYields),
    awaitFlushAttempt: (attempt, remainingYields = MAX_SYNCHRONIZATION_YIELDS) =>
      awaitCondition(
        attempt.isCompleted,
        () => "Expected exporter attempt did not complete",
        remainingYields,
      ),
    awaitObservationCount: (count, remainingYields = MAX_SYNCHRONIZATION_YIELDS) =>
      awaitCondition(
        () => observations.length >= count,
        () => `Expected ${count} telemetry observations, saw ${observations.length}`,
        remainingYields,
      ),
    registerConversation: (conversationId) =>
      Effect.acquireRelease(
        Effect.sync(() => {
          const existing = workerdFixtures.get(conversationId);
          if (existing !== undefined) {
            throw new Error(`Telemetry fixture already registered for ${conversationId}`);
          }
          const owner = {};
          workerdFixtures.set(conversationId, { fixture, owner });
          return owner;
        }),
        (owner) =>
          Effect.sync(() => {
            if (workerdFixtures.get(conversationId)?.owner === owner) {
              workerdFixtures.delete(conversationId);
            }
          }),
      ).pipe(Effect.asVoid),
    dispose: () => {
      for (const gate of unreleasedGates) gate.release();
    },
  };
  return fixture;
};

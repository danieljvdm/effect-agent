import { AgentId, ThreadId } from "@effect-agent/core/Identifiers";
import { makeEventSource } from "@effect-agent/thread/EventSource";
import { DefinitionDigests, Digest } from "@effect-agent/thread/Records";
import { Principal } from "@effect-agent/thread/SubmissionLedger";
import {
  AcceptedEvent,
  EventSourceVersion,
  SourcePartition,
  SubscriptionRecord,
  SubscriptionSourceError,
} from "@effect-agent/thread/Subscription";
import { makeSubscriptionInputBinding } from "@effect-agent/thread/SubscriptionInput";
import { describe, expect, it } from "@effect/vitest";
import { Cause, Context, Deferred, Effect, Exit, Fiber, Schema, SchemaGetter } from "effect";
import { TestClock } from "effect/testing";

const version = EventSourceVersion.make({ name: "scoped-callback", version: "1" });
const partition = SourcePartition.make({ tenantId: "tenant", address: "events" });
const principal = Schema.decodeSync(Principal)("principal");
const agentId = Schema.decodeSync(AgentId)("agent");
const digest = Schema.decodeSync(Digest)("a".repeat(64));
const definitions = DefinitionDigests.make({ agent: digest, model: digest, tools: digest });

const event = AcceptedEvent.make({
  schemaVersion: 1,
  partition,
  eventId: "event",
  source: version,
  matchingKey: "event",
  payload: "event",
  payloadDigest: digest,
  acceptedAtMillis: 0,
  cutoff: 1,
  cursor: 0,
  routingComplete: false,
  routingFailure: null,
  nextAttemptAtMillis: 0,
});

const subscription = SubscriptionRecord.make({
  schemaVersion: 1,
  key: { partition, ownerId: "owner", subscriptionId: "subscription" },
  creationFingerprint: digest,
  createdBy: principal,
  createdAtMillis: 0,
  ordinal: 1,
  configuration: {
    source: version,
    matchingKey: "event",
    parameters: "event",
    context: "context",
    mode: "once",
    expiresAtMillis: 1_000,
    destination: {
      _tag: "ExistingThread",
      threadId: Schema.decodeSync(ThreadId)("thread"),
    },
    deliveryPrincipal: principal,
    agentId,
    definitions,
  },
  state: "active",
  recovery: null,
});

class CallbackDependency extends Context.Service<CallbackDependency, string>()(
  "test/subscription-callbacks/CallbackDependency",
) {}

const makeOperation = Effect.fn("test.makeSubscriptionOperation")(function* <R>(
  boundary: "reconcile" | "prepare",
  callback: Effect.Effect<string, SubscriptionSourceError, R>,
) {
  if (boundary === "reconcile") {
    const source = yield* makeEventSource({
      source: version,
      continuity: "Test source retained for the host lifetime.",
      event: Schema.String,
      parameters: Schema.String,
      identity: (value) => value,
      eventKey: (value) => value,
      parameterKey: (value) => value,
      matches: (value, parameters) => value === parameters,
      reconcile: () => callback,
    });

    if (source.reconcile === undefined) return yield* Effect.die("Missing reconciliation");

    return source.reconcile(subscription).pipe(Effect.map((value) => value?.payload ?? null));
  }

  const binding = yield* makeSubscriptionInputBinding({
    source: version,
    agentId,
    definitions,
    event: Schema.String,
    parameters: Schema.String,
    context: Schema.String,
    input: Schema.String,
    prepare: () => callback,
  });

  return binding.prepare(event, subscription);
});

describe("subscription callback resource ownership", () => {
  it.effect("finalizes each invocation while retaining host services and their resources", () =>
    Effect.gen(function* () {
      for (const boundary of ["reconcile", "prepare"] as const) {
        const finalized: Array<string> = [];
        let hostFinalized = false;

        yield* Effect.scoped(
          Effect.gen(function* () {
            const host = yield* Effect.acquireRelease(Effect.succeed("host"), () =>
              Effect.sync(() => {
                hostFinalized = true;
              }),
            );

            const operation = yield* makeOperation(
              boundary,
              Effect.gen(function* () {
                const dependency = yield* CallbackDependency;

                yield* Effect.acquireRelease(Effect.void, () =>
                  Effect.sync(() => {
                    finalized.push(dependency);
                  }),
                );

                return dependency;
              }),
            ).pipe(Effect.provideService(CallbackDependency, host));

            for (let invocation = 1; invocation <= 2; invocation++) {
              expect(
                yield* operation.pipe(
                  Effect.provideService(CallbackDependency, "caller"),
                  Effect.scoped,
                ),
              ).toBe("host");
              expect(finalized).toEqual(Array.from({ length: invocation }, () => "host"));
              expect(hostFinalized).toBe(false);
            }
          }),
        );
        expect(hostFinalized).toBe(true);
        expect(finalized).toEqual(["host", "host"]);
      }
    }),
  );

  it.effect("finalizes failed callbacks without replacing expected failures or defects", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const failure = SubscriptionSourceError.make({ code: "failed", retryable: true });

        for (const boundary of ["reconcile", "prepare"] as const) {
          for (const scenario of [
            { effect: Effect.fail(failure), expected: failure },
            { effect: Effect.die("callback defect"), expected: "callback defect" },
          ]) {
            let finalized = 0;

            const operation = yield* makeOperation(
              boundary,
              Effect.acquireRelease(Effect.void, () =>
                Effect.sync(() => {
                  finalized += 1;
                }),
              ).pipe(Effect.andThen(scenario.effect)),
            );

            const exit = yield* Effect.exit(operation);

            expect(Exit.isFailure(exit)).toBe(true);
            if (Exit.isFailure(exit)) expect(Cause.squash(exit.cause)).toEqual(scenario.expected);
            expect(finalized).toBe(1);
          }
        }
      }),
    ),
  );

  it.effect("finalizes timed-out and interrupted callbacks before their host closes", () =>
    Effect.scoped(
      Effect.gen(function* () {
        for (const boundary of ["reconcile", "prepare"] as const) {
          for (const mode of ["timeout", "interrupt"] as const) {
            const entered = yield* Deferred.make<void>();
            let finalized = 0;

            const operation = yield* makeOperation(
              boundary,
              Effect.acquireRelease(Deferred.succeed(entered, undefined), () =>
                Effect.sync(() => {
                  finalized += 1;
                }),
              ).pipe(Effect.andThen(Effect.never)),
            );

            const fiber = yield* operation.pipe(Effect.timeout(1_000), Effect.forkChild);

            yield* Deferred.await(entered);
            if (mode === "timeout") yield* TestClock.adjust(1_000);
            else yield* Fiber.interrupt(fiber);
            const exit = yield* Fiber.await(fiber);

            expect(Exit.isFailure(exit)).toBe(true);
            if (Exit.isFailure(exit)) {
              if (mode === "interrupt") expect(Cause.hasInterrupts(exit.cause)).toBe(true);
              else expect(Cause.squash(exit.cause)).toMatchObject({ _tag: "TimeoutError" });
            }
            expect(finalized).toBe(1);
          }
        }
      }),
    ),
  );

  it.effect("owns scoped codecs throughout every source and input operation", () =>
    Effect.scoped(
      Effect.gen(function* () {
        let acquired = 0;
        let finalized = 0;

        const codecOperation = (value: string) =>
          Effect.acquireRelease(
            Effect.sync(() => {
              acquired += 1;

              return value;
            }),
            () =>
              Effect.sync(() => {
                finalized += 1;
              }),
          );

        const codec = Schema.String.pipe(
          Schema.decodeTo(Schema.String, {
            decode: SchemaGetter.transformOrFail(codecOperation),
            encode: SchemaGetter.transformOrFail(codecOperation),
          }),
        );

        const source = yield* makeEventSource({
          source: version,
          continuity: "Test source retained for the host lifetime.",
          event: codec,
          parameters: codec,
          identity: (value) => value,
          eventKey: (value) => value,
          parameterKey: (value) => value,
          matches: (value, parameters) => value === parameters,
          reconcile: () => Effect.succeed("event"),
        });

        const binding = yield* makeSubscriptionInputBinding({
          source: version,
          agentId,
          definitions,
          event: codec,
          parameters: codec,
          context: codec,
          input: codec,
          prepare: (_event, _parameters, context) => Effect.succeed(context),
        });

        if (source.reconcile === undefined) return yield* Effect.die("Missing reconciliation");

        for (const operation of [
          Effect.asVoid(source.normalize("event")),
          Effect.asVoid(source.parameters("event")),
          Effect.asVoid(source.matches(event, subscription)),
          Effect.asVoid(source.reconcile(subscription)),
          Effect.asVoid(binding.context("context")),
          Effect.asVoid(binding.prepare(event, subscription)),
        ]) {
          const before = acquired;

          yield* operation;
          expect(acquired).toBeGreaterThan(before);
          expect(finalized).toBe(acquired);
        }
      }),
    ),
  );
});

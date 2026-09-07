import { AgentId, ThreadId } from "@effect-agent/core/Identifiers";
import { makeEventSource } from "@effect-agent/thread/EventSource";
import { DefinitionDigests, Digest } from "@effect-agent/thread/Records";
import { Principal } from "@effect-agent/thread/SubmissionLedger";
import {
  AcceptedEvent,
  EventSourceVersion,
  SourcePartition,
  SubscriptionRecord,
} from "@effect-agent/thread/Subscription";
import { makeSubscriptionInputBinding } from "@effect-agent/thread/SubscriptionInput";
import { describe, expect, expectTypeOf, it } from "@effect/vitest";
import { Context, Effect, Schema, SchemaGetter } from "effect";

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
  configurationRevision: 1,
  configurationFingerprint: digest,
  creationConfiguration: {
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

class CallbackDependency extends Context.Service<CallbackDependency, string>()("test/Callback") {}
class Decoder extends Context.Service<Decoder, string>()("test/Decoder") {}
class Encoder extends Context.Service<Encoder, string>()("test/Encoder") {}

describe("subscription callback resource ownership", () => {
  it.effect("closes callback and codec resources per operation while retaining host services", () =>
    Effect.gen(function* () {
      let hostClosed = false;
      let acquired = 0;
      let finalized = 0;
      const observedServices: Array<string> = [];

      yield* Effect.scoped(
        Effect.gen(function* () {
          yield* Effect.addFinalizer(() =>
            Effect.sync(() => {
              hostClosed = true;
            }),
          );

          const scopedValue = (value: string) =>
            Effect.gen(function* () {
              acquired += 1;
              yield* Effect.addFinalizer(() =>
                Effect.sync(() => {
                  finalized += 1;
                }),
              );

              return value;
            });

          const codec = Schema.String.pipe(
            Schema.decodeTo(Schema.String, {
              decode: SchemaGetter.transformOrFail((value) =>
                Effect.gen(function* () {
                  observedServices.push(yield* Decoder);

                  return yield* scopedValue(value);
                }),
              ),
              encode: SchemaGetter.transformOrFail((value) =>
                Effect.gen(function* () {
                  observedServices.push(yield* Encoder);

                  return yield* scopedValue(value);
                }),
              ),
            }),
          );

          const callback = Effect.gen(function* () {
            observedServices.push(yield* CallbackDependency);

            return yield* scopedValue("event");
          });

          const sourceEffect = makeEventSource({
            source: version,
            continuity: "Retained for the host lifetime.",
            event: codec,
            parameters: codec,
            identity: (value) => value,
            eventKey: (value) => value,
            parameterKey: (value) => value,
            matches: (value, parameters) => value === parameters,
            reconcile: () => callback,
          });

          const bindingEffect = makeSubscriptionInputBinding({
            source: version,
            agentId,
            definitions,
            event: codec,
            parameters: codec,
            context: codec,
            input: codec,
            prepare: () => callback,
          });

          expectTypeOf<Effect.Services<typeof sourceEffect>>().toEqualTypeOf<
            CallbackDependency | Decoder | Encoder
          >();
          expectTypeOf<Effect.Services<typeof bindingEffect>>().toEqualTypeOf<
            CallbackDependency | Decoder | Encoder
          >();

          const source = yield* sourceEffect;
          const binding = yield* bindingEffect;

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

            yield* operation.pipe(
              Effect.provideService(CallbackDependency, "caller"),
              Effect.provideService(Decoder, "caller"),
              Effect.provideService(Encoder, "caller"),
            );
            expect(acquired).toBeGreaterThan(before);
            expect(finalized).toBe(acquired);
            expect(hostClosed).toBe(false);
          }
        }).pipe(
          Effect.provideService(CallbackDependency, "host"),
          Effect.provideService(Decoder, "host"),
          Effect.provideService(Encoder, "host"),
        ),
      );
      expect(hostClosed).toBe(true);
      expect(new Set(observedServices)).toEqual(new Set(["host"]));
    }),
  );
});

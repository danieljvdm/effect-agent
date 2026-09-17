import { expect, it } from "@effect/vitest";
import { Context, DateTime, Effect, Layer, Option, Tracer, Schema } from "effect";
import { Toolkit } from "effect/unstable/ai";

import * as Agent from "../../src/core/Agent.ts";
import { AttemptId, SubmissionId, ThreadId } from "../../src/core/Identifiers.ts";
import { DurableWorkerBinding } from "../../src/durable/AgentRegistration.ts";
import { DefinitionDigests, Digest, ProducerEpoch } from "../../src/durable/Records.ts";
import { Claim, OwnershipToken } from "../../src/durable/SubmissionLedger.ts";

class Dependency extends Context.Service<Dependency, string>()("test/registered-dependency") {}

// https://reve-r6.sentry.io/explore/traces/trace/09c140a052588528d87d0ab131c5cbcc
it.effect(
  "registered attempts use the current trace and sampling without losing dependencies",
  () =>
    Effect.gen(function* () {
      const definition = Agent.make("traced-registration", {
        input: Schema.String,
        output: Schema.String,
        instructions: "Answer",
        toolkit: Toolkit.empty,
      });

      const digest = Digest.make("a".repeat(64));

      const model = Layer.effectContext<Agent.ModelServices, never, never>(
        Effect.die("No model is invoked"),
      );

      const registrationSpans: Array<Tracer.Span> = [];

      const registrationTracer = Tracer.make({
        span: (options) => {
          const span = new Tracer.NativeSpan(options);

          registrationSpans.push(span);

          return span;
        },
      });

      const binding = yield* DurableWorkerBinding.make(
        Agent.withModel(definition, model),
        DefinitionDigests.make({ agent: digest, model: digest, tools: digest }),
      ).pipe(
        Effect.withSpan("registration", { root: true, sampled: true }),
        Effect.annotateSpans({ registrationOnly: true }),
        Effect.withTracer(registrationTracer),
        Effect.provideService(Dependency, "retained"),
      );

      const registration = registrationSpans[0]!;

      expect(registration.status._tag).toBe("Ended");
      const traceIds = new Set<string>();

      for (const sampled of [false, true]) {
        const attemptSpans: Array<Tracer.Span> = [];

        const tracer = Tracer.make({
          span: (options) => {
            const span = new Tracer.NativeSpan({ ...options, sampled });

            attemptSpans.push(span);

            return span;
          },
        });

        yield* Effect.gen(function* () {
          const parent = yield* Effect.currentSpan;

          traceIds.add(parent.traceId);
          yield* binding.attempt(
            () =>
              Effect.gen(function* () {
                const span = yield* Effect.currentSpan.pipe(Effect.orDie);

                expect(span.traceId).toBe(parent.traceId);
                expect(span.traceId).not.toBe(registration.traceId);
                expect(span.sampled).toBe(sampled);
                expect(Option.getOrUndefined(span.parent)?.spanId).toBe(parent.spanId);
                expect(span.attributes.get("registrationOnly")).toBeUndefined();
                expect(span.attributes.get("currentInvocation")).toBe(true);
                expect(yield* Effect.serviceOption(Dependency)).toEqual(Option.some("retained"));

                return Option.none();
              }).pipe(Effect.withSpan("attempt-body")),
            ThreadId.make("trace-test"),
            Claim.make({
              submissionId: SubmissionId.make("submission"),
              attemptId: AttemptId.make("attempt"),
              ownershipToken: OwnershipToken.make("owner"),
              producerEpoch: ProducerEpoch.make(1),
              leaseExpiresAt: DateTime.makeUnsafe(0),
              inputPayload: "input",
            }),
          );
        }).pipe(
          Effect.withSpan("invocation", { root: true, sampled }),
          Effect.annotateSpans({ currentInvocation: true }),
          Effect.withTracer(tracer),
        );
        expect(attemptSpans.map((span) => span.name)).toEqual(["invocation", "attempt-body"]);
      }
      expect(traceIds.size).toBe(2);
      expect(registrationSpans.map((span) => span.name)).toEqual(["registration"]);
    }),
);

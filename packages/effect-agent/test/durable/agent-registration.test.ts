import { NodeCrypto } from "@effect/platform-node";
import { expect, it } from "@effect/vitest";
import {
  Context,
  Crypto,
  DateTime,
  Effect,
  Layer,
  Option,
  PlatformError,
  Tracer,
  Schema,
} from "effect";
import { Tool, Toolkit } from "effect/unstable/ai";

import * as Agent from "../../src/core/Agent.ts";
import { AttemptId, SubmissionId, ThreadId } from "../../src/core/Identifiers.ts";
import { DurableWorkerBinding } from "../../src/durable/AgentRegistration.ts";
import { digestDefinitions, DigestError } from "../../src/durable/Digest.ts";
import {
  compileBindingContracts,
  compileRegistrations,
  toolReplayContracts,
} from "../../src/durable/internal/agent-registration.ts";
import {
  DefinitionDigestInput,
  DefinitionDigests,
  Digest,
  MAX_PERSISTED_JSON_DEPTH,
  ProducerEpoch,
} from "../../src/durable/Records.ts";
import { Claim, OwnershipToken } from "../../src/durable/SubmissionLedger.ts";
import { ToolExecutionClass } from "../../src/engine/DurableStep.ts";

class Dependency extends Context.Service<Dependency, string>()("test/registered-dependency") {}

it.effect("preserves admitted declarations when generated metadata exceeds persisted depth", () =>
  Effect.gen(function* () {
    let declaration: Schema.Json = "leaf";

    for (let depth = 0; depth < MAX_PERSISTED_JSON_DEPTH; depth++)
      declaration = { nested: declaration };
    const definitions = DefinitionDigestInput.make({ agent: declaration, model: "v1", tools: [] });

    const definition = Agent.make("deep-registration", {
      input: Schema.String,
      output: Schema.String,
      updates: Schema.String,
      instructions: "Answer",
      toolkit: Toolkit.empty,
    });

    const expected = yield* digestDefinitions({
      ...definitions,
      agent: {
        declaration,
        updates: Schema.decodeUnknownSync(Schema.Json)(Tool.getJsonSchemaFromSchema(Schema.String)),
        updateProtocol: { schemaVersion: 1, tool: "emit_update" },
      },
    });

    const entry = {
      agent: definition,
      model: Layer.effectContext<Agent.ModelServices, never, never>(
        Effect.die("No model is invoked"),
      ),
      definitions,
    };

    for (let rebuild = 0; rebuild < 2; rebuild++) {
      const compiled = yield* compileRegistrations([entry]);

      expect(compiled[0]?.digests.agent).toBe(expected.agent);
    }
  }).pipe(Effect.provide(NodeCrypto.layer)),
);

// Explicit failure-first request: stable-identity invalidation and reuse across runtime rebuilds.
// Final output cannot reveal repeated contract hashing, so count the real Crypto operations here.
it.effect(
  "reuses replay contracts across identities and invalidates changed declaration inputs",
  () =>
    Effect.gen(function* () {
      const crypto = yield* Crypto.Crypto;
      let hashes = 0;

      const counted = {
        ...crypto,
        digest: (algorithm: Crypto.DigestAlgorithm, bytes: Uint8Array) => {
          hashes++;

          return crypto.digest(algorithm, bytes);
        },
      };

      const cached = toolReplayContracts;

      const tool = Tool.make("lookup", {
        parameters: Schema.Struct({ id: Schema.String }),
        success: Schema.String,
      });

      const definition = Agent.make("cache-identity", {
        input: Schema.String,
        output: Schema.String,
        instructions: "Answer",
        toolkit: Toolkit.make(tool),
      });

      const digest = Digest.make("a".repeat(64));
      const digests = DefinitionDigests.make({ agent: digest, model: digest, tools: digest });

      const get = (value: Agent.AnyDefinition, version = "v1", identity = digests) =>
        cached(value, undefined, version, identity).pipe(
          Effect.provideService(Crypto.Crypto, counted),
        );

      const original = yield* get(definition);
      const reconstructed = { ...definition, toolkit: Toolkit.make(tool) };

      expect(yield* get(reconstructed)).toEqual(original);
      expect(hashes).toBe(1);

      const changedTools = [
        Tool.make("lookup", {
          parameters: Schema.Struct({ id: Schema.Number }),
          success: Schema.String,
        }),
        tool.annotate(ToolExecutionClass, "readonly"),
      ];

      for (const changed of changedTools) {
        const before = hashes;
        const contracts = yield* get({ ...definition, toolkit: Toolkit.make(changed) });

        expect(contracts.lookup).not.toBe(original.lookup);
        expect(hashes).toBe(before + 1);
      }
      expect((yield* get(definition, "v2")).lookup).not.toBe(original.lookup);
      expect(
        (yield* get({ ...definition, completion: { tool: "lookup", project: () => "done" } }))
          .lookup,
      ).not.toBe(original.lookup);
      expect(yield* get({ ...definition, toolkit: Toolkit.empty })).toEqual({});
      const args = { region: "one" };

      const hosted = Tool.providerDefined({
        id: "test.hosted",
        customName: "hosted",
        providerName: "hosted",
        args: Schema.Struct({ region: Schema.String }),
      })(args);

      const providerDefinition = { ...definition, toolkit: Toolkit.make(hosted) };
      const providerOriginal = yield* get(providerDefinition);

      args.region = "two";
      expect((yield* get(providerDefinition)).hosted).not.toBe(providerOriginal.hosted);
      yield* get(definition);
      const beforeDigestChange = hashes;

      yield* get(definition, "v1", { ...digests, tools: Digest.make("b".repeat(64)) });
      expect(hashes).toBe(beforeDigestChange + 1);

      // Rebuilt registrations reuse static metadata; captured services remain outside the compiler.
      const definitions = { agent: "v1", model: "model", tools: "v1" };

      const compiled = yield* compileBindingContracts(definition, definitions).pipe(
        Effect.provideService(Crypto.Crypto, counted),
      );

      const beforeRebuild = hashes;

      expect(
        yield* compileBindingContracts(definition, { ...definitions }).pipe(
          Effect.provideService(Crypto.Crypto, counted),
        ),
      ).toEqual(compiled);
      expect(hashes).toBe(beforeRebuild);
      yield* cached(definition, {
        tools: { lookup: { release: "v1", options: { region: "one", limit: 2 } } },
      }).pipe(Effect.provideService(Crypto.Crypto, counted));
      const beforeReordering = hashes;

      yield* cached(definition, {
        tools: { lookup: { options: { limit: 2, region: "one" }, release: "v1" } },
      }).pipe(Effect.provideService(Crypto.Crypto, counted));
      expect(hashes).toBe(beforeReordering);
    }).pipe(Effect.provide(NodeCrypto.layer)),
);

it.effect("retains typed contract failures without caching failed compilation", () =>
  Effect.gen(function* () {
    const crypto = yield* Crypto.Crypto;
    const cached = toolReplayContracts;

    const definition = Agent.make("cache-failure", {
      input: Schema.String,
      output: Schema.String,
      instructions: "Answer",
      toolkit: Toolkit.make(
        Tool.make("lookup", { parameters: Schema.Struct({}), success: Schema.String }),
      ),
    });

    const get = (version: string) => cached(definition, undefined, version);
    const original = yield* get("v1");

    expect(yield* cached(definition, { tools: {} }).pipe(Effect.flip)).toBeInstanceOf(DigestError);

    const cause = PlatformError.badArgument({
      module: "Crypto",
      method: "digest",
      description: "unavailable",
    });

    const failed = yield* get("v2").pipe(
      Effect.provideService(Crypto.Crypto, { ...crypto, digest: () => Effect.fail(cause) }),
      Effect.flip,
    );

    expect(failed).toBeInstanceOf(DigestError);
    expect(failed.cause).toBe(cause);
    expect((yield* get("v2")).lookup).not.toBe(original.lookup);
    expect(yield* get("v1")).toEqual(original);
  }).pipe(Effect.provide(NodeCrypto.layer)),
);

it.effect("hashes the serialized replay snapshot when inputs change during compilation", () =>
  Effect.gen(function* () {
    const crypto = yield* Crypto.Crypto;
    const first = Tool.make("first", { parameters: Schema.Struct({}), success: Schema.String });
    const second = Tool.make("second", { parameters: Schema.Struct({}), success: Schema.String });

    const make = (id: string, toolkit: Agent.AnyDefinition["toolkit"]) =>
      Agent.make(id, {
        input: Schema.String,
        output: Schema.String,
        instructions: "Answer",
        toolkit,
      });

    const version = { release: "v1" };
    const versions = { tools: { first: "v1", second: version } };

    const expected = yield* toolReplayContracts(
      make("snapshot-reference", Toolkit.make(second)),
      versions,
    );

    const definition = make("snapshot-compilation", Toolkit.make(first, second));
    let calls = 0;

    const compiled = yield* toolReplayContracts(definition, versions).pipe(
      Effect.provideService(Crypto.Crypto, {
        ...crypto,
        digest: (algorithm, bytes) => {
          if (++calls === 1) version.release = "v2";

          return crypto.digest(algorithm, bytes);
        },
      }),
    );

    version.release = "v1";
    expect(compiled.second).toBe(expected.second);
    expect((yield* toolReplayContracts(definition, versions)).second).toBe(expected.second);
  }).pipe(Effect.provide(NodeCrypto.layer)),
);

// Registration captures dependencies while each Attempt inherits its current trace and sampling.
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
            const span = new Tracer.NativeSpan(options);

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

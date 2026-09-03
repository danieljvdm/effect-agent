import * as Remembering from "@effect-agent/capabilities/Remembering";
import * as Agent from "@effect-agent/core/Agent";
import { IdGenerator } from "@effect-agent/core/IdGenerator";
import * as Memory from "@effect-agent/core/Memory";
import { MemoryPassage } from "@effect-agent/core/MemoryReference";
import {
  type MemoryDocument,
  MemoryMutationFailpoint,
  MemoryMutationFailure,
  MemoryReader,
  MemoryScope,
  MemoryWriter,
} from "@effect-agent/core/MemoryStore";
import * as Protocol from "@effect-agent/core/RememberingStore";
import * as AgentRuntime from "@effect-agent/engine/AgentRuntime";
import { ContextCompactor } from "@effect-agent/engine/ContextCompactor";
import { ThreadHistory } from "@effect-agent/engine/ThreadHistory";
import { doMemoryStoreLayerWithFailpoints } from "@effect-agent/storage-cloudflare/DoMemoryStore";
import { ScriptedModel } from "@effect-agent/testing/ScriptedModel";
import { DurableObject } from "cloudflare:workers";
import { Clock, Deferred, Effect, Layer, ManagedRuntime, Schema, Semaphore, Stream } from "effect";
import { LanguageModel, Model, Toolkit } from "effect/unstable/ai";

import {
  ForegroundSample,
  Profile,
  Proposal,
  Request as FixtureRequest,
  type Source,
  Status,
} from "./remembering-contract.ts";
import { HostFailure, namespace, OwnerStore, target } from "./remembering-store.ts";

interface Env {
  REMEMBERING: DurableObjectNamespace<RememberingOwner>;
  REMEMBERING_LINEAGE?: string;
}
const scope = MemoryScope.make("owner");

const limits = Remembering.Limits.make({
  maxSourceBytes: 8192,
  maxProposalBytes: 16_384,
  timeoutMillis: 10_000,
});

const recallLimits = {
  maxSources: 1,
  maxItems: 1,
  maxBytes: 32_768,
  maxTokens: 32_768,
  maxInputBytes: 65_536,
  timeoutMillis: 1000,
};

const foregroundLayer = Layer.mergeAll(
  IdGenerator.layer,
  ThreadHistory.layerTransient,
  ContextCompactor.layer,
);

const profile = (document: MemoryDocument | null) =>
  document?._tag === "ActiveMemoryDocument"
    ? Schema.decodeUnknownEffect(Profile)(document.content.metadata)
    : Effect.succeed(Profile.make({ facts: [] }));

const decision = (facts: Profile["facts"]): Remembering.Decision => ({
  _tag: "Put",
  locator: "profile://owner",
  scopes: [scope],
  content: {
    text: facts.length === 0 ? "No remembered facts." : facts.map((fact) => fact.text).join("\n"),
    metadata: { facts: [...facts] },
    recordedAt: 1,
    attributions:
      facts.length === 0
        ? [
            {
              originId: "profile:empty",
              speaker: "Owner",
              observers: ["owner"],
              locator: "profile://owner",
              activityAt: 1,
              interpretation: "Empty profile after source cleanup",
            },
          ]
        : facts.map((fact) => ({
            originId: `${fact.originId}:${fact.revision}`,
            speaker: "Human",
            observers: ["owner"],
            locator: `chat://${fact.originId}`,
            activityAt: 1,
            interpretation: fact.human ? "human correction" : "source quotation",
          })),
  },
});

const learner = (owner: RememberingOwner) =>
  Remembering.make({
    proposal: Proposal,
    loadSource: Effect.fn("fixture.loadSource")(function* (intent: Protocol.Intent) {
      const source = yield* Effect.sync(() => owner.store.source(intent.source.key.id));

      if (!source || source.author !== "human") return null;

      return Remembering.SourceSnapshot.make({
        source: {
          ...intent.source,
          revision: source.revision,
          position: {
            authorityGeneration: owner.store.configuredLineage,
            sequence: source.sequence,
          },
        },
        text: source.text,
      });
    }),
    extract: Effect.fn("fixture.extract")(function* (
      snapshot: Remembering.SourceSnapshot,
      intent: Protocol.Intent,
    ) {
      owner.extractionCalls++;
      yield* owner.waitGate("extraction");
      if (owner.extractionFailure === "retry")
        return yield* HostFailure.make({ reason: "retry", operation: "extract" });
      if (owner.extractionFailure === "defect")
        return yield* Effect.die("injected extractor defect");

      const model = ScriptedModel.layer([
        {
          _tag: "Generate",
          parts: [
            {
              type: "text",
              text: JSON.stringify({
                originId: intent.source.key.id,
                revision: intent.source.revision,
                text: snapshot.text,
                quote: snapshot.text,
              }),
            },
            { type: "finish", reason: "stop", usage: { inputTokens: {}, outputTokens: {} } },
          ],
        },
      ]);

      const generated = yield* LanguageModel.generateObject({
        prompt: snapshot.text,
        schema: Proposal,
      }).pipe(Effect.provide(model));

      return {
        value: generated.value,
        evidence: [
          Protocol.Evidence.make({
            source: {
              id: intent.source.key.id,
              revision: intent.source.revision,
              locator: intent.source.locator,
            },
            startByte: 0,
            endByte: new TextEncoder().encode(snapshot.text).byteLength,
            quote: generated.value.quote,
          }),
        ],
      };
    }),
    merge: Effect.fn("fixture.merge")(function* ({ proposal, current }) {
      owner.mergeCalls++;
      const value = proposal.value;
      const existing = yield* profile(current);

      // Human corrections are distinct authoritative entries and always survive model merging.
      if (
        existing.facts.some(
          (fact) =>
            fact.originId === value.originId &&
            fact.revision === value.revision &&
            fact.text === value.text,
        )
      )
        return { _tag: "NoChange" } as const;

      return decision([
        ...existing.facts.filter((fact) => fact.human || fact.originId !== value.originId),
        { originId: value.originId, revision: value.revision, text: value.text, human: false },
      ]);
    }),
    cleanup: Effect.fn("fixture.cleanup")(function* ({ proposal, current }) {
      const existing = yield* profile(current);

      const facts = existing.facts.filter(
        (fact) =>
          fact.human ||
          fact.originId !== proposal.value.originId ||
          fact.revision !== proposal.value.revision,
      );

      return facts.length === existing.facts.length
        ? ({ _tag: "NoChange" } as const)
        : decision(facts);
    }),
  });

/** Only the platform RPC/fetch/alarm boundary returns Promises. Each worker event runs one
 * finite Effect Scope, with two background permits and no foreground/provider permit sharing.
 * Alarms are durable hints; source outboxes and checkpoint jobs are the recovery truth.
 */
export class RememberingOwner extends DurableObject<Env> {
  readonly store = new OwnerStore(this.ctx.storage, this.env.REMEMBERING_LINEAGE);
  readonly runtime = ManagedRuntime.make(
    doMemoryStoreLayerWithFailpoints(this.ctx.storage).pipe(
      Layer.provide(
        Layer.succeed(MemoryMutationFailpoint, {
          hit: (point) =>
            Effect.try({
              try: () => this.store.hit(point),
              catch: () => MemoryMutationFailure.make({ point }),
            }),
        }),
      ),
    ),
  );
  readonly permits = Semaphore.makeUnsafe(2);
  extractionGate: Deferred.Deferred<void> | undefined;
  writeGate: Deferred.Deferred<void> | undefined;
  automaticWake = true;
  workerTimeoutMillis = 10_000;
  extractionFailure: "retry" | "defect" | "none" = "none";
  running = false;
  extractionCalls = 0;
  mergeCalls = 0;
  writeCalls = 0;
  workerStarts = 0;
  workerFinalizers = 0;
  extractionFinalizers = 0;
  foregroundFinalizers = 0;
  extractionWaiting = 0;
  writeWaiting = 0;

  waitGate = (kind: "extraction" | "write") =>
    Effect.acquireUseRelease(
      Effect.sync(() => {
        if (kind === "extraction") this.extractionWaiting++;
        else this.writeWaiting++;

        return kind === "extraction" ? this.extractionGate : this.writeGate;
      }),
      (gate) => (gate === undefined ? Effect.void : Deferred.await(gate)),
      () =>
        Effect.sync(() => {
          if (kind === "extraction") {
            this.extractionWaiting--;
            this.extractionFinalizers++;
          } else this.writeWaiting--;
        }),
    );

  wake = Effect.fn("fixture.wake")((delay: number) =>
    this.automaticWake
      ? Effect.tryPromise({
          try: () => this.ctx.storage.setAlarm(Date.now() + delay),
          catch: () => HostFailure.make({ reason: "storage", operation: "set alarm" }),
        })
      : Effect.void,
  );

  async alarm(): Promise<void> {
    await Effect.runPromise(this.store.validate);
    await this.runtime.runPromise(work(this).pipe(Effect.scoped));
  }

  async fetch(request: Request): Promise<Response> {
    const validation = await Effect.runPromise(Effect.result(this.store.validate));

    if (validation._tag === "Failure")
      return Response.json({ _tag: "Failure", failure: validation.failure });

    return this.runtime.runPromise(
      handle(this, request).pipe(
        Effect.scoped,
        Effect.catch((failure) => Effect.succeed(Response.json({ _tag: "Failure", failure }))),
        Effect.catchCause(() => Effect.succeed(Response.json({ _tag: "Defect" }))),
        Effect.provide(Protocol.MutationFailpoint.layer),
      ),
    );
  }
}

const advance = Effect.fn("fixture.advance")(function* (
  owner: RememberingOwner,
  intent: Protocol.Intent,
) {
  const writer = yield* MemoryWriter;

  const guardedWriter = MemoryWriter.fromAdapter({
    change: (command) =>
      owner.waitGate("write").pipe(
        Effect.andThen(
          Effect.sync(() => {
            owner.writeCalls++;
          }),
        ),
        Effect.andThen(writer.change(command)),
      ),
  });

  return yield* learner(owner)
    .advance({
      intent,
      store: owner.store,
      limits: { ...limits, timeoutMillis: owner.workerTimeoutMillis },
      extractionEnabled: true,
    })
    .pipe(Effect.provideService(MemoryWriter, guardedWriter));
});

const work = Effect.fn("fixture.work")(function* (owner: RememberingOwner) {
  yield* owner.store.validate;
  if (owner.running) return { busy: true };

  return yield* Effect.acquireUseRelease(
    Effect.sync(() => {
      owner.running = true;
      owner.workerStarts++;
    }),
    () =>
      Effect.gen(function* () {
        // Admission is retried later at capacity; a completed chat never enters this Effect.
        for (const intent of owner.store.outbox()) {
          yield* owner.store
            .admitOutbox(intent)
            .pipe(Effect.catchTag("RememberingAdmissionError", () => Effect.void));
        }
        for (let round = 0; round < 8; round++) {
          const intents = owner.store.pending();

          if (intents.length === 0) break;
          yield* Effect.forEach(
            intents,
            (intent) => owner.permits.withPermits(1)(advance(owner, intent)),
            { concurrency: 2 },
          );
        }

        return { busy: false };
      }),
    () =>
      Effect.gen(function* () {
        owner.running = false;
        owner.workerFinalizers++;
        if (owner.store.count("jobs") + owner.store.count("outbox") > 0)
          yield* owner.wake(2500).pipe(Effect.orDie);
      }),
  ).pipe(Effect.provide(Protocol.MutationFailpoint.layer));
});

const foreground = Effect.fn("fixture.foreground")(function* (
  owner: RememberingOwner,
  source: Source,
  learning: "off" | "automatic" | "explicit",
) {
  const started = yield* Clock.currentTimeMillis;
  let firstTokenMillis = 0;
  let completedResponseMillis = 0;
  let admissionMillis = 0;
  let output = "";
  let recalled = "";
  let promptIncludesRecall = false;

  const model = Model.make(
    "scripted",
    "consumer-chat",
    ScriptedModel.layer([
      {
        _tag: "Stream",
        termination: { _tag: "Complete" },
        assertRequest: (request) =>
          Effect.sync(() => {
            promptIncludesRecall =
              recalled === "" || JSON.stringify(request.prompt).includes(JSON.stringify(recalled));
          }),
        parts: [
          { type: "text-start", id: "answer" },
          { type: "text-delta", id: "answer", delta: '"Thanks, I can help with that."' },
          { type: "text-end", id: "answer" },
          { type: "finish", reason: "stop", usage: { inputTokens: {}, outputTokens: {} } },
        ],
      },
    ]),
  );

  const agent = Agent.withModel(
    Agent.make("remembering-chat", {
      input: Schema.String,
      output: Schema.String,
      instructions: "Respond helpfully.",
      toolkit: Toolkit.empty,
      policy: { maxTurns: 1, maxToolCalls: 1, maxDuration: "5 seconds", toolConcurrency: 1 },
    }),
    model,
  );

  // Source commit/outbox is independent of whether the active admission queue has room.
  yield* owner.wake(2500);
  yield* owner.store.commit(source, learning === "automatic");
  if (learning === "explicit") {
    const admissionStart = yield* Clock.currentTimeMillis;

    yield* Remembering.admit(owner.store, owner.store.intent(`explicit:${source.id}`, source));
    admissionMillis = (yield* Clock.currentTimeMillis) - admissionStart;
  }
  yield* AgentRuntime.stream(agent, source.text, {
    transientContext: {
      load: () =>
        readProfile(owner).pipe(
          Effect.map((current) => {
            recalled = current.recalled;

            return recalled;
          }),
        ),
    },
  }).pipe(
    Stream.runForEach(
      Effect.fn("fixture.observeForeground")(function* (event) {
        if (event._tag === "TextDelta")
          firstTokenMillis = (yield* Clock.currentTimeMillis) - started;
        if (event._tag === "RunCompleted") {
          completedResponseMillis = (yield* Clock.currentTimeMillis) - started;
          output = yield* Schema.decodeUnknownEffect(Schema.String)(event.output);
        }
      }),
    ),
    Effect.provide(foregroundLayer),
    Effect.ensuring(
      Effect.sync(() => {
        owner.foregroundFinalizers++;
      }),
    ),
  );

  return ForegroundSample.make({
    environment: "local workerd/Miniflare; scripted native Effect AI",
    learning,
    firstTokenMillis,
    completedResponseMillis,
    admissionMillis,
    output,
    recalled,
    promptIncludesRecall,
  });
});

const readProfile = Effect.fn("fixture.readProfile")(function* (owner: RememberingOwner) {
  yield* owner.store.validate;
  const reader = yield* MemoryReader;
  const current = yield* reader.get(target);

  if (current?._tag !== "ActiveMemoryDocument")
    return { profile: null, recalled: "", generation: current?.generation ?? 0 };
  if (!current.scopes.includes(scope))
    return yield* HostFailure.make({ reason: "denied", operation: "profile scope" });
  const currentProfile = yield* profile(current);

  // Keep the content and profile revision from one public read. The source owner rechecks
  // each contribution before the captured view can enter transient model context.
  const visible = currentProfile.facts.filter(
    (fact) => fact.human || owner.store.visible(fact.originId, fact.revision),
  );

  if (visible.length === 0) return { profile: null, recalled: "", generation: current.generation };
  const authorizedContent = decision(visible);

  const lookup =
    authorizedContent._tag === "Put"
      ? {
          _tag: "Found" as const,
          passages: [
            MemoryPassage.make({
              version: 1,
              source: current.source,
              passageId: "profile",
              content: authorizedContent.content,
            }),
          ],
        }
      : { _tag: "NoMatch" as const };

  const recalled = yield* Memory.recall(
    [{ id: "profile", essential: true, read: Effect.succeed(lookup) }],
    recallLimits,
  );

  return { profile: { facts: visible }, recalled: recalled.text, generation: current.generation };
});

const handle = Effect.fn("fixture.handle")(function* (owner: RememberingOwner, request: Request) {
  yield* owner.store.validate;
  const raw = yield* Effect.tryPromise(() => request.json());
  const input = yield* Schema.decodeUnknownEffect(FixtureRequest)(raw);

  switch (input._tag) {
    case "CorruptHeader":
      yield* owner.store.corruptHeader(input.kind);

      return Response.json({ corrupted: true });
    case "Configure": {
      if (input.extractionBlocked === true && !owner.extractionGate)
        owner.extractionGate = Deferred.makeUnsafe<void>();
      if (input.extractionBlocked === false && owner.extractionGate) {
        yield* Deferred.succeed(owner.extractionGate, undefined);
        owner.extractionGate = undefined;
      }
      if (input.writeBlocked === true && !owner.writeGate)
        owner.writeGate = Deferred.makeUnsafe<void>();
      if (input.writeBlocked === false && owner.writeGate) {
        yield* Deferred.succeed(owner.writeGate, undefined);
        owner.writeGate = undefined;
      }
      if (input.automaticWake !== undefined) owner.automaticWake = input.automaticWake;
      if (input.failAt !== undefined) owner.store.failAt = input.failAt;
      if (input.extractionFailure !== undefined) owner.extractionFailure = input.extractionFailure;
      if (input.workerTimeoutMillis !== undefined)
        owner.workerTimeoutMillis = input.workerTimeoutMillis;

      return Response.json({ configured: true });
    }
    case "Commit":
      yield* owner.wake(2500);
      yield* owner.store.commit(input.source, input.automatic);

      return Response.json({ committed: true });
    case "Remember": {
      const source = owner.store.source(input.sourceId);

      if (source === null)
        return yield* HostFailure.make({ reason: "denied", operation: "source missing" });
      yield* owner.wake(2500);

      return Response.json(
        yield* Remembering.admit(owner.store, owner.store.intent(input.id, source)),
      );
    }
    case "Foreground":
      return Response.json(yield* foreground(owner, input.source, input.learning));
    case "Work":
      return Response.json(yield* work(owner));
    case "Status":
      return Response.json(
        Status.make({
          active: owner.store.count("jobs"),
          archived: owner.store.count("references") - owner.store.count("jobs"),
          outbox: owner.store.count("outbox"),
          sources: owner.store.count("sources"),
          extractionCalls: owner.extractionCalls,
          mergeCalls: owner.mergeCalls,
          writeCalls: owner.writeCalls,
          workerStarts: owner.workerStarts,
          workerFinalizers: owner.workerFinalizers,
          extractionFinalizers: owner.extractionFinalizers,
          foregroundFinalizers: owner.foregroundFinalizers,
          extractionWaiting: owner.extractionWaiting,
          writeWaiting: owner.writeWaiting,
          running: owner.running,
          checkpoints: owner.store
            .references()
            .map((checkpoint) => ({ id: checkpoint.intent.id, tag: checkpoint.progress._tag })),
        }),
      );
    case "Correct": {
      const reader = yield* MemoryReader;
      const writer = yield* MemoryWriter;
      const current = yield* reader.get(target);
      const existing = yield* profile(current);

      const correction = decision([
        ...existing.facts.filter((fact) => !fact.human),
        { originId: "human:correction", revision: "1", text: input.text, human: true },
      ]);

      if (correction._tag !== "Put") return yield* Effect.die("Expected correction Put");

      return Response.json(
        yield* writer.change({
          ...correction,
          key: target,
          operationId: `human:${current?.source.revision ?? "0"}`,
          expectedRevision: current?.source.revision ?? null,
        }),
      );
    }
    case "Forget":
      yield* owner.wake(2500);

      return Response.json(
        yield* Remembering.invalidate(
          owner.store,
          Protocol.Invalidation.make({
            version: 1,
            id: `forget:${input.sourceId}:${input.sequence}`,
            source: { namespace, id: input.sourceId },
            position: {
              authorityGeneration: owner.store.configuredLineage,
              sequence: input.sequence,
            },
            reason: "forget",
          }),
        ),
      );
    case "Read": {
      if (!input.authorized)
        return yield* HostFailure.make({ reason: "denied", operation: "profile read" });

      return Response.json(yield* readProfile(owner));
    }
  }
});

export default {
  fetch(request: Request, env: Env): Promise<Response> {
    return env.REMEMBERING.getByName(new URL(request.url).pathname.slice(1) || "owner").fetch(
      request,
    );
  },
};

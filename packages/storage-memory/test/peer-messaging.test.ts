import * as Agent from "@effect-agent/core/Agent";
import { AgentPolicy } from "@effect-agent/core/AgentPolicy";
import { ThreadId } from "@effect-agent/core/Identifiers";
import { MessageRef, MessagingError } from "@effect-agent/core/Messaging";
import { IdempotencyKey, Principal } from "@effect-agent/core/Receipt";
import { RunToolAuthorization } from "@effect-agent/engine/RunOptions";
import { memoryMessageDeliveryStoreLayer } from "@effect-agent/storage-memory/MemoryMessageDeliveryStore";
import { MemorySubmissionLedgerLive } from "@effect-agent/storage-memory/MemorySubmissionLedger";
import { MemoryThreadStoreLive } from "@effect-agent/storage-memory/MemoryThreadStore";
import { DurableWorkerBinding } from "@effect-agent/thread/AgentRegistration";
import {
  DurableAgentRuntime,
  DurableRuntimeConfig,
  type DurableSubmitOptions,
} from "@effect-agent/thread/DurableAgentRuntime";
import { DurableRuntimeFailpoint } from "@effect-agent/thread/DurableFailpoint";
import {
  MessageDeliveryDriver,
  MessageDeliveryFailpoint,
  MessageDeliveryFailpointError,
  MessageDeliveryStore,
} from "@effect-agent/thread/MessageDelivery";
import {
  PeerAuthorizer,
  PeerDeliveryLifetime,
  PeerMessageCapacity,
  PeerRoutes,
  type PeerAuthorizationRequest,
} from "@effect-agent/thread/MessagingHost";
import { PreparedInputAdmission } from "@effect-agent/thread/PreparedInputAdmission";
import {
  DefinitionDigests,
  DeploymentId,
  Digest,
  PersistedJson,
  ProducerId,
} from "@effect-agent/thread/Records";
import { ScheduledInputRefused, ScheduledInputRetryable } from "@effect-agent/thread/Schedule";
import { SubmissionLedger, SubmissionLookupByKey } from "@effect-agent/thread/SubmissionLedger";
import { PreparedInput } from "@effect-agent/thread/Subscription";
import { ThreadExportRequest, ThreadStore } from "@effect-agent/thread/ThreadStore";
import { ToolReconciler } from "@effect-agent/thread/ToolReconciler";
import { WakeScheduler } from "@effect-agent/thread/WakeScheduler";
import { NodeCrypto } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import { Context, Duration, Effect, Layer, Option, Schema, Stream } from "effect";
import { TestClock } from "effect/testing";
import { LanguageModel, Model, Toolkit, type Response } from "effect/unstable/ai";

const Input = Schema.Struct({ text: Schema.String });

const policy = AgentPolicy.make({
  maxTurns: 2,
  maxToolCalls: 2,
  maxDuration: "10 seconds",
  toolConcurrency: 1,
});

const source = Agent.make("peer-source", {
  input: Input,
  output: Schema.String,
  instructions: "Answer as JSON.",
  toolkit: Toolkit.empty,
  policy,
});

const destination = Agent.make("peer-destination", {
  input: Input,
  output: Schema.String,
  instructions: "Answer as JSON.",
  toolkit: Toolkit.empty,
  policy,
});

const sourceThread = Schema.decodeSync(ThreadId)("peer-source-thread");
const destinationThread = Schema.decodeSync(ThreadId)("peer-destination-thread");
const otherThread = Schema.decodeSync(ThreadId)("peer-other-thread");
const caller = Schema.decodeSync(Principal)("peer-caller");
const otherCaller = Schema.decodeSync(Principal)("peer-other-caller");
const transport = Schema.decodeSync(Principal)("peer-transport");
const otherTransport = Schema.decodeSync(Principal)("peer-other-transport");
const key = (value: string) => Schema.decodeSync(IdempotencyKey)(value);
const digest = Schema.decodeSync(Digest)("a".repeat(64));
const definitions = DefinitionDigests.make({ agent: digest, model: digest, tools: digest });
const peer = { name: "colleague", target: destination };
const back = { name: "answer", target: source };

const parts: ReadonlyArray<Response.StreamPartEncoded> = [
  { type: "text-start", id: "answer" },
  { type: "text-delta", id: "answer", delta: '"done"' },
  { type: "text-end", id: "answer" },
  { type: "finish", reason: "stop", usage: { inputTokens: {}, outputTokens: {} } },
];

/** Real stores and coordinator; only external policy, routes, model and fault boundary vary. */
const makeHarness = Effect.fn(function* (
  options: {
    readonly lifetimeMillis?: number;
    readonly maxMessagesPerSource?: number;
    readonly maxPendingPerOwner?: number;
  } = {},
) {
  const controls = {
    fault: undefined as string | undefined,
    route: destinationThread,
    routeCalls: 0,
    denied: (_request: PeerAuthorizationRequest): boolean => false,
    deliveryPrincipal: (principal: Principal): Principal =>
      principal === otherCaller ? otherTransport : transport,
    authorizations: new Array<PeerAuthorizationRequest>(),
    modelCalls: new Array<string>(),
  };

  const faults = Layer.succeed(MessageDeliveryFailpoint, {
    hit: (point) =>
      Effect.suspend(() =>
        point === controls.fault
          ? Effect.fail(MessageDeliveryFailpointError.make({ point }))
          : Effect.void,
      ),
  });

  const shared = yield* Layer.build(
    Layer.mergeAll(
      MemorySubmissionLedgerLive,
      MemoryThreadStoreLive,
      memoryMessageDeliveryStoreLayer({
        maxPendingPerOwner: options.maxPendingPerOwner ?? 100,
        maxRetainedPerOwner: 100,
        maxEnvelopeBytes: 262_144,
      }),
      WakeScheduler.layerNoop,
      DurableRuntimeFailpoint.layer,
      ToolReconciler.uncertain,
      RunToolAuthorization.allowAll,
      DurableRuntimeConfig.layer({
        deploymentId: Schema.decodeSync(DeploymentId)("peer-test"),
        producerId: Schema.decodeSync(ProducerId)("peer-test"),
        leaseRenewalInterval: Duration.seconds(5),
        settlementPollInterval: Duration.millis(100),
      }),
      Layer.succeed(PeerAuthorizer, {
        authorize: (request) =>
          Effect.suspend(() => {
            controls.authorizations.push(request);
            if (controls.denied(request)) {
              return Effect.fail(
                MessagingError.make({ operation: request.operation, reason: "denied" }),
              );
            }

            return Effect.succeed(
              request.access === "send"
                ? controls.deliveryPrincipal(request.principal)
                : request.principal,
            );
          }),
      }),
      Layer.succeed(PeerRoutes, {
        resolve: () =>
          Effect.sync(() => {
            controls.routeCalls += 1;

            return controls.route;
          }),
      }),
    ).pipe(Layer.provideMerge(faults), Layer.provideMerge(NodeCrypto.layer)),
  );

  const bindings = yield* Effect.forEach([source, destination], (definition) => {
    const model = Model.make(
      "scripted",
      definition.id,
      Layer.effect(
        LanguageModel.LanguageModel,
        LanguageModel.make({
          generateText: () => Effect.succeed([]),
          streamText: () =>
            Stream.suspend(() => {
              controls.modelCalls.push(definition.id);

              return Stream.fromIterable(parts);
            }),
        }),
      ),
    );

    return DurableWorkerBinding.make(Agent.withModel(definition, model), definitions);
  });

  const makeRuntime = Effect.fn(function* (lifetimeMillis = options.lifetimeMillis ?? 60_000) {
    const context = yield* Layer.build(DurableAgentRuntime.layerWithBindings(bindings)).pipe(
      Effect.provideService(PeerDeliveryLifetime, lifetimeMillis),
      Effect.provideService(PeerMessageCapacity, options.maxMessagesPerSource ?? 256),
      Effect.provide(shared),
    );

    return Context.get(context, DurableAgentRuntime);
  });

  const runtime = yield* makeRuntime();

  const makeAdmission = (
    host: DurableAgentRuntime["Service"],
  ): PreparedInputAdmission["Service"] => ({
    submit: (envelope) =>
      host
        .submit({ definition: { id: envelope.agentId, input: PersistedJson } }, envelope.input, {
          threadId: envelope.threadId,
          principal: envelope.deliveryPrincipal,
          idempotencyKey: envelope.admissionKey,
          definitions: envelope.definitions,
          ...(envelope.messageAdmission === undefined
            ? {}
            : { messageAdmission: envelope.messageAdmission }),
          ...(envelope.workerAdmission === undefined
            ? {}
            : { workerAdmission: envelope.workerAdmission }),
        })
        .pipe(
          Effect.mapError((error) =>
            error._tag === "AdmissionPolicyError" && error.reason === "refused"
              ? ScheduledInputRefused.make({ code: error.code })
              : ScheduledInputRetryable.make({ reason: "storage" }),
          ),
        ),
    submissionStatus: (receipt) =>
      host
        .submissionStatus(receipt)
        .pipe(Effect.mapError(() => ScheduledInputRetryable.make({ reason: "storage" }))),
  });

  const makeDriver = Effect.fn(function* (host = runtime) {
    const context = yield* Layer.build(MessageDeliveryDriver.layer()).pipe(
      Effect.provideService(PreparedInputAdmission, makeAdmission(host)),
      Effect.provide(shared),
    );

    return Context.get(context, MessageDeliveryDriver);
  });

  const store = Context.get(shared, ThreadStore);
  const deliveries = Context.get(shared, MessageDeliveryStore);
  const ledger = Context.get(shared, SubmissionLedger);

  const seed = Effect.fn(function* (threadId: ThreadId, definition: typeof source) {
    const receipt = yield* runtime.submit(
      { definition },
      { text: "seed" },
      {
        threadId,
        principal: caller,
        idempotencyKey: key("seed"),
        definitions,
      },
    );

    yield* runtime.processThreadResolved(threadId);
    expect((yield* runtime.submissionStatus(receipt))._tag).toBe("settled");

    return receipt;
  });

  const sourceReceipt = yield* seed(sourceThread, source);
  const destinationReceipt = yield* seed(destinationThread, destination);
  const host = yield* runtime.messagingHost({ sourceThreadId: sourceThread, principal: caller });

  const receiver = yield* runtime.messagingHost({
    sourceThreadId: destinationThread,
    principal: caller,
  });

  const driver = yield* makeDriver();
  const history = (threadId: ThreadId) => store.export(ThreadExportRequest.make({ threadId }));

  const proofs = (threadId = sourceThread) =>
    history(threadId).pipe(
      Effect.map((log) =>
        log.records
          .map(({ record }) => record.payload)
          .filter((payload) => payload._tag === "PeerMessagePrepared"),
      ),
    );

  const row = Effect.fn(function* (message: MessageRef) {
    const found = yield* deliveries.get(message);

    if (found === null) throw new Error("Expected retained message delivery");

    return found;
  });

  const send = (id = "message") =>
    host.send({ ...peer, encodedInput: { text: "hello" }, idempotencyKey: key(id) });

  const submitEnvelope = (envelope: PreparedInput, overrides: Partial<DurableSubmitOptions> = {}) =>
    runtime.submit({ definition: { id: envelope.agentId, input: PersistedJson } }, envelope.input, {
      threadId: envelope.threadId,
      principal: envelope.deliveryPrincipal,
      idempotencyKey: envelope.admissionKey,
      definitions: envelope.definitions,
      ...(envelope.messageAdmission === undefined
        ? {}
        : { messageAdmission: envelope.messageAdmission }),
      ...overrides,
    });

  return {
    controls,
    runtime,
    makeRuntime,
    makeDriver,
    ledger,
    deliveries,
    sourceReceipt,
    destinationReceipt,
    host,
    receiver,
    driver,
    seed,
    history,
    proofs,
    row,
    send,
    submitEnvelope,
  };
});

describe("durable peer messaging boundaries", () => {
  it.effect(
    "delivers from a settled source to a settled receiver across runtime reconstruction",
    () =>
      Effect.gen(function* () {
        const h = yield* makeHarness();
        const pending = yield* h.send();

        expect(pending.status).toBe("pending");
        expect(pending.receipt).toBeNull();
        const frozen = yield* h.row(pending.message);

        expect(frozen.envelope.deliveryPrincipal).toBe(transport);
        expect((yield* h.proofs())[0]?.sourcePrincipal).toBe(caller);

        const restarted = yield* h.makeRuntime();
        const pump = yield* h.makeDriver(restarted);
        const [accepted] = yield* pump.runDue(sourceThread);

        expect(accepted?.status).toBe("accepted");
        if (accepted === undefined || accepted.receipt === null) {
          throw new Error("Expected accepted Receipt");
        }

        expect((yield* restarted.submissionStatus(accepted.receipt))._tag).toBe("pending");
        expect((yield* h.host.inspect({ ...peer, message: pending.message })).status).toBe(
          "accepted",
        );
        expect((yield* restarted.submissionStatus(h.sourceReceipt))._tag).toBe("settled");
        expect((yield* restarted.submissionStatus(h.destinationReceipt))._tag).toBe("settled");

        yield* restarted.processThreadResolved(destinationThread);
        yield* TestClock.adjust("5 seconds");
        const processed = yield* pump.process(pending.message);

        expect(processed.status).toBe("processed");
        expect(processed.receipt).toEqual(accepted.receipt);
        expect(processed.settlement?.outcome).toBe("completed");
        expect(h.controls.modelCalls).toEqual([source.id, destination.id, destination.id]);
        const inbox = yield* h.receiver.inbox({ ...back, limit: 10 });

        expect(inbox.items.map((item) => item.admission)).toEqual([
          frozen.envelope.messageAdmission,
        ]);
        expect((yield* h.send()).message).toEqual(pending.message);
        expect((yield* h.send()).status).toBe("processed");
      }).pipe(Effect.scoped),
  );

  it.effect("fails before source proof append without leaving a message obligation", () =>
    Effect.gen(function* () {
      const h = yield* makeHarness();
      const before = yield* h.history(sourceThread);

      h.controls.fault = "peer:before-prepared-append";
      expect(yield* h.send().pipe(Effect.flip)).toMatchObject({ reason: "storage" });
      expect((yield* h.history(sourceThread)).tailDigest).toBe(before.tailDigest);
      expect((yield* h.deliveries.list({ ownerThreadId: sourceThread, limit: 10 })).items).toEqual(
        [],
      );
      h.controls.fault = undefined;
      expect((yield* h.send()).status).toBe("pending");
      expect(yield* h.proofs()).toHaveLength(1);
    }).pipe(Effect.scoped),
  );

  it.effect.each([
    "peer:after-prepared-append",
    "message-delivery:insert:before",
    "message-delivery:insert:after",
  ])("replays the frozen route, input, principal and lifetime after %s", (point) =>
    Effect.gen(function* () {
      const h = yield* makeHarness();

      h.controls.fault = point;
      expect(yield* h.send().pipe(Effect.flip)).toMatchObject({ reason: "storage" });
      const [proof] = yield* h.proofs();

      if (proof === undefined)
        throw new Error("Expected canonical proof before failed acknowledgement");
      const envelope = yield* Schema.decodeUnknownEffect(PreparedInput)(proof.encodedEnvelope);

      expect(
        (yield* h.deliveries.list({ ownerThreadId: sourceThread, limit: 10 })).items,
      ).toHaveLength(point === "message-delivery:insert:after" ? 1 : 0);
      h.controls.fault = undefined;
      h.controls.route = otherThread;
      yield* TestClock.adjust("1 second");
      const restarted = yield* h.makeRuntime(120_000);

      const host = yield* restarted.messagingHost({
        sourceThreadId: sourceThread,
        principal: caller,
      });

      h.controls.denied = (request) =>
        request.access === "send" && request.destination?.threadId === destinationThread;
      expect(
        yield* host
          .send({ ...peer, encodedInput: { text: "hello" }, idempotencyKey: key("message") })
          .pipe(Effect.flip),
      ).toMatchObject({ reason: "denied" });
      expect(
        (yield* h.deliveries.list({ ownerThreadId: sourceThread, limit: 10 })).items,
      ).toHaveLength(point === "message-delivery:insert:after" ? 1 : 0);
      h.controls.denied = () => false;

      const replayed = yield* host.send({
        ...peer,
        encodedInput: { text: "hello" },
        idempotencyKey: key("message"),
      });

      const retained = yield* h.row(replayed.message);

      expect(retained.envelope).toEqual(envelope);
      expect(retained.envelope.threadId).toBe(destinationThread);
      expect(retained.initialDeadlineAtMillis).toBe(proof.deadlineAtMillis);
      expect(retained.deadlineAtMillis).toBe(proof.deadlineAtMillis);
      expect(h.controls.routeCalls).toBe(1);
      expect(yield* h.proofs()).toHaveLength(1);
      const before = yield* h.history(sourceThread);

      expect(
        yield* host
          .send({ ...peer, encodedInput: { text: "changed" }, idempotencyKey: key("message") })
          .pipe(Effect.flip),
      ).toMatchObject({ reason: "conflict" });
      expect((yield* h.history(sourceThread)).tailDigest).toBe(before.tailDigest);
      expect(
        h.controls.authorizations.some(
          (request) =>
            request.access === "send" &&
            request.principal === caller &&
            request.destination?.threadId === destinationThread,
        ),
      ).toBe(true);
      h.controls.deliveryPrincipal = () => otherTransport;
      expect(
        yield* host
          .send({ ...peer, encodedInput: { text: "hello" }, idempotencyKey: key("message") })
          .pipe(Effect.flip),
      ).toMatchObject({ reason: "denied" });
      expect(yield* h.row(replayed.message)).toEqual(retained);
    }).pipe(Effect.scoped),
  );

  it.effect("deduplicates destination admission when its acknowledgement is lost", () =>
    Effect.gen(function* () {
      const h = yield* makeHarness();
      const pending = yield* h.send();
      const frozen = yield* h.row(pending.message);

      h.controls.fault = "message-delivery:admission:after";
      expect(yield* h.driver.process(pending.message).pipe(Effect.flip)).toMatchObject({
        _tag: "MessageDeliveryFailpointError",
        point: "message-delivery:admission:after",
      });

      const admitted = yield* h.ledger.lookup(
        SubmissionLookupByKey.make({
          threadId: frozen.envelope.threadId,
          principal: frozen.envelope.deliveryPrincipal,
          idempotencyKey: frozen.envelope.admissionKey,
        }),
      );

      expect(Option.isSome(admitted)).toBe(true);
      if (Option.isNone(admitted)) throw new Error("Expected committed destination admission");
      expect((yield* h.row(pending.message)).receipt).toBeNull();
      h.controls.fault = undefined;
      yield* TestClock.adjust("31 seconds");
      const restarted = yield* h.makeRuntime();
      const pump = yield* h.makeDriver(restarted);
      const accepted = yield* pump.process(pending.message);

      expect(accepted.status).toBe("accepted");
      expect(accepted.receipt).toMatchObject({
        submissionId: admitted.value.submissionId,
        receiptId: admitted.value.receiptId,
        threadId: admitted.value.threadId,
      });
      yield* restarted.processThreadResolved(destinationThread);

      const messages = (yield* h.history(destinationThread)).records.filter(
        ({ record }) =>
          record.payload._tag === "UserInputRecorded" &&
          record.payload.messageAdmission !== undefined,
      );

      expect(messages).toHaveLength(1);
      expect(h.controls.modelCalls).toEqual([source.id, destination.id, destination.id]);
    }).pipe(Effect.scoped),
  );

  it.effect(
    "refuses forged admission and altered sender, return address, correlation and delivery identity before mutation",
    () =>
      Effect.gen(function* () {
        const h = yield* makeHarness();
        const pending = yield* h.send();
        const { envelope } = yield* h.row(pending.message);
        const metadata = envelope.messageAdmission;

        if (metadata === undefined) throw new Error("Expected peer metadata");

        const nonexistent = MessageRef.make({
          ownerThreadId: sourceThread,
          messageId: key("not-prepared"),
        });

        const cases: ReadonlyArray<Partial<DurableSubmitOptions>> = [
          {
            messageAdmission: { ...metadata, message: nonexistent },
            idempotencyKey: nonexistent.messageId,
          },
          {
            messageAdmission: {
              ...metadata,
              returnAddress: { threadId: otherThread, agentId: source.id },
            },
          },
          {
            messageAdmission: {
              ...metadata,
              sender: { threadId: sourceThread, agentId: destination.id },
              returnAddress: { threadId: sourceThread, agentId: destination.id },
            },
          },
          { messageAdmission: { ...metadata, inReplyTo: nonexistent } },
          { principal: otherTransport },
          { idempotencyKey: key("different-admission") },
        ];

        const before = yield* h.history(destinationThread);

        for (const overrides of cases) {
          expect(yield* h.submitEnvelope(envelope, overrides).pipe(Effect.flip)).toMatchObject({
            _tag: "AdmissionPolicyError",
            reason: "refused",
          });
          expect((yield* h.history(destinationThread)).tailDigest).toBe(before.tailDigest);
          expect(
            Option.isNone(
              yield* h.ledger.lookup(
                SubmissionLookupByKey.make({
                  threadId: destinationThread,
                  principal: overrides.principal ?? envelope.deliveryPrincipal,
                  idempotencyKey: overrides.idempotencyKey ?? envelope.admissionKey,
                }),
              ),
            ),
          ).toBe(true);
        }
        expect(
          yield* h.submitEnvelope({ ...envelope, input: { text: "changed" } }).pipe(Effect.flip),
        ).toMatchObject({ _tag: "AdmissionPolicyError", reason: "refused" });
        expect((yield* h.history(destinationThread)).tailDigest).toBe(before.tailDigest);
        expect((yield* h.row(pending.message)).status).toBe("pending");
      }).pipe(Effect.scoped),
  );

  it.effect(
    "requires canonical received input for replies; a correlation ref confers no authority",
    () =>
      Effect.gen(function* () {
        const h = yield* makeHarness();

        const correlation = MessageRef.make({
          ownerThreadId: sourceThread,
          messageId: key("correlation-only"),
        });

        const pending = yield* h.host.send({
          ...peer,
          encodedInput: { text: "hello" },
          idempotencyKey: key("message"),
          inReplyTo: correlation,
        });

        const before = yield* h.history(destinationThread);

        for (const inReplyTo of [correlation, pending.message]) {
          expect(
            yield* h.receiver
              .reply({
                ...back,
                encodedInput: { text: "reply" },
                idempotencyKey: key("reply"),
                inReplyTo,
              })
              .pipe(Effect.flip),
          ).toMatchObject({ reason: "invalid-reference" });
        }
        expect((yield* h.history(destinationThread)).tailDigest).toBe(before.tailDigest);
        expect(yield* h.proofs(destinationThread)).toHaveLength(0);
        expect(
          (yield* h.deliveries.list({ ownerThreadId: destinationThread, limit: 10 })).items,
        ).toEqual([]);
        yield* h.driver.process(pending.message);
        yield* h.runtime.processThreadResolved(destinationThread);
        expect(
          yield* h.receiver
            .reply({
              ...back,
              encodedInput: { text: "reply" },
              idempotencyKey: key("reply"),
              inReplyTo: correlation,
            })
            .pipe(Effect.flip),
        ).toMatchObject({ reason: "invalid-reference" });
      }).pipe(Effect.scoped),
  );

  it.effect(
    "keeps reply-only authority and the recorded return address through replay and ingress",
    () =>
      Effect.gen(function* () {
        const h = yield* makeHarness();
        const inbound = yield* h.send();

        yield* h.driver.process(inbound.message);
        yield* h.runtime.processThreadResolved(destinationThread);
        h.controls.route = otherThread;
        h.controls.denied = (request) =>
          request.source.threadId === destinationThread && request.operation === "send";
        expect(
          yield* h.receiver
            .send({ ...back, encodedInput: { text: "reply" }, idempotencyKey: key("reply") })
            .pipe(Effect.flip),
        ).toMatchObject({ reason: "denied" });

        const request = {
          ...back,
          encodedInput: { text: "reply" },
          idempotencyKey: key("reply"),
          inReplyTo: inbound.message,
        };

        const reply = yield* h.receiver.reply(request);

        expect((yield* h.row(reply.message)).envelope.threadId).toBe(sourceThread);
        expect((yield* h.receiver.reply(request)).message).toEqual(reply.message);
        expect((yield* h.proofs(destinationThread))[0]?.operation).toBe("reply");
        const restarted = yield* h.makeRuntime();
        const pump = yield* h.makeDriver(restarted);
        const accepted = yield* pump.process(reply.message);

        expect(accepted.status).toBe("accepted");
        yield* restarted.processThreadResolved(sourceThread);
        const inbox = yield* h.host.inbox({ name: "answer", target: destination, limit: 10 });

        expect(inbox.items[0]?.admission.inReplyTo).toEqual(inbound.message);
        expect(inbox.items[0]?.admission.sender.threadId).toBe(destinationThread);
        expect(h.controls.routeCalls).toBe(1);
      }).pipe(Effect.scoped),
  );

  it.effect(
    "separates read, send and control grants and reauthorizes each inbox sender Thread",
    () =>
      Effect.gen(function* () {
        const h = yield* makeHarness();

        h.controls.denied = (request) => request.access === "read" || request.access === "control";
        expect((yield* h.host.context)._tag).toBe("programmatic");
        const pending = yield* h.send();

        expect(
          yield* h.host.inspect({ ...peer, message: pending.message }).pipe(Effect.flip),
        ).toMatchObject({ reason: "denied" });
        expect(
          yield* h.host.retry({ ...peer, message: pending.message }).pipe(Effect.flip),
        ).toMatchObject({ reason: "denied" });
        expect(yield* h.receiver.inbox({ ...back, limit: 10 }).pipe(Effect.flip)).toMatchObject({
          reason: "denied",
        });
        expect((yield* h.row(pending.message)).version).toBe(1);
        h.controls.denied = () => false;
        yield* h.driver.process(pending.message);
        yield* h.runtime.processThreadResolved(destinationThread);
        yield* h.seed(otherThread, source);

        const other = yield* h.runtime.messagingHost({
          sourceThreadId: otherThread,
          principal: caller,
        });

        const another = yield* other.send({
          ...peer,
          encodedInput: { text: "another sender" },
          idempotencyKey: key("message"),
        });

        yield* h.driver.process(another.message);
        yield* h.runtime.processThreadResolved(destinationThread);
        const firstPage = yield* h.receiver.inbox({ ...back, limit: 1 });

        expect(firstPage.items[0]?.admission.sender.threadId).toBe(sourceThread);
        expect(firstPage.next).not.toBeNull();
        h.controls.denied = (request) =>
          request.access === "read" && request.destination?.threadId === otherThread;
        expect(yield* h.receiver.inbox({ ...back, limit: 10 }).pipe(Effect.flip)).toMatchObject({
          reason: "denied",
        });
        expect(
          h.controls.authorizations.some(
            (request) =>
              request.operation === "inbox" &&
              request.destination?.threadId === otherThread &&
              request.destination.agentId === source.id,
          ),
        ).toBe(true);
      }).pipe(Effect.scoped),
  );

  it.effect("counts canonical source proofs even when the delivery store cannot insert them", () =>
    Effect.gen(function* () {
      const h = yield* makeHarness({ maxMessagesPerSource: 2, maxPendingPerOwner: 1 });

      yield* h.send("first");
      expect(yield* h.send("second").pipe(Effect.flip)).toMatchObject({ reason: "capacity" });
      expect(yield* h.proofs()).toHaveLength(2);
      const before = yield* h.history(sourceThread);

      expect(yield* h.send("third").pipe(Effect.flip)).toMatchObject({ reason: "capacity" });
      expect((yield* h.history(sourceThread)).tailDigest).toBe(before.tailDigest);
      expect(
        (yield* h.deliveries.list({ ownerThreadId: sourceThread, limit: 10 })).items,
      ).toHaveLength(1);
      expect((yield* h.send("first")).status).toBe("pending");
      expect(yield* h.send("second").pipe(Effect.flip)).toMatchObject({ reason: "capacity" });
      expect(yield* h.proofs()).toHaveLength(2);
    }).pipe(Effect.scoped),
  );

  it.effect("rejects malformed destination wire input before recording proof or delivery", () =>
    Effect.gen(function* () {
      const h = yield* makeHarness();
      const before = yield* h.history(sourceThread);

      expect(
        yield* h.host
          .send({ ...peer, encodedInput: { text: 123 }, idempotencyKey: key("invalid") })
          .pipe(Effect.flip),
      ).toMatchObject({ reason: "invalid-input" });
      expect((yield* h.history(sourceThread)).tailDigest).toBe(before.tailDigest);
      expect(yield* h.proofs()).toHaveLength(0);
      expect(h.controls.routeCalls).toBe(0);
      expect((yield* h.deliveries.list({ ownerThreadId: sourceThread, limit: 10 })).items).toEqual(
        [],
      );
    }).pipe(Effect.scoped),
  );

  it.effect("keeps principal identities distinct even when callers reuse the same client key", () =>
    Effect.gen(function* () {
      const h = yield* makeHarness();
      const first = yield* h.send();

      const other = yield* h.runtime.messagingHost({
        sourceThreadId: sourceThread,
        principal: otherCaller,
      });

      const second = yield* other.send({
        ...peer,
        encodedInput: { text: "hello" },
        idempotencyKey: key("message"),
      });

      expect(second.message).not.toEqual(first.message);
      expect((yield* h.row(first.message)).envelope.deliveryPrincipal).toBe(transport);
      expect((yield* h.row(second.message)).envelope.deliveryPrincipal).toBe(otherTransport);
      expect((yield* h.proofs()).map((proof) => proof.sourcePrincipal)).toEqual([
        caller,
        otherCaller,
      ]);
    }).pipe(Effect.scoped),
  );

  it.effect(
    "reauthorizes parked retries and renews only the frozen envelope's delivery window",
    () =>
      Effect.gen(function* () {
        const h = yield* makeHarness({ lifetimeMillis: 1_000 });
        const pending = yield* h.send();
        const frozen = yield* h.row(pending.message);

        yield* TestClock.adjust("1001 millis");
        const parked = yield* h.driver.process(pending.message);

        expect(parked.status).toBe("parked");
        expect(parked.parkReason).toBe("deadline");
        h.controls.route = otherThread;
        h.controls.denied = (request) => request.access === "send";
        expect(
          yield* h.host.retry({ ...peer, message: pending.message }).pipe(Effect.flip),
        ).toMatchObject({ reason: "denied" });
        expect(yield* h.row(pending.message)).toEqual(parked);
        h.controls.denied = (request) => request.access === "control";
        expect(
          yield* h.host.retry({ ...peer, message: pending.message }).pipe(Effect.flip),
        ).toMatchObject({ reason: "denied" });
        expect(yield* h.row(pending.message)).toEqual(parked);
        h.controls.denied = () => false;
        expect((yield* h.host.retry({ ...peer, message: pending.message })).status).toBe("pending");
        const recovered = yield* h.row(pending.message);

        expect(recovered.envelope).toEqual(frozen.envelope);
        expect(recovered.envelopeDigest).toBe(frozen.envelopeDigest);
        expect(recovered.initialDeadlineAtMillis).toBe(frozen.initialDeadlineAtMillis);
        expect(recovered.deadlineAtMillis).toBeGreaterThan(frozen.deadlineAtMillis);
        expect(recovered.retry.generation).toBe(1);
        expect((yield* h.driver.process(pending.message)).status).toBe("accepted");
        expect(h.controls.routeCalls).toBe(1);
      }).pipe(Effect.scoped),
  );

  it.effect("permanently refuses revoked ingress without admitting input to the receiver", () =>
    Effect.gen(function* () {
      const h = yield* makeHarness();
      const pending = yield* h.send();
      const before = yield* h.history(destinationThread);

      h.controls.denied = (request) => request.access === "send";
      const refused = yield* h.driver.process(pending.message);

      expect(refused.status).toBe("refused");
      expect(refused.refusal).toBe("message-denied");
      expect(refused.receipt).toBeNull();
      expect((yield* h.history(destinationThread)).tailDigest).toBe(before.tailDigest);
      h.controls.denied = () => false;
      expect(yield* h.driver.process(pending.message)).toEqual(refused);
      expect((yield* h.send()).status).toBe("refused");
    }).pipe(Effect.scoped),
  );
});

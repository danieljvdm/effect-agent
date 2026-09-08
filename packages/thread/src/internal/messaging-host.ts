import type { AgentId, ThreadId } from "@effect-agent/core/Identifiers";
import type { InboxPage } from "@effect-agent/core/Messaging";
import {
  MessageAdmission,
  MessageRef,
  MessageStatus,
  MessagingError,
  PeerName,
} from "@effect-agent/core/Messaging";
import { IdempotencyKey, type Principal } from "@effect-agent/core/Receipt";
import type { WorkerSource } from "@effect-agent/core/Worker";
import {
  type MessagingHost,
  type PeerTarget,
  type SendPeerMessage,
} from "@effect-agent/engine/MessagingHost";
import { Clock, Crypto, DateTime, Effect, Option, Schema, Stream } from "effect";

import { digestJson } from "../Digest.ts";
import type { DurableSubmitOptions } from "../DurableAgentRuntime.ts";
import {
  MessageDeliveryFailpoint,
  MessageDeliveryStore,
  type MessageDeliveryRecord,
  prepareMessageDelivery,
} from "../MessageDelivery.ts";
import {
  PeerAuthorizer,
  PeerRoutes,
  PeerDeliveryLifetime,
  PeerMessageCapacity,
} from "../MessagingHost.ts";
import {
  BatchId,
  CanonicalBatch,
  CanonicalSequence,
  type DeploymentId,
  type Digest,
  PeerMessagePrepared,
  PersistedJson,
  type ProducerId,
  RecordEnvelope,
  RecordId,
} from "../Records.ts";
import { PreparedInput } from "../Subscription.ts";
import {
  FencedAppendRequest,
  ThreadExportRequest,
  ThreadRead,
  ThreadTailRequest,
  ThreadStore,
} from "../ThreadStore.ts";
import {
  definitionDigestsEqual,
  resolveDefinitionBinding,
  type ResolvedBinding,
} from "./agent-registration.ts";

export interface MessagingRuntimeOptions {
  readonly bindings: ReadonlyArray<ResolvedBinding>;
  readonly deploymentId: DeploymentId;
  readonly producerId: ProducerId;
}

const failure = (operation: MessagingError["operation"], reason: MessagingError["reason"]) =>
  MessagingError.make({ operation, reason });

const sameRef = Schema.toEquivalence(MessageRef);
const sameAdmission = Schema.toEquivalence(MessageAdmission);
const sameJson = Schema.toEquivalence(PersistedJson);

/** Thread-owned durable peer delivery. Source proof and the independent due index survive Run settlement. */
export const makeMessagingRuntime = Effect.fn("MessagingHost.make")(function* (
  options: MessagingRuntimeOptions,
) {
  const deps = {
    ...options,
    store: yield* ThreadStore,
    deliveries: yield* Effect.serviceOption(MessageDeliveryStore),
    crypto: yield* Crypto.Crypto,
    authorizer: yield* PeerAuthorizer,
    routes: yield* PeerRoutes,
    lifetimeMillis: yield* PeerDeliveryLifetime,
    maxMessagesPerSource: yield* PeerMessageCapacity,
    failpoint: yield* MessageDeliveryFailpoint,
  };

  const digest = (value: Schema.Json) =>
    digestJson(value).pipe(
      Effect.provideService(Crypto.Crypto, deps.crypto),
      Effect.mapError(() => failure("send", "storage")),
    );

  const read = (threadId: ThreadId) =>
    deps.store
      .export(ThreadExportRequest.make({ threadId }))
      .pipe(Effect.mapError(() => failure("context", "storage")));

  const sourceOf = Effect.fn("MessagingHost.source")(function* (threadId: ThreadId) {
    const log = yield* read(threadId);
    const created = log.records[0]?.record.payload;

    if (created?._tag !== "ThreadCreated") return yield* failure("context", "not-found");

    const matching = deps.bindings.filter(
      (binding) =>
        binding.agentId === created.agentId &&
        definitionDigestsEqual(binding.digests, created.definitions),
    );

    if (matching.length !== 1) return yield* failure("context", "binding-mismatch");

    return { log, address: { threadId, agentId: created.agentId } };
  });

  const decodeProof = Effect.fn("MessagingHost.decodeProof")(function* (record: RecordEnvelope) {
    if (record.payload._tag !== "PeerMessagePrepared") return yield* failure("send", "corrupt");

    const envelope = yield* Schema.decodeUnknownEffect(PreparedInput)(
      record.payload.encodedEnvelope,
    ).pipe(Effect.mapError(() => failure("send", "corrupt")));

    if (
      envelope.messageAdmission === undefined ||
      envelope.messageAdmission.message.messageId !== record.payload.messageId
    )
      return yield* failure("send", "corrupt");

    return {
      envelope,
      sourcePrincipal: record.payload.sourcePrincipal,
      operation: record.payload.operation,
      deadlineAtMillis: record.payload.deadlineAtMillis,
      createdAtMillis: DateTime.toEpochMillis(record.createdAt),
    };
  });

  const proof = Effect.fn("MessagingHost.proof")(function* (message: MessageRef) {
    const source = yield* sourceOf(message.ownerThreadId);

    const record = source.log.records.find(
      ({ record }) =>
        record.payload._tag === "PeerMessagePrepared" &&
        record.payload.messageId === message.messageId,
    )?.record;

    if (record === undefined) return yield* failure("reply", "invalid-reference");
    const saved = yield* decodeProof(record);

    if (
      saved.envelope.messageAdmission?.sender.agentId !== source.address.agentId ||
      !sameRef(saved.envelope.messageAdmission.message, message)
    )
      return yield* failure("reply", "invalid-reference");

    return saved;
  });

  const authorizeEnvelope = Effect.fn("MessagingHost.authorizeEnvelope")(function* (
    saved: Effect.Success<ReturnType<typeof proof>>,
    operation: "send" | "reply",
  ) {
    const metadata = saved.envelope.messageAdmission;

    if (metadata === undefined) return yield* failure(operation, "corrupt");

    const principal = yield* deps.authorizer.authorize({
      source: metadata.sender,
      principal: saved.sourcePrincipal,
      operation: saved.operation,
      access: "send",
      peerName: metadata.peerName,
      destination: { threadId: saved.envelope.threadId, agentId: saved.envelope.agentId },
    });

    if (principal !== saved.envelope.deliveryPrincipal) return yield* failure(operation, "denied");
  });

  const status = (row: MessageDeliveryRecord): MessageStatus =>
    MessageStatus.make({
      message: row.key,
      status: row.status,
      receipt: row.receipt,
      settlement:
        row.settlement === null
          ? null
          : { settlementId: row.settlement.settlementId, outcome: row.settlement.outcome },
      reason: row.refusal ?? row.parkReason,
    });

  const deliveries = (operation: MessagingError["operation"]) =>
    Option.isSome(deps.deliveries)
      ? Effect.succeed(deps.deliveries.value)
      : failure(operation, "unavailable");

  const mapStore =
    (operation: MessagingError["operation"]) =>
    (error: { readonly _tag: string; readonly reason?: string }) =>
      failure(
        operation,
        error.reason === "capacity"
          ? "capacity"
          : error.reason === "conflict"
            ? "conflict"
            : "storage",
      );

  const lifetime = Schema.decodeUnknownEffect(
    Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(604_800_000)),
  )(deps.lifetimeMillis).pipe(Effect.mapError(() => failure("send", "capacity")));

  const capacity = Schema.decodeUnknownEffect(
    Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(1_000)),
  )(deps.maxMessagesPerSource ?? 256).pipe(Effect.mapError(() => failure("send", "capacity")));

  const facet = (
    threadId: ThreadId,
    principal: Principal,
    toolSource?: WorkerSource,
  ): MessagingHost["Service"] => {
    const authorize = Effect.fn("MessagingHost.authorize")(function* (
      operation: MessagingError["operation"],
      access: "context" | "read" | "send" | "control",
      peer?: PeerTarget,
      destination?: { readonly threadId: ThreadId; readonly agentId: AgentId },
    ) {
      const source = yield* sourceOf(threadId);

      if (
        toolSource !== undefined &&
        (toolSource.agentId !== source.address.agentId || toolSource.threadId !== threadId)
      )
        return yield* failure(operation, "denied");
      if (peer !== undefined)
        yield* Schema.decodeUnknownEffect(PeerName)(peer.name).pipe(
          Effect.mapError(() => failure(operation, "invalid-input")),
        );

      const deliveryPrincipal = yield* deps.authorizer.authorize({
        source: source.address,
        principal,
        operation,
        access,
        ...(peer === undefined ? {} : { peerName: peer.name }),
        ...(destination === undefined ? {} : { destination }),
      });

      return { ...source, deliveryPrincipal };
    });

    const send = Effect.fn("MessagingHost.send")(function* (
      request: SendPeerMessage,
      operation: "send" | "reply",
    ) {
      const initial = yield* authorize(operation, "send", request);

      const target = yield* resolveDefinitionBinding(deps.bindings, request.target).pipe(
        Effect.mapError(() => failure(operation, "binding-mismatch")),
      );

      const encoded = yield* Schema.decodeUnknownEffect(Schema.toEncoded(target.definition.input))(
        request.encodedInput,
      ).pipe(Effect.mapError(() => failure(operation, "invalid-input")));

      const input = yield* Schema.decodeUnknownEffect(PersistedJson)(encoded).pipe(
        Effect.flatMap(Schema.encodeEffect(Schema.fromJsonString(PersistedJson))),
        Effect.flatMap(Schema.decodeEffect(Schema.fromJsonString(PersistedJson))),
        Effect.mapError(() => failure(operation, "invalid-input")),
      );

      const key = yield* Schema.decodeUnknownEffect(IdempotencyKey)(request.idempotencyKey).pipe(
        Effect.mapError(() => failure(operation, "invalid-input")),
      );

      const inReplyTo =
        request.inReplyTo === undefined
          ? undefined
          : yield* Schema.decodeUnknownEffect(MessageRef)(request.inReplyTo).pipe(
              Effect.mapError(() => failure(operation, "invalid-reference")),
            );

      const message: MessageRef = {
        ownerThreadId: threadId,
        messageId: Schema.decodeSync(IdempotencyKey)(
          `peer:${yield* digest([threadId, principal, request.name, operation, key])}`,
        ),
      };

      const store = yield* deliveries(operation);
      const window = yield* lifetime;
      const maxMessages = yield* capacity;

      for (let attempt = 0; attempt < 8; attempt++) {
        const current = attempt === 0 ? initial : yield* sourceOf(threadId);

        const prior = current.log.records.find(
          ({ record }) =>
            record.payload._tag === "PeerMessagePrepared" &&
            record.payload.messageId === message.messageId,
        )?.record;

        let saved: Effect.Success<ReturnType<typeof proof>>;

        if (prior !== undefined) {
          saved = yield* decodeProof(prior);
          const metadata = saved.envelope.messageAdmission;

          if (
            metadata === undefined ||
            metadata.peerName !== request.name ||
            saved.envelope.agentId !== target.agentId ||
            !definitionDigestsEqual(saved.envelope.definitions, target.digests) ||
            !sameJson(saved.envelope.input, input) ||
            (inReplyTo === undefined || metadata.inReplyTo === undefined
              ? inReplyTo !== metadata.inReplyTo
              : !sameRef(inReplyTo, metadata.inReplyTo))
          )
            return yield* failure(operation, "conflict");
          yield* authorize(operation, "send", request, {
            threadId: saved.envelope.threadId,
            agentId: target.agentId,
          });
          yield* authorizeEnvelope(saved, operation);
        } else {
          if (
            current.log.records.filter(
              ({ record }) => record.payload._tag === "PeerMessagePrepared",
            ).length >= maxMessages
          )
            return yield* failure(operation, "capacity");
          let destinationThread: ThreadId;

          if (operation === "reply") {
            if (inReplyTo === undefined) return yield* failure(operation, "invalid-reference");

            const inbound = current.log.records.find(
              ({ record }) =>
                record.payload._tag === "UserInputRecorded" &&
                record.payload.messageAdmission !== undefined &&
                sameRef(record.payload.messageAdmission.message, inReplyTo),
            )?.record.payload;

            if (
              inbound?._tag !== "UserInputRecorded" ||
              inbound.messageAdmission === undefined ||
              inbound.messageAdmission.sender.agentId !== target.agentId
            )
              return yield* failure(operation, "invalid-reference");
            destinationThread = inbound.messageAdmission.returnAddress.threadId;
          } else {
            destinationThread = yield* deps.routes.resolve({
              source: current.address,
              principal,
              peerName: request.name,
              targetAgentId: target.agentId,
            });
          }

          const allowed = yield* authorize(operation, "send", request, {
            threadId: destinationThread,
            agentId: target.agentId,
          });

          const metadata = MessageAdmission.make({
            schemaVersion: 1,
            message,
            peerName: request.name,
            sender: current.address,
            returnAddress: current.address,
            ...(inReplyTo === undefined ? {} : { inReplyTo }),
          });

          const envelope = PreparedInput.make({
            schemaVersion: 1,
            threadId: destinationThread,
            agentId: target.agentId,
            definitions: target.digests,
            deliveryPrincipal: allowed.deliveryPrincipal,
            input,
            inputDigest: yield* digest(input),
            admissionKey: message.messageId,
            authorization: { policyId: "peer-message", decisionId: message.messageId },
            messageAdmission: metadata,
          });

          const encodedEnvelope = yield* Schema.encodeEffect(PreparedInput)(envelope).pipe(
            Effect.mapError(() => failure(operation, "invalid-input")),
          );

          const tail = yield* deps.store
            .inspectTail(ThreadTailRequest.make({ threadId }))
            .pipe(Effect.mapError(mapStore(operation)));

          if (
            tail.tailSequence !== current.log.tailSequence ||
            tail.tailDigest !== current.log.tailDigest
          )
            continue;
          const createdAtMillis = yield* Clock.currentTimeMillis;

          yield* deps.failpoint
            .hit("peer:before-prepared-append")
            .pipe(Effect.mapError(mapStore(operation)));

          const appended = yield* deps.store
            .append(
              FencedAppendRequest.make({
                threadId,
                producerEpoch: tail.producerEpoch,
                expectedTailSequence: tail.tailSequence,
                expectedTailDigest: tail.tailDigest,
                batch: CanonicalBatch.make({
                  batchId: Schema.decodeSync(BatchId)(message.messageId),
                  producerId: deps.producerId,
                  records: [
                    RecordEnvelope.make({
                      recordId: Schema.decodeSync(RecordId)(message.messageId),
                      family: "thread",
                      schemaVersion: 1,
                      createdAt: DateTime.makeUnsafe(createdAtMillis),
                      deploymentId: deps.deploymentId,
                      payload: PeerMessagePrepared.make({
                        messageId: message.messageId,
                        encodedEnvelope,
                        sourcePrincipal: principal,
                        operation,
                        deadlineAtMillis: createdAtMillis + window,
                      }),
                    }),
                  ],
                }),
              }),
            )
            .pipe(
              Effect.as(true),
              Effect.catchTag(["AppendConflict", "FenceRejected"], () => Effect.succeed(false)),
              Effect.mapError(mapStore(operation)),
            );

          if (!appended) continue;
          yield* deps.failpoint
            .hit("peer:after-prepared-append")
            .pipe(Effect.mapError(mapStore(operation)));
          saved = {
            envelope,
            sourcePrincipal: principal,
            operation,
            createdAtMillis,
            deadlineAtMillis: createdAtMillis + window,
          };
        }

        const prepared = yield* prepareMessageDelivery({
          key: message,
          envelope: saved.envelope,
          createdAtMillis: saved.createdAtMillis,
          deadlineAtMillis: saved.deadlineAtMillis,
        }).pipe(
          Effect.provideService(Crypto.Crypto, deps.crypto),
          Effect.mapError(mapStore(operation)),
        );

        return status(yield* store.insert(prepared).pipe(Effect.mapError(mapStore(operation))));
      }

      return yield* failure(operation, "capacity");
    });

    const lookup = Effect.fn("MessagingHost.lookup")(function* (
      request: PeerTarget & { readonly message: MessageRef },
      operation: "inspect" | "retry",
    ) {
      yield* authorize(operation, operation === "inspect" ? "read" : "control", request);

      const message = yield* Schema.decodeUnknownEffect(MessageRef)(request.message).pipe(
        Effect.mapError(() => failure(operation, "invalid-reference")),
      );

      if (message.ownerThreadId !== threadId) return yield* failure(operation, "denied");
      const store = yield* deliveries(operation);
      const row = yield* store.get(message).pipe(Effect.mapError(mapStore(operation)));

      if (row === null) return yield* failure(operation, "not-found");
      if (
        row.envelope.messageAdmission?.peerName !== request.name ||
        row.envelope.agentId !== request.target.id
      )
        return yield* failure(operation, "invalid-reference");
      yield* authorize(operation, operation === "inspect" ? "read" : "control", request, {
        threadId: row.envelope.threadId,
        agentId: row.envelope.agentId,
      });

      return { row, store };
    });

    return {
      context: authorize("context", "context").pipe(
        Effect.map(
          (source): WorkerSource => toolSource ?? { _tag: "programmatic", ...source.address },
        ),
      ),
      send: (request) => send(request, "send"),
      reply: (request) => send(request, "reply"),
      inspect: (request) => lookup(request, "inspect").pipe(Effect.map(({ row }) => status(row))),
      retry: Effect.fn("MessagingHost.retry")(function* (request) {
        const { row, store } = yield* lookup(request, "retry");

        yield* authorizeEnvelope(yield* proof(row.key), "send");
        const nowMillis = yield* Clock.currentTimeMillis;

        return status(
          yield* store
            .change(row.key, {
              _tag: "Recover",
              expectedVersion: row.version,
              nowMillis,
              deadlineAtMillis: nowMillis + (yield* lifetime),
            })
            .pipe(Effect.mapError(mapStore("retry"))),
        );
      }),
      inbox: Effect.fn("MessagingHost.inbox")(function* (request) {
        const current = yield* authorize("inbox", "read", request);

        const limit = yield* Schema.decodeUnknownEffect(
          Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(100)),
        )(request.limit).pipe(Effect.mapError(() => failure("inbox", "capacity")));

        let after = yield* Schema.decodeUnknownEffect(CanonicalSequence)(request.after ?? 0).pipe(
          Effect.mapError(() => failure("inbox", "invalid-reference")),
        );

        const items: Array<(typeof InboxPage.Type)["items"][number]> = [];

        while (after < current.log.tailSequence && items.length < limit) {
          const records = yield* Stream.runCollect(
            deps.store.read(ThreadRead.make({ threadId, afterSequence: after, limit: 1_024 })),
          ).pipe(Effect.mapError(() => failure("inbox", "storage")));

          if (records.length === 0) break;
          for (const entry of records) {
            if (entry.sequence > current.log.tailSequence) break;
            after = entry.sequence;
            const payload = entry.record.payload;

            if (
              payload._tag === "UserInputRecorded" &&
              payload.messageAdmission?.sender.agentId === request.target.id
            ) {
              yield* authorize("inbox", "read", request, payload.messageAdmission.sender);
              items.push({ sequence: entry.sequence, admission: payload.messageAdmission });
            }
            if (items.length >= limit) break;
          }
        }

        return { items, next: after < current.log.tailSequence ? after : null };
      }),
    };
  };

  return {
    acquire: Effect.fn("MessagingHost.acquire")(function* (request: {
      readonly sourceThreadId: ThreadId;
      readonly principal: Principal;
    }) {
      const service = facet(request.sourceThreadId, request.principal);

      yield* service.context;

      return service;
    }),
    forTool: (source: WorkerSource, principal: Principal) =>
      facet(source.threadId, principal, source),
    validateAdmission: Effect.fn("MessagingHost.validateAdmission")(function* (
      unvalidated: MessageAdmission,
      options: DurableSubmitOptions,
      agentId: AgentId,
      inputDigest: Digest,
    ) {
      const admission = yield* Schema.decodeUnknownEffect(MessageAdmission)(unvalidated).pipe(
        Effect.mapError(() => failure("send", "invalid-input")),
      );

      const saved = yield* proof(admission.message);
      const envelope = saved.envelope;

      if (
        envelope.messageAdmission === undefined ||
        !sameAdmission(envelope.messageAdmission, admission) ||
        envelope.threadId !== options.threadId ||
        envelope.agentId !== agentId ||
        envelope.inputDigest !== inputDigest ||
        envelope.deliveryPrincipal !== options.principal ||
        envelope.admissionKey !== options.idempotencyKey ||
        !definitionDigestsEqual(envelope.definitions, options.definitions)
      )
        return yield* failure("send", "denied");
      yield* authorizeEnvelope(saved, "send");

      return admission;
    }),
  };
});

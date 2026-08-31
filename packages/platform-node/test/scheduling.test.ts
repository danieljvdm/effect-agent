import { AgentId } from "@effect-agent/core";
import {
  DefinitionDigests,
  Digest,
  type DurableRuntimeFailpointHandler,
  DurableRuntimeFailpointError,
  Principal,
  ScheduleAuthorizer,
  ScheduleFailpoint,
  ScheduleFailpointError,
  ScheduleId,
  ScheduleStore,
  Scheduling,
  SubmissionLedger,
  SubmissionLookupByKey,
  defaultSchedulingLimits,
  type ScheduleSnapshot,
} from "@effect-agent/thread";
import { NodeFileSystem } from "@effect/platform-node";
import { expect, it } from "@effect/vitest";
import type { PlatformError } from "effect";
import {
  Context,
  Deferred,
  Effect,
  Exit,
  FileSystem,
  Layer,
  Option,
  Ref,
  Schema,
  Scope,
} from "effect";
import { TestClock } from "effect/testing";

import { NodeDurableHost, NodeScheduling } from "../src/index.ts";

const digest = Schema.decodeSync(Digest)("a".repeat(64));
const definitions = DefinitionDigests.make({ agent: digest, model: digest, tools: digest });
const principal = Schema.decodeSync(Principal)("node-scheduling-principal");
const scheduleId = Schema.decodeSync(ScheduleId)("node-recovery");
const wakeScheduleId = Schema.decodeSync(ScheduleId)("node-earlier-wake");
const lostHintScheduleId = Schema.decodeSync(ScheduleId)("node-lost-hint");
const stoppedScheduleId = Schema.decodeSync(ScheduleId)("node-stopped-driver");
const agent = {
  definition: {
    id: Schema.decodeSync(AgentId)("node-scheduled-agent"),
    input: Schema.Struct({ question: Schema.String }),
  },
};
const scheduleScope = {
  owner: { tenantId: "node-scheduling-tenant", ownerId: "node-scheduling-owner" },
  principal,
};

const authorizerLayer = Layer.succeed(ScheduleAuthorizer)({
  manage: () => Effect.void,
  prepare: () => Effect.succeed({ policyId: "node-test-policy", decisionId: "allowed" }),
});

const limits = {
  ...defaultSchedulingLimits,
  retryBaseMillis: 100,
  retryMaxMillis: 1_000,
  recoveryPollMillis: 1_000,
};

const schedulingLayer = (filename: string, runtimeFailpoint?: DurableRuntimeFailpointHandler) =>
  NodeScheduling.layer({ limits }).pipe(
    Layer.provideMerge(
      NodeDurableHost.layerStack({
        filename,
        deploymentId: "node-scheduling-deployment",
        producerId: "node-scheduling-producer",
        wakeScanInterval: 1_000,
        ...(runtimeFailpoint === undefined ? {} : { runtimeFailpoint }),
      }),
    ),
    Layer.provide(authorizerLayer),
  );

const schedulingPorts = (host: NodeDurableHost["Service"], store: ScheduleStore["Service"]) =>
  Layer.mergeAll(
    Layer.succeed(NodeDurableHost)(host),
    Layer.succeed(ScheduleStore)(store),
    authorizerLayer,
  );

const withTemporaryDatabase = <A, E>(
  use: (filename: string) => Effect.Effect<A, E>,
): Effect.Effect<A, E | PlatformError.PlatformError> =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({
        prefix: "effect-agent-node-scheduling-",
      });
      return yield* use(`${directory}/runtime.sqlite`);
    }),
  ).pipe(Effect.provide(NodeFileSystem.layer));

const waitForSnapshot = (
  scheduling: Scheduling["Service"],
  predicate: (snapshot: ScheduleSnapshot) => boolean,
  id: ScheduleId = scheduleId,
) =>
  Effect.gen(function* () {
    for (let attempt = 0; attempt < 256; attempt += 1) {
      const snapshot = yield* scheduling.get(scheduleScope, id);
      if (predicate(snapshot)) return snapshot;
      yield* Effect.yieldNow;
    }
    return yield* Effect.die("Timed out waiting for the Node scheduling driver");
  });

it.effect("recovers one pending admission after a lost reply and host reopen", () =>
  withTemporaryDatabase((filename) =>
    Effect.scoped(
      Effect.gen(function* () {
        let loseFirstReply = true;
        const firstScope = yield* Scope.make();
        yield* Effect.addFinalizer(() => Scope.close(firstScope, Exit.void));
        const firstContext = yield* Layer.build(
          schedulingLayer(filename, (location) => {
            if (location !== "submit:after-admit" || !loseFirstReply) return Effect.void;
            loseFirstReply = false;
            return Effect.fail(DurableRuntimeFailpointError.make({ location }));
          }),
        ).pipe(Scope.provide(firstScope));
        const firstScheduling = Context.get(firstContext, Scheduling);
        const firstHost = Context.get(firstContext, NodeDurableHost);

        yield* firstScheduling.create(
          agent,
          { question: "recover me" },
          {
            scope: scheduleScope,
            scheduleId,
            timing: { _tag: "After", delayMillis: 100 },
            destination: { _tag: "FreshThread" },
            deliveryPrincipal: principal,
            definitions,
          },
        );
        yield* TestClock.adjust(100);
        const pending = yield* waitForSnapshot(
          firstScheduling,
          (snapshot) => snapshot.pending?.retry.attempts === 1,
        );
        expect(pending.lastReceipt).toBeNull();
        expect(pending.pending?.retry.lastFailure).toBe("ambiguous");
        const envelope = (yield* Context.get(firstContext, ScheduleStore).get({
          owner: scheduleScope.owner,
          scheduleId,
        }))?.pending?.envelope;
        expect(envelope).toBeDefined();

        yield* Scope.close(firstScope, Exit.void);
        expect(yield* firstHost.admissionOpen).toBe(false);

        const secondScope = yield* Scope.make();
        yield* Effect.addFinalizer(() => Scope.close(secondScope, Exit.void));
        const secondContext = yield* Layer.build(schedulingLayer(filename)).pipe(
          Scope.provide(secondScope),
        );
        const secondScheduling = Context.get(secondContext, Scheduling);
        const ledger = Context.get(secondContext, SubmissionLedger);

        yield* TestClock.adjust(100);
        const completed = yield* waitForSnapshot(
          secondScheduling,
          (snapshot) => snapshot.pending === null && snapshot.lastReceipt !== null,
        );
        expect(completed.lastReceipt?.occurrenceId).toBe(envelope?.occurrenceId);
        expect(completed.lastReceipt?.receipt.threadId).toBe(envelope?.threadId);

        if (envelope === undefined) return yield* Effect.die("Expected a pending envelope");
        const admitted = yield* ledger.lookup(
          SubmissionLookupByKey.make({
            threadId: envelope.threadId,
            principal: envelope.deliveryPrincipal,
            idempotencyKey: envelope.admissionKey,
          }),
        );
        expect(Option.isSome(admitted)).toBe(true);
        if (Option.isSome(admitted)) {
          expect(admitted.value.submissionId).toBe(completed.lastReceipt?.receipt.submissionId);
        }

        yield* Scope.close(secondScope, Exit.void);
      }),
    ),
  ),
);

it.effect("wakes for an earlier deadline and repairs a lost insert hint by indexed polling", () =>
  withTemporaryDatabase((filename) =>
    Effect.scoped(
      Effect.gen(function* () {
        const insertCount = yield* Ref.make(0);
        const failpoint = {
          hit: (point: string) =>
            point !== "schedule:insert:after"
              ? Effect.void
              : Ref.updateAndGet(insertCount, (count) => count + 1).pipe(
                  Effect.flatMap((count) =>
                    count === 2 ? Effect.fail(ScheduleFailpointError.make({ point })) : Effect.void,
                  ),
                ),
        };
        const baseScope = yield* Scope.make();
        yield* Effect.addFinalizer(() => Scope.close(baseScope, Exit.void));
        const baseContext = yield* Layer.build(
          NodeDurableHost.layerStack({
            filename,
            deploymentId: "node-scheduling-hints",
            producerId: "node-scheduling-hints-producer",
            wakeScanInterval: 1_000,
          }),
        ).pipe(Scope.provide(baseScope), Effect.provideService(ScheduleFailpoint, failpoint));
        const host = Context.get(baseContext, NodeDurableHost);
        const baseStore = Context.get(baseContext, ScheduleStore);
        const firstIdle = yield* Deferred.make<void>();
        const secondIdle = yield* Deferred.make<void>();
        const deadlineQueries = yield* Ref.make(0);
        const observedStore = ScheduleStore.of({
          insert: baseStore.insert,
          get: baseStore.get,
          list: baseStore.list,
          change: baseStore.change,
          due: baseStore.due,
          nextDeadline: (owner) =>
            Ref.updateAndGet(deadlineQueries, (count) => count + 1).pipe(
              Effect.tap((count) =>
                count === 1
                  ? Deferred.succeed(firstIdle, undefined)
                  : count === 2
                    ? Deferred.succeed(secondIdle, undefined)
                    : Effect.void,
              ),
              Effect.andThen(baseStore.nextDeadline(owner)),
            ),
        });
        const schedulingScope = yield* Scope.make();
        yield* Effect.addFinalizer(() => Scope.close(schedulingScope, Exit.void));
        const schedulingContext = yield* Layer.build(
          NodeScheduling.layer({ limits }).pipe(
            Layer.provide(schedulingPorts(host, observedStore)),
          ),
        ).pipe(Scope.provide(schedulingScope));
        const scheduling = Context.get(schedulingContext, Scheduling);
        yield* Deferred.await(firstIdle);

        yield* scheduling.create(
          agent,
          { question: "wake promptly" },
          {
            scope: scheduleScope,
            scheduleId: wakeScheduleId,
            timing: { _tag: "After", delayMillis: 100 },
            destination: { _tag: "FreshThread" },
            deliveryPrincipal: principal,
            definitions,
          },
        );
        yield* TestClock.adjust(100);
        yield* waitForSnapshot(
          scheduling,
          (snapshot) => snapshot.scheduleId === wakeScheduleId && snapshot.lastReceipt !== null,
          wakeScheduleId,
        );
        yield* Deferred.await(secondIdle);

        const lost = yield* scheduling
          .create(
            agent,
            { question: "recover the lost hint" },
            {
              scope: scheduleScope,
              scheduleId: lostHintScheduleId,
              timing: { _tag: "After", delayMillis: 100 },
              destination: { _tag: "FreshThread" },
              deliveryPrincipal: principal,
              definitions,
            },
          )
          .pipe(Effect.flip);
        expect(lost._tag).toBe("ScheduleFailpointError");
        yield* TestClock.adjust(100);
        const stillWaiting = yield* baseStore.get({
          owner: scheduleScope.owner,
          scheduleId: lostHintScheduleId,
        });
        expect(stillWaiting?.lastReceipt).toBeNull();
        yield* TestClock.adjust(900);
        yield* waitForSnapshot(
          scheduling,
          (snapshot) => snapshot.scheduleId === lostHintScheduleId && snapshot.lastReceipt !== null,
          lostHintScheduleId,
        );
      }),
    ),
  ),
);

it.effect("stops the scheduling driver when its Scope closes", () =>
  withTemporaryDatabase((filename) =>
    Effect.scoped(
      Effect.gen(function* () {
        const baseContext = yield* Layer.build(
          NodeDurableHost.layerStack({
            filename,
            deploymentId: "node-scheduling-finalizer",
            producerId: "node-scheduling-finalizer-producer",
            wakeScanInterval: 1_000,
          }),
        );
        const host = Context.get(baseContext, NodeDurableHost);
        const store = Context.get(baseContext, ScheduleStore);
        const schedulingScope = yield* Scope.make();
        const schedulingContext = yield* Layer.build(
          NodeScheduling.layer({ limits }).pipe(Layer.provide(schedulingPorts(host, store))),
        ).pipe(Scope.provide(schedulingScope));
        const scheduling = Context.get(schedulingContext, Scheduling);
        yield* scheduling.create(
          agent,
          { question: "do not admit after close" },
          {
            scope: scheduleScope,
            scheduleId: stoppedScheduleId,
            timing: { _tag: "After", delayMillis: 100 },
            destination: { _tag: "FreshThread" },
            deliveryPrincipal: principal,
            definitions,
          },
        );
        yield* Scope.close(schedulingScope, Exit.void);
        yield* TestClock.adjust(1_000);
        const record = yield* store.get({
          owner: scheduleScope.owner,
          scheduleId: stoppedScheduleId,
        });
        expect(record?.pending).toBeNull();
        expect(record?.lastReceipt).toBeNull();
        expect(record?.nextAtMillis).toBe(100);
      }),
    ),
  ),
);

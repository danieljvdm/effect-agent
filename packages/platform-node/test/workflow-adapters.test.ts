import {
  NodeWorkflowRepairTrigger,
  SqlWorkflowDispatchStore,
} from "@effect-agent/platform-node/NodeWorkflow";
import { Receipt } from "@effect-agent/thread/DurableAgentRuntime";
import {
  WorkflowDispatchIntent,
  WorkflowDispatchScan,
  WorkflowDispatchStore,
  WorkflowRepairTrigger,
} from "@effect-agent/workflow/WorkflowDispatch";
import { NodeFileSystem } from "@effect/platform-node";
import { SqliteClient } from "@effect/sql-sqlite-node";
import { expect, it } from "@effect/vitest";
import { Deferred, Effect, Exit, Fiber, Layer, Ref, Schema } from "effect";
import { TestClock } from "effect/testing";
import { SqlClient } from "effect/unstable/sql";

import { deploymentId, temporaryDirectory, workflowName } from "./workflow-fixtures.ts";

const intent = (executionId: string) =>
  Schema.decodeSync(WorkflowDispatchIntent)({
    version: 1,
    deploymentId,
    workflowName,
    executionId,
    receipt: Schema.decodeSync(Receipt)({
      receiptId: `receipt-${executionId}`,
      submissionId: `submission-${executionId}`,
      threadId: `thread-${executionId}`,
      queueSequence: 1,
    }),
  });

const scan = (after?: string) =>
  new WorkflowDispatchScan({
    deploymentId,
    workflowName,
    limit: 1,
    ...(after === undefined ? {} : { after }),
  });

it.live(
  "persists immutable intents across reopen and rejects conflicting, corrupt, and incompatible rows",
  () =>
    Effect.gen(function* () {
      const directory = yield* temporaryDirectory;

      const live = SqlWorkflowDispatchStore.layer.pipe(
        Layer.provideMerge(
          SqliteClient.layer({
            filename: `${directory}/dispatch.sqlite`,
          }),
        ),
      );

      const a = intent("a");
      const b = intent("b");
      const foreign = new WorkflowDispatchIntent({ ...intent("c"), workflowName: "other/v1" });

      yield* Effect.gen(function* () {
        const store = yield* WorkflowDispatchStore;

        yield* Effect.all([store.put(b), store.put(a), store.put(a), store.put(foreign)], {
          concurrency: 4,
        });
      }).pipe(Effect.provide(live));

      yield* Effect.gen(function* () {
        const store = yield* WorkflowDispatchStore;
        const sql = yield* SqlClient.SqlClient;

        expect(yield* store.scan(scan())).toEqual([a]);
        expect(yield* store.scan(scan("a"))).toEqual([b]);
        expect(yield* store.scan(scan("b"))).toEqual([]);
        const divergent = new WorkflowDispatchIntent({ ...a, receipt: b.receipt });

        expect((yield* store.put(divergent).pipe(Effect.result))._tag).toBe("Failure");
        expect((yield* store.remove(divergent).pipe(Effect.result))._tag).toBe("Failure");
        expect(yield* store.scan(scan())).toEqual([a]);

        const encoded = Schema.encodeSync(Schema.fromJsonString(WorkflowDispatchIntent))(a);

        for (const corrupted of [
          encoded.replace('"version":1', '"version":2'),
          encoded.replace('"version":1', '"version":1,"unexpected":true'),
          encoded.replace('"executionId":"a"', '"executionId":"wrong"'),
        ]) {
          yield* sql`UPDATE effect_agent_workflow_dispatch SET intent_json = ${corrupted} WHERE execution_id = 'a'`;
          expect((yield* store.scan(scan()).pipe(Effect.result))._tag).toBe("Failure");
          expect((yield* store.remove(a).pipe(Effect.result))._tag).toBe("Failure");
        }
        yield* sql`UPDATE effect_agent_workflow_dispatch SET intent_json = ${encoded} WHERE execution_id = 'a'`;
        yield* store.remove(a);
        yield* store.remove(a);
        expect(yield* store.scan(scan())).toEqual([b]);
      }).pipe(Effect.provide(live));
    }).pipe(Effect.scoped, Effect.provide(NodeFileSystem.layer)),
);

it.effect(
  "runs repair at startup, survives a defect, and interrupts its active callback on scope closure",
  () =>
    Effect.gen(function* () {
      const calls = yield* Ref.make(0);
      const finalized = yield* Ref.make(false);
      const third = yield* Deferred.make<void>();
      const registered = yield* Deferred.make<void>();

      const callback = Effect.gen(function* () {
        const call = yield* Ref.updateAndGet(calls, (n) => n + 1);

        if (call === 2) return yield* Effect.die("transient defect");
        if (call === 3) {
          yield* Deferred.succeed(third, undefined);

          return yield* Effect.never.pipe(Effect.ensuring(Ref.set(finalized, true)));
        }
      });

      const fiber = yield* Effect.gen(function* () {
        const trigger = yield* WorkflowRepairTrigger;

        yield* trigger.register(callback);
        yield* Deferred.succeed(registered, undefined);

        return yield* Effect.never;
      }).pipe(
        Effect.scoped,
        Effect.provide(NodeWorkflowRepairTrigger.layer({ interval: "1 second" })),
        Effect.forkChild,
      );

      yield* Deferred.await(registered);
      expect(yield* Ref.get(calls)).toBe(1);
      yield* TestClock.adjust("2 seconds");
      yield* Deferred.await(third);
      yield* Fiber.interrupt(fiber);
      expect(yield* Ref.get(finalized)).toBe(true);
      yield* TestClock.adjust("10 seconds");
      expect(yield* Ref.get(calls)).toBe(3);
    }),
);

it.effect.each([0, -1, Infinity, NaN])("rejects invalid repair interval %s", (interval) =>
  Effect.gen(function* () {
    const exit = yield* WorkflowRepairTrigger.pipe(
      Effect.provide(NodeWorkflowRepairTrigger.layer({ interval })),
      Effect.exit,
    );

    expect(Exit.isFailure(exit)).toBe(true);
  }),
);

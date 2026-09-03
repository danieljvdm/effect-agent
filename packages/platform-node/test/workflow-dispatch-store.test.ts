import { SqlWorkflowDispatchStore } from "@effect-agent/platform-node/NodeWorkflow";
import {
  WorkflowDispatchIntent,
  WorkflowDispatchScan,
  WorkflowDispatchStore,
} from "@effect-agent/workflow/WorkflowDispatch";
import { SqliteClient } from "@effect/sql-sqlite-node";
import { expect, it } from "@effect/vitest";
import { Effect, Layer, Schema, String } from "effect";
import { SqlClient } from "effect/unstable/sql";
import { DurableDeferred } from "effect/unstable/workflow";

const intent = Schema.decodeSync(WorkflowDispatchIntent)({
  version: 1,
  deploymentId: "transformed-client",
  workflowName: "agent/v1",
  executionId: "execution-a",
  receipt: {
    receiptId: "receipt-a",
    submissionId: "submission-a",
    threadId: "thread-a",
    queueSequence: 1,
  },
});

it.live("preserves immutable dispatch identities with application SQL result transforms", () =>
  Effect.gen(function* () {
    const store = yield* WorkflowDispatchStore;
    const sql = yield* SqlClient.SqlClient;

    const scan = new WorkflowDispatchScan({
      deploymentId: intent.deploymentId,
      workflowName: intent.workflowName,
      limit: 1,
    });

    yield* store.put(intent);
    yield* store.put(intent);
    expect(yield* store.scan(scan)).toEqual([intent]);
    expect(yield* sql`SELECT 1 AS application_value`).toEqual([{ applicationValue: 1 }]);

    const divergent = yield* Schema.decodeUnknownEffect(WorkflowDispatchIntent)({
      ...intent,
      receipt: { ...intent.receipt, queueSequence: 2 },
    });

    expect(yield* store.put(divergent).pipe(Effect.result)).toMatchObject({
      _tag: "Failure",
      failure: { _tag: "WorkflowDispatchError", operation: "put" },
    });
    expect(yield* store.remove(divergent).pipe(Effect.result)).toMatchObject({
      _tag: "Failure",
      failure: { _tag: "WorkflowDispatchError", operation: "remove" },
    });
    expect(yield* store.scan(scan)).toEqual([intent]);

    const token = new DurableDeferred.TokenParsed({
      workflowName: "Parent",
      executionId: "parent-1",
      deferredName: "agent",
    }).asToken;

    const waiting = new WorkflowDispatchIntent({ ...intent, completionToken: token });

    expect(yield* store.put(waiting)).toEqual(waiting);
    // A repair discovery without a token must retain the waiter. A stale cleanup must
    // never erase a token that was attached after its completion read.
    expect(yield* store.put(intent)).toEqual(waiting);
    expect(yield* store.remove(intent).pipe(Effect.result)).toMatchObject({ _tag: "Failure" });
    expect(yield* store.scan(scan)).toEqual([waiting]);
    expect(
      yield* store
        .put(
          new WorkflowDispatchIntent({
            ...intent,
            completionToken: new DurableDeferred.TokenParsed({
              workflowName: "Other",
              executionId: "parent-1",
              deferredName: "agent",
            }).asToken,
          }),
        )
        .pipe(Effect.result),
    ).toMatchObject({ _tag: "Failure" });
    yield* store.remove(waiting);
    yield* store.put(intent);

    const encode = Schema.encodeEffect(Schema.fromJsonString(WorkflowDispatchIntent));

    const corrupted = yield* encode(
      new WorkflowDispatchIntent({ ...intent, executionId: "different-execution" }),
    );

    yield* sql`UPDATE effect_agent_workflow_dispatch SET intent_json = ${corrupted}`;
    expect(yield* store.scan(scan).pipe(Effect.result)).toMatchObject({
      _tag: "Failure",
      failure: { _tag: "WorkflowDispatchError", operation: "decode" },
    });
    expect(yield* store.remove(intent).pipe(Effect.result)).toMatchObject({
      _tag: "Failure",
      failure: { _tag: "WorkflowDispatchError", operation: "decode" },
    });

    yield* sql`UPDATE effect_agent_workflow_dispatch SET intent_json = ${yield* encode(intent)}`;
    yield* store.remove(intent);
    yield* store.remove(intent);
    expect(yield* store.scan(scan)).toEqual([]);
  }).pipe(
    Effect.provide(
      SqlWorkflowDispatchStore.layer.pipe(
        Layer.provideMerge(
          SqliteClient.layer({
            filename: ":memory:",
            transformResultNames: String.snakeToCamel,
          }),
        ),
      ),
    ),
  ),
);

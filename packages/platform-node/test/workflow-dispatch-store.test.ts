import { SqlWorkflowDispatchStore } from "@effect-agent/platform-node/node-workflow";
import {
  WorkflowDispatchIntent,
  WorkflowDispatchScan,
  WorkflowDispatchStore,
} from "@effect-agent/workflow/workflow-dispatch";
import { SqliteClient } from "@effect/sql-sqlite-node";
import { expect, it } from "@effect/vitest";
import { Effect, Layer, Schema } from "effect";
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

it.live("retains a later completion token against stale repair and cleanup", () =>
  Effect.gen(function* () {
    const store = yield* WorkflowDispatchStore;

    const scan = new WorkflowDispatchScan({
      deploymentId: intent.deploymentId,
      workflowName: intent.workflowName,
      limit: 1,
    });

    yield* store.put(intent);

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
    expect(yield* store.scan(scan)).toEqual([]);
  }).pipe(
    Effect.provide(
      SqlWorkflowDispatchStore.layer.pipe(
        Layer.provideMerge(
          SqliteClient.layer({
            filename: ":memory:",
          }),
        ),
      ),
    ),
  ),
);

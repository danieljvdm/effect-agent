import * as Agent from "@effect-agent/core/Agent";
import { ContextCompactor } from "@effect-agent/engine/ContextCompactor";
import { RunContextPreparation, RunToolAuthorization } from "@effect-agent/engine/RunOptions";
import { DurableAgentRuntime } from "@effect-agent/thread/DurableAgentRuntime";
import {
  DurableRuntimeFailpoint,
  DurableRuntimeFailpointError,
} from "@effect-agent/thread/DurableFailpoint";
import {
  BatchId,
  CanonicalBatch,
  DeploymentId,
  ProducerId,
  RecordEnvelope,
  RecordId,
  RepairAnnotated,
} from "@effect-agent/thread/Records";
import {
  IdempotencyKey,
  Principal,
  RecoverySnapshotRequest,
  SubmissionLedger,
} from "@effect-agent/thread/SubmissionLedger";
import {
  FencedAppendRequest,
  LoadCheckpointRequest,
  ThreadExportRequest,
  ThreadStore,
  ThreadTailRequest,
  type ThreadRead,
} from "@effect-agent/thread/ThreadStore";
import { ToolReconciler } from "@effect-agent/thread/ToolReconciler";
import { NodeCrypto, NodeFileSystem } from "@effect/platform-node";
import { SqliteClient } from "@effect/sql-sqlite-node";
import { expect, it } from "@effect/vitest";
import {
  Array as EffectArray,
  Cause,
  DateTime,
  Effect,
  Exit,
  FileSystem,
  Layer,
  Option,
  Schema,
  Stream,
} from "effect";
import {
  LanguageModel,
  Model,
  Tool,
  Toolkit,
  type Prompt,
  type Response,
} from "effect/unstable/ai";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import type { Statement } from "effect/unstable/sql/Statement";

import { observeQueries } from "../../../test/fixtures/ledger-read-contracts.ts";
import {
  historyDefinition,
  historyDefinitions,
  historyResponse,
  measureHistory,
  seedHistory,
} from "../../../test/fixtures/runtime-history-cost.ts";
import { NodeDurableAgentRuntime } from "../src/NodeDurableAgentRuntime.ts";

type CompiledQuery = ReturnType<Statement<unknown>["compile"]>;

const host = (filename: string) =>
  NodeDurableAgentRuntime.layer({
    filename,
    deploymentId: "history-cost",
    producerId: "history-cost",
  });

// Rebuild the coordinator over the Node assembly's observed ports. The SQLite connection,
// durability settings, scheduler, ownership drain, and runtime configuration are unchanged.
const coordinatorServices = Layer.mergeAll(
  DurableRuntimeFailpoint.layer,
  ToolReconciler.uncertain,
  RunToolAuthorization.allowAll,
  NodeCrypto.layer,
);

const handoff = "Continue the checkpoint work-budget request.";

const checkpointContext = Layer.succeed(RunContextPreparation, {
  hook: {
    prepare: (request) =>
      Effect.succeed({
        prompt: request.source,
        ...(request.turn === 2 && !JSON.stringify(request.source).includes(handoff)
          ? { rollover: { handoff, through: request.source.content.length } }
          : {}),
      }),
  },
});

const measureCheckpoint = Effect.fn("WorkBudget.checkpoint")(function* (
  historySize: number,
  suffixSize: number,
) {
  const fs = yield* FileSystem.FileSystem;
  const directory = yield* fs.makeTempDirectoryScoped({ prefix: "checkpoint-work-budget-" });
  const filename = `${directory}/thread.sqlite`;
  const prompts: Array<Prompt.Prompt> = [];
  let modelFinalizers = 0;
  let toolCalls = 0;
  let toolFinalizers = 0;

  const tools = Toolkit.make(
    Tool.make("recorded_work", { parameters: Tool.EmptyParams, success: Schema.String }),
  );

  const handlers = tools.toLayer({
    recorded_work: () =>
      Effect.acquireUseRelease(
        Effect.sync(() => {
          toolCalls++;
        }),
        () => Effect.succeed("recorded"),
        () =>
          Effect.sync(() => {
            toolFinalizers++;
          }),
      ),
  });

  const model = Model.make(
    "scripted",
    "checkpoint-work-budget",
    Layer.effect(
      LanguageModel.LanguageModel,
      LanguageModel.make({
        generateText: () => Effect.die("Unexpected non-streaming model call"),
        streamText: (request) => {
          prompts.push(request.prompt);

          const parts: ReadonlyArray<Response.StreamPartEncoded> =
            prompts.length === 1
              ? [
                  { type: "tool-call", id: "recorded-call", name: "recorded_work", params: {} },
                  {
                    type: "finish",
                    reason: "tool-calls",
                    usage: { inputTokens: {}, outputTokens: {} },
                  },
                ]
              : historyResponse;

          return Stream.fromIterable(parts).pipe(
            Stream.ensuring(
              Effect.sync(() => {
                modelFinalizers++;
              }),
            ),
          );
        },
      }),
    ),
  );

  const agent = Agent.withModel(
    Agent.make(historyDefinition.id, {
      input: Schema.String,
      output: Schema.String,
      instructions: "Keep the checkpoint work-budget instructions.",
      toolkit: tools,
      policy: { maxTurns: 2, maxToolCalls: 1, maxDuration: "30 seconds", toolConcurrency: 1 },
    }),
    model,
  );

  const checkpointHost = (crash: boolean) =>
    NodeDurableAgentRuntime.layer({
      filename,
      deploymentId: "history-cost",
      producerId: "history-cost",
      runContext: checkpointContext,
      runtimeFailpoint: (location) =>
        crash && location === "checkpoint:after-save"
          ? DurableRuntimeFailpointError.make({ location })
          : Effect.void,
    }).pipe(Layer.provide(ContextCompactor.layerRollover));

  const prepared = yield* Effect.gen(function* () {
    const seed = yield* seedHistory(historySize);
    const runtime = yield* DurableAgentRuntime;
    const store = yield* ThreadStore;
    const ledger = yield* SubmissionLedger;

    const receipt = yield* runtime.submit(agent, "checkpoint work", {
      threadId: seed.threadId,
      principal: Principal.make("work-budget"),
      idempotencyKey: IdempotencyKey.make("work-budget"),
      definitions: historyDefinitions,
    });

    const stopped = yield* runtime.processThread(agent, seed.threadId).pipe(Effect.exit);

    expect(Exit.isFailure(stopped)).toBe(true);
    if (Exit.isFailure(stopped))
      expect(Cause.findErrorOption(stopped.cause)).toMatchObject({
        _tag: "Some",
        value: { _tag: "DurableRuntimeFailpointError", location: "checkpoint:after-save" },
      });
    expect(prompts).toHaveLength(1);
    expect(modelFinalizers).toBe(1);
    expect(toolCalls).toBe(1);
    expect(toolFinalizers).toBe(1);
    expect(
      (yield* ledger.loadRecoverySnapshot(
        RecoverySnapshotRequest.make({ submissionId: receipt.submissionId }),
      )).ownership,
    ).toBeUndefined();

    const checkpoint = yield* store.recoveryCheckpoints!.load(
      LoadCheckpointRequest.make({ threadId: seed.threadId }),
    );

    if (Option.isNone(checkpoint))
      return yield* Effect.die("The real runtime did not save a recovery checkpoint");

    for (let start = 0; start < suffixSize; start += 256) {
      const tail = yield* store.inspectTail(ThreadTailRequest.make({ threadId: seed.threadId }));

      yield* store.append(
        FencedAppendRequest.make({
          threadId: seed.threadId,
          producerEpoch: tail.producerEpoch,
          expectedTailSequence: tail.tailSequence,
          expectedTailDigest: tail.tailDigest,
          batch: CanonicalBatch.make({
            batchId: BatchId.make(`suffix-${start}`),
            producerId: ProducerId.make("history-cost"),
            records: EffectArray.makeBy(Math.min(256, suffixSize - start), (offset) =>
              RecordEnvelope.make({
                recordId: RecordId.make(`suffix-${start + offset}`),
                family: "thread",
                schemaVersion: 1,
                deploymentId: DeploymentId.make("history-cost"),
                createdAt: DateTime.makeUnsafe(0),
                payload: RepairAnnotated.make({
                  reason: "work-budget suffix",
                  details: { position: start + offset },
                }),
              }),
            ),
          }),
        }),
      );
    }
    const before = yield* store.export(ThreadExportRequest.make({ threadId: seed.threadId }));

    expect(before.tailSequence - checkpoint.value.throughSequence).toBe(suffixSize);

    return { receipt, checkpoint: checkpoint.value, before };
  }).pipe(Effect.provide(Layer.merge(checkpointHost(true), handlers)));

  let returnedRecords = 0;
  let retiredRecords = 0;
  let closedPages = 0;
  let checkpointLoads = 0;
  let checkpointHits = 0;
  const requests: Array<ThreadRead> = [];

  const { result, queries } = yield* observeQueries(
    Effect.gen(function* () {
      const store = yield* ThreadStore;
      const cache = store.recoveryCheckpoints!;

      const counted = ThreadStore.of({
        ...store,
        recoveryCheckpoints: {
          ...cache,
          load: (request) =>
            cache.load(request).pipe(
              Effect.tap((value) =>
                Effect.sync(() => {
                  checkpointLoads++;
                  if (Option.isSome(value)) checkpointHits++;
                }),
              ),
            ),
        },
        read: (request) =>
          Stream.suspend(() => {
            requests.push(request);

            return store.read(request);
          }).pipe(
            Stream.tap((record) =>
              Effect.sync(() => {
                returnedRecords++;
                if (record.sequence <= prepared.checkpoint.throughSequence) retiredRecords++;
              }),
            ),
            Stream.ensuring(
              Effect.sync(() => {
                closedPages++;
              }),
            ),
          ),
      });

      const runtime = yield* DurableAgentRuntime.pipe(
        Effect.provide(
          Layer.fresh(DurableAgentRuntime.layer).pipe(
            Layer.provide(
              Layer.mergeAll(
                coordinatorServices,
                checkpointContext,
                ContextCompactor.layerRollover,
              ),
            ),
          ),
        ),
        Effect.provideService(ThreadStore, counted),
      );

      const recovery = yield* runtime.runRecovery;

      expect(recovery.map(({ decision }) => decision._tag)).toEqual(["ResumeFromTurnBoundary"]);
      expect(prompts).toHaveLength(1);
      const settlements = yield* runtime.processThread(agent, prepared.receipt.threadId);
      const settled = yield* runtime.awaitSettlement(prepared.receipt);

      expect(settlements).toEqual([settled]);
      expect(settled.outcome).toBe("completed");
      const ledger = yield* SubmissionLedger;

      expect(
        (yield* ledger.loadRecoverySnapshot(
          RecoverySnapshotRequest.make({ submissionId: prepared.receipt.submissionId }),
        )).ownership,
      ).toBeUndefined();

      return settled;
    }).pipe(Effect.provide(Layer.merge(checkpointHost(false), handlers))),
  );

  expect(prompts).toHaveLength(2);
  expect(modelFinalizers).toBe(2);
  expect(toolCalls).toBe(1);
  expect(toolFinalizers).toBe(1);
  expect(result.usageSummary?.modelCalls).toBe(2);
  expect(JSON.stringify(prompts[1])).toContain(handoff);
  expect(JSON.stringify(prompts[1])).toContain("checkpoint work-budget instructions");
  expect(JSON.stringify(prompts[1])).toContain("checkpoint work");
  expect(JSON.stringify(prompts[1])).not.toContain("retained input");
  expect(retiredRecords).toBe(0);
  expect(checkpointHits).toBeGreaterThan(0);
  expect(checkpointHits).toBe(checkpointLoads);
  expect(closedPages).toBe(requests.length);

  const { batchDigestQueries } = yield* assertCanonicalReadPlans(queries).pipe(
    Effect.provide(SqliteClient.layer({ filename, readonly: true })),
  );

  // Each public operation may load one checkpoint and replay one active suffix. Completed
  // tools and the retired prefix must not be read/replayed again for this eligible checkpoint.
  expect(checkpointLoads).toBeLessThanOrEqual(2);
  // SQLite currently seeks these digests by Thread, then filters its batch metadata.
  // That is up to two linear batch searches, not constant-time SQLite/VM work.
  expect(batchDigestQueries).toBeLessThanOrEqual(2);
  expect(returnedRecords).toBeGreaterThanOrEqual(suffixSize);
  expect(returnedRecords, "Checkpoint canonical-record work budget").toBeLessThanOrEqual(
    2 * suffixSize,
  );
  expect(requests.length).toBeLessThanOrEqual(2 * Math.ceil(suffixSize / 1_024));

  const after = yield* Effect.gen(function* () {
    const store = yield* ThreadStore;

    return yield* store.export(ThreadExportRequest.make({ threadId: prepared.receipt.threadId }));
  }).pipe(Effect.provide(checkpointHost(false)));

  expect(after.records.slice(0, prepared.before.records.length)).toEqual(prepared.before.records);
  expect(
    after.records.flatMap(({ record }) =>
      record.payload._tag === "RunCompleted" ? [record.payload.output] : [],
    ),
  ).toEqual(["done"]);
  expect(
    after.records.filter(({ record }) => record.payload._tag === "SubmissionSettled"),
  ).toHaveLength(1);

  return { statements: queries.length };
}, Effect.provide(NodeFileSystem.layer));

// Retired history and active suffix grow independently. The baseline uses the same
// public operation, with one retired record and no suffix; no machine timing is involved.
it.effect.each([
  { historySize: 1_025, suffixSize: 0 },
  { historySize: 8_193, suffixSize: 0 },
  { historySize: 8_193, suffixSize: 1_025 },
])(
  "bounds checkpoint recovery after reopening SQLite with $historySize retired and $suffixSize suffix records",
  ({ historySize, suffixSize }) =>
    Effect.gen(function* () {
      const baseline = yield* measureCheckpoint(1, 0);
      const measured = yield* measureCheckpoint(historySize, suffixSize);

      // One recovery snapshot and one Attempt snapshot; each page has a Thread existence
      // read and a canonical range read. Retired history must add no statements or pages.
      expect(measured.statements).toBeLessThanOrEqual(
        baseline.statements + 4 * Math.ceil(suffixSize / 1_024),
      );
    }),
  30_000,
);

const assertCanonicalReadPlans = Effect.fn("WorkBudget.assertCanonicalReadPlans")(function* (
  queries: ReadonlyArray<CompiledQuery>,
) {
  const sql = yield* SqlClient.SqlClient;

  const reads = queries.filter(
    ([query]) =>
      /^\s*SELECT\b/i.test(query) && /\bFROM effect_agent_canonical_records\b/.test(query),
  );

  for (const [query, parameters] of reads) {
    const plan = yield* sql.unsafe<{ detail: string }>(`EXPLAIN QUERY PLAN ${query}`, parameters);
    const identityProbe = /\brecord_id IN\b/.test(query);

    // Identity probes are bounded by the append batch; history pages must seek by sequence.
    // Checking SEARCH alone is insufficient: a thread_id-only search scans that Thread.
    const accesses = plan.filter(({ detail }) => /\b(?:SCAN|SEARCH)\b/.test(detail));

    expect(accesses.length).toBeGreaterThan(0);
    for (const { detail } of accesses)
      expect(detail, "Canonical read must use its identity/range index").toMatch(
        identityProbe
          ? /SEARCH .*\(thread_id=\? AND record_id=\?\)/
          : /SEARCH .*\(thread_id=\? AND sequence>\?\)/,
      );
    if (!identityProbe) {
      expect(plan.some(({ detail }) => detail.includes("TEMP B-TREE"))).toBe(false);
      expect(query).toMatch(/\bLIMIT \?\s*$/);
      expect(parameters.at(-1)).toBeGreaterThan(0);
      expect(parameters.at(-1)).toBeLessThanOrEqual(1_024);
    }
  }
  let batchDigestQueries = 0;

  for (const [query, parameters] of queries.filter(
    ([query]) =>
      /^\s*SELECT\b/i.test(query) && /\bFROM effect_agent_canonical_batches\b/.test(query),
  )) {
    const digestProbe = /\bAND last_sequence\s*=/.test(query);

    if (digestProbe) batchDigestQueries++;
    const plan = yield* sql.unsafe<{ detail: string }>(`EXPLAIN QUERY PLAN ${query}`, parameters);
    const accesses = plan.filter(({ detail }) => /\b(?:SCAN|SEARCH)\b/.test(detail));

    expect(accesses.length).toBeGreaterThan(0);
    for (const { detail } of accesses)
      expect(detail, "Batch reads must stay within their Thread or exact batch identity").toMatch(
        digestProbe
          ? /SEARCH .*\(thread_id=\?(?: AND last_sequence=\?)?\)/
          : /SEARCH .*\(thread_id=\? AND batch_id=\?\)/,
      );
  }

  return { canonicalQueries: reads.length, batchDigestQueries };
});

const measureFresh = Effect.fn("WorkBudget.fresh")(function* (historySize: number) {
  const fs = yield* FileSystem.FileSystem;
  const directory = yield* fs.makeTempDirectoryScoped({ prefix: "runtime-work-budget-" });
  const filename = `${directory}/thread.sqlite`;
  const seed = yield* seedHistory(historySize).pipe(Effect.provide(host(filename)));

  const { result, queries } = yield* observeQueries(
    measureHistory(seed).pipe(Effect.provide(Layer.merge(host(filename), coordinatorServices))),
  );

  expect(Exit.isSuccess(result.exit)).toBe(true);
  if (Exit.isSuccess(result.exit))
    expect(result.exit.value.map(({ outcome }) => outcome)).toEqual(["completed"]);
  expect(result.measured).toBe(true);
  expect(result.modelCalls).toBe(1);
  expect(result.modelFinalizers).toBe(1);
  expect(result.openedPages).toBe(result.closedPages);
  expect(result.snapshot.ownership).toBeUndefined();
  expect(result.returnedRecords).toBeGreaterThanOrEqual(historySize);
  // The control/journal prefix and prompt fold each visit history once. The fixed six
  // permits new input/Run control records; it does not grow with retained history.
  expect(result.returnedRecords).toBeLessThanOrEqual(2 * historySize + 6);

  const { canonicalQueries } = yield* assertCanonicalReadPlans(queries).pipe(
    Effect.provide(SqliteClient.layer({ filename, readonly: true })),
  );

  expect(
    result.totalReturnedRecords,
    "Fresh submission canonical-record work budget",
  ).toBeLessThanOrEqual(2 * historySize + 6);

  return { statements: queries.length, canonicalQueries };
}, Effect.provide(NodeFileSystem.layer));

// Cross the 1,024-record page boundary, then grow the prefix eightfold. Each comparison
// owns its baseline and files, so test ordering and parallel workers cannot affect it.
it.effect.each([1_025, 8_193])(
  "bounds fresh submission work after reopening SQLite with %i retained records",
  (historySize) =>
    Effect.gen(function* () {
      const baseline = yield* measureFresh(1);
      const measured = yield* measureFresh(historySize);
      const extraPages = Math.ceil(historySize / 1_024) - 1;

      // Two history passes, each with one Thread existence query and one range query per
      // page. Fixed admission/settlement work is compared, not snapshotted as an exact count.
      expect(measured.statements).toBeLessThanOrEqual(baseline.statements + 4 * extraPages);
      expect(measured.canonicalQueries).toBeLessThanOrEqual(
        baseline.canonicalQueries + 2 * extraPages,
      );
    }),
  30_000,
);

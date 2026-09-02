import {
  MemoryAccess,
  SemanticIndexLimits,
  SemanticQueryLimits,
  indexMemorySource,
  querySemanticMemory,
  Memory,
} from "@effect-agent/capabilities";
import {
  MemoryKey,
  MemoryReader,
  MemoryRecallLimits,
  ThreadId,
  MemoryWrite,
  MemoryWriter,
  SemanticMemoryProfile,
  type ActiveMemoryDocument,
  type MemoryDocument,
  type MemoryLookup,
} from "@effect-agent/core";
import {
  InMemorySemanticIndexCapacity,
  inMemorySemanticIndexLayer,
} from "@effect-agent/storage-memory";
import {
  activityProcessorStoreLayer,
  layer as sqliteThreadStoreLayer,
  memoryStoreLayer,
} from "@effect-agent/storage-sqlite";
import {
  ActivityPassLimits,
  ActivityProcessorKey,
  BatchId,
  CanonicalBatch,
  DeploymentId,
  FencedAppendRequest,
  ProducerEpoch,
  ProducerId,
  RecordEnvelope,
  RecordId,
  ThreadMaterialization,
  ThreadStore,
  ThreadTailRequest,
  UserInputRecorded,
  processCommittedActivity,
  type CanonicalRecordEnvelope,
  type PreparedActivity,
} from "@effect-agent/thread";
import { NodeCrypto } from "@effect/platform-node";
import { SqliteClient } from "@effect/sql-sqlite-node";
import {
  Clock,
  Crypto,
  DateTime,
  Effect,
  Encoding,
  FileSystem,
  Layer,
  Option,
  Path,
  Schema,
} from "effect";
import { AiError, EmbeddingModel } from "effect/unstable/ai";

import {
  CORPUS_SHA256,
  CorpusDocument,
  EvaluationCorpus,
  EvaluationError,
  EvaluationReport,
  MEMORY_NAMESPACE,
  MEMORY_SCOPE,
  MODEL_ID,
  MODEL_REVISION,
  type CorpusQuery,
  type LatencySummary as LatencySummaryType,
  type QueryMeasurement,
} from "./contracts.ts";
import { makeNativeEmbeddingLayer } from "./native-embedding.ts";

export interface EvaluationOptions {
  readonly cachePath: string;
  readonly offline: boolean;
  readonly environment: string;
  readonly gitRevision: string;
}

const PROFILE = SemanticMemoryProfile.make({
  version: 1,
  provider: "transformers.js@4.2.0/onnxruntime@1.24.3/cpu/fp32/mean/l2",
  model: MODEL_ID,
  modelRevision: MODEL_REVISION,
  dimensions: 384,
  chunker: "utf8-codepoint@1",
  maxChunkBytes: 1024,
  distance: "cosine",
});

const INDEX_CAPACITY = InMemorySemanticIndexCapacity.make({ maxSources: 20, maxChunks: 160 });
const ACCESS = MemoryAccess.make({ namespace: MEMORY_NAMESPACE, scope: MEMORY_SCOPE });

const INDEX_LIMITS = SemanticIndexLimits.make({
  maxSourceBytes: 4096,
  maxChunks: 8,
  timeoutMillis: 60_000,
});

const QUERY_LIMITS = SemanticQueryLimits.make({
  maxQueryBytes: 4096,
  maxCandidates: 3,
  maxScannedChunks: 256,
  minScore: 0.35,
  timeoutMillis: 60_000,
});

const RECALL_LIMITS = MemoryRecallLimits.make({
  maxSources: 3,
  maxItems: 3,
  maxBytes: 4096,
  maxTokens: 4096,
  timeoutMillis: 10_000,
});

const ACTIVITY_LIMITS = ActivityPassLimits.make({
  maxRecords: 1,
  pageSize: 1,
  timeoutMillis: 5_000,
  leaseMillis: 6_500,
});

const THREAD_ID = Schema.decodeSync(ThreadId)("semantic-memory-evaluation-source");

const PROCESSOR_KEY = ActivityProcessorKey.make({
  processorId: "semantic-memory-evaluation-extractor",
  processorVersion: "frozen-corpus-v1",
  threadId: THREAD_ID,
});

const ActivityOutput = Schema.Struct({ document: CorpusDocument, recordId: Schema.NonEmptyString });
const byteLength = (text: string): number => Encoding.encodeHex(text).length / 2;

const elapsedMillis = (started: bigint, finished: bigint): number =>
  Number(finished - started) / 1_000_000;

const locator = (id: string): string => `memory://evaluation/${id}`;
const key = (id: string) => MemoryKey.make({ namespace: MEMORY_NAMESPACE, id });

const evalError = (operation: string, cause: unknown) =>
  EvaluationError.make({
    operation,
    message: cause instanceof Error ? cause.message : String(cause),
  });

export const summarizeLatencies = (
  samples: ReadonlyArray<number>,
  targetMillis: number | null,
): LatencySummaryType => {
  const sorted = [...samples].sort((left, right) => left - right);

  const percentile = (fraction: number) =>
    sorted.length === 0 ? null : (sorted[Math.ceil(sorted.length * fraction) - 1] ?? null);

  const maximum = sorted.at(-1) ?? null;

  return {
    n: sorted.length,
    samplesMillis: [...samples],
    p50Millis: percentile(0.5),
    p95Millis: percentile(0.95),
    maxMillis: maximum,
    targetMillis,
    withinTarget: targetMillis === null || maximum === null ? null : maximum <= targetMillis,
  };
};

const passage = (document: ActiveMemoryDocument) => ({
  version: 1 as const,
  source: document.source,
  passageId: "document",
  content: document.content,
});

const lookupFromIds = Effect.fn("SemanticMemoryEvaluation.lookupFromIds")(function* (
  ids: ReadonlyArray<string>,
) {
  const reader = yield* MemoryReader;
  const passages = [] as Array<ReturnType<typeof passage>>;

  for (const id of ids) {
    const document = yield* reader.get(key(id));

    if (document?._tag === "ActiveMemoryDocument" && document.scopes.includes(MEMORY_SCOPE)) {
      passages.push(passage(document));
    }
  }

  return passages.length === 0
    ? ({ _tag: "NoMatch" } as const)
    : ({ _tag: "Found", passages } as const);
});

const recall = (id: string, lookup: MemoryLookup) =>
  Memory.recall([{ id, essential: false, read: Effect.succeed(lookup) }], RECALL_LIMITS);

const tokenize = (text: string): ReadonlySet<string> =>
  new Set(text.toLocaleLowerCase("en-US").match(/[\p{L}\p{N}]+/gu) ?? []);

const lexicalIds = (query: string, documents: ReadonlyArray<CorpusDocument>): Array<string> => {
  const terms = tokenize(query);

  return documents
    .filter((document) => document.state === "active")
    .map((document) => ({
      id: document.id,
      score: [...tokenize(document.text)].filter((term) => terms.has(term)).length,
    }))
    .filter(({ score }) => score > 0)
    .sort((left, right) => right.score - left.score || left.id.localeCompare(right.id))
    .slice(0, 3)
    .map(({ id }) => id);
};

const appendDocument = Effect.fn("SemanticMemoryEvaluation.appendDocument")(function* (
  document: CorpusDocument,
  ordinal: number,
) {
  const store = yield* ThreadStore;
  const tail = yield* store.inspectTail(ThreadTailRequest.make({ threadId: THREAD_ID }));

  const recordId = yield* Schema.decodeUnknownEffect(RecordId)(
    `eval-record-${ordinal}-${document.id}`,
  );

  const deploymentId = yield* Schema.decodeUnknownEffect(DeploymentId)(
    "semantic-memory-evaluation",
  );

  const producerEpoch = yield* Schema.decodeUnknownEffect(ProducerEpoch)(1);

  const batchId = yield* Schema.decodeUnknownEffect(BatchId)(
    `eval-batch-${ordinal}-${document.id}`,
  );

  const producerId = yield* Schema.decodeUnknownEffect(ProducerId)("semantic-memory-evaluation");

  const record = RecordEnvelope.make({
    recordId,
    family: "thread",
    schemaVersion: 1,
    createdAt: DateTime.makeUnsafe(1_700_000_000_000 + ordinal),
    deploymentId,
    payload: UserInputRecorded.make({ kind: "user", input: document }),
  });

  return yield* store.append(
    FencedAppendRequest.make({
      threadId: THREAD_ID,
      producerEpoch,
      expectedTailSequence: tail.tailSequence,
      expectedTailDigest: tail.tailDigest,
      batch: CanonicalBatch.make({
        batchId,
        producerId,
        records: [record],
      }),
    }),
  );
});

const applyActivity = Effect.fn("SemanticMemoryEvaluation.applyActivity")(function* (
  work: PreparedActivity,
) {
  const output = yield* Schema.decodeUnknownEffect(ActivityOutput)(work.output);
  const document = output.document;
  const writer = yield* MemoryWriter;

  yield* writer.change(
    yield* Schema.decodeUnknownEffect(MemoryWrite.Wire)({
      _tag: "Put",
      key: key(document.id),
      operationId: work.workId,
      expectedRevision: null,
      locator: locator(document.id),
      content: {
        text: document.previousText ?? document.text,
        attributions: [
          {
            originId: document.originId,
            speaker: document.speaker,
            observers: document.observers,
            locator: locator(document.id),
            activityAt: 1_700_000_000_000 - document.activityDaysAgo * 86_400_000,
            interpretation: document.interpretation,
          },
        ],
        metadata: {
          sourceThreadId: THREAD_ID,
          sourceRecordId: output.recordId,
          sourceRecordDigest: work.recordDigest,
          sourceSequence: work.sequence,
        },
        recordedAt: 1_700_000_000_000,
        extractedAt: 1_700_000_000_500,
      },
      scopes: [MEMORY_SCOPE],
    }),
  );
});

const processOne = (extractionCalls: { value: number }) =>
  processCommittedActivity({
    key: PROCESSOR_KEY,
    owner: "semantic-memory-evaluation-worker",
    limits: ACTIVITY_LIMITS,
    extract: (record: CanonicalRecordEnvelope) => {
      extractionCalls.value += 1;

      return record.record.payload._tag === "UserInputRecorded"
        ? Schema.decodeUnknownEffect(CorpusDocument)(record.record.payload.input).pipe(
            Effect.flatMap((document) =>
              Schema.encodeEffect(ActivityOutput)({
                document,
                recordId: record.record.recordId,
              }),
            ),
          )
        : Effect.succeed(null);
    },
    apply: applyActivity,
  });

const currentDocuments = Effect.fn("SemanticMemoryEvaluation.currentDocuments")(function* (
  documents: ReadonlyArray<CorpusDocument>,
) {
  const reader = yield* MemoryReader;
  const result = new Map<string, MemoryDocument | null>();

  for (const document of documents) result.set(document.id, yield* reader.get(key(document.id)));

  return result;
});

const measureQuery = Effect.fn("SemanticMemoryEvaluation.measureQuery")(function* (
  query: CorpusQuery,
  method: QueryMeasurement["method"],
  corpusDocuments: ReadonlyArray<CorpusDocument>,
) {
  const started = yield* Clock.monotonicTimeNanos;
  let lookup: MemoryLookup;
  let staleExcluded = 0;
  let nativeInputTokens: number | null = null;

  if (method === "direct-locator") {
    const exact = corpusDocuments.find((document) => locator(document.id) === query.text);

    lookup = yield* lookupFromIds(exact === undefined ? [] : [exact.id]);
  } else if (method === "lexical-overlap") {
    lookup = yield* lookupFromIds(lexicalIds(query.text, corpusDocuments));
  } else {
    const semantic = yield* querySemanticMemory(query.text, ACCESS, QUERY_LIMITS);

    lookup = semantic.lookup;
    staleExcluded = semantic.staleExcluded;
    nativeInputTokens = semantic.inputTokens;
  }
  const recalled = yield* recall(`${method}:${query.id}`, lookup);
  const finished = yield* Clock.monotonicTimeNanos;
  const selectedIds = [...new Set(recalled.passages.map((item) => item.source.id))];
  const usefulMisses = query.usefulIds.filter((id) => !selectedIds.includes(id));

  const forbiddenActiveContextMatches = query.forbiddenIds.filter(
    (id) =>
      selectedIds.includes(id) &&
      corpusDocuments.find((document) => document.id === id)?.state === "active",
  );

  const withdrawnStaleMatches = selectedIds.filter(
    (id) => corpusDocuments.find((document) => document.id === id)?.state === "withdrawn",
  );

  const activeIrrelevantMatches = selectedIds.filter(
    (id) =>
      !query.usefulIds.includes(id) &&
      !query.forbiddenIds.includes(id) &&
      corpusDocuments.find((document) => document.id === id)?.state === "active",
  );

  const usefulOrigins = new Set(
    query.usefulIds.flatMap((id) => {
      const found = corpusDocuments.find((document) => document.id === id);

      return found === undefined ? [] : [found.originId];
    }),
  );

  const usefulOriginHits = new Set(
    selectedIds.flatMap((id) => {
      if (!query.usefulIds.includes(id)) return [];
      const found = corpusDocuments.find((document) => document.id === id);

      return found === undefined ? [] : [found.originId];
    }),
  ).size;

  const current = yield* currentDocuments(corpusDocuments);

  const attributionCorrect = recalled.passages.every((item) => {
    const fixture = corpusDocuments.find((document) => document.id === item.source.id);

    const expectedActivityAt =
      fixture === undefined ? null : 1_700_000_000_000 - fixture.activityDaysAgo * 86_400_000;

    return (
      fixture !== undefined &&
      item.content.attributions.length === 1 &&
      item.content.attributions[0]?.originId === fixture.originId &&
      item.content.attributions[0].speaker === fixture.speaker &&
      item.content.attributions[0].locator === locator(fixture.id) &&
      item.content.attributions[0].activityAt === expectedActivityAt &&
      item.content.attributions[0].interpretation === fixture.interpretation &&
      item.content.attributions[0].observers.length === fixture.observers.length &&
      item.content.attributions[0].observers.every(
        (observer, index) => observer === fixture.observers[index],
      )
    );
  });

  const currentRevisionCorrect = recalled.passages.every(
    (item) => current.get(item.source.id)?.source.revision === item.source.revision,
  );

  return {
    queryId: query.id,
    category: query.category,
    cohort:
      query.category === "direct-locator" ? "direct-locator-queries" : "natural-language-queries",
    method,
    selectedIds,
    usefulMisses,
    forbiddenActiveContextMatches,
    activeIrrelevantMatches,
    withdrawnStaleMatches,
    usefulOriginHits,
    usefulOrigins: usefulOrigins.size,
    attributionCorrect,
    currentRevisionCorrect,
    staleExcluded,
    contextBytes: recalled.bytes,
    conservativeContextTokens: recalled.estimatedTokens,
    queryBytes: byteLength(query.text),
    nativeInputTokens,
    elapsedMillis: elapsedMillis(started, finished),
  } satisfies QueryMeasurement;
});

export const summarizeMethods = (measurements: ReadonlyArray<QueryMeasurement>) =>
  (["direct-locator", "lexical-overlap", "semantic"] as const).flatMap((method) =>
    (["direct-locator-queries", "natural-language-queries"] as const).map((cohort) => {
      const rows = measurements.filter((row) => row.method === method && row.cohort === cohort);
      const usefulOriginHits = rows.reduce((total, row) => total + row.usefulOriginHits, 0);
      const usefulOrigins = rows.reduce((total, row) => total + row.usefulOrigins, 0);
      const negative = rows.filter((row) => row.usefulOrigins === 0);

      return {
        method,
        cohort,
        queryCount: rows.length,
        usefulOriginHits,
        usefulOrigins,
        usefulOriginRecall: usefulOrigins === 0 ? 0 : usefulOriginHits / usefulOrigins,
        usefulMisses: rows.reduce((total, row) => total + row.usefulMisses.length, 0),
        forbiddenActiveContextMatches: rows.reduce(
          (total, row) => total + row.forbiddenActiveContextMatches.length,
          0,
        ),
        activeIrrelevantMatches: rows.reduce(
          (total, row) => total + row.activeIrrelevantMatches.length,
          0,
        ),
        withdrawnStaleMatches: rows.reduce(
          (total, row) => total + row.withdrawnStaleMatches.length,
          0,
        ),
        attributionErrors: rows.filter((row) => !row.attributionCorrect).length,
        revisionErrors: rows.filter((row) => !row.currentRevisionCorrect).length,
        negativeQueries: negative.length,
        negativeFalsePositives: negative.filter((row) => row.selectedIds.length > 0).length,
        latency: summarizeLatencies(
          rows.map((row) => row.elapsedMillis),
          method === "semantic" ? 250 : null,
        ),
      };
    }),
  );

const injectedAiError = (message: string) =>
  AiError.make({
    module: "SemanticMemoryEvaluation",
    method: "embedMany",
    reason: AiError.UnknownError.make({ description: message }),
  });

const providerCohort = Effect.fn("SemanticMemoryEvaluation.providerCohort")(function* (
  name: string,
  base: EmbeddingModel.Service,
  provider: { readonly delayMillis?: number; readonly fail?: boolean },
  timeoutMillis: number | null,
) {
  let nativeCallStarted = false;

  const wrapper = yield* EmbeddingModel.make({
    embedMany: ({ inputs }) =>
      Effect.gen(function* () {
        if ((provider.delayMillis ?? 0) > 0) yield* Effect.sleep(provider.delayMillis!);
        if (provider.fail === true) return yield* injectedAiError("injected failure");
        nativeCallStarted = true;
        const response = yield* base.embedMany(inputs);

        return {
          results: response.embeddings.map(({ vector }) => [...vector]),
          usage: { inputTokens: response.usage.inputTokens },
        };
      }),
  });

  const started = yield* Clock.monotonicTimeNanos;

  const probe = querySemanticMemory(
    "When does Aurora reach production?",
    ACCESS,
    QUERY_LIMITS,
  ).pipe(
    Effect.flatMap((result) => recall(`provider-cohort:${name}`, result.lookup)),
    Effect.provideService(EmbeddingModel.EmbeddingModel, wrapper),
  );

  const observed = yield* timeoutMillis === null
    ? probe.pipe(Effect.match({ onFailure: String, onSuccess: () => "success" }))
    : probe.pipe(
        Effect.timeoutOption(timeoutMillis),
        Effect.match({
          onFailure: String,
          onSuccess: (value) =>
            Option.isNone(value) ? "cooperative-timeout-before-native-call" : "success",
        }),
      );

  const elapsed = elapsedMillis(started, yield* Clock.monotonicTimeNanos);

  return {
    report: {
      name,
      expected:
        provider.fail === true
          ? "typed AiError"
          : timeoutMillis === null
            ? "success after injected delay"
            : "cooperative timeout",
      observed,
      elapsedMillis: elapsed,
      nativeCallStarted,
    },
  };
});

const coldSample = Effect.fn("SemanticMemoryEvaluation.coldSample")(function* (
  options: EvaluationOptions,
) {
  const native = yield* makeNativeEmbeddingLayer({ cachePath: options.cachePath, offline: true });
  const started = yield* Clock.monotonicTimeNanos;

  const elapsed = yield* Effect.scoped(
    Effect.gen(function* () {
      const result = yield* querySemanticMemory(
        "When does Aurora reach production?",
        ACCESS,
        QUERY_LIMITS,
      );

      yield* recall("cold-semantic", result.lookup);

      return elapsedMillis(started, yield* Clock.monotonicTimeNanos);
    }).pipe(Effect.provide(native.layer)),
  );

  const snapshot = yield* native.snapshot;

  return { elapsed, calls: snapshot.calls, textBytes: snapshot.textBytes };
});

const loadCorpus = Effect.fn("SemanticMemoryEvaluation.loadCorpus")(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const modulePath = yield* path.fromFileUrl(new URL(import.meta.url));
  const fixture = path.resolve(path.dirname(modulePath), "../fixtures/semantic-memory-corpus.json");
  const raw = yield* fs.readFileString(fixture);
  const crypto = yield* Crypto.Crypto;

  const digest = yield* crypto
    .digest("SHA-256", new TextEncoder().encode(raw))
    .pipe(Effect.map(Encoding.encodeHex));

  if (digest !== CORPUS_SHA256) return yield* evalError("verify corpus", `SHA-256 ${digest}`);

  return yield* Schema.decodeUnknownEffect(Schema.fromJsonString(EvaluationCorpus))(raw).pipe(
    Effect.mapError((cause) => evalError("decode corpus", cause)),
  );
});

/** Run the fixed, label-frozen semantic memory evaluation against local native inference. */
export const runEvaluation = Effect.fn("runSemanticMemoryEvaluation")(function* (
  options: EvaluationOptions,
) {
  const corpus = yield* loadCorpus();
  const fs = yield* FileSystem.FileSystem;
  const directory = yield* fs.makeTempDirectoryScoped({ prefix: "effect-agent-semantic-eval-" });
  const database = `${directory}/evaluation.sqlite`;
  const sql = SqliteClient.layer({ filename: database, busyTimeout: 5_000 });

  const storage = Layer.mergeAll(
    sqliteThreadStoreLayer({ filename: database, busyTimeout: 5_000 }),
    activityProcessorStoreLayer.pipe(Layer.provide(sql)),
    memoryStoreLayer.pipe(Layer.provide(sql)),
    NodeCrypto.layer,
  );

  const native = yield* makeNativeEmbeddingLayer({
    cachePath: options.cachePath,
    offline: options.offline,
  });

  const semanticIndex = inMemorySemanticIndexLayer(PROFILE, INDEX_CAPACITY);
  const baseServices = Layer.merge(storage, semanticIndex);
  const extractionCalls = { value: 0 };

  const directRecallSamples: Array<{
    sourceId: string;
    outcome: "recalled" | "miss";
    elapsedMillis: number;
  }> = [];

  const semanticRecallSamples: Array<{
    sourceId: string;
    outcome: "recalled" | "miss";
    elapsedMillis: number;
  }> = [];

  let sourceEmbeddingCalls = 0;
  let initialEmbeddedBytes = 0;
  let semanticLagQueryCalls = 0;
  let extractionMillis = 0;
  let indexingMillis = 0;

  const main = Effect.gen(function* () {
    const threads = yield* ThreadStore;

    yield* threads.materialize(
      ThreadMaterialization.make({
        threadId: THREAD_ID,
        producerEpoch: yield* Schema.decodeUnknownEffect(ProducerEpoch)(1),
      }),
    );
    // Download/model initialization is complete before commit-lag measurements begin.
    yield* Effect.flatMap(EmbeddingModel.EmbeddingModel, (model) => model.embedMany(["warmup"]));
    for (let ordinal = 0; ordinal < corpus.documents.length; ordinal++) {
      const document = corpus.documents[ordinal];

      if (document === undefined) continue;
      yield* appendDocument(document, ordinal);
      const committedAt = yield* Clock.monotonicTimeNanos;
      const extractionStarted = yield* Clock.monotonicTimeNanos;

      yield* processOne(extractionCalls);
      extractionMillis += elapsedMillis(extractionStarted, yield* Clock.monotonicTimeNanos);
      const directLookup = yield* lookupFromIds([document.id]);
      const direct = yield* recall(`commit-direct:${document.id}`, directLookup);

      directRecallSamples.push({
        sourceId: document.id,
        outcome: direct.passages.some((item) => item.source.id === document.id)
          ? "recalled"
          : "miss",
        elapsedMillis: elapsedMillis(committedAt, yield* Clock.monotonicTimeNanos),
      });
      const indexStarted = yield* Clock.monotonicTimeNanos;
      const indexed = yield* indexMemorySource(key(document.id), INDEX_LIMITS);

      indexingMillis += elapsedMillis(indexStarted, yield* Clock.monotonicTimeNanos);
      if (indexed.embeddedChunks > 0) sourceEmbeddingCalls += 1;
      initialEmbeddedBytes += indexed.embeddedBytes;

      const semantic = yield* querySemanticMemory(
        document.previousText ?? document.text,
        ACCESS,
        QUERY_LIMITS,
      );

      semanticLagQueryCalls += 1;
      const recalled = yield* recall(`commit-semantic:${document.id}`, semantic.lookup);

      semanticRecallSamples.push({
        sourceId: document.id,
        outcome: recalled.passages.some((item) => item.source.id === document.id)
          ? "recalled"
          : "miss",
        elapsedMillis: elapsedMillis(committedAt, yield* Clock.monotonicTimeNanos),
      });
    }

    const writer = yield* MemoryWriter;

    for (const document of corpus.documents) {
      if (document.previousText !== undefined) {
        yield* writer.change(
          yield* Schema.decodeUnknownEffect(MemoryWrite.Wire)({
            _tag: "Put",
            key: key(document.id),
            operationId: `fixture-correction-${document.id}`,
            expectedRevision: "1",
            locator: locator(document.id),
            content: {
              text: document.text,
              attributions: [
                {
                  originId: document.originId,
                  speaker: document.speaker,
                  observers: document.observers,
                  locator: locator(document.id),
                  activityAt: 1_700_000_000_000 - document.activityDaysAgo * 86_400_000,
                  interpretation: document.interpretation,
                },
              ],
              metadata: { lifecycle: "correction" },
              recordedAt: 1_700_000_000_000,
              extractedAt: 1_700_000_001_000,
            },
            scopes: [MEMORY_SCOPE],
          }),
        );
      } else if (document.state === "withdrawn") {
        yield* writer.change(
          yield* Schema.decodeUnknownEffect(MemoryWrite.Wire)({
            _tag: "Withdraw",
            key: key(document.id),
            operationId: `fixture-withdrawal-${document.id}`,
            expectedRevision: "1",
            reason: "withdrawn by frozen evaluation fixture",
          }),
        );
      }
    }

    let staleExcluded = 0;
    let staleSelected = 0;

    const changed = corpus.documents.filter(
      (document) => document.previousText !== undefined || document.state === "withdrawn",
    );

    for (const document of changed) {
      const probe = yield* querySemanticMemory(
        document.previousText ?? document.text,
        ACCESS,
        QUERY_LIMITS,
      );

      staleExcluded += probe.staleExcluded;
      const recalled = yield* recall(`stale-probe:${document.id}`, probe.lookup);

      staleSelected += recalled.passages.filter((item) => item.source.id === document.id).length;
    }

    const refreshStarted = yield* Clock.monotonicTimeNanos;
    let refreshEmbeddedBytes = 0;

    for (const document of corpus.documents) {
      const indexed = yield* indexMemorySource(key(document.id), INDEX_LIMITS);

      if (indexed.embeddedChunks > 0) sourceEmbeddingCalls += 1;
      refreshEmbeddedBytes += indexed.embeddedBytes;
    }
    const refreshMillis = elapsedMillis(refreshStarted, yield* Clock.monotonicTimeNanos);

    indexingMillis += refreshMillis;

    const measurements: Array<QueryMeasurement> = [];

    for (const query of corpus.queries) {
      measurements.push(yield* measureQuery(query, "direct-locator", corpus.documents));
      measurements.push(yield* measureQuery(query, "lexical-overlap", corpus.documents));
      measurements.push(yield* measureQuery(query, "semantic", corpus.documents));
    }

    return {
      staleExcluded,
      staleSelected,
      refreshEmbeddedBytes,
      measurements,
    };
  });

  const phases = yield* Effect.gen(function* () {
    const result = yield* Effect.scoped(main.pipe(Effect.provide(native.layer)));
    const mainTelemetry = yield* native.snapshot;
    const coldSamples: Array<number> = [];
    let coldCalls = 0;
    let coldTextBytes = 0;

    for (let index = 0; index < 3; index++) {
      const sample = yield* coldSample(options);

      coldSamples.push(sample.elapsed);
      coldCalls += sample.calls;
      coldTextBytes += sample.textBytes;
    }

    const cohortNative = yield* makeNativeEmbeddingLayer({
      cachePath: options.cachePath,
      offline: true,
    });

    const providerCohorts = yield* Effect.scoped(
      Effect.gen(function* () {
        const base = yield* EmbeddingModel.EmbeddingModel;

        yield* base.embedMany(["warm provider cohort"]);

        return [
          (yield* providerCohort("injected-slow-200ms", base, { delayMillis: 200 }, null)).report,
          (yield* providerCohort("typed-failure", base, { fail: true }, null)).report,
          (yield* providerCohort("cooperative-timeout-20ms", base, { delayMillis: 100 }, 20))
            .report,
        ];
      }).pipe(Effect.provide(cohortNative.layer)),
    );

    const cohortTelemetry = yield* cohortNative.snapshot;

    return {
      result,
      mainTelemetry,
      coldSamples,
      coldCalls,
      coldTextBytes,
      providerCohorts,
      cohortCalls: cohortTelemetry.calls,
      cohortTextBytes: cohortTelemetry.textBytes,
    };
  }).pipe(Effect.provide(baseServices));

  const {
    result,
    mainTelemetry,
    coldSamples,
    coldCalls,
    coldTextBytes,
    providerCohorts,
    cohortCalls,
    cohortTextBytes,
  } = phases;

  const semanticRows = result.measurements.filter((row) => row.method === "semantic");
  const generatedAt = DateTime.makeUnsafe(yield* Clock.currentTimeMillis);

  return yield* Schema.decodeUnknownEffect(Schema.toType(EvaluationReport))({
    version: 1,
    metadata: {
      generatedAt,
      gitRevision: options.gitRevision,
      environment: options.environment,
      corpusSha256: CORPUS_SHA256,
      corpusDocuments: corpus.documents.length,
      corpusQueries: corpus.queries.length,
      offline: options.offline,
    },
    provider: {
      library: "@huggingface/transformers@4.2.0",
      runtime: "onnxruntime-node@1.24.3",
      model: MODEL_ID,
      revision: MODEL_REVISION,
      dtype: "fp32",
      pooling: "mean",
      normalization: "l2",
      dimensions: 384,
      executionProvider: "cpu",
      intraOpThreads: 1,
      interOpThreads: 1,
      executionMode: "sequential",
      cashChargeUsd: 0,
      cpuEconomicCost: null,
      nativeInputTokens: null,
      nativeCancellation: "unsupported; cooperative deadlines can overshoot an active inference",
    },
    fixedConfiguration: {
      semanticTopK: 3,
      semanticMinScore: 0.35,
      maxItems: 3,
      maxBytes: 4096,
      maxTokens: 4096,
      tokenEstimate: "one token per UTF-8 byte",
    },
    ingestion: {
      canonicalRecords: corpus.documents.length,
      extractionCalls: extractionCalls.value,
      extractionModelCalls: 0,
      extractionMillis,
      sourceTextBytes: corpus.documents.reduce(
        (total, document) => total + byteLength(document.previousText ?? document.text),
        0,
      ),
      directRecallLag: summarizeLatencies(
        directRecallSamples.map((sample) => sample.elapsedMillis),
        60_000,
      ),
      semanticRecallLag: summarizeLatencies(
        semanticRecallSamples.map((sample) => sample.elapsedMillis),
        60_000,
      ),
      directRecallSamples,
      semanticRecallSamples,
    },
    staleIndexProbe: {
      correctedSources: corpus.documents.filter((document) => document.previousText !== undefined)
        .length,
      withdrawnSources: corpus.documents.filter((document) => document.state === "withdrawn")
        .length,
      staleExcluded: result.staleExcluded,
      staleSelected: result.staleSelected,
    },
    modelStartupMillis: mainTelemetry.modelStartupMillis,
    backgroundIndex: {
      calls: sourceEmbeddingCalls,
      embeddedBytes: initialEmbeddedBytes + result.refreshEmbeddedBytes,
      nativeInputTokens: null,
      elapsedMillis: indexingMillis,
    },
    modelCalls: {
      overallTotal: mainTelemetry.calls + coldCalls + cohortCalls,
      mainPhaseTotal: mainTelemetry.calls,
      coldPhaseTotal: coldCalls,
      providerCohortTotal: cohortCalls,
      overallInputTextBytes: mainTelemetry.textBytes + coldTextBytes + cohortTextBytes,
      mainPhaseInputTextBytes: mainTelemetry.textBytes,
      coldPhaseInputTextBytes: coldTextBytes,
      providerCohortInputTextBytes: cohortTextBytes,
      sourceEmbeddingCalls,
      queryEmbeddingCalls:
        semanticLagQueryCalls +
        corpus.documents.filter(
          (document) => document.previousText !== undefined || document.state === "withdrawn",
        ).length +
        corpus.queries.length,
    },
    queryTextBytes: corpus.queries.reduce((total, query) => total + byteLength(query.text), 0),
    measurements: result.measurements,
    summaries: summarizeMethods(result.measurements),
    warmSemanticLatency: summarizeLatencies(
      semanticRows.map((row) => row.elapsedMillis),
      250,
    ),
    cachedModelColdLatency: summarizeLatencies(coldSamples, 3_000),
    providerCohorts,
    limitations: [
      "The explicit-locator method loads only an exact document locator and returns NoMatch for natural-language queries.",
      "Labels and fixed rankings were frozen before embeddings; this run does not use an LLM answerer or judge.",
      "Native provider input token counts are unavailable. Context tokens use the conservative one-UTF-8-byte estimate.",
      "Local inference has zero API cash charge; CPU time and economic cost were not measured.",
      "Cached-model cold samples exclude the first model download and CLI module-import time.",
      "Injected delay and failure cohorts test control flow and are not production-latency estimates.",
      "Same-origin references count once as useful evidence, including assistant repetition.",
    ],
  }).pipe(Effect.mapError((cause) => evalError("validate report", cause)));
});

import { MemoryNamespace, MemoryScope } from "@effect-agent/core";
import { Schema } from "effect";

export const MODEL_ID = "onnx-community/all-MiniLM-L6-v2-ONNX";
export const MODEL_REVISION = "aff7a1dc4e8a1ea593e6ea21e95c22ef0a25966f";
export const CORPUS_SHA256 = "47bf97a564f088bc32b0abd823f2ebbd8146f2f9b65766db9bb3779ac77a7e8e";
export const MEMORY_NAMESPACE = MemoryNamespace.define({
  name: "example/semantic-memory-evaluation",
  version: 1,
  identity: Schema.Null,
}).make(null);
export const MEMORY_SCOPE = MemoryScope.make("evaluation-reader");

export const CorpusDocument = Schema.Struct({
  id: Schema.NonEmptyString,
  text: Schema.NonEmptyString,
  speaker: Schema.NonEmptyString,
  observers: Schema.Array(Schema.NonEmptyString),
  interpretation: Schema.NonEmptyString,
  activityDaysAgo: Schema.Natural,
  originId: Schema.NonEmptyString,
  state: Schema.Literals(["active", "withdrawn"]),
  previousText: Schema.optionalKey(Schema.NonEmptyString),
});
export type CorpusDocument = typeof CorpusDocument.Type;

export const CorpusQuery = Schema.Struct({
  id: Schema.NonEmptyString,
  text: Schema.NonEmptyString,
  category: Schema.NonEmptyString,
  usefulIds: Schema.Array(Schema.NonEmptyString),
  forbiddenIds: Schema.Array(Schema.NonEmptyString),
});
export type CorpusQuery = typeof CorpusQuery.Type;

export const EvaluationCorpus = Schema.Struct({
  version: Schema.Literal(1),
  kind: Schema.Literal("synthetic-labeled-semantic-memory-regression"),
  documents: Schema.Array(CorpusDocument),
  queries: Schema.Array(CorpusQuery),
});
export type EvaluationCorpus = typeof EvaluationCorpus.Type;

export const LatencySummary = Schema.Struct({
  n: Schema.Natural,
  samplesMillis: Schema.Array(Schema.Finite),
  p50Millis: Schema.NullOr(Schema.Finite),
  p95Millis: Schema.NullOr(Schema.Finite),
  maxMillis: Schema.NullOr(Schema.Finite),
  targetMillis: Schema.NullOr(Schema.Finite),
  withinTarget: Schema.NullOr(Schema.Boolean),
});
export type LatencySummary = typeof LatencySummary.Type;

export const QueryMeasurement = Schema.Struct({
  queryId: Schema.NonEmptyString,
  category: Schema.NonEmptyString,
  cohort: Schema.Literals(["direct-locator-queries", "natural-language-queries"]),
  method: Schema.Literals(["direct-locator", "lexical-overlap", "semantic"]),
  selectedIds: Schema.Array(Schema.NonEmptyString),
  usefulMisses: Schema.Array(Schema.NonEmptyString),
  forbiddenActiveContextMatches: Schema.Array(Schema.NonEmptyString),
  activeIrrelevantMatches: Schema.Array(Schema.NonEmptyString),
  withdrawnStaleMatches: Schema.Array(Schema.NonEmptyString),
  usefulOriginHits: Schema.Natural,
  usefulOrigins: Schema.Natural,
  attributionCorrect: Schema.Boolean,
  currentRevisionCorrect: Schema.Boolean,
  staleExcluded: Schema.Natural,
  contextBytes: Schema.Natural,
  conservativeContextTokens: Schema.Natural,
  queryBytes: Schema.Natural,
  nativeInputTokens: Schema.NullOr(Schema.Natural),
  elapsedMillis: Schema.Finite,
});
export type QueryMeasurement = typeof QueryMeasurement.Type;

export const MethodSummary = Schema.Struct({
  method: Schema.Literals(["direct-locator", "lexical-overlap", "semantic"]),
  cohort: Schema.Literals(["direct-locator-queries", "natural-language-queries"]),
  queryCount: Schema.Natural,
  usefulOriginHits: Schema.Natural,
  usefulOrigins: Schema.Natural,
  usefulOriginRecall: Schema.Finite,
  usefulMisses: Schema.Natural,
  forbiddenActiveContextMatches: Schema.Natural,
  activeIrrelevantMatches: Schema.Natural,
  withdrawnStaleMatches: Schema.Natural,
  attributionErrors: Schema.Natural,
  revisionErrors: Schema.Natural,
  negativeQueries: Schema.Natural,
  negativeFalsePositives: Schema.Natural,
  latency: LatencySummary,
});

export const ProviderCohort = Schema.Struct({
  name: Schema.NonEmptyString,
  expected: Schema.NonEmptyString,
  observed: Schema.NonEmptyString,
  elapsedMillis: Schema.Finite,
  nativeCallStarted: Schema.Boolean,
});

export const CommitLagSample = Schema.Struct({
  sourceId: Schema.NonEmptyString,
  outcome: Schema.Literals(["recalled", "miss"]),
  elapsedMillis: Schema.Finite,
});

export const EvaluationReport = Schema.Struct({
  version: Schema.Literal(1),
  metadata: Schema.Struct({
    generatedAt: Schema.DateTimeUtcFromString,
    gitRevision: Schema.NonEmptyString,
    environment: Schema.NonEmptyString,
    corpusSha256: Schema.Literal(CORPUS_SHA256),
    corpusDocuments: Schema.Natural,
    corpusQueries: Schema.Natural,
    offline: Schema.Boolean,
  }),
  provider: Schema.Struct({
    library: Schema.Literal("@huggingface/transformers@4.2.0"),
    runtime: Schema.Literal("onnxruntime-node@1.24.3"),
    model: Schema.Literal(MODEL_ID),
    revision: Schema.Literal(MODEL_REVISION),
    dtype: Schema.Literal("fp32"),
    pooling: Schema.Literal("mean"),
    normalization: Schema.Literal("l2"),
    dimensions: Schema.Literal(384),
    executionProvider: Schema.Literal("cpu"),
    intraOpThreads: Schema.Literal(1),
    interOpThreads: Schema.Literal(1),
    executionMode: Schema.Literal("sequential"),
    cashChargeUsd: Schema.Literal(0),
    cpuEconomicCost: Schema.Null,
    nativeInputTokens: Schema.Null,
    nativeCancellation: Schema.Literal(
      "unsupported; cooperative deadlines can overshoot an active inference",
    ),
  }),
  fixedConfiguration: Schema.Struct({
    semanticTopK: Schema.Literal(3),
    semanticMinScore: Schema.Literal(0.35),
    maxItems: Schema.Literal(3),
    maxBytes: Schema.Literal(4096),
    maxTokens: Schema.Literal(4096),
    tokenEstimate: Schema.Literal("one token per UTF-8 byte"),
  }),
  ingestion: Schema.Struct({
    canonicalRecords: Schema.Natural,
    extractionCalls: Schema.Natural,
    extractionModelCalls: Schema.Literal(0),
    extractionMillis: Schema.Finite,
    sourceTextBytes: Schema.Natural,
    directRecallLag: LatencySummary,
    semanticRecallLag: LatencySummary,
    directRecallSamples: Schema.Array(CommitLagSample),
    semanticRecallSamples: Schema.Array(CommitLagSample),
  }),
  staleIndexProbe: Schema.Struct({
    correctedSources: Schema.Natural,
    withdrawnSources: Schema.Natural,
    staleExcluded: Schema.Natural,
    staleSelected: Schema.Natural,
  }),
  modelStartupMillis: Schema.Finite,
  backgroundIndex: Schema.Struct({
    calls: Schema.Natural,
    embeddedBytes: Schema.Natural,
    nativeInputTokens: Schema.Null,
    elapsedMillis: Schema.Finite,
  }),
  modelCalls: Schema.Struct({
    overallTotal: Schema.Natural,
    mainPhaseTotal: Schema.Natural,
    coldPhaseTotal: Schema.Natural,
    providerCohortTotal: Schema.Natural,
    overallInputTextBytes: Schema.Natural,
    mainPhaseInputTextBytes: Schema.Natural,
    coldPhaseInputTextBytes: Schema.Natural,
    providerCohortInputTextBytes: Schema.Natural,
    sourceEmbeddingCalls: Schema.Natural,
    queryEmbeddingCalls: Schema.Natural,
  }),
  queryTextBytes: Schema.Natural,
  measurements: Schema.Array(QueryMeasurement),
  summaries: Schema.Array(MethodSummary),
  warmSemanticLatency: LatencySummary,
  cachedModelColdLatency: LatencySummary,
  providerCohorts: Schema.Array(ProviderCohort),
  limitations: Schema.Array(Schema.NonEmptyString),
});
export type EvaluationReport = typeof EvaluationReport.Type;

export class EvaluationError extends Schema.TaggedError<EvaluationError>()("EvaluationError", {
  operation: Schema.NonEmptyString,
  message: Schema.NonEmptyString,
}) {}

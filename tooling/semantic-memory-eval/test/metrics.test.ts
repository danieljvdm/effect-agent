import { describe, expect, it } from "@effect/vitest";
import { Schema } from "effect";

import { EvaluationCorpus, LatencySummary } from "../src/contracts.ts";
import { summarizeLatencies, summarizeMethods } from "../src/evaluate.ts";

const row = (overrides: {
  readonly queryId: string;
  readonly selectedIds: ReadonlyArray<string>;
  readonly usefulOriginHits: number;
  readonly usefulOrigins: number;
}) => ({
  queryId: overrides.queryId,
  category: "test",
  cohort: "natural-language-queries" as const,
  method: "semantic" as const,
  selectedIds: overrides.selectedIds,
  usefulMisses: [],
  forbiddenActiveContextMatches: [],
  activeIrrelevantMatches: [],
  withdrawnStaleMatches: [],
  usefulOriginHits: overrides.usefulOriginHits,
  usefulOrigins: overrides.usefulOrigins,
  attributionCorrect: true,
  currentRevisionCorrect: true,
  staleExcluded: 0,
  contextBytes: 100,
  conservativeContextTokens: 100,
  queryBytes: 20,
  nativeInputTokens: null,
  elapsedMillis: 2,
});

describe("semantic memory evaluation metrics", () => {
  it("uses nearest-rank percentiles and retains failed slow samples", () => {
    const summary = summarizeLatencies([4, 1, 100, 3, 2], 50);

    expect(summary).toEqual({
      n: 5,
      samplesMillis: [4, 1, 100, 3, 2],
      p50Millis: 3,
      p95Millis: 100,
      maxMillis: 100,
      targetMillis: 50,
      withinTarget: false,
    });
    expect(Schema.is(LatencySummary)(summary)).toBe(true);
  });

  it("rejects a corpus query without frozen usefulness labels", () => {
    const malformed = {
      version: 1,
      kind: "synthetic-labeled-semantic-memory-regression",
      documents: [],
      queries: [{ id: "q", text: "query", category: "negative", forbiddenIds: [] }],
    };

    expect(Schema.is(EvaluationCorpus)(malformed)).toBe(false);
  });

  it("counts unique-origin usefulness separately from negative false positives", () => {
    const summaries = summarizeMethods([
      row({
        queryId: "same-origin",
        selectedIds: ["original", "assistant-reference"],
        usefulOriginHits: 1,
        usefulOrigins: 1,
      }),
      row({
        queryId: "negative",
        selectedIds: ["irrelevant"],
        usefulOriginHits: 0,
        usefulOrigins: 0,
      }),
    ]);

    const semantic = summaries.find(
      (summary) => summary.method === "semantic" && summary.cohort === "natural-language-queries",
    );

    expect(semantic).toMatchObject({
      queryCount: 2,
      usefulOriginHits: 1,
      usefulOrigins: 1,
      usefulOriginRecall: 1,
      negativeQueries: 1,
      negativeFalsePositives: 1,
    });
  });
});

import {
  MemoryNamespace,
  MemoryScope,
  MemoryRecallLimits,
  MemoryWrite,
  MemoryPassage,
  type MemoryLookup,
} from "@effect-agent/core";
import { Schema } from "effect";

export const Projects = MemoryNamespace.define({
  name: "benchmark/projects",
  version: 1,
  identity: Schema.String,
});

export const BenchmarkCase = Schema.Literals(["1", "4", "8", "16", "duplicates"]);
export type BenchmarkCase = typeof BenchmarkCase.Type;
export const cases = BenchmarkCase.literals;
export const memoryScope = MemoryScope.make("benchmark");
export const sourceCount = (name: BenchmarkCase) => (name === "duplicates" ? 16 : Number(name));

export const limits = MemoryRecallLimits.make({
  maxSources: 16,
  maxItems: 128,
  maxBytes: 1_000_000,
  maxTokens: 1_000_000,
  maxInputBytes: 1_000_000,
  timeoutMillis: 5_000,
});

export const command = (name: BenchmarkCase, index: number) =>
  MemoryWrite.make({
    _tag: "Put",
    key: { namespace: Projects.make(name), id: `document-${index}` },
    operationId: `seed-${index}`,
    expectedRevision: null,
    locator: `synthetic://document-${index}`,
    scopes: [memoryScope],
    content: {
      text: `source-${index}: `.padEnd(1024, "x"),
      metadata: {},
      recordedAt: 0,
      attributions: [
        {
          originId: `original-${index}`,
          speaker: "synthetic",
          observers: ["ingestion"],
          locator: `synthetic://original-${index}`,
          activityAt: 0,
          interpretation: "synthetic benchmark data",
        },
      ],
    },
  });

export const candidates = (name: BenchmarkCase): MemoryLookup => ({
  _tag: "Found",
  passages: Array.from({ length: name === "duplicates" ? 128 : sourceCount(name) }, (_, i) => {
    const write = command(name, i % sourceCount(name));

    if (write._tag !== "Put") throw new Error("Expected seed Put");

    return MemoryPassage.make({
      version: 1,
      source: { id: write.key.id, locator: write.locator, revision: "1" },
      passageId: "excerpt",
      content: { ...write.content, text: write.content.text.slice(0, 64) },
    });
  }),
});

export const Sample = Schema.Struct({
  case: BenchmarkCase,
  sourceCount: Schema.Natural,
  candidateCount: Schema.Natural,
  corpusTextBytes: Schema.Natural,
  candidateBytes: Schema.Natural,
  validatedBytes: Schema.Natural,
  renderedBytes: Schema.Natural,
  validationRpcMillis: Schema.Finite,
  fullRecallMillis: Schema.Finite,
  status: Schema.Literals(["ok", "timeout", "error"]),
  errorTag: Schema.NullOr(Schema.String),
});

export type Sample = typeof Sample.Type;

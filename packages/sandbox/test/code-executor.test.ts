import { Duration, Schema } from "effect";
import { describe, expect, it } from "vite-plus/test";

import {
  CodeExecutionError,
  CodeExecutionLimits,
  CodeExecutionRequest,
  CodeExecutionResult,
  CodeHostCall,
  CodeHostCallResult,
  JsIdentifier,
  NetworkDisabled,
  SandboxImplementation,
} from "../src/index.ts";

const implementation = SandboxImplementation.make({
  isolation: "isolated",
  identity: "dynamic-worker",
});

const limits = CodeExecutionLimits.make({
  maxSourceBytes: 64 * 1024,
  maxWallTime: Duration.seconds(5),
  cpuMillis: 1_000,
  maxLogBytes: 32 * 1024,
  maxResultBytes: 64 * 1024,
  maxHostCalls: 16,
  maxHostCallArgumentBytes: 8 * 1024,
  maxHostCallResultBytes: 32 * 1024,
});

describe("CAP-015 CodeExecutor schemas", () => {
  it("round-trips a complete execution request", () => {
    const request = CodeExecutionRequest.make({
      language: "javascript",
      source: "async () => 1",
      namespaces: [{ name: "warehouse", methods: ["query"] }],
      network: NetworkDisabled.make({}),
      limits,
    });

    expect(
      Schema.decodeSync(CodeExecutionRequest)(Schema.encodeSync(CodeExecutionRequest)(request)),
    ).toEqual(request);
  });

  it("round-trips host calls, host outcomes, and pass results", () => {
    const call = CodeHostCall.make({
      namespace: "warehouse",
      method: "query",
      argument: { sql: "select 1", parameters: [1, true, null] },
    });
    expect(Schema.decodeSync(CodeHostCall)(Schema.encodeSync(CodeHostCall)(call))).toEqual(call);

    const outcomes: ReadonlyArray<unknown> = [
      { _tag: "CodeHostCallSuccess", value: { rows: [] } },
      { _tag: "CodeHostCallFailure", error: { _tag: "ToolInputError", message: "bad" } },
    ];
    for (const outcome of outcomes) {
      expect(() => Schema.decodeUnknownSync(CodeHostCallResult)(outcome)).not.toThrow();
    }

    const result = CodeExecutionResult.make({
      implementation,
      value: { answer: 42 },
      logs: ["one line"],
      resourceUse: {
        wallTime: Duration.millis(12),
        hostCalls: 2,
        logBytes: 8,
        resultBytes: 13,
      },
    });
    expect(
      Schema.decodeSync(CodeExecutionResult)(Schema.encodeSync(CodeExecutionResult)(result)),
    ).toEqual(result);
  });

  it("keeps every expected execution failure schema-decodable", () => {
    const failures: ReadonlyArray<unknown> = [
      { _tag: "CodeSourceError", implementation, reason: "oversized", message: "too big" },
      {
        _tag: "CodeExecutorUnsupportedError",
        implementation,
        feature: "network",
        message: "no allowlist",
      },
      { _tag: "CodeExecutorStartError", implementation, message: "loader unavailable" },
      {
        _tag: "CodeExecutionTimeoutError",
        implementation,
        kind: "wall-clock",
        maxWallTime: Schema.encodeSync(CodeExecutionLimits)(limits).maxWallTime,
        logs: [],
      },
      {
        _tag: "CodeOutputLimitError",
        implementation,
        surface: "logs",
        limit: 1_024,
        observed: 2_048,
        logs: ["partial"],
      },
      { _tag: "CodeHostCallLimitError", implementation, limit: 4, logs: [] },
      {
        _tag: "CodeProgramFailedError",
        implementation,
        reason: "threw",
        thrown: "Error: nope",
        message: "Error: nope",
        logs: [],
      },
      { _tag: "CodeExecutionProtocolError", implementation, message: "malformed envelope" },
      { _tag: "CodeExecutorTerminatedError", implementation, message: "isolate evicted" },
    ];
    for (const failure of failures) {
      expect(() => Schema.decodeUnknownSync(CodeExecutionError)(failure)).not.toThrow();
    }
  });

  it("rejects reserved words, invalid identifiers, and oversized collections at the boundary", () => {
    expect(() => Schema.decodeUnknownSync(JsIdentifier)("await")).toThrow();
    expect(() => Schema.decodeUnknownSync(JsIdentifier)("not-an-identifier")).toThrow();
    expect(() => Schema.decodeUnknownSync(JsIdentifier)("1leading")).toThrow();
    expect(() => Schema.decodeUnknownSync(JsIdentifier)("valid_$Name")).not.toThrow();

    expect(() =>
      CodeExecutionRequest.make({
        language: "javascript",
        source: "async () => 1",
        namespaces: [{ name: "ns", methods: [] }],
        network: NetworkDisabled.make({}),
        limits,
      }),
    ).toThrow();

    expect(() => CodeExecutionLimits.make({ ...limits, maxLogBytes: 2 * 1024 * 1024 })).toThrow();
  });
});

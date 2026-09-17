import { Cause, Redacted, Schema } from "effect";
import {
  AgentToolAuthorizationCheckError,
  AgentToolAuthorizationDenied,
} from "effect-agent/agent-error";
import * as FailureDiagnostic from "effect-agent/failure-diagnostic";
import { ToolCallId } from "effect-agent/identifiers";
import { PersistedJson } from "effect-agent/records";
import { WorkerError } from "effect-agent/worker";
import { Tool } from "effect/unstable/ai";
import { describe, expect, it } from "vite-plus/test";

class DependencyFailure extends Schema.TaggedError<DependencyFailure>()("DependencyFailure", {
  message: Schema.String,
  reason: Schema.String,
  code: Schema.String,
  callId: Schema.String,
  cause: FailureDiagnostic.Value,
  privatePayload: Schema.String,
}) {}

describe("causal failure diagnostics", () => {
  it("preserves the original local Cause and structured fields through a JSON boundary", () => {
    const transport = new Error("connection closed");

    const dependency = new DependencyFailure({
      message: "Pending delivery lookup failed",
      reason: "storage",
      code: "SQLITE_BUSY",
      callId: "call-1",
      cause: transport,
      privatePayload: "private tool parameters",
    });

    const original = Cause.fromReasons([
      ...Cause.fail(dependency).reasons,
      ...Cause.die(new Error("cleanup failed")).reasons,
      ...Cause.interrupt(42).reasons,
    ]);

    const error = new AgentToolAuthorizationCheckError({
      toolCallId: Schema.decodeSync(ToolCallId)("call-tool-1"),
      toolName: "phone_call",
      check: "pending-operations",
      message: "Could not verify current authority",
      cause: original,
    });

    expect(error.cause).toBe(original);
    expect(dependency.cause).toBe(transport);
    const codec = Schema.toCodecJson(AgentToolAuthorizationCheckError);
    const encoded = Schema.encodeSync(codec)(error);
    const decoded = Schema.decodeSync(codec)(JSON.parse(JSON.stringify(encoded)));

    expect(decoded._tag).toBe("AgentToolAuthorizationCheckError");
    expect(decoded.stack).toBe(error.stack);
    expect(decoded.cause.reasons).toMatchObject([
      {
        _tag: "Fail",
        error: {
          _tag: "Error",
          errorTag: "DependencyFailure",
          message: dependency.message,
          reason: { _tag: "Value", value: "storage" },
          code: "SQLITE_BUSY",
          stack: dependency.stack,
          context: { callId: "call-1" },
          cause: { _tag: "Error", message: "connection closed", stack: transport.stack },
        },
      },
      { _tag: "Die", defect: { _tag: "Error", message: "cleanup failed" } },
      { _tag: "Interrupt", fiberId: 42 },
    ]);
    expect(JSON.stringify(encoded)).not.toContain("private tool parameters");
    expect(Schema.encodeSync(codec)(decoded)).toEqual(encoded);
    expect(FailureDiagnostic.capture(decoded)).toEqual(FailureDiagnostic.capture(error));

    const denial = AgentToolAuthorizationDenied.make({
      toolCallId: error.toolCallId,
      toolName: error.toolName,
      message: "Execution was revoked",
      cause: original,
    });

    expect(denial.cause).toBe(original);
    const denialCodec = Schema.toCodecJson(AgentToolAuthorizationDenied);

    expect(
      Schema.decodeSync(denialCodec)(Schema.encodeSync(denialCodec)(denial)).cause?.reasons,
    ).toEqual(decoded.cause.reasons);

    // Native registration hashes the JSON contract of every Tool, including its failure codec.
    for (const schema of [WorkerError, AgentToolAuthorizationCheckError])
      expect(Schema.decodeUnknownExit(Schema.Json)(Tool.getJsonSchemaFromSchema(schema))._tag).toBe(
        "Success",
      );
  });

  it("excludes payload fields, redacts credential forms and identifies cyclic causes", () => {
    const error = Object.assign(new Error("request failed with Bearer secret-token"), {
      _tag: "TransportFailure",
      code: "RESET",
      reason: "closed",
      callId: "call-2",
      request: { authorization: "private-authorization", body: "private body" },
      response: { body: "private response" },
      cause: Redacted.make("private credential"),
    });

    const diagnostic = FailureDiagnostic.capture(error);

    expect(diagnostic).toMatchObject({
      _tag: "Error",
      errorTag: "TransportFailure",
      code: "RESET",
      message: "request failed with Bearer [REDACTED]",
      reason: { _tag: "Value", value: "closed" },
      cause: { _tag: "Omitted", reason: "redacted" },
      context: { callId: "call-2" },
    });
    const encoded = JSON.stringify(diagnostic);

    for (const privateText of [
      "secret-token",
      "private-authorization",
      "private body",
      "private response",
      "private credential",
    ])
      expect(encoded).not.toContain(privateText);
    const cyclic = new Error("cyclic");

    cyclic.cause = cyclic;
    expect(FailureDiagnostic.capture(cyclic)).toMatchObject({
      _tag: "Error",
      cause: { _tag: "Omitted", reason: "cycle" },
    });
    expect(
      FailureDiagnostic.capture(Object.assign(new Error("tagged"), { _tag: "Error" })),
    ).toMatchObject({ errorTag: "Error", message: "tagged" });

    const unavailableStack = Object.defineProperty(new Error("keep this error"), "stack", {
      get: () => {
        throw new Error("stack getter failed");
      },
    });

    expect(FailureDiagnostic.capture(unavailableStack)).toMatchObject({
      message: "keep this error",
      omittedFields: ["stack"],
    });
    expect(
      Schema.decodeUnknownExit(FailureDiagnostic.Diagnostic)({
        ...diagnostic,
        request: "private body",
      })._tag,
    ).toBe("Failure");
    expect(
      Schema.decodeUnknownExit(FailureDiagnostic.Diagnostic)({
        _tag: "Error",
        cause: { _tag: "Error", message: "closed", request: "private nested body" },
      })._tag,
    ).toBe("Failure");
  });

  it("shares capture budgets with diagnostic subtrees received over RPC", () => {
    const received: FailureDiagnostic.Diagnostic = {
      _tag: "Error",
      errors: Array.from({ length: 64 }, () => ({
        _tag: "Error",
        errors: Array.from({ length: 64 }, () => ({
          _tag: "Error",
          message: "x".repeat(1_024),
        })),
      })),
    };

    const receivedCauses: FailureDiagnostic.Diagnostic = {
      _tag: "Error",
      errors: Array.from({ length: 8 }, () => ({
        _tag: "Error",
        errors: Array.from({ length: 128 }, () => ({
          _tag: "Cause",
          reasons: Array.from({ length: 128 }, () => ({ _tag: "Interrupt", fiberId: 42 })),
        })),
      })),
    };

    for (const value of [received, new Error("RPC context", { cause: received }), receivedCauses]) {
      const diagnostic = FailureDiagnostic.capture(value);
      const encoded = JSON.stringify(diagnostic);

      expect(encoded).toContain('"truncated":true');
      expect(encoded.match(/"_tag":/g)?.length).toBeLessThanOrEqual(1_024);
      expect(encoded.length).toBeLessThan(256 * 1_024);
      expect(Schema.decodeExit(PersistedJson)(diagnostic)._tag).toBe("Success");
    }

    let deep: FailureDiagnostic.Diagnostic = { _tag: "Error", message: "leaf" };

    for (let index = 0; index < 70; index++) deep = { _tag: "Error", cause: deep };
    const diagnostic = FailureDiagnostic.capture(new Error("RPC context", { cause: deep }));

    expect(JSON.stringify(diagnostic)).toContain('"reason":"limit"');
    expect(Schema.decodeExit(PersistedJson)(diagnostic)._tag).toBe("Success");

    const cyclic: { _tag: "Error"; cause?: unknown } = { _tag: "Error" };

    cyclic.cause = cyclic;
    expect(FailureDiagnostic.capture(cyclic)).toEqual({
      _tag: "Error",
      cause: { _tag: "Omitted", reason: "cycle" },
    });
  });
});

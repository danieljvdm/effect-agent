import { describe, expect, it } from "vite-plus/test";

import { Schema } from "effect";
import * as RpcSchema from "effect/unstable/rpc/RpcSchema";

import { RunEvent } from "@effect-agent/core";
import { DemoRunSelection } from "./contracts";
import { ChatOutput } from "./general-chat";
import { DemoRunRpcFailure, DemoRunRpcs, StreamDemoRun } from "./run-rpc";

describe("demo transport contracts", () => {
  it("accepts the two explicit profiles and rejects untrimmed input", () => {
    expect(
      Schema.decodeSync(DemoRunSelection)({
        mode: "openai",
        message: "What changed today?",
      }),
    ).toEqual({ mode: "openai", message: "What changed today?" });
    expect(() =>
      Schema.decodeSync(DemoRunSelection)({
        mode: "deterministic",
        message: "  padded  ",
      }),
    ).toThrow();
  });

  it("shares one streaming Run definition between the server and client", () => {
    expect(DemoRunRpcs.requests.get("StreamDemoRun")).toBe(StreamDemoRun);
    expect(RpcSchema.isStreamSchema(StreamDemoRun.successSchema)).toBe(true);
  });

  it("round-trips live semantic events and failures through their wire schemas", () => {
    const completed = Schema.decodeSync(RunEvent)({
      _tag: "RunCompleted",
      eventVersion: 1,
      runId: "run-contract",
      conversationId: "conversation-contract",
      agentId: "general-chat-openai",
      sequence: 0,
      timestamp: "1970-01-01T00:00:00.000Z",
      output: Schema.encodeSync(ChatOutput)({ answer: "A general answer." }),
      turns: 1,
      finishReason: "model-stop",
    });
    const encoded = Schema.encodeSync(RunEvent)(completed);
    const failure = new DemoRunRpcFailure({
      errorTag: "AgentOutputError",
      message: "Output did not match the ChatOutput Schema.",
    });

    expect(Schema.decodeSync(RunEvent)(encoded)).toEqual(completed);
    expect(
      Schema.decodeSync(DemoRunRpcFailure)(Schema.encodeSync(DemoRunRpcFailure)(failure)),
    ).toEqual(failure);
  });
});

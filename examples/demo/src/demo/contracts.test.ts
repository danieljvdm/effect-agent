import { describe, expect, it } from "vite-plus/test";

import { Schema } from "effect";
import * as RpcSchema from "effect/unstable/rpc/RpcSchema";

import { RunEvent } from "@effect-agent/core";
import { expectedTravelPlan, phase0Trip, TravelPlan } from "@effect-agent/testing";
import { DemoRunSelection } from "./contracts";
import { DemoRunRpcFailure, DemoRunRpcs, StreamDemoRun } from "./run-rpc";

describe("demo transport contracts", () => {
  it("accepts the two explicit profiles and rejects untrimmed input", () => {
    expect(
      Schema.decodeSync(DemoRunSelection)({
        mode: "openai",
        request: phase0Trip.request,
      }),
    ).toEqual({ mode: "openai", request: phase0Trip.request });
    expect(() =>
      Schema.decodeSync(DemoRunSelection)({
        mode: "deterministic",
        request: "  padded  ",
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
      agentId: "travel-planner",
      sequence: 0,
      timestamp: "1970-01-01T00:00:00.000Z",
      output: Schema.encodeSync(TravelPlan)(expectedTravelPlan),
      turns: 2,
      finishReason: "model-stop",
    });
    const encoded = Schema.encodeSync(RunEvent)(completed);
    const failure = new DemoRunRpcFailure({
      errorTag: "AgentOutputError",
      message: "Output did not match the TravelPlan Schema.",
    });

    expect(Schema.decodeSync(RunEvent)(encoded)).toEqual(completed);
    expect(
      Schema.decodeSync(DemoRunRpcFailure)(Schema.encodeSync(DemoRunRpcFailure)(failure)),
    ).toEqual(failure);
  });
});

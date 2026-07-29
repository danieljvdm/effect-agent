import { describe, expect, it } from "vite-plus/test";

import { Schema } from "effect";

import { RunEvent } from "@effect-agent/core";
import { expectedTravelPlan, phase0Trip, TravelPlan } from "@effect-agent/testing";
import {
  DemoRunSelection,
  OpenAiDemoRunResponse,
  type OpenAiDemoRunResponse as OpenAiDemoRunResponseValue,
} from "./contracts";

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

  it("round-trips live semantic events through their encoded form", () => {
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
    const response: OpenAiDemoRunResponseValue = {
      _tag: "OpenAiDemoRunSuccess",
      model: "gpt-5.6-luna",
      events: [completed],
      output: expectedTravelPlan,
    };
    const encoded = Schema.encodeSync(OpenAiDemoRunResponse)(response);

    expect(Schema.decodeSync(OpenAiDemoRunResponse)(encoded)).toEqual(response);
  });
});

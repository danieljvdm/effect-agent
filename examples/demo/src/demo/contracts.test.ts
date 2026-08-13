import { describe, expect, it } from "vite-plus/test";

import { Schema } from "effect";
import * as RpcSchema from "effect/unstable/rpc/RpcSchema";

import { DemoRunSelection } from "./contracts";
import {
  DemoOperationalEvent,
  DemoRunFailure,
  DemoRunOpened,
  QueueRunCommandRequest,
  StartLiveTravelChatRequest,
  StartOperationalRunRequest,
} from "./operational-contracts";
import {
  DemoRunRpcs,
  StreamChatRun,
  StreamLiveTravelChatRun,
  StreamOperationalRun,
} from "./run-rpc";

describe("Phase 2 demo transport contracts", () => {
  it("accepts explicit scenarios and bounds queued input", () => {
    expect(
      Schema.decodeSync(DemoRunSelection)({
        mode: "deterministic",
        message: "What does this demo prove?",
        history: [
          {
            role: "assistant",
            content: "The demo proves bounded agent control.",
          },
        ],
      }),
    ).toEqual({
      mode: "deterministic",
      message: "What does this demo prove?",
      history: [
        {
          role: "assistant",
          content: "The demo proves bounded agent control.",
        },
      ],
    });
    expect(
      Schema.decodeSync(StartOperationalRunRequest)({
        scenario: "budget-cost",
      }),
    ).toEqual({ scenario: "budget-cost" });
    expect(
      Schema.decodeSync(StartOperationalRunRequest)({
        scenario: "tool-defect",
      }),
    ).toEqual({ scenario: "tool-defect" });
    expect(
      Schema.encodeSync(StartOperationalRunRequest)(
        StartOperationalRunRequest.make({ scenario: "tool-defect" }),
      ),
    ).toEqual({ scenario: "tool-defect" });
    expect(
      Schema.decodeSync(StartLiveTravelChatRequest)({
        message: "Compare the fixture flights, stays, and activities.",
        scenario: "guided",
      }),
    ).toEqual({
      message: "Compare the fixture flights, stays, and activities.",
      scenario: "guided",
    });
    expect(() =>
      Schema.decodeSync(QueueRunCommandRequest)({
        handle: "demo-handle-contract",
        kind: "steering",
        content: "",
      }),
    ).toThrow();
  });

  it("shares general chat, live travel, and simulator streams plus two unary controls", () => {
    expect(DemoRunRpcs.requests.get("StreamChatRun")).toBe(StreamChatRun);
    expect(RpcSchema.isStreamSchema(StreamChatRun.successSchema)).toBe(true);
    expect(DemoRunRpcs.requests.get("StreamLiveTravelChatRun")).toBe(StreamLiveTravelChatRun);
    expect(RpcSchema.isStreamSchema(StreamLiveTravelChatRun.successSchema)).toBe(true);
    expect(DemoRunRpcs.requests.get("StreamOperationalRun")).toBe(StreamOperationalRun);
    expect(RpcSchema.isStreamSchema(StreamOperationalRun.successSchema)).toBe(true);
    expect(DemoRunRpcs.requests.has("QueueRunCommand")).toBe(true);
    expect(DemoRunRpcs.requests.has("ResolveRunApproval")).toBe(true);
  });

  it("round-trips operational evidence and typed failures", () => {
    const opened = Schema.decodeSync(DemoRunOpened)({
      _tag: "DemoRunOpened",
      handle: "demo-handle-contract",
      emittedAt: "2026-07-30T12:00:00.000Z",
      runId: "demo-run-contract",
      conversationId: "demo-conversation-contract",
      scenario: "guided",
      executionClass: "ephemeral",
      schedulerConcurrency: 3,
    });
    const encoded = Schema.encodeSync(DemoOperationalEvent)(opened);
    const failure = DemoRunFailure.make({
      errorTag: "BudgetExceeded",
      message: "The run-level cost fuse opened.",
    });

    expect(Schema.decodeSync(DemoOperationalEvent)(encoded)).toEqual(opened);
    expect(Schema.decodeSync(DemoRunFailure)(Schema.encodeSync(DemoRunFailure)(failure))).toEqual(
      failure,
    );
  });
});

import { Effect, Schema, Stream } from "effect";
import { Model } from "effect/unstable/ai";
import * as Atom from "effect/unstable/reactivity/Atom";

import { Agent, type RunEvent } from "@effect-agent/core";
import { AgentRuntime } from "@effect-agent/engine";
import {
  phase0HappyPathTurns,
  phase0Trip,
  ScriptedModel,
  TravelPlan,
  type TravelPlan as TravelPlanValue,
  TravelPlanner,
  TravelPlannerRuntimeLayer,
} from "@effect-agent/testing";
import type { DemoRunSelection } from "./contracts";
import { DemoRunRpcClient } from "./run-rpc-client";

export type DemoStatus = "idle" | "running" | "succeeded" | "failed" | "interrupted";

export interface ChatMessage {
  readonly id: string;
  readonly role: "assistant" | "user";
  readonly content: string;
  readonly reasoning?: string;
}

export interface DemoState {
  readonly status: DemoStatus;
  readonly mode: DemoRunSelection["mode"];
  readonly runNumber: number;
  readonly messages: ReadonlyArray<ChatMessage>;
  readonly events: ReadonlyArray<RunEvent>;
  readonly output: TravelPlanValue | null;
  readonly error: string | null;
  readonly activeRequest: string;
}

const initialPrompt = "Plan a review-only London trip and show me the best available option.";

export const initialDemoState: DemoState = {
  status: "idle",
  mode: "deterministic",
  runNumber: 0,
  messages: [
    {
      id: "intro",
      role: "assistant",
      content:
        "This bench runs the real Phase 0 agent loop. Use the direct in-browser fixture, or the streaming OpenAI RPC profile, then inspect its tool call and semantic events.",
    },
  ],
  events: [],
  output: null,
  error: null,
  activeRequest: initialPrompt,
};

/** Shared browser state for the current agent run and selected model profile. */
export const demoStateAtom = Atom.make<DemoState>(initialDemoState);

const failureMessage = (error: unknown): string => {
  if (
    typeof error === "object" &&
    error !== null &&
    "message" in error &&
    typeof error.message === "string"
  ) {
    return error.message;
  }
  return String(error);
};

const summarizePlan = (plan: TravelPlanValue): string => {
  const itinerary = plan.itineraries[0];
  if (itinerary === undefined) {
    return "The run completed with no itineraries.";
  }
  return [
    `**${itinerary.title}**`,
    `${itinerary.route} · ${itinerary.dates}`,
    `${itinerary.flight}\n${itinerary.lodging}`,
    `Estimated total: **$${(itinerary.estimatedTotalCents / 100).toLocaleString("en-US")} ${itinerary.currency}**`,
    "No reservation was made. The next action is review.",
  ].join("\n\n");
};

const makeAgent = () =>
  Agent.withModel(
    TravelPlanner,
    Model.make("scripted", "travel-planner-phase-0", ScriptedModel.layer(phase0HappyPathTurns)),
  );

const updateAssistantMessage = (
  messages: ReadonlyArray<ChatMessage>,
  id: string,
  update: (message: ChatMessage) => ChatMessage,
): ReadonlyArray<ChatMessage> => {
  const existing = messages.findIndex((message) => message.id === id);
  if (existing === -1) {
    return [...messages, update({ id, role: "assistant", content: "" })];
  }
  return messages.map((message, index) => (index === existing ? update(message) : message));
};

/** Starts one selected profile and projects its semantic events into browser state. */
export const runDemoAtom = Atom.fn<DemoRunSelection>()(({ mode, request }, context) => {
  const previous = context(demoStateAtom);
  const runNumber = previous.runNumber + 1;
  const assistantId = `assistant-${runNumber}`;

  context.set(demoStateAtom, {
    ...previous,
    status: "running",
    mode,
    runNumber,
    events: [],
    output: null,
    error: null,
    activeRequest: request,
    messages: [...previous.messages, { id: `user-${runNumber}`, role: "user", content: request }],
  });

  const projectEvent = Effect.fn("Demo.projectEvent")(function* (event: RunEvent) {
    const current = context(demoStateAtom);
    const messages =
      event._tag === "TextDelta"
        ? updateAssistantMessage(current.messages, assistantId, (message) => ({
            ...message,
            content: message.content + event.text,
          }))
        : event._tag === "ReasoningDelta"
          ? updateAssistantMessage(current.messages, assistantId, (message) => ({
              ...message,
              reasoning: (message.reasoning ?? "") + event.text,
            }))
          : current.messages;
    context.set(demoStateAtom, {
      ...current,
      messages,
      events: [...current.events, event],
    });

    if (event._tag === "RunCompleted") {
      const candidateOutput: unknown = event.output;
      const output = yield* Schema.decodeUnknownEffect(TravelPlan)(candidateOutput);
      const completed = context(demoStateAtom);
      context.set(demoStateAtom, {
        ...completed,
        status: "succeeded",
        output,
        messages: updateAssistantMessage(completed.messages, assistantId, (message) => ({
          ...message,
          content: summarizePlan(output),
        })),
      });
    }
  });

  const deterministic = AgentRuntime.stream(makeAgent(), { ...phase0Trip, request }).pipe(
    Stream.runForEach(projectEvent),
    Effect.provide(TravelPlannerRuntimeLayer),
    Effect.scoped,
  );
  const openai = Stream.unwrap(
    Effect.gen(function* () {
      const client = yield* DemoRunRpcClient;
      return client.StreamDemoRun({ request });
    }),
  ).pipe(Stream.runForEach(projectEvent), Effect.provide(DemoRunRpcClient.layer), Effect.scoped);
  const selected = Effect.gen(function* () {
    if (mode === "openai") {
      return yield* openai;
    }
    return yield* deterministic;
  });

  return selected.pipe(
    Effect.tap(() =>
      Effect.sync(() => {
        const current = context(demoStateAtom);
        if (current.status === "running") {
          context.set(demoStateAtom, {
            ...current,
            status: "failed",
            error: "The Run stream ended without a terminal event.",
          });
        }
      }),
    ),
    Effect.tapError((error) =>
      Effect.sync(() => {
        context.set(demoStateAtom, {
          ...context(demoStateAtom),
          status: "failed",
          error: failureMessage(error),
        });
      }),
    ),
    Effect.onInterrupt(() =>
      Effect.sync(() => {
        context.set(demoStateAtom, {
          ...context(demoStateAtom),
          status: "interrupted",
        });
      }),
    ),
  );
});

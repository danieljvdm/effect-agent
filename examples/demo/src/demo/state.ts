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
import { DemoRequestError, type DemoRunSelection, OpenAiDemoRunResponse } from "./contracts";
import { runOpenAiDemo } from "./run-openai";

export type DemoStatus = "idle" | "running" | "succeeded" | "failed" | "interrupted";

export interface ChatMessage {
  readonly id: string;
  readonly role: "assistant" | "user";
  readonly content: string;
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
        "This bench runs the real Phase 0 agent loop. Use the deterministic fixture, or opt into the server-side OpenAI profile, then inspect its tool call and semantic events.",
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

const requestOpenAiRun = Effect.fn("Demo.requestOpenAiRun")(function* (request: string) {
  const encoded = yield* Effect.tryPromise({
    try: (signal) => runOpenAiDemo({ data: { request }, signal }),
    catch: (cause) =>
      new DemoRequestError({
        message: failureMessage(cause),
      }),
  });
  const response = yield* Schema.decodeEffect(OpenAiDemoRunResponse)(encoded).pipe(
    Effect.mapError(
      (cause) =>
        new DemoRequestError({
          message: `Invalid live response: ${cause.message}`,
        }),
    ),
  );
  if (response._tag === "OpenAiDemoRunFailure") {
    return yield* new DemoRequestError({
      message: `${response.errorTag}: ${response.message}`,
    });
  }
  return response;
});

/** Starts one selected profile and projects its semantic events into browser state. */
export const runDemoAtom = Atom.fn<DemoRunSelection>()(({ mode, request }, context) => {
  const previous = context(demoStateAtom);
  const runNumber = previous.runNumber + 1;

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
    context.set(demoStateAtom, {
      ...context(demoStateAtom),
      events: [...context(demoStateAtom).events, event],
    });

    if (event._tag === "RunCompleted") {
      const candidateOutput: unknown = event.output;
      const output = yield* Schema.decodeUnknownEffect(TravelPlan)(candidateOutput);
      const current = context(demoStateAtom);
      context.set(demoStateAtom, {
        ...current,
        status: "succeeded",
        output,
        messages: [
          ...current.messages,
          {
            id: `assistant-${runNumber}`,
            role: "assistant",
            content: summarizePlan(output),
          },
        ],
      });
    }

    yield* Effect.sleep("35 millis");
  });

  const deterministic = AgentRuntime.stream(makeAgent(), { ...phase0Trip, request }).pipe(
    Stream.runForEach(projectEvent),
    Effect.provide(TravelPlannerRuntimeLayer),
    Effect.scoped,
  );
  const openai = requestOpenAiRun(request).pipe(
    Effect.flatMap((response) => Effect.forEach(response.events, projectEvent)),
  );
  const selected = Effect.gen(function* () {
    if (mode === "openai") {
      return yield* openai;
    }
    return yield* deterministic;
  });

  return selected.pipe(
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

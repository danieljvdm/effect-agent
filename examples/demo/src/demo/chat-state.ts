import type { RunEvent } from "@effect-agent/core";
import {
  TravelPlan,
  type TravelPlan as TravelPlanValue,
} from "@effect-agent/testing/fixtures/travel-planner";
import { Effect, Schema, Stream } from "effect";
import * as Atom from "effect/unstable/reactivity/Atom";

import { capabilityFailureMessage, formatTravelPlanForChat } from "./chat-capabilities";
import { DemoChatHistoryMessage, type DemoRunSelection } from "./contracts";
import { decodeErrorDetails } from "./error-details";
import { ChatOutput, type ChatOutput as ChatOutputValue } from "./general-chat";
import {
  DemoModelSettings,
  type DemoApprovalChoice,
  type DemoOperationalEvent,
  type DemoRunHandle,
  type DemoScenario,
} from "./operational-contracts";
import { DemoRunRpcClient, DemoRunRpcRuntime } from "./run-rpc-client";

export type ChatStatus = "idle" | "running" | "succeeded" | "failed" | "interrupted";

export interface ChatMessage {
  readonly id: string;
  readonly role: "assistant" | "user";
  readonly content: string;
  readonly reasoning?: string;
  readonly experience?: "general" | "capability";
  readonly scenario?: DemoScenario;
  readonly events?: ReadonlyArray<DemoOperationalEvent>;
}

export interface ChatState {
  readonly status: ChatStatus;
  readonly mode: DemoRunSelection["mode"];
  readonly runNumber: number;
  readonly activeExperience: "general" | "capability" | null;
  readonly handle: DemoRunHandle | null;
  readonly messages: ReadonlyArray<ChatMessage>;
  readonly events: ReadonlyArray<DemoOperationalEvent>;
  readonly output: ChatOutputValue | null;
  readonly capabilityOutput: TravelPlanValue | null;
  readonly error: string | null;
  readonly controlError: string | null;
}

export const initialChatState: ChatState = {
  status: "idle",
  mode: "openai",
  runNumber: 0,
  activeExperience: null,
  handle: null,
  messages: [
    {
      id: "intro",
      role: "assistant",
      content:
        "I’m an OpenAI-powered travel agent doing real web research. Ask me to plan any trip; I’ll cite my sources, show my tool work, and ask before any demo hold.",
    },
  ],
  events: [],
  output: null,
  capabilityOutput: null,
  error: null,
  controlError: null,
};

export const chatStateAtom = Atom.make<ChatState>(initialChatState);

/** Browser-selected model settings for the live research agent. */
export const modelSettingsAtom = Atom.make<DemoModelSettings>(
  DemoModelSettings.make({ model: "gpt-5.6-luna", reasoningEffort: "medium", fast: false }),
);

/** Projects the visible transcript into the bounded wire history for the next Run. */
export const chatHistoryFromMessages = (
  messages: ReadonlyArray<ChatMessage>,
): ReadonlyArray<DemoChatHistoryMessage> =>
  messages
    .filter((message) => message.id !== "intro" && message.content.trim().length > 0)
    .slice(-40)
    .map((message) =>
      DemoChatHistoryMessage.make({
        role: message.role,
        content: message.content.trim(),
      }),
    );

const failureMessage = (error: unknown): string =>
  decodeErrorDetails(error).message ?? "The chat Run failed.";

const updateAssistant = (
  messages: ReadonlyArray<ChatMessage>,
  id: string,
  update: (message: ChatMessage) => ChatMessage,
): ReadonlyArray<ChatMessage> => {
  const index = messages.findIndex((message) => message.id === id);

  if (index === -1) {
    return [...messages, update({ id, role: "assistant", content: "" })];
  }

  return messages.map((message, messageIndex) =>
    messageIndex === index ? update(message) : message,
  );
};

/** Starts one standalone chat Run without sharing simulator state or cancellation. */
export const runChatAtom = DemoRunRpcRuntime.fn<DemoRunSelection>()((
  { history, message, mode },
  context,
) => {
  const previous = context(chatStateAtom);
  const runNumber = previous.runNumber + 1;
  const assistantId = `assistant-${runNumber}`;

  context.set(chatStateAtom, {
    ...previous,
    status: "running",
    mode,
    runNumber,
    activeExperience: "general",
    handle: null,
    events: [],
    output: null,
    capabilityOutput: null,
    error: null,
    controlError: null,
    messages: [
      ...previous.messages,
      { id: `user-${runNumber}`, role: "user", content: message },
      { id: assistantId, role: "assistant", content: "", experience: "general", events: [] },
    ],
  });

  const projectEvent = Effect.fn("Demo.projectChatEvent")(function* (event: RunEvent) {
    const current = context(chatStateAtom);

    const messages =
      event._tag === "ReasoningDelta"
        ? updateAssistant(current.messages, assistantId, (assistant) => ({
            ...assistant,
            reasoning: (assistant.reasoning ?? "") + event.text,
            events: [...(assistant.events ?? []), event],
          }))
        : updateAssistant(current.messages, assistantId, (assistant) => ({
            ...assistant,
            events: [...(assistant.events ?? []), event],
          }));

    context.set(chatStateAtom, {
      ...current,
      messages,
      events: [...current.events, event],
    });

    if (event._tag === "RunCompleted") {
      const candidate: unknown = event.output;
      const output = yield* Schema.decodeUnknownEffect(ChatOutput)(candidate);
      const completed = context(chatStateAtom);

      context.set(chatStateAtom, {
        ...completed,
        status: "succeeded",
        activeExperience: null,
        output,
        messages: updateAssistant(completed.messages, assistantId, (assistant) => ({
          ...assistant,
          content: output.answer,
        })),
      });
    } else if (event._tag === "RunFailed") {
      context.set(chatStateAtom, {
        ...context(chatStateAtom),
        status: "failed",
        activeExperience: null,
        error: event.message,
      });
    } else if (event._tag === "RunInterrupted") {
      context.set(chatStateAtom, {
        ...context(chatStateAtom),
        status: "interrupted",
        activeExperience: null,
        error: event.message,
      });
    }
  });

  return Stream.unwrap(
    Effect.gen(function* () {
      const client = yield* DemoRunRpcClient;

      return client.StreamChatRun({ history, message, mode });
    }),
  ).pipe(
    Stream.runForEach(projectEvent),
    Effect.scoped,
    Effect.tap(() =>
      Effect.sync(() => {
        const current = context(chatStateAtom);

        if (current.status === "running") {
          context.set(chatStateAtom, {
            ...current,
            status: "failed",
            activeExperience: null,
            error: "The chat stream ended without a terminal event.",
          });
        }
      }),
    ),
    Effect.tapError((error) =>
      Effect.sync(() => {
        context.set(chatStateAtom, {
          ...context(chatStateAtom),
          status: "failed",
          activeExperience: null,
          error: failureMessage(error),
        });
      }),
    ),
    Effect.onInterrupt(() =>
      Effect.sync(() => {
        context.set(chatStateAtom, {
          ...context(chatStateAtom),
          status: "interrupted",
          activeExperience: null,
        });
      }),
    ),
  );
});

export interface CapabilityChatRequest {
  readonly message: string;
  readonly scenario: DemoScenario;
}

/** Starts a live OpenAI travel Run or an explicit scripted replay in Chat. */
export const runCapabilityChatAtom = DemoRunRpcRuntime.fn<CapabilityChatRequest>()((
  { message, scenario },
  context,
) => {
  const previous = context(chatStateAtom);
  const runNumber = previous.runNumber + 1;
  const assistantId = `assistant-${runNumber}`;

  context.set(chatStateAtom, {
    ...previous,
    status: "running",
    runNumber,
    activeExperience: "capability",
    handle: null,
    events: [],
    output: null,
    capabilityOutput: null,
    error: null,
    controlError: null,
    messages: [
      ...previous.messages,
      { id: `user-${runNumber}`, role: "user", content: message },
      {
        id: assistantId,
        role: "assistant",
        content: "",
        experience: "capability",
        scenario,
        events: [],
      },
    ],
  });

  const projectEvent = Effect.fn("Demo.projectCapabilityChatEvent")(function* (
    event: DemoOperationalEvent,
  ) {
    const current = context(chatStateAtom);
    const nextEvents = [...current.events, event];
    const handle = "handle" in event ? event.handle : current.handle;
    let status = current.status;
    let capabilityOutput = current.capabilityOutput;
    let error = current.error;
    let content: string | undefined;

    switch (event._tag) {
      case "DemoApprovalPending":
        status = "running";
        break;
      case "RunCompleted": {
        const candidate: unknown = event.output;

        capabilityOutput = yield* Schema.decodeUnknownEffect(TravelPlan)(candidate);
        content = formatTravelPlanForChat(capabilityOutput);
        status = "succeeded";
        break;
      }
      case "RunFailed":
        status = "failed";
        content = capabilityFailureMessage(nextEvents, event.message);
        break;
      case "RunInterrupted":
        status = "interrupted";
        content = event.message;
        break;
      case "RunSuspended":
        status = "running";
        break;
    }

    context.set(chatStateAtom, {
      ...current,
      status,
      activeExperience:
        status === "succeeded" || status === "failed" || status === "interrupted"
          ? null
          : current.activeExperience,
      handle,
      events: nextEvents,
      capabilityOutput,
      error,
      messages: updateAssistant(current.messages, assistantId, (assistant) => ({
        ...assistant,
        content: content ?? assistant.content,
        events: [...(assistant.events ?? []), event],
      })),
    });
  });

  return Stream.unwrap(
    Effect.gen(function* () {
      const client = yield* DemoRunRpcClient;

      return previous.mode === "openai"
        ? client.StreamLiveTravelChatRun({
            message,
            scenario,
            settings: context(modelSettingsAtom),
          })
        : client.StreamOperationalRun({ scenario });
    }),
  ).pipe(
    Stream.runForEach(projectEvent),
    Effect.scoped,
    Effect.tap(() =>
      Effect.sync(() => {
        const current = context(chatStateAtom);

        if (current.activeExperience === "capability") {
          context.set(chatStateAtom, {
            ...current,
            status: "failed",
            activeExperience: null,
            error: "The runtime experiment ended without a terminal event.",
          });
        }
      }),
    ),
    Effect.tapError((cause) =>
      Effect.sync(() => {
        const current = context(chatStateAtom);
        const messageText = capabilityFailureMessage(current.events, failureMessage(cause));

        context.set(chatStateAtom, {
          ...current,
          status: "failed",
          activeExperience: null,
          error: null,
          messages: updateAssistant(current.messages, assistantId, (assistant) => ({
            ...assistant,
            content: assistant.content.length > 0 ? assistant.content : messageText,
          })),
        });
      }),
    ),
    Effect.onInterrupt(() =>
      Effect.sync(() => {
        const current = context(chatStateAtom);

        context.set(chatStateAtom, {
          ...current,
          status: "interrupted",
          activeExperience: null,
        });
      }),
    ),
  );
});

export interface QueueChatUpdate {
  readonly content: string;
}

/** Queues ordinary chat input without replacing the active travel Run. */
export const queueChatUpdateAtom = DemoRunRpcRuntime.fn<QueueChatUpdate>()((
  { content },
  context,
) => {
  const current = context(chatStateAtom);

  if (current.handle === null || current.activeExperience !== "capability") {
    return Effect.sync(() => {
      context.set(chatStateAtom, {
        ...current,
        controlError: "The active Run is no longer accepting updates.",
      });
    });
  }

  const handle = current.handle;

  const kind = current.events.some(
    (event) => event._tag === "DemoCommandStateChanged" && event.kind === "steering",
  )
    ? ("follow-up" as const)
    : ("steering" as const);

  context.set(chatStateAtom, { ...current, controlError: null });

  return Effect.gen(function* () {
    const client = yield* DemoRunRpcClient;

    yield* client.QueueRunCommand({ handle, kind, content });
    const latest = context(chatStateAtom);

    context.set(chatStateAtom, {
      ...latest,
      messages: [
        ...latest.messages,
        {
          id: `queued-${latest.runNumber}-${latest.messages.length}`,
          role: "user",
          content,
          experience: "capability",
        },
      ],
    });
  }).pipe(
    Effect.scoped,
    Effect.catch((cause) =>
      Effect.sync(() => {
        const latest = context(chatStateAtom);

        context.set(chatStateAtom, {
          ...latest,
          controlError: failureMessage(cause),
        });
      }),
    ),
  );
});

export interface ResolveChatApproval {
  readonly requestId: string;
  readonly choice: DemoApprovalChoice;
}

/** Resolves the approval card without replacing or interrupting its active Run. */
export const resolveChatApprovalAtom = DemoRunRpcRuntime.fn<ResolveChatApproval>()((
  request,
  context,
) => {
  const current = context(chatStateAtom);

  if (current.handle === null || current.activeExperience !== "capability") {
    return Effect.sync(() => {
      context.set(chatStateAtom, {
        ...current,
        controlError: "This approval request is no longer active.",
      });
    });
  }

  const handle = current.handle;

  context.set(chatStateAtom, { ...current, controlError: null });

  return Effect.gen(function* () {
    const client = yield* DemoRunRpcClient;

    yield* client.ResolveRunApproval({
      handle,
      requestId: request.requestId,
      choice: request.choice,
    });
  }).pipe(
    Effect.scoped,
    Effect.catch((cause) =>
      Effect.sync(() => {
        const latest = context(chatStateAtom);

        context.set(chatStateAtom, {
          ...latest,
          controlError: failureMessage(cause),
        });
      }),
    ),
  );
});

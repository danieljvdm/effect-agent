"use client";

import { useAtomSet, useAtomValue } from "@effect/atom-react";
import type { ChatStatus as AiChatStatus } from "ai";
import { Schema } from "effect";
import * as Atom from "effect/unstable/reactivity/Atom";
import { ChevronDown, FlaskConical, RotateCcw, Sparkles } from "lucide-react";
import { useMemo, useState } from "react";

import { RunEvent } from "@effect-agent/core";
import { AgentActivity } from "@/components/ai-elements/agent-activity";
import { CapabilityChatTrace } from "@/components/capability-chat-trace";
import {
  Conversation,
  ConversationContent,
  ConversationScrollButton,
} from "@/components/ai-elements/conversation";
import { Message, MessageContent, MessageResponse } from "@/components/ai-elements/message";
import {
  PromptInput,
  PromptInputFooter,
  PromptInputSubmit,
  PromptInputTextarea,
} from "@/components/ai-elements/prompt-input";
import { Reasoning } from "@/components/ai-elements/reasoning";
import { Tool, ToolContent, ToolData, ToolHeader } from "@/components/ai-elements/tool";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { ToggleGroup, ToggleGroupItem } from "@/components/ui/toggle-group";
import {
  primaryCapabilityRecipes,
  secondaryCapabilityRecipes,
  type CapabilityRecipe,
} from "@/demo/chat-capabilities";
import {
  modelSettingsAtom,
  chatHistoryFromMessages,
  chatStateAtom,
  initialChatState,
  queueChatUpdateAtom,
  resolveChatApprovalAtom,
  runCapabilityChatAtom,
  runChatAtom,
  type ChatMessage,
  type ChatState,
  type ChatStatus,
} from "@/demo/chat-state";
import { DemoModelSettings } from "@/demo/operational-contracts";
import { projectRunActivity } from "@/demo/run-activity";
import { projectToolTraces } from "@/demo/tool-trace";

const examples = [
  "Plan the fixed London demo trip and compare all three suppliers.",
  "What can you do with the repeatable travel inventory?",
  "Explain what you would need before making a real reservation.",
] as const;

const statusForMessage = (
  message: ChatMessage,
  activeAssistantId: string,
  current: ChatStatus,
): ChatStatus => {
  if (message.id === activeAssistantId) return current;
  const events = message.events ?? [];
  if (events.some((event) => event._tag === "RunCompleted")) return "succeeded";
  if (events.some((event) => event._tag === "RunFailed")) return "failed";
  if (events.some((event) => event._tag === "RunInterrupted")) return "interrupted";
  return "idle";
};

function GeneralChatTrace({
  active,
  message,
  mode,
}: {
  readonly active: boolean;
  readonly message: ChatMessage;
  readonly mode: ChatState["mode"];
}) {
  const events = useMemo(
    () => (message.events ?? []).filter((event): event is RunEvent => Schema.is(RunEvent)(event)),
    [message.events],
  );
  const activity = useMemo(() => projectRunActivity(events, mode), [events, mode]);
  const tools = useMemo(() => projectToolTraces(events), [events]);

  return (
    <>
      {active ? <AgentActivity {...activity} /> : null}
      {tools.length > 0 ? (
        <div className="mt-3 grid gap-2">
          {tools.map((trace) => (
            <Tool key={trace.toolCallId}>
              <ToolHeader
                execution={trace.providerExecuted ? "provider" : "application"}
                name={trace.toolName}
                state={trace.state}
              />
              <ToolContent className="grid gap-3">
                <ToolData label="Parameters" value={trace.parameters} />
                {trace.result === undefined ? null : (
                  <ToolData label="Result" value={trace.result} />
                )}
              </ToolContent>
            </Tool>
          ))}
        </div>
      ) : null}
    </>
  );
}

/** Calm, default chat surface. Operational detail stays collapsed and optional. */
export function ChatWorkbench() {
  const state = useAtomValue(chatStateAtom);
  const setState = useAtomSet(chatStateAtom);
  const modelSettings = useAtomValue(modelSettingsAtom);
  const setModelSettings = useAtomSet(modelSettingsAtom);
  const run = useAtomSet(runChatAtom);
  const runCapability = useAtomSet(runCapabilityChatAtom);
  const queueUpdate = useAtomSet(queueChatUpdateAtom);
  const resolveApproval = useAtomSet(resolveChatApprovalAtom);
  const [prompt, setPrompt] = useState<string>(primaryCapabilityRecipes[0]?.message ?? examples[0]);
  const running = state.status === "running";
  const canQueue = running && state.activeExperience === "capability" && state.handle !== null;
  const hasQueuedSteering = state.events.some(
    (event) => event._tag === "DemoCommandStateChanged" && event.kind === "steering",
  );
  const quickUpdate = hasQueuedSteering
    ? "Prefer a quiet room away from the lift."
    : "Move the departure date to 2026-09-22.";
  const activeAssistantId = `assistant-${state.runNumber}`;
  const submitStatus: AiChatStatus = running
    ? "streaming"
    : state.status === "failed"
      ? "error"
      : "ready";

  const reset = () => {
    run(Atom.Interrupt);
    runCapability(Atom.Interrupt);
    setState({
      ...initialChatState,
      mode: state.mode,
      runNumber: state.runNumber,
    });
  };

  const selectMode = (mode: ChatState["mode"]) => {
    if (!running) {
      setState({ ...state, mode });
    }
  };

  const submitMessage = (candidate: string, recipe?: CapabilityRecipe) => {
    const message = candidate.trim();
    if (message.length > 0 && !running) {
      if (state.mode === "deterministic" && recipe === undefined) {
        run({
          mode: state.mode,
          message,
          history: chatHistoryFromMessages(state.messages),
        });
      } else {
        runCapability({ message, scenario: recipe?.scenario ?? "guided" });
      }
      setPrompt("");
    }
  };

  const submitPrompt = () => {
    const message = prompt.trim();
    if (message.length === 0) return;
    if (canQueue) {
      queueUpdate({ content: message });
      setPrompt("");
    } else {
      submitMessage(message);
    }
  };

  const launchRecipe = (recipe: CapabilityRecipe) => {
    submitMessage(recipe.message, recipe);
  };

  return (
    <div className="min-h-[calc(100svh-3.75rem)] bg-[radial-gradient(circle_at_50%_-20%,oklch(0.93_0.035_210),transparent_42%),linear-gradient(to_bottom,oklch(0.985_0.002_85),oklch(0.97_0.006_210))] px-3 py-5 sm:px-6">
      <div className="mx-auto flex min-h-[calc(100svh-6.25rem)] max-w-4xl flex-col">
        <div className="mb-4 flex items-center justify-between gap-3 px-1">
          <div>
            <p className="text-sm font-medium text-slate-900">Conversation</p>
            <p className="mt-0.5 text-xs text-muted-foreground">
              Plan a trip, revise it mid-run, or try a guarded demo hold.
            </p>
          </div>
          <div className="flex items-center gap-2">
            <Badge className="hidden sm:inline-flex">
              {state.activeExperience === "capability"
                ? state.mode === "openai"
                  ? "OpenAI agent · live web research"
                  : "Scripted replay"
                : state.mode === "openai"
                  ? "OpenAI agent · live web research"
                  : "Scripted replay"}
            </Badge>
            {state.mode === "openai" ? (
              <>
                <select
                  aria-label="Model"
                  className="h-7 rounded-md border border-slate-300 bg-white px-1.5 text-xs text-slate-700"
                  onChange={(event) =>
                    setModelSettings(
                      DemoModelSettings.make({
                        ...modelSettings,
                        model: event.target.value as DemoModelSettings["model"],
                      }),
                    )
                  }
                  value={modelSettings.model}
                >
                  <option value="gpt-5.6-luna">Luna</option>
                  <option value="gpt-5.6-terra">Terra</option>
                  <option value="gpt-5.6-sol">Sol</option>
                </select>
                <select
                  aria-label="Reasoning effort"
                  className="h-7 rounded-md border border-slate-300 bg-white px-1.5 text-xs text-slate-700"
                  onChange={(event) =>
                    setModelSettings(
                      DemoModelSettings.make({
                        ...modelSettings,
                        reasoningEffort: event.target.value as DemoModelSettings["reasoningEffort"],
                      }),
                    )
                  }
                  value={modelSettings.reasoningEffort}
                >
                  <option value="none">No reasoning</option>
                  <option value="minimal">Minimal</option>
                  <option value="low">Low</option>
                  <option value="medium">Medium</option>
                  <option value="high">High</option>
                  <option value="xhigh">X-high</option>
                </select>
                <Button
                  onClick={() =>
                    setModelSettings(
                      DemoModelSettings.make({ ...modelSettings, fast: !modelSettings.fast }),
                    )
                  }
                  size="sm"
                  title="OpenAI priority processing"
                  type="button"
                  variant={modelSettings.fast ? "default" : "outline"}
                >
                  Fast
                </Button>
              </>
            ) : null}
            <Button onClick={reset} size="icon-sm" title="New chat" type="button" variant="ghost">
              <RotateCcw />
            </Button>
          </div>
        </div>

        <Card className="flex min-h-0 flex-1 flex-col overflow-hidden border-slate-300 bg-white/90 shadow-[0_20px_60px_-40px_rgb(15_23_42/0.4)] backdrop-blur-sm">
          <Conversation aria-label="Conversation">
            <ConversationContent className="mx-auto w-full max-w-3xl gap-6 px-4 py-7 sm:px-8">
              {state.messages.map((message) => {
                const activeAssistant =
                  message.role === "assistant" && message.id === activeAssistantId;
                const messageStatus = statusForMessage(message, activeAssistantId, state.status);
                return (
                  <Message from={message.role} key={message.id}>
                    <span className="font-mono text-[9px] tracking-[0.12em] text-muted-foreground uppercase">
                      {message.role === "assistant" ? "Agent" : "You"}
                    </span>
                    <MessageContent>
                      {message.reasoning === undefined || message.reasoning.length === 0 ? null : (
                        <Reasoning streaming={running && activeAssistant}>
                          {message.reasoning}
                        </Reasoning>
                      )}
                      {message.content.length === 0 ? null : (
                        <MessageResponse>{message.content}</MessageResponse>
                      )}
                      {message.role === "assistant" && message.experience === "capability" ? (
                        <CapabilityChatTrace
                          canDecide={
                            activeAssistant &&
                            state.activeExperience === "capability" &&
                            state.handle !== null
                          }
                          message={message}
                          mode={state.mode}
                          onResolveApproval={resolveApproval}
                          status={messageStatus}
                        />
                      ) : message.role === "assistant" ? (
                        <GeneralChatTrace
                          active={running && activeAssistant}
                          message={message}
                          mode={state.mode}
                        />
                      ) : null}
                    </MessageContent>
                  </Message>
                );
              })}

              {state.messages.length === 1 ? (
                <div className="grid gap-4 pt-2">
                  <div className="rounded-lg border border-cyan-200 bg-cyan-50/60 p-3 sm:p-4">
                    <div className="mb-3 flex items-start gap-2">
                      <FlaskConical className="mt-0.5 size-4 text-cyan-800" />
                      <div>
                        <p className="text-sm font-medium text-cyan-950">
                          OpenAI travel agent · repeatable demo inventory
                        </p>
                        <p className="mt-0.5 text-xs leading-5 text-cyan-800">
                          {state.mode === "openai"
                            ? "The live model researches real options with hosted web search and cites sources. Prices are public estimates; no purchase is possible."
                            : "Scripted replay is selected. It exercises the same runtime without a live model."}
                        </p>
                      </div>
                    </div>
                    <div className="grid gap-2 sm:grid-cols-3">
                      {primaryCapabilityRecipes.map((recipe) => (
                        <button
                          className="rounded-md border border-cyan-200 bg-white px-3 py-3 text-left transition-colors hover:border-cyan-400 hover:bg-cyan-50"
                          key={recipe.label}
                          onClick={() => launchRecipe(recipe)}
                          type="button"
                        >
                          <span className="block text-xs font-medium text-slate-900">
                            {recipe.label}
                          </span>
                          <span className="mt-1 block text-[10px] leading-4 text-slate-500">
                            {recipe.detail}
                          </span>
                        </button>
                      ))}
                    </div>
                    <Collapsible className="group mt-2">
                      <CollapsibleTrigger className="flex items-center gap-1.5 py-1 text-[11px] text-cyan-900">
                        More safety checks
                        <ChevronDown className="size-3 transition-transform group-data-[panel-open]:rotate-180" />
                      </CollapsibleTrigger>
                      <CollapsibleContent className="grid gap-2 pt-2 sm:grid-cols-3">
                        {secondaryCapabilityRecipes.map((recipe) => (
                          <button
                            className="rounded-md border border-cyan-200/80 bg-white/80 px-3 py-2 text-left text-xs hover:border-cyan-400"
                            key={recipe.label}
                            onClick={() => launchRecipe(recipe)}
                            type="button"
                          >
                            {recipe.label}
                            <span className="mt-0.5 block text-[10px] text-slate-500">
                              {recipe.detail}
                            </span>
                          </button>
                        ))}
                      </CollapsibleContent>
                    </Collapsible>
                  </div>
                  <div>
                    <p className="mb-2 font-mono text-[9px] tracking-[0.1em] text-muted-foreground uppercase">
                      Or write your own request
                    </p>
                    <div className="grid gap-2 sm:grid-cols-3">
                      {examples.map((example) => (
                        <button
                          className="rounded-md border border-border bg-muted/35 px-3 py-3 text-left text-xs leading-5 text-slate-600 transition-colors hover:border-cyan-300 hover:bg-cyan-50 hover:text-slate-900"
                          key={example}
                          onClick={() => setPrompt(example)}
                          type="button"
                        >
                          {example}
                        </button>
                      ))}
                    </div>
                  </div>
                </div>
              ) : null}

              {state.controlError === null ? null : (
                <div
                  className="rounded-md border border-amber-200 bg-amber-50 px-3 py-2 text-sm text-amber-950"
                  role="alert"
                >
                  {state.controlError}
                </div>
              )}

              {state.error === null ? null : (
                <div
                  className="rounded-md border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-900"
                  role="alert"
                >
                  {state.error}
                </div>
              )}
            </ConversationContent>
            <ConversationScrollButton />
          </Conversation>

          <div className="border-t border-border bg-slate-50/80 p-3 sm:p-4">
            <div className="mx-auto max-w-3xl">
              <PromptInput
                onSubmit={(event) => {
                  event.preventDefault();
                  submitPrompt();
                }}
              >
                <PromptInputTextarea
                  aria-label="Message"
                  disabled={running && !canQueue}
                  onChange={(event) => setPrompt(event.currentTarget.value)}
                  onKeyDown={(event) => {
                    if (event.key === "Enter" && !event.shiftKey) {
                      event.preventDefault();
                      submitPrompt();
                    }
                  }}
                  placeholder={
                    canQueue ? "Add a change while the agent works…" : "Message the travel agent…"
                  }
                  value={prompt}
                />
                <PromptInputFooter>
                  <div className="flex items-center gap-2">
                    <Sparkles className="size-3 text-cyan-700" />
                    <ToggleGroup
                      aria-label="Model profile"
                      disabled={running}
                      onValueChange={(modes) => {
                        const mode = modes[0];
                        if (mode !== undefined) selectMode(mode);
                      }}
                      value={[state.mode]}
                    >
                      <ToggleGroupItem value="deterministic">Scripted replay</ToggleGroupItem>
                      <ToggleGroupItem value="openai">OpenAI agent</ToggleGroupItem>
                    </ToggleGroup>
                  </div>
                  <div className="flex items-center gap-2">
                    {canQueue ? (
                      <>
                        <Button
                          onClick={() => queueUpdate({ content: quickUpdate })}
                          size="sm"
                          type="button"
                          variant="outline"
                        >
                          {hasQueuedSteering ? "Add room preference" : "Move to Sep 22"}
                        </Button>
                        <Button disabled={prompt.trim().length === 0} size="sm" type="submit">
                          Queue update
                        </Button>
                      </>
                    ) : null}
                    <PromptInputSubmit
                      disabled={!running && prompt.trim().length === 0}
                      onClick={() => {
                        if (running) {
                          run(Atom.Interrupt);
                          runCapability(Atom.Interrupt);
                        } else {
                          submitPrompt();
                        }
                      }}
                      status={submitStatus}
                    />
                  </div>
                </PromptInputFooter>
              </PromptInput>
              <p className="mt-2 text-center font-mono text-[9px] text-muted-foreground">
                {state.mode === "openai"
                  ? "Live OpenAI model · real web research · no real reservation"
                  : "Scripted replay · same schemas and event stream · no model call"}
              </p>
            </div>
          </div>
        </Card>
      </div>
    </div>
  );
}

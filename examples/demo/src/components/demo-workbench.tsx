"use client";

import { useAtomSet, useAtomValue } from "@effect/atom-react";
import type { ChatStatus } from "ai";
import { Schema } from "effect";
import * as Atom from "effect/unstable/reactivity/Atom";
import { Activity, RotateCcw } from "lucide-react";
import { useMemo, useState } from "react";

import { RunEvent as RunEventSchema, type RunEvent } from "@effect-agent/core";
import { AgentActivity } from "@/components/ai-elements/agent-activity";
import { CodeBlock } from "@/components/ai-elements/code-block";
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
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { ToggleGroup, ToggleGroupItem } from "@/components/ui/toggle-group";
import {
  demoStateAtom,
  initialDemoState,
  runDemoAtom,
  type DemoState,
  type DemoStatus,
} from "@/demo/state";
import { ChatOutput } from "@/demo/general-chat";
import { projectRunActivity } from "@/demo/run-activity";
import { projectToolTraces } from "@/demo/tool-trace";
import { cn } from "@/lib/utils";

const statusLabel: Record<DemoStatus, string> = {
  idle: "Ready",
  running: "Running",
  succeeded: "Complete",
  failed: "Failed",
  interrupted: "Stopped",
  suspended: "Waiting",
};

const eventJson = (event: RunEvent): string =>
  JSON.stringify(Schema.encodeSync(RunEventSchema)(event), null, 2);

function StatusMark({ status }: { readonly status: DemoStatus }) {
  return (
    <span className="inline-flex items-center gap-2 font-mono text-[11px] text-muted-foreground">
      <span
        className={cn(
          "size-1.5 rounded-full bg-muted-foreground",
          status === "running" && "animate-pulse bg-amber-500",
          status === "succeeded" && "bg-emerald-600",
          status === "failed" && "bg-destructive",
          status === "suspended" && "bg-amber-500",
        )}
      />
      {statusLabel[status]}
    </span>
  );
}

function EventTrace({
  events,
  selectedSequence,
  onSelect,
}: {
  readonly events: ReadonlyArray<RunEvent>;
  readonly selectedSequence: number | null;
  readonly onSelect: (sequence: number) => void;
}) {
  if (events.length === 0) {
    return (
      <div className="grid min-h-48 place-items-center px-6 text-center">
        <p className="max-w-56 font-mono text-[11px] leading-5 text-muted-foreground">
          Submit a request to populate the semantic event stream.
        </p>
      </div>
    );
  }

  return (
    <ol className="max-h-[29rem] overflow-y-auto py-1">
      {events.map((event) => (
        <li key={`${event.runId}-${event.sequence}`}>
          <button
            className={cn(
              "grid w-full grid-cols-[2.25rem_1fr_auto] items-center gap-2 border-l-2 border-transparent px-3 py-2 text-left hover:bg-muted/60",
              selectedSequence === event.sequence && "border-foreground bg-muted",
            )}
            onClick={() => onSelect(event.sequence)}
            type="button"
          >
            <span className="font-mono text-[10px] text-muted-foreground">
              {String(event.sequence).padStart(2, "0")}
            </span>
            <span className="truncate font-mono text-[11px]">{event._tag}</span>
            {"turn" in event && typeof event.turn === "number" ? (
              <span className="font-mono text-[10px] text-muted-foreground">turn {event.turn}</span>
            ) : null}
          </button>
        </li>
      ))}
    </ol>
  );
}

/** Interactive general chat bench backed by fixture or live Tool profiles. */
export function DemoWorkbench() {
  const state = useAtomValue(demoStateAtom);
  const setState = useAtomSet(demoStateAtom);
  const run = useAtomSet(runDemoAtom);
  const [prompt, setPrompt] = useState(initialDemoState.activeMessage);
  const [selectedSequence, setSelectedSequence] = useState<number | null>(null);

  const selectedEvent =
    state.events.find((event) => event.sequence === selectedSequence) ?? state.events.at(-1);
  const toolTraces = useMemo(() => projectToolTraces(state.events), [state.events]);
  const runActivity = useMemo(
    () => projectRunActivity(state.events, state.mode),
    [state.events, state.mode],
  );
  const activeAssistantId = `assistant-${state.runNumber}`;
  const hasActiveAssistant = state.messages.some((message) => message.id === activeAssistantId);
  const chatStatus: ChatStatus =
    state.status === "running" ? "streaming" : state.status === "failed" ? "error" : "ready";

  const encodedOutput = useMemo(
    () =>
      state.output === null
        ? null
        : JSON.stringify(Schema.encodeSync(ChatOutput)(state.output), null, 2),
    [state.output],
  );

  const reset = () => {
    const mode = state.mode;
    run(Atom.Interrupt);
    setState({ ...initialDemoState, mode });
    setPrompt(initialDemoState.activeMessage);
    setSelectedSequence(null);
  };

  const selectMode = (mode: DemoState["mode"]) => {
    if (mode === state.mode) {
      return;
    }
    const wasRunning = state.status === "running";
    setState({
      ...state,
      mode,
      status: wasRunning ? "interrupted" : state.status,
    });
    if (wasRunning) {
      run(Atom.Interrupt);
    }
  };

  return (
    <main className="min-h-svh bg-background">
      <header className="border-b border-border bg-background">
        <div className="mx-auto flex max-w-[1440px] items-center justify-between gap-4 px-4 py-3 lg:px-6">
          <div className="flex min-w-0 items-center gap-3">
            <div className="grid size-7 place-items-center rounded-md border border-border bg-muted">
              <Activity className="size-3.5" />
            </div>
            <div className="min-w-0">
              <h1 className="truncate text-sm font-semibold">Effect Agent · chat bench</h1>
              <p className="truncate font-mono text-[10px] text-muted-foreground">
                General chat /{" "}
                {state.mode === "openai"
                  ? "gpt-5.6-luna / live tools"
                  : "scripted model / offline tools"}
              </p>
            </div>
          </div>
          <div className="flex items-center gap-3">
            <ToggleGroup
              aria-label="Model profile"
              onValueChange={(modes) => {
                const mode = modes[0];
                if (mode !== undefined) {
                  selectMode(mode);
                }
              }}
              value={[state.mode]}
            >
              <ToggleGroupItem value="deterministic">Offline fixtures</ToggleGroupItem>
              <ToggleGroupItem value="openai">OpenAI live</ToggleGroupItem>
            </ToggleGroup>
            <StatusMark status={state.status} />
            <Button onClick={reset} size="sm" type="button" variant="outline">
              <RotateCcw />
              Reset
            </Button>
          </div>
        </div>
      </header>

      <div className="mx-auto grid max-w-[1440px] gap-4 p-4 lg:grid-cols-[minmax(0,1.15fr)_minmax(25rem,0.85fr)] lg:p-6">
        <Card className="flex min-h-[42rem] flex-col overflow-hidden lg:h-[calc(100svh-6.75rem)]">
          <CardHeader className="flex items-center justify-between">
            <div>
              <h2 className="text-sm font-medium">Conversation</h2>
              <p className="mt-0.5 text-xs text-muted-foreground">
                Your text is Schema-validated agent input.
              </p>
            </div>
            <Badge>{state.mode === "openai" ? "Live tools" : "Deterministic"}</Badge>
          </CardHeader>

          <Conversation>
            <ConversationContent>
              {state.messages.map((message) => (
                <Message from={message.role} key={message.id}>
                  <span className="font-mono text-[10px] tracking-[0.08em] text-muted-foreground uppercase">
                    {message.role}
                  </span>
                  <MessageContent>
                    {message.reasoning === undefined || message.reasoning.length === 0 ? null : (
                      <Reasoning
                        streaming={
                          state.status === "running" &&
                          message.id === `assistant-${state.runNumber}`
                        }
                      >
                        {message.reasoning}
                      </Reasoning>
                    )}
                    {message.content.length === 0 ? null : (
                      <MessageResponse>{message.content}</MessageResponse>
                    )}
                    {state.status === "running" && message.id === activeAssistantId ? (
                      <AgentActivity {...runActivity} />
                    ) : null}
                  </MessageContent>
                </Message>
              ))}
              {state.status === "running" && !hasActiveAssistant ? (
                <Message from="assistant">
                  <span className="font-mono text-[10px] text-muted-foreground uppercase">
                    agent
                  </span>
                  <MessageContent>
                    <AgentActivity {...runActivity} />
                  </MessageContent>
                </Message>
              ) : null}
            </ConversationContent>
            <ConversationScrollButton />
          </Conversation>

          <div className="border-t border-border bg-muted/30 p-3">
            <PromptInput
              onSubmit={(event) => {
                event.preventDefault();
                const message = prompt.trim();
                if (message.length > 0 && state.status !== "running") {
                  run({ mode: state.mode, message });
                  setSelectedSequence(null);
                }
              }}
            >
              <PromptInputTextarea
                aria-label="Chat message"
                disabled={state.status === "running"}
                onChange={(event) => setPrompt(event.currentTarget.value)}
                placeholder="Ask anything…"
                value={prompt}
              />
              <PromptInputFooter>
                <span className="font-mono text-[10px] text-muted-foreground">
                  ENTER run · SHIFT+ENTER newline
                </span>
                <PromptInputSubmit
                  onClick={
                    state.status === "running"
                      ? () => {
                          run(Atom.Interrupt);
                        }
                      : undefined
                  }
                  status={chatStatus}
                />
              </PromptInputFooter>
            </PromptInput>
          </div>
        </Card>

        <div className="grid min-w-0 content-start gap-4 lg:max-h-[calc(100svh-6.75rem)] lg:grid-rows-[auto_minmax(0,1fr)]">
          <Card>
            <CardHeader className="flex items-center justify-between">
              <div>
                <h2 className="text-sm font-medium">Tool execution</h2>
                <p className="mt-0.5 text-xs text-muted-foreground">
                  {state.mode === "openai"
                    ? "Real arithmetic and OpenAI-hosted research."
                    : "Deterministic offline knowledge and arithmetic."}
                </p>
              </div>
              <Badge>{state.mode === "openai" ? "6 call max" : "2 call max"}</Badge>
            </CardHeader>
            <CardContent className="grid gap-2">
              {toolTraces.length === 0 ? (
                <p className="rounded-md border border-dashed border-border px-3 py-5 text-center font-mono text-[11px] text-muted-foreground">
                  Tool Calls will appear here during a run.
                </p>
              ) : (
                toolTraces.map((trace) => (
                  <Tool defaultOpen key={trace.toolCallId}>
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
                      {trace.failure === undefined ? null : (
                        <ToolData label="Failure" value={trace.failure} />
                      )}
                    </ToolContent>
                  </Tool>
                ))
              )}
            </CardContent>
          </Card>

          <Card className="min-h-[27rem] overflow-hidden">
            <Tabs defaultValue="events">
              <TabsList>
                <TabsTrigger value="events">Events · {state.events.length}</TabsTrigger>
                <TabsTrigger value="json">Selected JSON</TabsTrigger>
                <TabsTrigger value="output">Output</TabsTrigger>
              </TabsList>
              <TabsContent value="events">
                <EventTrace
                  events={state.events}
                  onSelect={setSelectedSequence}
                  selectedSequence={selectedEvent?.sequence ?? null}
                />
              </TabsContent>
              <TabsContent value="json">
                {selectedEvent === undefined ? (
                  <p className="p-6 font-mono text-[11px] text-muted-foreground">
                    No event selected.
                  </p>
                ) : (
                  <CodeBlock className="max-h-[29rem]" code={eventJson(selectedEvent)} />
                )}
              </TabsContent>
              <TabsContent value="output">
                {encodedOutput === null ? (
                  <p className="p-6 font-mono text-[11px] leading-5 text-muted-foreground">
                    The Schema-decoded `ChatOutput` will appear here after RunCompleted.
                  </p>
                ) : (
                  <CodeBlock className="max-h-[29rem]" code={encodedOutput} />
                )}
              </TabsContent>
            </Tabs>
            {state.error === null ? null : (
              <p className="border-t border-destructive/30 bg-destructive/5 px-4 py-3 font-mono text-xs text-destructive">
                {state.error}
              </p>
            )}
          </Card>
        </div>
      </div>
    </main>
  );
}

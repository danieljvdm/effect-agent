"use client";

import { RunEvent } from "@effect-agent/core";
import { Schema } from "effect";
import {
  AlertTriangle,
  Check,
  ChevronDown,
  CircleDollarSign,
  Database,
  GitMerge,
  Network,
  ShieldAlert,
  TerminalSquare,
} from "lucide-react";
import { useMemo } from "react";

import { AgentActivity } from "@/components/ai-elements/agent-activity";
import { Tool, ToolContent, ToolData, ToolHeader } from "@/components/ai-elements/tool";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import type { ChatMessage, ChatState, ChatStatus, ResolveChatApproval } from "@/demo/chat-state";
import type { DemoOperationalEvent } from "@/demo/operational-contracts";
import { projectRunActivity } from "@/demo/run-activity";
import { projectToolTraces } from "@/demo/tool-trace";

const toolLabels: Readonly<Record<string, string>> = {
  search_flights: "Searching flights",
  search_lodging: "Searching stays",
  search_activities: "Finding activities",
  hold_itinerary: "Placing itinerary hold",
};

const runEventsFrom = (events: ReadonlyArray<DemoOperationalEvent>) =>
  events.filter((event): event is RunEvent => Schema.is(RunEvent)(event));

const latestByCommand = (events: ReadonlyArray<DemoOperationalEvent>) => {
  const commands = new Map<
    string,
    Extract<DemoOperationalEvent, { readonly _tag: "DemoCommandStateChanged" }>
  >();

  for (const event of events) {
    if (event._tag === "DemoCommandStateChanged") commands.set(event.commandId, event);
  }

  return Array.from(commands.values());
};

const commandStatus = {
  queued: "Queued",
  claimed: "Waiting for the safe seam",
  delivered: "Applied",
} as const;

const readableToolOrder = (
  ids: ReadonlyArray<string>,
  events: ReadonlyArray<DemoOperationalEvent>,
): string =>
  ids
    .map((id) => {
      const declaration = events.find(
        (event) => event._tag === "ToolCallDeclared" && event.toolCallId === id,
      );

      return declaration?._tag === "ToolCallDeclared"
        ? (toolLabels[declaration.toolName] ?? declaration.toolName).replace(/ing /, "")
        : id;
    })
    .join(" → ");

export function CapabilityChatTrace({
  canDecide,
  message,
  mode,
  onResolveApproval,
  status,
}: {
  readonly canDecide: boolean;
  readonly message: ChatMessage;
  readonly mode: ChatState["mode"];
  readonly onResolveApproval: (request: ResolveChatApproval) => void;
  readonly status: ChatStatus;
}) {
  const events = message.events ?? [];
  const runEvents = useMemo(() => runEventsFrom(events), [events]);
  const tools = useMemo(() => projectToolTraces(runEvents), [runEvents]);
  const activity = useMemo(() => projectRunActivity(runEvents, mode), [mode, runEvents]);
  const commands = useMemo(() => latestByCommand(events), [events]);
  const batch = events.findLast((event) => event._tag === "DemoToolBatchCommitted");
  const approval = events.findLast((event) => event._tag === "DemoApprovalPending");

  const approvalSettlement =
    approval?._tag === "DemoApprovalPending"
      ? events.findLast(
          (event) =>
            event._tag === "DemoApprovalSettled" && event.requestId === approval.request.requestId,
        )
      : undefined;

  const rejection = events.findLast((event) => event._tag === "DemoBudgetRejected");

  const context = events.findLast(
    (event) => event._tag === "DemoContextPrepared" && event.compacted,
  );

  const mcp = events.findLast((event) => event._tag === "DemoMcpConnected");

  const sandboxStarted = events.findLast(
    (event) => event._tag === "DemoSandboxObserved" && event.event._tag === "SandboxStarted",
  );

  const sandboxExited = events.findLast(
    (event) => event._tag === "DemoSandboxObserved" && event.event._tag === "SandboxExited",
  );

  const active = status === "running" && message.content.length === 0;

  return (
    <div className="grid gap-3">
      {active ? <AgentActivity {...activity} /> : null}

      {tools.length === 0 ? null : (
        <div className="grid gap-2">
          {tools.map((tool) => (
            <Tool key={tool.toolCallId}>
              <ToolHeader
                execution={tool.providerExecuted ? "provider" : "application"}
                executionLabel={tool.providerExecuted ? "OpenAI hosted" : "Fixture tool"}
                name={toolLabels[tool.toolName] ?? tool.toolName}
                state={tool.state}
              />
              <ToolContent className="grid gap-3">
                <ToolData label="Parameters" value={tool.parameters} />
                {tool.result === undefined ? null : (
                  <ToolData label="Deterministic result" value={tool.result} />
                )}
              </ToolContent>
            </Tool>
          ))}
        </div>
      )}

      {commands.length === 0 ? null : (
        <div className="grid gap-2 rounded-md border border-cyan-200 bg-cyan-50/70 p-3">
          <div className="flex items-center gap-2 text-xs font-medium text-cyan-950">
            <GitMerge className="size-3.5" />
            Messages received while work was in flight
          </div>
          {commands.map((command) => (
            <div
              className="flex flex-wrap items-start justify-between gap-2 border-t border-cyan-200/70 pt-2 first:border-0 first:pt-0"
              key={command.commandId}
            >
              <p className="max-w-xl text-xs leading-5 text-cyan-950">{command.content}</p>
              <Badge className="border-cyan-300 bg-white text-cyan-900">
                {commandStatus[command.status]}
              </Badge>
            </div>
          ))}
        </div>
      )}

      {batch?._tag === "DemoToolBatchCommitted" ? (
        <Collapsible className="group rounded-md border border-emerald-200 bg-emerald-50/70">
          <CollapsibleTrigger className="flex w-full items-center justify-between gap-3 px-3 py-2.5 text-left">
            <span className="flex items-center gap-2 text-xs font-medium text-emerald-950">
              <Check className="size-3.5" />
              {batch.declaredOrder.length} tools finished · results committed in request order
            </span>
            <ChevronDown className="size-3.5 text-emerald-800 transition-transform group-data-[panel-open]:rotate-180" />
          </CollapsibleTrigger>
          <CollapsibleContent className="grid gap-2 border-t border-emerald-200 px-3 py-3 font-mono text-[10px] leading-5 text-emerald-950">
            <p>Finished: {readableToolOrder(batch.completionOrder, events)}</p>
            <p>Committed: {readableToolOrder(batch.declaredOrder, events)}</p>
          </CollapsibleContent>
        </Collapsible>
      ) : null}

      {approval?._tag === "DemoApprovalPending" ? (
        <div
          className="rounded-md border border-amber-300 bg-amber-50 p-4 text-amber-950"
          role="group"
          aria-label="Approval required"
        >
          <div className="flex items-start gap-3">
            <ShieldAlert className="mt-0.5 size-4 shrink-0" />
            <div className="min-w-0 flex-1">
              <p className="text-sm font-semibold">Place a 15-minute demo hold?</p>
              <p className="mt-1 text-xs leading-5">
                {approval.request.actionSummary} This uses fixture data and creates no real
                reservation. The handler has not started.
              </p>
              {approvalSettlement?._tag === "DemoApprovalSettled" ? (
                <Badge className="mt-3 border-amber-300 bg-white text-amber-950">
                  {approvalSettlement.choice === "approve" ? "Approved" : "Denied"}
                </Badge>
              ) : (
                <div className="mt-3 flex flex-wrap gap-2">
                  <Button
                    disabled={!canDecide}
                    onClick={() =>
                      onResolveApproval({
                        requestId: approval.request.requestId,
                        choice: "deny",
                      })
                    }
                    size="sm"
                    type="button"
                    variant="outline"
                  >
                    Don&apos;t hold
                  </Button>
                  <Button
                    disabled={!canDecide}
                    onClick={() =>
                      onResolveApproval({
                        requestId: approval.request.requestId,
                        choice: "approve",
                      })
                    }
                    size="sm"
                    type="button"
                  >
                    Place demo hold
                  </Button>
                </div>
              )}
            </div>
          </div>
        </div>
      ) : null}

      {rejection?._tag === "DemoBudgetRejected" ? (
        <div className="flex gap-3 rounded-md border border-rose-200 bg-rose-50 p-3 text-rose-950">
          <CircleDollarSign className="mt-0.5 size-4 shrink-0" />
          <div>
            <p className="text-xs font-semibold">
              The budget stopped the run before the next action.
            </p>
            <p className="mt-1 font-mono text-[10px]">
              {rejection.limit}: {rejection.observedValue} requested / {rejection.limitValue}{" "}
              allowed
            </p>
          </div>
        </div>
      ) : null}

      {context?._tag === "DemoContextPrepared" ||
      mcp?._tag === "DemoMcpConnected" ||
      sandboxStarted?._tag === "DemoSandboxObserved" ? (
        <Collapsible className="group rounded-md border border-slate-200 bg-slate-50/80">
          <CollapsibleTrigger className="flex w-full items-center justify-between gap-3 px-3 py-2.5 text-left">
            <span className="flex items-center gap-2 text-xs font-medium text-slate-800">
              <Database className="size-3.5" />
              Runtime checks
              <Badge>History kept · MCP bounded · network off</Badge>
            </span>
            <ChevronDown className="size-3.5 text-slate-500 transition-transform group-data-[panel-open]:rotate-180" />
          </CollapsibleTrigger>
          <CollapsibleContent className="grid gap-3 border-t border-slate-200 px-3 py-3 text-xs text-slate-700">
            {context?._tag === "DemoContextPrepared" ? (
              <div className="flex gap-2">
                <Database className="mt-0.5 size-3.5 shrink-0" />
                <p>
                  Context was compacted for the model ({context.modelMessageCount} messages) while
                  official history remained intact ({context.officialMessageCount} messages).
                </p>
              </div>
            ) : null}
            {mcp?._tag === "DemoMcpConnected" ? (
              <div className="flex gap-2">
                <Network className="mt-0.5 size-3.5 shrink-0" />
                <p>
                  MCP discovery accepted {mcp.toolCount} of {mcp.maxToolCount} allowed tools and{" "}
                  {mcp.encodedBytes} of {mcp.maxDiscoveryBytes} allowed bytes.
                </p>
              </div>
            ) : null}
            {sandboxStarted?._tag === "DemoSandboxObserved" ? (
              <div className="flex gap-2">
                <TerminalSquare className="mt-0.5 size-3.5 shrink-0" />
                <p>
                  The trusted local check ran with network disabled and bounded output. This adapter
                  is explicitly unisolated, not a security sandbox.
                  {sandboxExited?._tag === "DemoSandboxObserved" &&
                  sandboxExited.event._tag === "SandboxExited"
                    ? ` Exit ${sandboxExited.event.exitCode}.`
                    : ""}
                </p>
              </div>
            ) : null}
          </CollapsibleContent>
        </Collapsible>
      ) : null}

      {status === "failed" && rejection === undefined && approval === undefined ? (
        <div className="flex gap-2 rounded-md border border-rose-200 bg-rose-50 p-3 text-xs text-rose-950">
          <AlertTriangle className="size-3.5 shrink-0" />
          The runtime experiment stopped without completing.
        </div>
      ) : null}
    </div>
  );
}

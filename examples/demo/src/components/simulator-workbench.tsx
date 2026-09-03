"use client";

import { type RunEvent } from "@effect-agent/core/RunEvent";
import { useAtomSet, useAtomValue } from "@effect/atom-react";
import { Schema } from "effect";
import * as Atom from "effect/unstable/reactivity/Atom";
import {
  Activity,
  ArrowRight,
  Ban,
  Braces,
  Check,
  ChevronRight,
  CircleDollarSign,
  Clock3,
  Database,
  Gauge,
  GitCommitHorizontal,
  Layers3,
  LockKeyhole,
  Network,
  Play,
  Radio,
  RotateCcw,
  Send,
  ShieldCheck,
  SquareTerminal,
  TimerReset,
} from "lucide-react";
import { useMemo, useState, type ComponentType, type ReactNode } from "react";

import { CodeBlock } from "@/components/ai-elements/code-block";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import {
  DemoOperationalEvent,
  type DemoOperationalEvent as DemoOperationalEventValue,
  type DemoScenario,
} from "@/demo/operational-contracts";
import {
  demoStateAtom,
  initialDemoState,
  queueDemoCommandAtom,
  resolveDemoApprovalAtom,
  runOperationalDemoAtom,
  type DemoStatus,
} from "@/demo/state";
import { projectToolTraces } from "@/demo/tool-trace";
import { cn } from "@/lib/utils";

const statusLabel: Record<DemoStatus, string> = {
  idle: "STANDBY",
  running: "RUN ACTIVE",
  succeeded: "RUN COMPLETE",
  failed: "LIMIT / FAILURE",
  interrupted: "RUN STOPPED",
  suspended: "AWAITING DECISION",
};

const scenarios: ReadonlyArray<{
  readonly id: DemoScenario;
  readonly label: string;
  readonly detail: string;
}> = [
  {
    id: "guided",
    label: "Guided control run",
    detail: "Parallel tools, steering, follow-up, compaction, MCP, and local command",
  },
  {
    id: "hold",
    label: "Risky itinerary hold",
    detail: "Pauses before the handler and waits for your explicit decision",
  },
  {
    id: "budget-tokens",
    label: "Token fuse",
    detail: "Input allowance: 1 token",
  },
  {
    id: "budget-tools",
    label: "Tool-call fuse",
    detail: "Allowance: 1 of 3 declared calls",
  },
  {
    id: "budget-cost",
    label: "Spend fuse",
    detail: "Allowance: 100 μUSD",
  },
  {
    id: "budget-duration",
    label: "Time fuse",
    detail: "Deadline: 10 ms",
  },
  {
    id: "tool-defect",
    label: "Tool handler defect",
    detail: "A dying handler still ends the stream with a typed failure",
  },
];

const isRunEvent = (event: DemoOperationalEventValue): event is RunEvent =>
  "eventVersion" in event && "sequence" in event && "runId" in event && "threadId" in event;

const encodedEvent = (event: DemoOperationalEventValue): string =>
  JSON.stringify(Schema.encodeSync(DemoOperationalEvent)(event), null, 2);

const formatMoney = (cents: number): string =>
  new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0,
  }).format(cents / 100);

const shortTool = (name: string): string => name.replace("search_", "").replaceAll("_", " ");

function StatusMark({ status }: { readonly status: DemoStatus }) {
  return (
    <div className="flex items-center gap-2 font-mono text-[10px] tracking-[0.12em]">
      <span
        className={cn(
          "relative block size-2 rounded-full bg-slate-400",
          status === "running" && "bg-cyan-400",
          status === "suspended" && "bg-amber-400",
          status === "succeeded" && "bg-emerald-400",
          status === "failed" && "bg-rose-400",
        )}
      >
        {status === "running" ? (
          <span className="absolute inset-0 animate-ping rounded-full bg-cyan-400" />
        ) : null}
      </span>
      {statusLabel[status]}
    </div>
  );
}

function SectionLabel({
  icon: Icon,
  children,
}: {
  readonly icon: ComponentType<{ readonly className?: string }>;
  readonly children: ReactNode;
}) {
  return (
    <div className="flex items-center gap-2 font-mono text-[10px] font-medium tracking-[0.16em] text-muted-foreground uppercase">
      <Icon className="size-3.5" />
      {children}
    </div>
  );
}

function EmptyEvidence({ children }: { readonly children: ReactNode }) {
  return (
    <div className="grid min-h-28 place-items-center rounded-md border border-dashed border-border bg-muted/20 px-5 text-center font-mono text-[10px] leading-5 text-muted-foreground">
      {children}
    </div>
  );
}

/** Phase 2 operations board: controls and evidence are projections of server events. */
export function SimulatorWorkbench() {
  const state = useAtomValue(demoStateAtom);
  const setState = useAtomSet(demoStateAtom);
  const run = useAtomSet(runOperationalDemoAtom);
  const queueCommand = useAtomSet(queueDemoCommandAtom);
  const resolveApproval = useAtomSet(resolveDemoApprovalAtom);
  const [command, setCommand] = useState("Move departure to 2026-09-22.");
  const [commandKind, setCommandKind] = useState<"steering" | "follow-up">("steering");
  const [selectedEventIndex, setSelectedEventIndex] = useState<number | null>(null);

  const active = state.status === "running" || state.status === "suspended";

  const selectedEvent =
    (selectedEventIndex === null ? undefined : state.events[selectedEventIndex]) ??
    state.events.at(-1);

  const runEvents = useMemo(() => state.events.filter(isRunEvent), [state.events]);
  const toolTraces = useMemo(() => projectToolTraces(runEvents), [runEvents]);
  const commands = state.events.filter((event) => event._tag === "DemoCommandStateChanged");
  const contextEvents = state.events.filter((event) => event._tag === "DemoContextPrepared");
  const latestContext = contextEvents.at(-1);
  const latestBudget = state.events.filter((event) => event._tag === "DemoBudgetChanged").at(-1);
  const budgetRejected = state.events.filter((event) => event._tag === "DemoBudgetRejected").at(-1);

  const committedBatch = state.events
    .filter((event) => event._tag === "DemoToolBatchCommitted")
    .at(-1);

  const mcp = state.events.filter((event) => event._tag === "DemoMcpConnected").at(-1);
  const sandboxEvents = state.events.filter((event) => event._tag === "DemoSandboxObserved");
  const sandboxStarted = sandboxEvents.find((event) => event.event._tag === "SandboxStarted");
  const sandboxOutput = sandboxEvents.find((event) => event.event._tag === "SandboxOutput");
  const sandboxExited = sandboxEvents.find((event) => event.event._tag === "SandboxExited");
  const holdState = state.events.filter((event) => event._tag === "DemoHoldHandlerState").at(-1);

  const settlements = new Set(
    state.events
      .filter((event) => event._tag === "DemoApprovalSettled")
      .map((event) => event.requestId),
  );

  const pendingApproval = state.events
    .filter((event) => event._tag === "DemoApprovalPending")
    .find((event) => !settlements.has(event.request.requestId));

  const declaredTools = runEvents.filter((event) => event._tag === "ToolCallDeclared");
  const completedTools = runEvents.filter((event) => event._tag === "ToolCallSucceeded");
  const itinerary = state.output?.itineraries[0];

  const budgetMeters =
    latestBudget === undefined
      ? []
      : [
          {
            label: "input tokens",
            used: latestBudget.totals.inputTokens,
            limit: latestBudget.limits.maxInputTokens,
            icon: Braces,
          },
          {
            label: "tool calls",
            used: latestBudget.totals.toolCalls,
            limit: latestBudget.limits.maxToolCalls,
            icon: Activity,
          },
          {
            label: "cost μUSD",
            used: latestBudget.totals.costMicrousd,
            limit: latestBudget.limits.maxCostMicrousd,
            icon: CircleDollarSign,
          },
          {
            label: "elapsed ms",
            used: latestBudget.totals.elapsedMillis,
            limit: latestBudget.limits.maxDurationMillis,
            icon: Clock3,
          },
        ];

  const launch = (scenario: DemoScenario) => {
    run(scenario);
    setSelectedEventIndex(null);
  };

  const reset = () => {
    run(Atom.Interrupt);
    setState({
      ...initialDemoState,
      runNumber: state.runNumber,
      scenario: state.scenario,
    });
    setSelectedEventIndex(null);
  };

  return (
    <div className="min-h-svh bg-[radial-gradient(circle_at_80%_-10%,oklch(0.93_0.035_210),transparent_34%),linear-gradient(to_bottom,oklch(0.985_0.002_85),oklch(0.965_0.008_210))]">
      <header className="border-b border-slate-900/10 bg-slate-950 text-slate-100 shadow-sm">
        <div className="mx-auto flex max-w-[1600px] items-center justify-between gap-5 px-4 py-3 lg:px-7">
          <div className="flex min-w-0 items-center gap-3">
            <div className="grid size-8 place-items-center rounded-sm border border-cyan-300/30 bg-cyan-300/10">
              <ShieldCheck className="size-4 text-cyan-300" />
            </div>
            <div className="min-w-0">
              <h1 className="truncate text-sm font-semibold tracking-tight">Simulator</h1>
              <p className="truncate font-mono text-[9px] tracking-[0.12em] text-slate-400 uppercase">
                Ephemeral travel planner / deterministic operations bench
              </p>
            </div>
          </div>
          <div className="flex items-center gap-4">
            <StatusMark status={state.status} />
            <Button
              className="border-slate-700 bg-slate-900 text-slate-200 hover:bg-slate-800"
              onClick={reset}
              size="sm"
              type="button"
              variant="outline"
            >
              <RotateCcw />
              Reset
            </Button>
          </div>
        </div>
      </header>

      <div className="mx-auto max-w-[1600px] px-4 py-5 lg:px-7">
        <section className="mb-5 grid gap-4 border-b border-slate-900/10 pb-5 lg:grid-cols-[1fr_auto] lg:items-end">
          <div>
            <Badge className="mb-3 border-cyan-700/20 bg-cyan-600/10 text-cyan-800">
              P2 / controlled execution
            </Badge>
            <h2 className="max-w-4xl text-3xl font-semibold tracking-[-0.035em] text-slate-950 md:text-4xl">
              Watch control arrive while work is in flight—
              <span className="text-slate-500"> then land at a safe seam.</span>
            </h2>
          </div>
          <div className="grid grid-cols-3 gap-px overflow-hidden rounded-md border border-border bg-border text-center">
            {[
              ["run", state.runNumber === 0 ? "—" : String(state.runNumber).padStart(2, "0")],
              ["events", String(state.events.length).padStart(2, "0")],
              ["class", "E"],
            ].map(([label, value]) => (
              <div className="min-w-20 bg-card px-3 py-2" key={label}>
                <p className="font-mono text-[9px] tracking-[0.14em] text-muted-foreground uppercase">
                  {label}
                </p>
                <p className="mt-1 font-mono text-sm font-medium">{value}</p>
              </div>
            ))}
          </div>
        </section>

        <div className="grid gap-4 xl:grid-cols-[17rem_minmax(0,1fr)_23rem]">
          <aside className="grid content-start gap-4">
            <Card className="overflow-hidden">
              <CardHeader>
                <SectionLabel icon={Radio}>Scenario deck</SectionLabel>
              </CardHeader>
              <div className="divide-y divide-border">
                {scenarios.map((scenario, index) => (
                  <button
                    className={cn(
                      "group grid w-full grid-cols-[1.6rem_1fr_auto] gap-2 px-3 py-3 text-left transition-colors hover:bg-cyan-50",
                      state.scenario === scenario.id && state.runNumber > 0 && "bg-cyan-50/70",
                    )}
                    disabled={active}
                    key={scenario.id}
                    onClick={() => launch(scenario.id)}
                    type="button"
                  >
                    <span className="font-mono text-[10px] text-muted-foreground">
                      {String(index + 1).padStart(2, "0")}
                    </span>
                    <span>
                      <span className="block text-xs font-medium text-slate-900">
                        {scenario.label}
                      </span>
                      <span className="mt-1 block text-[10px] leading-4 text-muted-foreground">
                        {scenario.detail}
                      </span>
                    </span>
                    <Play className="mt-0.5 size-3 text-slate-400 group-hover:text-cyan-700" />
                  </button>
                ))}
              </div>
            </Card>

            <Card>
              <CardHeader>
                <SectionLabel icon={Send}>Live input queue</SectionLabel>
              </CardHeader>
              <CardContent className="grid gap-3">
                <div className="grid grid-cols-2 rounded-md border border-border bg-muted p-0.5">
                  {(["steering", "follow-up"] as const).map((kind) => (
                    <button
                      className={cn(
                        "rounded-sm px-2 py-1.5 font-mono text-[9px] tracking-[0.08em] uppercase",
                        commandKind === kind && "bg-slate-900 text-white shadow-sm",
                      )}
                      key={kind}
                      onClick={() => setCommandKind(kind)}
                      type="button"
                    >
                      {kind}
                    </button>
                  ))}
                </div>
                <textarea
                  aria-label="Queued command"
                  className="min-h-20 resize-none rounded-md border border-input bg-background px-3 py-2 text-xs leading-5 outline-none focus:border-cyan-600 focus:ring-2 focus:ring-cyan-500/15"
                  onChange={(event) => setCommand(event.currentTarget.value)}
                  value={command}
                />
                <Button
                  disabled={!active || state.handle === null || command.trim().length === 0}
                  onClick={() =>
                    queueCommand({
                      kind: commandKind,
                      content: command.trim(),
                    })
                  }
                  size="sm"
                  type="button"
                >
                  Queue without interrupting
                  <ArrowRight />
                </Button>
                <p className="font-mono text-[9px] leading-4 text-muted-foreground">
                  Steering → after Tool batch. Follow-up → when the Run would otherwise stop.
                </p>
              </CardContent>
            </Card>
          </aside>

          <section className="grid min-w-0 content-start gap-4">
            <Card className="overflow-hidden border-slate-300">
              <CardHeader className="flex items-center justify-between bg-slate-950 text-white">
                <SectionLabel icon={GitCommitHorizontal}>Deterministic commit lane</SectionLabel>
                <Badge className="border-slate-700 bg-slate-900 text-slate-300">
                  concurrency 3
                </Badge>
              </CardHeader>
              <CardContent className="grid gap-5">
                {declaredTools.length === 0 ? (
                  <EmptyEvidence>
                    Launch the guided run. Three independent searches will start together and finish
                    in reverse order.
                  </EmptyEvidence>
                ) : (
                  <>
                    <div className="grid gap-2 md:grid-cols-3">
                      {declaredTools.map((declared, index) => {
                        const trace = toolTraces.find(
                          (candidate) => candidate.toolCallId === declared.toolCallId,
                        );

                        const completionIndex = completedTools.findIndex(
                          (completed) => completed.toolCallId === declared.toolCallId,
                        );

                        return (
                          <div
                            className="relative overflow-hidden rounded-md border border-slate-200 bg-slate-50 p-3"
                            key={declared.toolCallId}
                          >
                            <div className="mb-5 flex items-center justify-between">
                              <span className="font-mono text-[9px] text-muted-foreground">
                                DECLARED {index + 1}
                              </span>
                              <span
                                className={cn(
                                  "size-2 rounded-full bg-amber-400",
                                  trace?.state === "output-available" && "bg-emerald-500",
                                )}
                              />
                            </div>
                            <p className="text-sm font-medium capitalize">
                              {shortTool(declared.toolName)}
                            </p>
                            <p className="mt-1 font-mono text-[9px] text-muted-foreground">
                              {completionIndex === -1
                                ? "executing…"
                                : `completed ${completionIndex + 1}`}
                            </p>
                            <div
                              className={cn(
                                "absolute inset-x-0 bottom-0 h-0.5 bg-amber-400",
                                trace?.state === "output-available" && "bg-emerald-500",
                              )}
                            />
                          </div>
                        );
                      })}
                    </div>
                    <div className="grid items-center gap-3 rounded-md border border-border bg-muted/30 p-3 md:grid-cols-[1fr_auto_1fr]">
                      <div>
                        <p className="font-mono text-[9px] tracking-[0.12em] text-muted-foreground uppercase">
                          Actual completion
                        </p>
                        <p className="mt-1 text-xs">
                          {(
                            committedBatch?.completionOrder ??
                            completedTools.map((event) => event.toolCallId)
                          )
                            .map((id) => shortTool(String(id).replace("-call-1", "")))
                            .join(" → ") || "settling…"}
                        </p>
                      </div>
                      <ChevronRight className="hidden size-4 text-slate-400 md:block" />
                      <div>
                        <p className="font-mono text-[9px] tracking-[0.12em] text-muted-foreground uppercase">
                          Committed to model
                        </p>
                        <p className="mt-1 text-xs">
                          {committedBatch === undefined
                            ? "waits for the complete batch"
                            : committedBatch.declaredOrder
                                .map((id) => shortTool(String(id).replace("-call-1", "")))
                                .join(" → ")}
                        </p>
                      </div>
                    </div>
                  </>
                )}

                <div>
                  <div className="mb-2 flex items-center justify-between">
                    <SectionLabel icon={Layers3}>Control delivery ledger</SectionLabel>
                    <span className="font-mono text-[9px] text-muted-foreground">
                      {commands.length} transitions
                    </span>
                  </div>
                  {commands.length === 0 ? (
                    <EmptyEvidence>
                      The guided run automatically queues one date change and one follow-up while
                      tools are running.
                    </EmptyEvidence>
                  ) : (
                    <div className="overflow-hidden rounded-md border border-border">
                      {commands.map((entry, index) => (
                        <div
                          className="grid grid-cols-[5.5rem_1fr_auto] items-center gap-3 border-b border-border px-3 py-2 last:border-b-0"
                          key={`${entry.commandId}-${entry.status}`}
                        >
                          <span
                            className={cn(
                              "w-fit rounded-sm px-1.5 py-0.5 font-mono text-[9px] uppercase",
                              entry.status === "queued" && "bg-amber-100 text-amber-800",
                              entry.status === "claimed" && "bg-cyan-100 text-cyan-800",
                              entry.status === "delivered" && "bg-emerald-100 text-emerald-800",
                            )}
                          >
                            {entry.status}
                          </span>
                          <div className="min-w-0">
                            <p className="truncate text-xs">{entry.content}</p>
                            <p className="font-mono text-[9px] text-muted-foreground">
                              {entry.kind} / {entry.deliverySeam}
                            </p>
                          </div>
                          <span className="font-mono text-[9px] text-muted-foreground">
                            {String(index + 1).padStart(2, "0")}
                          </span>
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              </CardContent>
            </Card>

            {pendingApproval === undefined || !active ? null : (
              <Card className="overflow-hidden border-amber-400 bg-amber-50 shadow-[0_10px_35px_-20px_oklch(0.55_0.14_70)]">
                <CardHeader className="flex items-center justify-between border-amber-300">
                  <SectionLabel icon={LockKeyhole}>Permission checkpoint</SectionLabel>
                  <Badge className="border-amber-400 bg-amber-100 text-amber-900">high risk</Badge>
                </CardHeader>
                <CardContent>
                  <p className="text-lg font-semibold tracking-tight">
                    Place a 15-minute itinerary hold?
                  </p>
                  <p className="mt-2 text-sm leading-6 text-slate-700">
                    The Tool handler has not started. Denying—or leaving this unanswered—keeps the
                    action fail-closed.
                  </p>
                  <div className="mt-4 rounded-md border border-amber-300 bg-white/70 p-3 font-mono text-[10px] leading-5 text-slate-600">
                    target · quote:quote-sfo-lhr-001
                    <br />
                    expires · 20 seconds
                    <br />
                    handler starts · {holdState?.starts ?? 0}
                  </div>
                  <div className="mt-4 grid grid-cols-2 gap-2">
                    <Button
                      onClick={() =>
                        resolveApproval({
                          requestId: pendingApproval.request.requestId,
                          choice: "deny",
                        })
                      }
                      type="button"
                      variant="outline"
                    >
                      <Ban />
                      Deny
                    </Button>
                    <Button
                      onClick={() =>
                        resolveApproval({
                          requestId: pendingApproval.request.requestId,
                          choice: "approve",
                        })
                      }
                      type="button"
                    >
                      <Check />
                      Approve hold
                    </Button>
                  </div>
                </CardContent>
              </Card>
            )}

            {itinerary === undefined ? null : (
              <Card className="overflow-hidden border-emerald-300 bg-emerald-50/40">
                <CardHeader className="flex items-center justify-between border-emerald-200">
                  <SectionLabel icon={Check}>Schema-decoded result</SectionLabel>
                  <Badge className="border-emerald-300 bg-emerald-100 text-emerald-800">
                    review only
                  </Badge>
                </CardHeader>
                <CardContent>
                  <div className="flex flex-wrap items-end justify-between gap-3">
                    <div>
                      <p className="text-xl font-semibold tracking-tight">{itinerary.route}</p>
                      <p className="mt-1 text-sm text-muted-foreground">{itinerary.dates}</p>
                    </div>
                    <p className="font-mono text-lg">
                      {formatMoney(itinerary.estimatedTotalCents)}
                    </p>
                  </div>
                  <div className="mt-4 grid gap-2 text-xs md:grid-cols-2">
                    <p className="rounded-md border border-emerald-200 bg-white/70 p-3">
                      {itinerary.flight}
                    </p>
                    <p className="rounded-md border border-emerald-200 bg-white/70 p-3">
                      {itinerary.lodging}
                    </p>
                  </div>
                  <p className="mt-3 font-mono text-[10px] text-emerald-900">
                    {itinerary.assumptions.join(" · ")}
                  </p>
                </CardContent>
              </Card>
            )}

            {state.error === null && state.controlError === null ? null : (
              <div className="rounded-md border border-rose-300 bg-rose-50 px-4 py-3">
                <p className="font-mono text-[10px] font-medium tracking-[0.1em] text-rose-800 uppercase">
                  Fail-closed report
                </p>
                <p className="mt-1 text-xs leading-5 text-rose-900">
                  {state.controlError ?? state.error}
                </p>
              </div>
            )}
          </section>

          <aside className="grid min-w-0 content-start gap-4">
            <Card>
              <CardHeader className="flex items-center justify-between">
                <SectionLabel icon={Gauge}>Run budget</SectionLabel>
                <Badge>{latestBudget?.scopeLevel ?? "run"}</Badge>
              </CardHeader>
              <CardContent>
                {latestBudget === undefined ? (
                  <EmptyEvidence>Limits are attached before the first model pull.</EmptyEvidence>
                ) : (
                  <div className="grid grid-cols-2 gap-px overflow-hidden rounded-md border border-border bg-border">
                    {budgetMeters.map(({ icon: Icon, label, limit, used }) => (
                      <div className="bg-card p-3" key={label}>
                        <Icon className="mb-2 size-3.5 text-slate-500" />
                        <p className="font-mono text-[9px] text-muted-foreground uppercase">
                          {label}
                        </p>
                        <p className="mt-1 font-mono text-xs">
                          {used} / {limit === undefined ? "∞" : limit}
                        </p>
                      </div>
                    ))}
                  </div>
                )}
                {budgetRejected === undefined ? null : (
                  <div className="mt-3 rounded-md border border-rose-300 bg-rose-50 p-3">
                    <p className="font-mono text-[9px] tracking-[0.1em] text-rose-700 uppercase">
                      {budgetRejected.limit} rejected
                    </p>
                    <p className="mt-1 font-mono text-xs text-rose-950">
                      observed {budgetRejected.observedValue} / limit {budgetRejected.limitValue}
                    </p>
                    <p className="mt-1 text-[10px] text-rose-800">
                      Usage was rejected atomically; no Tool handler started.
                    </p>
                  </div>
                )}
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <SectionLabel icon={Database}>Context aperture</SectionLabel>
              </CardHeader>
              <CardContent>
                {latestContext === undefined ? (
                  <EmptyEvidence>
                    Official history and model context are measured separately.
                  </EmptyEvidence>
                ) : (
                  <>
                    <div className="grid grid-cols-[1fr_auto_1fr] items-center gap-2 text-center">
                      <div className="rounded-md bg-slate-900 p-3 text-white">
                        <p className="font-mono text-xl">{latestContext.officialMessageCount}</p>
                        <p className="font-mono text-[8px] text-slate-400 uppercase">official</p>
                      </div>
                      <ArrowRight className="size-4 text-slate-400" />
                      <div className="rounded-md bg-cyan-100 p-3 text-cyan-950">
                        <p className="font-mono text-xl">{latestContext.modelMessageCount}</p>
                        <p className="font-mono text-[8px] text-cyan-700 uppercase">model view</p>
                      </div>
                    </div>
                    <p className="mt-3 text-[11px] leading-5 text-muted-foreground">
                      {latestContext.summary}
                    </p>
                    <div className="mt-3 flex items-center gap-2 font-mono text-[9px] text-emerald-700">
                      <Check className="size-3" />
                      Source history preserved
                    </div>
                  </>
                )}
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <SectionLabel icon={Network}>MCP boundary</SectionLabel>
              </CardHeader>
              <CardContent>
                {mcp === undefined ? (
                  <EmptyEvidence>Discovery will be validated against bounded limits.</EmptyEvidence>
                ) : (
                  <div className="grid gap-2 font-mono text-[10px]">
                    <div className="flex items-center justify-between">
                      <span className="text-muted-foreground">server</span>
                      <span>{mcp.serverId}</span>
                    </div>
                    <div className="flex items-center justify-between">
                      <span className="text-muted-foreground">tools</span>
                      <span>
                        {mcp.toolCount} / {mcp.maxToolCount}
                      </span>
                    </div>
                    <div className="flex items-center justify-between">
                      <span className="text-muted-foreground">discovery</span>
                      <span>
                        {mcp.encodedBytes} / {mcp.maxDiscoveryBytes} B
                      </span>
                    </div>
                    <p className="truncate border-t border-border pt-2 text-[9px] text-muted-foreground">
                      digest · {mcp.toolkitSchemaDigest}
                    </p>
                  </div>
                )}
              </CardContent>
            </Card>

            <Card>
              <CardHeader>
                <SectionLabel icon={SquareTerminal}>Local command envelope</SectionLabel>
              </CardHeader>
              <CardContent>
                {sandboxStarted === undefined ? (
                  <EmptyEvidence>A fixed command runs only inside explicit controls.</EmptyEvidence>
                ) : (
                  <div className="grid gap-3">
                    <div className="grid grid-cols-2 gap-2 font-mono text-[9px]">
                      <div className="rounded-md bg-rose-50 p-2 text-rose-800">
                        <p className="text-[8px] uppercase opacity-70">isolation</p>
                        <p className="mt-1">{sandboxStarted.event.implementation.isolation}</p>
                      </div>
                      <div className="rounded-md bg-emerald-50 p-2 text-emerald-800">
                        <p className="text-[8px] uppercase opacity-70">network</p>
                        <p className="mt-1">disabled</p>
                      </div>
                    </div>
                    <div className="rounded-md bg-slate-950 p-3 font-mono text-[10px] text-cyan-200">
                      <span className="text-slate-500">$</span> /bin/echo itinerary-check
                      <br />
                      <span className="text-emerald-300">
                        {sandboxOutput?.event._tag === "SandboxOutput"
                          ? sandboxOutput.event.text.trim()
                          : "…"}
                      </span>
                    </div>
                    <div className="flex items-center justify-between font-mono text-[9px] text-muted-foreground">
                      <span>output ≤ 2 KiB · wall ≤ 2s</span>
                      <span>
                        exit{" "}
                        {sandboxExited?.event._tag === "SandboxExited"
                          ? sandboxExited.event.exitCode
                          : "…"}
                      </span>
                    </div>
                  </div>
                )}
              </CardContent>
            </Card>
          </aside>
        </div>

        <Card className="mt-4 overflow-hidden">
          <CardHeader className="flex items-center justify-between">
            <SectionLabel icon={TimerReset}>Evidence stream</SectionLabel>
            <Badge>{state.events.length} schema events</Badge>
          </CardHeader>
          <div className="grid min-h-72 lg:grid-cols-[22rem_minmax(0,1fr)]">
            <ol className="max-h-96 overflow-y-auto border-b border-border py-1 lg:border-r lg:border-b-0">
              {state.events.length === 0 ? (
                <li className="p-5 font-mono text-[10px] leading-5 text-muted-foreground">
                  Every tile above is driven by this stream. Pick a scenario to begin.
                </li>
              ) : (
                state.events.map((event, index) => (
                  <li key={`${event._tag}-${index}`}>
                    <button
                      className={cn(
                        "grid w-full grid-cols-[2rem_1fr] gap-2 border-l-2 border-transparent px-3 py-2 text-left hover:bg-muted",
                        selectedEvent === event && "border-cyan-600 bg-cyan-50",
                      )}
                      onClick={() => setSelectedEventIndex(index)}
                      type="button"
                    >
                      <span className="font-mono text-[9px] text-muted-foreground">
                        {String(index + 1).padStart(2, "0")}
                      </span>
                      <span className="truncate font-mono text-[10px]">{event._tag}</span>
                    </button>
                  </li>
                ))
              )}
            </ol>
            {selectedEvent === undefined ? (
              <div className="grid place-items-center p-8 font-mono text-[10px] text-muted-foreground">
                Select an event to inspect its Schema-encoded payload.
              </div>
            ) : (
              <CodeBlock
                className="max-h-96 rounded-none border-0"
                code={encodedEvent(selectedEvent)}
              />
            )}
          </div>
        </Card>
      </div>
    </div>
  );
}

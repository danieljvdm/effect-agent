"use client";

import {
  Brain,
  Check,
  CircleAlert,
  CircleEllipsis,
  CircleStop,
  LoaderCircle,
  PenLine,
  Wrench,
} from "lucide-react";
import type { ReactNode } from "react";

import { cn } from "@/lib/utils";

type AgentActivityPhase =
  | "starting"
  | "thinking"
  | "tool"
  | "composing"
  | "waiting"
  | "complete"
  | "failed"
  | "stopped";

const phaseIcon: Record<AgentActivityPhase, ReactNode> = {
  starting: <LoaderCircle className="animate-spin" />,
  thinking: <Brain className="animate-pulse" />,
  tool: <Wrench className="animate-pulse" />,
  composing: <PenLine className="animate-pulse" />,
  waiting: <CircleEllipsis className="animate-pulse" />,
  complete: <Check />,
  failed: <CircleAlert />,
  stopped: <CircleStop />,
};

/** Live status line for an agent run, driven by semantic runtime events. */
export function AgentActivity({
  className,
  detail,
  label,
  phase,
}: {
  readonly className?: string;
  readonly detail?: string;
  readonly label: string;
  readonly phase: AgentActivityPhase;
}) {
  return (
    <div
      aria-atomic="true"
      aria-live="polite"
      className={cn("flex min-w-0 items-center gap-2.5 py-1 text-muted-foreground", className)}
      role="status"
    >
      <span className="grid size-5 shrink-0 place-items-center rounded-full bg-muted [&_svg]:size-3">
        {phaseIcon[phase]}
      </span>
      <span className="min-w-0">
        <span className="block text-sm leading-5 text-foreground/75">{label}</span>
        {detail === undefined ? null : (
          <span className="block truncate font-mono text-[10px] leading-4 text-muted-foreground">
            {detail}
          </span>
        )}
      </span>
    </div>
  );
}

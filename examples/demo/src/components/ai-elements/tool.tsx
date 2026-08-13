"use client";

import type { DynamicToolUIPart } from "ai";
import { Check, ChevronDown, Circle, LoaderCircle, Wrench, X } from "lucide-react";
import type { ComponentProps, ReactNode } from "react";

import { CodeBlock } from "@/components/ai-elements/code-block";
import { Badge } from "@/components/ui/badge";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { cn } from "@/lib/utils";

type ToolState = DynamicToolUIPart["state"];

const stateMeta: Record<ToolState, { readonly icon: ReactNode; readonly label: string }> = {
  "approval-requested": { icon: <Circle />, label: "Approval" },
  "approval-responded": { icon: <Check />, label: "Approved" },
  "input-available": { icon: <LoaderCircle className="animate-spin" />, label: "Running" },
  "input-streaming": { icon: <Circle />, label: "Pending" },
  "output-available": { icon: <Check />, label: "Complete" },
  "output-denied": { icon: <X />, label: "Denied" },
  "output-error": { icon: <X />, label: "Error" },
};

export function Tool({ className, ...props }: ComponentProps<typeof Collapsible>) {
  return (
    <Collapsible
      className={cn(
        "group overflow-hidden rounded-md border border-border bg-background",
        className,
      )}
      {...props}
    />
  );
}

export function ToolHeader({
  execution,
  executionLabel,
  name,
  state,
}: {
  readonly execution?: "application" | "provider";
  readonly executionLabel?: string;
  readonly name: string;
  readonly state: ToolState;
}) {
  const meta = stateMeta[state];
  return (
    <CollapsibleTrigger className="flex w-full items-center justify-between gap-3 px-3 py-2.5 text-left outline-none">
      <span className="flex min-w-0 items-center gap-2">
        <Wrench className="size-3.5 text-muted-foreground" />
        <span className="truncate font-mono text-xs">{name}</span>
        <Badge className="gap-1 [&_svg]:size-2.5">
          {meta.icon}
          {meta.label}
        </Badge>
        {execution === undefined ? null : (
          <Badge>
            {executionLabel ?? (execution === "provider" ? "OpenAI hosted" : "Framework")}
          </Badge>
        )}
      </span>
      <ChevronDown className="size-3.5 text-muted-foreground transition-transform group-data-[panel-open]:rotate-180" />
    </CollapsibleTrigger>
  );
}

export function ToolContent({ className, ...props }: ComponentProps<typeof CollapsibleContent>) {
  return (
    <CollapsibleContent
      className={cn("border-t border-border px-3 py-3 data-[ending-style]:hidden", className)}
      {...props}
    />
  );
}

export function ToolData({ label, value }: { readonly label: string; readonly value: unknown }) {
  return (
    <div className="space-y-1.5">
      <p className="font-mono text-[10px] tracking-[0.1em] text-muted-foreground uppercase">
        {label}
      </p>
      <CodeBlock className="rounded-md bg-muted/60" code={JSON.stringify(value, null, 2)} />
    </div>
  );
}

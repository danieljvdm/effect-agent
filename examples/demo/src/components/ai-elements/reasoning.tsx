"use client";

import { Brain, ChevronDown } from "lucide-react";

import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";

import { MessageResponse } from "./message";

/** Compact disclosure for reasoning text explicitly returned by a provider. */
export function Reasoning({
  children,
  streaming,
}: {
  readonly children: string;
  readonly streaming: boolean;
}) {
  return (
    <Collapsible className="w-full rounded-md border border-border bg-muted/30" defaultOpen={false}>
      <CollapsibleTrigger className="group/reasoning flex w-full items-center gap-2 px-2.5 py-2 text-left font-mono text-[10px] text-muted-foreground uppercase">
        <Brain className="size-3" />
        <span>{streaming ? "Reasoning · streaming" : "Reasoning"}</span>
        <ChevronDown className="ml-auto size-3 transition-transform group-data-[panel-open]/reasoning:rotate-180" />
      </CollapsibleTrigger>
      <CollapsibleContent className="border-t border-border px-2.5 py-2 text-muted-foreground">
        <MessageResponse className="text-xs leading-5">{children}</MessageResponse>
      </CollapsibleContent>
    </Collapsible>
  );
}

"use client";

import { MessageCircle, ShieldCheck, SlidersHorizontal } from "lucide-react";

import { ChatWorkbench } from "@/components/chat-workbench";
import { SimulatorWorkbench } from "@/components/simulator-workbench";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";

/** Chat-first shell with the Phase 2 diagnostics one deliberate tab away. */
export function DemoWorkbench() {
  return (
    <Tabs defaultValue="chat">
      <header className="sticky top-0 z-50 border-b border-slate-800 bg-slate-950 text-slate-100 shadow-sm">
        <div className="mx-auto flex h-[3.75rem] max-w-[1600px] items-center justify-between gap-4 px-4 lg:px-7">
          <div className="flex min-w-0 items-center gap-3">
            <div className="grid size-8 place-items-center rounded-sm border border-cyan-300/30 bg-cyan-300/10">
              <ShieldCheck className="size-4 text-cyan-300" />
            </div>
            <div className="min-w-0">
              <p className="truncate text-sm font-semibold tracking-tight">Effect Agent</p>
              <p className="hidden truncate font-mono text-[9px] tracking-[0.1em] text-slate-400 uppercase sm:block">
                Chat first · internals on demand
              </p>
            </div>
          </div>
          <TabsList className="h-[3.75rem] items-center border-0 px-0">
            <TabsTrigger
              className="flex h-[3.75rem] items-center gap-2 border-slate-950 text-slate-400 data-[active]:border-cyan-300 data-[active]:text-white"
              value="chat"
            >
              <MessageCircle className="size-3.5" />
              Chat
            </TabsTrigger>
            <TabsTrigger
              className="flex h-[3.75rem] items-center gap-2 border-slate-950 text-slate-400 data-[active]:border-cyan-300 data-[active]:text-white"
              value="simulator"
            >
              <SlidersHorizontal className="size-3.5" />
              Simulator
            </TabsTrigger>
          </TabsList>
        </div>
      </header>
      <TabsContent value="chat">
        <ChatWorkbench />
      </TabsContent>
      <TabsContent value="simulator">
        <SimulatorWorkbench />
      </TabsContent>
    </Tabs>
  );
}

"use client";

import type { ChatStatus } from "ai";
import { ArrowUp, Square } from "lucide-react";
import type { ComponentProps, FormEvent } from "react";

import { Button } from "@/components/ui/button";
import { Textarea } from "@/components/ui/textarea";
import { cn } from "@/lib/utils";

export function PromptInput({ className, ...props }: ComponentProps<"form">) {
  return (
    <form
      className={cn(
        "overflow-hidden rounded-lg border border-border bg-background shadow-[0_1px_2px_rgb(0_0_0/0.03)] focus-within:border-foreground/35 focus-within:ring-2 focus-within:ring-ring/15",
        className,
      )}
      {...props}
    />
  );
}

export function PromptInputTextarea({ onKeyDown, ...props }: ComponentProps<typeof Textarea>) {
  return (
    <Textarea
      onKeyDown={(event) => {
        onKeyDown?.(event);
        if (!event.defaultPrevented && event.key === "Enter" && !event.shiftKey) {
          event.preventDefault();
          event.currentTarget.form?.requestSubmit();
        }
      }}
      {...props}
    />
  );
}

export function PromptInputFooter({ className, ...props }: ComponentProps<"div">) {
  return (
    <div
      className={cn(
        "flex items-center justify-between border-t border-border px-2 py-1.5",
        className,
      )}
      {...props}
    />
  );
}

export function PromptInputSubmit({
  status = "ready",
  ...props
}: ComponentProps<typeof Button> & { readonly status?: ChatStatus }) {
  const running = status === "submitted" || status === "streaming";
  return (
    <Button aria-label={running ? "Stop run" : "Run agent"} size="icon-sm" type="button" {...props}>
      {running ? <Square className="size-3 fill-current" /> : <ArrowUp />}
    </Button>
  );
}

export type PromptSubmitEvent = FormEvent<HTMLFormElement>;

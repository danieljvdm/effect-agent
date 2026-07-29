"use client";

import { ArrowDown } from "lucide-react";
import type { ComponentProps } from "react";
import { useCallback } from "react";
import { StickToBottom, useStickToBottomContext } from "use-stick-to-bottom";

import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";

/** AI Elements conversation viewport with managed bottom anchoring. */
export function Conversation({ className, ...props }: ComponentProps<typeof StickToBottom>) {
  return (
    <StickToBottom
      className={cn("relative flex-1 overflow-y-hidden", className)}
      initial="smooth"
      resize="smooth"
      role="log"
      {...props}
    />
  );
}

export function ConversationContent({
  className,
  ...props
}: ComponentProps<typeof StickToBottom.Content>) {
  return (
    <StickToBottom.Content className={cn("flex flex-col gap-5 px-4 py-5", className)} {...props} />
  );
}

export function ConversationScrollButton({ className, ...props }: ComponentProps<typeof Button>) {
  const { isAtBottom, scrollToBottom } = useStickToBottomContext();
  const handleClick = useCallback(() => scrollToBottom(), [scrollToBottom]);

  if (isAtBottom) {
    return null;
  }

  return (
    <Button
      aria-label="Scroll to latest message"
      className={cn("absolute bottom-3 left-1/2 -translate-x-1/2 rounded-full", className)}
      onClick={handleClick}
      size="icon-sm"
      type="button"
      variant="outline"
      {...props}
    >
      <ArrowDown />
    </Button>
  );
}

import type { ComponentProps } from "react";

import { cn } from "@/lib/utils";

/** Compact status label using the shared shadcn token palette. */
export function Badge({ className, ...props }: ComponentProps<"span">) {
  return (
    <span
      className={cn(
        "inline-flex h-5 items-center rounded-sm border border-border bg-muted px-1.5 font-mono text-[10px] font-medium tracking-[0.08em] text-muted-foreground uppercase",
        className,
      )}
      {...props}
    />
  );
}

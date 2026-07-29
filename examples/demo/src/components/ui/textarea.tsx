import type { ComponentProps } from "react";

import { cn } from "@/lib/utils";

/** Auto-resizable-friendly textarea styled with shadcn tokens. */
export function Textarea({ className, ...props }: ComponentProps<"textarea">) {
  return (
    <textarea
      className={cn(
        "min-h-20 w-full resize-none bg-transparent px-3 py-2.5 text-sm leading-6 outline-none placeholder:text-muted-foreground disabled:cursor-not-allowed disabled:opacity-50",
        className,
      )}
      data-slot="textarea"
      {...props}
    />
  );
}

import type { ComponentProps } from "react";

import { cn } from "@/lib/utils";

/** Small JSON-focused code surface used by the tool and event inspectors. */
export function CodeBlock({
  className,
  code,
  ...props
}: Omit<ComponentProps<"pre">, "children"> & { readonly code: string }) {
  return (
    <pre
      className={cn(
        "overflow-auto whitespace-pre-wrap break-words p-3 font-mono text-[11px] leading-5 text-foreground",
        className,
      )}
      {...props}
    >
      <code>{code}</code>
    </pre>
  );
}

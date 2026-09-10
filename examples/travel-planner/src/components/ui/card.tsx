import type { ComponentProps } from "react";

import { cn } from "../../lib/utils";

export function Card({ className, ...props }: ComponentProps<"section">) {
  return (
    <section
      data-slot="card"
      className={cn(
        "overflow-hidden rounded-2xl border border-solid border-border bg-card text-card-foreground",
        className,
      )}
      {...props}
    />
  );
}

export function CardContent({ className, ...props }: ComponentProps<"div">) {
  return <div className={cn("p-4", className)} {...props} />;
}

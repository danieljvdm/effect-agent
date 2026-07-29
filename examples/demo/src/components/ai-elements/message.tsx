"use client";

import type { UIMessage } from "ai";
import type { HTMLAttributes } from "react";
import { Streamdown } from "streamdown";

import { cn } from "@/lib/utils";

export function Message({
  className,
  from,
  ...props
}: HTMLAttributes<HTMLDivElement> & { readonly from: UIMessage["role"] }) {
  return (
    <div
      className={cn(
        "group flex w-full max-w-[92%] flex-col gap-1.5",
        from === "user" ? "is-user ml-auto items-end" : "is-assistant items-start",
        className,
      )}
      {...props}
    />
  );
}

export function MessageContent({ className, ...props }: HTMLAttributes<HTMLDivElement>) {
  return (
    <div
      className={cn(
        "min-w-0 max-w-full text-sm leading-6",
        "group-[.is-user]:rounded-lg group-[.is-user]:bg-secondary group-[.is-user]:px-3.5 group-[.is-user]:py-2.5",
        className,
      )}
      {...props}
    />
  );
}

/** Streaming-friendly Markdown renderer from the AI Elements message primitive. */
export function MessageResponse({
  className,
  children,
}: {
  readonly className?: string;
  readonly children: string;
}) {
  return (
    <Streamdown className={cn("max-w-none text-sm [&_p]:my-0 [&_p+p]:mt-2", className)}>
      {children}
    </Streamdown>
  );
}

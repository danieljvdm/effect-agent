import { Toggle as TogglePrimitive } from "@base-ui/react/toggle";
import { ToggleGroup as ToggleGroupPrimitive } from "@base-ui/react/toggle-group";

import { cn } from "@/lib/utils";

/** shadcn-styled Base UI toggle group. */
export function ToggleGroup<Value extends string>({
  className,
  ...props
}: ToggleGroupPrimitive.Props<Value>) {
  return (
    <ToggleGroupPrimitive
      className={cn("inline-flex items-center rounded-md border border-border p-0.5", className)}
      {...props}
    />
  );
}

/** One selectable item within a toggle group. */
export function ToggleGroupItem<Value extends string>({
  className,
  ...props
}: TogglePrimitive.Props<Value>) {
  return (
    <TogglePrimitive
      className={cn(
        "inline-flex h-7 items-center justify-center rounded-sm px-2.5 text-xs font-medium text-muted-foreground outline-none transition-colors hover:bg-muted hover:text-foreground focus-visible:ring-2 focus-visible:ring-ring/30 data-[pressed]:bg-background data-[pressed]:text-foreground data-[pressed]:shadow-xs disabled:pointer-events-none disabled:opacity-45",
        className,
      )}
      {...props}
    />
  );
}

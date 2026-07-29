import { createFileRoute } from "@tanstack/react-router";

import { DemoWorkbench } from "@/components/demo-workbench";

export const Route = createFileRoute("/")({
  component: DemoWorkbench,
});

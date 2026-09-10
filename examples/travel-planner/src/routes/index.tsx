import { createFileRoute, redirect } from "@tanstack/react-router";

export const Route = createFileRoute("/")({
  beforeLoad: () => {
    throw redirect({
      to: "/conversations/$conversationId",
      params: { conversationId: crypto.randomUUID() },
      replace: true,
    });
  },
});

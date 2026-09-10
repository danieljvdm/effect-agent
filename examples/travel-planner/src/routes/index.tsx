import { ClientOnly, createFileRoute } from "@tanstack/react-router";

import { Planner } from "../planner";

export const Route = createFileRoute("/")({
  component: () => (
    <ClientOnly
      fallback={
        <main className="empty">
          <span className="wordmark">
            elsewhere<span>↗</span>
          </span>
          <h1>Where do you want to go?</h1>
          <p>Your next trip starts with a conversation.</p>
        </main>
      }
    >
      <Planner />
    </ClientOnly>
  ),
});

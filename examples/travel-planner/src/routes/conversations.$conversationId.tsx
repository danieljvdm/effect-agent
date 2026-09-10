import { ClientOnly, createFileRoute, notFound } from "@tanstack/react-router";
import { Schema } from "effect";

import { ConversationId } from "../domain";
import { Planner } from "../planner";

export const Route = createFileRoute("/conversations/$conversationId")({
  beforeLoad: ({ params }) => {
    if (!Schema.is(ConversationId)(params.conversationId)) throw notFound();
  },
  component: Conversation,
});

function Conversation() {
  const { conversationId } = Route.useParams();

  return (
    <ClientOnly
      fallback={
        <main className="empty">
          <span className="wordmark">
            elsewhere<span>↗</span>
          </span>
          <h1>Your next trip awaits.</h1>
          <p>Loading your conversation…</p>
        </main>
      }
    >
      <Planner conversationId={conversationId} />
    </ClientOnly>
  );
}

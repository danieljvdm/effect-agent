import { CircleAlert, Clock3, Send } from "lucide-react";

export function PendingMessages({
  messages,
  onRetry,
  retrying,
}: {
  readonly messages: ReadonlyArray<{
    readonly id: string;
    readonly text: string;
    readonly status: "sending" | "queued" | "failed";
  }>;
  readonly onRetry: (id: string) => void;
  readonly retrying: boolean;
}) {
  if (messages.length === 0) return null;

  return (
    <section className="pending-messages" aria-label="Pending messages" tabIndex={0}>
      <ol>
        {messages.map((message) => (
          <li className="pending-message" data-status={message.status} key={message.id}>
            <p>{message.text}</p>
            <div className="pending-message-footer">
              <span className="pending-message-status" role="status">
                {message.status === "failed" ? (
                  <CircleAlert size={13} aria-hidden="true" />
                ) : message.status === "queued" ? (
                  <Clock3 size={13} aria-hidden="true" />
                ) : (
                  <Send size={13} aria-hidden="true" />
                )}
                {message.status === "sending"
                  ? "Sending…"
                  : message.status === "queued"
                    ? "Queued · waiting for the agent"
                    : "Couldn't confirm delivery"}
              </span>
              {message.status === "failed" && (
                <button
                  className="pending-message-retry"
                  type="button"
                  disabled={retrying}
                  onClick={() => onRetry(message.id)}
                >
                  Retry
                </button>
              )}
            </div>
          </li>
        ))}
      </ol>
    </section>
  );
}

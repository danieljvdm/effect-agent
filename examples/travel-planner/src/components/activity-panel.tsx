import { DateTime } from "effect";
import { useEffect, useState } from "react";

import type { PlannerActivity, PlannerProgress, PlannerSnapshot } from "../domain";

export function durationLabel(milliseconds: number) {
  if (milliseconds < 1000) return `${Math.round(milliseconds)} ms`;
  if (milliseconds < 60_000) return `${(milliseconds / 1000).toFixed(1)} s`;

  return `${Math.floor(milliseconds / 60_000)}m ${Math.floor((milliseconds % 60_000) / 1000)}s`;
}

const now = () => DateTime.toEpochMillis(DateTime.nowUnsafe());

/** A presentation-only clock; no requests or business state change on a tick. */
export function ElapsedTime({
  startedAt,
  completedAt,
}: {
  readonly startedAt?: number;
  readonly completedAt?: number;
}) {
  const [current, setCurrent] = useState(now);

  useEffect(() => {
    if (startedAt === undefined || completedAt !== undefined) return;
    const timer = window.setInterval(() => setCurrent(now()), 250);

    return () => window.clearInterval(timer);
  }, [startedAt, completedAt]);

  return startedAt === undefined ? null : (
    <span className="elapsed-time">
      {durationLabel(Math.max(0, (completedAt ?? current) - startedAt))}
    </span>
  );
}

/** Canonical public events shared by the planner and its app editor. */
export function ActivityEvents({ events }: { readonly events: readonly PlannerActivity[] }) {
  return (
    <ol className="trace-events">
      {events.map((item) => (
        <li key={item.id} className={`trace-event trace-${item.kind}`}>
          <details>
            <summary>
              <span className="trace-meta">
                <small>{item.kind}</small>
                {item.timestamp && (
                  <time dateTime={item.timestamp} title={item.timestamp}>
                    {item.timestamp.slice(11, 23)}
                  </time>
                )}
                {item.elapsedMs !== undefined && <span>T+ {durationLabel(item.elapsedMs)}</span>}
              </span>
              <span className="trace-event-title">{item.text}</span>
              {item.durationMs !== undefined && (
                <span className="trace-duration">
                  {item.durationLabel}: {durationLabel(item.durationMs)}
                </span>
              )}
            </summary>
            {item.runId && (
              <p className="trace-run">
                Run <code>{item.runId}</code>
              </p>
            )}
            {item.details?.map((entry) => (
              <details className="trace-detail" key={entry.label}>
                <summary>
                  {entry.label}
                  {entry.truncated && <span> · bounded excerpt; some data was omitted</span>}
                </summary>
                <pre tabIndex={0}>{entry.text}</pre>
              </details>
            ))}
            {!item.details?.length && <p>No further details recorded.</p>}
          </details>
        </li>
      ))}
    </ol>
  );
}

export function ActivityPanel({
  snapshot,
  progress,
  onClose,
}: {
  readonly snapshot: PlannerSnapshot | null;
  readonly progress: PlannerProgress | null;
  readonly onClose: () => void;
}) {
  return (
    <aside className="activity-panel trace-panel" aria-label="Agent activity">
      <div className="activity-title">
        <h2>Behind the trip</h2>
        <button aria-label="Close activity" onClick={onClose}>
          ×
        </button>
      </div>
      <p>
        Expand an event for inputs, results, error causes, and provider diagnostics. Times are UTC;
        T+ is time since the run started.
      </p>
      <dl className="trace-overview">
        <div>
          <dt>Model</dt>
          <dd>{snapshot?.usage.model ?? "—"}</dd>
        </div>
        <div>
          <dt>Input / output tokens</dt>
          <dd>
            {snapshot?.usage.inputTokens ?? "—"} / {snapshot?.usage.outputTokens ?? "—"}
          </dd>
        </div>
        <div>
          <dt>Estimated model cost</dt>
          <dd>
            {snapshot?.usage.estimatedCostMicrousd === null ||
            snapshot?.usage.estimatedCostMicrousd === undefined
              ? "Not available"
              : `$${(snapshot.usage.estimatedCostMicrousd / 1_000_000).toFixed(4)}`}
          </dd>
        </div>
        {progress?.startedAt !== undefined && (
          <div>
            <dt>{progress.completedAt === undefined ? "Current response" : "Last response"}</dt>
            <dd>
              <ElapsedTime startedAt={progress.startedAt} completedAt={progress.completedAt} />
            </dd>
          </div>
        )}
      </dl>
      <p>
        Recorded intervals include orchestration and storage. Live step timers measure the observed
        operation. Model cost is separate from your travel budget.
      </p>
      <ActivityEvents events={snapshot?.activity ?? []} />
      {!snapshot?.activity.length && (
        <p className="muted">Your planner's activity will appear here.</p>
      )}
      {!!snapshot?.activity.length && (
        <p>
          Showing the latest {snapshot.activity.length} events in this conversation. Credentials and
          private provider reasoning are excluded. Failure diagnostics are retained across reloads;
          older failures may predate detailed recording.
        </p>
      )}
    </aside>
  );
}

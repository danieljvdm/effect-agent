import { Dialog } from "@base-ui/react/dialog";
import { Check, CircleAlert, Expand, LoaderCircle, Search, X } from "lucide-react";

import type { ResearchScoutActivity } from "../domain.ts";
import { ActivityEvents, ElapsedTime } from "./activity-panel.tsx";
import { AgentProgress } from "./agent-progress.tsx";
import { Button } from "./ui/button.tsx";

const stateLabel = {
  starting: "Starting",
  active: "Working",
  idle: "Finished",
  failed: "Needs attention",
  unavailable: "Updates unavailable",
};

export function ResearchScoutDetails({ scout }: { readonly scout: ResearchScoutActivity }) {
  const active = scout.state === "active" || scout.state === "starting";
  const current = scout.progress.tools.findLast((tool) => tool.state === "running");

  return (
    <section className="trip-app-editor research-scout" aria-label={scout.title}>
      <div className="trip-app-section-heading">
        <h3>{scout.title}</h3>
        <span data-state={scout.state}>{stateLabel[scout.state]}</span>
      </div>
      <p className="research-scout-task">{scout.task}</p>
      {(active || scout.progress.completedAt !== undefined) && (
        <ElapsedTime
          startedAt={scout.progress.startedAt}
          completedAt={scout.progress.completedAt}
        />
      )}
      {scout.state === "failed" && (
        <p className="trip-app-action-error" role="status">
          This scout couldn't finish. Ask your planner to try another approach.
        </p>
      )}
      {scout.state === "unavailable" && (
        <p className="trip-app-editor-note" role="status">
          Updates are temporarily unavailable. Checking again…
        </p>
      )}
      {active && current && !scout.progress.text.trim() && (
        <p className="research-scout-current" role="status">
          {current.label}
        </p>
      )}
      <AgentProgress progress={scout.progress} active={active} busy={false} persistText />
      <details className="trip-app-editor-trace">
        <summary>Recorded activity · {scout.activity.length}</summary>
        {scout.activity.length > 0 ? (
          <>
            <p>
              Expand an event for inputs, results, and diagnostics. Times are UTC; T+ is time since
              the run started.
            </p>
            <ActivityEvents events={scout.activity} />
          </>
        ) : (
          <p>No activity has been recorded yet.</p>
        )}
      </details>
    </section>
  );
}

/** One dock for the bounded set of background scouts in this conversation. */
export function ResearchScoutCard({
  scouts,
}: {
  readonly scouts: readonly ResearchScoutActivity[];
}) {
  const working = scouts.filter(
    (scout) => scout.state === "active" || scout.state === "starting",
  ).length;

  const finished = scouts.filter((scout) => scout.state === "idle").length;

  const attention = scouts.filter(
    (scout) => scout.state === "failed" || scout.state === "unavailable",
  ).length;

  const summary = `${finished} of ${scouts.length} finished${working ? ` · ${working} working` : ""}${attention ? ` · ${attention} ${attention === 1 ? "needs" : "need"} attention` : ""}`;

  if (scouts.length === 0) return null;

  return (
    <Dialog.Root>
      <div
        className="trip-app-card research-scout-card"
        data-status={working ? "building" : attention ? "failed" : "ready"}
      >
        <Dialog.Trigger
          type="button"
          className="trip-app-trigger"
          aria-label={`Research scouts: ${summary}. View research activity`}
        >
          <span className="trip-app-symbol">
            {working ? (
              <LoaderCircle className="trip-app-spinner" size={19} aria-hidden="true" />
            ) : attention ? (
              <CircleAlert size={19} aria-hidden="true" />
            ) : (
              <Check size={19} aria-hidden="true" />
            )}
          </span>
          <span className="trip-app-summary">
            <strong>Research scouts</strong>
            <span role="status">{summary}</span>
          </span>
          <Expand className="trip-app-expand" size={16} aria-hidden="true" />
        </Dialog.Trigger>
      </div>
      <Dialog.Portal>
        <Dialog.Backdrop className="trip-app-backdrop" />
        <Dialog.Popup className="trip-app-dialog research-scout-dialog">
          <header className="trip-app-dialog-header">
            <span className="trip-app-symbol">
              <Search size={21} aria-hidden="true" />
            </span>
            <div>
              <Dialog.Title>Research scouts</Dialog.Title>
              <Dialog.Description>{summary}</Dialog.Description>
            </div>
            <Dialog.Close
              render={<Button variant="ghost" size="icon" />}
              className="trip-app-close"
              aria-label="Close research activity"
            >
              <X size={22} aria-hidden="true" />
            </Dialog.Close>
          </header>
          <div className="trip-app-dialog-body">
            {scouts.map((scout) => (
              <ResearchScoutDetails key={scout.id} scout={scout} />
            ))}
          </div>
          {working > 0 && (
            <footer className="trip-app-dialog-footer">
              <p>You can keep planning while your scouts research.</p>
            </footer>
          )}
        </Dialog.Popup>
      </Dialog.Portal>
    </Dialog.Root>
  );
}

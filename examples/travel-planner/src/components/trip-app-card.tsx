import { Dialog } from "@base-ui/react/dialog";
import { useAtom } from "@effect/atom-react";
import { AsyncResult } from "effect/unstable/reactivity";
import {
  ArrowUpRight,
  Check,
  CircleAlert,
  Code2,
  Expand,
  GitCommitHorizontal,
  LoaderCircle,
  RotateCcw,
  X,
} from "lucide-react";

import type { EditorActivity, TripApp } from "../domain.ts";
import { changeTripAppAtom } from "../state.ts";
import { ActivityEvents, ElapsedTime } from "./activity-panel.tsx";
import { MessageText } from "./message-text.tsx";
import { Button } from "./ui/button.tsx";

const statusLabel = {
  building: "Building your changes…",
  failed: "Build needs attention",
  ready: "Ready to explore",
};

function BuildSymbol({ status }: { readonly status: TripApp["status"] }) {
  return status === "building" ? (
    <LoaderCircle className="trip-app-spinner" size={19} aria-hidden="true" />
  ) : status === "failed" ? (
    <CircleAlert size={19} aria-hidden="true" />
  ) : (
    <Check size={19} aria-hidden="true" />
  );
}

function AppEditorActivity({
  editor,
  summary,
}: {
  readonly editor: EditorActivity;
  readonly summary: string;
}) {
  const active = editor.state === "active" || editor.state === "starting";
  const { progress } = editor;

  const stateLabel = {
    starting: "Starting",
    active: "Working",
    idle: "Finished",
    failed: "Needs attention",
    unavailable: "Activity unavailable",
  };

  return (
    <section className="trip-app-editor" aria-label="App editor activity">
      <div className="trip-app-section-heading">
        <h3>App editor</h3>
        <span>{stateLabel[editor.state]}</span>
      </div>
      <div
        className="trip-app-progress"
        data-status={active ? "building" : editor.state === "idle" ? "ready" : "failed"}
      >
        <div className="trip-app-progress-label">
          <BuildSymbol
            status={active ? "building" : editor.state === "idle" ? "ready" : "failed"}
          />
          <span>Current request</span>
          {(active || progress.completedAt !== undefined) && (
            <ElapsedTime startedAt={progress.startedAt} completedAt={progress.completedAt} />
          )}
        </div>
        <h3 role="status">{summary}</h3>
        <p className="trip-app-editor-task">{editor.task}</p>
      </div>
      {editor.state === "failed" && (
        <p className="trip-app-action-error" role="status">
          The editor couldn't finish this request. Review its recorded activity, then ask your
          planner to continue.
        </p>
      )}
      {editor.state === "unavailable" && (
        <p className="trip-app-editor-note" role="status">
          App activity couldn't load. Checking again for updates…
        </p>
      )}
      {progress.text && (
        <div className="trip-app-editor-response" aria-label="App editor update">
          <MessageText text={progress.text} streaming={active} />
        </div>
      )}
      {progress.tools.length > 0 && (
        <div className="trip-app-activity trip-app-editor-tools">
          <div className="trip-app-section-heading">
            <h3>Steps</h3>
            <span>{progress.tools.length}</span>
          </div>
          <ol>
            {progress.tools.map((tool) => (
              <li key={tool.id} data-phase={tool.state}>
                <span className="trip-app-event-mark">
                  {tool.state === "failed" ? (
                    <CircleAlert size={15} aria-hidden="true" />
                  ) : active && tool.state === "running" ? (
                    <LoaderCircle className="trip-app-spinner" size={15} aria-hidden="true" />
                  ) : tool.state === "complete" ? (
                    <Check size={14} aria-hidden="true" />
                  ) : (
                    <span className="trip-app-event-dot" />
                  )}
                </span>
                <span>
                  {tool.label}
                  <small>
                    {tool.state === "complete"
                      ? "Done"
                      : tool.state === "failed"
                        ? "Couldn't finish"
                        : active
                          ? "Working"
                          : "Last reported as running"}
                  </small>
                </span>
                {(tool.completedAt !== undefined || (active && tool.state === "running")) && (
                  <ElapsedTime startedAt={tool.startedAt} completedAt={tool.completedAt} />
                )}
              </li>
            ))}
          </ol>
        </div>
      )}
      <details className="trip-app-editor-trace">
        <summary>Recorded activity · {editor.activity.length}</summary>
        {editor.activity.length > 0 ? (
          <>
            <p>
              Expand an event for inputs, results, and diagnostics. Times are UTC; T+ is time since
              the run started.
            </p>
            <ActivityEvents events={editor.activity} />
          </>
        ) : (
          <p>No activity has been recorded yet.</p>
        )}
      </details>
    </section>
  );
}

/** Keep this dock beside the composer so build activity stays visible while messages scroll. */
export function TripAppCard({
  app,
  editor,
}: {
  readonly app: TripApp | null;
  readonly editor?: EditorActivity | null;
}) {
  const [result, change] = useAtom(changeTripAppAtom);
  const building = app?.status === "building";
  const editing = editor?.state === "active" || editor?.state === "starting";
  const editorIssue = editor?.state === "failed" || editor?.state === "unavailable";
  const progress = app?.buildProgress ?? [];
  const latest = progress.at(-1);

  const buildSummary = building
    ? (latest?.message ?? statusLabel.building)
    : app
      ? statusLabel[app.status]
      : "No app build yet";

  const editorSummary = editing
    ? (editor?.progress.tools.findLast((tool) => tool.state === "running")?.label ??
      "Editing your trip app…")
    : editor?.state === "failed"
      ? "App editor needs attention"
      : editor?.state === "unavailable"
        ? "App activity unavailable"
        : "App editing finished";

  const summary = editing || editorIssue || !app ? editorSummary : buildSummary;
  const status = editing ? "building" : editorIssue ? "failed" : (app?.status ?? "ready");

  if (!app && !editor) return null;

  return (
    <Dialog.Root>
      <div className="trip-app-card" data-status={status}>
        <Dialog.Trigger
          type="button"
          className="trip-app-trigger"
          aria-label={`Your trip app: ${summary}. View app activity`}
        >
          <span className="trip-app-symbol">
            <BuildSymbol status={status} />
          </span>
          <span className="trip-app-summary">
            <strong>Your trip app</strong>
            <span role="status">{summary}</span>
          </span>
          <Expand className="trip-app-expand" size={16} aria-hidden="true" />
        </Dialog.Trigger>
        {app?.activeCommit && (
          <a
            className="trip-app-open"
            href={app.url}
            target="_blank"
            rel="noopener noreferrer"
            aria-label="Open trip app in a new tab"
          >
            <ArrowUpRight size={21} aria-hidden="true" />
          </a>
        )}
      </div>
      <Dialog.Portal>
        <Dialog.Backdrop className="trip-app-backdrop" />
        <Dialog.Popup className="trip-app-dialog" data-status={status}>
          <header className="trip-app-dialog-header">
            <span className="trip-app-symbol">
              <Code2 size={21} aria-hidden="true" />
            </span>
            <div>
              <Dialog.Title>Your trip app</Dialog.Title>
              <Dialog.Description>App activity & version history</Dialog.Description>
            </div>
            <Dialog.Close
              render={<Button variant="ghost" size="icon" />}
              className="trip-app-close"
              aria-label="Close app activity"
            >
              <X size={22} aria-hidden="true" />
            </Dialog.Close>
          </header>

          <div className="trip-app-dialog-body">
            {editor && <AppEditorActivity editor={editor} summary={editorSummary} />}
            {app && (
              <>
                <section
                  className="trip-app-progress"
                  data-status={app.status}
                  aria-label="Current build"
                >
                  <div className="trip-app-progress-label">
                    <BuildSymbol status={app.status} />
                    <span>
                      {building
                        ? "In progress"
                        : app.status === "ready"
                          ? "Up to date"
                          : "Needs attention"}
                    </span>
                  </div>
                  <h3 role="status">{buildSummary}</h3>
                  <p>
                    {building
                      ? app.activeCommit
                        ? "Your last working version is still available while these changes build."
                        : "Your app will be available here when the build finishes."
                      : app.status === "failed"
                        ? app.activeCommit
                          ? "Your last working version is still available. You can retry the latest changes below."
                          : "The app isn't ready yet. Review the build details and try again."
                        : "Publicly available to anyone with the link. Uses your latest saved trip details."}
                  </p>
                  <time dateTime={app.updatedAt}>
                    Updated {new Date(app.updatedAt).toLocaleString()}
                  </time>
                </section>

                {progress.length > 0 && (
                  <section className="trip-app-activity" aria-label="Build activity">
                    <div className="trip-app-section-heading">
                      <h3>Build activity</h3>
                      {building && <span>Live</span>}
                    </div>
                    <ol>
                      {progress.map((event, index) => (
                        <li key={`${event.at}-${index}`} data-phase={event.phase}>
                          <span className="trip-app-event-mark">
                            {event.phase === "failed" ? (
                              <CircleAlert size={15} aria-hidden="true" />
                            ) : building && index === progress.length - 1 ? (
                              <LoaderCircle
                                className="trip-app-spinner"
                                size={15}
                                aria-hidden="true"
                              />
                            ) : event.phase === "ready" ? (
                              <Check size={14} aria-hidden="true" />
                            ) : (
                              <span className="trip-app-event-dot" />
                            )}
                          </span>
                          <span>{event.message}</span>
                          <time dateTime={event.at}>
                            {new Date(event.at).toLocaleTimeString([], {
                              hour: "2-digit",
                              minute: "2-digit",
                              second: "2-digit",
                            })}
                          </time>
                        </li>
                      ))}
                    </ol>
                  </section>
                )}

                {app.error && (
                  <details className="trip-app-error" open={app.status === "failed"}>
                    <summary>Build details</summary>
                    <pre>{app.error}</pre>
                  </details>
                )}

                {AsyncResult.isFailure(result) && (
                  <p className="trip-app-action-error" role="alert">
                    The app change couldn't be saved. Please retry.
                  </p>
                )}

                <section className="trip-app-versions" aria-label="Version history">
                  <div className="trip-app-section-heading">
                    <h3>Version history</h3>
                    <span>{app.versions.length}</span>
                  </div>
                  {app.versions.length > 0 ? (
                    <>
                      <ol>
                        {[...app.versions].reverse().map((version) => (
                          <li key={version.commitId}>
                            <span className="trip-app-version-mark">
                              <GitCommitHorizontal size={17} aria-hidden="true" />
                            </span>
                            <div className="trip-app-version-content">
                              <strong>{version.label}</strong>
                              <time dateTime={version.createdAt}>
                                {new Date(version.createdAt).toLocaleString()}
                              </time>
                              <code>{version.commitId}</code>
                            </div>
                            {version.commitId === app.activeCommit ? (
                              <span className="trip-app-current">
                                <Check size={12} aria-hidden="true" /> Current
                              </span>
                            ) : (
                              <Button
                                type="button"
                                variant="outline"
                                className="trip-app-restore"
                                disabled={result.waiting || building || editing}
                                aria-label={`Restore ${version.label}`}
                                onClick={() =>
                                  change({
                                    action: "restore",
                                    tripId: app.tripId,
                                    commitId: version.commitId,
                                  })
                                }
                              >
                                <RotateCcw size={14} aria-hidden="true" /> Restore
                              </Button>
                            )}
                          </li>
                        ))}
                      </ol>
                      <p>Restoring code keeps your current trip details.</p>
                    </>
                  ) : (
                    <p>Your first version will appear here after a successful build.</p>
                  )}
                </section>

                <details className="trip-app-source">
                  <summary>Source & build information</summary>
                  <dl>
                    <div>
                      <dt>Repository</dt>
                      <dd>{app.repoName}</dd>
                    </div>
                    <div>
                      <dt>Source commit</dt>
                      <dd>
                        <code>{app.sourceCommit}</code>
                      </dd>
                    </div>
                    <div>
                      <dt>Current commit</dt>
                      <dd>
                        <code>{app.activeCommit ?? "No working version yet"}</code>
                      </dd>
                    </div>
                    <div>
                      <dt>Building commit</dt>
                      <dd>
                        <code>{app.pendingCommit ?? "None"}</code>
                      </dd>
                    </div>
                  </dl>
                </details>
              </>
            )}
          </div>

          <footer className="trip-app-dialog-footer">
            {app?.status === "failed" && (
              <Button
                type="button"
                variant={app.activeCommit ? "outline" : "default"}
                disabled={result.waiting || editing}
                onClick={() => change({ action: "retry", tripId: app.tripId })}
              >
                <RotateCcw size={16} aria-hidden="true" />
                {result.waiting ? "Saving…" : "Retry build"}
              </Button>
            )}
            {app?.activeCommit ? (
              <a className="primary" href={app.url} target="_blank" rel="noopener noreferrer">
                Open trip app <ArrowUpRight size={17} aria-hidden="true" />
              </a>
            ) : building || editing ? (
              <p>You can keep planning while your app takes shape.</p>
            ) : null}
          </footer>
        </Dialog.Popup>
      </Dialog.Portal>
    </Dialog.Root>
  );
}

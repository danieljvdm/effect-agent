import { ArrowUpRight } from "lucide-react";

import type { PlannerProgress } from "../domain.ts";
import { ElapsedTime } from "./activity-panel.tsx";
import { MessageText } from "./message-text.tsx";

export function AgentProgress({
  progress,
  active,
  busy,
  persistText = false,
  showText = true,
}: {
  readonly progress: PlannerProgress | null;
  readonly active: boolean;
  readonly busy: boolean;
  readonly persistText?: boolean;
  readonly showText?: boolean;
}) {
  const hasText = showText && (active || persistText) && !!progress?.text.trim();
  const hasSteps = !!progress?.tools.length;

  return (
    <>
      {progress && hasSteps && <ToolActivity progress={progress} active={active} />}
      {progress && hasText && (
        <article
          className={`message assistant${active ? " streaming-answer" : ""}`}
          aria-label={active ? "Response in progress" : "Latest update"}
          aria-live="off"
        >
          <span className="message-label">
            ELSEWHERE <ArrowUpRight size={13} aria-hidden="true" />
          </span>
          <MessageText text={progress.text} streaming={active} />
        </article>
      )}
      {busy && !hasText && !hasSteps && (
        <p className="working" role="status">
          <span /> Thinking through your trip…
        </p>
      )}
    </>
  );
}

function ToolActivity({
  progress,
  active,
}: {
  readonly progress: PlannerProgress;
  readonly active: boolean;
}) {
  const failed = progress.tools.some((tool) => tool.state === "failed");
  const incomplete = progress.tools.some((tool) => tool.state === "incomplete");

  return (
    <details className={`tool-activity ${active ? "is-working" : ""}`}>
      <summary>
        <span className={`tool-indicator ${active ? "spinning" : ""}`} aria-hidden="true">
          {active ? "" : failed ? "!" : incomplete ? "–" : "✓"}
        </span>
        <span role="status">
          {progress.tools.length} {progress.tools.length === 1 ? "step" : "steps"}{" "}
          {active ? "so far" : "in this response"}
        </span>
        <span className="tool-chevron" aria-hidden="true">
          ⌄
        </span>
      </summary>
      <ol>
        {progress.tools.map((tool) => (
          <li key={tool.id}>
            <span
              className={`tool-indicator ${active && tool.state === "running" ? "spinning" : ""}`}
              aria-hidden="true"
            >
              {tool.state === "failed"
                ? "!"
                : tool.state === "complete"
                  ? "✓"
                  : tool.state === "incomplete"
                    ? "–"
                    : active
                      ? ""
                      : "–"}
            </span>
            <span>{tool.label}</span>
            {(tool.completedAt !== undefined || (active && tool.state === "running")) && (
              <ElapsedTime startedAt={tool.startedAt} completedAt={tool.completedAt} />
            )}
            <small>
              {tool.state === "complete"
                ? "Done"
                : tool.state === "failed"
                  ? "Couldn't finish"
                  : tool.state === "incomplete"
                    ? "Not completed"
                    : active
                      ? "Working"
                      : "Last seen running"}
            </small>
          </li>
        ))}
      </ol>
    </details>
  );
}

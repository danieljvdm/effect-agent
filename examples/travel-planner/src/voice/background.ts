import type { PlannerSnapshot } from "../domain.ts";

export type VoiceBackground = Pick<PlannerSnapshot, "scouts" | "editor" | "app">;

/** Verified UI state, ahead of optional card text so a long card cannot hide completion. */
export const websiteContext = (snapshot: VoiceBackground | null): string => {
  const app = snapshot?.app;
  const editor = snapshot?.editor;

  if (editor?.state === "unavailable")
    return "The latest website editing status is unavailable. Do not infer that the requested edits have finished from an earlier published version.";

  if (editor?.state === "failed")
    return "The requested website edits failed. An earlier published version may still be available.";
  const editing = editor?.state === "active" || editor?.state === "starting";
  const step = editor?.progress.tools.findLast((tool) => tool.state === "running")?.label;

  if (!app)
    return editing ? `Website edits are running.${step ? ` Current action: ${step}.` : ""}` : "";
  if (editing)
    return `Website edits are still running.${step ? ` Current action: ${step}.` : ""} Any published version may precede these edits.`;
  if (app.status === "ready")
    return `The requested website is ready to open: ${app.url}. No website build is running.`;
  if (app.status === "failed")
    return "The website build failed. Do not describe it as still building or ready.";

  return `Website changes are building. ${app.buildProgress?.at(-1)?.message ?? ""}`;
};

/** Only meaningful state changes request speech; detailed build phases remain quiet context. */
export const websiteUpdate = (snapshot: VoiceBackground | null) => {
  const app = snapshot?.app;

  if (snapshot?.editor?.state === "unavailable") return null;

  if (snapshot?.editor?.state === "failed")
    return {
      id: `${snapshot.editor.id}:${snapshot.editor.progress.attemptId}:failed`,
      text: websiteContext(snapshot),
    };

  if (!app || snapshot?.editor?.state === "active" || snapshot?.editor?.state === "starting")
    return null;
  if (app.status !== "ready" && app.status !== "failed") return null;

  return { id: `${app.id}:${app.sourceCommit}:${app.status}`, text: websiteContext(snapshot) };
};

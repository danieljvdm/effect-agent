import { commentableLines } from "./diff.ts";
import type { ChangedFile } from "./diff.ts";
import type { ReviewFinding } from "./review-agent.ts";

/** Why a finding cannot anchor to the current new-version diff, if any. */
export const anchorViolation = (
  finding: ReviewFinding,
  files: ReadonlyArray<ChangedFile>,
): string | undefined => {
  const file = files.find((candidate) => candidate.path === finding.path);
  if (file === undefined) return "path is not part of the changeset";
  if (file.patch === undefined) return "file has no anchorable textual diff";
  if (finding.endLine < finding.startLine) return "endLine precedes startLine";
  if (finding.endLine - finding.startLine + 1 > 100) return "range is implausibly large";
  const anchors = commentableLines(file.patch);
  for (let line = finding.startLine; line <= finding.endLine; line += 1) {
    if (!anchors.has(line)) return `line ${line} is not part of the diff`;
  }
  return undefined;
};

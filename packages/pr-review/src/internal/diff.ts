import { Schema } from "effect";

// ---------------------------------------------------------------------------
// Changed-file and unified-diff primitives shared by the tool surface, the
// publication planner, and the GitHub adapter. The parser is deterministic
// and bounded; it never throws on malformed hunks — unparseable patch text
// simply yields no commentable lines, which fails findings closed.
// ---------------------------------------------------------------------------

/** A repository-relative file path as transported values carry it. */
export const ChangedPath = Schema.NonEmptyString.check(Schema.isMaxLength(512));

/** GitHub's changed-file status vocabulary, kept verbatim. */
export const ChangedFileStatus = Schema.Literals([
  "added",
  "removed",
  "modified",
  "renamed",
  "copied",
  "changed",
  "unchanged",
]);

/** One file changed by the pull request, with its optional textual patch. */
export class ChangedFile extends Schema.Class<ChangedFile>("@effect-agent/pr-review/ChangedFile")({
  path: ChangedPath,
  status: ChangedFileStatus,
  additions: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  deletions: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  /** Present for renames/copies: the path the file previously had. */
  previousPath: Schema.optionalKey(ChangedPath),
  /** Unified-diff hunks; absent for binary or oversized files. */
  patch: Schema.optionalKey(Schema.String),
  /**
   * Bounded UTF-8 content used only when the provider omitted `patch`.
   * Modified files require both sides; additions require head content and
   * deletions require base content. These values are review evidence, never
   * GitHub inline-comment anchors.
   */
  reviewBaseContent: Schema.optionalKey(Schema.String.check(Schema.isMaxLength(200_000))),
  reviewHeadContent: Schema.optionalKey(Schema.String.check(Schema.isMaxLength(200_000))),
}) {}

/** Complete rendered fallback evidence must fit one ordinary model context. */
export const MAX_REVIEW_CONTENT_CHARS = 220_000;

/**
 * Render complete patchless evidence, or refuse it when a required side is
 * absent or B/H annotation would exceed the model-facing bound. Callers use
 * this same value for planning and tool output so truncated fallback evidence
 * can never count as complete coverage.
 */
export const renderReviewContent = (file: ChangedFile): string | undefined => {
  if (file.patch !== undefined) return undefined;
  const includeBase = file.status !== "added";
  const includeHead = file.status !== "removed";
  const sections: Array<string> = [
    "[GitHub omitted the unified diff. B/H lines below are bounded full-file review content, not valid inline-comment anchors. Report defects from this evidence as non-anchored concerns.]",
  ];
  let renderedLength = sections[0]?.length ?? 0;
  const append = (part: string): boolean => {
    const nextLength = renderedLength + 1 + part.length;
    if (nextLength > MAX_REVIEW_CONTENT_CHARS) return false;
    sections.push(part);
    renderedLength = nextLength;
    return true;
  };
  const appendSide = (side: "B" | "H", header: string, content: string): boolean => {
    if (!append(header)) return false;
    const lines = content.split("\n");
    for (let index = 0; index < lines.length; index += 1) {
      if (!append(`${side}${index + 1}   ${lines[index] ?? ""}`)) return false;
    }
    return true;
  };
  if (includeBase) {
    if (file.reviewBaseContent === undefined) return undefined;
    if (!appendSide("B", "[BASE VERSION]", file.reviewBaseContent)) return undefined;
  }
  if (includeHead) {
    if (file.reviewHeadContent === undefined) return undefined;
    if (!appendSide("H", "[HEAD VERSION]", file.reviewHeadContent)) return undefined;
  }
  return sections.join("\n");
};

/** Whether complete patchless evidence fits the model-facing review bound. */
export const hasReviewableContent = (file: ChangedFile): boolean =>
  renderReviewContent(file) !== undefined;

/** Whether the reviewer has either a real patch or bounded textual fallback evidence. */
export const isReviewableFile = (file: ChangedFile): boolean =>
  file.patch !== undefined || hasReviewableContent(file);

/** One parsed line of a unified diff, with both coordinate systems. */
export interface PatchLine {
  readonly kind: "context" | "add" | "del";
  readonly oldLine: number | undefined;
  readonly newLine: number | undefined;
  readonly text: string;
}

const HUNK_HEADER = /^@@ -(\d+)(?:,\d+)? \+(\d+)(?:,\d+)? @@/;

/**
 * Parse unified-diff hunk text into coordinate-tagged lines. Lines outside a
 * recognized hunk header are ignored rather than guessed at.
 */
export const parsePatch = (patch: string): ReadonlyArray<PatchLine> => {
  const lines: Array<PatchLine> = [];
  let oldLine = 0;
  let newLine = 0;
  let inHunk = false;
  for (const raw of patch.split("\n")) {
    const header = HUNK_HEADER.exec(raw);
    if (header !== null) {
      oldLine = Number(header[1]);
      newLine = Number(header[2]);
      inHunk = true;
      continue;
    }
    if (!inHunk) continue;
    if (raw.startsWith("+")) {
      lines.push({ kind: "add", oldLine: undefined, newLine, text: raw.slice(1) });
      newLine += 1;
    } else if (raw.startsWith("-")) {
      lines.push({ kind: "del", oldLine, newLine: undefined, text: raw.slice(1) });
      oldLine += 1;
    } else if (raw.startsWith(" ") || raw === "") {
      lines.push({ kind: "context", oldLine, newLine, text: raw.slice(1) });
      oldLine += 1;
      newLine += 1;
    } else if (raw.startsWith("\\")) {
      // "\ No newline at end of file" — metadata, not a diff line.
    } else {
      // Unrecognized content ends the current hunk conservatively.
      inHunk = false;
    }
  }
  return lines;
};

/**
 * The new-file line numbers a GitHub review comment may anchor to on the
 * RIGHT side: every added or context line that appears in the diff.
 */
export const commentableLines = (patch: string): ReadonlySet<number> => {
  const lines = new Set<number>();
  for (const line of parsePatch(patch)) {
    if (line.newLine !== undefined) lines.add(line.newLine);
  }
  return lines;
};

/**
 * Render a patch with explicit RIGHT-side line numbers so the model can
 * anchor findings without arithmetic. `R<n>` marks a line that exists in the
 * new version of the file (`+` added, blank context); deleted lines keep a
 * bare `-` marker and no number.
 */
export const annotatePatch = (patch: string): string => {
  const output: Array<string> = [];
  let oldLine = 0;
  let newLine = 0;
  let inHunk = false;
  for (const raw of patch.split("\n")) {
    const header = HUNK_HEADER.exec(raw);
    if (header !== null) {
      oldLine = Number(header[1]);
      newLine = Number(header[2]);
      inHunk = true;
      output.push(raw);
      continue;
    }
    if (!inHunk) continue;
    if (raw.startsWith("+")) {
      output.push(`R${newLine} + ${raw.slice(1)}`);
      newLine += 1;
    } else if (raw.startsWith("-")) {
      output.push(`      - ${raw.slice(1)}`);
      oldLine += 1;
    } else if (raw.startsWith(" ") || raw === "") {
      output.push(`R${newLine}   ${raw.slice(1)}`);
      oldLine += 1;
      newLine += 1;
    } else if (raw.startsWith("\\")) {
      output.push(`        ${raw}`);
    } else {
      inHunk = false;
    }
  }
  return output.join("\n");
};

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
export class ChangedFile extends Schema.Class<ChangedFile>(
  "@effect-agent/example-pr-review/ChangedFile",
)({
  path: ChangedPath,
  status: ChangedFileStatus,
  additions: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  deletions: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  /** Present for renames/copies: the path the file previously had. */
  previousPath: Schema.optionalKey(ChangedPath),
  /** Unified-diff hunks; absent for binary or oversized files. */
  patch: Schema.optionalKey(Schema.String),
}) {}

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

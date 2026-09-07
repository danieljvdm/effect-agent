import { parsePatch } from "diff";
import { Option, Schema } from "effect";

/** Host-authored disclosure; omitted payloads are never claimed as reviewed. */
export class GeneratedContentOmission extends Schema.Class<GeneratedContentOmission>(
  "GeneratedContentOmission",
)({
  path: Schema.String,
  lines: Schema.Natural,
  characters: Schema.Natural,
}) {}

// Recognize only single-line regular source maps. Unknown formats, indexed maps,
// malformed patches and ordinary source remain literal evidence.
// https://tc39.es/ecma426/#sec-source-map-format
const decodeSourceMap = Schema.decodeUnknownOption(
  Schema.fromJsonString(
    Schema.Struct({
      version: Schema.Literal(3),
      sources: Schema.Array(Schema.NullOr(Schema.String)),
      names: Schema.optionalKey(Schema.Array(Schema.String)),
      mappings: Schema.String,
      file: Schema.optionalKey(Schema.String),
      sourceRoot: Schema.optionalKey(Schema.String),
      sourcesContent: Schema.optionalKey(Schema.Array(Schema.NullOr(Schema.String))),
      ignoreList: Schema.optionalKey(Schema.Array(Schema.Natural)),
    }),
  ),
);

const sourceMapLines = (content: string): ReadonlySet<number> => {
  const result = new Set<number>();
  const lines = content.split("\n");
  const starts = lines.flatMap((line, index) => (line.startsWith("diff --git ") ? [index] : []));

  for (const [index, start] of starts.entries()) {
    const end = starts[index + 1] ?? lines.length;
    const section = lines.slice(start, end);
    let parsed: ReturnType<typeof parsePatch>;

    try {
      parsed = parsePatch(section.join("\n"));
    } catch {
      continue;
    }
    const patch = parsed[0];

    if (parsed.length !== 1 || patch === undefined) continue;
    const paths = [patch.oldFileName, patch.newFileName].filter((path) => path !== "/dev/null");

    if (paths.length === 0 || paths.some((path) => path === undefined || !path.endsWith(".map"))) {
      continue;
    }

    let inHunk = false;

    for (const [offset, line] of section.entries()) {
      if (line.startsWith("@@ ")) inHunk = true;
      if (!inHunk || !/^[ +-]\s*\{/.test(line)) continue;
      if (Option.isSome(decodeSourceMap(line.slice(1)))) result.add(start + offset + 1);
    }
  }

  return result;
};

/**
 * Elide validated source-map payload lines inside dependency patches, after
 * generating the exact outer diff. Keep every hunk coordinate and line prefix,
 * including the nested +/- prefix, so source finding anchors do not shift.
 */
export const omitGeneratedSourceMaps = (input: {
  readonly path: string;
  readonly basePath: string;
  readonly before: string;
  readonly after: string;
  readonly patch: string;
}) => {
  const before = input.basePath.endsWith(".patch")
    ? sourceMapLines(input.before)
    : new Set<number>();

  const after = input.path.endsWith(".patch") ? sourceMapLines(input.after) : new Set<number>();

  if (before.size === 0 && after.size === 0) return { patch: input.patch, omission: undefined };

  let oldLine = 0;
  let newLine = 0;
  let lines = 0;
  let characters = 0;

  const patch = input.patch
    .split("\n")
    .map((line) => {
      const hunk = /^@@ -(\d+)(?:,\d+)? \+(\d+)(?:,\d+)? @@/.exec(line);

      if (hunk !== null) {
        oldLine = Number(hunk[1]);
        newLine = Number(hunk[2]);

        return line;
      }
      if (oldLine === 0 && newLine === 0) return line;
      const prefix = line[0];

      const omit =
        prefix === "-"
          ? before.has(oldLine)
          : prefix === "+"
            ? after.has(newLine)
            : prefix === " " && before.has(oldLine) && after.has(newLine);

      if (prefix === "-" || prefix === " ") oldLine += 1;
      if (prefix === "+" || prefix === " ") newLine += 1;
      if (!omit) return line;
      const count = line.length - 2;

      lines += 1;
      characters += count;

      return `${line.slice(0, 2)}[Generated source-map JSON omitted: ${count} characters]`;
    })
    .join("\n");

  return {
    patch,
    omission:
      lines === 0
        ? undefined
        : GeneratedContentOmission.make({ path: input.path, lines, characters }),
  };
};

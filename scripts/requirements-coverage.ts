/**
 * Requirements coverage gate (TEST-011, P7 WP0).
 *
 * Extracts every requirement ID defined under `docs/spec`, collects the IDs referenced by
 * executable test titles (string literals in each workspace's `test` tree and the shared
 * adapter conformance suites whose case names run as test titles), and checks them
 * against the "Coverage exceptions" table in `docs/REQUIREMENTS.md`.
 *
 * The gate fails when:
 * - a requirement ID is defined more than once;
 * - a test title or exception row references an ID no specification defines;
 * - a defined ID has neither a test-title reference nor a documented exception row;
 * - an exception row exists for an ID that test titles already reference (stale row);
 * - an `evidence` row cites no existing repository file, or a `deferred`/`process` row names
 *   no owner or future gate.
 *
 * References are counted only inside single-line string literals (the
 * `describe("DUR-009 ...")` convention from docs/spec/testing.md §12); comments never count
 * as coverage.
 */
import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Console, Effect, FileSystem, Path, Schema } from "effect";
import { Command as CliCommand, Flag } from "effect/unstable/cli";

const RequirementId = Schema.String.check(Schema.isPattern(/^[A-Z]{2,7}-\d{3}$/));

const CoverageStatus = Schema.Literals(["tested", "evidence", "deferred", "process"]);
type CoverageStatus = typeof CoverageStatus.Type;

export class RequirementCoverageEntry extends Schema.Class<RequirementCoverageEntry>(
  "scripts/requirements-coverage/RequirementCoverageEntry",
)({
  id: RequirementId,
  specPath: Schema.String,
  status: CoverageStatus,
  references: Schema.Array(Schema.String),
  note: Schema.optionalKey(Schema.String),
}) {}

export class RequirementCoverageReport extends Schema.Class<RequirementCoverageReport>(
  "scripts/requirements-coverage/RequirementCoverageReport",
)({
  specFiles: Schema.Array(Schema.String),
  requirementCount: Schema.Int,
  testedCount: Schema.Int,
  evidenceCount: Schema.Int,
  deferredCount: Schema.Int,
  processCount: Schema.Int,
  entries: Schema.Array(RequirementCoverageEntry),
}) {}

class RequirementsCoverageFailed extends Schema.TaggedErrorClass<RequirementsCoverageFailed>()(
  "RequirementsCoverageFailed",
  {
    problems: Schema.Array(Schema.String),
  },
) {
  override get message() {
    return [
      `Requirements coverage failed with ${this.problems.length} problem(s):`,
      ...this.problems.map((problem) => `- ${problem}`),
    ].join("\n");
  }
}

const REQUIREMENT_ID_PATTERN = /\b[A-Z]{2,7}-\d{3}\b/g;
const DEFINITION_PATTERN = /^- \*\*([A-Z]{2,7}-\d{3})(?:\*\*:|:\*\*)/;
const EXCEPTIONS_HEADING = "## Coverage exceptions";

const json = Flag.boolean("json").pipe(
  Flag.withDescription("Print the Schema-encoded coverage report as JSON instead of the summary."),
);

const resolveRepositoryRoot = Effect.fn("requirementsCoverage.resolveRepositoryRoot")(function* () {
  const path = yield* Path.Path;
  const scriptPath = yield* path.fromFileUrl(new URL(import.meta.url));
  return path.resolve(path.dirname(scriptPath), "..");
});

/**
 * Collects the contents of single-line string literals (`"..."`, `'...'`, and single-line
 * `` `...` ``), skipping line comments and block comments. Multi-line template literals are
 * intentionally not scanned: coverage references belong in test titles.
 */
const extractStringLiterals = (source: string): Array<string> => {
  const literals: Array<string> = [];
  const lines = source.split("\n");
  let inBlockComment = false;

  for (const line of lines) {
    let index = 0;
    while (index < line.length) {
      if (inBlockComment) {
        const end = line.indexOf("*/", index);
        if (end === -1) {
          index = line.length;
          break;
        }
        inBlockComment = false;
        index = end + 2;
        continue;
      }
      const char = line[index];
      if (char === "/" && line[index + 1] === "/") {
        break;
      }
      if (char === "/" && line[index + 1] === "*") {
        inBlockComment = true;
        index += 2;
        continue;
      }
      if (char === '"' || char === "'" || char === "`") {
        let cursor = index + 1;
        let value = "";
        let closed = false;
        while (cursor < line.length) {
          const current = line[cursor];
          if (current === "\\") {
            value += line[cursor + 1] ?? "";
            cursor += 2;
            continue;
          }
          if (current === char) {
            closed = true;
            break;
          }
          value += current;
          cursor += 1;
        }
        if (!closed) {
          // An unterminated quote on this line (multi-line template literal or generated
          // fragment): skip the rest of the line rather than misreading code as a string.
          break;
        }
        literals.push(value);
        index = cursor + 1;
        continue;
      }
      index += 1;
    }
  }

  return literals;
};

const requirementIdsIn = (text: string): Array<string> =>
  [...text.matchAll(REQUIREMENT_ID_PATTERN)].map((match) => match[0]);

const listFiles = Effect.fn("requirementsCoverage.listFiles")(function* (
  root: string,
  relativeDirectory: string,
  suffix: string,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const directory = path.join(root, relativeDirectory);
  if (!(yield* fs.exists(directory))) {
    return [] as Array<string>;
  }
  const entries = yield* fs.readDirectory(directory, { recursive: true });
  return entries
    .filter((entry) => entry.endsWith(suffix))
    .map((entry) => path.join(relativeDirectory, entry))
    .sort();
});

const listWorkspaceDirectories = Effect.fn("requirementsCoverage.listWorkspaceDirectories")(
  function* (root: string, parent: string) {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const directory = path.join(root, parent);
    if (!(yield* fs.exists(directory))) {
      return [] as Array<string>;
    }
    const entries = yield* fs.readDirectory(directory);
    const directories: Array<string> = [];
    for (const entry of entries.sort()) {
      const info = yield* fs.stat(path.join(directory, entry));
      if (info.type === "Directory") {
        directories.push(path.join(parent, entry));
      }
    }
    return directories;
  },
);

interface ExceptionRow {
  readonly id: string;
  readonly status: string;
  readonly evidence: string;
  readonly gate: string;
}

const parseExceptionRows = (requirementsIndex: string): Array<ExceptionRow> => {
  const lines = requirementsIndex.split("\n");
  const start = lines.findIndex((line) => line.trim() === EXCEPTIONS_HEADING);
  if (start === -1) {
    return [];
  }
  const rows: Array<ExceptionRow> = [];
  for (let index = start + 1; index < lines.length; index += 1) {
    const line = lines[index] ?? "";
    if (line.startsWith("## ")) {
      break;
    }
    if (!line.trimStart().startsWith("|")) {
      continue;
    }
    const cells = line
      .split("|")
      .slice(1, -1)
      .map((cell) => cell.trim());
    if (cells.length < 4) {
      continue;
    }
    const [id = "", status = "", evidence = "", gate = ""] = cells;
    if (id === "ID" || /^:?-+:?$/.test(id) || id.length === 0) {
      continue;
    }
    rows.push({ evidence, gate, id: id.replace(/`/g, ""), status });
  }
  return rows;
};

const backtickedPaths = (cell: string): Array<string> =>
  [...cell.matchAll(/`([^`]+)`/g)]
    .map((match) => match[1] ?? "")
    .filter((token) => token.includes("/"));

const runCoverage = Effect.fn("requirementsCoverage")(function* (options: {
  readonly json: boolean;
}) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const root = yield* resolveRepositoryRoot();
  const problems: Array<string> = [];

  // 1. Requirement definitions from the normative specifications.
  const specFiles = yield* listFiles(root, path.join("docs", "spec"), ".md");
  const definedBy = new Map<string, string>();
  for (const specFile of specFiles) {
    const content = yield* fs.readFileString(path.join(root, specFile));
    for (const line of content.split("\n")) {
      const match = DEFINITION_PATTERN.exec(line);
      if (match?.[1] !== undefined) {
        const id = match[1];
        const existing = definedBy.get(id);
        if (existing !== undefined) {
          problems.push(`${id} is defined in both ${existing} and ${specFile}.`);
        } else {
          definedBy.set(id, specFile);
        }
      }
    }
  }

  // 2. Requirement references from executable test titles: package/example test trees plus
  // the shared adapter conformance suites whose case names run as adapter test titles.
  const testFiles: Array<string> = [];
  for (const parent of ["packages", "examples"]) {
    for (const workspace of yield* listWorkspaceDirectories(root, parent)) {
      testFiles.push(...(yield* listFiles(root, path.join(workspace, "test"), ".ts")));
      const sourceFiles = yield* listFiles(root, path.join(workspace, "src"), ".ts");
      testFiles.push(...sourceFiles.filter((file) => path.basename(file).includes("conformance")));
    }
  }
  const referencedBy = new Map<string, Array<string>>();
  for (const testFile of testFiles) {
    const content = yield* fs.readFileString(path.join(root, testFile));
    const ids = new Set(extractStringLiterals(content).flatMap(requirementIdsIn));
    for (const id of ids) {
      referencedBy.set(id, [...(referencedBy.get(id) ?? []), testFile]);
    }
  }
  for (const [id, files] of [...referencedBy.entries()].sort(([left], [right]) =>
    left.localeCompare(right),
  )) {
    if (!definedBy.has(id)) {
      problems.push(`${id} is referenced by ${files.join(", ")} but no specification defines it.`);
    }
  }

  // 3. Documented exceptions from docs/REQUIREMENTS.md.
  const requirementsIndexPath = path.join("docs", "REQUIREMENTS.md");
  const requirementsIndex = yield* fs.readFileString(path.join(root, requirementsIndexPath));
  const exceptionRows = parseExceptionRows(requirementsIndex);
  const exceptions = new Map<string, ExceptionRow>();
  if (exceptionRows.length === 0) {
    problems.push(`${requirementsIndexPath} has no "${EXCEPTIONS_HEADING}" table rows.`);
  }
  for (const row of exceptionRows) {
    if (exceptions.has(row.id)) {
      problems.push(`${row.id} has duplicate coverage-exception rows.`);
      continue;
    }
    exceptions.set(row.id, row);
    if (!definedBy.has(row.id)) {
      problems.push(`${row.id} has a coverage-exception row but no specification defines it.`);
      continue;
    }
    if (referencedBy.has(row.id)) {
      problems.push(
        `${row.id} has a coverage-exception row but is already referenced by test titles in ${(
          referencedBy.get(row.id) ?? []
        ).join(", ")}; remove the stale row.`,
      );
    }
    if (row.status === "evidence") {
      const citedPaths = backtickedPaths(row.evidence);
      if (citedPaths.length === 0) {
        problems.push(`${row.id} is marked "evidence" but cites no repository file.`);
      }
      for (const cited of citedPaths) {
        if (!(yield* fs.exists(path.join(root, cited)))) {
          problems.push(`${row.id} cites \`${cited}\`, which does not exist.`);
        }
      }
    } else if (row.status === "deferred" || row.status === "process") {
      if (row.gate.length === 0 || row.gate === "—") {
        problems.push(`${row.id} is marked "${row.status}" but names no owner or future gate.`);
      }
      if (row.status === "process" && row.evidence.length === 0) {
        problems.push(`${row.id} is marked "process" but documents no reason.`);
      }
    } else {
      problems.push(
        `${row.id} has unknown coverage-exception status "${row.status}" (expected "evidence", "deferred", or "process").`,
      );
    }
  }

  // 4. Every defined requirement is either test-referenced or documented.
  const entries: Array<RequirementCoverageEntry> = [];
  for (const [id, specPath] of [...definedBy.entries()].sort(([left], [right]) =>
    left.localeCompare(right),
  )) {
    const references = referencedBy.get(id);
    if (references !== undefined) {
      entries.push(RequirementCoverageEntry.make({ id, references, specPath, status: "tested" }));
      continue;
    }
    const exception = exceptions.get(id);
    if (exception === undefined) {
      problems.push(
        `${id} (${specPath}) has neither a test-title reference nor a documented coverage-exception row.`,
      );
      continue;
    }
    if (
      exception.status === "evidence" ||
      exception.status === "deferred" ||
      exception.status === "process"
    ) {
      entries.push(
        RequirementCoverageEntry.make({
          id,
          note: `${exception.evidence} — ${exception.gate}`,
          references: backtickedPaths(exception.evidence),
          specPath,
          status: exception.status,
        }),
      );
    }
  }

  if (problems.length > 0) {
    return yield* RequirementsCoverageFailed.make({ problems });
  }

  const report = RequirementCoverageReport.make({
    deferredCount: entries.filter((entry) => entry.status === "deferred").length,
    entries,
    evidenceCount: entries.filter((entry) => entry.status === "evidence").length,
    processCount: entries.filter((entry) => entry.status === "process").length,
    requirementCount: entries.length,
    specFiles,
    testedCount: entries.filter((entry) => entry.status === "tested").length,
  });

  if (options.json) {
    const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(RequirementCoverageReport))(
      report,
    ).pipe(Effect.orDie);
    yield* Console.log(encoded);
    return;
  }

  yield* Console.log("Requirements coverage");
  yield* Console.log(
    `- requirements: ${report.requirementCount} across ${report.specFiles.length} specification files`,
  );
  yield* Console.log(`- tested (test-title reference): ${report.testedCount}`);
  yield* Console.log(`- evidence (documented executable evidence): ${report.evidenceCount}`);
  yield* Console.log(`- deferred (documented owner + future gate): ${report.deferredCount}`);
  yield* Console.log(`- process (non-executable, documented reason): ${report.processCount}`);
});

const command = CliCommand.make("requirements-coverage", { json }, runCoverage).pipe(
  CliCommand.withDescription(
    "Check that every specification requirement ID is linked to executable tests or a documented coverage exception (TEST-011).",
  ),
);

const program = CliCommand.run(command, { version: "1.0.0" }).pipe(
  Effect.tapError((error) => Console.error(String(error))),
  Effect.scoped,
  Effect.provide(NodeServices.layer),
);

NodeRuntime.runMain(program, { disableErrorReporting: true });

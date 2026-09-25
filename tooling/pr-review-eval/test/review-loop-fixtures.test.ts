import { fileURLToPath } from "node:url";

import { NodeServices } from "@effect/platform-node";
import { expect, it } from "@effect/vitest";
import { Effect, FileSystem } from "effect";

import { loadEvalSuite } from "../src/index.ts";

const fixtureDirectory = fileURLToPath(new URL("../fixtures/", import.meta.url));

const lines = (value: string): Array<string> =>
  value.length === 0 ? [] : value.replace(/\n$/, "").split("\n");

/** Apply the observed unified patch to its frozen base without trusting the frozen head. */
const applyPatch = (base: string, patch: string) => {
  const before = lines(base);
  const rows = lines(patch);
  const after: Array<string> = [];
  const rightLines = new Set<number>();
  let sourceIndex = 0;
  let rowIndex = 0;
  let hunks = 0;

  while (rowIndex < rows.length) {
    const header = /^@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@/.exec(rows[rowIndex] ?? "");

    if (header === null) {
      if (hunks > 0) throw new Error(`Unexpected patch row: ${rows[rowIndex]}`);
      rowIndex++;
      continue;
    }

    hunks++;
    const oldStart = Number(header[1]);
    const oldCount = Number(header[2] ?? "1");
    const newStart = Number(header[3]);
    const newCount = Number(header[4] ?? "1");
    const hunkIndex = oldStart === 0 ? 0 : oldStart - 1;

    if (hunkIndex < sourceIndex) throw new Error("Overlapping patch hunks");
    after.push(...before.slice(sourceIndex, hunkIndex));
    sourceIndex = hunkIndex;
    if (after.length + 1 !== newStart) throw new Error("Incorrect new hunk offset");
    rowIndex++;

    let oldRows = 0;
    let newRows = 0;
    let rightLine = newStart;

    while (rowIndex < rows.length && !rows[rowIndex]?.startsWith("@@ ")) {
      const row = rows[rowIndex] ?? "";
      const marker = row[0];
      const content = row.slice(1);

      if (marker === " " || marker === "-") {
        if (before[sourceIndex] !== content)
          throw new Error("Patch source differs from frozen base");
        sourceIndex++;
        oldRows++;
      }
      if (marker === " " || marker === "+") {
        after.push(content);
        rightLines.add(rightLine++);
        newRows++;
      }
      if (marker !== " " && marker !== "-" && marker !== "+") {
        throw new Error(`Unsupported patch row: ${row}`);
      }
      rowIndex++;
    }
    if (oldRows !== oldCount || newRows !== newCount) {
      throw new Error("Patch hunk length disagrees with its header");
    }
  }

  if (hunks === 0) throw new Error("Patch has no hunk");
  after.push(...before.slice(sourceIndex));

  return { content: after.length === 0 ? "" : `${after.join("\n")}\n`, rightLines };
};

it.effect("reconstructs every public review-loop head from its frozen base and patch", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;

    const fixtureNames = (yield* fs.readDirectory(fixtureDirectory)).filter(
      (name) => name.startsWith("synthetic-") && name.endsWith(".json"),
    );

    expect(fixtureNames.length).toBeGreaterThan(0);

    for (const name of fixtureNames) {
      const suite = yield* loadEvalSuite(`${fixtureDirectory}/${name}`);

      for (const evalCase of suite.cases) {
        if (evalCase.repository === undefined)
          throw new Error(`${evalCase.id} has no frozen source`);

        const source = new Map(
          evalCase.repository.files.map((file) => [`${file.revision}\0${file.path}`, file.content]),
        );

        const anchors = new Map<string, ReadonlySet<number>>();

        for (const change of evalCase.request.changes) {
          const base = source.get(`base\0${change.path}`) ?? "";
          const head = source.get(`head\0${change.path}`) ?? "";
          const reconstructed = applyPatch(base, change.patch);

          expect(reconstructed.content, `${evalCase.id}: ${change.path}`).toBe(head);
          anchors.set(change.path, reconstructed.rightLines);
        }
        for (const defect of evalCase.expectedDefects) {
          for (const evidence of defect.evidence) {
            if (evidence.line !== undefined) {
              expect(
                anchors.get(evidence.path)?.has(evidence.line),
                `${evalCase.id}: ${defect.id} evidence line`,
              ).toBe(true);
            }
          }
        }
      }
    }
  }).pipe(Effect.provide(NodeServices.layer)),
);

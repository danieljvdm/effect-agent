import { ContextHistoryPage } from "@effect-agent/engine/ContextHistory";
import { type CanonicalRecordEnvelope } from "@effect-agent/thread/Records";
import { project } from "@effect-agent/thread/ThreadContextHistoryProjection";
import { Effect, Option, Schema } from "effect";

import { check, type Check, type ProjectStatus, type WindowEvidence } from "./contracts.ts";
import { hasSearchPathToRead } from "./evidence.ts";
import { gradeStatus, type ScenarioPhase } from "./scenario.ts";

export const gradePhase = Effect.fn("ContextContinuity.gradePhase")(function* (
  phase: ScenarioPhase,
  result: {
    readonly runId: string;
    readonly notes: { readonly text: string };
    readonly output: ProjectStatus;
  },
  records: ReadonlyArray<CanonicalRecordEnvelope>,
  windowsEvidence: ReadonlyArray<typeof WindowEvidence.Type>,
  answerAbsentBeforeRetrieval: boolean | undefined,
  pressure: boolean,
) {
  const runRecords = records.filter(
    ({ record }) => "runId" in record.payload && record.payload.runId === result.runId,
  );

  const boundaries = runRecords.filter(({ record }) => record.payload._tag === "CompactionCreated");

  const windows = boundaries.filter(
    ({ record }) =>
      record.payload._tag === "CompactionCreated" && record.payload.kind === "rollover",
  );

  const lastWindow = windows.at(-1);

  const settledTool = (name: string) =>
    runRecords.filter(
      ({ record }) =>
        record.payload._tag === "ToolCallSettled" &&
        record.payload.toolName === name &&
        !record.payload.isFailure,
    );

  const checks: Array<Check> = [
    ...gradeStatus(phase, result.output),
    check(`phase-${phase.index}/no-summary-or-pruning`, boundaries.length, windows.length),
    check(`phase-${phase.index}/notes-bounded`, result.notes.text.length <= 2_000, true),
    check(`phase-${phase.index}/notes-written`, settledTool("write_notes").length > 0, true),
    check(
      `phase-${phase.index}/single-logical-run`,
      runRecords.filter(({ record }) => record.payload._tag === "RunStarted").length,
      1,
    ),
  ];

  if (phase.index > 0)
    checks.push(
      check(`phase-${phase.index}/native-rollover`, windows.length >= 1, true),
      check(
        `phase-${phase.index}/single-rollover-request`,
        settledTool("new_context").length,
        pressure ? 0 : 1,
      ),
      check(
        `phase-${phase.index}/notes-saved-before-rollover`,
        settledTool("write_notes").some((record) => record.sequence < (lastWindow?.sequence ?? 0)),
        true,
      ),
      check(
        `phase-${phase.index}/notes-read-after-rollover`,
        settledTool("read_notes").some(
          (record) => record.sequence > (lastWindow?.sequence ?? Number.MAX_SAFE_INTEGER),
        ),
        true,
      ),
    );
  if (phase.receipt !== null) {
    const answer = result.output.receipts[0];
    const source = records.find((record) => record.record.recordId === answer?.recordId);
    const evidence = source === undefined ? undefined : (yield* project(source)).evidence;

    const searched =
      answer !== undefined &&
      hasSearchPathToRead(
        runRecords,
        lastWindow?.sequence ?? Number.MAX_SAFE_INTEGER,
        answer.recordId,
        phase.receipt.code,
      );

    const read = settledTool("read_context_window").some(({ record, sequence }) => {
      if (
        record.payload._tag !== "ToolCallSettled" ||
        sequence <= (lastWindow?.sequence ?? Number.MAX_SAFE_INTEGER)
      )
        return false;
      const page = Schema.decodeUnknownOption(ContextHistoryPage)(record.payload.result);

      return (
        Option.isSome(page) &&
        page.value.recordId === answer?.recordId &&
        page.value.text.includes(phase.receipt?.code ?? "")
      );
    });

    const closedWindows =
      source === undefined
        ? 0
        : windowsEvidence.filter((window) => window.coversThrough >= source.sequence).length;

    checks.push(
      check(
        `phase-${phase.index}/answer-absent-before-retrieval`,
        answerAbsentBeforeRetrieval,
        true,
      ),
      check(`phase-${phase.index}/search-path-to-original-read`, searched, true),
      check(`phase-${phase.index}/successful-read-after-rollover`, read, true),
      check(
        `phase-${phase.index}/cites-original-evidence`,
        evidence !== undefined &&
          evidence.text.includes(phase.receipt.label) &&
          evidence.text.includes(phase.receipt.code) &&
          source !== undefined &&
          source.sequence < (lastWindow?.sequence ?? 0),
        true,
      ),
      check(
        `phase-${phase.index}/archive-distance`,
        closedWindows >= (phase.index === 12 ? 10 : 4),
        true,
      ),
      check(
        `phase-${phase.index}/receipt-not-copied-into-notes`,
        result.notes.text.includes(phase.receipt.code),
        false,
      ),
    );
  }

  return checks;
});

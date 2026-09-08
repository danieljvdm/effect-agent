import { ContextHistoryHit, ContextHistoryPage } from "@effect-agent/engine/ContextHistory";
import type { CanonicalRecordEnvelope } from "@effect-agent/thread/Records";
import { Option, Schema } from "effect";

const referencesRecord = (text: string, recordId: string): boolean => {
  const literal = recordId.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

  // A native record ending in :1 must not match a citation ending in :10.
  return new RegExp(`(^|[^\\w:./-])${literal}($|[^\\w:./-])`).test(text);
};

/**
 * Follow searches and reads in canonical order. A search may locate a later citation which
 * the agent reads before following it to the original source. A copied answer alone does not
 * pass: the path must finish by reading the cited original record and its requested code.
 * The caller separately verifies that source against the retained canonical log and window age.
 */
export const hasSearchPathToRead = (
  records: ReadonlyArray<CanonicalRecordEnvelope>,
  afterSequence: number,
  sourceRecordId: string,
  code: string,
): boolean => {
  const found = new Set<string>();
  const evidence: Array<string> = [];

  for (const { record, sequence } of records) {
    const payload = record.payload;

    if (sequence <= afterSequence || payload._tag !== "ToolCallSettled" || payload.isFailure)
      continue;
    if (payload.toolName === "search_context_windows") {
      const hits = Schema.decodeUnknownOption(Schema.Array(ContextHistoryHit))(payload.result);

      if (Option.isNone(hits)) continue;
      for (const hit of hits.value) {
        found.add(hit.recordId);
        evidence.push(hit.text);
      }
    } else if (payload.toolName === "read_context_window") {
      const decoded = Schema.decodeUnknownOption(ContextHistoryPage)(payload.result);

      if (Option.isNone(decoded)) continue;
      const page = decoded.value;

      if (
        !found.has(page.recordId) &&
        !evidence.some((text) => referencesRecord(text, page.recordId))
      )
        continue;
      if (page.recordId === sourceRecordId && page.text.includes(code)) return true;
      found.add(page.recordId);
      evidence.push(page.text);
    }
  }

  return false;
};

/** Identify provenance from the accepted archive input, independently of the model's citation.
 * Raw UserInputRecorded is deliberately not searchable. Its first model response retains the
 * normalized input; later model responses and tool results are copies, even when equally old.
 */
export const originalArchiveRecord = (
  records: ReadonlyArray<CanonicalRecordEnvelope>,
  archiveInput: string,
  citedRecordId: string | undefined,
): CanonicalRecordEnvelope | undefined => {
  const inputs = records.filter(
    ({ record }) =>
      record.payload._tag === "UserInputRecorded" &&
      record.payload.kind === "user" &&
      record.payload.input === archiveInput,
  );

  if (inputs.length !== 1) return undefined;
  const input = inputs[0];

  if (
    input?.record.payload._tag !== "UserInputRecorded" ||
    input.record.payload.runId === undefined
  )
    return undefined;
  const runId = input.record.payload.runId;

  const original = records.find(
    ({ record, sequence }) =>
      sequence > input.sequence &&
      record.payload._tag === "ModelResponseRecorded" &&
      record.payload.runId === runId &&
      record.payload.turn === 1,
  );

  return original?.record.recordId === citedRecordId ? original : undefined;
};

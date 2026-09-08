import type { PersistedJson } from "@effect-agent/thread/Records";
import { CanonicalRecordEnvelope } from "@effect-agent/thread/Records";
import { Schema } from "effect";
import { describe, expect, it } from "vite-plus/test";

import { hasSearchPathToRead } from "../src/evidence.ts";

const source = "model-response:run:archive:1";
const citation = "tool-settled:run:earlier:10:read-archive";
const code = "harbor-665786a8";

const settled = (sequence: number, toolName: string, result: PersistedJson, isFailure = false) =>
  Schema.decodeSync(CanonicalRecordEnvelope)({
    threadId: "thread-1",
    batchId: `batch:${sequence}`,
    sequence,
    offset: `offset:${sequence}`,
    record: {
      recordId: `record:${sequence}`,
      family: "thread",
      schemaVersion: 1,
      createdAt: "2026-09-08T00:00:00.000Z",
      deploymentId: "test",
      payload: {
        _tag: "ToolCallSettled",
        runId: "run-1",
        toolCallId: `call:${sequence}`,
        toolName,
        result,
        isFailure,
      },
    },
  });

const search = (sequence: number, recordId: string, text = "Archive evidence", failed = false) =>
  settled(
    sequence,
    "search_context_windows",
    [{ recordId, windowId: "archive-window", text }],
    failed,
  );

const read = (sequence: number, recordId: string, text: string) =>
  settled(sequence, "read_context_window", {
    recordId,
    windowId: "archive-window",
    text,
    nextOffset: null,
  });

// The live model followed a search hit to an earlier read result, then to its original source:
// https://github.com/danieljvdm/effect-agent/actions/runs/34245755789
describe("canonical retrieval evidence", () => {
  it("accepts a direct hit and a citation path ending at the verified original read", () => {
    expect(
      hasSearchPathToRead([search(21, source), read(22, source, code)], 20, source, code),
    ).toBe(true);
    expect(
      hasSearchPathToRead(
        [
          search(21, citation),
          read(22, citation, `Earlier result: ${JSON.stringify({ recordId: source, text: code })}`),
          read(23, source, code),
        ],
        20,
        source,
        code,
      ),
    ).toBe(true);
  });

  it.each([
    ["copied answer without the original read", [search(21, citation), read(22, citation, code)]],
    ["read with no search provenance", [read(21, source, code)]],
    ["failed search", [search(21, source, "unavailable", true), read(22, source, code)]],
    ["search from before the window", [search(19, source), read(22, source, code)]],
    ["search after the read", [read(21, source, code), search(22, source)]],
    ["wrong evidence", [search(21, source), read(22, source, "different-code")]],
    [
      "record identifier prefix collision",
      [
        search(21, citation),
        read(22, citation, JSON.stringify({ recordId: `${source}0`, text: code })),
        read(23, source, code),
      ],
    ],
  ] as const)("rejects %s", (_, records) => {
    expect(hasSearchPathToRead(records, 20, source, code)).toBe(false);
  });
});

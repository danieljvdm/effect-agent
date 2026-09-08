import { type ThreadId } from "@effect-agent/core/Identifiers";
import * as MemoryNamespace from "@effect-agent/core/MemoryNamespace";
import type { MemoryKey } from "@effect-agent/core/MemoryStore";
import { MemoryReader } from "@effect-agent/core/MemoryStore";
import type { CanonicalRecordEnvelope } from "@effect-agent/thread/Records";
import { CanonicalSequence } from "@effect-agent/thread/Records";
import {
  LoadCheckpointRequest,
  ThreadRead,
  ThreadStore,
  ThreadTailRequest,
} from "@effect-agent/thread/ThreadStore";
import { Effect, Option, Schema, Stream } from "effect";

import { EvaluationError, type RecoveryCheckpointEvidence } from "./contracts.ts";

export const notesNamespace = MemoryNamespace.define({
  name: "example/context-continuity-notes",
  version: 1,
  identity: Schema.Struct({ threadId: Schema.String }),
});

export const readLog = Effect.fn("ContextContinuity.readLog")(function* (threadId: ThreadId) {
  const store = yield* ThreadStore;
  const tail = yield* store.inspectTail(ThreadTailRequest.make({ threadId }));
  const records: Array<CanonicalRecordEnvelope> = [];
  let cursor = 0;

  while (cursor < tail.tailSequence) {
    const limit = Math.min(64, tail.tailSequence - cursor);

    const page = yield* store
      .read(
        ThreadRead.make({
          threadId,
          afterSequence: yield* Schema.decodeEffect(CanonicalSequence)(cursor),
          limit,
        }),
      )
      .pipe(Stream.runCollect);

    if (
      page.length !== limit ||
      page.some((record, index) => record.sequence !== cursor + index + 1)
    )
      return yield* EvaluationError.make({
        stage: "evidence",
        message: "Canonical evidence was not contiguous",
      });
    records.push(...page);
    cursor += page.length;
  }

  return records;
});

export const readNotes = Effect.fn("ContextContinuity.readNotes")(function* (key: MemoryKey) {
  const reader = yield* MemoryReader;
  const document = yield* reader.get(key);

  if (document?._tag === "WithdrawnMemoryDocument")
    return yield* EvaluationError.make({
      stage: "notes",
      message: "Evaluation notes were unexpectedly withdrawn",
    });

  return { revision: document?.source.revision ?? null, text: document?.content.text ?? "" };
});

/** Read only public cache metadata. Canonical evidence remains independently captured in full. */
export const readRecoveryCheckpoint = Effect.fn("ContextContinuity.readRecoveryCheckpoint")(
  function* (threadId: ThreadId) {
    const store = yield* ThreadStore;

    if (store.recoveryCheckpoints === undefined) {
      const result: RecoveryCheckpointEvidence = { status: "unsupported" };

      return result;
    }

    return yield* store.recoveryCheckpoints.load(LoadCheckpointRequest.make({ threadId })).pipe(
      Effect.map((checkpoint): RecoveryCheckpointEvidence =>
        Option.isSome(checkpoint)
          ? {
              status: "present",
              throughSequence: checkpoint.value.throughSequence,
              tailDigest: checkpoint.value.tailDigest,
            }
          : { status: "missing" },
      ),
      Effect.catchTag("CheckpointRejected", (error) => {
        const result: RecoveryCheckpointEvidence = { status: "rejected", reason: error.reason };

        return Effect.succeed(result);
      }),
    );
  },
);

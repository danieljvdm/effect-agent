import { CanonicalBatch, CanonicalRecord, UserInputRecorded } from "@effect-agent/session";
import { Schema } from "effect";

import { at, id, TEST_DEPLOYMENT, TEST_PRODUCER } from "./harness.ts";

/** A minimal canonical `UserInputRecorded` record for store round-trip tests. */
export const inputRecord = (recordId: string, input: string): CanonicalRecord =>
  CanonicalRecord.make({
    recordId: id(
      Schema.NonEmptyString.pipe(Schema.brand("@effect-agent/session/RecordId")),
      recordId,
    ),
    family: "conversation",
    schemaVersion: 1,
    createdAt: at(1),
    deploymentId: TEST_DEPLOYMENT,
    payload: UserInputRecorded.make({
      submissionId: id(SubmissionId, "submission-do-store"),
      kind: "user",
      input,
    }),
  });

/** A canonical batch of the given records under the shared test producer identity. */
export const batch = (
  batchId: string,
  records: readonly [CanonicalRecord, ...Array<CanonicalRecord>],
): CanonicalBatch =>
  CanonicalBatch.make({
    batchId: id(Schema.NonEmptyString.pipe(Schema.brand("@effect-agent/session/BatchId")), batchId),
    producerId: TEST_PRODUCER,
    records,
  });
import { SubmissionId } from "@effect-agent/core";

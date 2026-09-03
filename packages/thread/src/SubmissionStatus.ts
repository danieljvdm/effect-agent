import { Schema } from "effect";

import { Settlement } from "./SubmissionLedger.ts";

/** A Submission still owes its canonical Settlement, including while suspended. */
export class PendingSubmission extends Schema.TaggedClass<PendingSubmission>()("pending", {}) {}

/** The existing durable Settlement is the only authority for completion. */
export class SettledSubmission extends Schema.TaggedClass<SettledSubmission>()("settled", {
  settlement: Settlement,
}) {}

export const SubmissionStatus = Schema.Union([PendingSubmission, SettledSubmission]);
export type SubmissionStatus = typeof SubmissionStatus.Type;

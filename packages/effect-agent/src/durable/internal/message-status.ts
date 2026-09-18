import { Schema } from "effect";

import { MessageStatus } from "../../core/Messaging.ts";
import type { MessageDeliveryRecord } from "../MessageDelivery.ts";

/** Share the bounded projection across peer and worker delivery; diagnostics stay in storage. */
export const messageStatus = (row: MessageDeliveryRecord) =>
  Schema.decodeUnknownEffect(MessageStatus)({
    message: row.key,
    status: row.status,
    receipt: row.receipt,
    settlement:
      row.settlement === null
        ? null
        : { settlementId: row.settlement.settlementId, outcome: row.settlement.outcome },
    reason:
      row.refusal ??
      row.parkReason ??
      (row.status === "pending" || row.status === "accepted" ? row.retry.lastFailure : null),
  });

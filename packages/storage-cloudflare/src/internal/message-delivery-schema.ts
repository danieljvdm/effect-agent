import { Effect } from "effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

/** Additive storage owned by this adapter; creation participates in its version transaction. */
export const createMessageDeliveryTables = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`
    CREATE TABLE effect_agent_message_deliveries (
      owner_thread_id TEXT NOT NULL,
      message_id TEXT NOT NULL,
      version INTEGER NOT NULL,
      state TEXT NOT NULL,
      deadline_at_millis INTEGER,
      record_json TEXT NOT NULL,
      PRIMARY KEY (owner_thread_id, message_id)
    )
  `.withoutTransform;
  yield* sql`
    CREATE INDEX effect_agent_message_deliveries_due
    ON effect_agent_message_deliveries (deadline_at_millis, owner_thread_id, message_id)
    WHERE deadline_at_millis IS NOT NULL
  `.withoutTransform;
});

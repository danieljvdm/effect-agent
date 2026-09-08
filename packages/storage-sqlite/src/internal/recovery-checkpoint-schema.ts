import { Effect } from "effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

/** One disposable recovery snapshot per Thread, independent of generic projections. */
export const createRecoveryCheckpointTable = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`
    CREATE TABLE effect_agent_recovery_checkpoints (
      thread_id TEXT PRIMARY KEY NOT NULL,
      through_sequence INTEGER NOT NULL,
      tail_digest TEXT NOT NULL,
      checkpoint_json TEXT NOT NULL,
      FOREIGN KEY (thread_id) REFERENCES effect_agent_threads(thread_id) ON DELETE RESTRICT
    )
  `.withoutTransform;
});

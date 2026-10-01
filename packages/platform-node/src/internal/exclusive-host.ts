import { Context } from "effect";

/** Only the automatically managed host owns the SQLite connection for its entire lifetime. */
export const ExclusiveSqliteHost = Context.Reference<boolean>(
  "@effect-agent/platform-node/internal/ExclusiveSqliteHost",
  { defaultValue: () => false },
);

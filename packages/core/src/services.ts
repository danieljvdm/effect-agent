import { Context, type Effect } from "effect";

import type { ConversationId, RunId, TurnId } from "./identifiers.ts";

/** Replaceable authority for creating runtime identities, including deterministic test IDs. */
export class IdGenerator extends Context.Service<
  IdGenerator,
  {
    /** Create the identity for a new conversation. */
    readonly nextConversationId: Effect.Effect<ConversationId>;
    /** Create the identity for a new run. */
    readonly nextRunId: Effect.Effect<RunId>;
    /** Create the identity for a new model turn. */
    readonly nextTurnId: Effect.Effect<TurnId>;
  }
>()("@effect-agent/core/IdGenerator") {}

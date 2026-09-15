import { Layer } from "effect";

import { SubagentReservationsMemoryLive } from "./capabilities/SubagentReservations.ts";
import { layer as historyLayer } from "./engine/ThreadHistory.ts";

/**
 * Run agents and attached subagents with in-memory conversation history.
 * Provide once around the parent program and all child handler Layers so siblings
 * share history and one reservation ledger. Reuse a Thread ID to continue a conversation.
 * Conversations can span any number of Runs within the store's capacity limits.
 * Each independent Layer build owns fresh state; state is released when its Scope closes.
 * Process loss loses history and active execution; this Layer provides no crash recovery.
 *
 * Models, tool handlers, and provider clients remain application-supplied. Default
 * IDs need no Layer; enclosing ID and context-preparation overrides are preserved.
 * For storage-backed history, provide PersistentHistory.layer and a shared
 * SubagentReservationsMemoryLive instead. Durable hosts own their own assembly.
 */
export const layer = Layer.merge(historyLayer, SubagentReservationsMemoryLive);

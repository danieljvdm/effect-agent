import { SubagentReservationsMemoryLive } from "@effect-agent/capabilities/SubagentReservations";
import { layerTransient } from "@effect-agent/engine/ThreadHistory";
import { Layer } from "effect";

/**
 * Run agents and attached subagents without retaining completed history.
 * Provide once around the parent program and all child handler Layers so siblings
 * share one reservation ledger. Each independent Layer build owns fresh state;
 * no ledger is allocated at import time or shared globally.
 *
 * Models, tool handlers, and provider clients remain application-supplied. Default
 * IDs need no Layer; enclosing ID and context-preparation overrides are preserved.
 * For retained history, provide PersistentHistory.layer and a shared
 * SubagentReservationsMemoryLive instead. Durable hosts own their own assembly.
 */
export const layer = Layer.merge(layerTransient, SubagentReservationsMemoryLive);

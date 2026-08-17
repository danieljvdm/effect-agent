/**
 * `@effect-agent/platform-cloudflare` — the Cloudflare Layer-assembly library for deployment
 * class DC (deployment spec §12: a Layer-assembly library, not an application entrypoint).
 *
 * One SQLite-backed Durable Object per Conversation is the serialized owner (durability §6):
 * `makeConversationObjectClass` builds the class applications export from their Worker,
 * `CloudflareDurableRuntime.layer` assembles the coordinator over the storage-cloudflare
 * adapters and the WP2 cross-Object routing, `DurableAlarmService`/`ConversationMaintenance`
 * multiplex every cadence into the Object's single alarm slot (nonterminal work implies a
 * committed alarm, so eviction recovers without an incoming request), and
 * `CloudflareConversationClient` is the Worker-side ingress. Platform bindings enter ONLY
 * through the `bindings.ts` Layers (DEPLOY-010). This is the only workspace package allowed
 * to import the `cloudflare:workers` runtime module.
 */
export * from "./bindings.ts";
export * from "./crypto.ts";
export * from "./config.ts";
export * from "./alarm.ts";
export * from "./wake-scheduler.ts";
export * from "./transport.ts";
export * from "./layers.ts";
export * from "./conversation-object.ts";
export * from "./client.ts";
export * from "./code-mode-executor.ts";

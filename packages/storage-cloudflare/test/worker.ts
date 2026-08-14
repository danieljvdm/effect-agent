import { DurableObject } from "cloudflare:workers";
import { Effect } from "effect";

import {
  handleEncodedPortRequest,
  layer as doConversationStoreLayer,
  ledgerLayer,
} from "../src/index.ts";

export { ProbeDurableObject } from "./probe-worker.ts";

/**
 * SQLite-backed Durable Object shell hosting the WP1/WP2 adapter suites. Tests use
 * `runInDurableObject` to run the adapter Layers directly against `ctx.storage`; the class
 * itself holds NO state — important state never lives in in-memory Durable Object fields,
 * and the real Conversation Object class factory is WP3's `@effect-agent/platform-cloudflare`
 * concern.
 *
 * `portCall` is the WP2 cross-Object endpoint (plan §1.3, D-P6-3): Schema-encoded port
 * request envelopes arrive over native Durable Object JS RPC and execute against THIS
 * Object's LOCAL facets over its own private SQLite storage — never against the routed
 * decorators, so a request cannot bounce between Objects. The Object identity rule is
 * `idFromName(conversationId)`: routing tests name each Object by the Conversation it owns.
 */
export class ConversationStorageObject extends DurableObject {
  async portCall(encoded: unknown): Promise<unknown> {
    return Effect.runPromise(
      handleEncodedPortRequest(encoded).pipe(
        Effect.provide([
          ledgerLayer({ storage: this.ctx.storage }),
          doConversationStoreLayer({ storage: this.ctx.storage, observationPollInterval: 1 }),
        ]),
      ),
    );
  }
}

export default {
  fetch(): Response {
    return new Response("effect-agent storage-cloudflare test worker");
  },
};

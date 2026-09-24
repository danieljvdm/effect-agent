import { doMessageDeliveryStoreLayer } from "@effect-agent/storage-cloudflare/do-message-delivery-store";
import { DoStorageFailpoint } from "@effect-agent/storage-cloudflare/do-storage-failpoint";
import { ledgerLayer } from "@effect-agent/storage-cloudflare/do-submission-ledger";
import {
  storageConfigLayer,
  layer as doThreadStoreLayer,
} from "@effect-agent/storage-cloudflare/do-thread-store";
import { handleEncodedPortRequest } from "@effect-agent/storage-cloudflare/port-routing";
import { SqliteClient } from "@effect/sql-sqlite-do";
import { DurableObject } from "cloudflare:workers";
import { Effect, Layer } from "effect";

/**
 * SQLite-backed Durable Object shell hosting the WP1/WP2 adapter suites. Tests use
 * `runInDurableObject` to run the adapter Layers directly against `ctx.storage`; the class
 * itself holds NO state — important state never lives in in-memory Durable Object fields,
 * and the real Thread Object class factory is WP3's `@effect-agent/platform-cloudflare`
 * concern.
 *
 * `portCall` is the WP2 cross-Object endpoint (plan §1.3, D-P6-3): Schema-encoded port
 * request envelopes arrive over native Durable Object JS RPC and execute against THIS
 * Object's LOCAL facets over its own private SQLite storage — never against the routed
 * decorators, so a request cannot bounce between Objects. The Object identity rule is
 * `idFromName(threadId)`: routing tests name each Object by the Thread it owns.
 */
export class ThreadStorageObject extends DurableObject {
  async portCall(encoded: unknown): Promise<unknown> {
    return Effect.runPromise(
      handleEncodedPortRequest(encoded).pipe(
        Effect.provide([
          doMessageDeliveryStoreLayer().pipe(
            Layer.provide([
              storageConfigLayer({ storage: this.ctx.storage }),
              DoStorageFailpoint.layer,
              SqliteClient.layer({ storage: this.ctx.storage }),
            ]),
          ),
          ledgerLayer({ storage: this.ctx.storage }),
          doThreadStoreLayer({ storage: this.ctx.storage, observationPollInterval: 1 }),
        ]),
      ),
    );
  }
}

/** Schedule Store test shell. Contract cases run Layers directly through `runInDurableObject`. */
export class ScheduleStorageObject extends DurableObject {}

export default {
  fetch(): Response {
    return new Response("effect-agent storage-cloudflare test worker");
  },
};

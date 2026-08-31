import { DoStorageFailpointLocation } from "@effect-agent/storage-cloudflare";
import {
  TravelPlannerPhase4,
  makePhase6TravelPlannerBindings,
  phase1Trip,
  phase4TravelPlannerSubmitOptions,
  phase6SupplierReconcilerLayer,
  phase6TravelPlannerDeploymentId,
  phase6TravelPlannerProducerPrefix,
} from "@effect-agent/testing/fixtures/travel-planner";
import {
  DurableRuntimeFailpointLocation,
  Receipt,
  type CanonicalRecordEnvelope,
} from "@effect-agent/thread";
import { BrowserCrypto } from "@effect/platform-browser";
import { Effect, Layer, Schema } from "effect";

import {
  CloudflareThreadClient,
  ThreadObjectNamespace,
  ThreadObject,
  type ThreadObjectRpc,
} from "../../src/index.ts";
import { layerFromBindings } from "../../src/layers.ts";
import {
  armRuntimeEviction,
  armStorageEviction,
  armedEvictionsRemaining,
  decodeThreadId,
  decodeIdempotencyKey,
  runtimeEvictionFailpoint,
  storageEvictionFailpoint,
} from "../fixtures.ts";

/**
 * The WP5 Miniflare restart-lane Worker (plan §3, §6 restart row): the REAL Travel Planner
 * Thread Object under armed `ctx.abort()` failpoints, plus an HTTP control plane the
 * Node-side test drives ACROSS full runtime restarts. Module state (armed queues, delivery
 * counters) intentionally dies with each runtime: a reopened Miniflare starts unarmed — the
 * "fresh deployment over surviving storage" the lane exists to prove.
 *
 * Instrumentation records every Durable Object ENTRY by kind, so the test can assert the
 * strongest form of the eviction gate: after reopening the persisted directory, the ONLY
 * deliveries the Object received before convergence were its own redelivered alarms — no
 * client request of any kind drove recovery.
 */

let alarmDeliveries = 0;
const clientEntries: Array<string> = [];

const baseClass = ThreadObject.make(
  Layer.unwrap(Effect.map(makePhase6TravelPlannerBindings, layerFromBindings)),
  {
    namespaceBinding: "THREADS",
    deploymentId: phase6TravelPlannerDeploymentId,
    producerPrefix: phase6TravelPlannerProducerPrefix,
    ownershipLeaseDuration: 1_000,
    leaseRenewalInterval: 100,
    wakeScanInterval: 100,
    settlementPollInterval: 25,
    abortPollInterval: 25,
    alarmBackoffBase: 10,
    alarmBackoffCap: 100,
    observationPollInterval: 10,
    toolReconciler: phase6SupplierReconcilerLayer,
    storageFailpoint: storageEvictionFailpoint,
    runtimeFailpoint: runtimeEvictionFailpoint,
  },
);

/** The restart lane's Thread Object, with entry-kind instrumentation. */
export class TravelPlannerRestartObject extends baseClass {
  override async alarm(): Promise<void> {
    alarmDeliveries += 1;
    return super.alarm();
  }
  override async submitEncoded(encoded: unknown): Promise<unknown> {
    clientEntries.push("submit");
    return super.submitEncoded(encoded);
  }
  override async awaitSettlementEncoded(encoded: unknown): Promise<unknown> {
    clientEntries.push("awaitSettlement");
    return super.awaitSettlementEncoded(encoded);
  }
  override async observePage(encoded: unknown): Promise<unknown> {
    clientEntries.push("observePage");
    return super.observePage(encoded);
  }
  override async abortEncoded(encoded: unknown): Promise<unknown> {
    clientEntries.push("abort");
    return super.abortEncoded(encoded);
  }
  override async resolveApprovalEncoded(encoded: unknown): Promise<unknown> {
    clientEntries.push("resolveApproval");
    return super.resolveApprovalEncoded(encoded);
  }
  override async resolveUnknownEncoded(encoded: unknown): Promise<unknown> {
    clientEntries.push("resolveUnknown");
    return super.resolveUnknownEncoded(encoded);
  }
  override async portCall(encoded: unknown): Promise<unknown> {
    clientEntries.push("portCall");
    return super.portCall(encoded);
  }
  override async wake(): Promise<void> {
    clientEntries.push("wake");
    return super.wake();
  }
}

const encodeReceipt = Schema.encodeSync(Receipt);
const decodeReceipt = Schema.decodeUnknownSync(Receipt);

const ArmRequest = Schema.Union([
  Schema.TaggedStruct("runtime", {
    thread: Schema.String,
    location: DurableRuntimeFailpointLocation,
    count: Schema.Int.check(Schema.isGreaterThan(0)),
  }),
  Schema.TaggedStruct("storage", {
    thread: Schema.String,
    location: DoStorageFailpointLocation,
    count: Schema.Int.check(Schema.isGreaterThan(0)),
  }),
]);
const decodeArmRequest = Schema.decodeUnknownSync(ArmRequest);

const SubmitRequest = Schema.Struct({ thread: Schema.String, key: Schema.String });
const decodeSubmitRequest = Schema.decodeUnknownSync(SubmitRequest);

const AwaitRequest = Schema.Struct({ receipt: Schema.Unknown });
const decodeAwaitRequest = Schema.decodeUnknownSync(AwaitRequest);

const RecordsRequest = Schema.Struct({ thread: Schema.String });
const decodeRecordsRequest = Schema.decodeUnknownSync(RecordsRequest);

const runClient = <A, E>(
  env: Cloudflare.Env,
  effect: Effect.Effect<A, E, CloudflareThreadClient>,
): Promise<A> =>
  Effect.runPromise(
    effect.pipe(
      Effect.provide(
        CloudflareThreadClient.layer.pipe(
          Layer.provide(
            Layer.mergeAll(
              ThreadObjectNamespace.layer(
                env.THREADS as unknown as DurableObjectNamespace<ThreadObjectRpc>,
              ),
              BrowserCrypto.layer,
            ),
          ),
        ),
      ),
    ),
  );

export default {
  async fetch(request: Request, env: Cloudflare.Env): Promise<Response> {
    const url = new URL(request.url);
    try {
      switch (url.pathname) {
        case "/arm": {
          const arm = decodeArmRequest(await request.json());
          if (arm._tag === "runtime") {
            armRuntimeEviction(
              arm.thread,
              ...Array.from({ length: arm.count }, () => arm.location),
            );
          } else {
            armStorageEviction(
              arm.thread,
              ...Array.from({ length: arm.count }, () => arm.location),
            );
          }
          return Response.json({ armed: arm.count });
        }
        case "/armed": {
          const thread = url.searchParams.get("thread") ?? "";
          return Response.json({ remaining: armedEvictionsRemaining(thread) });
        }
        case "/introspect": {
          return Response.json({ alarmDeliveries, clientEntries });
        }
        case "/submit": {
          const submit = decodeSubmitRequest(await request.json());
          const receipt = await runClient(
            env,
            Effect.gen(function* () {
              const client = yield* CloudflareThreadClient;
              return yield* client.submit(
                { definition: TravelPlannerPhase4 },
                phase1Trip,
                phase4TravelPlannerSubmitOptions(
                  decodeThreadId(submit.thread),
                  decodeIdempotencyKey(submit.key),
                ),
              );
            }),
          );
          return Response.json({ receipt: encodeReceipt(receipt) });
        }
        case "/await": {
          const body = decodeAwaitRequest(await request.json());
          const receipt = decodeReceipt(body.receipt);
          const settlement = await runClient(
            env,
            Effect.gen(function* () {
              const client = yield* CloudflareThreadClient;
              return yield* client.awaitSettlement(receipt);
            }),
          );
          return Response.json({ outcome: settlement.outcome });
        }
        case "/records": {
          const body = decodeRecordsRequest(await request.json());
          const records: ReadonlyArray<CanonicalRecordEnvelope> = await runClient(
            env,
            Effect.gen(function* () {
              const client = yield* CloudflareThreadClient;
              return yield* client.readAll(decodeThreadId(body.thread));
            }),
          );
          return Response.json({
            tags: records.map((envelope) => envelope.record.payload._tag),
          });
        }
        default:
          return new Response("not found", { status: 404 });
      }
    } catch (error) {
      return Response.json(
        { error: error instanceof Error ? error.message : String(error) },
        { status: 500 },
      );
    }
  },
};

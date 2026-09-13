import { type ResolvedBinding } from "@effect-agent/thread/agent-registration";
import { DurableAgentRuntime } from "@effect-agent/thread/durable-agent-runtime";
import { PreparedInputAdmission } from "@effect-agent/thread/prepared-input-admission";
import { ThreadProjectionMaintenance } from "@effect-agent/thread/thread-projection-maintenance";
import { ThreadStore } from "@effect-agent/thread/thread-store";
import { SqliteClient } from "@effect/sql-sqlite-do";
import { Context, Effect, Layer, ManagedRuntime } from "effect";
import { type ThreadId } from "effect-agent/identifiers";
import { SqlClient } from "effect/unstable/sql/SqlClient";

import { ThreadMutationGate } from "../src/Alarm.ts";
import { DurableObjectContext, ThreadObjectNamespace } from "../src/CloudflareBindings.ts";
import * as ThreadObject from "../src/ThreadObject.ts";

/** An application service acquired from the same ports its Bindings use. */
class LocalReads extends Context.Service<LocalReads, { readonly store: ThreadStore["Service"] }>()(
  "shared-owner/LocalReads",
) {}

/**
 * Build once per physical incarnation. The host routes each logical Thread deterministically,
 * calls handleRpc with that identity, and owns one bounded ThreadMaintenance.pass per alarm.
 */
export const sharedOwnerRuntime = (
  state: DurableObjectState,
  environment: unknown,
  namespace: ThreadObjectNamespace["Service"],
  ownsThread: (threadId: ThreadId) => boolean,
  bindings: ReadonlyArray<ResolvedBinding>,
) => {
  const localReads = Layer.effect(LocalReads)(Effect.map(ThreadStore, (store) => ({ store })));
  const projection = Layer.merge(ThreadProjectionMaintenance.layer, localReads);

  const application = Layer.unwrap(
    Effect.gen(function* () {
      // Each service is supplied by layerInHost before the application is constructed.
      yield* SqlClient;
      yield* PreparedInputAdmission;
      yield* ThreadMutationGate;
      yield* LocalReads;

      return DurableAgentRuntime.layerWithBindings(bindings);
    }),
  );

  const platform = Layer.merge(
    DurableObjectContext.layer(state, environment),
    Layer.succeed(ThreadObjectNamespace, namespace),
  );

  return ManagedRuntime.make(
    ThreadObject.layerInHost(application, { projection }).pipe(
      Layer.provideMerge(SqliteClient.layer({ storage: state.storage })),
      Layer.provideMerge(
        ThreadObject.layerHostConfig(
          { deploymentId: "application", producerPrefix: "application" },
          ownsThread,
        ).pipe(Layer.provideMerge(platform)),
      ),
    ),
  );
};

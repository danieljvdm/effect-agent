import { SqliteClient } from "@effect/sql-sqlite-do";
import { type ResolvedBinding } from "@yielded/agent/agent-registration";
import { DurableAgentRuntime } from "@yielded/agent/durable-agent-runtime";
import { type ThreadId } from "@yielded/agent/identifiers";
import { PreparedInputAdmission } from "@yielded/agent/prepared-input-admission";
import { ThreadProjectionMaintenance } from "@yielded/agent/thread-projection-maintenance";
import { ThreadStore } from "@yielded/agent/thread-store";
import { Context, Effect, Layer, ManagedRuntime } from "effect";
import { SqlClient } from "effect/sql/SqlClient";

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

import { Agent } from "@effect-agent/core";
import {
  makeConversationObjectClass,
  dynamicWorkerCodeExecutorLayer,
  type CloudflareBindingSource,
  type CodeModeHostStub,
} from "@effect-agent/platform-cloudflare";
import {
  DefinitionDigests,
  Digest,
  DurableWorkerBinding,
  type ResolvedBinding,
} from "@effect-agent/session";
import { Effect, Layer, Schema } from "effect";

import { codeModeAgent, codeModeHandlersLayer } from "./agent.ts";
import { invoiceDbSqlLayer } from "./db.ts";
import {
  liveModel,
  scriptedCteProbeModel,
  scriptedModel,
  scriptedWriteProbeModel,
} from "./profiles.ts";

/**
 * The demo's Conversation Object: the DC assembly (Durable Cloudflare) agent
 * host from `@effect-agent/platform-cloudflare`. Each submitted question
 * becomes a Run under the DC assembly's guarantees — every step lands in the
 * Object's append-only canonical log (SQLite in Durable Object storage), a
 * multiplexed alarm drives the Run to settlement, and clients follow it
 * through `awaitSettlement`/`readAll`.
 *
 * The Code Mode toolkit is wired INTERNALLY, from the Object's own env
 * bindings, via the `bindings` capture: the callback receives the live
 * `{ ctx, env }` and builds the handler Layer directly over `env.LOADER`
 * (the Dynamic Worker executor), `env.CODE_MODE_HOST` (the host RPC seam),
 * and `env.DB` (Effect SQL over D1). The Worker submits only a definition
 * plus digests; models and tools never leave the Object.
 *
 * Honest scope note: Code Mode is specified for deployment class E
 * (ADR-0017); running it on the durable runtime is owner-directed demo
 * territory — mid-pass eviction semantics (C5/C6) remain future work.
 */

const digestOf = (pair: string) => Schema.decodeSync(Digest)(pair.repeat(32));

/**
 * Deterministic demo digest triples. The Object resolves which registered
 * binding serves a claim by the digests the submitter supplied, so each
 * profile registers under its own triple and the Worker picks one at submit.
 */
export const scriptedDigests = DefinitionDigests.make({
  agent: digestOf("a0"),
  model: digestOf("a1"),
  tools: digestOf("a2"),
});
export const writeProbeDigests = DefinitionDigests.make({
  agent: digestOf("b0"),
  model: digestOf("b1"),
  tools: digestOf("b2"),
});
export const cteProbeDigests = DefinitionDigests.make({
  agent: digestOf("c0"),
  model: digestOf("c1"),
  tools: digestOf("c2"),
});
export const liveDigests = DefinitionDigests.make({
  agent: digestOf("d0"),
  model: digestOf("d1"),
  tools: digestOf("d2"),
});

interface DemoEnv {
  readonly DB: D1Database;
  readonly LOADER: WorkerLoader;
  readonly CODE_MODE_HOST: CodeModeHostStub;
  readonly OPENAI_API_KEY?: string;
}

/**
 * Register the Code Mode worker Bindings from the Object's own env. The
 * capture runs once per incarnation; every profile shares one handler Layer
 * built over the direct bindings.
 */
const demoBindings: CloudflareBindingSource = ({ env }) =>
  Effect.gen(function* () {
    const bindings = env as DemoEnv;
    const handlers = codeModeHandlersLayer.pipe(
      Layer.provide(
        dynamicWorkerCodeExecutorLayer({
          loader: bindings.LOADER,
          hostStub: bindings.CODE_MODE_HOST,
        }),
      ),
      Layer.provide(invoiceDbSqlLayer(bindings.DB)),
    );

    // Concrete captures per profile (a generic capture helper trips the
    // Model-union inference; see the repository's binding conventions).
    const registered: Array<ResolvedBinding> = [
      yield* DurableWorkerBinding.make(
        Agent.withModel(codeModeAgent, scriptedModel),
        scriptedDigests,
      ).pipe(Effect.provide(handlers)),
      yield* DurableWorkerBinding.make(
        Agent.withModel(codeModeAgent, scriptedWriteProbeModel),
        writeProbeDigests,
      ).pipe(Effect.provide(handlers)),
      yield* DurableWorkerBinding.make(
        Agent.withModel(codeModeAgent, scriptedCteProbeModel),
        cteProbeDigests,
      ).pipe(Effect.provide(handlers)),
    ];
    const liveKey = bindings.OPENAI_API_KEY;
    if (liveKey !== undefined && liveKey.length > 0) {
      registered.push(
        yield* DurableWorkerBinding.make(
          Agent.withModel(codeModeAgent, liveModel(liveKey)),
          liveDigests,
        ).pipe(Effect.provide(handlers)),
      );
    }
    return registered;
  });

/** The demo's durable agent host. */
export class InvoiceAgentConversationObject extends makeConversationObjectClass({
  namespaceBinding: "AGENTS",
  deploymentId: "code-mode-demo-deployment",
  producerPrefix: "code-mode-demo",
  bindings: demoBindings,
  // Snappy demo cadences: settle polls and wake scans fast enough that a
  // single /ask feels interactive while staying alarm-driven underneath.
  ownershipLeaseDuration: 5_000,
  leaseRenewalInterval: 500,
  wakeScanInterval: 200,
  settlementPollInterval: 50,
  abortPollInterval: 50,
  alarmBackoffBase: 25,
  alarmBackoffCap: 500,
  observationPollInterval: 50,
}) {}

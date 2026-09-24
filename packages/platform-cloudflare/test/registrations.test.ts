import {
  ThreadObjectPlacement,
  ThreadObjectNamespace,
  DurableObjectContext,
} from "@effect-agent/platform-cloudflare/cloudflare-bindings";
import * as ThreadObject from "@effect-agent/platform-cloudflare/thread-object";
import { BrowserCrypto } from "@effect/platform-browser";
import { env, runInDurableObject } from "cloudflare:test";
import { Context, Effect, Layer, Option } from "effect";
import { digestDefinitions, digestJson } from "effect-agent/digest";
import { AgentId, SubmissionId, ThreadId } from "effect-agent/identifiers";
import { IdempotencyKey, Principal } from "effect-agent/receipt";
import { DefinitionDigestInput, DeploymentId } from "effect-agent/records";
import { AdmissionRequest, SubmissionLedger } from "effect-agent/submission-ledger";
import { Statement } from "effect/unstable/sql";
import { SqlClient } from "effect/unstable/sql/SqlClient";
import { expect, it } from "vite-plus/test";

import { stubFor } from "./harness.ts";

const options = { deploymentId: "binding-layer", producerPrefix: "binding-layer" };

// Regression: https://github.com/danieljvdm/effect-agent/commit/227b5e8a98ce4d1b303bacbeaf35c15ff45f6c75
it("keeps local submission lookups inside the physical owner, including corrupt identity rows", async () => {
  const thread = `local-lookup-${crypto.randomUUID()}`;
  const colocated = ThreadId.make(`${thread}:colocated`);
  const foreign = ThreadId.make(`${thread}:foreign`);

  await runInDurableObject(stubFor(thread), (_instance, state) =>
    Effect.runPromise(
      Effect.gen(function* () {
        const runtime = ThreadObject.layer([]).pipe(
          Layer.provide(
            Layer.succeed(ThreadObjectPlacement, {
              ownsThread: (id) => id === thread || id === colocated,
            }),
          ),
          Layer.provide(ThreadObject.layerConfig(options)),
          Layer.provide([
            DurableObjectContext.layer(state, env),
            ThreadObjectNamespace.layer(env.THREADS),
          ]),
        );

        const built = yield* Layer.build(runtime);
        const ports = Context.get(built, ThreadObject.ThreadObjectPorts);
        const ledger = Context.get(built, SubmissionLedger);
        const sql = Context.get(built, SqlClient);

        const agentDigests = yield* digestDefinitions(
          DefinitionDigestInput.make({ agent: "local-lookup", model: "none", tools: "none" }),
        );

        const inputDigest = yield* digestJson({});

        const admit = (id: ThreadId) =>
          ledger.admit(
            AdmissionRequest.make({
              threadId: id,
              principal: Principal.make("local-lookup"),
              idempotencyKey: IdempotencyKey.make("local-lookup"),
              agentId: AgentId.make("local-lookup"),
              agentDigests,
              deploymentId: DeploymentId.make("local-lookup"),
              inputPayload: {},
              inputDigest,
            }),
          );

        const owned = yield* admit(ThreadId.make(thread));
        const sibling = yield* admit(colocated);

        expect(Option.getOrThrow(yield* ports.lookupSubmission(owned.submissionId)).threadId).toBe(
          thread,
        );
        expect(
          Option.getOrThrow(yield* ports.lookupSubmission(sibling.submissionId)).threadId,
        ).toBe(colocated);
        expect(yield* ports.lookupSubmission(SubmissionId.make("absent-local-submission"))).toEqual(
          Option.none(),
        );

        const foreignId = SubmissionId.make(`0198f6c0-0000-7000-8000-000000000001:${foreign}`);

        yield* sql`UPDATE effect_agent_submissions SET submission_id=${foreignId} WHERE submission_id=${owned.submissionId}`;
        let queries = 0;

        const rejected = yield* ports.lookupSubmission(foreignId).pipe(
          Effect.provideService(Statement.CurrentTransformer, (statement) => {
            queries++;

            return Effect.succeed(statement);
          }),
          Effect.result,
        );

        expect(rejected).toMatchObject({ _tag: "Failure", failure: { _tag: "LedgerError" } });
        expect(queries).toBe(0);
        expect(
          yield* ports
            .lookupSubmission(SubmissionId.make(`0198f6c0-0000-7000-8000-000000000002:${foreign}`))
            .pipe(Effect.result),
        ).toMatchObject({ _tag: "Failure", failure: { _tag: "LedgerError" } });

        yield* sql`UPDATE effect_agent_submissions SET submission_id=${owned.submissionId},thread_id=${foreign} WHERE submission_id=${foreignId}`;
        expect(yield* ports.lookupSubmission(owned.submissionId).pipe(Effect.result)).toMatchObject(
          { _tag: "Failure", failure: { _tag: "LedgerError" } },
        );
        const opaqueId = SubmissionId.make("opaque-local-submission");

        yield* sql`UPDATE effect_agent_submissions SET submission_id=${opaqueId} WHERE submission_id=${owned.submissionId}`;
        expect(yield* ports.lookupSubmission(opaqueId).pipe(Effect.result)).toMatchObject({
          _tag: "Failure",
          failure: { _tag: "LedgerError" },
        });
        yield* sql`UPDATE effect_agent_submissions SET thread_id=${thread} WHERE submission_id=${opaqueId}`;
        expect(Option.getOrThrow(yield* ports.lookupSubmission(opaqueId)).threadId).toBe(thread);
      }).pipe(Effect.provide(BrowserCrypto.layer), Effect.scoped),
    ),
  );
});

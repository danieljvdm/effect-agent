import * as MemoryNamespace from "@effect-agent/core/MemoryNamespace";
import { type MemoryLookup, type MemoryRecallLimits } from "@effect-agent/core/MemoryReference";
import { MemoryAccess } from "@effect-agent/core/MemoryRevalidation";
import { type MemoryWrite } from "@effect-agent/core/MemoryStore";
import { MemoryScope } from "@effect-agent/core/MemoryStore";
import {
  MemoryOwnerAuthorizer,
  MemoryOwnerIdentity,
  MemoryRpcError,
} from "@effect-agent/storage-cloudflare/MemoryProtocol";
import { Principal } from "@effect-agent/thread/SubmissionLedger";
import { Effect, Layer, Schema } from "effect";

import { MemoryObject, CloudflareMemoryClient } from "../src/CloudflareMemory.ts";

export const Projects = MemoryNamespace.define({
  name: "application/projects",
  version: 1,
  identity: Schema.Struct({ tenantId: Schema.String, projectId: Schema.String }),
});

/** Example host policy. Replace the fixed principal/scope with your application's ACL. */
const authorizer = Layer.effect(
  MemoryOwnerAuthorizer,
  Effect.gen(function* () {
    const owner = yield* MemoryOwnerIdentity;
    const namespace = yield* Projects.restore(owner.namespace.address);

    return {
      authorize: (request) =>
        request.principal === `tenant:${namespace.identity.tenantId}` &&
        request.access.scope === "project"
          ? Effect.void
          : Effect.fail(MemoryRpcError.make({ reason: "denied" })),
    };
  }),
);

export class ProjectMemory extends MemoryObject.make(authorizer) {}

/** Called by any authorized Thread or ingestion job, not by the framework automatically. */
export const correctProjectMemory = Effect.fn("example.correctProjectMemory")(function* (
  namespace: ReturnType<typeof Projects.make>,
  write: MemoryWrite<ReturnType<typeof Projects.make>>,
) {
  const client = yield* CloudflareMemoryClient.make(
    MemoryAccess.make({ namespace, scope: MemoryScope.make("project") }),
    yield* Schema.decodeUnknownEffect(Principal)(`tenant:${namespace.identity.tenantId}`),
  );

  return yield* client.change(write);
});

/** Candidates are application-selected; recall validates and renders them in one operation. */
export const recallProjectMemory = Effect.fn("example.recallProjectMemory")(function* (
  namespace: ReturnType<typeof Projects.make>,
  candidates: MemoryLookup,
  limits: MemoryRecallLimits,
) {
  const memory = yield* CloudflareMemoryClient.make(
    MemoryAccess.make({ namespace, scope: MemoryScope.make("project") }),
    yield* Schema.decodeUnknownEffect(Principal)(`tenant:${namespace.identity.tenantId}`),
  );

  return yield* memory.recall(candidates, limits);
});

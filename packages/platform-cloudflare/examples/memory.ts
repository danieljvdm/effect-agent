import type { MemoryWrite } from "@effect-agent/core";
import { MemoryAccess, MemoryNamespace } from "@effect-agent/core";
import {
  MemoryOwnerAuthorizer,
  MemoryOwnerIdentity,
  MemoryRpcError,
} from "@effect-agent/storage-cloudflare";
import { Effect, Layer, Schema } from "effect";

import { makeMemoryObjectClass, makeCloudflareMemoryClient } from "../src/memory.ts";

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

export class ProjectMemory extends makeMemoryObjectClass(authorizer) {}

/** Called by any authorized Thread or ingestion job, not by the framework automatically. */
export const correctProjectMemory = Effect.fn("example.correctProjectMemory")(function* (
  namespace: ReturnType<typeof Projects.make>,
  write: MemoryWrite<ReturnType<typeof Projects.make>>,
) {
  const client = yield* makeCloudflareMemoryClient(
    MemoryAccess.make({ namespace, scope: "project" }),
    `tenant:${namespace.identity.tenantId}`,
  );
  return yield* client.change(write);
});

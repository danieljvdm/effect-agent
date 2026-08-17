import { CanonicalRecordEnvelope, type OperationCaller, Principal } from "@effect-agent/session";
import { Crypto, Effect, Layer, Schema } from "effect";
import { describe, expect, it } from "vite-plus/test";

import {
  CloudflareConversationClient,
  ConversationObjectNamespace,
  ConversationReadLimitExceeded,
  ObservePageRequest,
  ObservedPage,
  type ConversationObjectRpc,
  encodeHostResponse,
} from "../src/index.ts";

const CONVERSATION_ID = Schema.decodeSync(CanonicalRecordEnvelope.fields.conversationId)(
  "client-read-all",
);
const CALLER: OperationCaller = {
  principal: Schema.decodeSync(Principal)("client-read-all-principal"),
};
const DIGEST_A = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const DIGEST_B = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const DIGEST_C = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";

const recordAt = (sequence: number): CanonicalRecordEnvelope =>
  Schema.decodeUnknownSync(CanonicalRecordEnvelope)({
    conversationId: CONVERSATION_ID,
    batchId: `client-read-all-batch-${sequence}`,
    sequence,
    offset: `memory:${sequence}`,
    record: {
      recordId: `client-read-all-record-${sequence}`,
      family: "conversation",
      schemaVersion: 1,
      createdAt: "2026-08-17T00:00:00.000Z",
      deploymentId: "client-read-all-test",
      payload: {
        _tag: "ConversationCreated",
        agentId: "client-read-all-agent",
        definitions: { agent: DIGEST_A, model: DIGEST_B, tools: DIGEST_C },
      },
    },
  });

const testCryptoLayer = Layer.succeed(
  Crypto.Crypto,
  Crypto.make({
    randomBytes: (size) => new Uint8Array(size),
    digest: (_algorithm, data) => Effect.succeed(data),
  }),
);

describe("CloudflareConversationClient readAll paging", () => {
  it("requests one record for every page, including the maxRecords over-limit probe", async () => {
    const records = [recordAt(1), recordAt(2), recordAt(3)];
    const requests: Array<ObservePageRequest> = [];
    const stub = {
      observePage: async (encoded: unknown): Promise<unknown> => {
        const request = Schema.decodeUnknownSync(ObservePageRequest)(encoded);
        requests.push(request);
        const start = request.afterSequence ?? 0;
        return Effect.runPromise(
          encodeHostResponse(
            ObservedPage.make({ records: records.slice(start, start + request.limit) }),
          ),
        );
      },
    };
    // The fake implements exactly the two host methods the client calls (`idFromName`, `get`)
    // and the one RPC exercised here. This assertion is the test adapter's Cloudflare host seam.
    const namespace = {
      idFromName: () => ({}),
      get: () => stub,
    } as unknown as DurableObjectNamespace<ConversationObjectRpc>;
    const clientLayer = CloudflareConversationClient.layer.pipe(
      Layer.provide(Layer.mergeAll(ConversationObjectNamespace.layer(namespace), testCryptoLayer)),
    );

    const failure = await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* CloudflareConversationClient;
        return yield* client.readAll(CONVERSATION_ID, CALLER, { maxRecords: 2 }).pipe(Effect.flip);
      }).pipe(Effect.provide(clientLayer)),
    );

    expect(failure).toBeInstanceOf(ConversationReadLimitExceeded);
    expect(requests.map(({ limit }) => limit)).toEqual([1, 1, 1]);
    expect(requests.map(({ afterSequence }) => afterSequence)).toEqual([undefined, 1, 2]);
  });
});

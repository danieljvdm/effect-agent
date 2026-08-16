import { ConversationId } from "@effect-agent/core";
import {
  CloudflareConversationClient,
  CodeModeHostEntrypoint,
  ConversationObjectNamespace,
  dynamicWorkerImplementation,
  type CodeModeHostStub,
  type ConversationObjectRpc,
} from "@effect-agent/platform-cloudflare";
import {
  IdempotencyKey,
  Principal,
  type CanonicalRecordEnvelope,
  type CanonicalSequence,
  type DefinitionDigests,
} from "@effect-agent/session";
import { Cause, Context, Effect, Exit, Layer, Schema, Stream } from "effect";
import { Worker, WorkerEnvironment } from "effect-cf";
import type * as SqlClient from "effect/unstable/sql/SqlClient";

import { codeModeAgent } from "./agent.ts";
import {
  cteProbeDigests,
  InvoiceAgentConversationObject,
  liveDigests,
  scriptedDigests,
  writeProbeDigests,
} from "./conversation.ts";
import { invoiceDbSqlLayer, seedInvoices } from "./db.ts";

/**
 * The demo Worker is a thin client of the DC assembly (the Durable Cloudflare
 * assembly: Conversation Objects over Durable Object SQLite): it
 * authenticates the request, submits the question to the Conversation Object
 * — every step lands in the DC assembly's append-only canonical log and an
 * alarm drives the Run to settlement — awaits the settlement, and reads the
 * log back to build the Code Mode receipt. Models and tool handlers live
 * INSIDE the Object, wired from its own env bindings; the Worker submits only
 * a definition plus digests.
 *
 * `InvoiceAgentConversationObject` and `CodeModeHostEntrypoint` are exported
 * for the Worker runtime; the host entrypoint is bound to itself as
 * `CODE_MODE_HOST` (see wrangler.jsonc), matching the production
 * `ctx.exports.CodeModeHostEntrypoint()` seam.
 */
export { InvoiceAgentConversationObject, CodeModeHostEntrypoint };

declare global {
  namespace Cloudflare {
    interface Env {
      readonly AGENTS: DurableObjectNamespace<ConversationObjectRpc & Rpc.DurableObjectBranded>;
      readonly DB: D1Database;
      readonly LOADER: WorkerLoader;
      readonly CODE_MODE_HOST: CodeModeHostStub;
      readonly OPENAI_API_KEY?: string;
      /**
       * Optional shared secret. When set, `/ask` requires a matching
       * `Authorization: Bearer <token>` header — set it whenever
       * `OPENAI_API_KEY` is deployed so the paid live endpoint cannot be
       * driven anonymously.
       */
      readonly DEMO_AUTH_TOKEN?: string;
    }
  }
}

const decodeConversationId = Schema.decodeSync(ConversationId);
const decodeIdempotencyKey = Schema.decodeSync(IdempotencyKey);
const demoPrincipal = Schema.decodeSync(Principal)("code-mode-demo");

/**
 * Identifier authority as an owned service: business logic never touches
 * ambient randomness — the capability is provided at the Worker composition
 * root, so the authority and requirement stay visible.
 */
class DemoIdentifiers extends Context.Service<
  DemoIdentifiers,
  {
    readonly nextConversationId: Effect.Effect<ConversationId>;
    readonly nextIdempotencyKey: Effect.Effect<IdempotencyKey>;
  }
>()("@effect-agent/example-code-mode-cloudflare/DemoIdentifiers") {}

const demoIdentifiersLayer = Layer.succeed(DemoIdentifiers)({
  nextConversationId: Effect.sync(() =>
    decodeConversationId(`conversation-${crypto.randomUUID()}`),
  ),
  nextIdempotencyKey: Effect.sync(() => decodeIdempotencyKey(crypto.randomUUID())),
});

const DEFAULT_QUESTION = "Which customers have more than $10,000 in revenue?";

/** The logical request body, decoded ONCE at the HTTP boundary. */
const AskRequestBody = Schema.Struct({
  question: Schema.optionalKey(Schema.String),
});
const decodeAskRequestBody = Schema.decodeUnknownOption(AskRequestBody);

const MAX_BODY_BYTES = 64 * 1024;

class BodyTooLarge extends Schema.TaggedError<BodyTooLarge>()("BodyTooLarge", {}) {}

/**
 * Read the request body through a size-limited stream: the bound is enforced
 * on bytes ACTUALLY consumed, so chunked requests without (or with forged)
 * `Content-Length` cannot buffer past `MAX_BODY_BYTES`. Malformed bodies fall
 * to the default question; only oversize is a typed failure (413 at the edge).
 */
const readQuestion = (request: Request): Effect.Effect<string, BodyTooLarge> =>
  Effect.tryPromise(async () => {
    const reader = request.body?.getReader();
    if (reader === undefined) return "";
    const chunks: Array<Uint8Array> = [];
    let received = 0;
    for (;;) {
      const chunk = await reader.read();
      if (chunk.done) break;
      received += chunk.value.byteLength;
      if (received > MAX_BODY_BYTES) {
        await reader.cancel();
        throw new BodyTooLarge();
      }
      chunks.push(chunk.value);
    }
    const merged = new Uint8Array(received);
    let offset = 0;
    for (const chunk of chunks) {
      merged.set(chunk, offset);
      offset += chunk.byteLength;
    }
    return new TextDecoder().decode(merged);
  }).pipe(
    Effect.map((textBody) => {
      let parsed: unknown;
      try {
        parsed = JSON.parse(textBody);
      } catch {
        return DEFAULT_QUESTION;
      }
      const body = decodeAskRequestBody(parsed);
      const question = body._tag === "Some" ? body.value.question : undefined;
      return typeof question === "string" && question.trim().length > 0
        ? // Bound the question so an oversized body cannot drive an unbounded prompt.
          question.slice(0, 2_000)
        : DEFAULT_QUESTION;
    }),
    Effect.catch((error) =>
      error.cause instanceof BodyTooLarge
        ? Effect.fail(new BodyTooLarge())
        : Effect.succeed(DEFAULT_QUESTION),
    ),
  );

/** Which registered binding should serve this request. */
const digestsFor = (probe: string | null, live: boolean): DefinitionDigests => {
  if (live) return liveDigests;
  if (probe === "write") return writeProbeDigests;
  if (probe === "cte") return cteProbeDigests;
  return scriptedDigests;
};

const payloadOf = (
  envelope: CanonicalRecordEnvelope,
): { readonly _tag: string } & Record<string, unknown> =>
  envelope.record.payload as unknown as { readonly _tag: string } & Record<string, unknown>;

// ---------------------------------------------------------------------------
// Receipt extraction: model output is UNTRUSTED, so every step of the durable
// log that originated with the model is Schema-decoded fail-closed before it
// contributes to provenance. Duplicate or malformed ids disqualify
// single-program attribution rather than silently colliding.
// ---------------------------------------------------------------------------

const EncodedPrompt = Schema.Struct({ content: Schema.Array(Schema.Unknown) });
const decodeEncodedPrompt = Schema.decodeUnknownOption(EncodedPrompt);

const PromptMessage = Schema.Struct({ content: Schema.Unknown });
const decodePromptMessage = Schema.decodeUnknownOption(PromptMessage);

const ToolCallPart = Schema.Struct({
  type: Schema.Literal("tool-call"),
  id: Schema.String,
  name: Schema.String,
  params: Schema.optionalKey(Schema.Unknown),
});
const decodeToolCallPart = Schema.decodeUnknownOption(ToolCallPart);

const CodeParams = Schema.Struct({ code: Schema.String });
const decodeCodeParams = Schema.decodeUnknownOption(CodeParams);

const SettledValue = Schema.Struct({
  result: Schema.optionalKey(Schema.Unknown),
  logs: Schema.optionalKey(Schema.Array(Schema.Unknown)),
});
const decodeSettledValue = Schema.decodeUnknownOption(SettledValue);

const AnswerOutput = Schema.Struct({ answer: Schema.String });
const decodeAnswerOutput = Schema.decodeUnknownOption(AnswerOutput);

/**
 * Build the Code Mode receipt from the canonical log. Readonly tools write no
 * `ToolCallPrepared` records (those guard uncertain-class external effects),
 * so the model's program comes from the committed Turn messages in
 * `ModelResponseRecorded`, its outcome from the matching `ToolCallSettled`,
 * and the answer from `SubmissionSettled.result`. Exactly-one successful call
 * with a unique, well-formed id attaches a single program/result; ambiguous
 * runs stay honest with counts only.
 */
const receiptFromRecords = (records: ReadonlyArray<CanonicalRecordEnvelope>) => {
  const declared = new Map<string, string | undefined>();
  const settled = new Map<string, { result: unknown; logs: ReadonlyArray<unknown> }>();
  const duplicateIds = new Set<string>();
  let declaredCalls = 0;
  let answer = "";
  for (const envelope of records) {
    const payload = payloadOf(envelope);
    if (payload._tag === "ModelResponseRecorded") {
      const prompt = decodeEncodedPrompt(payload.messages);
      if (prompt._tag !== "Some") continue;
      for (const rawMessage of prompt.value.content) {
        const message = decodePromptMessage(rawMessage);
        if (message._tag !== "Some" || !Array.isArray(message.value.content)) continue;
        for (const rawPart of message.value.content) {
          const part = decodeToolCallPart(rawPart);
          if (part._tag !== "Some" || part.value.name !== "run_javascript") continue;
          declaredCalls += 1;
          if (declared.has(part.value.id)) duplicateIds.add(part.value.id);
          const params = decodeCodeParams(part.value.params);
          declared.set(part.value.id, params._tag === "Some" ? params.value.code : undefined);
        }
      }
    }
    if (
      payload._tag === "ToolCallSettled" &&
      payload.toolName === "run_javascript" &&
      payload.isFailure !== true &&
      typeof payload.toolCallId === "string"
    ) {
      if (settled.has(payload.toolCallId)) duplicateIds.add(payload.toolCallId);
      const value = decodeSettledValue(payload.result);
      settled.set(payload.toolCallId, {
        result: value._tag === "Some" ? value.value.result : undefined,
        logs: value._tag === "Some" ? (value.value.logs ?? []) : [],
      });
    }
    if (payload._tag === "SubmissionSettled") {
      const output = decodeAnswerOutput(payload.result);
      if (output._tag === "Some") {
        answer = output.value.answer;
      }
    }
  }
  const settledIds = [...settled.keys()];
  const soleId =
    settledIds.length === 1 && declared.has(settledIds[0]) && !duplicateIds.has(settledIds[0])
      ? settledIds[0]
      : undefined;
  const executed = soleId === undefined ? undefined : settled.get(soleId);
  return {
    answer,
    codeMode: {
      used: settled.size > 0,
      tool: "run_javascript",
      executor: dynamicWorkerImplementation.identity,
      calls: declaredCalls,
      program: soleId === undefined ? undefined : declared.get(soleId),
      result: executed?.result,
      logs: executed?.logs,
    },
    records: records.map((envelope) => payloadOf(envelope)._tag),
  };
};

// ---------------------------------------------------------------------------
// Streaming: the canonical-log tail as a scoped Effect Stream. Polling uses
// the owned Clock (`Effect.sleep`), inactivity ends through `Stream.timeout`,
// read failures terminate the stream with a stable error record, and the
// ReadableStream bridge propagates consumer cancellation as fiber
// interruption — no naked timers, no wall-clock reads, no orphaned polls.
// ---------------------------------------------------------------------------

const encoder = new TextEncoder();

const line = (value: unknown): Uint8Array => encoder.encode(`${JSON.stringify(value)}\n`);

const streamCanonicalLog = (
  conversationId: ConversationId,
  liveMode: boolean,
): Effect.Effect<ReadableStream<Uint8Array>, never, CloudflareConversationClient> =>
  Effect.gen(function* () {
    const client = yield* CloudflareConversationClient;
    const seen: Array<CanonicalRecordEnvelope> = [];

    const pages = Stream.unfold(undefined as CanonicalSequence | undefined, (after) =>
      Effect.gen(function* () {
        for (;;) {
          const page = yield* client.readPage(conversationId, { afterSequence: after });
          if (page.length > 0) {
            return [page, page[page.length - 1].sequence] as const;
          }
          yield* Effect.sleep("100 millis");
        }
      }),
    );

    const lines = pages.pipe(
      Stream.flatMap((page) => Stream.fromIterable(page)),
      Stream.takeUntil((envelope) => payloadOf(envelope)._tag === "SubmissionSettled"),
      // Inactivity bound: if no record commits for this long, end the tail.
      Stream.timeout("120 seconds"),
      Stream.tap((envelope) => Effect.sync(() => seen.push(envelope))),
      Stream.map((envelope) =>
        line({ sequence: envelope.sequence, record: payloadOf(envelope)._tag }),
      ),
      Stream.concat(
        Stream.fromEffect(
          Effect.sync(() => {
            const last = seen[seen.length - 1];
            const settledRun = last !== undefined && payloadOf(last)._tag === "SubmissionSettled";
            return settledRun
              ? line({
                  done: true,
                  conversationId,
                  outcome: payloadOf(last).outcome,
                  ...receiptFromRecords(seen),
                  profile: liveMode ? "openai" : "scripted",
                })
              : line({ error: "timed out waiting for settlement" });
          }),
        ),
      ),
      // A TYPED read failure is not "no records yet": log it server-side and
      // terminate the stream with a stable error record. Defects and
      // interruption propagate untouched.
      Stream.catch((error) =>
        Stream.fromEffect(
          Effect.sync(() => {
            console.error("canonical log observation failed", error);
            return line({ error: "observation failed" });
          }),
        ),
      ),
    );

    return yield* Stream.toReadableStreamEffect(lines);
  });

const askHandler = Effect.gen(function* () {
  const request = yield* Worker.NativeRequest;
  const env = yield* WorkerEnvironment;
  const url = new URL(request.url);
  if (url.pathname !== "/ask") {
    return new Response(
      "POST /ask { question } — durable Code Mode runs on a Conversation Object over D1",
      { status: url.pathname === "/" ? 200 : 404 },
    );
  }
  if (request.method !== "POST") {
    return Response.json({ error: "POST required" }, { status: 405 });
  }
  // Early rejection when the client declares an oversized body; the REAL
  // bound is enforced on consumed bytes inside `readQuestion`.
  const declaredLength = Number(request.headers.get("content-length") ?? "0");
  if (!Number.isFinite(declaredLength) || declaredLength > MAX_BODY_BYTES) {
    return Response.json({ error: "request body too large" }, { status: 413 });
  }
  const liveMode = env.OPENAI_API_KEY !== undefined && env.OPENAI_API_KEY.length > 0;
  const authToken = env.DEMO_AUTH_TOKEN;
  const hasAuthToken = authToken !== undefined && authToken.length > 0;
  // Fail CLOSED on the paid path: if the live model is enabled but no shared
  // secret is configured, refuse rather than serve paid inference to
  // anonymous callers. The offline scripted default needs no secret.
  if (liveMode && !hasAuthToken) {
    return Response.json(
      { error: "server misconfigured: DEMO_AUTH_TOKEN must be set when OPENAI_API_KEY is" },
      { status: 503 },
    );
  }
  // When a shared secret is configured, require a matching bearer token.
  if (hasAuthToken && request.headers.get("authorization") !== `Bearer ${authToken}`) {
    return Response.json({ error: "unauthorized" }, { status: 401 });
  }
  const questionOutcome = yield* readQuestion(request).pipe(Effect.exit);
  if (Exit.isFailure(questionOutcome)) {
    return Response.json({ error: "request body too large" }, { status: 413 });
  }
  const question = questionOutcome.value;

  // Idempotent seed over the root-provided D1 SqlClient: a persistence
  // failure is EXPECTED — log the cause server-side, answer a generic 503.
  const seeded = yield* seedInvoices.pipe(Effect.exit);
  if (Exit.isFailure(seeded)) {
    console.error("invoice database seed failed", Cause.squash(seeded.cause));
    return Response.json({ error: "invoice database unavailable" }, { status: 503 });
  }

  const identifiers = yield* DemoIdentifiers;
  const conversationId = yield* identifiers.nextConversationId;
  const submitOptions = {
    conversationId,
    principal: demoPrincipal,
    idempotencyKey: yield* identifiers.nextIdempotencyKey,
    definitions: digestsFor(url.searchParams.get("probe"), liveMode),
  };

  // Streaming mode (`?stream=1`, curl -N): submit, then emit one NDJSON line
  // per canonical record AS IT COMMITS, closing with the receipt once
  // `SubmissionSettled` lands. The observer is optional by construction — the
  // durable run does not depend on anyone watching it.
  if (url.searchParams.get("stream") === "1") {
    const client = yield* CloudflareConversationClient;
    const submitted = yield* client
      .submit({ definition: codeModeAgent }, { question }, submitOptions)
      .pipe(Effect.exit);
    if (Exit.isFailure(submitted)) {
      console.error("durable submit failed", Cause.squash(submitted.cause));
      return Response.json({ error: "the durable run failed" }, { status: 500 });
    }
    const body = yield* streamCanonicalLog(conversationId, liveMode);
    return new Response(body, {
      headers: { "content-type": "application/x-ndjson", "cache-control": "no-cache" },
    });
  }

  const outcome = yield* Effect.gen(function* () {
    const client = yield* CloudflareConversationClient;
    const receipt = yield* client.submit(
      { definition: codeModeAgent },
      { question },
      submitOptions,
    );
    const settlement = yield* client.awaitSettlement(receipt);
    const records = yield* client.readAll(conversationId);
    return { settlement, records };
  }).pipe(Effect.exit);

  if (Exit.isSuccess(outcome)) {
    const { settlement, records } = outcome.value;
    return Response.json({
      conversationId,
      outcome: settlement.outcome,
      ...receiptFromRecords(records),
      profile: liveMode ? "openai" : "scripted",
    });
  }
  // Never expose internal causes to callers: log server-side, answer generic.
  console.error("durable run failed", Cause.squash(outcome.cause));
  return Response.json({ error: "the durable run failed" }, { status: 500 });
});

const clientLayer = Layer.unwrap(
  Effect.gen(function* () {
    const env = yield* WorkerEnvironment;
    return CloudflareConversationClient.layer.pipe(
      Layer.provide(ConversationObjectNamespace.layer(env.AGENTS)),
    );
  }),
);

/** D1 `SqlClient` for the Worker-side seed, provided at the composition root. */
const seedSqlLayer: Layer.Layer<SqlClient.SqlClient, never, WorkerEnvironment> = Layer.unwrap(
  Effect.gen(function* () {
    const env = yield* WorkerEnvironment;
    return invoiceDbSqlLayer(env.DB);
  }),
);

export default Worker.make(Layer.mergeAll(clientLayer, seedSqlLayer, demoIdentifiersLayer), {
  fetch: askHandler,
});

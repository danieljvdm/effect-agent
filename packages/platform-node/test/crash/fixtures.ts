import { Agent, AgentPolicy, ConversationId } from "@effect-agent/core";
import {
  DefinitionDigests,
  Digest,
  IdempotencyKey,
  Principal,
  Receipt,
  Settlement,
  type DurableSubmitOptions,
} from "@effect-agent/session";
import { Effect, Layer, Ref, Schema, Stream } from "effect";
import { LanguageModel, Model, Tool, Toolkit, type Response } from "effect/unstable/ai";

/**
 * Shared contract between the crash-harness test (`crash.test.ts`) and the child worker process
 * (`worker-entry.ts`). Both sides import the SAME agent definitions, identities, and stdout
 * message Schemas so a restarted "client" resubmits byte-identical canonical input.
 */

/** Environment variable names the harness uses to drive one child worker process. */
export const CrashEnv = {
  database: "EFFECT_AGENT_DB",
  scenario: "EFFECT_AGENT_SCENARIO",
  conversation: "EFFECT_AGENT_CONVERSATION",
  idempotencyKey: "EFFECT_AGENT_KEY",
  killAt: "EFFECT_AGENT_KILL_AT",
  killAtStorage: "EFFECT_AGENT_KILL_AT_STORAGE",
  leaseMillis: "EFFECT_AGENT_LEASE_MS",
  markerFile: "EFFECT_AGENT_MARKER_FILE",
  releaseFile: "EFFECT_AGENT_RELEASE_FILE",
} as const;

/**
 * Child scenario scripts. Every scenario first assembles the full DN stack against the SQLite
 * file named by `EFFECT_AGENT_DB`:
 *
 * - `submit` — durably submit one Submission and print its Receipt.
 * - `abort-ready` — submit, then durably abort the still-unclaimed Submission.
 * - `run` — submit, then drain the lane to Settlement with a single-turn model.
 * - `run-two` — submit two FIFO Submissions, then drain the lane.
 * - `run-blocked` — submit, commit Turn 1 (a tool call), then block Turn 2's model stream until
 *   `EFFECT_AGENT_RELEASE_FILE` appears (writing `EFFECT_AGENT_MARKER_FILE` first).
 * - `abort-active` — submit, then block inside Turn 1's model stream forever (marker written),
 *   waiting to be aborted through the durable ledger.
 */
export const CrashScenario = Schema.Literals([
  "submit",
  "abort-ready",
  "run",
  "run-two",
  "run-blocked",
  "abort-active",
]);
export type CrashScenario = typeof CrashScenario.Type;

/** Exit code armed at a kill failpoint (`process.exit`, mirroring SIGKILL's 128+9). */
export const KILL_EXIT_CODE = 137;
/** Exit code for a child whose Attempt was fenced out by a newer producer epoch (expected). */
export const FENCED_EXIT_CODE = 87;

export const CRASH_DEPLOYMENT_ID = "deployment-crash";
export const CHILD_PRODUCER_ID = "producer-crash-child";
export const HOST_PRODUCER_ID = "producer-crash-host";
/** The canonical question; both sides must submit the exact same payload for digest replay. */
export const CRASH_QUESTION = "does accepted work survive a process kill?";
export const CHILD_ANSWER = '{"answer":"child"}';
export const STALE_ANSWER = '{"answer":"stale"}';
export const FRESH_ANSWER = '{"answer":"fresh"}';

const SHA_A = Schema.decodeSync(Digest)("a".repeat(64));
export const CRASH_DIGESTS = DefinitionDigests.make({ agent: SHA_A, model: SHA_A, tools: SHA_A });
export const CRASH_PRINCIPAL = Schema.decodeSync(Principal)("principal-crash");

export const decodeConversationId = Schema.decodeSync(ConversationId);
export const decodeIdempotencyKey = Schema.decodeSync(IdempotencyKey);

export const crashSubmitOptions = (
  conversationId: string,
  idempotencyKey: string,
): DurableSubmitOptions => ({
  conversationId: decodeConversationId(conversationId),
  principal: CRASH_PRINCIPAL,
  idempotencyKey: decodeIdempotencyKey(idempotencyKey),
  definitions: CRASH_DIGESTS,
});

const usage = { inputTokens: {}, outputTokens: {} };

export const finalParts = (text: string): ReadonlyArray<Response.StreamPartEncoded> => [
  { type: "text-start", id: "answer" },
  { type: "text-delta", id: "answer", delta: text },
  { type: "text-end", id: "answer" },
  { type: "finish", reason: "stop", usage },
];

export const TOOL_CALL_ID = "search-1";

export const toolCallParts: ReadonlyArray<Response.StreamPartEncoded> = [
  {
    type: "tool-call",
    id: TOOL_CALL_ID,
    name: "search",
    params: { query: "sea" },
    providerExecuted: false,
  },
  { type: "finish", reason: "tool-calls", usage },
];

/** Scripted model whose per-call scripts are full streams (so a call can block on a file). */
export const makeScriptedStreamModel = Effect.fn("CrashFixtures.makeScriptedStreamModel")(
  function* (script: (call: number) => Stream.Stream<Response.StreamPartEncoded>) {
    const calls = yield* Ref.make(0);
    return Model.make(
      "scripted",
      "crash-harness",
      Layer.effect(
        LanguageModel.LanguageModel,
        LanguageModel.make({
          generateText: () => Effect.succeed([]),
          streamText: () =>
            Stream.unwrap(
              Ref.getAndUpdate(calls, (call) => call + 1).pipe(Effect.map((call) => script(call))),
            ),
        }),
      ),
    );
  },
);

/** Scripted model that emits a fixed part list per call. */
export const makeScriptedModel = (
  script: (call: number) => ReadonlyArray<Response.StreamPartEncoded>,
) => makeScriptedStreamModel((call) => Stream.fromIterable(script(call)));

export const plannerDefinition = Agent.define("crash-planner", {
  input: Schema.Struct({ question: Schema.String }),
  output: Schema.Struct({ answer: Schema.String }),
  instructions: ({ question }) => `Answer ${question} as JSON.`,
  toolkit: Toolkit.empty,
  policy: AgentPolicy.make({
    maxTurns: 3,
    maxToolCalls: 2,
    maxDuration: "30 seconds",
    toolConcurrency: 1,
  }),
});

const Search = Tool.make("search", {
  parameters: Schema.Struct({ query: Schema.String }),
  success: Schema.Struct({ available: Schema.Boolean }),
});
export const searchTools = Toolkit.make(Search);

export const searchDefinition = Agent.define("crash-search", {
  input: Schema.Struct({ question: Schema.String }),
  output: Schema.Struct({ answer: Schema.String }),
  instructions: "Search before answering.",
  toolkit: searchTools,
  policy: AgentPolicy.make({
    maxTurns: 3,
    maxToolCalls: 2,
    maxDuration: "30 seconds",
    toolConcurrency: 1,
  }),
});

export const searchToolLayer = searchTools.toLayer({
  search: () => Effect.succeed({ available: true }),
});

/** Line-framed JSON messages the child prints on stdout for the harness to decode. */
export const ChildMessage = Schema.Union([
  Schema.Struct({
    kind: Schema.Literal("receipt"),
    key: Schema.String,
    receipt: Receipt,
  }),
  Schema.Struct({
    kind: Schema.Literal("settlements"),
    settlements: Schema.Array(Settlement),
  }),
  Schema.Struct({
    kind: Schema.Literal("worker-failure"),
    tag: Schema.String,
  }),
]);
export type ChildMessage = typeof ChildMessage.Type;

export const encodeChildMessage = Schema.encodeSync(ChildMessage);
export const decodeChildMessageOption = Schema.decodeUnknownOption(ChildMessage);

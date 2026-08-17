import { Agent, AgentPolicy, ConversationId, ToolCallId } from "@effect-agent/core";
import { DurableStep, DurableStepError, ToolExecutionClass } from "@effect-agent/engine";
import {
  DefinitionDigests,
  Digest,
  DurableRuntimeFailpointError,
  DurableWorkerBinding,
  IdempotencyKey,
  Principal,
  OperationCaller,
  ReconciliationSafeToRetry,
  ReconciliationUncertain,
  ToolReconciler,
  type DurableRuntimeFailpointHandler,
  type DurableRuntimeFailpointLocation,
  type DurableSubmitOptions,
  type ResolvedBinding,
} from "@effect-agent/session";
import {
  evictionFailpointHandler,
  type DoStorageFailpointHandler,
  type DoStorageFailpointLocation,
} from "@effect-agent/storage-cloudflare";
import { Duration, Effect, Layer, Schema, Stream } from "effect";
import { LanguageModel, Model, Tool, Toolkit, type Response } from "effect/unstable/ai";

/**
 * Workerd-safe eviction-harness fixtures (plan §3, §4 WP3). The vitest pool runs test files
 * and the worker entry in ONE isolate, so this module's state is shared between the tests
 * and the Durable Object instances: armed failpoints are consumed exactly once by the doomed
 * incarnation (a fresh incarnation starts unarmed and converges), the release gates unblock
 * hanging scripted models, and the in-memory supplier store plays the crash harness's
 * file-backed external-truth role — it survives `ctx.abort()` exactly like a real supplier's
 * ledger survives a process kill, keyed per test by the Conversation-unique `ref`.
 */

// ---------------------------------------------------------------------------
// Armed failpoints (consumed exactly once, keyed by Conversation name)
// ---------------------------------------------------------------------------

const armedStorageEvictions = new Map<string, Array<DoStorageFailpointLocation>>();
const armedRuntimeEvictions = new Map<string, Array<DurableRuntimeFailpointLocation>>();
/** Typed (thrown, NOT abort) pass failures for the workerd alarm-retry row. */
export const armedRuntimeFailures = new Map<string, DurableRuntimeFailpointLocation>();

/**
 * Arm an ORDERED queue of eviction locations for one Conversation. Chained rows (a location
 * only reachable through the recovery of an earlier abort) must arm the whole chain UP
 * FRONT: due alarms auto-fire in the pool, so the recovery pass after an abort can run
 * within milliseconds — long before a test observing the first abort could arm the next
 * location. Each hit consumes exactly its queue head, so the doomed incarnations die in
 * order and the final one converges unarmed.
 */
export const armStorageEviction = (
  conversation: string,
  ...locations: ReadonlyArray<DoStorageFailpointLocation>
): void => {
  const queue = armedStorageEvictions.get(conversation) ?? [];
  queue.push(...locations);
  armedStorageEvictions.set(conversation, queue);
};

export const armRuntimeEviction = (
  conversation: string,
  ...locations: ReadonlyArray<DurableRuntimeFailpointLocation>
): void => {
  const queue = armedRuntimeEvictions.get(conversation) ?? [];
  queue.push(...locations);
  armedRuntimeEvictions.set(conversation, queue);
};

/** Locations still armed (either kind); zero means every armed eviction actually fired. */
export const armedEvictionsRemaining = (conversation: string): number =>
  (armedStorageEvictions.get(conversation)?.length ?? 0) +
  (armedRuntimeEvictions.get(conversation)?.length ?? 0);

/** Storage failpoint factory for `makeConversationObjectClass`: armed hit → `ctx.abort()`. */
export const storageEvictionFailpoint = (ctx: DurableObjectState): DoStorageFailpointHandler =>
  evictionFailpointHandler({
    isArmed: (location) =>
      Effect.sync(() => {
        const name = ctx.id.name;
        if (name === undefined) return false;
        const queue = armedStorageEvictions.get(name);
        if (queue === undefined || queue[0] !== location) return false;
        // Consume BEFORE the abort: the next incarnation must run past this location.
        queue.shift();
        if (queue.length === 0) armedStorageEvictions.delete(name);
        return true;
      }),
    evict: () => {
      ctx.abort("armed storage eviction failpoint");
    },
  });

/** Coordinator failpoint factory: armed eviction → `ctx.abort()`; armed failure → typed. */
export const runtimeEvictionFailpoint =
  (ctx: DurableObjectState): DurableRuntimeFailpointHandler =>
  (location) =>
    Effect.suspend(() => {
      const name = ctx.id.name;
      if (name === undefined) return Effect.void;
      if (armedRuntimeFailures.get(name) === location) {
        armedRuntimeFailures.delete(name);
        return Effect.fail(DurableRuntimeFailpointError.make({ location }));
      }
      const queue = armedRuntimeEvictions.get(name);
      if (queue !== undefined && queue[0] === location) {
        queue.shift();
        if (queue.length === 0) armedRuntimeEvictions.delete(name);
        return Effect.sync((): never => {
          ctx.abort("armed runtime eviction failpoint");
          // `ctx.abort()` never returns; this defensive throw keeps the guarantee even if a
          // harness runtime lets it return.
          throw new Error(`Durable Object eviction did not interrupt execution at ${location}.`);
        });
      }
      return Effect.void;
    });

// ---------------------------------------------------------------------------
// Release gates (sticky; hanging scripted models poll them)
// ---------------------------------------------------------------------------

const releasedGates = new Set<string>();

export const releaseGate = (ref: string): void => {
  releasedGates.add(ref);
};

export const resetGate = (ref: string): void => {
  releasedGates.delete(ref);
};

const awaitGate = (ref: string): Effect.Effect<void> =>
  Effect.gen(function* () {
    while (!releasedGates.has(ref)) {
      yield* Effect.sleep(Duration.millis(10));
    }
  });

// ---------------------------------------------------------------------------
// In-memory external supplier store (the never-fabricate reference set)
// ---------------------------------------------------------------------------

export interface SupplierRecord {
  readonly op: string;
  readonly key: string;
  readonly value: string;
}

const supplierLog: Array<SupplierRecord> = [];

export const recordSupplierCall = (op: string, key: string, value: string): void => {
  supplierLog.push({ op, key, value });
};

/** Exact per-operation invocation counts for one `ref` — the honesty-claim currency. */
export const supplierCountsFor = (ref: string): Record<string, number> => {
  const counts: Record<string, number> = {};
  for (const record of supplierLog) {
    if (record.key !== ref) continue;
    counts[record.op] = (counts[record.op] ?? 0) + 1;
  }
  return counts;
};

/** Every value the external supplier ever produced for one `ref`. */
export const supplierValuesFor = (ref: string): ReadonlySet<string> =>
  new Set(supplierLog.filter((record) => record.key === ref).map((record) => record.value));

// ---------------------------------------------------------------------------
// Identities
// ---------------------------------------------------------------------------

export const decodeConversationId = Schema.decodeSync(ConversationId);
export const decodeIdempotencyKey = Schema.decodeSync(IdempotencyKey);

export const TEST_PRINCIPAL = Schema.decodeSync(Principal)("principal-cf-eviction");
export const TEST_CALLER = OperationCaller.make({ principal: TEST_PRINCIPAL });
const SHA_A = Schema.decodeSync(Digest)("a".repeat(64));
export const TEST_DIGESTS = DefinitionDigests.make({ agent: SHA_A, model: SHA_A, tools: SHA_A });

export const DEPLOYMENT_ID = "cf-test-deployment";
export const PRODUCER_PREFIX = "cf-test-producer";
export const CONVERSATIONS_BINDING = "CONVERSATIONS";

export const submitOptions = (
  conversation: string,
  idempotencyKey: string,
): DurableSubmitOptions => ({
  conversationId: decodeConversationId(conversation),
  principal: TEST_PRINCIPAL,
  idempotencyKey: decodeIdempotencyKey(idempotencyKey),
  definitions: TEST_DIGESTS,
});

// ---------------------------------------------------------------------------
// Scripted prompt-aware models
// ---------------------------------------------------------------------------

const usage = { inputTokens: {}, outputTokens: {} };

export const FINAL_ANSWER = '{"answer":"done"}';

export const finalParts = (text: string): ReadonlyArray<Response.StreamPartEncoded> => [
  { type: "text-start", id: "answer" },
  { type: "text-delta", id: "answer", delta: text },
  { type: "text-end", id: "answer" },
  { type: "finish", reason: "stop", usage },
];

export const SEARCH_CALL_ID = "search-1";
export const BOOK_CALL_ID = "book-1";
export const ITINERARY_CALL_ID = "itinerary-1";
/** The booking Tool Call identity as the branded type the resolution commands carry. */
export const BOOK_TOOL_CALL_ID: ToolCallId = Schema.decodeSync(ToolCallId)(BOOK_CALL_ID);

const searchToolCallParts: ReadonlyArray<Response.StreamPartEncoded> = [
  {
    type: "tool-call",
    id: SEARCH_CALL_ID,
    name: "search",
    params: { query: "sea" },
    providerExecuted: false,
  },
  { type: "finish", reason: "tool-calls", usage },
];

const bookToolCallParts = (ref: string): ReadonlyArray<Response.StreamPartEncoded> => [
  {
    type: "tool-call",
    id: BOOK_CALL_ID,
    name: "book",
    params: { ref },
    providerExecuted: false,
  },
  { type: "finish", reason: "tool-calls", usage },
];

const itineraryToolCallParts = (ref: string): ReadonlyArray<Response.StreamPartEncoded> => [
  {
    type: "tool-call",
    id: ITINERARY_CALL_ID,
    name: "itinerary",
    params: { ref },
    providerExecuted: false,
  },
  { type: "finish", reason: "tool-calls", usage },
];

/** Extract the Conversation-unique `ref` the instructions embed as `[ref:...]`. */
const refFromPrompt = (promptJson: string): string => {
  const match = /\[ref:([^\]]+)\]/.exec(promptJson);
  return match?.[1] ?? "unknown-ref";
};

/**
 * A PROMPT-AWARE scripted model: the response is a pure function of the normalized request,
 * never of an in-memory call counter. This is what makes ONE registered Binding correct at
 * every resume point — a fresh incarnation's Attempt sees the committed history in its
 * prompt and the model continues from exactly there, the way a real model would.
 */
const promptAwareModel = (
  name: string,
  decide: (promptJson: string) => Stream.Stream<Response.StreamPartEncoded>,
) =>
  Model.make(
    "scripted",
    name,
    Layer.effect(
      LanguageModel.LanguageModel,
      LanguageModel.make({
        generateText: () => Effect.succeed([]),
        streamText: (options) =>
          Stream.unwrap(Effect.sync(() => decide(JSON.stringify(options.prompt)))),
      }),
    ),
  );

// ---------------------------------------------------------------------------
// Agent definitions (crash-fixture shapes, workerd-safe handlers)
// ---------------------------------------------------------------------------

const FixtureInput = Schema.Struct({ question: Schema.String, ref: Schema.String });
const FixtureOutput = Schema.Struct({ answer: Schema.String });

const fixturePolicy = AgentPolicy.make({
  maxTurns: 3,
  maxToolCalls: 2,
  maxDuration: "30 seconds",
  toolConcurrency: 1,
});

/** No tools: one final model response per Run. */
export const plannerDefinition = Agent.define("cf-planner", {
  input: FixtureInput,
  output: FixtureOutput,
  instructions: ({ question, ref }) => `Answer ${question} as JSON. [ref:${ref}]`,
  toolkit: Toolkit.empty,
  policy: fixturePolicy,
});

// `readonly`: no external mutation, so a crash between start and settlement is a free re-run
// (the P4 canonical shape; an unannotated Tool would fail closed to `uncertain`).
const SearchTool = Tool.make("search", {
  parameters: Schema.Struct({ query: Schema.String }),
  success: Schema.Struct({ available: Schema.Boolean }),
}).annotate(ToolExecutionClass, "readonly");
const searchTools = Toolkit.make(SearchTool);
export const searchToolLayer = searchTools.toLayer({
  search: () => Effect.succeed({ available: true }),
});

export const searchDefinition = Agent.define("cf-search", {
  input: FixtureInput,
  output: FixtureOutput,
  instructions: ({ ref }) => `Search before answering. [ref:${ref}]`,
  toolkit: searchTools,
  policy: fixturePolicy,
});

/** UNANNOTATED booking tool: fails closed to `uncertain` (prepared/settled protocol). */
const BookTool = Tool.make("book", {
  parameters: Schema.Struct({ ref: Schema.String }),
  success: Schema.Struct({ confirmation: Schema.String }),
});
const bookTools = Toolkit.make(BookTool);
export const bookToolLayer = bookTools.toLayer({
  book: ({ ref }) =>
    Effect.sync(() => {
      const confirmation = `confirmed-${ref}`;
      recordSupplierCall("book", ref, confirmation);
      return { confirmation };
    }),
});

export const bookDefinition = Agent.define("cf-book", {
  input: FixtureInput,
  output: FixtureOutput,
  instructions: ({ ref }) => `Book it. [ref:${ref}]`,
  toolkit: bookTools,
  policy: fixturePolicy,
});

/** Approval-gated booking tool; unannotated → fail-closed `uncertain` execution class. */
const BookApprovalTool = Tool.make("book", {
  parameters: Schema.Struct({ ref: Schema.String }),
  success: Schema.Struct({ confirmation: Schema.String }),
  needsApproval: true,
});
const approvalTools = Toolkit.make(BookApprovalTool);
export const approvalToolLayer = approvalTools.toLayer({
  book: ({ ref }) =>
    Effect.sync(() => {
      const confirmation = `confirmed-${ref}`;
      recordSupplierCall("book", ref, confirmation);
      return { confirmation };
    }),
});

export const approvalDefinition = Agent.define("cf-book-approval", {
  input: FixtureInput,
  output: FixtureOutput,
  instructions: ({ ref }) => `Book after approval. [ref:${ref}]`,
  toolkit: approvalTools,
  policy: fixturePolicy,
});

/** Durable Tool: declaring `DurableStep` as a dependency is what makes it durable. */
const ItineraryTool = Tool.make("itinerary", {
  parameters: Schema.Struct({ ref: Schema.String }),
  success: Schema.Struct({ state: Schema.String }),
  failure: DurableStepError,
  dependencies: [DurableStep],
});
const itineraryTools = Toolkit.make(ItineraryTool);
export const itineraryToolLayer = itineraryTools.toLayer({
  itinerary: ({ ref }) =>
    Effect.gen(function* () {
      yield* Effect.sync(() => recordSupplierCall("itinerary-enter", ref, `enter-${ref}`));
      const step = yield* DurableStep;
      const flight = yield* step.do(
        "reserve-flight",
        Schema.String,
        Effect.sync(() => {
          const value = `flight-${ref}`;
          recordSupplierCall("reserve-flight", ref, value);
          return value;
        }),
      );
      const lodging = yield* step.do(
        "reserve-lodging",
        Schema.String,
        Effect.sync(() => {
          const value = `lodging-${ref}`;
          recordSupplierCall("reserve-lodging", ref, value);
          return value;
        }),
      );
      return { state: `${flight}+${lodging}` };
    }),
});

export const itineraryDefinition = Agent.define("cf-itinerary", {
  input: FixtureInput,
  output: FixtureOutput,
  instructions: ({ ref }) => `Reserve the itinerary. [ref:${ref}]`,
  toolkit: itineraryTools,
  policy: fixturePolicy,
});

/** Host agent whose FIRST model request hangs on the release gate (join/renewal rows). */
export const joinDefinition = Agent.define("cf-join-host", {
  input: FixtureInput,
  output: FixtureOutput,
  instructions: ({ ref }) => `Search, wait for the gate, and fold in queued input. [ref:${ref}]`,
  toolkit: searchTools,
  policy: fixturePolicy,
});

// ---------------------------------------------------------------------------
// Models
// ---------------------------------------------------------------------------

const plannerModel = promptAwareModel("cf-planner", () =>
  Stream.fromIterable(finalParts(FINAL_ANSWER)),
);

const searchModel = promptAwareModel("cf-search", (promptJson) =>
  promptJson.includes(SEARCH_CALL_ID)
    ? Stream.fromIterable(finalParts(FINAL_ANSWER))
    : Stream.fromIterable(searchToolCallParts),
);

const bookModel = promptAwareModel("cf-book", (promptJson) =>
  promptJson.includes(BOOK_CALL_ID)
    ? Stream.fromIterable(finalParts(FINAL_ANSWER))
    : Stream.fromIterable(bookToolCallParts(refFromPrompt(promptJson))),
);

const approvalModel = promptAwareModel("cf-book-approval", (promptJson) =>
  promptJson.includes(BOOK_CALL_ID)
    ? Stream.fromIterable(finalParts(FINAL_ANSWER))
    : Stream.fromIterable(bookToolCallParts(refFromPrompt(promptJson))),
);

const itineraryModel = promptAwareModel("cf-itinerary", (promptJson) =>
  promptJson.includes(ITINERARY_CALL_ID)
    ? Stream.fromIterable(finalParts(FINAL_ANSWER))
    : Stream.fromIterable(itineraryToolCallParts(refFromPrompt(promptJson))),
);

/** First request per committed history: hang on the gate, then declare the search call. */
const joinModel = promptAwareModel("cf-join-host", (promptJson) =>
  promptJson.includes(SEARCH_CALL_ID)
    ? Stream.fromIterable(finalParts(FINAL_ANSWER))
    : Stream.fromEffectDrain(awaitGate(refFromPrompt(promptJson))).pipe(
        Stream.concat(Stream.fromIterable(searchToolCallParts)),
      ),
);

/**
 * Reconciliation policy for the fixture toolkits (durability §10): the re-enterable Durable
 * Tool `itinerary` is `SafeToRetry` (its Steps replay from their exactly-once records);
 * every ordinary call keeps the fail-closed default answer — no proof means Uncertain, so
 * the `book` rows still block on a durable Unknown Outcome until `resolveUnknown`.
 */
export const fixtureReconcilerLayer: Layer.Layer<ToolReconciler> = Layer.succeed(ToolReconciler)({
  reconcile: (evidence) =>
    Effect.sync(() =>
      evidence.toolName === "itinerary"
        ? ReconciliationSafeToRetry.make()
        : ReconciliationUncertain.make({
            reason: `No proof exists for ${evidence.toolName}; fail closed`,
          }),
    ),
});

// ---------------------------------------------------------------------------
// Registered worker bindings (S2 shape: one resolver serves every fixture lane)
// ---------------------------------------------------------------------------

/**
 * Every fixture Binding, captured with its tool layers (spec/subagents.md §11): the
 * Conversation Object resolves each claimed head's stored `(agentId, digests)` to exactly
 * one of these before any code runs (SUB-023).
 */
export const makeTestBindings: Effect.Effect<ReadonlyArray<ResolvedBinding>> = Effect.gen(
  function* () {
    const planner: ResolvedBinding = yield* DurableWorkerBinding.make(
      Agent.withModel(plannerDefinition, plannerModel),
      TEST_DIGESTS,
    );
    const search: ResolvedBinding = yield* DurableWorkerBinding.make(
      Agent.withModel(searchDefinition, searchModel),
      TEST_DIGESTS,
    ).pipe(Effect.provide(searchToolLayer));
    const book: ResolvedBinding = yield* DurableWorkerBinding.make(
      Agent.withModel(bookDefinition, bookModel),
      TEST_DIGESTS,
    ).pipe(Effect.provide(bookToolLayer));
    const approval: ResolvedBinding = yield* DurableWorkerBinding.make(
      Agent.withModel(approvalDefinition, approvalModel),
      TEST_DIGESTS,
    ).pipe(Effect.provide(approvalToolLayer));
    const itinerary: ResolvedBinding = yield* DurableWorkerBinding.make(
      Agent.withModel(itineraryDefinition, itineraryModel),
      TEST_DIGESTS,
    ).pipe(Effect.provide(itineraryToolLayer));
    const join: ResolvedBinding = yield* DurableWorkerBinding.make(
      Agent.withModel(joinDefinition, joinModel),
      TEST_DIGESTS,
    ).pipe(Effect.provide(searchToolLayer));
    return [planner, search, book, approval, itinerary, join];
  },
);

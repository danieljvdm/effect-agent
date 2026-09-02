import {
  Subagent,
  SubagentPolicy,
  SubagentReservationsMemoryLive,
  SubagentRuntime,
} from "@effect-agent/capabilities";
import { Agent, AgentPolicy, ThreadId, IdGenerator, RunId, TurnId } from "@effect-agent/core";
import {
  DefinitionDigests,
  Digest,
  DurableWorkerBinding,
  IdempotencyKey,
  Principal,
  type DurableSubmitOptions,
  type ResolvedBinding,
} from "@effect-agent/thread";
import { Effect, Layer, Ref, Schema, Stream } from "effect";
import { type Prompt, LanguageModel, Model, Toolkit, type Response } from "effect/unstable/ai";

/**
 * P7 WP4 soak fixtures shared by the soak test (submitting client) and the soak worker entry
 * (real killed processes). Models are PROMPT-shaped — the response depends only on the request
 * prompt — so any number of worker incarnations, resumes, and joined steering inputs stay
 * deterministic without cross-process counters.
 */

export const SOAK_DEPLOYMENT_ID = "deployment-soak";
export const SOAK_PRINCIPAL = Schema.decodeSync(Principal)("principal-soak");
export const SOAK_DELEGATE_CALL_ID = "soak-delegate-1";
export const SOAK_KILL_EXIT_CODE = 137;

export const SoakEnv = {
  database: "EFFECT_AGENT_SOAK_DB",
  producer: "EFFECT_AGENT_SOAK_PRODUCER",
  leaseMillis: "EFFECT_AGENT_SOAK_LEASE_MS",
} as const;

const SHA_A = Schema.decodeSync(Digest)("a".repeat(64));

export const SOAK_DIGESTS = DefinitionDigests.make({ agent: SHA_A, model: SHA_A, tools: SHA_A });

export const SOAK_CHILD_DIGEST_STRINGS = {
  agent: "b".repeat(64),
  model: "c".repeat(64),
  tools: "d".repeat(64),
} as const;

export const SOAK_CHILD_DIGESTS = DefinitionDigests.make({
  agent: Schema.decodeSync(Digest)(SOAK_CHILD_DIGEST_STRINGS.agent),
  model: Schema.decodeSync(Digest)(SOAK_CHILD_DIGEST_STRINGS.model),
  tools: Schema.decodeSync(Digest)(SOAK_CHILD_DIGEST_STRINGS.tools),
});

export const decodeThreadId = Schema.decodeSync(ThreadId);
const decodeIdempotencyKey = Schema.decodeSync(IdempotencyKey);
const decodeRunId = Schema.decodeSync(RunId);
const decodeTurnId = Schema.decodeSync(TurnId);

export const soakSubmitOptions = (
  thread: string,
  key: string,
  digests: DefinitionDigests = SOAK_DIGESTS,
): DurableSubmitOptions => ({
  threadId: decodeThreadId(thread),
  principal: SOAK_PRINCIPAL,
  idempotencyKey: decodeIdempotencyKey(key),
  definitions: digests,
});

const usage = { inputTokens: {}, outputTokens: {} };

const finalParts = (text: string): ReadonlyArray<Response.StreamPartEncoded> => [
  { type: "text-start", id: "answer" },
  { type: "text-delta", id: "answer", delta: text },
  { type: "text-end", id: "answer" },
  { type: "finish", reason: "stop", usage },
];

const delegateParts: ReadonlyArray<Response.StreamPartEncoded> = [
  {
    type: "tool-call",
    id: SOAK_DELEGATE_CALL_ID,
    name: "delegate_soak",
    params: { topic: "soak" },
    providerExecuted: false,
  },
  { type: "finish", reason: "tool-calls", usage },
];

/** Prompt-shaped scripted model: deterministic across incarnations and resumes. */
const promptScriptedModel = (
  label: string,
  script: (prompt: Prompt.Prompt) => ReadonlyArray<Response.StreamPartEncoded>,
) =>
  Model.make(
    "scripted",
    label,
    Layer.effect(
      LanguageModel.LanguageModel,
      LanguageModel.make({
        generateText: () => Effect.succeed([]),
        streamText: (request) => Stream.fromIterable(script(request.prompt)),
      }),
    ),
  );

export const soakPlannerDefinition = Agent.make("soak-planner", {
  input: Schema.Struct({ question: Schema.String }),
  output: Schema.Struct({ answer: Schema.String }),
  instructions: "Answer as JSON.",
  toolkit: Toolkit.empty,
  policy: AgentPolicy.make({
    maxTurns: 3,
    maxToolCalls: 2,
    maxDuration: "30 seconds",
    toolConcurrency: 1,
  }),
});

export const soakChildDefinition = Agent.make("soak-child", {
  input: Schema.Struct({ question: Schema.String }),
  output: Schema.Struct({ answer: Schema.String }),
  instructions: "Answer as JSON.",
  toolkit: Toolkit.empty,
  policy: AgentPolicy.make({
    maxTurns: 2,
    maxToolCalls: 1,
    maxDuration: "30 seconds",
    toolConcurrency: 1,
  }),
});

export class SoakDelegationFailed extends Schema.TaggedError<SoakDelegationFailed>()(
  "SoakDelegationFailed",
  { childErrorTag: Schema.String },
) {}

export const soakDelegation = Subagent.define("delegate_soak", {
  description: "Delegate one bounded soak question.",
  target: soakChildDefinition,
  parameters: Schema.Struct({ topic: Schema.String }),
  success: Schema.Struct({ summary: Schema.String }),
  failure: SoakDelegationFailed,
  prepareInput: ({ topic }) => Effect.succeed({ question: `soak:${topic}` }),
  projectResult: (output) => Effect.succeed({ summary: `finding:${output.answer}` }),
  policy: SubagentPolicy.make({
    maxChildren: 1,
    maxConcurrency: 1,
    maxTurns: 4,
    maxToolCalls: 4,
    maxDuration: "30 seconds",
  }),
});

export const soakCoordinatorDefinition = Agent.make("soak-coordinator", {
  input: Schema.Struct({ mission: Schema.String }),
  output: Schema.Struct({ report: Schema.String }),
  instructions: "Delegate, then report as JSON.",
  toolkit: Toolkit.make(soakDelegation.tool),
  policy: AgentPolicy.make({
    maxTurns: 3,
    maxToolCalls: 2,
    maxDuration: "30 seconds",
    toolConcurrency: 1,
  }),
});

/** Structural submit slices so the client process never builds models or tool Layers. */
export const soakPlannerSubmitSlice = {
  definition: { id: soakPlannerDefinition.id, input: soakPlannerDefinition.input },
} as const;

export const soakCoordinatorSubmitSlice = {
  definition: { id: soakCoordinatorDefinition.id, input: soakCoordinatorDefinition.input },
} as const;

/** Fixture-only identity source consumed by the delegation Layer's ephemeral capture. */
const soakIdentifiers = Layer.effect(
  IdGenerator,
  Effect.gen(function* () {
    const counter = yield* Ref.make(0);

    const next = <A>(decode: (value: string) => A, prefix: string) =>
      Ref.getAndUpdate(counter, (value) => value + 1).pipe(
        Effect.map((value) => decode(`${prefix}-${value}`)),
      );

    return {
      nextThreadId: next(decodeThreadId, "soak-fixture-thread"),
      nextRunId: next(decodeRunId, "soak-fixture-run"),
      nextTurnId: next(decodeTurnId, "soak-fixture-turn"),
    };
  }),
);

const delegationSupport = Layer.mergeAll(SubagentReservationsMemoryLive, soakIdentifiers);

/**
 * The three resolvable worker Bindings of the soak: planner (plain + joins), coordinator
 * (durable delegation), and the child — registered under EXACTLY the digests the client
 * submits with, so any worker incarnation serves any lane (spec §11).
 */
export const makeSoakBindings = Effect.fn("SoakFixtures.makeSoakBindings")(function* () {
  const plannerModel = promptScriptedModel("soak-planner", () => finalParts('{"answer":"soak"}'));
  const plannerBinding = Agent.withModel(soakPlannerDefinition, plannerModel);

  // Delegate exactly once per Run: only a first-turn prompt (no assistant/tool history yet)
  // declares the delegation; joined steering and post-join turns answer final, so a host Run
  // that absorbed queued Submissions still owns exactly one child.
  const coordinatorModel = promptScriptedModel("soak-coordinator", (prompt) =>
    prompt.content.some((message) => message.role === "assistant" || message.role === "tool")
      ? finalParts('{"report":"done"}')
      : delegateParts,
  );

  const coordinatorBinding = Agent.withModel(soakCoordinatorDefinition, coordinatorModel);

  const childModel = promptScriptedModel("soak-child", () => finalParts('{"answer":"child"}'));
  const childBinding = Agent.withModel(soakChildDefinition, childModel);

  const delegationLayer = SubagentRuntime.layer(soakDelegation, childBinding, {
    mapChildFailure: (failure) => SoakDelegationFailed.make({ childErrorTag: failure._tag }),
    durable: { targetDigests: SOAK_CHILD_DIGEST_STRINGS },
  }).pipe(Layer.provide(delegationSupport));

  const plannerResolved: ResolvedBinding = yield* DurableWorkerBinding.make(
    plannerBinding,
    SOAK_DIGESTS,
  );

  const coordinatorResolved: ResolvedBinding = yield* DurableWorkerBinding.make(
    coordinatorBinding,
    SOAK_DIGESTS,
  ).pipe(Effect.provide(delegationLayer));

  const childResolved: ResolvedBinding = yield* DurableWorkerBinding.make(
    childBinding,
    SOAK_CHILD_DIGESTS,
  );

  return [plannerResolved, coordinatorResolved, childResolved];
});

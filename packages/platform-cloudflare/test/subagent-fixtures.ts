import {
  Subagent,
  SubagentPolicy,
  SubagentReservationsMemoryLive,
  SubagentRuntime,
} from "@effect-agent/capabilities";
import { Agent, AgentPolicy, IdGenerator } from "@effect-agent/core";
import {
  DefinitionDigests,
  Digest,
  DurableWorkerBinding,
  type ResolvedBinding,
} from "@effect-agent/session";
import { Duration, Effect, Layer, Schema, Stream } from "effect";
import { LanguageModel, Model, Tool, Toolkit, type Response } from "effect/unstable/ai";

import {
  TEST_DIGESTS,
  bookTools,
  bookToolLayer,
  recordSupplierCall,
  supplierCountsFor,
} from "./fixtures.ts";

/**
 * WP4 cross-Object subagent fixtures (plan §4 WP4): the crash-shape coordinator → researcher
 * delegation (the platform-node S2 crash-matrix fixture, `test/crash/fixtures.ts`) rebuilt on
 * the WP3 workerd conventions. Because the vitest pool runs the test files and the Worker
 * entry in ONE isolate, this module's state is shared between the tests and every Durable
 * Object incarnation:
 *
 * - Every scripted model is PROMPT-AWARE (a pure function of the normalized request, never of
 *   an in-memory call counter), so one registered Binding is correct at every resume point —
 *   a fresh incarnation's Attempt continues from exactly the committed history in its prompt.
 * - Child-model invocations are counted through the fixtures' supplier log keyed by the
 *   Conversation-unique `ref`, so "a completed child is never re-executed" (SUB-018, §16.4)
 *   is asserted from state that survives `ctx.abort()` exactly like a real supplier's ledger.
 * - The delegation Tool Call id embeds the `ref` (`delegate-{ref}`), which makes the derived
 *   child Conversation (`subagent:{parentSubmissionId}:delegate-{ref}`) suffix-addressable
 *   BEFORE the parent Submission id exists — the seam the transport-fault switch needs.
 */

// ---------------------------------------------------------------------------
// Injected cross-Object transport faults (the DO-unreachable lever)
// ---------------------------------------------------------------------------

const faultedTargetSuffixes = new Set<string>();

/**
 * Make every cross-Object call ADDRESSED TO a Conversation whose name ends with `suffix`
 * reject at the RPC boundary — the platform's own "Durable Object unreachable" failure mode
 * (workerd overload, deploy-in-progress). The routed caller sees a `PortTransportError`;
 * on `resolveAdmission` the WP2 routing layer answers `AdmissionIndeterminate` (SUB-031).
 */
export const armTransportFault = (suffix: string): void => {
  faultedTargetSuffixes.add(suffix);
};

export const healTransportFault = (suffix: string): void => {
  faultedTargetSuffixes.delete(suffix);
};

/** The injected rejection reason for this Object name, when a matching fault is armed. */
export const transportFaultReason = (name: string | undefined): string | undefined => {
  if (name === undefined) return undefined;
  for (const suffix of faultedTargetSuffixes) {
    if (name.endsWith(suffix)) {
      return `injected transport fault: the Durable Object owning ${name} is unreachable`;
    }
  }
  return undefined;
};

// ---------------------------------------------------------------------------
// Child-model gate (deterministic hanging child for the abort/lost-wake rows)
// ---------------------------------------------------------------------------

const gatedChildRefs = new Set<string>();
/** Exercise ordinary Tool uncertainty inside the existing attached child fixture. */
export const uncertainChildRefs = new Set<string>();

/** Make the researcher model for `ref` HANG mid-stream until released. */
export const gateChildModel = (ref: string): void => {
  gatedChildRefs.add(ref);
};

export const releaseChildModel = (ref: string): void => {
  gatedChildRefs.delete(ref);
};

const awaitChildRelease = (ref: string): Effect.Effect<void> =>
  Effect.gen(function* () {
    while (gatedChildRefs.has(ref)) {
      yield* Effect.sleep(Duration.millis(10));
    }
  });

// ---------------------------------------------------------------------------
// Identities and constants
// ---------------------------------------------------------------------------

export const CHILD_ANSWER = '{"answer":"child"}';
export const COORDINATOR_REPORT = '{"report":"done"}';
/** The bounded parent Tool result projected from the child answer `{"answer":"child"}`. */
export const PROJECTED_SUMMARY = "finding:child";
/** Supplier-log operation recorded once per researcher LanguageModel invocation. */
export const CHILD_MODEL_OP = "child-model";
/** Supplier-log operation recorded once per sibling lookup handler execution. */
export const SIBLING_LOOKUP_OP = "sibling-lookup";

/** The one delegation Tool Call id of a coordinator Run on the `ref` lane. */
export const delegateCallIdFor = (ref: string): string => `delegate-${ref}`;
/** The sibling ordinary lookup Tool Call id of the mixed-batch coordinator Run. */
export const siblingCallIdFor = (ref: string): string => `lookup-${ref}`;

/** Exact researcher model invocation count for one `ref` (the never-re-executed currency). */
export const childModelInvocations = (ref: string): number =>
  supplierCountsFor(ref)[CHILD_MODEL_OP] ?? 0;

/** Exact sibling lookup handler execution count for one `ref`. */
export const siblingLookupInvocations = (ref: string): number =>
  supplierCountsFor(ref)[SIBLING_LOOKUP_OP] ?? 0;

/**
 * The child Binding digest strings the delegation declares
 * (`SubagentRuntimeOptions.durable.targetDigests`) AND the researcher Binding is registered
 * under — stored and verified byte-for-byte by the coordinator (SUB-023).
 */
export const SUBAGENT_CHILD_DIGEST_STRINGS = {
  agent: "b".repeat(64),
  model: "c".repeat(64),
  tools: "d".repeat(64),
} as const;

export const SUBAGENT_CHILD_DIGESTS = DefinitionDigests.make({
  agent: Schema.decodeSync(Digest)(SUBAGENT_CHILD_DIGEST_STRINGS.agent),
  model: Schema.decodeSync(Digest)(SUBAGENT_CHILD_DIGEST_STRINGS.model),
  tools: Schema.decodeSync(Digest)(SUBAGENT_CHILD_DIGEST_STRINGS.tools),
});

// ---------------------------------------------------------------------------
// Prompt-aware scripted models
// ---------------------------------------------------------------------------

const usage = { inputTokens: {}, outputTokens: {} };

const finalParts = (text: string): ReadonlyArray<Response.StreamPartEncoded> => [
  { type: "text-start", id: "answer" },
  { type: "text-delta", id: "answer", delta: text },
  { type: "text-end", id: "answer" },
  { type: "finish", reason: "stop", usage },
];

const toolCallPart = (
  id: string,
  name: string,
  params: Record<string, string>,
): Response.StreamPartEncoded => ({
  type: "tool-call",
  id,
  name,
  params,
  providerExecuted: false,
});

/** Extract the Conversation-unique `ref` the coordinator instructions embed as `[ref:...]`. */
const refFromPrompt = (promptJson: string): string => {
  const match = /\[ref:([^\]]+)\]/.exec(promptJson);
  return match?.[1] ?? "unknown-ref";
};

/** Extract the `ref` from the researcher's prepared input `research:{ref}`. */
const childRefFromPrompt = (promptJson: string): string => {
  const match = /research:([A-Za-z0-9-]+)/.exec(promptJson);
  return match?.[1] ?? "unknown-ref";
};

/**
 * A PROMPT-AWARE scripted model (the WP3 fixture pattern): `decide` runs once per model
 * invocation over the serialized request, so behavior follows the committed history and
 * never an in-memory counter that an eviction would reset.
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

/**
 * Coordinator model: a prompt WITHOUT the delegation call declares it; a prompt that already
 * contains the delegation call (the joined batch is canonical) writes the final report. A
 * batch resume never re-invokes the model, so exactly two invocations ever happen per lane.
 */
const coordinatorModel = promptAwareModel("cf-s2-coordinator", (promptJson) => {
  const ref = refFromPrompt(promptJson);
  return promptJson.includes(delegateCallIdFor(ref))
    ? Stream.fromIterable(finalParts(COORDINATOR_REPORT))
    : Stream.fromIterable([
        toolCallPart(delegateCallIdFor(ref), "delegate_research", { topic: ref }),
        { type: "finish", reason: "tool-calls", usage },
      ]);
});

/** Mixed-batch coordinator: the delegation call plus an ordinary sibling lookup in ONE Turn. */
const siblingCoordinatorModel = promptAwareModel("cf-s2-sibling-coordinator", (promptJson) => {
  const ref = refFromPrompt(promptJson);
  return promptJson.includes(delegateCallIdFor(ref))
    ? Stream.fromIterable(finalParts(COORDINATOR_REPORT))
    : Stream.fromIterable([
        toolCallPart(delegateCallIdFor(ref), "delegate_research", { topic: ref }),
        toolCallPart(siblingCallIdFor(ref), "lookup", { key: ref }),
        { type: "finish", reason: "tool-calls", usage },
      ]);
});

/**
 * Researcher model with supplier-log invocation counting: every `streamText` call appends one
 * `child-model` line for its `ref` BEFORE any part is emitted, so invocation counts survive
 * `ctx.abort()` of the child Object. A gated `ref` hangs mid-stream until released — the
 * deterministic "active child" the abort-propagation and lost-notification rows need.
 */
const researcherModel = promptAwareModel("cf-s2-researcher", (promptJson) => {
  const ref = childRefFromPrompt(promptJson);
  recordSupplierCall(CHILD_MODEL_OP, ref, "invoked");
  const response = Stream.fromIterable(
    uncertainChildRefs.has(ref)
      ? [
          toolCallPart("book-1", "book", { ref }),
          { type: "finish", reason: "tool-calls", usage } satisfies Response.StreamPartEncoded,
        ]
      : finalParts(CHILD_ANSWER),
  );
  return gatedChildRefs.has(ref)
    ? Stream.fromEffectDrain(awaitChildRelease(ref)).pipe(Stream.concat(response))
    : response;
});

// ---------------------------------------------------------------------------
// Agent definitions and the delegation
// ---------------------------------------------------------------------------

const CoordinatorInput = Schema.Struct({ mission: Schema.String, ref: Schema.String });
const CoordinatorOutput = Schema.Struct({ report: Schema.String });
const ResearcherInput = Schema.Struct({ question: Schema.String });
const ResearcherOutput = Schema.Struct({ answer: Schema.String });

export const researcherDefinition = Agent.make("cf-s2-researcher", {
  input: ResearcherInput,
  output: ResearcherOutput,
  instructions: ({ question }) => `Answer ${question} as JSON.`,
  toolkit: bookTools,
  policy: AgentPolicy.make({
    maxTurns: 2,
    maxToolCalls: 1,
    maxDuration: "30 seconds",
    toolConcurrency: 1,
  }),
});

export class CfDelegationFailed extends Schema.TaggedError<CfDelegationFailed>()(
  "CfDelegationFailed",
  { childErrorTag: Schema.String },
) {}

const researchDelegation = Subagent.define("delegate_research", {
  description: "Research one bounded question and return findings.",
  target: researcherDefinition,
  parameters: Schema.Struct({ topic: Schema.String }),
  success: Schema.Struct({ summary: Schema.String }),
  failure: CfDelegationFailed,
  prepareInput: ({ topic }) => Effect.succeed({ question: `research:${topic}` }),
  projectResult: (output) => Effect.succeed({ summary: `finding:${output.answer}` }),
  policy: SubagentPolicy.make({
    maxChildren: 1,
    maxConcurrency: 1,
    maxTurns: 4,
    maxToolCalls: 4,
    maxDuration: "30 seconds",
  }),
});

export const coordinatorDefinition = Agent.make("cf-s2-coordinator", {
  input: CoordinatorInput,
  output: CoordinatorOutput,
  instructions: ({ mission, ref }) => `Delegate the research for ${mission}. [ref:${ref}]`,
  toolkit: Toolkit.make(researchDelegation.tool),
  policy: AgentPolicy.make({
    maxTurns: 3,
    maxToolCalls: 2,
    maxDuration: "30 seconds",
    toolConcurrency: 1,
  }),
});

/** UNANNOTATED sibling lookup — the ordinary uncertain-class call in the mixed batch. */
const SiblingLookup = Tool.make("lookup", {
  parameters: Schema.Struct({ key: Schema.String }),
  success: Schema.Struct({ value: Schema.String }),
});

export const siblingCoordinatorDefinition = Agent.make("cf-s2-sibling-coordinator", {
  input: CoordinatorInput,
  output: CoordinatorOutput,
  instructions: ({ mission, ref }) =>
    `Delegate the research for ${mission} and look it up. [ref:${ref}]`,
  toolkit: Toolkit.make(researchDelegation.tool, SiblingLookup),
  policy: AgentPolicy.make({
    maxTurns: 3,
    maxToolCalls: 3,
    maxDuration: "30 seconds",
    toolConcurrency: 2,
  }),
});

// ---------------------------------------------------------------------------
// Registered worker Bindings (SUB-023: exact digest registration)
// ---------------------------------------------------------------------------

const delegationSupport = Layer.mergeAll(SubagentReservationsMemoryLive, IdGenerator.layer);

const mapChildFailure = (failure: { readonly _tag: string }) =>
  CfDelegationFailed.make({ childErrorTag: failure._tag });

/**
 * The three resolvable worker Bindings of the WP4 fixture, registered under EXACTLY the
 * digest strings the tests submit with (`TEST_DIGESTS`) and the delegation declares
 * (`SUBAGENT_CHILD_DIGEST_STRINGS`). Runs once
 * per Object incarnation during Layer construction; every observable counter lives in the
 * module-level supplier log, never in the Binding values.
 */
export const makeSubagentTestBindings: Effect.Effect<ReadonlyArray<ResolvedBinding>> = Effect.gen(
  function* () {
    const childBinding = Agent.withModel(researcherDefinition, researcherModel);
    const delegationLayer = SubagentRuntime.layer(researchDelegation, childBinding, {
      mapChildFailure,
      durable: { targetDigests: SUBAGENT_CHILD_DIGEST_STRINGS },
    }).pipe(Layer.provide([delegationSupport, bookToolLayer]));
    const siblingLookupLayer = Toolkit.make(SiblingLookup).toLayer({
      lookup: ({ key }) =>
        Effect.sync(() => {
          const value = `found-${key}`;
          recordSupplierCall(SIBLING_LOOKUP_OP, key, value);
          return { value };
        }),
    });

    const coordinator: ResolvedBinding = yield* DurableWorkerBinding.make(
      Agent.withModel(coordinatorDefinition, coordinatorModel),
      TEST_DIGESTS,
    ).pipe(Effect.provide(delegationLayer));
    const sibling: ResolvedBinding = yield* DurableWorkerBinding.make(
      Agent.withModel(siblingCoordinatorDefinition, siblingCoordinatorModel),
      TEST_DIGESTS,
    ).pipe(Effect.provide(Layer.mergeAll(delegationLayer, siblingLookupLayer)));
    const researcher: ResolvedBinding = yield* DurableWorkerBinding.make(
      childBinding,
      SUBAGENT_CHILD_DIGESTS,
    ).pipe(Effect.provide(bookToolLayer));
    return [coordinator, sibling, researcher];
  },
);

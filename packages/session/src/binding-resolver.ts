import { AgentId, ConversationId, type RunDispositionDeclaration } from "@effect-agent/core";
import type { RuntimeBinding } from "@effect-agent/engine";
import { Context, Effect, Layer, Option, Schema } from "effect";
import type { Tool } from "effect/unstable/ai";

import type { DurableWorkerFailure, DurableWorkerRequirements } from "./durable-runtime.ts";
import type { Claim, LedgerError, Settlement } from "./ledger.ts";
import { DefinitionDigests } from "./records.ts";

/**
 * No Agent Binding is registered for the requested stable identity. Recovery
 * fails closed: it never substitutes the latest Binding or runs different
 * code.
 */
export class BindingUnavailable extends Schema.TaggedError<BindingUnavailable>()(
  "BindingUnavailable",
  {
    agentId: AgentId,
    message: Schema.String,
  },
) {}

/**
 * A Binding is registered for the identity but its exact stored
 * definition/model/tool digests do not match the claimed head's stored
 * digests byte-for-byte. Fail closed (SUB-023): missing or different code is
 * a typed compatibility failure, never a silent substitution.
 */
export class BindingDigestMismatch extends Schema.TaggedError<BindingDigestMismatch>()(
  "BindingDigestMismatch",
  {
    agentId: AgentId,
    message: Schema.String,
  },
) {}

/** The typed refusal family of durable Binding resolution (spec §11, SUB-032). */
export type DurableBindingFailure = BindingUnavailable | BindingDigestMismatch;

/**
 * Instruction failure/requirement derivation mirroring the engine's `RuntimeBinding` defaults:
 * declaring them as generic DEFAULTS (instead of independent inference sites) keeps a plain
 * string-returning instruction function from widening both parameters to `unknown`.
 */
type InstructionResultOf<Instructions, Input> = Instructions extends (input: Input) => infer Result
  ? Result
  : Instructions;

type InstructionErrorOf<Instructions, Input> =
  InstructionResultOf<Instructions, Input> extends Effect.Effect<
    infer _Success,
    infer Error,
    infer _Requirements
  >
    ? Error
    : never;

type InstructionRequirementsOf<Instructions, Input> =
  InstructionResultOf<Instructions, Input> extends Effect.Effect<
    infer _Success,
    infer _Error,
    infer Requirements
  >
    ? Requirements
    : never;

/** Byte-for-byte equality of two stored definition digest triples (SUB-023). */
export const definitionDigestsEqual = (
  left: DefinitionDigests,
  right: DefinitionDigests,
): boolean =>
  left.agent === right.agent && left.model === right.model && left.tools === right.tools;

/**
 * INTERNAL coordinator entry point a `ResolvedBinding` drives: one fenced
 * Attempt over the resolved binding and an already-granted claim. The
 * coordinator's `runAttempt` satisfies this shape; the polymorphic signature
 * keeps the existential capture fully typed (no generics escape as `any`).
 */
export type ResolvedAttemptDriver = <
  InputSchema extends Schema.Top,
  OutputSchema extends Schema.Top,
  Instructions,
  Tools extends Record<string, Tool.Any>,
  Provider,
  ModelProvides,
  ModelRequires,
  InstructionError,
  InstructionRequirements,
  RunDispositionValue extends
    | RunDispositionDeclaration<OutputSchema["Type"], Schema.Top>
    | undefined,
>(
  agent: RuntimeBinding<
    InputSchema,
    OutputSchema,
    Instructions,
    Tools,
    Provider,
    ModelProvides,
    ModelRequires,
    InstructionError,
    InstructionRequirements,
    RunDispositionValue
  >,
  conversationId: ConversationId,
  claim: Claim,
) => Effect.Effect<
  Option.Option<Settlement>,
  DurableWorkerFailure,
  DurableWorkerRequirements<
    RuntimeBinding<
      InputSchema,
      OutputSchema,
      Instructions,
      Tools,
      Provider,
      ModelProvides,
      ModelRequires,
      InstructionError,
      InstructionRequirements,
      RunDispositionValue
    >,
    InstructionRequirements
  >
>;

/**
 * One resolvable Agent Binding as an existential value: the stable identity,
 * the exact registered definition digests, and an `attempt` closure that runs
 * a coordinator Attempt over the captured binding with its worker
 * requirements Context already provided (the S1 `Effect.context` capture
 * precedent from `SubagentRuntime.layer`). `digests` is `undefined` only for
 * the digest-transparent singleton registration used by the legacy
 * single-binding `runWorker` — identity-exact but accepting whatever digests
 * the claimed head stored; exact-digest enforcement requires
 * `DurableWorkerBinding.make` plus `runResolvedWorker`.
 */
export interface ResolvedBinding {
  readonly agentId: AgentId;
  readonly digests: DefinitionDigests | undefined;
  readonly attempt: (
    driver: ResolvedAttemptDriver,
    conversationId: ConversationId,
    claim: Claim,
  ) => Effect.Effect<Option.Option<Settlement>, DurableWorkerFailure>;
}

const capture = <
  InputSchema extends Schema.Top,
  OutputSchema extends Schema.Top,
  Instructions,
  Tools extends Record<string, Tool.Any>,
  Provider,
  ModelProvides,
  ModelRequires,
  InstructionError = InstructionErrorOf<Instructions, InputSchema["Type"]>,
  InstructionRequirements = InstructionRequirementsOf<Instructions, InputSchema["Type"]>,
  RunDispositionValue extends
    | RunDispositionDeclaration<OutputSchema["Type"], Schema.Top>
    | undefined = undefined,
>(
  agent: RuntimeBinding<
    InputSchema,
    OutputSchema,
    Instructions,
    Tools,
    Provider,
    ModelProvides,
    ModelRequires,
    InstructionError,
    InstructionRequirements,
    RunDispositionValue
  >,
  digests: DefinitionDigests | undefined,
): Effect.Effect<
  ResolvedBinding,
  never,
  DurableWorkerRequirements<typeof agent, InstructionRequirements>
> =>
  Effect.map(
    Effect.context<DurableWorkerRequirements<typeof agent, InstructionRequirements>>(),
    (context): ResolvedBinding => ({
      agentId: agent.definition.id,
      digests,
      attempt: (driver, conversationId, claim) =>
        // `Exclude<R, R>` does not reduce to `never` while `R` stays
        // parametric, so this one documented assertion closes the provision
        // TypeScript cannot see: the captured Context is exactly the driver's
        // requirement set for this binding instantiation.
        driver(agent, conversationId, claim).pipe(Effect.provide(context)) as Effect.Effect<
          Option.Option<Settlement>,
          DurableWorkerFailure
        >,
    }),
  );

/**
 * Build resolvable worker bindings. The definition
 * resolver is an explicit host-supplied Effect service; it resolves an Agent
 * Binding by stable identity and exact stored digest.
 *
 * - `make(agent, digests)` captures the binding plus its worker-requirement
 *   Context at Layer/effect construction time and registers the EXACT digest
 *   triple the host stores for it. Hosts MUST register the same digest
 *   strings the application submits with (`DurableSubmitOptions.definitions`)
 *   and declares on durable delegation Layers
 *   (`SubagentRuntimeOptions.durable.targetDigests`).
 * - `makeDigestTransparent(agent)` registers identity-only resolution for the
 *   legacy single-binding `runWorker`: a claimed head with a different
 *   `agentId` still fails closed, but the head's own stored digests are
 *   accepted as-is because this registration carries no digest authority.
 */
export const DurableWorkerBinding = {
  make: <
    InputSchema extends Schema.Top,
    OutputSchema extends Schema.Top,
    Instructions,
    Tools extends Record<string, Tool.Any>,
    Provider,
    ModelProvides,
    ModelRequires,
    InstructionError = InstructionErrorOf<Instructions, InputSchema["Type"]>,
    InstructionRequirements = InstructionRequirementsOf<Instructions, InputSchema["Type"]>,
    RunDispositionValue extends
      | RunDispositionDeclaration<OutputSchema["Type"], Schema.Top>
      | undefined = undefined,
  >(
    agent: RuntimeBinding<
      InputSchema,
      OutputSchema,
      Instructions,
      Tools,
      Provider,
      ModelProvides,
      ModelRequires,
      InstructionError,
      InstructionRequirements,
      RunDispositionValue
    >,
    digests: DefinitionDigests,
  ): Effect.Effect<
    ResolvedBinding,
    never,
    DurableWorkerRequirements<typeof agent, InstructionRequirements>
  > => capture(agent, digests),
  makeDigestTransparent: <
    InputSchema extends Schema.Top,
    OutputSchema extends Schema.Top,
    Instructions,
    Tools extends Record<string, Tool.Any>,
    Provider,
    ModelProvides,
    ModelRequires,
    InstructionError = InstructionErrorOf<Instructions, InputSchema["Type"]>,
    InstructionRequirements = InstructionRequirementsOf<Instructions, InputSchema["Type"]>,
    RunDispositionValue extends
      | RunDispositionDeclaration<OutputSchema["Type"], Schema.Top>
      | undefined = undefined,
  >(
    agent: RuntimeBinding<
      InputSchema,
      OutputSchema,
      Instructions,
      Tools,
      Provider,
      ModelProvides,
      ModelRequires,
      InstructionError,
      InstructionRequirements,
      RunDispositionValue
    >,
  ): Effect.Effect<
    ResolvedBinding,
    never,
    DurableWorkerRequirements<typeof agent, InstructionRequirements>
  > => capture(agent, undefined),
} as const;

/**
 * Host-supplied definition/binding resolver used at durable claim time
 * using stable identity plus exact stored digests to
 * resolve one executable `ResolvedBinding`. Missing or different code is a
 * typed, fail-closed refusal — recovery never substitutes the latest Binding
 * (SUB-023); the coordinator turns a refusal into the framework
 * `ChildCompatibilityFailure` Settlement for a parent-linked child and a
 * typed report for a root Submission (SUB-032).
 */
export class AgentBindingResolver extends Context.Service<
  AgentBindingResolver,
  {
    readonly resolve: (
      agentId: AgentId,
      digests: DefinitionDigests,
    ) => Effect.Effect<ResolvedBinding, DurableBindingFailure | LedgerError>;
  }
>()("@effect-agent/session/AgentBindingResolver") {
  /** Resolver over a fixed registration list: identity first, then exact digests. */
  static fromBindings(
    bindings: ReadonlyArray<ResolvedBinding>,
  ): (typeof AgentBindingResolver)["Service"] {
    return {
      resolve: (agentId, digests) => {
        const registered = bindings.filter((binding) => binding.agentId === agentId);
        if (registered.length === 0) {
          return Effect.fail(
            BindingUnavailable.make({
              agentId,
              message: `No Agent Binding is registered for ${agentId}; recovery never substitutes different code (SUB-023)`,
            }),
          );
        }
        const exact = registered.find(
          (binding) =>
            binding.digests === undefined || definitionDigestsEqual(binding.digests, digests),
        );
        if (exact === undefined) {
          return Effect.fail(
            BindingDigestMismatch.make({
              agentId,
              message: `The registered Binding for ${agentId} does not match the stored definition digests exactly; missing or different code fails closed (SUB-023)`,
            }),
          );
        }
        return Effect.succeed(exact);
      },
    };
  }

  static layer(bindings: ReadonlyArray<ResolvedBinding>): Layer.Layer<AgentBindingResolver> {
    return Layer.succeed(AgentBindingResolver)(AgentBindingResolver.fromBindings(bindings));
  }
}

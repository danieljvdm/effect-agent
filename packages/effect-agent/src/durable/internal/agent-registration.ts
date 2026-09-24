import {
  type Crypto,
  Option,
  type Scope,
  Context,
  Effect,
  Layer,
  References,
  Schema,
  Tracer,
} from "effect";
import { Tool } from "effect/unstable/ai";

import type * as Agent from "../../core/Agent.ts";
import {
  type InputPromptSource,
  type InstructionSource,
  type ModelServices,
  type RunDispositionDeclaration,
} from "../../core/Agent.ts";
import { DecisionTurnError } from "../../core/AgentError.ts";
import { type ThreadId, AgentId } from "../../core/Identifiers.ts";
import { getToolExecutionKind } from "../../core/SubagentContract.ts";
import {
  AdditionalToolCatalog,
  DiscoveryTool,
  IncludesCatalogDocumentation,
  PinnedTool,
  ToolNamespace,
} from "../../core/ToolExposure.ts";
import { type RuntimeBinding } from "../../engine/AgentRuntime.ts";
import { ContextRolloverTool } from "../../engine/ContextWindow.ts";
import { getToolExecutionClass } from "../../engine/DurableStep.ts";
import { ConfiguredDecisionTurn } from "../../engine/internal/decision-turn.ts";
import {
  BackgroundReporting,
  WorkerReportPreparationFailure,
  type WorkerReporting,
} from "../../engine/SubagentHost.ts";
import type * as DecisionTurn from "../DecisionTurn.ts";
import { digestDefinitions, digestJson, DigestError } from "../Digest.ts";
import type { DurableWorkerFailure, DurableWorkerRequirements } from "../DurableAgentRuntime.ts";
import type { DefinitionDigestInput, PersistedJson } from "../Records.ts";
import { DefinitionDigests, ReplayContract } from "../Records.ts";
import type { Claim, Settlement } from "../SubmissionLedger.ts";

/** No unique current executable is registered for the stable Agent identity. */
export class BindingUnavailable extends Schema.TaggedError<BindingUnavailable>()(
  "BindingUnavailable",
  { agentId: AgentId, message: Schema.String },
) {}

export type DurableBindingFailure = BindingUnavailable;

/** Exact immutable admission, lineage and delivery evidence; never an executable-code gate. */
export const definitionDigestsEqual = (
  left: DefinitionDigests,
  right: DefinitionDigests,
): boolean =>
  left.agent === right.agent &&
  left.model === right.model &&
  left.tools === right.tools &&
  Schema.toEquivalence(Schema.UndefinedOr(ReplayContract))(left.replay, right.replay);

/** Change an operation version when its meaning, durable Steps or idempotency keys change. */
export interface ReplayVersions {
  readonly tools: Readonly<Record<string, PersistedJson>>;
}

/** Hash current operation contracts; no historical definitions or schemas are retained. */
export const toolReplayContracts = Effect.fn("AgentRegistration.toolReplayContracts")(function* (
  definition: Agent.AnyDefinition,
  versions?: ReplayVersions,
  fallbackVersion: PersistedJson = null,
) {
  const declarations = yield* Effect.try({
    try: () => {
      const jsonSchema = (schema: Schema.Top) =>
        Schema.decodeUnknownSync(Schema.Json)(Tool.getJsonSchemaFromSchema(schema));

      return Object.values(definition.toolkit.tools).map((tool) => {
        const version = versions === undefined ? fallbackVersion : versions.tools[tool.name];

        if (
          version === undefined ||
          (versions !== undefined && !Object.hasOwn(versions.tools, tool.name))
        )
          throw new Error("Every Tool needs an explicit replay semantic version");

        return {
          name: tool.name,
          contract: {
            version,
            parameters: jsonSchema(tool.parametersSchema),
            success: jsonSchema(tool.successSchema),
            failure: jsonSchema(tool.failureSchema),
            failureMode: tool.failureMode,
            needsApproval:
              typeof tool.needsApproval === "function" ? "dynamic" : (tool.needsApproval ?? false),
            executionClass: getToolExecutionClass(tool),
            executionKind: getToolExecutionKind(tool.annotations),
            contextRollover: Context.get(tool.annotations, ContextRolloverTool),
            completion: definition.completion?.tool === tool.name,
            completionFromTool:
              definition.completionFromTools?.some(
                (declaration) => declaration.tool === tool.name,
              ) ?? false,
          },
        };
      });
    },
    catch: () =>
      DigestError.make({
        message: "Operation contracts require serializable codecs and a version for every Tool",
      }),
  });

  return Object.fromEntries(
    yield* Effect.forEach(declarations, ({ name, contract }) =>
      digestJson(contract).pipe(Effect.map((digest) => [name, digest] as const)),
    ),
  );
});

/** Compile current metadata without acquiring executable services. */
export const compileBindingContracts = Effect.fn("AgentRegistration.compileBindingContracts")(
  function* (
    definition: Agent.AnyDefinition,
    definitions: DefinitionDigestInput,
    versions?: ReplayVersions,
  ): Effect.fn.Return<Pick<ResolvedBinding, "digests">, DigestError, Crypto.Crypto> {
    const digests = yield* digestDefinitions(definitions);
    const tools = yield* toolReplayContracts(definition, versions, definitions.tools);

    return {
      digests: DefinitionDigests.make({
        ...digests,
        replay: ReplayContract.make({ agent: digests.agent, tools }),
      }),
    };
  },
);

type ExecutableDefinition = Agent.AnyDefinition & {
  readonly instructions: InstructionSource<never, unknown, unknown>;
  readonly inputPrompt?: InputPromptSource<never, unknown, unknown> | undefined;
};

/** Executable model Binding accepted by durable registration descriptors. */
export type ExecutableAgentBinding = {
  readonly definition: ExecutableDefinition;
  readonly model: Layer.Layer<ModelServices, never, unknown>;
};

/**
 * Internal coordinator entry point for one fenced Attempt over an already-granted claim.
 * The generic signature matches `runAttempt` and is specialized for each captured Agent.
 */
type ResolvedAttemptDriver = <
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
  InputPromptValue extends InputPromptSource<InputSchema["Type"], unknown, unknown> | undefined,
  UpdatesSchema extends Schema.Top | undefined,
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
    RunDispositionValue,
    InputPromptValue,
    UpdatesSchema
  >,
  threadId: ThreadId,
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
      RunDispositionValue,
      InputPromptValue,
      UpdatesSchema
    >,
    InstructionRequirements
  >
>;

type CapturedAttempt<A extends ExecutableAgentBinding> = (
  agent: A,
  threadId: ThreadId,
  claim: Claim,
) => Effect.Effect<Option.Option<Settlement>, DurableWorkerFailure, DurableWorkerRequirements<A>>;

/**
 * An Agent with its worker services captured, ready to drive a fenced Attempt.
 * Concrete Schema and model types stay inside this closure when registrations are collected.
 */
interface CapturedBinding {
  readonly agentId: AgentId;
  /** Exact immutable definition whose codecs and behavior the worker captured. */
  readonly definition: Agent.AnyDefinition;
  /** Captured independently from per-Attempt services: report preparation has no fenced Claim. */
  readonly reporting?: ReadonlyArray<WorkerReporting<WorkerReportPreparationFailure>>;
  readonly attempt: (
    driver: ResolvedAttemptDriver,
    threadId: ThreadId,
    claim: Claim,
  ) => Effect.Effect<Option.Option<Settlement>, DurableWorkerFailure>;
}

// Registrations outlive their construction span. Dependencies remain captured,
// while every attempt/report inherits the invoking fiber's tracing state.
const omitTraceContext = Context.omit(
  Tracer.ParentSpan,
  Tracer.Tracer,
  Tracer.MinimumTraceLevel,
  Tracer.CurrentTraceLevel,
  References.TracerEnabled,
  References.TracerTimingEnabled,
  References.TracerSpanAnnotations,
  References.TracerSpanLinks,
);

// R describes captured application services; ParentSpan is supplied by the
// live invocation rather than stored as a registration dependency.
const withoutTraceContext = <R>(context: Context.Context<R>): Context.Context<R> =>
  omitTraceContext(context) as Context.Context<R>;

const captureReporting = <R>(reports: ReadonlyArray<WorkerReporting<unknown, R>>) =>
  Effect.map(
    Effect.context<Exclude<R, Scope.Scope>>().pipe(Effect.map(withoutTraceContext)),
    (context) =>
      reports.map((report): WorkerReporting<WorkerReportPreparationFailure> => ({
        ...report,
        prepare: (value) =>
          Effect.scoped(Effect.suspend(() => report.prepare(value))).pipe(
            Effect.provide(context),
            Effect.mapError((error) =>
              Schema.is(WorkerReportPreparationFailure)(error)
                ? error
                : WorkerReportPreparationFailure.make({ stage: "preparation" }),
            ),
          ),
      })),
  );

const backgroundReports = (definition: Agent.AnyDefinition) => [
  ...new Set(
    Object.values(definition.toolkit.tools).flatMap((tool) => {
      const report = Context.get(tool.annotations, BackgroundReporting);

      return report === undefined ? [] : [report];
    }),
  ),
];

/** One exact executable registration used by durable claim-time resolution. */
export interface ResolvedBinding extends CapturedBinding {
  readonly digests: DefinitionDigests;
}

/** Trusted claim identity for resolving per-Attempt application services. Not model input. */
export interface AgentAttemptContext {
  readonly threadId: ThreadId;
  readonly submissionId: Claim["submissionId"];
  readonly attemptId: Claim["attemptId"];
}

type DecisionTurnRequirements<D> = Exclude<DecisionTurn.Requirements<D>, Scope.Scope>;

const capture = <
  A extends ExecutableAgentBinding,
  Provides = never,
  Requires = never,
  D extends DecisionTurn.Definition<unknown, unknown> | undefined = undefined,
>(
  agent: A,
  attemptLayer?: (context: AgentAttemptContext) => Layer.Layer<Provides, never, Requires>,
  decisionTurn?: D,
): Effect.Effect<
  CapturedBinding,
  never,
  Exclude<DurableWorkerRequirements<A> | DecisionTurnRequirements<D>, Provides> | Requires
> =>
  Effect.map(
    Effect.context<
      Exclude<DurableWorkerRequirements<A> | DecisionTurnRequirements<D>, Provides> | Requires
    >().pipe(Effect.map(withoutTraceContext)),
    (context): CapturedBinding => ({
      agentId: agent.definition.id,
      definition: agent.definition,
      attempt: (driver, threadId, claim) => {
        // Registration closes one concrete Binding before heterogeneous registrations are
        // collected. TypeScript cannot instantiate the higher-rank RuntimeBinding parameters
        // from the intentionally erased public shape, so specialize the driver back to A here.
        const run = driver as unknown as CapturedAttempt<A>;

        const execute =
          decisionTurn === undefined
            ? run(agent, threadId, claim)
            : Effect.gen(function* () {
                const controllerContext = yield* Effect.context<unknown>();

                return yield* run(agent, threadId, claim).pipe(
                  Effect.provideService(ConfiguredDecisionTurn, (request) =>
                    decisionTurn
                      .prepare({
                        ...request,
                        submissionId: claim.submissionId,
                        attemptId: claim.attemptId,
                      })
                      .pipe(
                        Effect.provide(controllerContext),
                        Effect.mapError((cause) =>
                          Schema.is(DecisionTurnError)(cause)
                            ? cause
                            : DecisionTurnError.make({ stage: "prepare", cause }),
                        ),
                        Effect.map(
                          Option.map((prepared) => ({
                            ...prepared,
                            evaluate: prepared.evaluate.pipe(
                              Effect.map((evaluated) => ({
                                ...evaluated,
                                project: evaluated.project.pipe(Effect.provide(controllerContext)),
                              })),
                              Effect.provide(controllerContext),
                              Effect.mapError((cause) =>
                                Schema.is(DecisionTurnError)(cause)
                                  ? cause
                                  : DecisionTurnError.make({ stage: "decide", cause }),
                              ),
                            ),
                          })),
                        ),
                      ),
                  ),
                );
              }).pipe(Effect.scoped);

        const scoped =
          attemptLayer === undefined
            ? execute
            : execute.pipe(
                Effect.provide(
                  Layer.fresh(
                    attemptLayer({
                      threadId,
                      submissionId: claim.submissionId,
                      attemptId: claim.attemptId,
                    }),
                  ),
                ),
              );

        return scoped.pipe(Effect.provide(context)) as Effect.Effect<
          Option.Option<Settlement>,
          DurableWorkerFailure
        >;
      },
    }),
  );

const validateDecisionTurn = (
  definition: Agent.AnyDefinition,
  decisionTurn: DecisionTurn.Definition<unknown, unknown>,
) =>
  Object.is(decisionTurn.parent, definition) &&
  Object.is(definition.toolkit.tools[decisionTurn.tool.name], decisionTurn.tool)
    ? Effect.void
    : DigestError.make({
        message: "Decision Turn requires the exact registered Parent and Tool",
      });

function makeWorkerBinding<A extends ExecutableAgentBinding>(
  agent: A,
  digests: DefinitionDigests,
): Effect.Effect<ResolvedBinding, never, DurableWorkerRequirements<A>>;
function makeWorkerBinding<
  A extends ExecutableAgentBinding,
  D extends DecisionTurn.Definition<unknown, unknown>,
>(
  agent: A,
  digests: DefinitionDigests,
  options: { readonly decisionTurn: D },
): Effect.Effect<
  ResolvedBinding,
  DigestError,
  DurableWorkerRequirements<A> | DecisionTurnRequirements<D>
>;
function makeWorkerBinding<
  A extends ExecutableAgentBinding,
  D extends DecisionTurn.Definition<unknown, unknown>,
>(
  agent: A,
  digests: DefinitionDigests,
  options?: { readonly decisionTurn: D },
): Effect.Effect<
  ResolvedBinding,
  DigestError,
  DurableWorkerRequirements<A> | DecisionTurnRequirements<D>
> {
  return Effect.gen(function* () {
    if (options !== undefined) yield* validateDecisionTurn(agent.definition, options.decisionTurn);
    const binding = yield* capture(agent, undefined, options?.decisionTurn);
    const reporting = yield* captureReporting(backgroundReports(agent.definition));

    return { ...binding, digests, reporting };
  });
}

/**
 * Capture an executable Binding with caller-owned, already-computed definition digests.
 * The optional Decision uses the same interpreter as compiled registrations. Its caller
 * MUST include `yield* decisionTurn.contract` in the agent declaration before hashing
 * the supplied digests; this low-level constructor never silently rewrites a prehash.
 * Construct the Decision against this exact Definition, including per-Attempt policy overrides.
 * Hosts with unhashed declarations can use compileRegistrations to include the contract.
 */
export const DurableWorkerBinding = { make: makeWorkerBinding } as const;

/** INTERNAL identity-only capture retained for the legacy direct worker path. */
export const makeLegacyWorkerBinding = capture;

export const resolveWorkerBinding = (
  bindings: ReadonlyArray<ResolvedBinding>,
  agentId: AgentId,
): Effect.Effect<ResolvedBinding, BindingUnavailable> => {
  const registered = bindings.filter((binding) => binding.agentId === agentId);
  const binding = registered[0];

  return registered.length === 1 && binding !== undefined
    ? Effect.succeed(binding)
    : Effect.fail(
        BindingUnavailable.make({
          agentId,
          message: `Exactly one current Agent Binding must be registered for ${agentId}`,
        }),
      );
};

/** Resolve authoring-time admission through the one exact host registration, never by identity alone. */
export const resolveDefinitionBinding = (
  bindings: ReadonlyArray<ResolvedBinding>,
  definition: Pick<Agent.AnyDefinition, "id">,
): Effect.Effect<ResolvedBinding, BindingUnavailable> => {
  const candidates = bindings.filter((binding) => binding.agentId === definition.id);
  const binding = candidates[0];

  if (candidates.length !== 1 || binding === undefined) {
    return Effect.fail(
      BindingUnavailable.make({
        agentId: definition.id,
        message: "Registered admission requires exactly one binding for the Agent identity",
      }),
    );
  }

  if (!Object.is(binding.definition, definition)) {
    return Effect.fail(
      BindingUnavailable.make({
        agentId: definition.id,
        message: "Registered admission requires the exact Agent Definition used in registration",
      }),
    );
  }

  return Effect.succeed(binding);
};

/** Application versions and a model Layer, supplied directly or through an existing Binding. */
export type AgentRegistration<A extends ExecutableAgentBinding = ExecutableAgentBinding> = (
  | { readonly agent: A; readonly model?: never; readonly definitions: DefinitionDigestInput }
  | {
      readonly agent: A["definition"];
      readonly model: A["model"];
      readonly definitions: DefinitionDigestInput;
    }
) & {
  /** Optional first-eligible ordinary-Turn classification, at most one committed Decision per Run. */
  readonly decisionTurn?: DecisionTurn.Definition<unknown, unknown>;
  /** Explicit operation versions let unchanged unfinished handlers survive toolbox changes. */
  readonly continuity?: {
    readonly versions: ReplayVersions;
  };
  /**
   * Build fresh services for exactly one fenced Attempt, across all its Tool/model turns.
   * Finalizes on completion, suspension, failure and interruption. Never reused after eviction.
   * Resolve invocation authority from the trusted claim identity, not captured caller state.
   * Keep fallible resource acquisition lazy in typed Tool operations; this Layer cannot fail.
   * Provide DurableApprovalSuspension here when resources need retention before approval waits.
   */
  readonly attemptLayer?: (context: AgentAttemptContext) => Layer.Layer<never, never, unknown>;
};

type EntryWorkerRequirements<Entry> = Entry extends {
  readonly agent: infer A extends ExecutableAgentBinding;
}
  ? DurableWorkerRequirements<A>
  : Entry extends {
        readonly agent: infer D extends ExecutableDefinition;
        readonly model: infer M extends ExecutableAgentBinding["model"];
      }
    ? DurableWorkerRequirements<{ readonly definition: D; readonly model: M }>
    : never;

type AttemptLayerRequirements<Requirements, AttemptLayer> = AttemptLayer extends (
  context: AgentAttemptContext,
) => Layer.Layer<infer Provides, never, infer Requires>
  ? Exclude<Requirements, Provides> | Requires
  : Requirements;

// Conditional options retain every service they may consume. An absent attempt Layer
// still needs the original worker services; only a definite Layer can remove them.
type EntryRequirements<Entry> = Entry extends unknown
  ? AttemptLayerRequirements<
      | EntryWorkerRequirements<Entry>
      | (Entry extends { readonly decisionTurn: infer D } ? DecisionTurnRequirements<D> : never),
      "attemptLayer" extends keyof Entry ? Entry["attemptLayer"] : undefined
    >
  : never;

type RegistrationRequirements<Entries extends ReadonlyArray<AgentRegistration>> = [
  Entries[number],
] extends [never]
  ? never
  : EntryRequirements<Entries[number]>;

const registrationDefinitions = (entry: AgentRegistration): DefinitionDigestInput => {
  const definition = entry.model === undefined ? entry.agent.definition : entry.agent;
  const automatic = backgroundReports(definition);

  const declaredDefinitions =
    definition.updates === undefined
      ? entry.definitions
      : {
          ...entry.definitions,
          agent: {
            declaration: entry.definitions.agent,
            updates: Schema.decodeUnknownSync(Schema.Json)(
              Tool.getJsonSchemaFromSchema(definition.updates),
            ),
            updateProtocol: { schemaVersion: 1, tool: "emit_update" },
          },
        };

  const definitions =
    automatic.length === 0
      ? declaredDefinitions
      : {
          ...declaredDefinitions,
          agent: {
            declaration: declaredDefinitions.agent,
            backgroundReporting: automatic.map((report) => ({
              delegationId: report.delegationId,
              targetAgentId: report.target.id,
              mode: "standard",
              // Preserve the fingerprint of existing standard registrations.
              destinationDelegationId: null,
            })),
          },
        };

  const exposure = definition.toolExposure;

  if (
    exposure === undefined &&
    !Object.values(definition.toolkit.tools).some(
      (tool) =>
        Context.get(tool.annotations, DiscoveryTool) || Context.get(tool.annotations, PinnedTool),
    )
  )
    return definitions;

  return {
    ...definitions,
    agent: {
      declaration: definitions.agent,
      toolExposure: {
        initialToolNames: exposure === undefined ? null : [...(exposure.initialToolNames ?? [])],
        maxTools: exposure?.maxTools ?? 64,
        maxSchemaBytes: exposure?.maxSchemaBytes ?? 262_144,
        requiredCompletion:
          definition.completion?.required === true ? definition.completion.tool : null,
        tools: Object.values(definition.toolkit.tools).map((tool) => ({
          name: tool.name,
          pinned: Context.get(tool.annotations, PinnedTool),
          discovery: Context.get(tool.annotations, DiscoveryTool),
          namespace: Context.get(tool.annotations, ToolNamespace) ?? null,
          includesCatalogDocumentation: Context.get(tool.annotations, IncludesCatalogDocumentation),
          additional: Context.get(tool.annotations, AdditionalToolCatalog).map((entry) => ({
            name: entry.tool.name,
            namespace: entry.namespace,
            method: entry.method,
          })),
        })),
      },
    },
  };
};

const compileRegistration = <Entry extends AgentRegistration>(
  entry: Entry,
): Effect.Effect<ResolvedBinding, DigestError, Crypto.Crypto | EntryRequirements<Entry>> =>
  Effect.flatMap(
    Effect.try({
      try: () => registrationDefinitions(entry),
      catch: () =>
        DigestError.make({ message: "Agent update schema has no serializable wire contract" }),
    }),
    (definitions) =>
      Effect.gen(function* () {
        const binding = yield* capture(
          entry.model === undefined ? entry.agent : { definition: entry.agent, model: entry.model },
          entry.attemptLayer,
          entry.decisionTurn,
        );

        if (entry.decisionTurn !== undefined)
          yield* validateDecisionTurn(binding.definition, entry.decisionTurn);

        const controllerDigest =
          entry.decisionTurn === undefined ? undefined : yield* entry.decisionTurn.contract;

        const reporting = yield* captureReporting(backgroundReports(binding.definition));

        return {
          ...binding,
          ...(yield* compileBindingContracts(
            binding.definition,
            controllerDigest === undefined
              ? definitions
              : {
                  ...definitions,
                  agent: { declaration: definitions.agent, decisionTurn: controllerDigest },
                },
            entry.continuity?.versions,
          )),
          reporting,
        };
      }),
    // The erased descriptor accepts arbitrary provided services. Restore the concrete entry's
    // Exclude<worker requirements, provided services> | layer requirements at this collection seam.
  ) as Effect.Effect<ResolvedBinding, DigestError, Crypto.Crypto | EntryRequirements<Entry>>;

/** Compile heterogeneous Agent descriptors into exact, dependency-closed worker registrations. */
export const compileRegistrations = <const Entries extends ReadonlyArray<AgentRegistration>>(
  entries: Entries,
): Effect.Effect<
  ReadonlyArray<ResolvedBinding>,
  DigestError,
  Crypto.Crypto | RegistrationRequirements<Entries>
> => {
  // `Effect.forEach` instantiates a generic callback at its constraint. Specialize the callback to
  // this tuple's distributed requirement union before collection so empty and heterogeneous tuples
  // retain their exact service requirements.
  const compileEntry = compileRegistration as unknown as (
    entry: Entries[number],
  ) => Effect.Effect<
    ResolvedBinding,
    DigestError,
    Crypto.Crypto | RegistrationRequirements<Entries>
  >;

  return Effect.forEach(entries, compileEntry);
};

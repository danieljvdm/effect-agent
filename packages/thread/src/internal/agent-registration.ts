import type * as Agent from "@effect-agent/core/Agent";
import {
  type InputPromptSource,
  type InstructionSource,
  type ModelServices,
  type RunDispositionDeclaration,
} from "@effect-agent/core/Agent";
import { type ThreadId, AgentId } from "@effect-agent/core/Identifiers";
import {
  AdditionalToolCatalog,
  DiscoveryTool,
  IncludesCatalogDocumentation,
  PinnedTool,
  ToolNamespace,
} from "@effect-agent/core/ToolExposure";
import { type RuntimeBinding } from "@effect-agent/engine/AgentRuntime";
import {
  BackgroundReporting,
  WorkerReportPreparationFailure,
  type WorkerReporting,
} from "@effect-agent/engine/SubagentHost";
import { type Crypto, type Option, type Scope, Context, Effect, Layer, Schema } from "effect";
import { Tool } from "effect/unstable/ai";

import { digestDefinitions, DigestError } from "../Digest.ts";
import type { DurableWorkerFailure, DurableWorkerRequirements } from "../DurableAgentRuntime.ts";
import type { DefinitionDigestInput, DefinitionDigests } from "../Records.ts";
import type { Claim, Settlement } from "../SubmissionLedger.ts";

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

/** Byte-for-byte equality of two stored definition digest triples (SUB-023). */
export const definitionDigestsEqual = (
  left: DefinitionDigests,
  right: DefinitionDigests,
): boolean =>
  left.agent === right.agent && left.model === right.model && left.tools === right.tools;

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

type ReportRequirements<Reports extends ReadonlyArray<WorkerReporting<unknown, unknown>>> = [
  Reports[number],
] extends [never]
  ? never
  : Reports[number] extends WorkerReporting<unknown, infer R>
    ? Exclude<R, Scope.Scope>
    : never;

const captureReporting = <R>(reports: ReadonlyArray<WorkerReporting<unknown, R>>) =>
  Effect.map(Effect.context<Exclude<R, Scope.Scope>>(), (context) =>
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

const capture = <A extends ExecutableAgentBinding, Provides = never, Requires = never>(
  agent: A,
  attemptLayer?: (context: AgentAttemptContext) => Layer.Layer<Provides, never, Requires>,
): Effect.Effect<
  CapturedBinding,
  never,
  Exclude<DurableWorkerRequirements<A>, Provides> | Requires
> =>
  Effect.map(
    Effect.context<Exclude<DurableWorkerRequirements<A>, Provides> | Requires>(),
    (context): CapturedBinding => ({
      agentId: agent.definition.id,
      definition: agent.definition,
      attempt: (driver, threadId, claim) => {
        // Registration closes one concrete Binding before heterogeneous registrations are
        // collected. TypeScript cannot instantiate the higher-rank RuntimeBinding parameters
        // from the intentionally erased public shape, so specialize the driver back to A here.
        const run = driver as unknown as CapturedAttempt<A>;
        const execute = run(agent, threadId, claim);

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

/**
 * Build one exact worker registration from an executable Agent Binding and
 * its already-computed definition digests.
 *
 * `make(agent, digests)` captures the binding plus its worker-requirement
 * Context at Layer/effect construction time. Hosts that start from application
 * version declarations should use `compileRegistrations`; low-level fixtures
 * may supply a previously computed digest triple directly.
 */
export const DurableWorkerBinding = {
  make: <
    A extends ExecutableAgentBinding,
    const Reports extends ReadonlyArray<WorkerReporting<unknown, unknown>> = readonly [],
  >(
    agent: A,
    digests: DefinitionDigests,
    reporting?: Reports,
  ): Effect.Effect<
    ResolvedBinding,
    never,
    DurableWorkerRequirements<A> | ReportRequirements<Reports>
  > =>
    Effect.gen(function* () {
      const binding = yield* capture(agent);

      const reports = yield* captureReporting([
        ...(reporting ?? []),
        ...backgroundReports(agent.definition),
      ]);

      return { ...binding, digests, reporting: reports };
    }) as Effect.Effect<
      ResolvedBinding,
      never,
      DurableWorkerRequirements<A> | ReportRequirements<Reports>
    >,
} as const;

/** INTERNAL identity-only capture retained for the legacy direct worker path. */
export const makeLegacyWorkerBinding = capture;

export const resolveWorkerBinding = (
  bindings: ReadonlyArray<ResolvedBinding>,
  agentId: AgentId,
  digests: DefinitionDigests,
): Effect.Effect<ResolvedBinding, DurableBindingFailure> => {
  const registered = bindings.filter((binding) => binding.agentId === agentId);

  if (registered.length === 0) {
    return Effect.fail(
      BindingUnavailable.make({
        agentId,
        message: `No Agent Binding is registered for ${agentId}; recovery never substitutes different code (SUB-023)`,
      }),
    );
  }
  const exact = registered.find((binding) => definitionDigestsEqual(binding.digests, digests));

  if (exact === undefined) {
    return Effect.fail(
      BindingDigestMismatch.make({
        agentId,
        message: `The registered Binding for ${agentId} does not match the stored definition digests exactly; missing or different code fails closed (SUB-023)`,
      }),
    );
  }

  return Effect.succeed(exact);
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
  /**
   * Source-owned reports capture services outside attemptLayer and open a fresh Scope per
   * preparation. Version changes with this registration. Missing exact code and preparation
   * failures become retained report refusals; durable delivery retries reuse the frozen input.
   */
  readonly reporting?: ReadonlyArray<WorkerReporting<unknown, unknown>>;
  /**
   * Build fresh services for exactly one fenced Attempt, across all its Tool/model turns.
   * Finalizes on completion, suspension, failure and interruption. Never reused after eviction.
   * Resolve invocation authority from the trusted claim identity, not captured caller state.
   * Keep fallible resource acquisition lazy in typed Tool operations; this Layer cannot fail.
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

type EntryAttemptRequirements<Entry> = Entry extends {
  readonly attemptLayer: (
    context: AgentAttemptContext,
  ) => Layer.Layer<infer Provides, never, infer Requires>;
}
  ? Exclude<EntryWorkerRequirements<Entry>, Provides> | Requires
  : EntryWorkerRequirements<Entry>;

type EntryRequirements<Entry> =
  | EntryAttemptRequirements<Entry>
  | (Entry extends {
      readonly reporting: infer Reports extends ReadonlyArray<WorkerReporting<unknown, unknown>>;
    }
      ? ReportRequirements<Reports>
      : never);

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
              mode: report.mode ?? "custom",
              destinationDelegationId: report.destination?.delegationId ?? null,
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
    }).pipe(Effect.flatMap(digestDefinitions)),
    (digests) =>
      Effect.gen(function* () {
        const binding = yield* capture(
          entry.model === undefined ? entry.agent : { definition: entry.agent, model: entry.model },
          entry.attemptLayer,
        );

        const reporting = yield* captureReporting([
          ...(entry.reporting ?? []),
          ...backgroundReports(binding.definition),
        ]);

        return { ...binding, digests, reporting };
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

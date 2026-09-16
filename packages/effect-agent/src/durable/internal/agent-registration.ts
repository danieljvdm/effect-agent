import {
  type Crypto,
  type Option,
  type Scope,
  Array,
  Context,
  Effect,
  Layer,
  Schema,
} from "effect";
import { Tool } from "effect/unstable/ai";

import type * as Agent from "../../core/Agent.ts";
import {
  type InputPromptSource,
  type InstructionSource,
  type ModelServices,
  type RunDispositionDeclaration,
} from "../../core/Agent.ts";
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
import { getToolExecutionClass } from "../../engine/DurableStep.ts";
import {
  BackgroundReporting,
  WorkerReportPreparationFailure,
  type WorkerReporting,
} from "../../engine/SubagentHost.ts";
import { digestDefinitions, digestJson, DigestError } from "../Digest.ts";
import type { DurableWorkerFailure, DurableWorkerRequirements } from "../DurableAgentRuntime.ts";
import {
  DefinitionDigestInput,
  DefinitionDigests,
  ReplayContract,
  PersistedJson,
} from "../Records.ts";
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
 * A Binding is registered for the identity but neither the exact admission fingerprints
 * nor its declared replay contract support the retained pending operations. Refusal leaves
 * the original Submission pending until compatible code is available.
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
  left.agent === right.agent &&
  left.model === right.model &&
  left.tools === right.tools &&
  Schema.toEquivalence(Schema.UndefinedOr(ReplayContract))(left.replay, right.replay);

/**
 * Host-owned semantic versions, independent of deployment and tool inventory. Change a version
 * when the meaning of an operation, its durable Step names or external idempotency keys changes.
 * Codecs and execution class are included automatically; JSON Schema alone cannot prove semantics.
 */
export interface ReplayVersions {
  readonly agent: PersistedJson;
  readonly tools: Readonly<Record<string, PersistedJson>>;
}

/**
 * Data exported from a definition, never an old executable Binding. A host may supply an audited
 * manifest from the original release for work admitted before replay contracts were retained.
 * Its original declarations must hash to that admission; a bare old digest is not a manifest.
 */
const ToolReplayDeclaration = Schema.Struct({
  version: PersistedJson,
  parameters: PersistedJson,
  success: PersistedJson,
  failure: PersistedJson,
  failureMode: Schema.String,
  needsApproval: Schema.Union([Schema.Boolean, Schema.Literal("dynamic")]),
  executionClass: Schema.String,
  executionKind: Schema.String,
});

const AgentReplayDeclaration = Schema.Struct({
  version: PersistedJson,
  id: AgentId,
  input: PersistedJson,
  output: PersistedJson,
  updates: Schema.NullOr(PersistedJson),
  completion: Schema.NullOr(Schema.Struct({ tool: Schema.String, required: Schema.Boolean })),
  completionFromTools: Schema.Array(Schema.String),
  disposition: Schema.NullOr(PersistedJson),
});

export class BindingManifest extends Schema.Class<BindingManifest>("BindingManifest")({
  agentId: AgentId,
  definitions: DefinitionDigestInput,
  replay: ReplayContract,
  agentContract: Schema.optionalKey(AgentReplayDeclaration),
  toolContracts: Schema.optionalKey(Schema.Record(Schema.String, ToolReplayDeclaration)),
}) {}

export const makeBindingManifest = Effect.fn("AgentRegistration.makeBindingManifest")(function* (
  definition: Agent.AnyDefinition,
  definitions: DefinitionDigestInput,
  versions: ReplayVersions,
) {
  const jsonSchema = (schema: Schema.Top) =>
    Schema.decodeUnknownSync(Schema.Json)(Tool.getJsonSchemaFromSchema(schema));

  const contracts = yield* Effect.try({
    try: () => {
      const tools = Object.values(definition.toolkit.tools);

      if (tools.some((tool) => !Object.hasOwn(versions.tools, tool.name)))
        throw new Error("Every Tool needs an explicit replay semantic version");

      return {
        agent: {
          version: versions.agent,
          id: definition.id,
          input: jsonSchema(definition.input),
          output: jsonSchema(definition.output),
          updates: definition.updates === undefined ? null : jsonSchema(definition.updates),
          completion:
            definition.completion === undefined
              ? null
              : {
                  tool: definition.completion.tool,
                  required: definition.completion.required ?? false,
                },
          completionFromTools:
            definition.completionFromTools?.map((entry) => entry.tool).sort() ?? [],
          disposition:
            definition.runDisposition === undefined
              ? null
              : jsonSchema(definition.runDisposition.schema),
        },
        tools: tools.map((tool) => ({
          name: tool.name,
          contract: {
            version: versions.tools[tool.name]!,
            parameters: jsonSchema(tool.parametersSchema),
            success: jsonSchema(tool.successSchema),
            failure: jsonSchema(tool.failureSchema),
            failureMode: tool.failureMode,
            needsApproval:
              typeof tool.needsApproval === "function" ? "dynamic" : (tool.needsApproval ?? false),
            executionClass: getToolExecutionClass(tool),
            executionKind: getToolExecutionKind(tool.annotations),
          },
        })),
      };
    },
    catch: () =>
      DigestError.make({
        message:
          "Replay contracts require serializable codecs and an explicit semantic version for every Tool",
      }),
  });

  const agent = yield* digestJson(contracts.agent);

  const tools = yield* Effect.forEach(contracts.tools, ({ name, contract }) =>
    digestJson(contract).pipe(Effect.map((digest) => [name, digest] as const)),
  );

  return BindingManifest.make({
    agentId: definition.id,
    definitions,
    replay: ReplayContract.make({ agent, tools: Object.fromEntries(tools) }),
    agentContract: contracts.agent,
    toolContracts: Object.fromEntries(
      contracts.tools.map(({ name, contract }) => [name, contract]),
    ),
  });
});

const jsonEqual = Schema.toEquivalence(PersistedJson);

// JSON Schema generation can widen object openness without changing an Effect codec.
// Prove only this widening; all other codec changes still require an exact contract.
const schemasCompatible = (previous: PersistedJson, current: PersistedJson): boolean => {
  if (jsonEqual(previous, current)) return true;

  const hasNegativeSchema = (value: PersistedJson): boolean => {
    if (Array.isArray<PersistedJson>(value)) return value.some(hasNegativeSchema);
    if (value === null || typeof value !== "object") return false;

    return Object.entries(value).some(
      ([key, child]) =>
        key === "oneOf" ||
        key === "maxContains" ||
        key === "unevaluatedProperties" ||
        key === "unevaluatedItems" ||
        key === "if" ||
        key === "then" ||
        key === "else" ||
        key === "$dynamicRef" ||
        key === "$recursiveRef" ||
        (key === "not" && !jsonEqual(child, {})) ||
        hasNegativeSchema(child),
    );
  };

  if (hasNegativeSchema(previous) || hasNegativeSchema(current)) return false;

  const compare = (before: PersistedJson, after: PersistedJson): boolean => {
    if (jsonEqual(before, after)) return true;
    if (
      before === null ||
      typeof before !== "object" ||
      Array.isArray<PersistedJson>(before) ||
      after === null ||
      typeof after !== "object" ||
      Array.isArray<PersistedJson>(after)
    )
      return false;
    if (Object.keys(before).length !== Object.keys(after).length) return false;

    return Object.entries(before).every(([key, value]) => {
      if (!Object.hasOwn(after, key)) return false;
      const next = after[key]!;

      if (jsonEqual(value, next)) return true;
      if (key === "additionalProperties") return value === false && next === true;
      if (key === "items") return compare(value, next);
      if (
        (key === "anyOf" || key === "allOf") &&
        Array.isArray<PersistedJson>(value) &&
        Array.isArray<PersistedJson>(next)
      )
        return (
          value.length === next.length &&
          value.every((entry, index) => compare(entry, next[index]!))
        );
      if (
        (key === "properties" || key === "$defs" || key === "definitions") &&
        value !== null &&
        typeof value === "object" &&
        !Array.isArray<PersistedJson>(value) &&
        next !== null &&
        typeof next === "object" &&
        !Array.isArray<PersistedJson>(next)
      )
        return (
          Object.keys(value).length === Object.keys(next).length &&
          Object.entries(value).every(
            ([name, schema]) => Object.hasOwn(next, name) && compare(schema, next[name]!),
          )
        );

      return false;
    });
  };

  return compare(previous, current);
};

/** Compile audited declaration data once, without acquiring an executable or rewriting receipts.
 * Full input schemas stay in registration metadata, never in each admission's replay contract. */
export const compileBindingManifest = Effect.fn("AgentRegistration.compileBindingManifest")(
  function* (
    definition: Agent.AnyDefinition,
    definitions: DefinitionDigestInput,
    versions: ReplayVersions,
    retained: ReadonlyArray<BindingManifest> = [],
  ): Effect.fn.Return<
    Pick<ResolvedBinding, "digests" | "manifest" | "retainedManifests">,
    DigestError,
    Crypto.Crypto
  > {
    const manifest = yield* makeBindingManifest(definition, definitions, versions);

    const validContract = Effect.fnUntraced(function* (entry: BindingManifest) {
      if (entry.agentContract === undefined) return undefined;
      if (
        entry.agentContract.id !== entry.agentId ||
        (yield* digestJson(entry.agentContract)) !== entry.replay.agent
      )
        return yield* DigestError.make({
          message: "A manifest's Agent contract does not match its replay digest",
        });

      return entry.agentContract;
    });

    const current = manifest.agentContract!;
    const acceptsInput = Schema.is(Schema.toEncoded(definition.input));

    const validateTools = Effect.fnUntraced(function* (entry: BindingManifest) {
      for (const [name, contract] of Object.entries(entry.toolContracts ?? {}))
        if ((yield* digestJson(contract)) !== entry.replay.tools[name])
          return yield* DigestError.make({
            message: "A manifest's Tool contract does not match its replay digest",
          });
    });

    const digests = yield* digestDefinitions(manifest.definitions);

    const retainedManifests = yield* Effect.forEach(
      retained,
      Effect.fnUntraced(function* (entry) {
        if (entry.agentId !== manifest.agentId)
          return yield* DigestError.make({
            message: "A retained manifest belongs to another Agent",
          });
        const previous = yield* validContract(entry);

        yield* validateTools(entry);

        const compatible =
          previous !== undefined &&
          jsonEqual(
            { ...previous, input: null, output: null, updates: null, disposition: null },
            { ...current, input: null, output: null, updates: null, disposition: null },
          ) &&
          schemasCompatible(previous.output, current.output) &&
          schemasCompatible(previous.updates, current.updates) &&
          schemasCompatible(previous.disposition, current.disposition);

        const compatibleTools: Record<string, string> = {};

        for (const [name, oldTool] of Object.entries(entry.toolContracts ?? {})) {
          const newTool = manifest.toolContracts?.[name];

          if (
            newTool !== undefined &&
            jsonEqual(
              { ...oldTool, parameters: null, success: null, failure: null },
              { ...newTool, parameters: null, success: null, failure: null },
            ) &&
            schemasCompatible(oldTool.parameters, newTool.parameters) &&
            schemasCompatible(oldTool.success, newTool.success) &&
            schemasCompatible(oldTool.failure, newTool.failure)
          )
            compatibleTools[name] = manifest.replay.tools[name]!;
        }

        return {
          digests: yield* digestDefinitions(entry.definitions),
          replay: entry.replay,
          compatibleTools,
          ...(compatible
            ? {
                inputCompatibility: {
                  agent: manifest.replay.agent,
                  accepts: (input: PersistedJson) => {
                    // Async checks and defective guards cannot establish synchronous replay proof.
                    try {
                      return acceptsInput(input);
                    } catch {
                      return false;
                    }
                  },
                },
              }
            : {}),
        };
      }),
    );

    return {
      digests: DefinitionDigests.make({ ...digests, replay: manifest.replay }),
      manifest,
      retainedManifests,
    };
  },
);

const replaySupports = (
  current: ReplayContract,
  retained: ReplayContract,
  requiredTools?: ReadonlyArray<string>,
  inputCompatible = false,
  compatibleTools: Readonly<Record<string, string>> = {},
): boolean =>
  (current.agent === retained.agent || inputCompatible) &&
  (requiredTools ?? Object.keys(retained.tools)).every(
    (name) =>
      retained.tools[name] !== undefined &&
      (current.tools[name] === retained.tools[name] ||
        (current.tools[name] !== undefined && compatibleTools[name] === current.tools[name])),
  );

/** Resolve executable code only. Admission, delivery, lineage and receipt comparisons stay exact. */
export const bindingSupports = (
  binding: Pick<ResolvedBinding, "digests" | "retainedManifests">,
  digests: DefinitionDigests,
  requiredTools?: ReadonlyArray<string>,
  input?: PersistedJson,
): boolean => {
  if (definitionDigestsEqual(binding.digests, digests)) return true;

  const manifest = binding.retainedManifests?.find(
    (entry) =>
      definitionDigestsEqual(entry.digests, digests) ||
      definitionDigestsEqual({ ...entry.digests, replay: entry.replay }, digests),
  );

  const retained = digests.replay ?? manifest?.replay;

  return (
    binding.digests.replay !== undefined &&
    retained !== undefined &&
    replaySupports(
      binding.digests.replay,
      retained,
      requiredTools,
      manifest?.inputCompatibility?.agent === binding.digests.replay.agent &&
        input !== undefined &&
        manifest.inputCompatibility.accepts(input),
      manifest?.compatibleTools,
    )
  );
};

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

type ReportRequirements<Reports> =
  Reports extends ReadonlyArray<WorkerReporting<unknown, unknown>>
    ? [Reports[number]] extends [never]
      ? never
      : Reports[number] extends WorkerReporting<unknown, infer R>
        ? Exclude<R, Scope.Scope>
        : never
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
  readonly manifest?: BindingManifest;
  readonly retainedManifests?: ReadonlyArray<{
    readonly digests: DefinitionDigests;
    readonly replay: ReplayContract;
    readonly compatibleTools?: Readonly<Record<string, string>>;
    /** Verified against both original declarations; applies only to this current Agent digest. */
    readonly inputCompatibility?: {
      readonly agent: string;
      readonly accepts: (input: PersistedJson) => boolean;
    };
  }>;
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
  requiredTools?: ReadonlyArray<string>,
  input?: PersistedJson,
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

  const compatible =
    exact ??
    (registered.length === 1 && bindingSupports(registered[0]!, digests, requiredTools, input)
      ? registered[0]
      : undefined);

  if (compatible === undefined) {
    return Effect.fail(
      BindingDigestMismatch.make({
        agentId,
        message: `No registered Binding proves the retained replay contracts for ${agentId}; the original request and receipts remain pending`,
      }),
    );
  }

  return Effect.succeed(compatible);
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
  /** Opt into contract-checked deployment continuity. Omit to require an exact registration. */
  readonly continuity?: {
    readonly versions: ReplayVersions;
    readonly retainedManifests?: ReadonlyArray<BindingManifest>;
  };
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

type AttemptLayerRequirements<Requirements, AttemptLayer> = AttemptLayer extends (
  context: AgentAttemptContext,
) => Layer.Layer<infer Provides, never, infer Requires>
  ? Exclude<Requirements, Provides> | Requires
  : Requirements;

// Conditional options retain every service they may consume. An absent attempt Layer
// still needs the original worker services; only a definite Layer can remove them.
type EntryRequirements<Entry> = Entry extends unknown
  ?
      | AttemptLayerRequirements<
          EntryWorkerRequirements<Entry>,
          "attemptLayer" extends keyof Entry ? Entry["attemptLayer"] : undefined
        >
      | ("reporting" extends keyof Entry ? ReportRequirements<Entry["reporting"]> : never)
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
    }),
    (definitions) =>
      Effect.gen(function* () {
        const binding = yield* capture(
          entry.model === undefined ? entry.agent : { definition: entry.agent, model: entry.model },
          entry.attemptLayer,
        );

        const reporting = yield* captureReporting([
          ...(entry.reporting ?? []),
          ...backgroundReports(binding.definition),
        ]);

        if (entry.continuity === undefined)
          return { ...binding, digests: yield* digestDefinitions(definitions), reporting };

        return {
          ...binding,
          ...(yield* compileBindingManifest(
            binding.definition,
            definitions,
            entry.continuity.versions,
            entry.continuity.retainedManifests,
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

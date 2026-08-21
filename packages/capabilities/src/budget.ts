import { Clock, Duration, Effect, Ref, Schema, type Scope } from "effect";

const Natural = Schema.Natural;
const BudgetLevel = Schema.Literals(["global", "tenant", "agent", "conversation", "run"]);
export type BudgetLevel = typeof BudgetLevel.Type;

/** Fixed units avoid floating-point currency accounting: costs are micro-USD. */
export class UsageTotals extends Schema.Class<UsageTotals>(
  "@effect-agent/capabilities/UsageTotals",
)({
  /** Total input tokens across every model call, INCLUDING cache reads and writes at par. */
  inputTokens: Natural,
  outputTokens: Natural,
  /** Informational split of `inputTokens` served from the provider prompt cache; never separately limited. */
  cacheReadInputTokens: Natural,
  /** Informational split of `inputTokens` written to the provider prompt cache; never separately limited. */
  cacheWriteInputTokens: Natural,
  /** The most recent model call's input tokens — the live model-context estimate (CAP-017). */
  lastInputTokens: Natural,
  /** The most recent model call's output tokens. */
  lastOutputTokens: Natural,
  toolCalls: Natural,
  costMicrousd: Natural,
  elapsedMillis: Natural,
}) {}

/** Increment recorded at one model/tool accounting boundary. */
export class UsageDelta extends Schema.Class<UsageDelta>("@effect-agent/capabilities/UsageDelta")({
  /** `1` for a model-response boundary, `0` for a mid-pass tool charge; gates last-call tracking. */
  modelCalls: Natural,
  /** Total input tokens of this boundary, INCLUDING cache reads and writes at par. */
  inputTokens: Natural,
  outputTokens: Natural,
  /** Informational split of `inputTokens` served from the provider prompt cache; never separately limited. */
  cacheReadInputTokens: Natural,
  /** Informational split of `inputTokens` written to the provider prompt cache; never separately limited. */
  cacheWriteInputTokens: Natural,
  toolCalls: Natural,
  costMicrousd: Natural,
}) {}

/** All optional limits are finite when present; absent means not configured. */
export class UsageBudgetLimits extends Schema.Class<UsageBudgetLimits>(
  "@effect-agent/capabilities/UsageBudgetLimits",
)({
  maxInputTokens: Schema.optionalKey(Natural),
  maxOutputTokens: Schema.optionalKey(Natural),
  maxToolCalls: Schema.optionalKey(Natural),
  maxCostMicrousd: Schema.optionalKey(Natural),
  maxDurationMillis: Schema.optionalKey(Natural),
}) {}

/** Identity and local limits for one node in a global-to-Run budget hierarchy. */
export class UsageBudgetNodeConfig extends Schema.Class<UsageBudgetNodeConfig>(
  "@effect-agent/capabilities/UsageBudgetNodeConfig",
)({
  level: BudgetLevel,
  id: Schema.NonEmptyString,
  limits: UsageBudgetLimits,
}) {}

/** Typed finite-budget rejection. A rejected increment is not committed at any hierarchy level. */
export class BudgetExceeded extends Schema.TaggedError<BudgetExceeded>()("BudgetExceeded", {
  scopeLevel: BudgetLevel,
  scopeId: Schema.NonEmptyString,
  limit: Schema.Literals(["input-tokens", "output-tokens", "tool-calls", "cost", "duration"]),
  limitValue: Natural,
  observedValue: Natural,
}) {}

/** Hierarchical accounting authority. All ancestors share one atomic Ref-backed transaction. */
export interface UsageBudgetNode {
  readonly level: BudgetLevel;
  readonly id: string;
  /**
   * Register or re-attach one child node. Re-attaching an already-registered
   * `level:id` with identical limits returns a handle to the shared
   * registration; different limits fail with BudgetNodeConflict rather than
   * being silently ignored. Every returned handle owns one registration and
   * must be retired when its lifetime ends; prefer `childScoped` when the
   * lifetime already belongs to an Effect Scope.
   */
  readonly child: (
    config: UsageBudgetNodeConfig,
  ) => Effect.Effect<UsageBudgetNode, InvalidBudgetHierarchy | BudgetNodeConflict>;
  /** Register a child whose registration is retired when the current Scope closes. */
  readonly childScoped: (
    config: UsageBudgetNodeConfig,
  ) => Effect.Effect<UsageBudgetNode, InvalidBudgetHierarchy | BudgetNodeConflict, Scope.Scope>;
  /**
   * Release this handle's registration exactly once. Accumulated usage stays
   * charged to every live ancestor; a node is reclaimed only after its last
   * handle and every descendant have retired.
   */
  readonly retire: Effect.Effect<void>;
  readonly consume: (delta: UsageDelta) => Effect.Effect<UsageTotals, BudgetExceeded>;
  readonly snapshot: Effect.Effect<UsageTotals>;
  /**
   * Guard stalled model/tool work with the earliest configured ancestor deadline.
   * The timeout is driven by Effect Clock and interrupts the guarded child effect.
   */
  readonly guard: <A, E, R>(
    effect: Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E | BudgetExceeded, R>;
}

/** A child budget must move strictly inward through the declared hierarchy. */
export class InvalidBudgetHierarchy extends Schema.TaggedError<InvalidBudgetHierarchy>()(
  "InvalidBudgetHierarchy",
  {
    parentLevel: BudgetLevel,
    childLevel: BudgetLevel,
  },
) {}

/** A child `level:id` is already registered with different limits; neither set may win silently. */
export class BudgetNodeConflict extends Schema.TaggedError<BudgetNodeConflict>()(
  "BudgetNodeConflict",
  {
    scopeLevel: BudgetLevel,
    scopeId: Schema.NonEmptyString,
  },
) {}

interface LedgerNode {
  readonly config: UsageBudgetNodeConfig;
  readonly startedAt: number;
  readonly totals: UsageTotals;
  /** Ledger-owned identities of the live handles for this exact registration. */
  readonly handles: ReadonlySet<symbol>;
}

interface BudgetLedger {
  readonly nodes: ReadonlyMap<string, LedgerNode>;
}

type ConsumptionResult =
  | { readonly _tag: "success"; readonly value: UsageTotals }
  | { readonly _tag: "failure"; readonly error: BudgetExceeded }
  | { readonly _tag: "retired" };

type ChildRegistrationResult =
  | { readonly _tag: "success" }
  | { readonly _tag: "conflict"; readonly error: BudgetNodeConflict }
  | { readonly _tag: "invalid"; readonly error: InvalidBudgetHierarchy }
  | { readonly _tag: "retired" };

const levelOrder: Readonly<Record<BudgetLevel, number>> = {
  global: 0,
  tenant: 1,
  agent: 2,
  conversation: 3,
  run: 4,
};

const emptyTotals = (elapsedMillis = 0): UsageTotals =>
  UsageTotals.make({
    inputTokens: 0,
    outputTokens: 0,
    cacheReadInputTokens: 0,
    cacheWriteInputTokens: 0,
    lastInputTokens: 0,
    lastOutputTokens: 0,
    toolCalls: 0,
    costMicrousd: 0,
    elapsedMillis,
  });

// Spread-preserve every accumulated field so future additions cannot be
// silently dropped by an elapsed-only snapshot.
const withElapsed = (node: LedgerNode, now: number): UsageTotals =>
  UsageTotals.make({
    ...node.totals,
    elapsedMillis: Math.max(0, now - node.startedAt),
  });

const addUsage = (node: LedgerNode, delta: UsageDelta, now: number): UsageTotals =>
  UsageTotals.make({
    inputTokens: node.totals.inputTokens + delta.inputTokens,
    outputTokens: node.totals.outputTokens + delta.outputTokens,
    cacheReadInputTokens: node.totals.cacheReadInputTokens + delta.cacheReadInputTokens,
    cacheWriteInputTokens: node.totals.cacheWriteInputTokens + delta.cacheWriteInputTokens,
    lastInputTokens: delta.modelCalls > 0 ? delta.inputTokens : node.totals.lastInputTokens,
    lastOutputTokens: delta.modelCalls > 0 ? delta.outputTokens : node.totals.lastOutputTokens,
    toolCalls: node.totals.toolCalls + delta.toolCalls,
    costMicrousd: node.totals.costMicrousd + delta.costMicrousd,
    elapsedMillis: Math.max(0, now - node.startedAt),
  });

const exceeded = (node: LedgerNode, totals: UsageTotals): BudgetExceeded | undefined => {
  const limits = node.config.limits;
  const checks: ReadonlyArray<readonly [BudgetExceeded["limit"], number | undefined, number]> = [
    ["input-tokens", limits.maxInputTokens, totals.inputTokens],
    ["output-tokens", limits.maxOutputTokens, totals.outputTokens],
    ["tool-calls", limits.maxToolCalls, totals.toolCalls],
    ["cost", limits.maxCostMicrousd, totals.costMicrousd],
    ["duration", limits.maxDurationMillis, totals.elapsedMillis],
  ];
  for (const [limit, limitValue, observedValue] of checks) {
    if (limitValue !== undefined && observedValue > limitValue) {
      return BudgetExceeded.make({
        scopeLevel: node.config.level,
        scopeId: node.config.id,
        limit,
        limitValue,
        observedValue,
      });
    }
  }
  return undefined;
};

const sameLimits = (a: UsageBudgetLimits, b: UsageBudgetLimits): boolean =>
  a.maxInputTokens === b.maxInputTokens &&
  a.maxOutputTokens === b.maxOutputTokens &&
  a.maxToolCalls === b.maxToolCalls &&
  a.maxCostMicrousd === b.maxCostMicrousd &&
  a.maxDurationMillis === b.maxDurationMillis;

const durationExceeded = (node: LedgerNode, now: number): BudgetExceeded => {
  const limitValue = node.config.limits.maxDurationMillis ?? 0;
  return BudgetExceeded.make({
    scopeLevel: node.config.level,
    scopeId: node.config.id,
    limit: "duration",
    limitValue,
    observedValue: Math.max(limitValue + 1, now - node.startedAt),
  });
};

/** Reclaim one released leaf and any newly unowned ancestors on its fixed hierarchy path. */
const pruneRetiredHierarchy = (
  nodes: Map<string, LedgerNode>,
  hierarchy: ReadonlyArray<string>,
): Map<string, LedgerNode> => {
  for (let index = hierarchy.length - 1; index >= 0; index -= 1) {
    const candidateKey = hierarchy[index];
    if (candidateKey === undefined) continue;
    const candidate = nodes.get(candidateKey);
    if (candidate === undefined) continue;
    if (candidate.handles.size > 0) break;
    const childPrefix = `${candidateKey}/`;
    if ([...nodes.keys()].some((otherKey) => otherKey.startsWith(childPrefix))) {
      break;
    }
    nodes.delete(candidateKey);
  }
  return nodes;
};

const makeNode = (
  ledger: Ref.Ref<BudgetLedger>,
  key: string,
  ancestors: ReadonlyArray<string>,
  config: UsageBudgetNodeConfig,
  handleId: symbol,
): UsageBudgetNode => {
  const hierarchy = [...ancestors, key];
  const retiredDefect = () =>
    Effect.die(
      new Error(`Usage budget handle ${config.level}:${config.id} was used after retirement`),
    );
  const liveHierarchy = (state: BudgetLedger): ReadonlyArray<LedgerNode> | undefined => {
    const current = state.nodes.get(key);
    if (current === undefined || !current.handles.has(handleId)) return undefined;
    const nodes: Array<LedgerNode> = [];
    for (const ancestorKey of hierarchy) {
      const node = state.nodes.get(ancestorKey);
      if (node === undefined) return undefined;
      nodes.push(node);
    }
    return nodes;
  };
  const readLiveHierarchy = Ref.modify(
    ledger,
    (state): readonly [ReadonlyArray<LedgerNode> | undefined, BudgetLedger] => [
      liveHierarchy(state),
      state,
    ],
  );

  const snapshot = Effect.gen(function* () {
    const now = yield* Clock.currentTimeMillis;
    const nodes = yield* readLiveHierarchy;
    if (nodes === undefined) return yield* retiredDefect();
    const node = nodes[nodes.length - 1];
    if (node === undefined) return yield* retiredDefect();
    return withElapsed(node, now);
  });

  const guard = <A, E, R>(
    effect: Effect.Effect<A, E, R>,
  ): Effect.Effect<A, E | BudgetExceeded, R> =>
    Effect.gen(function* () {
      const now = yield* Clock.currentTimeMillis;
      const nodes = yield* readLiveHierarchy;
      if (nodes === undefined) return yield* retiredDefect();
      let deadline: { readonly remaining: number; readonly node: LedgerNode } | undefined;
      for (const node of nodes) {
        const current = withElapsed(node, now);
        const alreadyExceeded = exceeded(node, current);
        if (alreadyExceeded !== undefined) {
          return yield* alreadyExceeded;
        }
        const maxDuration = node.config.limits.maxDurationMillis;
        if (maxDuration !== undefined) {
          const remaining = maxDuration - current.elapsedMillis;
          if (remaining <= 0) {
            return yield* durationExceeded(node, now);
          }
          if (deadline === undefined || remaining < deadline.remaining) {
            deadline = { remaining, node };
          }
        }
      }
      if (deadline === undefined) {
        return yield* effect;
      }
      const selected = deadline;
      return yield* effect.pipe(
        Effect.timeoutOrElse({
          duration: Duration.millis(selected.remaining),
          orElse: () =>
            Clock.currentTimeMillis.pipe(
              Effect.flatMap((timeoutAt) =>
                Effect.fail(durationExceeded(selected.node, timeoutAt)),
              ),
            ),
        }),
      );
    });

  const child = Effect.fn("UsageBudgetNode.child")(function* (childConfig: UsageBudgetNodeConfig) {
    const childKey = `${key}/${childConfig.level}:${childConfig.id}`;
    const childHandleId = Symbol(`usage-budget:${childConfig.level}:${childConfig.id}`);
    const now = yield* Clock.currentTimeMillis;
    const registration = yield* Ref.modify(
      ledger,
      (state): readonly [ChildRegistrationResult, BudgetLedger] => {
        if (liveHierarchy(state) === undefined) {
          return [{ _tag: "retired" }, state];
        }
        if (levelOrder[childConfig.level] <= levelOrder[config.level]) {
          return [
            {
              _tag: "invalid",
              error: InvalidBudgetHierarchy.make({
                parentLevel: config.level,
                childLevel: childConfig.level,
              }),
            },
            state,
          ];
        }
        const existing = state.nodes.get(childKey);
        if (existing !== undefined) {
          return sameLimits(existing.config.limits, childConfig.limits)
            ? [
                { _tag: "success" },
                {
                  nodes: new Map(state.nodes).set(childKey, {
                    ...existing,
                    handles: new Set(existing.handles).add(childHandleId),
                  }),
                },
              ]
            : [
                {
                  _tag: "conflict",
                  error: BudgetNodeConflict.make({
                    scopeLevel: childConfig.level,
                    scopeId: childConfig.id,
                  }),
                },
                state,
              ];
        }
        return [
          { _tag: "success" },
          {
            nodes: new Map(state.nodes).set(childKey, {
              config: childConfig,
              startedAt: now,
              totals: emptyTotals(),
              handles: new Set([childHandleId]),
            }),
          },
        ];
      },
    );
    if (registration._tag === "retired") return yield* retiredDefect();
    if (registration._tag === "conflict" || registration._tag === "invalid") {
      return yield* registration.error;
    }
    return makeNode(ledger, childKey, hierarchy, childConfig, childHandleId);
  });

  const retire = Effect.uninterruptible(
    Ref.update(ledger, (state) => {
      const existing = state.nodes.get(key);
      if (existing === undefined || !existing.handles.has(handleId)) return state;
      const handles = new Set(existing.handles);
      handles.delete(handleId);
      const nodes = new Map(state.nodes).set(key, { ...existing, handles });
      return { nodes: pruneRetiredHierarchy(nodes, hierarchy) };
    }),
  );

  return {
    level: config.level,
    id: config.id,
    child,
    childScoped: (childConfig) => Effect.acquireRelease(child(childConfig), (node) => node.retire),
    retire,
    consume: (delta) =>
      Clock.currentTimeMillis.pipe(
        Effect.flatMap((now) =>
          Ref.modify(ledger, (state): readonly [ConsumptionResult, BudgetLedger] => {
            if (liveHierarchy(state) === undefined) {
              return [{ _tag: "retired" }, state];
            }
            const updates = new Map<string, LedgerNode>();
            for (const ancestorKey of hierarchy) {
              const node = state.nodes.get(ancestorKey);
              if (node === undefined) return [{ _tag: "retired" }, state];
              const totals = addUsage(node, delta, now);
              const error = exceeded(node, totals);
              if (error !== undefined) {
                return [{ _tag: "failure", error }, state];
              }
              updates.set(ancestorKey, { ...node, totals });
            }
            const nextNodes = new Map(state.nodes);
            for (const [updatedKey, updatedNode] of updates) {
              nextNodes.set(updatedKey, updatedNode);
            }
            const value = updates.get(key)?.totals;
            return value === undefined
              ? [{ _tag: "retired" }, state]
              : [{ _tag: "success", value }, { nodes: nextNodes }];
          }).pipe(
            Effect.flatMap((result) => {
              switch (result._tag) {
                case "success":
                  return Effect.succeed(result.value);
                case "failure":
                  return Effect.fail(result.error);
                case "retired":
                  return retiredDefect();
              }
            }),
          ),
        ),
      ),
    snapshot,
    guard,
  };
};

/** Create the outermost node of a hierarchy. Child nodes share its atomic ledger. */
export const makeUsageBudgetRoot = Effect.fn("makeUsageBudgetRoot")(function* (
  config: UsageBudgetNodeConfig,
) {
  const startedAt = yield* Clock.currentTimeMillis;
  const key = `${config.level}:${config.id}`;
  const handleId = Symbol(`usage-budget:${config.level}:${config.id}`);
  const ledger = yield* Ref.make<BudgetLedger>({
    nodes: new Map([
      [
        key,
        {
          config,
          startedAt,
          totals: emptyTotals(),
          handles: new Set([handleId]),
        },
      ],
    ]),
  });
  return makeNode(ledger, key, [], config, handleId);
});

/** Compatibility constructor for a standalone Run budget. */
export const makeUsageBudget = (limits: UsageBudgetLimits): Effect.Effect<UsageBudgetNode> =>
  makeUsageBudgetRoot(
    UsageBudgetNodeConfig.make({
      level: "run",
      id: "standalone-run",
      limits,
    }),
  );

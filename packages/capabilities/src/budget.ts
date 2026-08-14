import { Clock, Context, Duration, Effect, Layer, Ref, Schema } from "effect";

const Natural = Schema.Natural;
const BudgetLevel = Schema.Literals(["global", "tenant", "agent", "conversation", "run"]);
export type BudgetLevel = typeof BudgetLevel.Type;

/** Fixed units avoid floating-point currency accounting: costs are micro-USD. */
export class UsageTotals extends Schema.Class<UsageTotals>(
  "@effect-agent/capabilities/UsageTotals",
)({
  inputTokens: Natural,
  outputTokens: Natural,
  toolCalls: Natural,
  costMicrousd: Natural,
  elapsedMillis: Natural,
}) {}

/** Increment recorded at one model/tool accounting boundary. */
export class UsageDelta extends Schema.Class<UsageDelta>("@effect-agent/capabilities/UsageDelta")({
  inputTokens: Natural,
  outputTokens: Natural,
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
   * being silently ignored.
   */
  readonly child: (
    config: UsageBudgetNodeConfig,
  ) => Effect.Effect<UsageBudgetNode, InvalidBudgetHierarchy | BudgetNodeConflict>;
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

/** The Run-local node supplied to engine workflows. */
export class UsageBudget extends Context.Service<UsageBudget, UsageBudgetNode>()(
  "@effect-agent/capabilities/UsageBudget",
) {}

interface LedgerNode {
  readonly config: UsageBudgetNodeConfig;
  readonly startedAt: number;
  readonly totals: UsageTotals;
}

interface BudgetLedger {
  readonly nodes: ReadonlyMap<string, LedgerNode>;
}

type ConsumptionResult =
  | { readonly _tag: "success"; readonly value: UsageTotals }
  | { readonly _tag: "failure"; readonly error: BudgetExceeded };

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
    toolCalls: 0,
    costMicrousd: 0,
    elapsedMillis,
  });

const withElapsed = (node: LedgerNode, now: number): UsageTotals =>
  UsageTotals.make({
    inputTokens: node.totals.inputTokens,
    outputTokens: node.totals.outputTokens,
    toolCalls: node.totals.toolCalls,
    costMicrousd: node.totals.costMicrousd,
    elapsedMillis: Math.max(0, now - node.startedAt),
  });

const addUsage = (node: LedgerNode, delta: UsageDelta, now: number): UsageTotals =>
  UsageTotals.make({
    inputTokens: node.totals.inputTokens + delta.inputTokens,
    outputTokens: node.totals.outputTokens + delta.outputTokens,
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

const makeNode = (
  ledger: Ref.Ref<BudgetLedger>,
  key: string,
  ancestors: ReadonlyArray<string>,
  config: UsageBudgetNodeConfig,
): UsageBudgetNode => {
  const hierarchy = [...ancestors, key];

  const snapshot = Effect.gen(function* () {
    const now = yield* Clock.currentTimeMillis;
    const state = yield* Ref.get(ledger);
    const node = state.nodes.get(key);
    return node === undefined ? emptyTotals() : withElapsed(node, now);
  });

  const guard = <A, E, R>(
    effect: Effect.Effect<A, E, R>,
  ): Effect.Effect<A, E | BudgetExceeded, R> =>
    Effect.gen(function* () {
      const now = yield* Clock.currentTimeMillis;
      const state = yield* Ref.get(ledger);
      let deadline: { readonly remaining: number; readonly node: LedgerNode } | undefined;
      for (const ancestorKey of hierarchy) {
        const node = state.nodes.get(ancestorKey);
        if (node === undefined) {
          continue;
        }
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

  return {
    level: config.level,
    id: config.id,
    child: (childConfig) =>
      Effect.gen(function* () {
        if (levelOrder[childConfig.level] <= levelOrder[config.level]) {
          return yield* InvalidBudgetHierarchy.make({
            parentLevel: config.level,
            childLevel: childConfig.level,
          });
        }
        const childKey = `${key}/${childConfig.level}:${childConfig.id}`;
        const now = yield* Clock.currentTimeMillis;
        const conflict = yield* Ref.modify(
          ledger,
          (state): readonly [BudgetNodeConflict | undefined, BudgetLedger] => {
            const existing = state.nodes.get(childKey);
            if (existing !== undefined) {
              return sameLimits(existing.config.limits, childConfig.limits)
                ? [undefined, state]
                : [
                    BudgetNodeConflict.make({
                      scopeLevel: childConfig.level,
                      scopeId: childConfig.id,
                    }),
                    state,
                  ];
            }
            return [
              undefined,
              {
                nodes: new Map(state.nodes).set(childKey, {
                  config: childConfig,
                  startedAt: now,
                  totals: emptyTotals(),
                }),
              },
            ];
          },
        );
        if (conflict !== undefined) return yield* conflict;
        return makeNode(ledger, childKey, hierarchy, childConfig);
      }),
    consume: (delta) =>
      Clock.currentTimeMillis.pipe(
        Effect.flatMap((now) =>
          Ref.modify(ledger, (state): readonly [ConsumptionResult, BudgetLedger] => {
            const updates = new Map<string, LedgerNode>();
            for (const ancestorKey of hierarchy) {
              const node = state.nodes.get(ancestorKey);
              if (node === undefined) {
                continue;
              }
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
            const value = updates.get(key)?.totals ?? emptyTotals();
            return [{ _tag: "success", value }, { nodes: nextNodes }];
          }).pipe(
            Effect.flatMap((result) =>
              result._tag === "success" ? Effect.succeed(result.value) : Effect.fail(result.error),
            ),
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
  const ledger = yield* Ref.make<BudgetLedger>({
    nodes: new Map([
      [
        key,
        {
          config,
          startedAt,
          totals: emptyTotals(),
        },
      ],
    ]),
  });
  return makeNode(ledger, key, [], config);
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

/** Intrinsic limits are explicit at layer construction; Clock remains test-controllable. */
export const usageBudgetLayer = (limits: UsageBudgetLimits) =>
  Layer.effect(UsageBudget)(makeUsageBudget(limits));

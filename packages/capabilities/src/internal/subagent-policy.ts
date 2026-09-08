import type { AnyDefinition } from "@effect-agent/core/Agent";
import { AgentPolicy } from "@effect-agent/core/AgentPolicy";
import {
  SubagentDelegationCaps,
  type SubagentBudgetReservation,
} from "@effect-agent/core/SubagentContract";
import { Duration } from "effect";

import {
  SubagentPolicy,
  delegationCapsFromPolicy,
  delegationAllocationFromPolicy,
} from "./subagent-contract.ts";

export const resolveSubagentPolicy = (
  delegation: {
    readonly target: Pick<AnyDefinition, "policy" | "policyOverrides">;
    readonly policy?: SubagentPolicy | undefined;
  },
  parent: AgentPolicy,
  parentCaps?: SubagentDelegationCaps,
  mode: "root-attached" | "conserved" = "conserved",
  resolvedTarget?: AgentPolicy,
) => {
  // A root attached declaration has its own explicit pool. Child overrides retain
  // that established behavior; background and inherited subtree launches also obey
  // the source's own ceilings, before their shared reservation checks the residual.
  const parentCeiling = mode === "conserved" ? parent : undefined;

  const inherited =
    resolvedTarget ??
    AgentPolicy.resolve(delegation.target.policyOverrides ?? delegation.target.policy, parent);

  const policy =
    delegation.policy ??
    SubagentPolicy.make({
      maxChildren: parent.maxToolCalls,
      maxConcurrency: parent.toolConcurrency,
      maxTurns: Math.min(inherited.maxTurns, parent.maxTurns),
      maxToolCalls: Math.min(inherited.maxToolCalls, parent.maxToolCalls),
      maxDuration: Duration.min(inherited.maxDuration, parent.maxDuration),
      ...(parent.tokenBudget === undefined
        ? {}
        : { maxInputTokens: parent.tokenBudget, maxOutputTokens: parent.tokenBudget }),
      ...(parent.costBudgetMicrousd === undefined
        ? {}
        : { maxCostMicrousd: parent.costBudgetMicrousd }),
      maxResultBytes: parent.toolResultBounds.maxBytes,
    });

  const tokenCeiling =
    policy.maxInputTokens === undefined || policy.maxOutputTokens === undefined
      ? Math.min(inherited.tokenBudget ?? Infinity, parentCeiling?.tokenBudget ?? Infinity)
      : Math.min(
          inherited.tokenBudget ?? Infinity,
          parentCeiling?.tokenBudget ?? Infinity,
          policy.maxInputTokens + policy.maxOutputTokens,
        );

  const childPolicy = AgentPolicy.make({
    ...inherited,
    maxTurns: Math.min(inherited.maxTurns, policy.maxTurns, parentCeiling?.maxTurns ?? Infinity),
    maxToolCalls: Math.min(
      inherited.maxToolCalls,
      policy.maxToolCalls,
      parentCeiling?.maxToolCalls ?? Infinity,
    ),
    maxDuration: Duration.min(
      Duration.min(inherited.maxDuration, policy.maxDuration),
      parentCeiling?.maxDuration ?? Duration.infinity,
    ),
    toolConcurrency: Math.min(
      inherited.toolConcurrency,
      parentCeiling?.toolConcurrency ?? Infinity,
    ),
    ...(parentCeiling === undefined
      ? {}
      : {
          toolResultBounds: {
            maxBytes: Math.min(
              inherited.toolResultBounds.maxBytes,
              parentCeiling.toolResultBounds.maxBytes,
              Math.max(256, policy.maxResultBytes ?? Infinity),
            ),
          },
        }),
    ...(tokenCeiling === Infinity
      ? {}
      : {
          tokenBudget: tokenCeiling,
          completionReserveTokens: Math.min(inherited.completionReserveTokens, tokenCeiling),
        }),
    ...(Math.min(
      inherited.costBudgetMicrousd ?? Infinity,
      parentCeiling?.costBudgetMicrousd ?? Infinity,
      policy.maxCostMicrousd ?? Infinity,
    ) === Infinity
      ? {}
      : {
          costBudgetMicrousd: Math.min(
            inherited.costBudgetMicrousd ?? Infinity,
            parentCeiling?.costBudgetMicrousd ?? Infinity,
            policy.maxCostMicrousd ?? Infinity,
          ),
        }),
  });

  const caps =
    parentCaps ??
    (delegation.policy === undefined
      ? SubagentDelegationCaps.make({
          maxTotalChildInvocations: parent.maxToolCalls,
          maxConcurrentChildren: parent.toolConcurrency,
          maxTurns: parent.maxTurns,
          maxToolCalls: parent.maxToolCalls,
          maxDurationMillis: Math.ceil(Duration.toMillis(parent.maxDuration)),
          ...(parent.tokenBudget === undefined
            ? {}
            : { maxInputTokens: parent.tokenBudget, maxOutputTokens: parent.tokenBudget }),
          ...(parent.costBudgetMicrousd === undefined
            ? {}
            : { maxCostMicrousd: parent.costBudgetMicrousd }),
        })
      : delegationCapsFromPolicy(policy));

  return { policy, childPolicy, allocation: delegationAllocationFromPolicy(policy), caps };
};

export const resolveToolCallAllowance = <Parameters>(
  option:
    | {
        readonly default: number;
        readonly fromParameters?: (parameters: Parameters) => number | undefined;
      }
    | undefined,
  parameters: Parameters,
  policy: SubagentPolicy,
  childPolicy: AgentPolicy,
): number => {
  if (option === undefined) return childPolicy.maxToolCalls;
  const extracted = option.fromParameters?.(parameters);

  // A non-finite parameter falls back to the author default, then the reservation.
  const requested =
    extracted !== undefined && Number.isFinite(extracted)
      ? extracted
      : Number.isFinite(option.default)
        ? option.default
        : policy.maxToolCalls;

  return Math.min(
    Math.max(1, Math.floor(requested)),
    policy.maxToolCalls,
    childPolicy.maxToolCalls,
  );
};

/** Conservative subtree remainder after reserving this Run's own full execution ceiling. */
export const residualSubagentCaps = (
  declared: SubagentDelegationCaps,
  parent: AgentPolicy,
  inherited: SubagentBudgetReservation,
  independentRun = false,
): SubagentDelegationCaps => {
  const allocation = inherited.allocation;

  const remaining = (allocated: number, own: number, cap: number | undefined) =>
    Math.min(cap ?? Infinity, Math.max(0, allocated - own));

  return SubagentDelegationCaps.make({
    maxTotalChildInvocations: Math.min(
      declared.maxTotalChildInvocations ?? Infinity,
      inherited.descendantInvocations ?? 0,
    ),
    maxConcurrentChildren: Math.min(
      declared.maxConcurrentChildren ?? Infinity,
      parent.toolConcurrency,
      inherited.descendantInvocations ?? 0,
    ),
    maxTurns: remaining(allocation.turns, parent.maxTurns, declared.maxTurns),
    maxToolCalls: remaining(allocation.toolCalls, parent.maxToolCalls, declared.maxToolCalls),
    maxDurationMillis: remaining(
      allocation.durationMillis,
      Math.ceil(Duration.toMillis(parent.maxDuration)),
      declared.maxDurationMillis,
    ),
    ...(independentRun &&
    inherited.caps.maxInputTokens === undefined &&
    allocation.inputTokens === 0 &&
    parent.tokenBudget === undefined
      ? declared.maxInputTokens === undefined
        ? {}
        : { maxInputTokens: declared.maxInputTokens }
      : {
          maxInputTokens: remaining(
            allocation.inputTokens,
            parent.tokenBudget ?? allocation.inputTokens,
            declared.maxInputTokens,
          ),
        }),
    ...(independentRun &&
    inherited.caps.maxOutputTokens === undefined &&
    allocation.outputTokens === 0 &&
    parent.tokenBudget === undefined
      ? declared.maxOutputTokens === undefined
        ? {}
        : { maxOutputTokens: declared.maxOutputTokens }
      : {
          maxOutputTokens: remaining(
            allocation.outputTokens,
            parent.tokenBudget ?? allocation.outputTokens,
            declared.maxOutputTokens,
          ),
        }),
    ...(independentRun &&
    inherited.caps.maxCostMicrousd === undefined &&
    allocation.costMicrousd === 0 &&
    parent.costBudgetMicrousd === undefined
      ? declared.maxCostMicrousd === undefined
        ? {}
        : { maxCostMicrousd: declared.maxCostMicrousd }
      : {
          maxCostMicrousd: remaining(
            allocation.costMicrousd,
            parent.costBudgetMicrousd ?? allocation.costMicrousd,
            declared.maxCostMicrousd,
          ),
        }),
    maxResultBytes: remaining(
      allocation.resultBytes,
      parent.toolResultBounds.maxBytes,
      declared.maxResultBytes,
    ),
  });
};

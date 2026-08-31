---
title: Budgets & bounded autonomy
description: Set run limits and choose what happens when they are reached.
---

# Budgets & bounded autonomy

Every agent has finite turn, tool call, and duration bounds. Policy also decides whether an
exhausted run returns one constrained final answer or fails.

Normal work stops at a turn or tool call limit. Final-answer policy may allow one constrained
grace turn. Token and cost usage are checked at turn boundaries, so measured usage may cross the
threshold. `maxDuration` is a deadline and can interrupt in-flight work.

## The stop policy

The agent definition fixes its `AgentPolicy`:

```ts
AgentPolicy.make({
  maxTurns: 12,
  maxToolCalls: 24,
  maxDuration: "5 minutes",
  toolConcurrency: 4,
  repeatedFailureLimit: 3,
  tokenBudget: 80_000,
  completionReserveTokens: 8_000,
  costBudgetMicrousd: 2_000_000,
  onExhaustion: "final-answer",
});
```

| Bound                     | What it limits                                 | Result at the limit              |
| ------------------------- | ---------------------------------------------- | -------------------------------- |
| `maxTurns`                | model requests per run                         | follows `onExhaustion`           |
| `maxToolCalls`            | declared and programmatic tool calls           | follows `onExhaustion`           |
| `maxDuration`             | logical run time, including durable suspension | typed failure                    |
| `tokenBudget`             | cumulative input and output tokens             | follows `onExhaustion` once      |
| `completionReserveTokens` | capacity held for finalization                 | ends research before reserve use |
| `costBudgetMicrousd`      | estimated run cost                             | typed failure                    |
| `repeatedFailureLimit`    | consecutive terminal tool failures             | typed failure                    |
| `toolConcurrency`         | parallel tool handlers per batch               | limits concurrency               |

`AgentPolicyError.limit` names a failed bound. DN and DC persist the same value in
`SubmissionSettled.policyLimit`.

## Exhaustion: final answer or failure

`onExhaustion: "final-answer"` is the default for turns, tool calls, and tokens.

1. An over-budget tool batch executes no handlers. Each call becomes a synthetic failed result
   that asks the model to answer from existing evidence. DN and DC do not durably declare the
   rejected batch.
2. Later model requests forbid tools through `toolChoice: "none"`. If the definition owns a
   completion tool, only its singleton call remains allowed.
3. Turn exhaustion permits one grace turn. A second grace turn is impossible.
4. Completion records `finishReason: "budget-exhausted"` and an `exhausted` value of `tokens`,
   `tool-calls`, or `turns`.
5. Any disallowed tool call fails with `ModelProtocolError` before execution.

Synthetic rejections do not count toward `repeatedFailureLimit` because no handler ran.

`onExhaustion: "fail"` rejects work past the bound. The final permitted turn may still finish
through the definition's singleton completion tool. Use this mode when a partial answer is invalid.

Duration, cost, and repeated tool failure always fail. Token exhaustion gets at most one
constrained final answer.

## Per-run allowances

A caller may tighten turn and tool call limits for one run:

```ts
AgentRuntime.run(agent, input, { toolCallAllowance: 8, turnAllowance: 4 });
```

The effective limit is `min(policy bound, max(1, floor(allowance)))`. The runtime ignores
non-finite allowances. A run allowance never raises the definition's ceiling.

## Hierarchical usage budgets

`@effect-agent/capabilities` can apply shared budgets in this order:
`global → tenant → agent → thread → run`.

```ts
Effect.gen(function* () {
  const root = yield* makeUsageBudgetRoot(
    UsageBudgetNodeConfig.make({
      level: "tenant",
      id: "acme",
      limits: UsageBudgetLimits.make({ maxCostMicrousd: 50_000_000 }),
    }),
  );
  const runNode = yield* root.child(
    UsageBudgetNodeConfig.make({ level: "run", id: runId, limits: runLimits }),
  );
  return yield* AgentRuntime.run(agent, input, { budget: toRunBudgetHook(runNode) });
});
```

Consumption is atomic across the ancestor chain. A rejected increment changes no node and fails
as `BudgetExceeded` with the scope and bound. The run budget hook checks the model stream and tool
batches. It accounts for observed usage after the fact; it does not reserve a provider's maximum
charge before I/O.

## Delegation budgets

A requested child tool call allowance is bounded independently by the reservation and definition:

```text
effective allowance = min(normalized request, reserved tool calls, definition maxToolCalls)
```

`SubagentPolicy` limits child count, concurrency, turns, tool calls, duration, tokens, cost, and
result bytes. The parent reserves the slice before starting the child and releases it once after
settlement. An overrun remains charged and reduces later headroom. The child's stop policy still
controls its run and final-answer behavior.

### Request a larger child budget {#containment-and-the-extension-flow}

`failureMode: "return"` converts expected child failures into model-visible result data. Engine
suspension and durability failures stay in the error channel.

`toolCallAllowance: { default, fromParameters }` grants one child invocation a tool call allowance
under its reservation and definition ceilings. This applies to ephemeral and durable children.
To extend work:

1. the child reaches its allowance and returns a partial result with `budgetExhausted`;
2. the parent starts a new delegation with a larger author-owned parameter;
3. the runtime clamps the new allowance, and the child continues from forwarded findings.

An extension creates a new child thread. It cannot enlarge a live reservation. Durable
establishment records the effective allowance before the child becomes runnable. Each replacement
attempt restores that allowance and the committed tool-call usage, including after approval
suspension or ownership loss. Recovery never grants a fresh budget. The child's settlement carries
the exhaustion marker for the parent's result projection.

## Programmatic calls and Code Mode

Code Mode checks and reserves `maxToolCalls` before each inner handler. Exhaustion becomes that
call's outcome. The turn boundary then enforces the combined declared and programmatic count.

## Sizing guidance

- Raise `maxTurns` with `maxToolCalls` for research agents. Repeated read cycles often use one of
  each.
- A token quota cannot guarantee a dollar ceiling. Strict spending admission must reserve the
  maximum request charge before provider I/O, including finalization and cache misses.
- Split independent work into bounded child runs. Grant a larger follow-up only when the partial
  result warrants it.

## Related guides {#where-the-rules-live}

- [Agent definitions guide](/guide/agents) covers policy declarations.
- [Run and stream guide](/guide/run-agents) covers finish reasons and Run options.
- [Persistence & durability](/concepts/durability#attached-subagents) covers child recovery.

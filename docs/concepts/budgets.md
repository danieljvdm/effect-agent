---
title: Budgets & bounded autonomy
description: Stop Policy limits, exhaustion behavior, per-Run allowances, usage budgets, and delegation reservations.
---

# Budgets & bounded autonomy

Every Agent runs under finite, explicit bounds. Unlimited execution is an expert opt-in. When a
Run reaches a limit after ten useful Tool Calls, throwing away its work is often the wrong result.
The budget model therefore separates the limit from what happens next.

- **The bound.** Countable work such as Turns and Tool Calls never starts if it would exceed a
  limit. For measured usage such as tokens, cost, and wall-clock time, the Run stops at the first
  Turn boundary after the overage. Bounds are not negotiable.
- **The resolution.** Policy decides whether the Run gets a constrained final answer or fails.

## The stop policy

`AgentPolicy` declares each Agent's bounds. The Definition fixes them at authoring time, and the
engine reads them at every Turn boundary.

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

| Bound                     | What it limits                                       | On exhaustion                     |
| ------------------------- | ---------------------------------------------------- | --------------------------------- |
| `maxTurns`                | model requests per Run                               | `onExhaustion` resolves           |
| `maxToolCalls`            | declared Tool Calls per Run (programmatic included)  | `onExhaustion` resolves           |
| `maxDuration`             | logical-Run wall clock, including durable suspension | always fails typed                |
| `tokenBudget`             | input + output tokens per Run                        | `onExhaustion` resolves (RUN-025) |
| `completionReserveTokens` | capacity withheld from research for delivery         | enters finalization before spend  |
| `costBudgetMicrousd`      | estimated cost per Run (needs a cost estimator)      | always fails typed                |
| `repeatedFailureLimit`    | consecutive terminal Tool failures                   | always fails typed                |
| `toolConcurrency`         | parallel Tool Handlers per batch                     | concurrency gate                  |

A typed exhaustion failure is `AgentPolicyError` with a `limit` literal naming the exhausted limit.
In the DN and DC assemblies a Run failed this way settles with that literal preserved as the
canonical settlement's `policyLimit` under RUN-011. Consumers read the typed dimension instead
of parsing the failure message.

## Exhaustion: final answer or failure

`onExhaustion` selects the resolution for Turn and Tool Call limits under RUN-018 and RUN-019. It
also applies once to the token budget under RUN-025.

**`"final-answer"` (the default).** The Run gets one constrained opportunity to deliver:

1. An over-budget declared Tool batch **never executes**. Every call settles as a synthetic
   failed result telling the model the budget is exhausted and to answer from what it already
   has. No handler runs; in the DN and DC assemblies the batch is never durably declared.
2. Every subsequent model request forbids tool use (Effect AI `toolChoice: "none"`) unless the
   Definition owns a completion Tool, in which case only that Tool remains available.
3. Turn exhaustion admits exactly **one grace Turn** past `maxTurns`, under the same constraint.
   A second grace is structurally impossible.
4. The Run settles _completed_ with `finishReason: "budget-exhausted"`, never a plain
   `"model-stop"`. RUN-011 does not allow budget exhaustion to look like ordinary success. In the
   DN and DC assemblies, the canonical `SubmissionSettled` record carries the same marker plus the
   typed `exhausted` dimension (`tokens`, `tool-calls`, or `turns`), so a rebuilt projection
   distinguishes a truncated answer from an ordinary one and names the exhausted limit without
   the live event stream or message parsing.
5. A model that declares a Tool Call under the constraint fails the Run typed
   with `ModelProtocolError` under RUN-020, except for the singleton Definition-owned completion
   Tool in RUN-032. That Tool delivers and settles immediately without another summary turn.

Synthetic rejections are exempt from `repeatedFailureLimit` folding: no handler ran, so a
rejected four-call batch cannot trip a limit of three.

**`"fail"`.** Exhaustion fails the Run typed before any declared application Handler starts,
including a completion Tool. Choose fail mode for pipelines that must never accept a truncated
answer or start a delivery side effect after a policy breach. The repository's own PR reviewer
pins it for review _children_, because every configured discovery and verification pass must
settle exactly; a partial result would launder exhaustion into settled review assurance. Its
separate input-coverage claim is host-derived and does not imply defect recall.

Duration, cost, and repeated-failure limits always fail the Run. Token exhaustion allows only one
constrained final answer under RUN-025, so it cannot loop or spend without a bound.

## Per-Run allowances

A caller can tighten the countable bounds per Run under RUN-021. It cannot widen them.

```ts
AgentRuntime.run(agent, input, { toolCallAllowance: 8, turnAllowance: 4 });
```

The effective limit is `min(policy bound, max(1, floor(allowance)))`, and the `onExhaustion`
resolution uses the effective limits. The runtime ignores non-finite allowances, leaving the
Definition policy in force; a `NaN` must not poison comparisons and erase the bound. Allowances are
the mechanism behind delegation budget extensions below. The Definition's policy remains the
ceiling.

## Hierarchical usage budgets

The Stop Policy is per-Agent. Some limits span Agents, such as a tenant's monthly spend or a
Conversation's token pool. `@effect-agent/capabilities` provides this ordered budget hierarchy:
`global → tenant → agent → conversation → run`.

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

Every consumption is atomic across the full ancestor chain. A rejected increment commits nothing
at any level and fails typed as `BudgetExceeded`, naming the scope and exceeded limit. Wired
into a Run through the `budget` hook, these are hard limits. The engine guards the model
stream and every Tool batch, and a hook failure is fatal at the Turn boundary. This layer accounts
_after the fact_; it is deliberately not a reservation service.

## Delegation budgets

A delegated child answers to three ceilings at once (spec/subagents.md §7):

```text
model-granted allowance ≤ delegation reservation slice ≤ child Definition policy
```

- **`SubagentPolicy`.** The Delegation Definition declares parent-side limits for total child
  invocations, concurrency, and the per-invocation slice of turns, tool calls, duration, tokens,
  cost, and result bytes.
- **Reservations.** The parent allocates all reserved dimensions before a child starts and
  releases them exactly once after it settles. A child that exceeds its slice is recorded and
  charged rather than clipped. The overrun reduces headroom for future reservations.
- **The child's Stop Policy.** The engine enforces it inside the child Run. This includes the
  child's final-answer behavior, so a scout that exhausts its slice returns a partial report
  instead of failing.

### Containment and the extension flow

Two delegation options control this behavior:

**`failureMode: "return"`.** SUB-033 converts each expected child failure, including the declared
failure and the framework failure family, into model-visible result data. One failed scout does
not fail the parent. The engine-owned suspension signal and durability error always stay in the
error channel, so durable children still suspend correctly.

**`toolCallAllowance: { default, fromParameters }`.** Under SUB-034, each child runs with a
per-invocation allowance below its Definition ceiling. The orchestrator may grant more in a new
invocation.

1. The scout runs at the default allowance and exhausts it. Its soft landing returns a partial;
   `projectResult` receives the `budgetExhausted` marker for the result Schema.
2. The orchestrator observes the truncated partial and re-invokes the delegation with a raised
   allowance through an author-owned parameter field. The runtime clamps it to the reservation
   slice and rejects invalid values.
3. The new child continues from the forwarded findings. A budget extension always creates a new
   delegation. It never adds to a reservation in flight or reuses the child Conversation.

The granted allowance reaches ephemeral children; in the DN and DC assemblies a child lane owns
no per-Run options channel yet, so a durable child runs at its Definition policy (the exhausted
marker still travels durably via the child Settlement).

## Programmatic calls and Code Mode

Code Mode's generated programs consume the same Run budgets mid-pass (RUN-017): every inner Tool
call checks and reserves against `maxToolCalls` before its handler runs, exhaustion becomes that
_call's_ outcome rather than a Run failure, and the Turn boundary re-enforces the combined
declared-plus-programmatic count.

## Sizing guidance

- **Research-shaped agents bind on turns first.** At a grep-then-read cadence each observation is
  one Tool Call and each batch one Turn, so `maxTurns` usually binds before `maxToolCalls`; raise
  them together, and let the soft landing convert the residual tail into a partial cited answer
  instead of provisioning for the worst case.
- **Treat the token budget as the spend ceiling.** RUN-025 permits one constrained final answer.
  Set `tokenBudget` high enough to cover that call, but keep it finite.
- **Delegate instead of raising.** If independent work needs 100 Tool Calls, five scouts with 20
  calls each may fit better. Give each invocation its own allowance and grant extensions only
  when the partial result justifies them.

## Where the rules live

- [Agent definitions guide](/guide/agents) explains policy declarations.
- [Run and stream guide](/guide/run-agents) covers finish reasons and Run options.
- [Runtime specification](/spec/runtime) defines RUN-011 and RUN-016 through RUN-021.
- [Subagent specification](/spec/subagents) defines hierarchical budgets in section 7 and
  SUB-033/034.

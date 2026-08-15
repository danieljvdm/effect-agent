---
title: Budgets & bounded autonomy
description: Every bound an Agent runs under — Stop Policy limits, exhaustion soft landing, per-Run allowances, hierarchical usage budgets, and delegation reservations.
---

# Budgets & bounded autonomy

<StatusCallout status="available" phase="P2 + budget arc" title="Finite bounds, the final-answer exhaustion resolution, per-Run allowances, hierarchical usage budgets, and delegation reservations are all implemented and tested." />

Every Agent runs under finite, explicit bounds. Unlimited execution is an expert opt-in, never a
default. But _bounded_ does not mean _brittle_: a bound the model cannot see is a cliff, and an
agent that falls off a cliff after ten productive tool calls delivers nothing. The budget model
therefore separates two ideas that are often conflated:

- **The bound** — countable work (Turns, Tool Calls) that would exceed a limit never starts, and
  measured usage (tokens, cost, wall clock) stops the Run at the seam where the overage is
  observed. Bounds are never negotiable.
- **The resolution** — what happens to the Run once a bound binds. This is policy.

## The Stop Policy

`AgentPolicy` declares each Agent's bounds. They are part of the Definition — fixed at authoring
time, read by the engine at every Turn seam.

```ts
AgentPolicy.make({
  maxTurns: 12,
  maxToolCalls: 24,
  maxDuration: "5 minutes",
  toolConcurrency: 4,
  repeatedFailureLimit: 3,
  tokenBudget: 80_000,
  costBudgetMicrousd: 2_000_000,
  onExhaustion: "final-answer",
});
```

| Bound                  | What it limits                                      | On exhaustion           |
| ---------------------- | --------------------------------------------------- | ----------------------- |
| `maxTurns`             | model requests per Run                              | `onExhaustion` resolves |
| `maxToolCalls`         | declared Tool Calls per Run (programmatic included) | `onExhaustion` resolves |
| `maxDuration`          | wall clock per Run                                  | always fails typed      |
| `tokenBudget`          | input + output tokens per Run                       | always fails typed      |
| `costBudgetMicrousd`   | estimated cost per Run (needs a cost estimator)     | always fails typed      |
| `repeatedFailureLimit` | consecutive terminal Tool failures                  | always fails typed      |
| `toolConcurrency`      | parallel Tool Handlers per batch                    | not a failure — a gate  |

A typed exhaustion failure is `AgentPolicyError` with a `limit` literal naming which bound bound.

## Exhaustion: soft landing or fail

`onExhaustion` selects the resolution for the two _countable work_ bounds — Turns and Tool Calls
(runtime spec RUN-018/RUN-019):

**`"final-answer"` (the default).** The Run gets one constrained opportunity to deliver:

1. An over-budget declared Tool batch **never executes** — every call settles as a synthetic
   failed result telling the model the budget is exhausted and to answer from what it already
   has. No handler runs; in the DN and DC assemblies the batch is never durably declared.
2. Every subsequent model request forbids tool use (Effect AI `toolChoice: "none"`).
3. Turn exhaustion admits exactly **one grace Turn** past `maxTurns`, under the same constraint.
   A second grace is structurally impossible.
4. The Run settles _completed_ — with the honest `finishReason: "budget-exhausted"`, never a
   plain `"model-stop"` (RUN-011: budget exhaustion cannot masquerade as success). In the DN and
   DC assemblies the canonical `SubmissionSettled` record carries the same marker, so a rebuilt
   projection can distinguish an exhaustion-truncated answer from an ordinary one without the
   live event stream.
5. A model that declares a Tool Call under the constraint fails the Run typed
   (`ModelProtocolError`, RUN-020) — fail-closed, no rejection loops.

Synthetic rejections are exempt from `repeatedFailureLimit` folding: no handler ran, so a
rejected four-call batch cannot trip a limit of three.

**`"fail"`.** The pre-arc behavior, byte for byte: exhaustion fails the Run typed before the
exceeding work starts. Choose it for pipelines that must never accept a truncated answer — the
repository's own PR reviewer pins it for review _children_, because a review is a coverage claim
and a partial produced without reading the diff would launder exhaustion into "reviewed".

Duration, token, cost, and repeated-failure bounds are hard rails regardless: a soft landing can
never loop or spend unboundedly, because the rails that measure spending stay fatal.

## Per-Run allowances

A caller can tighten — never widen — the countable bounds per Run (RUN-021):

```ts
AgentRuntime.run(agent, input, { toolCallAllowance: 8, turnAllowance: 4 });
```

The effective limit is `min(policy bound, max(1, floor(allowance)))`, and the `onExhaustion`
resolution keys off the effective limits. Non-finite allowances are ignored fail-closed — a `NaN`
would otherwise poison every comparison and silently erase the bound. Allowances are the
mechanism behind delegation budget extensions (below): the Definition's policy is the ceiling an
allowance moves _within_.

## Hierarchical usage budgets

The Stop Policy is per-Agent. For limits that span Agents — a tenant's monthly spend, a
Conversation's token pool — `@effect-agent/capabilities` provides a strictly ordered budget
hierarchy: `global → tenant → agent → conversation → run`.

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

Every consumption is atomic across the full ancestor chain: a rejected increment commits nothing
at any level and fails typed as `BudgetExceeded` naming the scope and limit that bound. Wired
into a Run through the `budget` hook, these are **hard rails** — the engine guards the model
stream and every Tool batch, and a hook failure is fatal at the Turn seam. This layer accounts
_after the fact_; it is deliberately not a reservation service.

## Delegation budgets

A delegated child answers to three ceilings at once (spec/subagents.md §7):

```text
model-granted allowance ≤ delegation reservation slice ≤ child Definition policy
```

- **`SubagentPolicy`** on the Delegation Definition declares the parent-side bounds: total child
  invocations, concurrency, and the per-invocation slice of turns, tool calls, duration, tokens,
  cost, and result bytes.
- **Reservations** conserve those dimensions across a parent Run: all-or-nothing allocation
  before a child starts, exactly-once release after it settles, and honest _overrun_ accounting —
  a child that consumes past its slice is recorded and charged, never clipped, and the overrun
  reduces headroom for future reservations.
- **The child's own Stop Policy** is what the engine actually enforces inside the child Run —
  including its own soft landing, so a scout that exhausts its slice returns a partial report
  instead of failing.

### Containment and the extension flow

Two delegation options close the loop:

**`failureMode: "return"`** (SUB-033) contains every _expected_ child failure — the declared
failure plus the framework family — as model-visible result data instead of a parent-fatal
error: one dead scout cannot detonate a fan-out. The engine-owned suspension signal and
durability error always stay in the error channel, so durable children still suspend correctly.

**`toolCallAllowance: { default, fromParameters }`** (SUB-034) runs each child under a
per-invocation allowance below its Definition ceiling, and lets the orchestrator _grant more_:

1. The scout runs at the default allowance and exhausts it. Its soft landing returns a partial;
   `projectResult` receives the honest `budgetExhausted` marker to surface in the result Schema.
2. The orchestrator observes the truncated partial and re-invokes the delegation with a raised
   allowance through an author-owned parameter field — clamped fail-closed to the reservation
   slice.
3. The new child continues from the forwarded findings. A budget extension is always a **fresh
   re-delegation** — never a mid-flight reservation top-up (conservation stays intact) and never
   child-Conversation reuse.

The granted allowance reaches ephemeral children; in the DN and DC assemblies a child lane owns
no per-Run options channel yet, so a durable child runs at its Definition policy (the exhausted
marker still travels durably via the child Settlement).

## Programmatic calls and Code Mode

Code Mode's generated programs consume the same Run budgets mid-pass (RUN-017): every inner Tool
call checks and reserves against `maxToolCalls` before its handler runs, exhaustion becomes that
_call's_ outcome rather than a Run failure, and the Turn seam re-enforces the combined
declared-plus-programmatic count.

## Sizing guidance

- **Research-shaped agents bind on turns first.** At a grep-then-read cadence each observation is
  one Tool Call and each batch one Turn, so `maxTurns` usually binds before `maxToolCalls`; raise
  them together, and let the soft landing convert the residual tail into a partial cited answer
  instead of provisioning for the worst case.
- **The token budget is the honest spend ceiling.** It stays fatal by design — one more turn
  costs tokens the Run does not have — so pair a generous soft-landing surface with a firm
  `tokenBudget`.
- **Delegate instead of raising.** A single window that needs 100 tool calls is usually a fan-out
  of five scouts needing 20 — with per-invocation allowances, containment, and extension grants
  where depth is actually warranted.

## Where the rules live

- [Agent definitions guide](/guide/agents) — declaring a policy.
- [Run & stream guide](/guide/run-agents) — finish reasons and Run options.
- [Runtime specification](/spec/runtime) — RUN-011, RUN-016–021 normative text.
- [Subagent specification](/spec/subagents) — §7 hierarchical budgets, SUB-033/034.

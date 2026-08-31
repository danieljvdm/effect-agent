---
title: Subagents
description: Delegate bounded work to child agents with explicit input, results, authority, and recovery.
---

# Subagents

A subagent runs a task for a parent agent in a fresh conversation. It has its own model binding,
instructions, toolkit, and policy. The parent calls it through a declared delegation tool and
receives the projected result when the child finishes.

Use a child when a task needs a specialist model or toolkit, or when intermediate research would
crowd the parent's context. The child repeats the same [runtime loop](../concepts/runtime-model).
Its raw transcript stays private unless the result projection exposes it.

## Define a delegation

`Subagent.define` from `@effect-agent/capabilities` exposes an ordinary Agent Definition as an
Effect AI tool:

1. Supply the child definition as `target`, Schemas for delegation parameters, success, and failure,
   and a finite `SubagentPolicy`.
2. Use `prepareInput` to turn the tool parameters into the child's input. Use `projectResult` to
   turn the child's output into the parent's tool result. Both return Effects with the declared
   failure type.
3. Add the returned `delegation.tool` to the parent's toolkit.
4. Provide `SubagentRuntime.layer(delegation, childBinding, { mapChildFailure })` with an explicit
   `Agent.withModel` binding. Map every expected child failure to the declared failure Schema.

The [Travel Planner delegation example](https://github.com/danieljvdm/effect-agent/blob/main/packages/testing/src/fixtures/travel-planner/subagents.ts)
contains a destination researcher, its delegation, model bindings, handler Layers, and parent agent.

## Choose what crosses the boundary

The delegation's parameters describe what the parent model may request. The child's input Schema
describes what the child needs to run. These can differ: `prepareInput` performs that translation
and selects the context the parent shares.
It receives decoded parameters and bounded parent metadata, not the parent's transcript.

`projectResult` controls the information returned to the parent. A research child can return a
short report and source references while keeping its intermediate tool output out of the parent's
prompt. Projected results must satisfy the delegation's success Schema and result-size limit.

## Bound child work

The parent reserves an allowance before starting each child. `SubagentPolicy` limits child count,
concurrency, turns, tool calls, duration, tokens, cost, and result size. Delegation uses the parent's
bounded tool scheduler. The child's own stop policy also applies.

An optional `toolCallAllowance` supplies a default and a `fromParameters` function for the requested
allowance. The effective allowance cannot exceed either the delegation's reserved tool calls or the
child definition's `maxToolCalls`.

When a child reaches its allowance, its stop policy determines whether it fails or returns a
constrained final answer. Expose `projectResult`'s `budgetExhausted` flag when the parent needs to
recognize partial findings. The parent can then delegate again with a larger requested allowance
and forward those findings. This creates a new child conversation; it does not enlarge a live
reservation.

Reservations release once after settlement. An overrun remains charged and reduces later headroom.
Durable recovery restores the effective allowance and committed usage, so a replacement attempt
receives no fresh budget. See [delegation budgets](../concepts/budgets#delegation-budgets) for the
extension flow and exact accounting rules.

## Handle failures

The default `failureMode: "error"` fails the parent tool batch on an expected child failure.
Use `"return"` to let the parent model receive and act on that failure as result data.
Suspension and durability failures remain in the error channel.

A partial successful answer and a failed child are different outcomes. Preserve that distinction
in the result projection so the parent can decide whether to use the findings, request more work,
or stop.

## Limit authority

The default grant permits the target's declared tools at depth one. A grant that excludes a target
tool is rejected; it does not remove that tool from the child. Define a narrower child toolkit when
the task needs less authority.

`needsApproval` approves establishing this child only. It never authorizes the child's actions,
siblings, or retries. Apply [tool and resource policy](./operations#tools-delegation-and-external-resources)
to each action.

## Keep children attached

The parent joins the child's terminal outcome before settling the delegation tool call. Ephemeral
children share the parent's Scope. Durable children run in separate conversations and attempts,
with stable parent linkage. A parent waiting for a durable child releases its worker permit.

For durable execution, provide fixed child definition digests in the handler Layer's `durable`
option and matching host registrations. Recovery rejoins the established child. If admission
cannot confirm whether that child was accepted, the parent keeps waiting and never starts a
replacement child.

A durable parent abort records child abort intent and joins the child's outcome before settling
the parent. Unknown external effects remain operator obligations. See
[child recovery and abort](../concepts/durability#attached-subagents) for settlement verification,
budget release, and deadline behavior.

Nested delegation, handoff, and detached children are currently unsupported.

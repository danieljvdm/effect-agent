---
title: Architecture
description: Understand Effect Agent's execution model, resource boundaries, budgets, and durable recovery.
---

# Architecture

Understand how Effect Agent runs work, bounds its resources, and recovers recorded progress.
These pages explain the contracts behind the [implementation guides](../guide/).

## Effect foundations

[Built on Effect](./effect-native) explains typed errors, Schema-defined data, dependency
Layers, and scoped resource ownership.

## Agent execution

[The runtime model](./runtime-model) follows a run through context preparation, model calls,
tool batches, subagents, and input delivered between turns.

## Budgets and limits

[Budgets and bounded autonomy](./budgets) explains execution ceilings, shared delegation
allowances, token limits, and cost accounting.

## Durability and recovery

[Persistence and durability](./durability) explains accepted work, recorded results,
ownership loss, child recovery, and uncertain external effects.

For host-specific setup, see [Platforms](../platforms/). For package boundaries and
advanced configuration, see [Reference](../reference/).

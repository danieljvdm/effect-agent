---
title: Architecture
description: Understand Effect Agent's execution model, resource boundaries, budgets, and durable recovery.
---

# Architecture

Understand how Effect Agent runs work, bounds its resources, and recovers recorded progress.
These pages explain the contracts behind the [implementation guides](../guide/).

## Agent execution

[The runtime model](./runtime-model) defines the turn, ownership, recovery, and wake rules
shared by runtime, storage, and platform changes.

## Budgets and limits

[Budgets and bounded autonomy](./budgets) explains execution ceilings, shared delegation
allowances, token limits, and cost accounting.

## Durability and recovery

[Persistence and durability](./durability) explains accepted work, recorded results,
ownership loss, child recovery, and uncertain external effects.

For host-specific setup, see [Platforms](../platforms/). For package boundaries and
advanced configuration, see [Reference](../reference/).

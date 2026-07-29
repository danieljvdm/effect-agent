# ADR-0004: Represent uncertain external effects instead of blindly replaying them

- Status: Accepted
- Date: 2026-07-28
- Decision owners: Project owner
- Related decisions: D-008, D-009, D-010

## Context

A worker can crash after an external tool performs a side effect but before the
runtime records its result. On recovery, storage alone cannot tell whether the tool
never started, partially ran, or completed. Retrying may duplicate a charge,
message, deployment, deletion, or other irreversible action. Declaring
exactly-once execution does not remove this ambiguity.

## Decision

Ordinary tools are at-least-once and may become `UnknownToolOutcome`.

Before invocation, the engine records a prepared call. After validated completion,
it records the settled result. Recovery of a prepared-but-unsettled call may proceed
only when an explicit policy can prove it did not start, recover its result, or
safely retry it under an external idempotency contract. Otherwise the submission
enters `Unknown` and automatic continuation stops.

Durable steps provide exactly-once recording of one accepted result with
at-least-once body execution. Step authors remain responsible for external
idempotency, reconciliation, or compensation.

## Consequences

Positive:

- the runtime does not silently duplicate irreversible actions;
- uncertainty is observable and auditable;
- applications can add domain-specific reconciliation;
- the product makes an honest, testable guarantee.

Negative:

- some runs need operator intervention;
- applications must provide execution annotations or the Durable Step service;
- the framework needs reconciliation APIs and UI;
- application authors must understand idempotency.

## Tool execution classes

- **Pure/read-only**: retryable subject to resource and consistency policy.
- **Idempotent external**: retryable with a stable external idempotency key.
- **Reconciliable**: query external state before deciding.
- **Compensatable**: may proceed through an explicit saga/workflow.
- **Uncertain ordinary**: stop and surface unknown after ambiguous ownership loss.

Classification is declared through Effect AI Tool annotations or by requiring the
Durable Step service, then policy-checked. The runtime may downgrade trust but never
infer a safer class from a Tool name or model claim.

## Alternatives considered

### Retry every unresolved call

Rejected because it is unsafe for non-idempotent effects.

### Fail every unresolved call and continue

Rejected because a failure result falsely says the external action did not happen.

### Require all tools to be exactly once

Rejected because most external systems cannot provide that property, and
distributed transactions are not generally available.

### Never retry any tool

Rejected because pure and truly idempotent work can recover automatically and
safely.

## Validation

- crash tests kill the worker before invocation, during invocation, and after
  return/before record;
- non-idempotent test tools record whether duplicate effects occur;
- stale workers cannot settle after fencing;
- operator resolution is idempotent and audited;
- model context never receives a fabricated ordinary tool result.

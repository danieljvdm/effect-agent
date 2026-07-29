# ADR-0005: Use bounded parallel Tool execution with deterministic result order

- Status: Accepted
- Date: 2026-07-29
- Decision owners: Project owner
- Related decisions: D-007, D-008, D-012

## Context

The source research and Effect AI behavior both demonstrate parallel Tool batches with request-order
results. The exact observations are recorded in
[the reference analysis](../REFERENCE-ANALYSIS.md). Unbounded execution is not an appropriate
default for a framework promising bounded autonomy.

## Decision

The engine executes Effect AI Toolkit Handler effects under a finite parallelism limit in Agent
policy. It uses Effect structured concurrency and `Semaphore.withPermit` so permits are released
on success, failure, or interruption. Preserve original Tool Call order in the results supplied to
the next model Turn and committed to conversation history.

A Tool or Run may require sequential execution. Later scheduling annotations may
add exclusive resource groups, but the first implementation should keep the policy
small:

- finite maximum concurrency;
- per-run sequential override;
- per-Tool sequential requirement;
- deterministic result order.

## Consequences

Positive:

- independent Tool Calls finish faster;
- Tool definitions, handlers, failures, and requirements remain native Effect AI values;
- deterministic history remains possible.

Negative:

- model-requested calls may not actually be independent;
- side-effecting Tools can race unless marked sequential;
- live completion order differs from committed result order;
- the default bound needs a sensible initial value and load testing.

## Alternatives considered

### Sequential by default

Safer and easier to reason about, but adds latency to common read-heavy Tool batches.

### Unbounded parallel execution

Rejected because model output can request many calls and exhaust connections,
memory, provider limits, or external services.

### Commit results in completion order

Rejected because timing would change canonical conversation history.

## Validation

- varying completion order does not change committed Tool result order;
- the configured concurrency bound is never exceeded;
- sequential Tools never overlap with siblings in their batch;
- failure and interruption settle every started Tool fiber;
- Effect upgrades retain `Semaphore` permit and interruption behavior used by the scheduler.

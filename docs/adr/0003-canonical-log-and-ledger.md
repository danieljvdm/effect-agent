# ADR-0003: Separate the canonical conversation log from the submission ledger

- Status: Accepted
- Date: 2026-07-29
- Decision owners: Project owner
- Related decisions: D-004, D-005, D-006, D-015, D-020

## Context

An agent system has two related but different kinds of truth:

- what happened in the conversation and should be replayed or audited;
- what operational work is accepted, leased, retried, or awaiting settlement.

A single mutable run row is easy to implement but loses history, couples UI
projections to scheduling, and makes crash recovery ambiguous. A single event log
can represent everything, but work claiming, lease scans, and idempotent admission
then require complex projections on the critical path.

## Decision

Use two logical durable structures:

1. an append-only Conversation Log of canonical batches;
2. a mutable-but-audited Submission Ledger of operational obligations.

Admission first writes the durable ledger record. The runtime then materializes the Conversation
and attachments and marks the Submission ready before returning a Receipt. The actual user input
becomes canonical history when the Submission is claimed for execution.

Terminalization reserves one exact settlement record in the ledger, appends that record to
canonical history, then finalizes the ledger row and releases the conversation lane. Canonical
settlement wins if the last step did not finish, and recovery repairs the ledger from history.

The canonical log is authoritative for conversation history. The ledger is
authoritative for outstanding operational obligations. Recovery reads both and
rejects contradictions rather than choosing silently.

## Consequences

Positive:

- audit/replay and scheduling have appropriate data shapes;
- projections are rebuildable;
- accepted work cannot exist only in memory;
- a Receipt has a precise meaning;
- recovery can identify missing settlement;
- storage adapters can optimize work scans without rewriting history.

Negative:

- admission and terminalization require explicit intermediate states and repair;
- invariants span two structures;
- recovery and repair tooling is required;
- adapters with weak transactions may not qualify as durable.

## Key invariants

- no acknowledged submission lacks a ledger record and a materialized, ready
  Conversation;
- canonical input application is idempotent and occurs before the model consumes the
  Submission;
- exactly one terminal settlement exists per accepted submission;
- conversation sequence is monotonic and gap-free for committed records;
- a ledger state cannot authorize rewriting canonical history;
- projections and checkpoints are never sources of canonical truth;
- later FIFO work cannot pass an unsettled head.

## Alternatives considered

### One event log for everything

Potentially elegant, but work claiming and operational scanning would rely on
strong, synchronous projections. It may be revisited if an adapter can prove the
same atomic admission and liveness properties with less complexity.

### One mutable run/session table

Rejected because it cannot provide sufficient audit, replay, or deterministic
recovery evidence.

## Validation

- model-based tests cover every transition;
- transaction failpoints exist around admission and terminalization;
- duplicate and conflicting retries are tested;
- process-kill tests show every accepted submission reaches one settlement;
- projection deletion does not affect recovery.

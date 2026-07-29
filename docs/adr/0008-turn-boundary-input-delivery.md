# ADR-0008: Deliver steering, follow-up, and joined input only at safe Turn seams

- Status: Accepted
- Date: 2026-07-29
- Decision owners: Project owner
- Related decisions: D-006, D-011

## Context

Input may arrive while a model response or Tool batch is active. Mutating an in-flight response,
skipping active Tool Calls, or delivering the same input twice would make the Run nondeterministic
and make durable recovery ambiguous.

Ephemeral and durable execution need the same visible delivery rules even though one uses in-memory
queues and the other uses persistent Submission states.

## Decision

- Steering is delivered at Run start or after a complete assistant response and Tool batch, before
  the next model request.
- Steering never mutates or cancels an in-flight model response and never skips active Tool Calls.
- Follow-up input is delivered only when the Agent would otherwise stop.
- The initial queue drain policy is one item; an explicit policy may drain all available items at
  that seam.
- Ephemeral commands use bounded Effect queues owned by the Run Scope.
- Durable queued input is first claimed as `joining`. It becomes `joined` only after its exact
  canonical input is appended.
- Recovery returns a pre-append `joining` Submission to ready and reattaches a post-append `joined`
  Submission to its host Run.

## Consequences

- each model request observes a complete prior response and Tool batch;
- command timing cannot change the contents of an already active Turn;
- ephemeral and durable modes share one semantic seam;
- durable joining needs explicit claim, append, repair, and settlement transitions;
- follow-up cannot preempt active work.

## Validation

- steering received during streaming is not visible until the next model request;
- steering received during Tool execution waits for the complete batch;
- follow-up is ignored until the Run would otherwise stop;
- interruption closes command queues and all waiting fibers;
- crash tests cover both sides of the `joining` → canonical append → `joined` boundary;
- replay never delivers one queued input twice.

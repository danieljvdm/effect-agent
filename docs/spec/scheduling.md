# Scheduling specification

Status: Draft

Scheduling delivers Schema-encoded Agent input through ordinary durable Submission admission at a specified time. It is a host-facing `@effect-agent/session` capability. It does not run Agents, bypass admission, or provide webhook ingress. Once admission commits, durability.md governs the accepted Submission.

## 1. Contract

A successful registration survives Node restart and Cloudflare eviction. A Schedule owns a due occurrence until it records the existing Receipt or a conclusive refusal. A lost reply retries the same immutable admission request. It never creates a second input for that occurrence.

Scheduling does not promise one Run per firing. Ordinary Conversation FIFO and joining rules decide how accepted input is processed. It makes no exactly-once external-effect claim.

Webhook ingress, arbitrary callbacks or jobs, an event bus, catch-up replay, complete firing history, multi-node Node scheduling, further Cloudflare sharding, and Conversation/Schedule co-location are out of scope.

## 2. Values and defaults

`ScheduleId` is a non-empty string with at most 128 characters. `ScheduleOwner` is the tenant-qualified `{ tenantId, ownerId }` management and listing scope. `ScheduleScope` adds the caller principal. `deliveryPrincipal` is the stable principal authorized for a due occurrence and is distinct from the management caller.

Schedule, tenant, and owner names must contain well-formed Unicode. Schemas reject unpaired UTF-16
surrogates before UTF-8 storage can replace them and collapse distinct identities.

`ScheduleInstant` is a UTC Unix millisecond safe integer between zero and 8,640,000,000,000,000. `DateTime` and `Cron` calculate time but are never stored.

```ts
type ScheduleTimingRequest =
  | { readonly _tag: "At"; readonly atMillis: ScheduleInstant }
  | { readonly _tag: "After"; readonly delayMillis: number }
  | {
      readonly _tag: "Interval";
      readonly everyMillis: number;
      readonly anchorMillis?: ScheduleInstant;
    }
  | { readonly _tag: "Cron"; readonly expression: string; readonly timeZone?: string };

type ScheduleTiming =
  | { readonly _tag: "At"; readonly atMillis: ScheduleInstant }
  | {
      readonly _tag: "Interval";
      readonly everyMillis: number;
      readonly anchorMillis: ScheduleInstant;
    }
  | { readonly _tag: "Cron"; readonly expression: string; readonly timeZone: string };

type ScheduleDestination =
  | { readonly _tag: "ExistingConversation"; readonly conversationId: ConversationId }
  | { readonly _tag: "FreshConversation" };
```

`After` is request-only. Creation or a timing update captures Clock time once and resolves it to `At`; create replay never resolves it again. Intervals are positive and at least the configured minimum. Without an explicit anchor, the anchor is registration time plus one interval. With a past anchor, creation selects the first anchored firing strictly after registration. An update that leaves timing unchanged retains the anchor.

Cron time zone defaults to `UTC` and is always stored explicitly. Five-field input means seconds
zero. Six-field input MUST contain one fixed seconds value. Validation rejects expressions whose
seconds field can vary. A configured minimum above 60 seconds requires UTC or a fixed-offset zone.
For those zones, validation uses conservative cyclic spacing of the allowed minute and hour values
while ignoring day restrictions. That check may reject a sparse valid rule. Named IANA zones remain
supported with the default one-minute minimum. Local spacing cannot prove a larger elapsed-time
minimum across offset changes: a daily rule can have only 23 hours between firings at spring DST.
Validation does not enumerate calendar occurrences to prove frequency.

Cron follows Effect's named-zone behavior and persists UTC intended instants. A nonexistent local
time moves forward across the spring gap. A repeated fall time selects its first occurrence, not
both. Fixed offsets such as `+05:30` are also supported.

The defaults are: 1,000 Schedules per owner, 60-second minimum recurrence, 64 KiB encoded input, 64 due records per driver pass, 8 concurrent admissions, 1-second retry base, 5-minute retry maximum, 30-second admission timeout, and 30-second recovery poll. Hosts configure finite limits through `SchedulingLimits`. The hard bounds are 100,000 Schedules per owner, a recurrence minimum of at least 60 seconds, at most 64 KiB input, at most 1,024 records per pass, and at most 64 concurrent admissions. A limit violation is typed and occurs before durable mutation. Paused, completed, and cancelled records still count toward the owner quota because they retain creation-idempotency evidence.

Building `Scheduling.layer` rejects a recovery interval whose deadline, measured from the current
Clock, exceeds `ScheduleInstant` with `ScheduleValidationError`. Cloudflare also validates each
pre-arm deadline before entering its storage transaction. An unrepresentable alarm timestamp
fails with a typed storage error instead of a defect; a failed transaction retains the prior wake.

## 3. Management and registration

`Scheduling` provides `create`, `get`, `list`, `update`, `pause`, `resume`, and `cancel`. Every management call authorizes `ScheduleScope`. There is no global get or list.

List is owner-scoped keyset pagination, ordered by `ScheduleId` ascending. `after` is the last ID, and a page limit is at most 100. The service default is 50. Pages are not snapshots, but a cursor chain cannot return an item twice.

Create accepts owner, ID, delivery principal, Agent ID, exact definition digests, destination, timing request, and decoded input. It encodes input exactly once into `PersistedJson` and stores its digest. The immutable creation fingerprint covers the registering principal and every original requested field, including unresolved `After.delayMillis` and input digest. Repeating owner, ID, and fingerprint returns the current Schedule snapshot, even after edits. A different fingerprint conflicts. Create must calculate a next occurrence or fail validation.

Update requires `expectedRevision`. It preserves pending delivery, replaces future configuration, reactivates a paused Schedule, and recalculates next occurrence from one captured update time. A recurring rule selects strictly after that time. A one-shot accepts its supplied time and is immediately due if it is in the past. Configuration revision and the store CAS version are distinct. Management controls also fence the storage version so a concurrent completion cannot let resume restore a consumed one-shot cursor.

Pause stops preparation but does not stop pending recovery. Resume skips recurring firing times missed while paused and selects strictly after resume. A never-prepared overdue one-shot retains its original intended time. Resume does not recreate a completed or conclusively refused one-shot. Update is required. Cancel is irreversible, stops future preparation, preserves pending delivery until it resolves, and does not abort an accepted Submission.

## 4. Record and protocol

One versioned `ScheduleRecord` contains the immutable creation fingerprint, current configuration, configuration revision, CAS version, state, next unprepared `nextAtMillis`, optional `pending`, and bounded latest Receipt, refusal, and skipped range. It retains no complete firing history.

`pending.envelope` is `ScheduledEnvelope`. It stores owner, Schedule ID, configuration revision, intended and prepared times, occurrence identity, resolved Conversation ID, delivery principal, Agent ID, exact definition digests, canonical input and digest, admission key, and credential-free authorization identifiers. `pending.retry` is `ScheduleRetry`: attempts, next and last attempt times, and a bounded retry reason. Retry MAY update only `pending.retry`; it MUST NOT replace or patch `pending.envelope`.

Occurrence identity and a fresh Conversation ID derive canonically from full owner identity, Schedule ID, configuration revision, and intended time. The delivery envelope uses the existing submission idempotency semantics.

Preparation captures `now` once, selects a due firing, and authorizes that exact occurrence. One `ScheduleStore.change` transaction verifies revision and cursor, active state, and no pending delivery, then stores the envelope and advances the cursor. A denied preparation records bounded refusal, pauses an active Schedule, and creates no pending work.

The driver recovers pending work before considering the cursor. It calls `ScheduledInputAdmission.submit` outside the local transaction with the exact stored envelope. It never rebuilds it from current configuration, binding, Clock, retry deadline, or alarm time. Receipt completion conditionally clears only a matching occurrence. A stale completion is a full no-op, including cursor and status. A crash after admission retries the same envelope and recovers the existing Receipt.

## 5. Downtime, retry, and authorization

At most one pending delivery exists per Schedule. With none pending, missed recurring firings coalesce to the latest eligible intended firing at or before captured `now`, and the cursor advances to the first future firing. Missed firings are not replayed. A one-shot retains its original intended time until preparation.

`lastSkippedRange` is the half-open interval `[fromMillis, toMillis)`. Coalescing excludes the
selected occurrence at its upper bound; resume excludes the new future cursor. It records a time
range rather than enumerating or counting missed cron firings. Snapshots compute nonnegative
lateness and pending age from the observation Clock, including after a backward clock adjustment.

Transient `transport`, `capacity`, `storage`, `host-closed`, `timeout`, and `ambiguous` outcomes retain pending work. The persisted deterministic delay is `min(1_000 * 2^attempts, 300_000)` milliseconds, with the first failure at zero attempts. There is no jitter and no attempt cap.

`ScheduledInputRefused` is conclusive only when the adapter proves the occurrence was not admitted and the unchanged request cannot succeed. It records refusal, clears only matching pending work, and pauses an active Schedule. Lost replies, ambiguity, corrupt values, and unsupported versions are never refusal.

If local storage fails before a terminal outcome is recorded, recovery may replay the unchanged
admission request. The adapter must return the existing Receipt or the same conclusive refusal.
Temporary rejection belongs in `ScheduledInputRetryable`.

Management authorizes each call. Preparation authorizes each occurrence. A successful preparation authorizes only completion of its frozen envelope. Later revocation blocks future preparation but cannot discard prepared delivery or silently abort accepted work. Existing runtime Tool authorization remains action-time.

Hosts must provide `ScheduleAuthorizer`; there is no default allow policy. A preparation denial
pauses without consuming the cursor, so an authorized resume can recover a never-prepared overdue
one-shot. An unavailable authorization backend is a storage failure, not a denial. Authorization
metadata contains only bounded policy and decision identifiers. Input, credentials, schema
diagnostics containing input, and raw error Causes are not attached to scheduling logs or spans.

## 6. Ports and deployment

`ScheduleStore` is the inward atomic port. Its methods own insert, owner-scoped reads and pages, revisioned changes, indexed due selection, and next-deadline lookup. `ScheduledInputAdmission` admits only an encoded envelope and returns Receipt without exposing ledger internals. Memory, SQLite, and Cloudflare adapters implement `ScheduleStore`.

The optional `NodeScheduling.layer()` calls `NodeDurableHost.submit` through its shutdown gate.
It reuses the host's SQLite `ScheduleStore`. One Scope-owned driver uses indexed due and retry
queries, recovers pending work at startup, and treats the bounded `ScheduleWake` slot as a liveness
hint. Indexed polling repairs a lost hint. Scope closure interrupts the driver and releases its
subscription. Node supports one active scheduler. SQLite storage version 5 adds Schedule records;
older private-development databases fail initialization and must be reset under the existing
storage compatibility policy.

Cloudflare calls `CloudflareConversationClient.submit`. `makeScheduleOwnerObjectClass` creates one
Schedule Owner Durable Object per management scope, addressed by `ScheduleOwnerNamespace` and
`CloudflareSchedulingClient`. The object verifies that every request matches its tenant-qualified
identity. The caller supplies authenticated principal information and an explicit authorizer.

The class factory accepts a host Layer providing `ScheduleAuthorizer` and
`ConversationObjectNamespace`. The host Layer may read effect-cf's `WorkerEnvironment` and
`DurableObjectState`, and the derived `ScheduleOwnerIdentity`. It must provide any other
application dependencies itself. Bindings come from the application's typed Worker environment;
the scheduler does not look them up by string or assemble services through callbacks. Optional
`ScheduleFailpoint` overrides use the same host Layer and reach both scheduling and storage.

The host Layer is cached for one in-memory object incarnation. Cloudflare does not guarantee
finalizers on eviction, so this Layer must not acquire resources that require cleanup. Policy
implementations acquire temporary resources inside `Effect.scoped` in `manage` or `prepare`;
those scopes close on success, failure, and interruption of the operation. The database, alarm
service, and shared admission semaphore stay in the instance runtime.

`effect-cf` 0.37.0 owns the logical alarm table and native alarm lifecycle. Schedule SQL and
`tx.scheduleAlarm` or `tx.cancelAlarm` run in the same `DurableObjectAlarm.transaction` callback
fiber. There is no second alarm queue or nested SQL transaction. Every replacement has a persisted
generation in its payload, including replacements at the same deadline, so acknowledgement of an
old event cannot remove it. Handler entry durably arms a future recovery wake before cross-object
admission. The subsequent Schedule transition replaces or cancels that wake atomically. Unknown
dispatch tags and unsupported payload versions fail closed. Conversation Object maintenance is
unchanged.

Native alarm retries are finite. A successfully pre-armed recovery wake survives a failed handler
and provides another delivery opportunity after those retries. If storage remains unavailable or
the platform cannot arm another alarm, automatic recovery cannot be guaranteed after native
retries are exhausted. The durable pending record remains authoritative and requires restored
storage and a later wake or operator action; it is never treated as refused or completed.

An owner needs recovery wake for any pending delivery, including paused or cancelled work, and deadline wake for active future work. It is idle only when neither exists.

The compiling [Node example](../../packages/platform-node/test/fixtures/scheduling-example.ts)
keeps schedule creation and `runResolvedWorkers` in the same process Scope. The
[Cloudflare example](../../packages/platform-cloudflare/examples/scheduling.ts) provides a
Schedule Owner class factory and a typed daily-report workflow. Both workflows yield their
scheduling service and take the application's actual Agent, input, exact registered definition
digests, and owner scope as data. The application supplies runtime and authorization Layers at
its entry point. Neither example substitutes fabricated binding digests or a default allow policy.

## 7. Evidence

Adapter contracts cover memory, SQLite, and Cloudflare. Deterministic suites cover timing forms, zones and DST, Clock changes, coalescing, updates, pause/resume, cancellation, quotas, revoked authority, creation replay after edit, duplicate wake, immutable pending envelopes, and lost replies. Failpoints surround each durable mutation. Platform suites cover restart or eviction before admission, after admission before Receipt recording, and around cursor, status, and alarm updates.

Node's process-kill matrix checks the database after SIGKILL at insert, preparation, admission, and
Receipt-completion boundaries, then reopens the same file and verifies one Submission. Cloudflare
tests cover preparation and admission eviction, atomic row/alarm rollback, lost post-commit
registration replies, and full Miniflare disposal/restart using persisted native alarms.

Cloudflare tests also prove idle quiescence, recovery after pause/cancel, safe alarm replacement, and unchanged Conversation maintenance. Status tests cover lateness, retries, skipped ranges, and refusals without occurrence history. Public Node and Cloudflare examples must compile before the feature is complete.

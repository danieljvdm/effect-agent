---
title: Operations
description: Inspect, authorize, recover, and back up durable work.
---

# Operations

A durable host must expose accepted work, explain blocked work, and recover it safely. The same
administration contract applies to Node and SQLite class `DN` and Cloudflare Durable Objects class
`DC`.

## Inspect and retry work {#administrative-operations}

`DurableAgentRuntime` provides five operator functions:

- `explain(submissionId)` and `explainThread(threadId)` return the recovery decision,
  operator meaning, and expected disposition. They write nothing.
- `verify(threadId)` runs read-only integrity checks. The digest-chain check reports
  `skipped` unless the host supplies producer identity.
- `retry(RetryCommand.make({ submissionId, author, reason }))` records an audit entry and repeats the classifier's
  decision. It refuses settled work and lanes owned by `resolveUnknown` or `resolveApproval`.
- `wake(threadId)` sends a droppable liveness hint.
- `scanObligations(thresholds)` reports blocked or aging accepted work.

`NodeDurableHost` exposes all five. Use
`vp run admin:durable <explain|verify|retry|wake|obligations> --database <file>` on Node.
Cloudflare Thread Objects expose encoded administration methods through the application's
Worker.

The `explain --thread <id> --json` payload is an array of recovery explanations, including `[]`
when the lane has no nonterminal work. `explain --submission <id> --json` returns one explanation
object.

The administrative methods above, observation, settlement waits, abort, and unknown or approval
resolution consult `OperationAuthorizer`. Its default allows trusted service holders. Install a
real authorizer before exposing these methods outside a trusted host. Denial fails as
`OperationDenied` before protected I/O. The host must authorize admissions before calling `submit`.

## Monitor unfinished work {#obligation-monitoring}

`scanObligations` scans current ledger state. It returns `submissionId`, `threadId`, state,
age, severity, and one of these blockers: `unknown`, `approval`, `waitingForChild`, `ready-aged`,
or `running-aged`.

Run the scan from cron, an alarm, or your monitoring service. Alert on every unknown outcome,
every overdue row, and a growing approval backlog. The framework starts no monitoring daemon and
sends no alerts.

## Abort unknown work

After authorization, abort a submission with:

```ts twoslash
import { AbortCommand } from "@effect-agent/thread/SubmissionLedger";
import { DurableAgentRuntime } from "@effect-agent/thread/DurableAgentRuntime";
import { Effect } from "effect";

const abortSubmission = Effect.fn("abortSubmission")(function* (command: AbortCommand) {
  const runtime = yield* DurableAgentRuntime;
  return yield* runtime.abort(command);
});
```

Create the command with `AbortCommand.make({ submissionId, author, reason })`.
On Cloudflare, obtain `client` from `yield* CloudflareThreadClient`, then call
`client.abort(receipt.threadId, command)`.
The runtime checks authorization before reading or mutating the target.

Recovery claims the unknown head with its abort intent, handles attached children, records an
aborted settlement, and releases queued followers. It does not replay uncertain ordinary tools.
The unknown evidence and first abort audit remain. Abort cannot roll back an external effect. A
`SettlementConflict` reports a terminal result that won before the abort.

Do not edit ledger state, wake the lane in a loop, resolve every open call separately, or clean up
children by hand. The runtime owns those steps after it durably accepts the parent abort. It never
chooses parent abort merely because a child is unknown. The host makes that decision.

There is no automatic inactivity timeout. Unknown and approval-waiting heads stay quiet until an
authorized mutation restores maintenance.

<a id="observe-a-submission-outcome"></a>

## Observe an outcome

Persist the admission `Receipt`. Its submission, receipt, thread, and queue identifiers
remain stable across retries and replacement attempts.

`awaitSettlement` authorizes the receipt's thread and submission before lookup. A receipt
that mixes identifiers fails as `OperationDenied`. Authorization lasts for one wait. To enforce
revocation, interrupt the wait and start another. Interrupting a wait does not abort work.

| Need                      | API or record                                                                         |
| ------------------------- | ------------------------------------------------------------------------------------- |
| admitted or queued        | `SubmissionLedger.lookup` returns `admitted` or `ready`                               |
| execution stage           | lookup returns `running` or `input-applied`                                           |
| suspension or joined host | `loadRecoverySnapshot` returns suspension and host linkage                            |
| unknown outcome           | lookup returns `unknown`; `explain` adds calls, audits, resolutions, and abort intent |
| terminal outcome          | `awaitSettlement(receipt)` returns the durable settlement                             |
| budget or stop detail     | `SubmissionSettled` records `finishReason`, `exhausted`, and `policyLimit`            |

Use `runtime.observe(receipt, { after })` for live progress. Filter the thread
stream by submission or run identifier. Joined input shares its host run, while its own terminal
record keeps the original submission identifier.

```ts
import { DurableAgentRuntime, type Receipt } from "@effect-agent/thread/DurableAgentRuntime";
import { type ObservationOffset } from "@effect-agent/thread/Records";
import { Effect, Stream } from "effect";

const observeOutcome = (receipt: Receipt, after?: ObservationOffset) =>
  Stream.unwrap(
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      return runtime.observe(receipt, { after }).pipe(
        Stream.filter(
          ({ record }) =>
            record.payload._tag === "SubmissionSettled" &&
            record.payload.submissionId === receipt.submissionId,
        ),
        Stream.take(1),
      );
    }),
  );
```

On Cloudflare, read bounded pages with `readPage`, save the last sequence, and call
`awaitProgress` after an empty page. Avoid `readAll` and repeated `explain` calls in polling code.
Keep reads incremental and retain only the application's projection.

The public Cloudflare client cannot read the complete local recovery snapshot or scan all
nonterminal rows. If an external Worker needs suspension, FIFO blocker, input marker, or child
references, add an authorized Schema-backed read-only RPC to the owning Thread Object. Use
its local ledger and recovery snapshot. Do not copy the recovery classifier into the Worker.

Persist observation cursors and make downstream projection or delivery idempotent. A crash after
an external side effect can redeliver the record. Streams, notifications, callbacks, and process
finalizers provide no exactly-once delivery guarantee.

## Back up and restore on Node.js {#backup-and-restore-on-dn}

Use a file-consistent SQLite snapshot. Copy the database, WAL, and SHM files while no process owns
the database, or use `VACUUM INTO` or SQLite's online backup API. The supported `DN` shape has one
process owner per database file.

A restore has four rules:

1. Pre-backup history must pass the same `verify` checks as the original.
2. Fence or terminate every producer from the original timeline before serving restored data.
   Restored storage rejects post-backup ownership tokens.
3. Treat external effects recorded only after the backup as unknown. Never replay them
   automatically. Resolve them through `resolveUnknown` using supplier records.
4. Receipts issued after the backup are gone. Clients must resubmit with idempotency keys. Reconcile
   any external effects from those lost submissions.

<a id="point-in-time-recovery-on-dc-manual-runbook"></a>

## Restore Cloudflare to an earlier point

SQLite-backed Durable Objects provide [30-day point-in-time recovery](https://developers.cloudflare.com/durable-objects/api/sqlite-storage-api/#pitr-point-in-time-recovery-api) through
`getCurrentBookmark`, `getBookmarkForTime`, and `onNextSessionRestoreBookmark`. Miniflare does not
implement these APIs, so follow this hosted runbook:

1. stop admission for affected threads and let current alarms drain;
2. obtain the desired bookmark;
3. call `onNextSessionRestoreBookmark(bookmark)` inside the Object, then `ctx.abort()`;
4. apply the four restore rules above, including unknown external effects and lost receipts;
5. run `verifyEncoded` and `obligationsEncoded` before reopening admission.

Cross-Object children created after the bookmark need parent recovery. A restored Object replaces
old local producer epochs automatically.

## Authorization and isolation

Put each runtime storage domain behind authenticated ingress. Effect Agent provides no tenant
authentication or row-level tenant isolation. Receipts, IDs, principal strings, and audit authors
identify records. They grant no access.

### Storage and addressing

Schedule and subscription owners are tenant-qualified. Ordinary thread requests and records
have no tenant field. A different principal does not create a separate thread log.

- On Node, bind each tenant to its own SQLite database or enforce application isolation on every
  read, write, scan, worker, and administration path.
- On Cloudflare, derive tenant-qualified Thread Object addresses in trusted Worker code.
  `CloudflareThreadClient` does not infer or authorize a tenant from a thread ID.

Keep tenant addressing stable across admission, history, children, schedules, subscriptions,
exports, backups, and operator access. Always check that a submission belongs to its selected
thread.

### Authorize each operation

| Operation                       | Host responsibility                                                     | Framework boundary                                                                     |
| ------------------------------- | ----------------------------------------------------------------------- | -------------------------------------------------------------------------------------- |
| admission and prepared delivery | authenticate ingress, derive principal, authorize agent and destination | Schema validation and idempotency grant no access; authorizer is not an admission hook |
| reads and observation           | authorize the thread and submission before returning data               | runtime observation and waits consult the authorizer; direct store reads do not        |
| abort and resolution            | authorize the decision and supply trusted audit fields                  | runtime checks authorization before target I/O                                         |
| administration and scans        | restrict operators to the selected storage domain                       | default authorizer allows all; scans have no automatic tenant filter                   |

`possessionOperationAuthorizer` allows every request. Use it only behind a trusted host. Install
`operationAuthorizerLayer` when constructing the durable runtime. The runtime captures the policy
at Layer acquisition, so a Layer around a later method call cannot replace it.

Authenticate callers before invoking runtime methods. Some operation requests carry no principal,
and audit fields provide no authorization. Keep raw stores, ledgers, runtime services, and Durable
Object RPCs away from untrusted callers.

### Tools, delegation, and external resources

Use [`RunToolAuthorization`](./tools#authorize-tool-calls) to check proposed tool calls.
`OperationAuthorizer` protects the runtime operations listed above; the host authorizes admission.
Recheck policy after durable suspension. Approval applies to one exact action and expires. Parent
approval does not authorize child actions.

Delegation grants are immutable ceilings. Each child action must satisfy current policy, its grant,
target requirements, and resource scope. Model parameters cannot supply bindings, secrets,
identity, or policy. Validate and bound child output before use.

Keep secrets as handles. Redact diagnostics before storage or export. Generated code needs an
isolated executor with host tools behind the validated broker. Local sandbox processes have no
isolation. Exact-host browser checks do not provide connection-time network isolation. Enforce
read-only SQL through database permissions and host tenant scope.

## Scheduled input

`Scheduling` delivers encoded agent input through durable admission. A due occurrence remains its
obligation until it records a Receipt or proves permanent refusal. The host provides
`ScheduleAuthorizer` and owner-scoped management. Keep `ScheduleDriver` and `ScheduleStore` inside
the privileged host.

Preparation freezes the authorized envelope and advances the cursor atomically. Recover pending
delivery before preparing another occurrence. A lost admission reply retries the same envelope and
idempotency key. Transient or ambiguous failure stays pending. Automatic delivery retries stop after
`maxAutomaticAttempts` (default 8); parked work retains its envelope and receipt uncertainty.
`Scheduling.recover` re-arms that same identity, including after cancellation. Recovery advances an
attempt generation (pass the observed `pending.retry.generation` to `recover`), so a stale failed attempt cannot spend the new retry allowance. A late valid
Receipt can still complete the original obligation. Permanent refusal requires proof
that admission did not occur and the unchanged request cannot succeed.

Recurring downtime coalesces to the latest due firing. Pause and cancel stop new preparation while
pending delivery continues. Cancel is irreversible. Revocation blocks future preparation but must
finish already authorized envelopes. Resuming a paused schedule skips missed recurring times.

Use `{ _tag: "Interval", everyMillis: 60_000 }` for an interval, or
`{ _tag: "Cron", expression: "0 9 * * *", timeZone: "America/New_York" }` for an explicit IANA
cron zone. Cron without a zone keeps its existing UTC default; no host-local zone is inferred.

Quotas count pending delivery plus active or paused cursors. Terminal records retain replay
evidence without using capacity when no delivery remains. Schedule IDs and creation evidence are
never recycled. One corrupt due record cannot block later records; retry a failed sweep after the
recovery poll.

Node runs one Scope-owned indexed polling driver. Cloudflare commits schedule changes and alarms
together, pre-arms recovery before admission, and fences alarm acknowledgements by generation.
Storage failure that prevents alarm repair needs a later wake or operator action after storage
recovers.

See the compiling [Node](https://github.com/danieljvdm/effect-agent/blob/main/packages/platform-node/test/fixtures/scheduling-example.ts)
and [Cloudflare](https://github.com/danieljvdm/effect-agent/blob/main/packages/platform-cloudflare/examples/scheduling.ts)
examples.

## Event subscriptions

Subscriptions retain normalized events, select matching registrations, prepare agent input, and
deliver through durable admission. `EventAcknowledgement` confirms retained intake. A `Receipt`
confirms admission. No run or waiter stays open to watch the source.

Set `expiresAtMillis: null` for no time-based expiry; a once subscription is still consumed by
selection, and cancellation remains explicit. Finite deadlines keep their existing maximum-lifetime
validation. Pausing does not erase selected delivery evidence.

Without an explicit retention policy, records remain for the partition lifetime. Set
`SubscriptionLimits.retention` to bound completed event payloads and deliveries. Intake then
requires a stable authenticated `occurredAtMillis` from the source adapter and rejects fresh
identities outside `replayHorizonMillis`. This horizon is fixed for the partition once used.
Completed work becomes a compact deduplication tombstone until that horizon ends; duplicate intake
cannot reopen routing. `completedRetentionMillis` controls completed payload retention and
`maxTombstones` bounds deduplication storage. Backpressure remains explicit if live evidence or
in-horizon tombstones fill capacity. Idle native maintenance expires old tombstones.

Selected, prepared, parked, recovery-related and admitted-but-unsettled evidence is protected.
Node and Cloudflare admission adapters use the runtime's canonical `submissionStatus`. Enabling
retention with a custom `PreparedInputAdmission` requires that observation capability. An
unavailable probe is logged and retried without releasing evidence. Reconciliation of a payload
already reclaimed reports `event-reclaimed` rather than inventing a new delivery.

Each maintenance pass examines at most `batchSize` event candidates and `batchSize` delivery
candidates, plus one referenced event per delivery. Indexed relationship checks protect recovery
and unsettled work. Independent durable cursors advance past corrupt or protected candidates;
corrupt evidence is preserved and reported. Idle maintenance rearms while retained events remain.
The in-memory adapter copies its bounded maps/indexes, but does not decode the whole partition.

`EventSource.occurredAtMillis` must derive a stable timestamp from authenticated source facts.
Do not substitute intake time or generate a new timestamp on replay. Exact retained identities
replay before current horizon/capacity checks, including their timestamp and payload digest.
Expired pruned identities are rejected by the fixed horizon. Omit retention when the source
cannot supply trusted occurrence time; there is no undated-event pruning exception.

Each event and registration belongs to one stable `SourcePartition` with a tenant ID and source
address. Keep the address unchanged across deployments and source versions. There is no
cross-partition transaction or global subscription directory.

Use `makeEventSource` and `EventSources` for versioned event schemas, identity, matching, and
optional reconciliation. Use `makeSubscriptionInputBinding` and `SubscriptionInputBindings` for
destination preparation. Their callbacks and Schema codecs receive a fresh Scope per operation,
so acquired resources finalize when that operation completes, fails, or is interrupted. Other
service dependencies are captured at host assembly. Keep old source versions and bindings installed while retained work
needs them. Missing or ambiguous bindings leave selected delivery pending as
`unsupported-binding`. Persisted records contain no callbacks, Schemas, Effects, credentials, or
captured services.

`SubscriptionAuthorizer` covers management, intake, reconciliation, and preparation. Recovery has
its own `reconcile` decision. Keep stores, intake, and drivers out of model tool environments.
Restricted tools must bind owner, agent, principal, source catalog, and thread in the host.
A host may also permit a deterministic fresh thread for each selected event.

Use `getSubscription` to inspect the current revision, state, and configuration fingerprint.
`updateSubscription`, `pauseSubscription`, `resumeSubscription`, and `recoverSubscription` require
the expected `configurationRevision`. `cancelSubscription` also accepts an optional expected
revision. Revision conflicts expose the current revision/state; inspect the fingerprint after a
lost management reply to determine whether the intended configuration won. Creation replay always
compares the original request fingerprint, even after edits.

Every configuration/control revision advances the registration's eligibility ordinal. It can select
only events accepted after that revision. Events accepted earlier but not yet selected do not gain
eligibility under the new configuration, including changed matching keys. Selected deliveries retain
their captured configuration; pausing stops new selection while selected delivery continues.
Cancellation and captured expiry can refuse unprepared work. Prepared envelopes remain unchanged.
An uncertain ordinary tool is never replayed automatically.

Source recovery completions carry the captured registration revision. Pause preserves its recovery
intent, resume restores polling, and callbacks from an older revision cannot overwrite a new one.
Recovery reads share the per-item timeout and failure boundary. An unreadable record is reported
without guessing its revision or blocking routing, delivery, and maintenance for other records.
`recoverSubscription` re-arms source reconciliation where the configured source supports it;
`recoverDelivery(scope, key, expectedGeneration)` re-arms an individual parked delivery without
changing its identity or envelope. Repeating the same recovery generation is an idempotent no-op.

Intake deduplicates by tenant, source address, and logical event ID. Conflicting payload or source
version fails. Each event records a registration cutoff. Duplicate intake cannot move that cutoff
or reopen routing. Selection atomically advances the cursor, creates delivery obligations, and
consumes once registrations. Continuous subscriptions retain each event separately. Thread
admission order decides execution order.

Preparation rechecks authority, validates input, and freezes destination, principal, digests,
authorization metadata, and admission key. Cancellation, expiry, or revocation blocks new
preparation. Already prepared envelopes continue unchanged. Expiry never sends agent input.

Lost admission replies retry the exact envelope and key. After `maxAutomaticAttempts` (default 8),
delivery parks until `recoverDelivery` explicitly re-arms it. Cancellation never erases a prepared
uncertain envelope. Capacity waits spend the same automatic retry allowance; hosts should size it
for expected job duration and explicitly recover parked work. Event routing and source recovery
use their existing bounded sweeps and retry deadlines; settlement probes retry conservatively.
The delivery attempt cap does not cap all background maintenance.

Optional `admissionGroup` permits one actually unsettled submission per group **in a destination
thread**. Admission, suspension, unknown outcome and terminalization all retain occupancy until
canonical settlement finalization. A lagging finalization conservatively holds capacity until repair.
`FreshThread` per event does not provide exclusion across threads. Schedules retain one frozen
pending occurrence and coalesce missed recurring times; distinct events keep separate durable
obligations and produce explicit backpressure when backlog capacity is exhausted.

`admissionFence` captures bounded `{ policyId, key, revision }` coordinates. Install
`SubmissionAdmissionFence` when acquiring the destination ledger. Exact retained requests replay
before policy and occupancy checks; changed input, group or fence conflicts. Fresh admission checks
policy in the **same local transaction** as the ledger insert. SQL hosts must read policy through
that transaction's SqlClient, and all policy writers must share its authority. Remote policy reads
cannot fence remote mutations. Memory executes the bounded callback synchronously inside its
atomic mutation; an asynchronous callback fails closed as unavailable and is interrupted. Do not
fork, reenter admission, or retain transaction resources in the callback.

`AdmissionPolicyError.reason` distinguishes `refused` (conclusive stale/unsupported policy),
`unavailable` (retry without assuming nonadmission), and `occupied` (group capacity). Uninterpreted
fences fail closed. Ingress authorization is still required for replays. Put no secrets in fence
coordinates or error codes. Public status excludes payloads, parameters, context and credentials.
Execution remains at least once; provider failures never invent event completion.

### Partition-owned ancillary alarms

Cloudflare hosts can install `SubscriptionPartitionAlarmExtension` with handlers built by
`makeSubscriptionPartitionAlarmHandler`. Each handler owns one non-framework tag, a payload
Schema and a bounded timeout (at most 30 seconds). The factory captures host services but defers
`Subscriptions`, `SubscriptionIntake` and `SubscriptionDriver` requirements, including those in
payload decoders, until invocation. `makeSubscriptionPartitionObjectClass` supplies these native
services from the addressed partition after building the host Layer. Keep their lookup inside the
callback or decoder; yielding them while building the host Layer still requires them at assembly.
Handlers retain only their required native services in `R`; host requirements remain on the factory
Effect. Even if native services were present at assembly, the invocation uses its own instances.

An existing `handle: () => runtime.process` can therefore keep `SubscriptionIntake` in the process
Effect's requirements. Map its expected failures to `SubscriptionAlarmExtensionError`; no extra
handler argument or client Layer is needed. See the compiling Cloudflare example below for a
handler that accepts an event through native intake.

Each codec/callback invocation owns a fresh Scope. Invocation cleanup runs on success, typed failure,
defect, timeout and interruption; captured host services keep their host lifetime.

The native multiplexer processes at most 16 alarms per invocation and durably retries failed rows
independently, so a failed or unknown extension cannot block the subscription driver. Installing
handlers reserves eight minutes of the twelve-minute invocation budget for ancillary work; native
driver limits must fit the remaining four minutes. Unknown,
ambiguous, malformed and reserved `effect-agent/` tags fail closed and are reported. A replacement
alarm survives acknowledgement of its earlier version. The host owns external-effect idempotency,
uncertainty, payloads and transactional prearming; this extension defines no provider scheduler.

### Adopting these contracts

The persistent adapters automatically upgrade the unpatched beta49/beta50 format on acquisition:
Cloudflare Thread, Schedule and Subscription stores move from version 2 to 3; the combined SQLite
file moves from version 7 to 8. Each owning store upgrades in one native transaction and advances
its version marker last. Reopening after interruption retries the entire uncommitted upgrade.
Namespaces, canonical history and digests, receipts, pending work, ownership, deadlines, alarm
generations and scan cursors are preserved. Keep the existing namespace/file and the old source
versions, input bindings and agent registrations needed to finish retained work.

Legacy subscription configurations become revision 1 and remain the immutable configuration for
already selected deliveries. Retry counts become the initial generation's automatic attempt count;
existing runnable work stays runnable even if it already exceeds a newly configured retry cap.
The next failed attempt applies the current cap. No admission group or fence is inferred.

Historical occurrence and settlement timestamps stay absent. An event replay with the same retained
identity and payload digest returns the original event even if the caller now supplies an occurrence
time that was previously unknown. Once an occurrence time is stored it must match on replay. Replay
never fills missing historical timestamps or invents a receipt. Retention conservatively keeps
history whose occurrence or settlement time is unknown.

An unsupported layout, corrupt transformed record, orphaned delivery, mismatched fingerprint or translated value
over the adapter's size limit fails acquisition without advancing the version. Thread errors identify
the table and row; Schedule and Subscription errors retain the structured diagnosis in `cause`.
Unchanged canonical history uses the existing reader validation rather than a new startup audit.
Preserve the original store and investigate with its original writer or backup. Do not replace a
namespace to work around an upgrade failure. Upgrades are forward-only: older binaries reject the
new marker. Back up SQLite files or use the platform's backup facilities before an application rollout.

Custom stores must implement revision and retry-generation fencing, bounded retention cursors and
the canonical observation contract. Existing finite lifetimes, UTC cron defaults and no-retention
behavior remain available.

Node uses a Scope-owned indexed polling driver. Cloudflare commits work and required alarms
together and re-arms after failed passes. If storage prevents both mutation and alarm repair,
restore storage and send a new wake or intervene as an operator.

See the compiling [Node](https://github.com/danieljvdm/effect-agent/blob/main/packages/platform-node/test/fixtures/subscriptions-example.ts)
and [Cloudflare](https://github.com/danieljvdm/effect-agent/blob/main/packages/platform-cloudflare/examples/subscriptions.ts)
examples.

### GitHub workflow run completion

Import GitHub integration from `@effect-agent/thread/GitHubWorkflowSource`.
`makeGitHubWorkflowRunSource` watches one repository, run ID, attempt, and expected head SHA. It
reports successful and unsuccessful completion. It does not aggregate every check for a commit.

`acceptVerifiedGitHubWorkflowRunWebhook` verifies raw bytes before parsing a completed
`workflow_run` event. Webhook and exact-attempt API observations normalize to one completion
identity. A webhook delivery ID alone cannot deduplicate them.

Registration arms reconciliation before provider reads. An already completed attempt can notify a
new watch without a check-then-subscribe race. Cancellation, expiry, and once selection stop
provider polling. GitHub does not automatically redeliver failed webhooks, so reconciliation checks
the registered attempt while it remains retained, readable, and authorized. It does not provide
general historical replay.

## Next steps

- [Persistence & durability](../concepts/durability) explains recovery.
- [Certify storage adapters](./certify-adapters) covers third-party adapter requirements.

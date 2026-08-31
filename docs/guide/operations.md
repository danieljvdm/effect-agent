---
title: Operations
description: Durable runtime administration, obligation monitoring, and backup and restore for both deployment classes.
---

# Operations

A durable runtime accepts obligations, so someone has to be able to see them, explain them, and
unblock them. This guide covers the administrative operations, obligation monitoring, and backup
and restore on Node/SQLite (`DN`) and Cloudflare Durable Objects (`DC`).

## Administrative operations

Five operations are members of `DurableAgentRuntime`, implemented over the `SubmissionLedger`
and `ConversationStore` ports only, so they behave identically on `DN` and `DC`:

- `explain(submissionId)` / `explainConversation(conversationId)` returns a read-only recovery
  explanation: the classifier decision, its operator meaning, and the disposition a recovery
  pass would report. It writes nothing.
- `verify(conversationId)` runs read-only integrity checks with typed per-check results. It never
  repairs. The digest-chain check reports `skipped` with the reason unless per-batch
  producer identity is supplied out-of-band.
- `retry(submissionId, { author, reason })` records an audit entry and re-drives exactly the
  classifier's decision, with typed refusals for settled work and lanes owned by the
  `resolveUnknown`/`resolveApproval` paths. Author and reason are mandatory.
- `wake(conversationId)` sends a droppable liveness notification.
- `scanObligations(thresholds)` returns the obligation report described below.

On `DN`, `NodeDurableHost` re-exposes all five, and
`vp run admin:durable <explain|verify|retry|wake|obligations> --database <file>` is the CLI.
On `DC`, the Conversation Object exposes Schema-encoded entry points (`explainEncoded`,
`verifyEncoded`, `retryEncoded`, `obligationsEncoded`, `wake`); deployments reach them through
their own Worker. Every operation, including `observe`, `awaitSettlement`, `abort`,
`resolveUnknown`, and `resolveApproval`,
consults `OperationAuthorizer`. Its default permits trusted service holders; it does not
authenticate callers or enforce tenant policy. An installed authorizer's denials become typed
`OperationDenied` failures before the operation reads or writes. See
[Authorization and isolation](#authorization-and-isolation) before exposing these operations.

## Obligation monitoring

`scanObligations` is scan-based, never a daemon. It folds the ledger's nonterminal scan into
rows `{submissionId, conversationId, state, blockedOn, ageSeconds, severity}`, where `blockedOn`
is one of `unknown`, `approval`, `waitingForChild`, `ready-aged`, `running-aged` and severity is
classified against your `{agingSeconds, overdueSeconds}` thresholds.

Hosts own the alert loop. The framework deliberately does not schedule the scan or deliver
alerts: run `scanObligations` periodically from your host (cron, alarm, monitoring agent),
export the rows as logs or metrics, and alert on:

- any row with `blockedOn: "unknown"`, because an Unknown Outcome needs an authorized
  `resolveUnknown` decision;
- any row with severity `overdue`, which marks accepted work without timely settlement;
- a growing `approval` backlog.

## Abort unknown work

Call `DurableAgentRuntime.abort(AbortCommand.make({ submissionId, author, reason }))` after the
host authorizes stopping that Submission. On Cloudflare, use
`CloudflareConversationClient.abort(receipt.conversationId, command)`. The runtime asks
`OperationAuthorizer` about the target Submission before reading the ledger, recording intent,
or notifying workers. The possession default preserves trusted-host behavior.

Normal recovery/maintenance now claims an unknown head with that durable intent, cleans up and
joins attached children, and records the aborted settlement. It releases queued followers without
replaying uncertain ordinary Tools. The original abort audit and unknown evidence remain; abort
does not claim that an external action was rolled back. Repeated commands preserve the first
intent. A `SettlementConflict` reports an outcome that already won, including an aborted outcome
whose acknowledgement was lost.

Remove consumer loops that follow abort with `ResolutionAbortSubmission` for every unresolved
call, manually clean up children after an accepted parent abort, edit ledger state, or repeatedly
wake a blocked lane.
An unknown or approval-waiting head with ready followers quiesces until an authorized mutation
restores its maintenance alarm. Keep host decisions about whether and when to abort; there is no
automatic inactivity deadline.

A parent suspended in `WaitingForChild` does not acquire abort authority because its child is
unknown. The host must explicitly decide to abort the parent, or apply a separately configured
authorized policy. The fix owns child abort propagation, joining, and reservation cleanup only
after that parent abort is durably accepted. It does not choose the parent's outcome beforehand.

## Observe a Submission outcome

`awaitSettlement` authorizes the Receipt's Conversation and Submission before its first lookup.
The stored Submission must belong to that Conversation. A mixed Receipt fails with
`OperationDenied` before settlement finalization, recovery reads, or polling.
Authorization lasts for that wait, just as one `observe` subscription authorizes once at
subscription time. Hosts enforcing revocation during a long wait must interrupt it and start a
new authorized wait. Neither operation accepts an authenticated caller identity from the wire;
hosts can capture their authenticated context in the installed authorizer. A Receipt identifies
work and does not grant access.

Persist the admission `Receipt`: its `submissionId`, `receiptId`, `conversationId`, and
`queueSequence` identify the same obligation across retries and replacement Attempts. The APIs
below supply the evidence inside a host with the real `SubmissionLedger` and `DurableAgentRuntime`
services. They are not all remotely available through `CloudflareConversationClient`.

| Need                         | Public API and evidence                                                                                                                                                                                                 |
| ---------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Admitted or queued           | `SubmissionLedger.lookup(SubmissionLookupById.make({ submissionId }))`: `admitted` or `ready`; the Receipt proves admission.                                                                                            |
| Execution stage              | The same lookup returns `running` or `input-applied`. This is durable operational state, not proof that a worker is alive.                                                                                              |
| Intentional wait             | `loadRecoverySnapshot(RecoverySnapshotRequest.make({ submissionId }))` exposes `suspension` and joined host linkage. `explain(submissionId)` supplies the existing classifier decision.                                 |
| Unknown outcome              | Lookup returns `unknown`; `explain` adds open calls, uncertainty audits, accepted resolutions, and abort intent.                                                                                                        |
| Completed, failed, aborted   | `awaitSettlement(receipt)` returns the durable terminal identity/outcome and bounded failure diagnostic. Interrupting the wait detaches the caller; it does not abort the Submission.                                   |
| Terminal stop/budget details | The canonical `SubmissionSettled` record carries `finishReason`, `exhausted`, and `policyLimit` where applicable. These fields are **not** currently on the returned `Settlement`; read the record through observation. |

For live progress use `DurableAgentRuntime.observe(receipt, { after })`. It is a Conversation
Stream, so filter by `submissionId` or `runIdForSubmission(submissionId)` as appropriate. Joined
input shares its host's Run; use the snapshot's `hostSubmissionId` for Run evidence, and the
original Submission ID for its own terminal record. For example, this Stream yields the exact
terminal envelope, including budget metadata and its resumable cursor:

```ts
import { DurableAgentRuntime, type ObservationOffset, type Receipt } from "@effect-agent/session";
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

On Cloudflare, consume bounded `CloudflareConversationClient.readPage` pages and retain the last
`sequence`; after an empty page, `awaitProgress(conversationId, sequence)` waits for a hint or
already committed progress before another read. Do not call `readAll` or `explain` in a polling
loop: `explain` is a diagnostic read of conversation history. For a first run-scoped read inside
an authorized host, the public recovery snapshot's `inputApplied.sequence` provides a lower
bound for `ConversationStore.read` (use `sequence - 1` to include the input); a ready follower has
no Run history yet. Keep subsequent reads incremental and retain only the application's projection.
Cloudflare already exposes `explainEncoded` for occasional remote diagnosis.

An independent Worker outside the Conversation Object cannot obtain the complete operational
snapshot through that client. The public `portCall` protocol supports ledger lookup, which returns
the admitted input and Submission state, but has no recovery-snapshot or nonterminal-scan request.
The routed ledger keeps `loadRecoverySnapshot` local-only and `scanNonterminal` local to its owning
Object. Canonical paging and progress waits do not expose the authoritative suspension,
`inputApplied` marker, or FIFO blocker. There is no public composition that obtains all of these
remotely without an additional host boundary; repeated `explainEncoded` calls are not a bounded
snapshot substitute.

For an external Worker that needs those fields, retain an authorized, Schema-backed, read-only
snapshot RPC inside the owning Conversation Object and its external client adapter. That RPC
validates the Receipt against local lookup, uses the real ledger's `loadRecoverySnapshot` for
input application, suspension, abort intent, and joined host linkage, and scans the local ordered
nonterminal rows for an earlier FIFO blocker. It can expose the relevant child references for
host policy. It need not copy the recovery classifier or construct dummy local services outside
the Object. Any application-specific activity or Tool-result projection can use bounded canonical
reads from the input marker and an incremental cursor. This remains a host adapter, not an API
added by the abort fix.

Inside the owning Object, the ordered public `SubmissionLedger.scanNonterminal` identifies the
earlier unsettled head in its Conversation; `explain` diagnoses that head without reimplementing
recovery. Do not infer the follower's state from the Conversation's latest Run.

Canonical records replay from the saved cursor. Consumers own cursor persistence and idempotent
projection or delivery; checkpointing after a side effect can redeliver it after a crash. Neither
the Stream, a notification, a callback, nor a process-local finalizer guarantees durable external
delivery. No exactly-once external delivery is promised. Hosts must authorize public ledger reads
and observation, and handle typed storage, protocol, and authorization errors without interpreting
them as a terminal outcome.

Replace only inspection/classification and outcome-polling logic that the APIs available at the
caller's boundary actually cover. Keep the host snapshot RPC when an external Worker needs the
operational evidence above. Keep application policy for visible responses, whether an acknowledgement is sufficient,
destinations and authorization, inactivity thresholds, and delivery retries. Tool success or an
application result tag is not a library-defined answer obligation; project that policy from
canonical records without adding it to the runtime.

## Backup and restore on DN

Any file-consistent snapshot works as a backup: copy the `.sqlite`, `-wal`, and `-shm` files
while no process holds the database, or use `VACUUM INTO` or SQLite's backup API online. The
claimed `DN` deployment shape is one process owner per database file.

Restoring a backup means accepting four semantics (the repository proves them with an executable
restore drill):

1. **Pre-backup history survives intact.** The restored store passes the same integrity checks
   (`verify`) as the original.
2. **Post-backup epochs are fenced.** An ownership token minted on the original timeline after
   the backup point is rejected typed by the restored store. Before serving traffic from a
   restore, fence or terminate every producer that ever ran against the original store: a
   divergent producer must never be left assuming it still owns anything.
3. **Post-backup external effects enter the Unknown regime.** An external call whose
   outcome was only recorded after the backup re-enters recovery as an open call and is marked
   `ToolCallUnknown`. It is never assumed rolled back and never automatically replayed: resolve
   each one through `resolveUnknown` from external truth (the supplier's records).
4. **Post-backup admissions are gone.** Receipts issued after the backup point do not exist in
   the restored store. Clients holding such Receipts must resubmit (idempotency keys make the
   resubmission safe), and any external effects those lost Submissions performed must be
   reconciled through the same Unknown discipline.

## Point-in-time recovery on DC (manual runbook)

Cloudflare Durable Objects provide point-in-time recovery over the last 30 days through the
hosted platform (`ctx.storage.getCurrentBookmark()`, `getBookmarkForTime(...)`,
`onNextSessionRestoreBookmark(...)`). Miniflare does not implement these APIs, so this procedure
is a manual runbook, not an executed claim:

1. Stop new admission for the affected Conversations (route submissions away or deny at your
   Worker) and let in-flight alarms drain.
2. Obtain a bookmark: `getBookmarkForTime(timestamp)` for the desired restore point, or a
   bookmark captured earlier (for example, logged before a risky operation).
3. Call `onNextSessionRestoreBookmark(bookmark)` inside the Object, then `ctx.abort()`; the next
   session starts from the restored state.
4. Apply the same four restore semantics as on `DN`: the restored Object's startup
   reconciliation re-classifies in-flight work; treat every open external effect as Unknown and
   resolve it from supplier truth; treat Receipts issued after the bookmark as lost admissions;
   assume every pre-restore producer epoch is superseded (automatic within one Object, but
   cross-Object children established after the bookmark must be reconciled through the parent's
   recovery ladder).
5. Run `verifyEncoded` and `obligationsEncoded` before reopening admission.

## Authorization and isolation

The supported boundary is a host-controlled storage domain and trusted runtime services behind
authenticated ingress. The framework does not provide general tenant authentication or row-level
tenant isolation. A Receipt, Conversation ID, Submission ID, principal string, or audit author
identifies something; none proves that a caller may access it.

### Storage and addressing

[`ScheduleOwner`](https://github.com/danieljvdm/effect-agent/blob/main/packages/session/src/schedule.ts)
contains `tenantId` and `ownerId`. Subscription owners and source partitions are also
tenant-qualified. Those scopes govern their own registrations, management, and delivery state.
They do not qualify ordinary Conversation operations:
[`AdmissionRequest`](https://github.com/danieljvdm/effect-agent/blob/main/packages/session/src/ledger.ts)
and [`CanonicalRecordEnvelope`](https://github.com/danieljvdm/effect-agent/blob/main/packages/session/src/records.ts)
have no tenant field. Admission keys are scoped by Conversation, principal, and idempotency key;
a different principal does not create a separate Conversation log.

For tenant isolation, the host must choose and enforce database or namespace separation:

- On Node, bind each tenant's runtime to its own SQLite database/storage domain. One process owns
  each database. Sharing a database across tenants requires additional application isolation for
  every read, mutation, scan, worker, and administration path; the adapters do not supply it.
- On Cloudflare, select the permitted Durable Object namespace and derive a tenant-qualified
  Conversation address in trusted Worker code. Separate namespaces can provide a stronger host
  boundary. `CloudflareConversationClient` routes with `namespace.idFromName(conversationId)`;
  it does not infer a tenant or authorize an arbitrary name supplied by a caller. A private Object
  database isolates that Object's storage, not access through a Worker holding its namespace.

Keep the tenant-to-storage/address mapping stable across restarts, and apply it to admission,
history, child Conversations, schedule/subscription destinations, and administration. Check the
relationship between a Submission and its owning Conversation; a guessed or copied ID must not
select another tenant's runtime. Scope logs, exports, backups, and operator access to that same
boundary. Owner-scoped scheduling alone is insufficient.

### Authorize each operation

| Operation                                                  | Host responsibility                                                                                                                                                    | Framework boundary                                                                                                                                                                                   |
| ---------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Admission, including retries and prepared deliveries       | Authenticate ingress, derive the principal from trusted identity, authorize the Agent and destination, and select the permitted storage domain before calling `submit` | Schema validation, admission idempotency, and durability do not grant access. `OperationAuthorizer` is not an admission hook.                                                                        |
| Reads, exports, observation, progress and Settlement waits | Authorize the Conversation/Submission and filter the accessible storage domain before returning any records or status                                                  | Durable observation/wait paths consult the operation authorizer. Direct `ConversationStore` reads/exports and `PersistentHistory.layer` use the supplied store's authority and add no access policy. |
| Abort and approval/Unknown Outcome resolution              | Authenticate the decision maker, verify target ownership and action permission, and supply trusted author/reason audit fields                                          | The runtime consults the operation authorizer before reading or mutating the target. Audit fields alone do not authorize the command.                                                                |
| Explain, verify, retry, wake and obligation scans          | Restrict administration to authorized operators in the selected storage domain; scans can expose multiple Conversations                                                | The operation authorizer covers these commands, but the default allows them. A scan is not automatically filtered by tenant.                                                                         |

[`OperationAuthorizer`](https://github.com/danieljvdm/effect-agent/blob/main/packages/session/src/operation-authorizer.ts)
defaults to `possessionOperationAuthorizer`, which allows every request. This is suitable only
inside a trusted host boundary. Install a real policy through `operationAuthorizerLayer` when
constructing the durable runtime; denials propagate as `OperationDenied` before protected I/O.
The runtime captures the authorizer during Layer acquisition. Adding an override around a later
method call does not replace that captured policy.

The framework does not parse tokens or verify a `Principal` string. Some operation requests have
no principal, and observation/wait wire APIs do not carry authenticated caller identity. Authorize
in the host before invoking them and provide any identity needed by the installed policy through
trusted host context. Do not expose raw storage, ledger, runtime, or Durable Object RPC services
to untrusted callers. An authorizer cannot reconstruct identity that ingress never authenticated.

An observation or Settlement wait authorizes once for its lifetime. To enforce later revocation,
interrupt it and begin a new authorized operation. Use host credentials and policy for scheduled
or subscription recovery; a retained principal is evidence of identity, not continuing permission.

### Tools, delegation, and external resources

Operation authorization is separate from Tool authorization. Default durable assemblies use
`RunToolAuthorization.allowAll`; install a host Tool policy through the
[independent authorization Layer](./context-management#composing-preparation-and-tool-authorization).
Recheck action policy after durable suspension. Approval is bound to the exact action and expires;
parent approval never authorizes a child's later actions.

Delegation grants are immutable ceilings. Each child action must satisfy current policy, its
grant, target requirements, and normalized resource scope. The model supplies decoded task
parameters, not bindings, identities, secrets, or policy. Missing authorization dependencies deny
the action. Child output and artifacts remain untrusted, bounded, Schema-validated input.

Keep secrets as handles and redact diagnostic data before persistence or export. Raw Tool Causes
belong only to an explicitly installed trusted local observer. Generated programs need an isolated
executor with no ambient authority; host Tools remain behind the validated broker. Local sandbox
processes are unisolated. Browser exact-host URL checks are not connection-time network isolation;
use a supported containment policy for hostile content. Enforce read-only SQL with database
permissions and host-owned tenant scope, not source-text inspection.

## Scheduled input

`Scheduling` delivers encoded Agent input through ordinary durable admission. It owns a due
occurrence until it records a Receipt or a proven permanent refusal; it does not promise one Run
per firing. Hosts provide an explicit `ScheduleAuthorizer` and owner-scoped management.
`Scheduling` exposes only management and bounded status projections, excluding input, digests,
storage versions, and pending admission envelopes. Keep `ScheduleDriver` and `ScheduleStore` in
the privileged host; never provide them to management callers or model Tools.

Preparation atomically freezes the authorized envelope and advances the cursor. Recover pending
delivery before preparing another occurrence. A lost reply retries the exact envelope and
idempotency key, never current configuration or a newly calculated time. Only a matching
occurrence may clear pending state. Transient or ambiguous failure remains pending; a conclusive
refusal requires proof that admission did not occur and the unchanged request cannot succeed.

Recurring downtime coalesces to the latest due firing instead of replaying missed firings.
Named-zone cron selection follows the forward sequence from the stored cursor, including spring
gaps and the first fall-fold occurrence. Reverse traversal is not its inverse across offset changes.
Pause stops new preparation; cancel is irreversible and stops future preparation. Neither drops
pending delivery nor aborts accepted work. Revocation blocks future preparation, but recovery must
finish already-authorized envelopes. Resume from paused skips missed recurring times; resuming an
active Schedule is a no-op after authorization and revision checks. Resume cannot resurrect a
consumed one-shot; update is required. Creation replay uses its original fingerprint even after edits.

Owner quotas count operational work, including pending delivery and active or paused cursors.
Completed or cancelled records without pending work retain replay evidence without consuming
capacity. Reactivation checks capacity atomically; Schedule IDs and their creation evidence are
not recycled. Due queries page by indexed key, and one failed or corrupt record cannot block later
records. A query-wide storage failure still fails the sweep. Retry failed sweeps after the recovery
poll, with no in-memory exclusion list or busy loop; interruption preserves pending work.

Node runs one Scope-owned driver with indexed polling to repair lost wake hints. Cloudflare stores
Schedule changes and alarm updates in one transaction, pre-arms recovery before remote admission,
and fences acknowledgements by alarm generation. A sweep with failures re-arms recovery after its
transitions, because they may have replaced the earlier wake. A cancelled or paused Schedule still needs a wake
while delivery is pending. Storage failure that prevents a recovery alarm requires restored storage
and another wake or operator intervention; pending work is never silently marked complete.

See the compiling [Node](https://github.com/danieljvdm/effect-agent/blob/main/packages/platform-node/test/fixtures/scheduling-example.ts)
and [Cloudflare](https://github.com/danieljvdm/effect-agent/blob/main/packages/platform-cloudflare/examples/scheduling.ts)
examples for host setup and typed registration.

## Event subscriptions

The bounded subscription implementation tracked by
[#223](https://github.com/danieljvdm/effect-agent/issues/223) is available. It includes trusted
application events and the GitHub workflow-attempt source below; it is not a general event bus or
provider catalog.

`Subscriptions` is the event-driven sibling of `Scheduling`. A subscription can outlive its Run.
`SubscriptionIntake` first retains a normalized event and its unfinished routing work, then returns
an `EventAcknowledgement`. That acknowledgement is not a Submission Receipt. Delivery uses ordinary
Conversation admission, ordering, and joining; each event does not necessarily start its own Run.
No waiter or Run stays open to watch a source.

::: warning Alpha retention limit
Completed records consume quota permanently. A continuous subscription eventually exhausts
partition or owner capacity even when every delivery succeeds. There is no automatic pruning;
safe pruning requires a separate durability design. Plan capacity for the partition's full lifetime.
:::

The host assigns each source a stable `SourcePartition`, consisting of a tenant ID and source
address, such as a repository identity. Every event and registration belongs to exactly one
partition. Keep that address unchanged across deployments, source versions, and payload changes.
Node uses partition-qualified SQLite tables under one process owner per database. Cloudflare uses
one addressed Subscription Partition Object. There is no cross-partition transaction or global
subscription directory. Hosts must bound the partitions accessible to each owner.

Install exact, versioned behavior with `makeEventSource` and `EventSources`. The source supplies
event and parameter Schemas, canonical event identity and matching keys, deterministic bounded
matching, and optional reconciliation. One source implementation serves every destination Agent
in its partition.

Install destination preparation with `makeSubscriptionInputBinding` and `SubscriptionInputBindings`.
Each binding supplies the source event and parameter Schemas, continuation Schema, destination
Agent input Schema, and Effect preparation callback. Resolution requires exactly one binding for
the source name/version, Agent ID, and retained definition digests. Two Agents can consume the same
event with different context and input shapes. Include the context Schema and mapper in the
destination's definition version; changing either requires new definition digests. Keep old source
versions and preparation bindings installed while retained work needs them. Missing or ambiguous
bindings reject registration and leave selected work pending with an `unsupported-binding` failure.
Prepared work retries its frozen envelope without resolving preparation code again. Persisted data
never includes callbacks, Schemas, Effects, credentials, or captured services.

Provide an explicit `SubscriptionAuthorizer`. It authorizes every management operation, intake,
reconciliation, and preparation. Its required `reconcile(subscription)` method grants host recovery
authority separately from `intake(partition, source, principal)`. A subscribing user's identity is
registration evidence and need not have webhook ingress permissions. Recovery checks this policy
before reading the provider; denial records a conclusive recovery failure and stops polling.
`Subscriptions` exposes `subscribe`, `listSubscriptions`, `cancelSubscription`,
and redacted delivery status. Management is scoped to one partition and tenant-qualified owner;
possession of a handle is not authority. Keep `SubscriptionStore`, `SubscriptionIntake`, and
`SubscriptionDriver` out of model Tool environments. Restricted subscription Tools bind the owner,
Agent, principal, permitted source catalog, and current Conversation in the host. A host may also
permit a deterministic fresh Conversation per selected event.

Registrations cannot be edited, paused, or resumed. Cancel and register a new creation identity to
change configuration. Reusing a creation identity with the same fingerprint returns the retained
registration, including after cancellation; conflicting reuse fails. Durable Tools retain their
creation identity and result through `DurableStep`. An uncertain ordinary Tool is not automatically
replayed after ownership loss.

Intake deduplicates by tenant, stable source address, and logical source event ID. A conflicting
payload or source version fails. Registration and intake share a local storage sequence. An event
can select only registrations created by its recorded cutoff; duplicate intake never moves that
cutoff or reopens routing. Sources index registrations by exact source version and canonical
matching key. Additional filters inspect only that bounded candidate set.

Selection atomically advances the event cursor, inserts delivery obligations, and consumes once
registrations. The first atomic selection wins, regardless of provider timestamp. Failure or
refusal does not reactivate a consumed registration. Continuous subscriptions keep distinct events
in a backlog without coalescing. Delivery and retries may reorder admissions; Conversation admission
order governs execution. There is no global or provider-event ordering promise.

Preparation rechecks authority, constructs and validates input, and freezes an envelope with the
destination, principal, definition and input digests, authorization metadata, and stable admission
key. Storage rechecks cancellation and expiry when committing it. Cancellation, expiry, and policy
revocation stop new preparation, including a selected once delivery, which records an explicit
refusal. Already-prepared envelopes finish recovery unchanged. Aborting admitted work is separate.
Expiry itself never sends input to an Agent.

Lost admission replies retry the exact prepared envelope and key. Only the matching delivery can
record its Receipt. Permanent refusal requires proof that this unchanged request was not admitted
and cannot succeed. Execution remains at least once; there is no exactly-once external-effect claim.
Expected transient failures, defects, timeouts, and interruption preserve committed or recoverable
work. Public status omits payloads, parameters, continuation context, and credentials.

Each recovery pass processes one bounded page of source recovery, event routing, and delivery work.
Durable scan cursors advance independently of individual failures, so restart or eviction cannot
repeatedly send every pass back to a corrupt first record. A corrupt candidate or missing source
version leaves that event's fanout pending with its cursor intact; unrelated events and deliveries
continue. `SubscriptionIntake.status` exposes routing progress and failures without returning the
event payload. Source reconciliation status retains bounded failure codes; conclusive source
failures stop polling, while unavailable state and rate limits remain pending recovery.

Node's Scope-owned driver uses indexed recovery polling. Cloudflare commits work and required
alarms together, pre-arms recovery before external calls, and re-arms after failed passes. Wake hints
never own work. A storage outage that prevents both durable mutation and alarm repair requires
restored storage and a new wake or operator intervention; it never authorizes dropping work.

See the compiling [Node](https://github.com/danieljvdm/effect-agent/blob/main/packages/platform-node/test/fixtures/subscriptions-example.ts)
and [Cloudflare](https://github.com/danieljvdm/effect-agent/blob/main/packages/platform-cloudflare/examples/subscriptions.ts)
examples for source registration, policy, storage, and driver assembly.

`SubscriptionLimits` bounds payload, context, lifetime, candidate pages, concurrency, retries, and
retained records per partition and owner. This implementation retains event deduplication, creation
replay, registration configuration, and completed delivery evidence for the partition's lifetime.
There is no automatic pruning or identity recycling. Quotas count retained records, including
terminal ones, and reject new work before acknowledgment when full. A delivery quota reached after
intake pauses routing with the event and cursor intact. Size capacity for that retention policy;
do not delete evidence still needed for routing, delivery, source recovery, or durable Tool replay.

### GitHub workflow run completion

Import GitHub integration from `@effect-agent/session/github`. The session root exports generic
subscription contracts and preparation bindings.

`makeGitHubWorkflowRunSource` watches one repository, workflow run ID, attempt number, and expected
head SHA. Both successful and unsuccessful completions reach the Agent. It does not aggregate all
CI checks for a commit. The host owns repository authorization, credentials, webhook setup, and the
binding from repository identity to source partition.

`acceptVerifiedGitHubWorkflowRunWebhook` verifies raw bytes before parsing a `workflow_run` delivery
with action `completed`. Webhook and exact-attempt API observations normalize to one logical
completion identity and canonical payload. A webhook delivery ID alone is insufficient for this
deduplication. See GitHub's [workflow_run payload](https://docs.github.com/en/webhooks/webhook-events-and-payloads#workflow_run)
and [exact workflow run attempt endpoint](https://docs.github.com/en/rest/actions/workflow-runs#get-a-workflow-run-attempt).

Registration durably arms source reconciliation before any provider read. An already-completed
attempt can therefore notify a newly registered watch without a check-then-subscribe race. A
register-then-check catch-up may atomically select that one later watch against a retained matching
completion, even after the ordinary cutoff. It never reroutes the event to other subscriptions.
Cancellation, expiry, and once selection stop provider polling; preparation and admission then
recover without further GitHub reads.

GitHub [does not automatically redeliver failed webhooks](https://docs.github.com/en/webhooks/using-webhooks/handling-failed-webhook-deliveries).
Reconciliation covers a missed completion while the exact attempt remains retained, readable, and
authorized. It is a state check for registered watches, not historical event replay. Provider failures
never invent completion. Generic source durability starts at accepted intake; trusted callers should
register before triggering work unless the source supplies register-then-check or a durable cursor.

## Next steps

- [Persistence and durability](../concepts/durability) defines the contract these operations
  administer.
- [Certify storage adapters](./certify-adapters) explains how a third-party adapter proves the same
  invariants.

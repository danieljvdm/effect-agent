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
idempotency key. Transient or ambiguous failure stays pending. Permanent refusal requires proof
that admission did not occur and the unchanged request cannot succeed.

Recurring downtime coalesces to the latest due firing. Pause and cancel stop new preparation while
pending delivery continues. Cancel is irreversible. Revocation blocks future preparation but must
finish already authorized envelopes. Resuming a paused schedule skips missed recurring times.

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

::: warning Beta retention limit
Completed records consume quota for the partition's lifetime. There is no automatic pruning or
identity recycling. Size capacity for all retained events, registrations, and deliveries.
:::

Each event and registration belongs to one stable `SourcePartition` with a tenant ID and source
address. Keep the address unchanged across deployments and source versions. There is no
cross-partition transaction or global subscription directory.

Use `makeEventSource` and `EventSources` for versioned event schemas, identity, matching, and
optional reconciliation. Use `makeSubscriptionInputBinding` and `SubscriptionInputBindings` for
destination preparation. Keep old source versions and bindings installed while retained work
needs them. Missing or ambiguous bindings leave selected delivery pending as
`unsupported-binding`. Persisted records contain no callbacks, Schemas, Effects, credentials, or
captured services.

`SubscriptionAuthorizer` covers management, intake, reconciliation, and preparation. Recovery has
its own `reconcile` decision. Keep stores, intake, and drivers out of model tool environments.
Restricted tools must bind owner, agent, principal, source catalog, and thread in the host.
A host may also permit a deterministic fresh thread for each selected event.

Registrations cannot be edited, paused, or resumed. Cancel and create a new identity to change
configuration. Reusing a creation identity with the same fingerprint returns the retained
registration; conflicting reuse fails. An uncertain ordinary tool is never replayed automatically.

Intake deduplicates by tenant, source address, and logical event ID. Conflicting payload or source
version fails. Each event records a registration cutoff. Duplicate intake cannot move that cutoff
or reopen routing. Selection atomically advances the cursor, creates delivery obligations, and
consumes once registrations. Continuous subscriptions retain each event separately. Thread
admission order decides execution order.

Preparation rechecks authority, validates input, and freezes destination, principal, digests,
authorization metadata, and admission key. Cancellation, expiry, or revocation blocks new
preparation. Already prepared envelopes continue unchanged. Expiry never sends agent input.

Lost admission replies retry the exact envelope and key. Execution remains at least once. Public
status omits payloads, parameters, context, and credentials. Recovery uses bounded pages and
durable cursors so one corrupt record does not block unrelated work. Provider failures never
invent event completion.

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

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
`bun run admin:durable -- <explain|verify|retry|wake|obligations> --database <file>` is the CLI.
On `DC`, the Conversation Object exposes Schema-encoded entry points (`explainEncoded`,
`verifyEncoded`, `retryEncoded`, `obligationsEncoded`, `wake`); deployments reach them through
their own Worker. Every operation, including `observe`, `resolveUnknown`, and `resolveApproval`,
consults the `OperationAuthorizer` fail-closed: the default Layer preserves service-possession
behavior, and a host-supplied authorizer turns denials into the typed `OperationDenied` before
any read or write.

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

## Next steps

- [Persistence and durability](../concepts/durability) defines the contract these operations
  administer.
- [Certify storage adapters](./certify-adapters) explains how a third-party adapter proves the same
  invariants.
- [Security and operations specification](../spec/security-operations) contains the normative
  authorization and audit requirements.

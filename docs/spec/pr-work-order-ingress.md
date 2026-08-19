# Pull-request work-order ingress

Status: **Implemented**

This specification is the operational GitHub surface for
[pull-request work orders](pr-work-orders.md). The work-order specification owns the canonical
instruction, bounded implementer, and deterministic host semantics. This document owns exact
GitHub dispatch, persistent admission, Actions isolation, atomic publication, and thread
presentation.

The product is a separately named, precompiled JavaScript Action at `work-order-action/` used in a
five-job workflow. It is not exported from `@effect-agent/pr-review` or `action/`; those surfaces
remain read-only.

## 1. Product and authority

A human or bot may leave an inline review comment on one file. That comment is evidence only. An
authorized human supplies implementation authority by posting a new inline reply whose complete
body is exactly:

```text
@effect-agent fix this
```

The platform `in_reply_to_id` must identify one existing, path-bearing review comment. A top-level
review body, PR conversation comment, pathless comment, approximate command, edited source anchor,
or ambiguous target is rejected. The live Action does not admit reactions or general inline Q&A.

The dispatching actor is authorized by stable numeric GitHub user id. Login and source authorship
are diagnostic metadata, never principals. The source may be a human comment or a read-only review
finding; reviewer output does not transfer authority.

The first operational version accepts only a same-repository, non-fork pull request whose source
comment is anchored to its still-current head. One explicit dispatch authorizes one immutable work
order and one consumed attempt. A new attempt requires a new explicit dispatch reply.

## 2. Authentication and work-order construction

The admission job runs only for the trusted `pull_request_review_comment: created` Actions event.
It reads the event from `GITHUB_EVENT_PATH`, requires the repository/event identity supplied by
Actions, and derives `dispatch.eventId` from the stable dispatch comment id:
`review-comment:<comment-id>`. GitHub redelivery or workflow rerun therefore retains the same event
identity; a second human reply has a different identity.

Before admission, trusted base code:

1. decodes the bounded event and requires the exact command and `in_reply_to_id`;
2. authorizes `sender.id` against the configured stable actor-id set;
3. reloads the target review comment and pull request through GitHub;
4. requires the configured repository and pull-request number, same base/head repository, and
   `headIsFork === false`;
5. normalizes the inline source path and bounds the comment/range snapshot; and
6. requires `source.commitSha === current headSha` before constructing the canonical work order.

The work-order id and digest cover the full immutable snapshot. The source body and any suggestion
remain untrusted input. No Schema-valid field is self-authorizing.

## 3. GitHub-retained admission journal

The repository-appropriate persistent attempt record is one bot-authored reply on the selected review
thread. Before any model invocation, admission creates that reply with a bounded visible pending
status and a hidden Schema-encoded journal state authenticated by HMAC-SHA-256. The marker binds:

- version, repository, pull request, source comment, and stable event id;
- work-order id and digest;
- expected head;
- owning Actions run id; and
- either `claimed` or the typed terminal outcome.

Admission accepts a marker only when both its HMAC and the reply author's configured stable GitHub
actor id match. The workflow concurrency key serializes deliveries by repository id and dispatch
comment id. If an authenticated marker already owns that event/work-order identity, admission
returns its stored `claimed` or terminal outcome and sets `should-run=false`; no implementer job is
started. Multiple matching authenticated markers fail closed. Admission proceeds only when the
GitHub create response echoes the exact authenticated claim, bot actor id, and source-thread target.

This GitHub flow is not an Effect Agent DN or DC assembly and makes no DN/DC guarantee.
Its recovery boundary is GitHub's retention and availability of review comments: admission is
recovered by rereading that authenticated external repository state across Actions jobs and runs.

The presenter updates that same reply from `claimed` to `completed`; it does not add a second status
reply. A crash after claim leaves an authenticated incomplete attempt. While the journal reply is
retained, rerunning or redelivering it returns the stored incomplete state and never replays the
implementer. A new explicit dispatch creates a distinct work order and journal reply.

The journal reply is also the only claim record. A principal with pull-request write authority can
delete it, and a rerun after that deletion admits a fresh attempt, exactly as that principal could
authorize a new attempt with a new explicit reply. Replay prevention is therefore scoped to GitHub's
retention of the reply and to already-trusted write-authorized principals — it is not a defense
against them — and publication stays fenced by the expected-head compare-and-swap either way.

Actions artifacts carry bounded Schema-decoded envelopes between jobs in one run, but they are not
the persistent duplicate-delivery authority. Artifact retention can expire without reopening an
attempt. The HMAC secret must remain stable; rotation must retain the old secret until all claimed
attempts are settled or intentionally abandoned.

## 4. Job and credential isolation

The reference workflow declares `permissions: {}` globally and grants each job only its explicit
scope:

| Job         | Holds                                                       | Does not hold / execute                                               |
| ----------- | ----------------------------------------------------------- | --------------------------------------------------------------------- |
| `admit`     | contents read, PR write, journal HMAC secret                | model key, PR code, checks, contents write                            |
| `implement` | contents read, one provider key, exact-head checkout        | GitHub write token, shell/process tool, repository-code execution     |
| `checks`    | contents read, exact-head checkout, pinned check image      | provider key, GitHub write token, journal secret, publisher artifacts |
| `publish`   | contents write, PR read, journal HMAC secret, checked files | provider key, PR checkout, repository-code execution                  |
| `present`   | contents read, PR write, journal HMAC secret                | provider key, PR checkout, repository-code execution                  |

The implementer exposes only Schema-defined read/search/exact-edit/inspect/check-request tools over
a scoped detached worktree. In deferred Actions mode it cannot run checks; it returns an empty
check claim and the host collects the complete patch, digest, and changed paths. It has no shell,
network tool, GitHub capability, or publication capability. The Action runtime alone calls the
configured model provider.

The checks phase first installs locked dependencies with lifecycle scripts disabled in a
networked setup container that can see only the exact-head worktree and an ephemeral tool-runtime
directory. The host then restores a separate copy of that validated patched worktree and tool
runtime for each required check. Each required host-configured command is executed without a shell
in a fresh container from that same immutable image with:

- no network;
- read-only container root;
- all Linux capabilities dropped and `no-new-privileges`;
- bounded processes, CPU, memory, output, and timeout;
- only the exact-head worktree and ephemeral tool-runtime directory mounted read/write.

Required-check containers have no network and cannot see sibling artifact state,
provider/publisher credentials, the Docker socket, or the host filesystem outside those two
mounts. A check therefore cannot alter a later check's worktree, dependencies, or runtime cache.
Scoped finalizers forcibly remove every setup/check container and copied runtime directory on
success, failure, timeout, or interruption. The host rejects a dirty post-install checkout, a
patch that does not reproduce exactly in every restored worktree, false/missing check evidence, a
failed check, or any check mutation of the accepted patch.

## 5. Independent publication

The publisher never checks out or executes pull-request code. It first re-reads the bot-authored
journal reply and verifies its HMAC, author id, source reply target, repository/pull/head identity,
event id, work-order id/digest, and owning run. This prevents an untrusted cross-job artifact from
creating publication authority.

It then independently:

1. recomputes the canonical work-order id and digest;
2. reauthorizes the dispatch actor id from publisher configuration;
3. normalizes the source and fixed support-path allowlist;
4. parses every complete `diff --git`, source, and destination path pair;
5. rejects additions, deletions, renames, copies, binary patches, mode changes, path escapes,
   duplicate/ambiguous headers, and any path outside the allowlist;
6. recomputes the patch digest and compares patch paths with proposal paths and checked files;
7. requires the exact configured check-name set with every result `passed`;
8. requires existing non-executable regular tracked UTF-8 files within the size bound and
   reproduces the exact patch/final content from GitHub's expected-head blobs; and
9. reloads the pull request and repeats same-repository/non-fork provenance and expected-head
   checks.

Publication uses GitHub GraphQL `createCommitOnBranch` with `expectedHeadOid=headSha` and the full
accepted file set. That mutation is the compare-and-swap; a read followed by an unconditional push
is forbidden. A lost race is `StalePullRequestHead`. When GitHub does not confirm the mutation, the
publisher re-reads the head: movement is stale-head, while an unchanged or unreadable head is
`PublicationUncertainty`. No exactly-once external-effect claim is made and failures are never
automatically replayed.

## 6. Presentation

The presenter updates the one authenticated admission reply with bounded host-authored text:

- **published** — confirmed new head plus host-verified changed paths;
- **settled** — `not-applicable` or `needs-human`, with no publication; or
- **failed** — the typed error tag.

Raw model prose is not copied as authoritative status. A missing terminal artifact is typed
`AttemptIncomplete` before publisher eligibility and conservatively becomes
`PublicationUncertainty` after validated check evidence made publication eligible.
`PublicationUncertainty` is rendered as unconfirmed publication that requires head inspection,
never as confirmed non-publication. The source thread remains open; no phase calls a
thread-resolution API. Presentation succeeds only when the GitHub update response echoes the same
journal comment, bot actor, source-thread target, response body, and exact authenticated terminal
state — or when the reply already carries that exact authenticated terminal state from an earlier
attempt whose acknowledgement was lost. A conflicting completed state fails closed.

## 7. Trusted workflow and consumer surface

Every job checks out the pull request's base SHA into a separate trusted Action directory with
`persist-credentials: false`. Only `implement` and `checks` also check out the exact head SHA, also
without persisted credentials. Publisher and presenter never check out the head. External Actions,
the check image, and downstream uses of `work-order-action/` are pinned to immutable digests or full
commit SHAs.

This prevents a pull request from changing the code that authenticates, authorizes, publishes, or
presents its own work order. Downstream repositories adopt the separate `work-order-action/`
surface through the documented multi-job workflow. Installing or upgrading
`@effect-agent/pr-review` does not enable work orders.

## 8. Deterministic proof obligations

The committed suites preserve:

- exact target parsing, stable-id authorization, same-repository provenance, and stale-anchor
  rejection;
- deterministic identity, persistent duplicate/incomplete settlement, and distinct new dispatches;
- Schema/model `E` and `R` proofs and absence of unknown error widening;
- worktree finalization on success, expected failure, timeout, interruption, and release failure;
- false/failed/mutating checks and changed-path rejection;
- patch escape, rename, copy, addition, deletion, binary, mode, and incomplete-header rejection;
- digest, identity, required-check, stale-head, compare-and-swap, and publication-uncertainty
  rejection;
- HMAC journal tamper rejection and one non-resolving host reply; and
- the enabled workflow's trusted-base checkouts, immutable pins, exact five-job permissions,
  per-job secret set, exact-head isolation, and networkless check container shape.

Live GitHub network calls are not made from deterministic CI tests.

## 9. Out of scope and limits

- fork pull requests and foreign head repositories;
- reactions, general inline Q&A, PR-conversation instructions, labels, and broad sweeps;
- automatic dispatch on reviewer output or review creation;
- multiple source comments or dynamically model-selected support paths in one work order;
- file additions/deletions, renames/copies, binary/executable changes, symlinks/submodules, or mode
  changes;
- checks that require network access or files outside the checked worktree;
- automatic thread resolution or closed-loop re-review;
- replay prevention against write-authorized principals who delete the bot journal reply; and
- exactly-once GitHub publication.

## 10. Rejected alternatives

- **Reviewer output as authority.** Human dispatch is the only implementation authority.
- **One job holding provider and write credentials.** The operational boundary is job separation.
- **Runner-local or artifact-only admission.** Neither survives independent workflow runs; the
  authenticated repository journal does.
- **Running checks directly on the hosted runner.** Untrusted checks must not see sibling trust
  artifacts or host credentials.
- **Publisher trusts the implementer/check artifact.** It authenticates admission and repeats every
  publication decision.
- **Read-then-push publication.** GitHub must compare the expected head atomically.
- **A work-order API on the reviewer Action.** The products retain separate authority and surfaces.
- **Resolving the thread on `fixed`.** Presentation reports an outcome; humans own resolution.

## 11. Requirements

- **WOI-001**: Actions authentication and the exact command uniquely identify one inline,
  path-bearing review comment.
- **WOI-002**: Only configured stable actor ids authorize; source authorship, login, and prose do
  not.
- **WOI-003**: Admission and publication require the configured same-repository, non-fork pull
  request.
- **WOI-004**: The source anchor and current head match at construction; publication atomically
  compares that expected head again.
- **WOI-005**: The existing bounded implementer runs for the immutable canonical work order;
  `not-applicable` and `needs-human` remain valid no-publication settlements.
- **WOI-006**: One authenticated GitHub-retained journal claim exists per explicit dispatch. Duplicate or
  interrupted delivery returns stored state, never invokes another implementer, and never replays
  automatically.
- **WOI-007**: Model, untrusted checks, publisher, and presenter run in separate least-privilege jobs;
  repository code executes only in the networkless bounded check container.
- **WOI-008**: Publisher configuration and the authenticated journal independently bind actor,
  identity, digest, complete allowed paths/files, required-check evidence, and expected head.
- **WOI-009**: `createCommitOnBranch(expectedHeadOid)` is the atomic publication compare-and-swap;
  stale-head and uncertain outcomes remain typed and make no exactly-once claim.
- **WOI-010**: One bounded host-authored reply records pending and terminal state without resolving
  the source thread or treating model prose as status.
- **WOI-011**: Every enabled phase runs trusted base Action code, and untrusted head code cannot
  modify its own authorization or publisher.
- **WOI-012**: `work-order-action/` is a separate commit-pinnable consumer surface;
  `@effect-agent/pr-review` stays read-only.
- **WOI-013**: Expected authentication, admission, model, validation, check, publication,
  presentation, timeout, interruption, and cleanup failures remain typed in `E`; dependencies stay
  visible in `R`; acquired worktrees and check containers are scoped.

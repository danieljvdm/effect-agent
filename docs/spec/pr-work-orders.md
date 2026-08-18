# Pull-request work orders

Status: **Implemented**

A **work order** is one immutable, head-bound, path-scoped instruction admitted from an explicit
human dispatch on a pull request. A distinct **implementer** may propose a patch. Deterministic
**host** Effect code owns admission, attempt policy, resource scope, validation, and publication.

This specification intentionally supersedes the earlier reviewer-coupled
review → remediation → re-review loop. The useful primitive is an independently authorized work
order, not an automatic transfer of authority from a reviewer to an implementer. The packaged
`@effect-agent/pr-review` reviewer remains read-only and exports no remediation handoff, edit, or
publication authority.

## 1. Product

Review findings and pull-request comments are observations, not authorization. A work order exists
only after an authorized human explicitly dispatches one path-bearing review comment. In the first
version, dispatch is either:

- a fixed mention command, such as `@effect-agent fix this`, in a platform-linked reply that
  unambiguously selects one inline review comment; or
- a configured reaction that directly targets one inline review comment.

If the transport cannot prove which single inline comment the mention or reaction targets, it must
reject the dispatch rather than infer a target from conversation prose.

The source comment may have been authored by a human, the packaged reviewer, or another bot. The
dispatching human supplies authority; the source author and comment contents do not. Automatic
dispatch on every comment is forbidden.

One work order addresses one source-comment snapshot on one repository path at one exact pull-
request head. It is useful independently of any particular reviewer. A later label or sweep may
enumerate comments and emit several independent work orders, but it must not turn a whole pull
request into one broadly scoped implementation instruction.

The host may publish one validated patch or settle without publication. A resulting pull-request
head may be observed later by ordinary CI or an independent reviewer, but re-review is not a nested
stage or success condition of this pipeline. The implementer never grades its own work.

## 2. Canonical work order

The host decodes external input and constructs one bounded, Schema-validated immutable work order.
Schema validity does not authorize it: ingress and host policy must separately authenticate the
dispatch, authorize its actor, establish same-repository provenance, and bind the current head.

The canonical value contains:

| Field                                        | Role                                                                |
| -------------------------------------------- | ------------------------------------------------------------------- |
| `version`                                    | Work-order schema version                                           |
| `workOrderId`                                | Host-issued identity for this immutable admitted dispatch snapshot  |
| `repository`, `pullRequestNumber`, `headSha` | Exact target                                                        |
| `source.commentId`, `source.threadId`        | Stable source identities; thread identity is optional when absent   |
| `source.authorId`, `source.authorLogin`      | Source authorship for audit, never dispatch authority               |
| `source.commitSha`                           | Commit against which the inline source comment was anchored         |
| `source.path`                                | Normalized repository-relative path named by the inline comment     |
| `source.lineRange`                           | Optional `{ startLine, endLine }` with `startLine <= endLine`       |
| `source.body`                                | Bounded, untrusted comment snapshot                                 |
| `source.suggestion`                          | Bounded, untrusted optional suggestion snapshot                     |
| `dispatch.kind`                              | `mention` or `reaction`                                             |
| `dispatch.eventId`                           | Stable delivery/event identity used for duplicate-delivery handling |
| `dispatch.actorId`, `dispatch.actorLogin`    | Human principal that supplied authority                             |

Stable platform IDs, not mutable display logins, participate in authorization and identity.
Logins are retained only for diagnostics and audit presentation.

`workOrderId` identifies the complete admitted snapshot, including the dispatch event and exact
source-comment contents. Duplicate delivery of the same dispatch derives the same identity;
another explicit dispatch derives a new identity. The host computes a canonical
`workOrderDigest` over the complete work order. The implementation report must name that digest so
an edited comment, changed path, stale model settlement, or replayed result cannot be mistaken for
the admitted instruction. If a production work order crosses a trust boundary, it must travel in
authenticated transport or trusted durable storage; a digest alone is not authentication.

`source.body` and `source.suggestion` are evidence. The host never applies a suggestion as a patch.
The implementer must inspect the exact target head and may reject the premise of the instruction.
The conservative first version requires `source.commitSha === headSha`; a comment anchored to an
older head must be explicitly re-established on the current head before dispatch.

The run allowlist is the normalized source path plus a bounded set of host-configured support
paths fixed before model execution. Support paths are repository policy, not model output. A
general pull-request conversation comment, label, or mention has no path and cannot directly form
this work-order type.

## 3. Dispatch and attempt admission

The first version admits only a trusted same-repository pull request. The host must verify:

1. the dispatch event is authentic and its actor satisfies configured repository policy;
2. the event unambiguously targets an inline, path-bearing review comment on the configured pull
   request;
3. the pull request is not a fork and its repository identity matches the configured repository;
4. the source comment was anchored to `headSha` and the current pull-request head still equals that
   SHA before the attempt is claimed; and
5. the normalized source path, line range, and configured support paths satisfy the workspace
   policy.

No field inside the work order is self-authorizing. In particular, a claimed login, a mention in
untrusted prose, and a Schema-valid `dispatch.kind` are not proof of human authority.

One admission attempt exists per
`(repository, pullRequestNumber, headSha, workOrderId)`. Claiming is atomic. Once claimed, success,
model failure, validation rejection, check failure, interruption, or resource failure consumes the
attempt; the host never retries automatically. A later attempt requires another explicit human
dispatch, which creates a new work order. Duplicate delivery of the original event must return the
already-claimed outcome rather than invoke another implementer.

The trusted-local proof may use an in-memory attempt policy and construct admitted work orders in
process. The GitHub Action uses the authenticated durable journal and job serialization specified
in [work-order ingress](pr-work-order-ingress.md); that operational surface, rather than this local
attempt policy, owns cross-job and cross-run duplicate delivery.

## 4. Outcomes

The implementer returns one Schema-validated report bound to `workOrderDigest` and `headSha`. It
must choose exactly one disposition:

- **`fixed`** — proposes a non-empty patch and reports the host-observed changed paths, patch
  digest, and named-check results;
- **`not-applicable`** — explains why the instruction is incorrect or already satisfied and
  proposes no patch; or
- **`needs-human`** — explains the ambiguity, missing authority, or decision that prevents a safe
  implementation and proposes no patch.

`not-applicable` and `needs-human` are successful non-publication settlements, not implementation
failures. They keep the GitHub thread open for humans. The host must verify that their worktree has
no patch. A patch attached to either disposition is rejected.

`fixed` is only a proposal. The disposition, model prose, reported checks, and patch digest never
authorize publication. The host independently validates all publication conditions below. An
empty `fixed` patch is rejected.

The host-facing result distinguishes a published work order, a valid non-publication settlement,
and a typed failure. It must never report publication merely because the implementer settled.

## 5. Authority

Three principals remain distinct:

- **Reviewer** — optional and separate. Read-only. It may post comments that later become untrusted
  source evidence after human dispatch. It exports no work-order, handoff, edit, or publish API.
- **Implementer** — receives the work order, its digest, and host policy metadata such as required
  check names. Its complete model-visible authority is five Schema-defined tools on a scoped
  detached worktree: read an allowed file, search allowed files, perform an exact-string edit in
  an allowed file, inspect the host-collected patch, and request a named host-configured check. It
  receives no GitHub credential, provider secret, unrestricted process or shell, push tool, or
  publication capability.
- **Host** — authenticates and authorizes ingress, constructs and claims the work order, acquires
  and releases the worktree, collects the patch and digest, validates the report, compares
  model-reported checks with host-observed check-tool results, reruns required checks, and owns
  compare-and-swap publication.

The implementer's only instruction is the canonical work order. Required check names and other
host policy metadata may accompany it in a Schema-defined mission, but must not broaden its file,
process, credential, network, or publication authority.

Publication does not resolve the source GitHub thread. Thread replies, status presentation, and
resolution are separate host/user-interface concerns and must not be inferred from model prose.

## 6. Deterministic host algorithm

1. Authenticate the dispatch and authorize the actor, repository, pull request, comment source,
   path policy, and exact current head.
2. Decode the canonical work order, compute its digest, and atomically claim the attempt key.
3. Acquire a scoped detached worktree at `headSha` with only the source path and configured support
   paths exposed to model-visible tools.
4. Run the distinct implementation Agent with the Schema-defined mission.
5. Decode and validate the report. Reject a work-order digest/head mismatch or model-reported
   check result that differs from the host-observed check-tool result.
6. Independently collect the patch, changed paths, and digest.
7. For `not-applicable` or `needs-human`, require an empty patch and settle without publication.
8. For `fixed`, require a non-empty patch, exact reported/observed path and digest agreement, and
   no path outside the allowlist.
9. Rerun every required named check in host-owned execution. The local proof runs checks inline;
   the GitHub flow transfers a bounded proposal to a credential-free, network-disabled container
   job. Reject a failed check or a check that mutates the validated patch.
10. Re-read the pull-request head and atomically publish only if it still names `headSha`. In the
    GitHub flow an independent publisher first authenticates the durable admission journal and
    repeats identity, digest, complete-path, allowlist, file, and check-evidence verification.
11. Release the worktree and all temporary resources on success, expected failure, defect,
    timeout, or interruption.

Resource release failures remain typed. A release failure may be observed after compare-and-swap
publication has already succeeded. Such an outcome must report the publication uncertainty and
current observed head; operators reconcile it against host state and must not blindly retry. No
exactly-once external publication claim is made.

## 7. Security and deployment posture

`examples/pr-work-orders` remains deployment class E trusted-local evidence: it constructs
authorized work orders in process, uses a local Git repository, and proves the semantic host
boundary. `examples/pr-work-order-ingress` and the separately named `work-order-action/` add the
operational GitHub transport, durable repository journal, isolated checks, and network publisher
without changing the local proof's deployment claim.

Pull-request code and repository checks are untrusted. No process that executes them may hold a
GitHub write token, model-provider secret, or other publishing credential. A production adapter
must isolate checks from the model/provider process and from the credentialed publisher. The
publisher receives only host-validated bounded artifacts and metadata, independently verifies the
patch digest, required-check evidence, allowed paths, work-order identity, and expected head, and
then performs compare-and-swap publication.

The model never receives GitHub credentials, provider publication capabilities, arbitrary
publishing authority, or an unrestricted push tool. Schema validation, model settlement, a source
comment, or a suggestion cannot substitute for authentication or authorization. The enabled
workflow runs authorization and publication from trusted base code, admits no fork, and separates
model, untrusted checks, publisher, and presenter into jobs with least-privilege credentials.

## 8. Leaf proof and retained substrate

The executable proof belongs in a private leaf workspace under `examples/pr-work-orders`. It may
depend inward on existing public framework packages and `@effect-agent/testing`, but it must not
create a new public framework package merely for symmetry.

Private development has no compatibility obligation to the discarded loop. Existing
reviewer-handoff Schemas, authenticators, exports, finding-accounting code, loop orchestration,
tests, and documentation must be removed rather than deprecated or adapted into pass-through
abstractions. The old example name does not constrain the new leaf name.

The proof retains the generally useful implementation substrate from the discarded loop design:

- the separate bounded implementation Agent and its five model-visible tools;
- scoped worktree acquisition and typed cleanup;
- normalized path allowlisting and host-configured support paths;
- host-collected patch paths and digest;
- model-observed versus host-observed named-check comparison;
- independent required-check reruns and patch-mutation detection;
- exact-head compare-and-swap publication;
- one-attempt admission; and
- deterministic real-Git adversarial evidence.

These mechanisms are retained because the work-order product independently requires them, not for
compatibility with the earlier reviewer-coupled design.

## 9. Deterministic proof obligations

Committed tests protect stable host capabilities through the public leaf seam, not internal file
structure. The smallest sufficient deterministic real-Git suite must demonstrate:

- a `fixed` work order produces one independently validated commit on the expected head;
- `not-applicable` and `needs-human` settle with no commit and reject any accompanying patch;
- malformed, absolute, traversal, control-character, or otherwise escaping paths never publish;
- a changed path outside the source/support allowlist never publishes;
- a stale or replayed work-order digest, edited source snapshot, mismatched head, false check
  claim, failed required check, or check-mutated patch never publishes;
- a source comment anchored to an older commit is not admitted against the current head;
- a head that moves before compare-and-swap never publishes the stale patch;
- duplicate delivery and a second run of the same claimed work order do not invoke another
  implementer, while a new explicit dispatch has a distinct identity;
- interruption and timeout release the scoped worktree and do not publish;
- release failures remain typed, including the distinguishable post-publication case; and
- compile-time evidence preserves the implementation Agent's expected `E` and visible `R` while
  excluding workspace authority already provided at the composition boundary.

Tests must use deterministic scripted model behavior derived from model-visible requests, avoid
wall-clock sleeps, and assert externally observable branch, patch, check, settlement, and cleanup
behavior. They must not test GitHub parsing or production isolation that the proof does not
implement.

## 10. Out of scope

GitHub dispatch, durable production admission, isolation, network publication,
and thread presentation are specified in
[work-order ingress](pr-work-order-ingress.md). They are out of scope for this
class E proof:

- GitHub webhooks, mention parsers, reactions, Actions, and network publication;
- automatic dispatch on review creation or every comment;
- general PR-conversation instructions without an explicit path;
- one implementation attempt spanning multiple comments or paths;
- a label or sweep that grants one broad patch authority;
- a synchronous closed-loop review → implement → re-review workflow;
- reviewer-produced remediation handoffs, HMAC review handoffs, and finding multisets;
- automatic GitHub thread resolution;
- a new framework package;
- durable production attempt admission; and
- production execution of untrusted pull-request code.

## 11. Rejected alternatives

- **Reviewer output as implementation authority.** A finding is evidence, not authorization. Human
  dispatch creates the work order.
- **Bot-to-bot remediation as the product.** It couples discovery, authority, and verification and
  creates ping-pong risk. Independent review or CI may observe a published head later.
- **A public remediation handoff in `@effect-agent/pr-review`.** Read-only reviewer output may be
  rendered as comments; remediation does not require a reviewer-owned transport or authenticator.
- **A monolithic remediation-loop coordinator.** The work-order host ends after a validated
  publication or non-publication settlement. Downstream review remains an independent workflow.
- **Automatic execution of suggestions.** Comment bodies and suggestions are untrusted evidence;
  the implementer inspects the current head and proposes its own patch.
- **Auto-run on every review comment.** Comments are conversation. Explicit authorized dispatch is
  admission.
- **PR-wide label as the first trigger.** A label does not identify one path-scoped instruction.
  Future label/sweep ingress may emit independently claimed work orders for explicitly selected
  comments.
- **Keeping the earlier design because it exists.** Reviewer handoffs, finding accounting, and
  built-in re-review are removed. Only substrate independently required by work orders remains.

## 12. Requirements

- **WO-001**: One immutable Schema-validated work order binds the repository, pull request, exact
  target and source head, source-comment snapshot, normalized path/range, and explicit human
  dispatch under a canonical digest.
- **WO-002**: The host authenticates and authorizes the dispatch actor and trusted same-repository
  provenance; no work-order field or comment prose self-authorizes.
- **WO-003**: Review, implementation, and publication authority stay separate; the implementer has
  no credential, unrestricted process/shell, provider secret, or publish tool.
- **WO-004**: One consumed attempt exists per
  `(repository, pullRequestNumber, headSha, workOrderId)`; duplicate delivery is idempotent and no
  automatic retry occurs.
- **WO-005**: `fixed`, `not-applicable`, and `needs-human` are distinct typed dispositions;
  non-fix dispositions require an empty patch and never publish.
- **WO-006**: For `fixed`, the host independently validates work-order/head identity, patch paths
  and digest, model-observed checks, required check reruns, and patch stability before publication.
- **WO-007**: Publication uses compare-and-swap against the still-current `headSha`; post-
  publication cleanup failure remains distinguishable and makes no exactly-once claim.
- **WO-008**: Worktrees and temporary resources are scoped and released on success, expected
  failure, defect, timeout, and interruption, with release failures kept typed.
- **WO-009**: The semantic host proof remains class E trusted-local in a private leaf; the
  operational GitHub product is a separately named, precompiled Action and enabled multi-job
  workflow, not a public framework package or reviewer export.
- **WO-010**: `@effect-agent/pr-review` remains read-only and exports no work-order, remediation
  handoff, implementer, edit, or publication surface.
- **WO-011**: Re-review and CI are independent downstream observers of a published head, not steps
  owned or graded by the work-order implementer.
- **WO-012**: Public operations preserve expected admission, validation, check, stale-head,
  publication, and release failures in `E`, keep construction requirements visible in `R`, and
  acquire every worktree and temporary resource in `Scope`.

# Pull-request work-order ingress

Status: **Draft**

This specification is the GitHub-facing follow-up to
[pull-request work orders](pr-work-orders.md). That document owns the canonical
work order, the jailed implementer, and the deterministic host algorithm. This
document owns how a human dispatch on GitHub becomes that work order, how
production attempt admission stays durable, how checks and credentials stay
isolated, and how a validated patch is published back to the pull request.

The class E leaf `examples/pr-work-orders` remains the host proof. It does not
implement this ingress. No enabled GitHub workflow may run an implementer until
the isolation and credential-separation requirements below are met.

## 1. Product

A reviewer, human or bot, leaves an **inline** comment on one file. An
authorized human then **dispatches that one comment**. Dispatch is:

- a reply whose body is exactly the configured mention command, default
  `@effect-agent fix this`, and whose platform `in_reply_to` identity names
  that inline comment; or
- the configured reaction on that inline comment.

The transport must prove which single inline, path-bearing review comment was
targeted. If it cannot — a top-level review body, a PR conversation comment, a
mention without `in_reply_to`, or more than one candidate — it rejects the
event. It never infers a target from prose.

The dispatching human supplies authority. The source comment's author does not.
Automatic dispatch on review creation or every comment is forbidden.

One accepted event becomes one work order, then one host attempt. A later
label or sweep, if added, must emit independently claimed work orders for
explicitly selected comments. It must not grant one PR-wide patch.

After the host settles, ingress may post a reply on the source thread. It must
not resolve the thread. Ordinary CI or an independent reviewer may observe a
published head. Re-review is not a stage of this pipeline.

## 2. Event to work order

Ingress authenticates the platform delivery, authorizes the actor, loads the
target comment and the current pull-request head from GitHub, and constructs
the canonical work order defined in [pr-work-orders.md](pr-work-orders.md) §2.

It must establish, before the host is invoked:

1. the delivery signature or Actions identity is authentic;
2. `dispatch.actorId` is a configured human principal, using the stable
   platform user id, not the login;
3. the event targets one inline review comment on the configured repository
   and pull-request number;
4. that comment has a normalized repository-relative path;
5. the pull request is not a fork and its repository identity matches
   configuration;
6. `source.commitSha` equals the current `headSha`; otherwise reject and tell
   the dispatcher to re-establish the comment on the current head;
7. the mention command or reaction matches configuration exactly.

`dispatch.eventId` is the platform delivery id. Duplicate HTTP or Action
retries of the same delivery keep that id. A second human dispatch is a new
event id and therefore a new work order.

No field copied from the comment, and no Schema-valid `dispatch.kind`,
authorizes the run. Authorization is the authenticated actor plus repository
policy.

## 3. Durable admission

Production ingress must persist the attempt key
`(repository, pullRequestNumber, headSha, workOrderId)` and the terminal
host outcome. Claiming is atomic. Duplicate delivery returns the stored
outcome and must not invoke another implementer.

The in-memory policy in `examples/pr-work-orders` is not this store. A crash
after claim and before a stored outcome still consumes the attempt: recovery
records a typed incomplete settlement and does not retry. A later fix requires
another explicit human dispatch.

## 4. Isolation and publication

Three processes stay separate. No process may hold credentials it does not
need.

| Process         | May hold                                      | Must not hold                                       |
| --------------- | --------------------------------------------- | --------------------------------------------------- |
| Ingress / model | authenticated event, model-provider secret    | GitHub write token                                  |
| Checks          | the detached worktree, host-configured checks | GitHub write token, model-provider secret           |
| Publisher       | GitHub write token, host-validated artifacts  | model-provider secret, unrestricted worktree access |

The publisher receives only the host-validated patch, digest, allowed paths,
required-check evidence, work-order identity, and expected `headSha`. It
independently re-verifies those facts, re-reads the pull-request head, and
updates the ref only if it still names that SHA. A lost race is
`StalePullRequestHead`. A successful update followed by a cleanup failure
reports publication uncertainty and the observed head. No exactly-once
publication claim is made.

Pull-request code and repository checks are untrusted. Until this isolation is
implemented and tested, the repository must not add an enabled workflow that
runs an implementer.

## 5. Presentation

Ingress may post one bounded reply on the source thread:

- **published** — names the new head and the changed paths the host observed;
- **settled** — names `not-applicable` or `needs-human` and the host-accepted
  summary;
- **failed** — names the typed host error.

The reply is host-authored. Model prose is not copied as status. The thread
stays open.

## 6. Placement

`@effect-agent/pr-review` and `action/` remain the read-only reviewer. Ingress
must not appear on that public surface.

The first executable proof belongs in a private leaf, for example
`examples/pr-work-order-ingress`. It may depend inward on public framework
packages, `@effect-agent/testing`, and the existing work-order host. It must
not create a new public framework package. A production Action or webhook
adapter is added only after the isolation proof, as a separate entrypoint, not
as a reviewer export.

## 7. Deterministic proof obligations

Tests use recorded GitHub event fixtures and a fake GitHub API. They do not
call the live GitHub network and they do not enable a repository workflow.

The suite must demonstrate:

- a mention reply with `in_reply_to` naming one inline path-bearing comment
  constructs one work order and invokes the host once;
- a reaction on that comment does the same;
- a mention without a unique inline target is rejected and does not invoke the
  implementer;
- a PR conversation comment, review summary, or pathless event is rejected;
- an unauthorized actor id is rejected even when the login matches a
  configured human;
- a fork or foreign repository is rejected;
- a comment anchored to an older SHA is rejected against the current head;
- duplicate delivery of the same `eventId` returns the stored outcome;
- a second explicit dispatch has a distinct work-order id;
- the check process environment contains neither a GitHub write token nor a
  provider secret;
- the publisher rejects a digest, path, or head mismatch and does not update
  the ref;
- a published or settled run posts one thread reply and does not resolve the
  thread.

## 8. Out of scope

- enabling a GitHub workflow before isolation evidence exists;
- admitting fork or untrusted pull requests;
- automatic dispatch on review creation or every comment;
- general PR-conversation instructions;
- one attempt spanning multiple comments or paths;
- a label or sweep that grants one broad patch;
- closed-loop review → implement → re-review;
- automatic thread resolution;
- a work-order API on `@effect-agent/pr-review`;
- live GitHub in CI.

## 9. Rejected alternatives

- **Inferring the target comment from mention prose.** Ambiguous targeting
  must fail. Platform linkage is the only selector.
- **Treating a PR conversation comment as a work order.** That comment has no
  path.
- **One workflow job that holds the model key and the GitHub write token.**
  Isolation is the enablement gate.
- **Publisher trusts the implementer-reported digest.** The publisher repeats
  the host's verification.
- **Resolving the GitHub thread on `fixed`.** Presentation is a reply, not
  closure.

## 10. Requirements

- **WOI-001**: Ingress authenticates the platform delivery and accepts a
  dispatch only when it uniquely names one inline, path-bearing review
  comment.
- **WOI-002**: Only a configured stable actor id authorizes dispatch; source
  authorship, logins, and comment prose do not.
- **WOI-003**: Ingress admits only a trusted same-repository, non-fork pull
  request whose identity matches configuration.
- **WOI-004**: Ingress requires `source.commitSha === headSha` at construction
  time; an older anchor is rejected.
- **WOI-005**: Ingress constructs the canonical work order and invokes the
  existing host; it does not widen implementer file, process, credential, or
  publish authority.
- **WOI-006**: Production admission is durable on
  `(repository, pullRequestNumber, headSha, workOrderId)`; duplicate delivery
  of `dispatch.eventId` is idempotent and never retries automatically.
- **WOI-007**: Checks, model/provider, and publication run in isolated
  processes with fail-closed credential separation.
- **WOI-008**: The publisher independently verifies patch digest, allowed
  paths, required-check evidence, work-order identity, and expected head
  before compare-and-swap.
- **WOI-009**: Network publication is compare-and-swap against the still-
  current `headSha`; post-publication uncertainty stays typed and makes no
  exactly-once claim.
- **WOI-010**: Presentation is one host-authored thread reply; the source
  thread is never resolved.
- **WOI-011**: No enabled workflow may run an implementer until isolation and
  credential separation are implemented and tested.
- **WOI-012**: `@effect-agent/pr-review` remains read-only; ingress is a
  separate leaf or Action entrypoint.
- **WOI-013**: Ingress operations keep expected authentication, targeting,
  admission, isolation, publication, and presentation failures in `E`, keep
  construction requirements visible in `R`, and acquire temporary resources
  in `Scope`.

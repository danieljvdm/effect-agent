# @effect-agent/pr-review

A small, provider-neutral review agent. One bounded model run receives every admitted patch as
numbered new and old hunks, reads immutable base or head source when needed, and returns findings
through a required native completion Tool. There is no voting, candidate cache, private hypothesis
handoff, or repository code execution.

The reviewer traces changed entry points and boundaries through downstream consumers, guards,
finite resources, transformations, effects, completion, and unchanged callees. A finding must have
a supported trigger, concrete terminal failure, causative changed edge, reachable impact, and a
cause-level repair checked against both a legitimate boundary input and excluded scope. Incremental
findings must be introduced, exposed, or materially affected by the exact delta; unrelated old bugs
and target-only changes remain out of scope, while explicit reverts remain reviewable.

The result contains model findings with host-validated paths and line anchors, exact duplicate
removal, aggregate usage, and optional host-priced cost. Unknown changed paths fail closed and
invalid inline anchors become top-level findings. Model output, usage, context, and retained response
bytes remain bounded. Public finding paths, titles, and bodies retain their 512, 200, and
2,000-character limits.

The reviewer does not resolve previous findings or declare a partial review safe
to merge. GitHub history, credentials, diff collection, and publication belong to
the host channel.

The engine allows eight research turns and one constrained final turn, with 128 tool calls.
Its cumulative token policy is 1,440,000 tokens: nine calls at a 128,000-token context plus the
Action's 32,000-token output allowance. It reserves 160,000 tokens for final delivery. The model
sees the usable research balance separately from that reserve and receives a warning before a
context the size of its previous call would consume the remaining research balance. Each model
call logs input, output, cumulative usage, and the remaining research balance, without source text.
Token, turn, or tool exhaustion permits one constrained completion through `submit_review`; it returns
validated findings and accounted usage with `ReviewOutcome.exhausted` naming the limit. Hosts must
treat that outcome as incomplete, even when it contains no findings. Measured usage can exceed a
policy threshold before the engine observes it; this is not a provider-side spending cap.
The usage ledger records research, compaction, and finalization without a second token limit that
could abort delivery. Duration and protocol failures still fail through the typed error channel.

```ts
const reviewer = makeReviewer({ model, guidance, estimateCostMicrousd });
const program = reviewer.review(request).pipe(Effect.provideService(ReviewRepository, repository));
```

`repository.readFile` and `repository.findFiles` return typed Effects. Hosts must
authorize the source sent to their model, enforce immutable revisions and read
bounds, and treat source content as untrusted data. The reviewer exposes this
dependency in its Effect requirements; it has no ambient filesystem or network
access and does not cache review answers.

`ReviewSource.fromText(request, text)` applies the shared line and character
bounds after the host authorizes and reads a file.

The numbered hunk presentation and portions of the review instructions are adapted from
[PR-Agent](https://github.com/The-PR-Agent/pr-agent). See `NOTICE` for its MIT license attribution.

# @effect-agent/pr-review

A small, provider-neutral review agent. Bounded model runs receive admitted patches as
literal unified diffs, read immutable base or head source when needed, record established
findings with `record_finding`, and return findings through a required native completion Tool.
There is no voting, candidate cache, private hypothesis
handoff, or repository code execution.

The native Agent `inputPrompt` projects each complete patch once, with literal newlines. It keeps
file headers, hunk ranges, additions, deletions, context, and mode or rename metadata. It does not
duplicate context into separate old/new views or JSON-escape the source for the model. The canonical
`ReviewRequest` and finding validation retain the original patches. This reduces repeated input
overhead without excluding changes; it does not guarantee a complete review within a spending cap.

The reviewer assesses every supplied patch first. Source reads resolve concrete questions about
plausible defects, such as a missing caller, guard, contract, or limit. It reuses supplied evidence
and finishes when those questions are resolved; straightforward changes can finish without source
tools. Reads prioritize implementation and owned boundary schemas over test examples, including
the definitions needed to resolve the question rather than only a nearby call site.
Findings explain a supported trigger, concrete impact, and needed correction. Changes that
expose an unchanged downstream failure remain eligible. Incremental findings must arise from the
exact delta; unrelated old bugs and target-only changes stay out of scope, while explicit reverts
remain reviewable.
Changes to collection membership, cardinality, or representation warrant checking affected
consumer limits with a supported boundary input, including transformations and aggregation.
New or moved resource acquisition warrants checking an early-failure sequence and its cleanup.
Owned untrusted-input and model-output Schema boundaries must safely handle every admitted value,
including adversarial values at the field and collection bounds.
Novelty compares base and head with the same supported operation input, including when a
previously failing helper becomes newly reachable.

The result contains model findings with host-validated paths and line anchors, exact duplicate
removal, aggregate usage, and optional host-priced cost. Unknown changed paths fail closed and
invalid inline anchors become top-level findings. Model output, usage, context, and retained response
bytes remain bounded. Public finding paths, titles, and bodies retain their 512, 200, and
2,000-character limits.

Hosts may supply up to eight `ReviewFollowUp` values containing complete prior feedback, each
bounded to 32,000 characters. The reviewer separately checks those blockers against current source
and may return `ReviewOutcome.resolutions` with their exact IDs and fixing evidence. Omitted or
uncertain resolutions leave prior feedback open. Unknown or duplicate resolution IDs fail
verification. Incomplete, exhausted, pending-path, or excluded-path results return no resolutions.
Follow-ups appear only in the final patch batch, sharing the existing execution and spending limits.
They do not expand new-finding scope or declare a partial review safe to merge.
GitHub history, credentials, selection, dismissal authorization, and publication belong to the host.

Without host spending admission, the engine owns a cumulative 416,000-token stop policy and
reserves 160,000 tokens for a final context and completion response. The model sees its current
turn, tool, and token usage. Token, turn, or tool exhaustion permits one constrained completion
through `submit_review`; it returns validated findings and accounted usage with
`ReviewOutcome.exhausted` naming the limit. Hosts must
treat that outcome as incomplete, even when it contains no findings. Measured usage can exceed a
policy threshold before the engine observes it; this is not a provider-side spending cap.
The usage ledger records research, compaction, and finalization without a second token limit that
could abort delivery.

An optional `costControl` reports the host's pre-request spending admission and provider usage.
Supplying it replaces the cumulative token quota and completion reserve with that admission.
Cached reads still contribute to usage diagnostics, but cannot force early token finalization.
Cost-admitted runs allow up to 64 turns, matching the 64-tool-call allowance, while retaining
the shared 5-minute and 128,000-token context bounds. Uncapped runs retain eight turns. A cost
estimator alone does not disable the token quota. Capped hosts own model-visible spending feedback
at their provider boundary; the generic turn/tool status is disabled for these runs. The Action
counts its outgoing spending status before admission and keeps it outside the reusable cache prefix.
With `costControl`, large requests run in sequential batches of at most 256,000 patch characters,
preserving the host's file order and keeping each patch complete. Each batch has a fresh context
and the same source service. All batches share the host ledger, turn and tool allowances, deadline,
and 24-finding capacity. They stop on an incomplete result or exhaustion. `pendingPaths` identifies
admitted patches never sent to a model, including a batch refused before paid inference. Hosts
must disclose those paths as unreviewed. Without `costControl`, the reviewer retains one run and its
cumulative token policy.
When the host stops research for cost, the reviewer returns `exhausted: "cost"` and delivers
recorded findings without requiring another paid call. Hosts must reserve the full possible charge
before sending each request; the port itself does not enforce a cap. The
[GitHub Action](../../action/README.md) supplies that implementation for its supported OpenAI models.
`reservedCostMicrousd` reports the maximum additional charge for requests whose usage is still
unknown, separately from the observed usage estimate.

Recorded findings also survive a later expected execution or verification failure. Such a result
has `incomplete: true`; hosts must not treat an empty or partial result as clearing the change.
The model can also set `incomplete: true` in `submit_review` when it cannot finish assessing the
supplied patches. That declaration preserves findings and becomes `ReviewOutcome.incomplete`
even when no engine limit or provider failure occurred. Host-tracked `unreviewedPaths` remain
disclosed separately and do not by themselves mark the admitted patches unfinished. An empty
complete result is not proof that the repository is defect-free.
With `costControl`, an accounted provider attempt also returns an incomplete outcome after an
expected failure, even without findings, so hosts can publish its usage and outstanding charges.
Otherwise failures without recorded findings remain typed. Defects and interruption still
propagate, and these records belong to the current run's Scope, not persistent storage.
The report retains recorded findings before adding newly submitted findings, removes exact
duplicates, and marks coverage incomplete if the combined report exceeds 24 findings.

```ts
const reviewer = makeReviewer({ model, guidance, estimateCostMicrousd, costControl });
const program = reviewer.review(request).pipe(Effect.provideService(ReviewRepository, repository));
```

`repository.readFile` and `repository.findFiles` return typed Effects. Hosts must
authorize the source sent to their model, enforce immutable revisions and read
bounds, and treat source content as untrusted data. The reviewer exposes this
dependency in its Effect requirements; it has no ambient filesystem or network
access and does not cache review answers.

`ReviewSource.fromText(request, text)` applies the shared line and character
bounds after the host authorizes and reads a file.

Portions of the review instructions are adapted from
[PR-Agent](https://github.com/The-PR-Agent/pr-agent). See `NOTICE` for its MIT license attribution.

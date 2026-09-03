# @effect-agent/pr-review

A read-only PR reviewer built on Effect AI. It reviews unified diffs, reads source at exact
base and head revisions, and returns findings with validated paths and line anchors.

The default strategy is `baseline`. Optional finding verification is experimental; the
[GitHub Action](../../action/README.md) still uses baseline.

## Use

Supply an Effect AI model, a `ReviewRepository` for authorized source access, and `Crypto.Crypto`
for candidate IDs. The host owns credentials, spending policy, and publication.

```ts
const reviewer = makeReviewer({ model, guidance, estimateCostMicrousd, costControl });
const program = reviewer
  .review(request)
  .pipe(
    Effect.provideService(ReviewRepository, repository),
    Effect.provideService(Crypto.Crypto, crypto),
  );
```

`ReviewRequest` supplies the patches, revisions, scope, and optional prior feedback.
Repository methods return Effects and must enforce immutable revisions and read limits.
`ReviewSource.fromText` applies the source bounds after the host reads a file.
The reviewer has no ambient filesystem access and cannot execute repository code.

## Results and incomplete reviews

`ReviewOutcome` contains the report, usage, diagnostics, and any incomplete or exhausted status.
The host rejects findings on unknown paths, converts invalid inline anchors to top-level feedback,
and removes exact duplicates. Findings recorded during research and at completion share a
24-finding limit; overflow makes the review incomplete.

Expected failures preserve recorded findings in an incomplete result. Otherwise failures remain
typed, except that cost-admitted attempts retain an incomplete outcome for accounting.
Defects and interruption propagate after cleanup.

Hosts must disclose excluded paths and `pendingPaths`, which lists patches never sent to a model.
An empty incomplete report cannot clear a change. "Patches supplied" counts input, not independently
verified coverage.

Optional `ReviewFollowUp` entries let the reviewer check prior blockers. Only a complete review can
return their IDs in `resolutions`, with fixing evidence. Omitted or uncertain resolutions leave
the blockers open; refuting a new finding does not resolve prior feedback.

## Spending and limits

`costControl` connects the reviewer to the host's spending admission. The host must reserve each
request's full possible charge before dispatch and retain uncertain charges. A cost estimator
alone does not enforce a cap. The Action supplies a [$0.999999 ledger](../../action/README.md#spending-and-prompt-caching).

With `costControl`, large inputs run in sequential batches of complete patches. All batches and
verification share one ledger, five-minute deadline, 64 turns, 64 tool calls, and the finding limit.
When spending stops, the reviewer returns recorded findings without another paid call.
Without `costControl`, it uses the engine's cumulative token policy and eight-turn limit.
See [`REVIEW_LIMITS` and `ReviewCostControl`](src/review.ts) for exact limits and accounting fields.

## Experimental verification

Set `strategy: "verified"` in `makeReviewer` to check every accepted finding after discovery.
One fresh verifier receives the original findings, diffs, guidance, and read-only source tools.
It receives no discovery transcript and cannot rewrite findings or change their severity.

| Decision   | Publication                                                           |
| ---------- | --------------------------------------------------------------------- |
| Supported  | Publish the original finding. Supported blockers may request changes. |
| Refuted    | Suppress the finding and retain it in diagnostics.                    |
| Unresolved | Publish visibly unverified feedback and mark the review incomplete.   |

Supported and refuted decisions need reasons and citations to supplied diffs or source actually
read at the exact revision. The host validates every decision before accepting completion.
Rejected submissions can be corrected within the same verifier's remaining budget and limits.
These checks establish access to evidence; they do not prove the conclusion correct.

Unavailable verification leaves findings unresolved. Verification cannot clear incomplete
discovery or missing coverage. Without supported blockers, unresolved feedback requires a comment
and an incomplete result. See the [evaluation guide](../../examples/pr-review-eval/README.md)
for the comparison and rollout criteria.

## Diagnostics

Diagnostics separate discovery's declared assessment, host completion, and verification status.
They record calls, usage, stop reasons, and up to 128 source activities with revisions, requested
and returned ranges, outcomes, and dropped-record counts. Activity contains no source text,
search queries, credentials, raw failures, or model reasoning.

Up to 24 original candidates remain available, including suppressed findings and their evidence.
Their IDs bind to the exact request; hashing failures stay typed as `ReviewVerificationError`.
The optional `onDiagnostics` Effect runs once at scoped finalization, including failure, defect,
and interruption. These records are transient and do not survive process termination.

Portions of the review instructions are adapted from
[PR-Agent](https://github.com/The-PR-Agent/pr-agent). See `NOTICE` for its MIT license attribution.

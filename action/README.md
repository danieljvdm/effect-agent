# Effect Agent PR Review action

This directory is the published GitHub Action path. It contains the consumer
contract in `action.yml` and the committed JavaScript bundle in `dist/`.

The private
[`@effect-agent/pr-review-action`](../packages/pr-review-action) workspace
owns the source and tests. The public
[`@effect-agent/pr-review`](../packages/pr-review) package remains provider-
and transport-neutral.

Rebuild the committed bundle with `vp run action:build`.

## Review behavior

One bounded review run receives the admitted diff and reads immutable base and head source as
needed. The host validates paths and RIGHT-side anchors and publishes against the inspected
head. A stopped run preserves findings recorded before research ended. A failure without recorded
findings publishes a failure marker. Blocking findings request changes and fail the Action after publication; other outcomes
remain comments and cannot clear an older change request.

Automatic waves use the configured limit, defaulting to two; zero disables automatic reviews.
Only trusted bot-authored terminal markers count. Failed attempts count but cannot become diff
baselines. An owner, member, or collaborator can request `@effect-agent review` for incremental
review or `@effect-agent review full` for the whole admitted diff. Manual waves do not consume the
automatic allowance. Missing baselines or incomplete comparisons stop incremental review instead
of silently expanding scope. The workflow runs trusted default-branch code, serializes attempts,
and refuses stale findings. If a push makes an attempt stale before publication, the Action logs
the inspected and current commits and posts only an incomplete notice bound to the inspected commit.
The stale attempt counts toward the automatic allowance, but cannot mark the new commit as already
attempted. The queued review can proceed while allowance remains. The attempt logs failure types;
budget failures include the exhausted limit and observed usage.
Failure comments on an unchanged head also report budget exhaustion without exposing provider
diagnostics or model output.

The reviewer sees its remaining research budget and reserved completion capacity. If token,
turn, tool, or cost limits stop research, the Action publishes established findings with an incomplete-coverage
warning and fails the check, including when no defects were found. Such an attempt cannot become
an incremental baseline or clear an earlier change request. This preserves useful findings without
claiming the full change was reviewed.

## Spending and prompt caching

Every review attempt has a fixed **$0.999999 admission ceiling**. The Action keeps the configured
model and reasoning effort, defaulting to `gpt-5.6-sol` and `xhigh`, and explicitly requests the
standard `default` service tier. It accepts only the priced GPT-5.6 family IDs and alias listed in
`action.yml`. The rate card was verified on 2026-08-30 and expires after 2026-11-21; refresh it before
then or later reviews fail closed. See [OpenAI pricing](https://developers.openai.com/api/docs/pricing).

Before each research, compaction, or completion request, the Action uses OpenAI's
[input-token counting endpoint](https://developers.openai.com/api/docs/guides/token-counting) on the
encoded input, tools, reasoning, and output-format settings. It rejects inputs above 128,000 tokens,
then reserves every input token at the cache-write rate plus the full output allowance, including
reasoning. Admission never assumes a cache hit. The ledger releases unused reservations only after
validating the response's usage, model, tier, and counted bounds. Failed, interrupted, or unmetered
requests retain their possible charge; the transport does not automatically retry them.

Research retains the 32,000-token maximum output allowance. When another research request cannot
fit, the Action permits at most one affordable request restricted to `submit_review`, with its
output allowance reduced to the remaining balance. It counts the changed final request again.
If even that request cannot fit, the host delivers already recorded findings with no further model
call. Both paths report incomplete coverage. Large changes may therefore remain incomplete under
the ceiling; the Action does not call a partial empty result a successful review.

This is a client admission guarantee under the pinned pricing and token-count contracts, not an
invoice audit or an OpenAI account spending limit. Usage estimates remain separate from outstanding
reservations. Each review's logs and footer show model calls, ordinary input, cache reads, cache
writes, output, cache-hit ratio, and estimated cost. Raw provider failure causes, credentials, and
repository source are excluded from the Action's diagnostics.

The Action uses explicit-only caching with a 30-minute TTL and a stable head-based routing key.
It marks reusable instructions, the diff, and completed tool batches before the ephemeral run-status
message, retaining earlier boundaries as history grows. Cache fields are added only at the native
Effect OpenAI client boundary; canonical history and provider encoding remain unchanged. This works
with the pinned Effect `4.0.0-rc.111` client, which serializes the additional request fields unchanged.
Compaction can change prefixes, and routing and cache availability still affect hits. See
[OpenAI prompt caching](https://developers.openai.com/api/docs/guides/prompt-caching).

Input admission allows at most 100 files, 256,000 aggregate patch characters, 80,000 characters per
patch, and 8 MB of hydrated base/head source. These bounds decide which files are supplied, not how
much inference may spend. Unavailable paths remain explicit coverage gaps.

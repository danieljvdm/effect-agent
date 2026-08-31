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

One bounded review run assesses every admitted patch before using immutable base and head source
to resolve specific questions about plausible defects. Straightforward changes can finish from
the diff; source tools are not an exhaustive repository audit. The host validates paths and
RIGHT-side anchors and publishes against the inspected head. A stopped run preserves findings
recorded before research ended. Preparation failures publish a failure marker. Blocking findings
request changes and fail the Action after publication; other outcomes remain comments and cannot
clear an older change request.

Automatic waves use the configured limit, defaulting to two; zero disables automatic reviews.
Only trusted bot-authored terminal markers count. Failed attempts count but cannot become diff
baselines. An owner, member, or collaborator can request `@effect-agent review` for incremental
review or `@effect-agent review full` for the whole admitted diff. Manual waves do not consume the
automatic allowance. If a rebase changes the merge base, automatic mode reviews the full diff
from the current merge base in the same attempt, under the same spending ceiling. It labels the
result as a full review; this fallback does not reset the automatic allowance. Explicit incremental
requests still stop when their baseline is missing or its merge base changed. Incomplete repository
comparisons fail in every mode. The workflow runs trusted default-branch code, serializes attempts,
and refuses stale findings. If a push makes an attempt stale before publication, the Action logs
the inspected and current commits and posts only an incomplete notice bound to the inspected commit.
The stale attempt counts toward the automatic allowance, but cannot mark the new commit as already
attempted. The queued review can proceed while allowance remains. The attempt logs failure types;
budget failures include the exhausted limit and observed usage.
Failure comments on an unchanged head also report budget exhaustion without exposing provider
diagnostics or model output.

The reviewer sees its actual spending balance before each request. It can explicitly report
unfinished coverage while preserving established findings. If turn, tool, or cost limits stop
research, the Action publishes established findings with an incomplete-coverage warning and fails
the check, including when no defects were found. Such an attempt cannot become
an incremental baseline or clear an earlier change request. This preserves useful findings without
claiming the full change was reviewed.
The model's `incomplete` flag describes unfinished assessment of supplied patches. The Action
separately discloses unavailable paths, even when assessment of the supplied patches completes.

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

The non-inference token count has a 10-second timeout per attempt and retries at most once for
timeouts, transport failures, or HTTP 408, 429, 500, 502, 503, and 504. Other HTTP failures and
malformed counts fail immediately. Exhausted preflight fails closed before spending admission;
it never authorizes an uncounted request. Cancellation interrupts the active attempt without a
retry. Diagnostics report only the preflight phase, attempt, failure category, and HTTP status.
Paid inference is never automatically retried.

The spending status is an outgoing-only, uncached suffix included in that token count. It shows
the balance before dispatch, estimated charges, outstanding reservations, and full cache-miss
input and output prices. It does not tell the reviewer when to finish or claim an output allowance
before counting the actual request. The remaining-turn
and tool counters are not presented as a research target. Their safety limits still apply.

The output allowance starts at 32,000 tokens and is reduced before each request when needed to fit
the remaining balance. Research can continue with that smaller allowance; the model, reasoning
effort, tool definitions, and tool choice stay unchanged. Logs show the requested and admitted
output allowances. If a response is truncated by the cost limit, or no further request fits, the
host delivers already recorded findings without another paid call and reports incomplete coverage.
Large changes may therefore remain incomplete under the ceiling. An incomplete empty result is
labeled `None recorded · incomplete`, never given a green check or counted as a successful review.

This is a client admission guarantee under the pinned pricing and token-count contracts, not an
invoice audit or an OpenAI account spending limit. Usage estimates remain separate from outstanding
reservations. Expected model or validation failures after a provider attempt retain an incomplete
report with those diagnostics even when no finding was recorded. Each review's logs and footer
show model calls, ordinary input, cache reads, cache writes, output, cache-hit ratio, and estimated
cost. Raw provider failure causes, credentials, and
repository source are excluded from the Action's diagnostics. Logs also count supplied tool
definitions, returned function calls, and completion calls to diagnose protocol failures.

Each admitted patch reaches the model once as literal unified diff text. The native Agent input
projection keeps all supplied changes while avoiding JSON-escaped source and duplicated old/new
context. A large remaining input can still prevent another call before the observed spend reaches
$1, because admission must cover a cache miss. Refusal logs report the counted input, remaining
balance, and minimum possible request reservation. The Action's spending admission replaces the
reviewer's cumulative token quota, so reusing cached context does not force early finalization.
The 8 research turns, 64 tool calls, 5 minutes, and 128,000-token context bounds still apply.

The Action uses explicit-only caching with a 30-minute TTL and a stable head-based routing key.
It marks reusable instructions, the diff, and completed tool batches before the ephemeral run-status
message, retaining earlier boundaries as history grows. Cache fields are added only at the native
Effect OpenAI client boundary; canonical history and provider encoding remain unchanged. This works
with the pinned Effect `4.0.0-rc.111` client, which serializes the additional request fields unchanged.
Required finalization selects `submit_review` through the native exact-tool choice, preserving
the research tool definitions and their order in the encoded request.
Compaction can change prefixes, and routing and cache availability still affect hits. See
[OpenAI prompt caching](https://developers.openai.com/api/docs/guides/prompt-caching).

Input admission allows at most 100 files, 256,000 aggregate patch characters, 80,000 characters per
patch, and 8 MB of hydrated base/head source. These bounds decide which files are supplied, not how
much inference may spend. Unavailable paths remain explicit coverage gaps.

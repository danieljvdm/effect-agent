# Effect Agent PR Review action

This directory contains the GitHub Action contract in `action.yml`. CI builds
the JavaScript bundle and commits it only on distribution tags.

Use `danieljvdm/effect-agent/action@action-v1` for the latest validated release,
or pin the distribution commit SHA reported by CI for an immutable version.
Each release also has an immutable `action-<source-commit-sha>` tag.
New source commits, including `@main`, do not contain a runnable bundle. Switch
to a distribution ref to receive updates. Older SHA pins that contain a bundle
continue to work.

The private
[`@effect-agent/pr-review-action`](../packages/pr-review-action) workspace
owns the source and tests. The public
[`@effect-agent/pr-review`](../packages/pr-review) package remains provider-
and transport-neutral.

Build locally with `vp run action:build`. The generated `action/dist/` directory
is ignored by Git. `vp run ready` also builds the Action; no bundle update or
generated-file merge is needed in a source PR.

After a push to `main` passes static checks, tests, and builds, CI publishes that
run's bundle in a child commit of the validated source. It creates the immutable
tag and advances `action-v1` atomically. Failed or superseded runs leave the
previous release available. Publication installs no dependencies and runs no
project code with repository write permission. Package releases remain separate.

## Review behavior

The reviewer automatically ignores known binary asset formats, including raster images,
fonts, audio/video, archives, PDFs, and compiled binaries, before fetching their contents.
Other bounded blobs containing NUL bytes are also ignored. These files count as ignored,
not incomplete coverage, and are unavailable to source tools. A binary-only PR needs no
model call. SVG, JSON, XML, and other text assets remain reviewable. API failures, malformed
responses, invalid UTF-8 without NUL bytes, and source-size limits still fail coverage checks.
Binary detection by content retains the existing file and byte read limits.
GitHub reads retry transport errors, incomplete or invalid JSON bodies, timeouts, HTTP 408,
5xx responses, and rate-limited 403/429 responses up to three times. Attempts have a 15-second
timeout, exponential backoff starting at one second, and a shared 90-second read deadline.
Retry-After and rate-limit reset delays are respected; a delay beyond the deadline stops the read
instead of retrying early. Generated-file classification retains its tighter 10-second deadline.
Schema-invalid responses, invalid UTF-8, identity mismatches, and ordinary permission or missing-file
errors fail immediately. Retries use the same immutable blob SHA. GitHub writes are never retried.
Read diagnostics include the operation, attempt, failure category, HTTP status, and GitHub request
ID when available, without response bodies or credentials. Exhausted source reads log the affected
path and revisions and leave coverage incomplete.
When a rename or content replacement crosses between binary and text, the textual side
is still reviewed as an addition or deletion. Explicit ignore rules continue to exclude
an entire rename when either path matches.

One bounded review run sees every admitted changed path, reads complete diffs, and follows affected
callers, contracts, and cleanup paths through immutable base and head source. Small diffs are supplied
directly; larger diffs are available through bounded `read_diff` pages in the same conversation.
Literal code search locates relevant source without requiring the reviewer to guess filenames.
The host tracks unread diff ranges, validates finding paths and
RIGHT-side anchors and publishes against the inspected head. A stopped run preserves findings
recorded before research ended. Preparation failures publish a failure marker. Blocking findings
request changes and fail the Action after publication; other outcomes remain comments.

Reviews with findings include a **Copy all findings** dropdown. Expand it and use the code
block's copy button to copy every finding from that review, including paths, inline line numbers
when available, and the inspected commit. The block reminds coding agents to verify findings
before making changes. It opens by default when any finding has no inline comment.

A complete pass with no new blockers can dismiss this bot's earlier change requests, but only
when the reviewer explicitly verifies every blocker in each selected review against current source.
The dismissal records the inspected commit and the fixing evidence. A clean delta, changed line,
resolved conversation, or commit message alone does not clear earlier feedback. Human and other
bots' reviews are never dismissed.

Incremental passes revisit unresolved reviews, including body-only findings and findings on paths
outside the latest delta. A fix retained because its pass found a new blocker can be verified again
on a later pass even when the original path no longer changes. This verification does not expand
new-defect discovery beyond the delta. Use `@effect-agent review full` for a manual same-head retry.
At most eight prior reviews are considered, each with its complete review body and bot comments
within 32,000 characters. Oversized feedback stays blocking; it is never truncated for verification.
Follow-up verification shares the same conversation and spending and execution
limits. Incomplete, exhausted, excluded-path, or newly blocking results dismiss nothing.
The Action rechecks review ownership, feedback, and head before each dismissal. GitHub does not
support a conditional dismissal, so a push can still race the final API request. Dismissals happen
before the new comment is posted; if a later API call fails, the Action fails and any completed
dismissals retain their evidence in GitHub. A failed dismissal records an incomplete attempt when
GitHub still accepts review comments, so it consumes the automatic allowance. Retry with a full
review or inspect and dismiss manually.

Automatic waves use the configured limit, defaulting to two; this repository allows five.
Zero disables automatic reviews. Rerunning the workflow can retry an incomplete review on the same
head while automatic attempts remain. Each published attempt consumes that allowance; incomplete
attempts never become incremental baselines. Once the allowance is exhausted, an incomplete head
continues to fail until a manual review completes. Completed heads are still skipped.
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
The reviewer refuses early completion while admitted diffs remain unread. A model-reported
`blockedOn` reason names specific unavailable evidence and is retained in an incomplete result.
The Action
separately lists excluded paths and their reasons, including input limits, unreadable source, and
diffs that were not read completely. Excluded paths prevent a complete review even when assessment of the
supplied patches completes. The comment shows up to 30 exclusions; the Action log includes all of
them. Paths excluded only by input capacity remain available to bounded source tools, while ignore
rules and unsupported or unreadable entries continue to block access.

Source search uses a case-sensitive literal query and a path substring at either exact revision.
Each page scans up to 20 authorized regular files, with four concurrent reads, and returns at most
five matching lines per file. Snippets retain the complete query within 200 characters. A next
cursor identifies more files; `truncated` identifies omitted matching lines, and unreadable paths
are listed separately. A partial search cannot establish that no callers exist. Ignore rules,
binary exclusions, and symlink restrictions apply equally to reads, filename search, and code search.

### Generated files

Modified and deleted generated files are ignored before reading their contents, using GitHub's
classification at the trusted PR merge base. Removing their attributes or ignore rules in the PR
does not change that classification, including during incremental reviews. New paths, renames,
permission changes, and unsupported entries follow normal admission rules. Classification failures
leave the review incomplete.
Classification attempts have a separate 100-file limit. Ignored generated files do not consume
review capacity. After that limit, remaining files follow normal admission rules without automatic
generated-file exclusion.

## Spending and prompt caching

Review attempts default to a configurable **$2.50 maximum**. Set the Action's `max-cost-usd`
input or local `PR_REVIEW_MAX_COST_USD` environment variable to a value from $0.01 to $100.
The base defaults to **$1**. Set `base-cost-usd` or local `PR_REVIEW_BASE_COST_USD` to a value
from $0.01 to $100. The allowance is **base plus $1 per 100,000 characters** in admitted patches
and selected prior feedback, capped at the maximum even when the base exceeds it.
For example, 10,000 characters allow $1.10, 50,000 allow
$1.50, and 150,000 or more allow $2.50 with the default configuration. Ignored and excluded
files do not increase the allowance. Empty or skipped reviews have a zero allowance.
Validated source-map JSON payloads omitted from dependency patches do not increase it either.
The footer, logs, and `cost-limit-usd` output show the actual scaled allowance, including
both settled charges and outstanding reservations. The same policy applies to full reviews,
incremental reviews, and eval trials; a retry gets a new allowance.

With `base-cost-usd: "4.00"` and `max-cost-usd: "20.00"`, 10,000 characters allow $4.10
and 1,600,000 or more allow $20. These are per-attempt allowances, not a cumulative PR limit.

Every consumer must specify `model` (or local `PR_REVIEW_MODEL`); there is no fallback.
Missing, blank, and unpriced models fail before paid inference. Reasoning effort defaults to
`medium` and accepts `low`, `medium`, `high`, `xhigh`, or `max`.
Omit `priority` to omit the API's `service_tier` parameter and inherit the OpenAI project setting.
Set `priority: default` to force Standard processing, or `priority: fast` (local
`PR_REVIEW_PRIORITY=fast`) to request [OpenAI Fast mode](https://developers.openai.com/api/docs/guides/fast-mode).
Fast is supported for the listed models, subject to account and regional availability.
Fast mode costs twice the standard token rates for the supported models and uses the same
size-scaled spending cap, so the allowance buys fewer tokens. The effect-agent repository's
workflow opts into Fast mode with `base-cost-usd: "20.00"` and `max-cost-usd: "25.00"`;
its allowance still scales with PR size.
Requests with omitted priority reserve at Fast rates because the project setting can enable Fast.
Explicit priorities reserve at the selected tier's rates, and settlement uses the tier reported by OpenAI,
including standard-rate fallback from Fast mode. Both `fast` and `priority` response tags identify
Fast pricing. Unknown response tiers retain their reservation and stop the attempt. Rejected
requests are not retried at another tier; the selected model and effort stay unchanged.

```yaml
- uses: danieljvdm/effect-agent/action@action-v1
  with:
    openai-api-key: ${{ secrets.OPENAI_API_KEY }}
    github-token: ${{ secrets.GITHUB_TOKEN }}
    pull-request: ${{ github.event.pull_request.number }}
    model: gpt-6-astra
    effort: medium # Optional; this is the default.
    priority: fast
```

**BEHAVIOR CHANGE:** Add an explicit `model`. Replace `fast: "true"` with `priority: fast`;
use `priority: default` to retain explicitly Standard processing.

It accepts only the priced model IDs listed in `action.yml`. The rate card was verified on
2026-09-05. Sol and its `gpt-5.6` alias refuse new paid requests on or after 2026-11-22 UTC
until their promotional rate card is refreshed. This deadline does not apply to Astra, Terra,
or Luna. See [OpenAI pricing](https://developers.openai.com/api/docs/pricing).

Before each research, compaction, or completion request, the Action uses OpenAI's
[input-token counting endpoint](https://developers.openai.com/api/docs/guides/token-counting) on the
encoded input, tools, reasoning, and output-format settings. It rejects inputs above 128,000 tokens,
then reserves every input token at the cache-write rate plus the full output allowance, including
reasoning. Admission never assumes a cache hit. The ledger releases unused reservations only after
validating the response's usage, model, tier, and counted bounds. Failed, interrupted, or unmetered
requests retain their possible charge; the transport does not automatically retry them.

Character admission does not guarantee a token fit. If the engine's context estimate or the
provider's exact count exceeds the input limit, the Action publishes an incomplete token-budget
result, preserves earlier findings, and lists unread diffs as unreviewed. A refusal
before the first model call reports zero spend and reserves nothing. The attempt stops without
truncating patches or retrying paid inference.

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

Within `.patch` files, the Action replaces single-line source-map JSON payloads in
valid nested `.map` diffs with explicit omission markers before model input and spending
admission. It recognizes the source-map structure, not arbitrary generated-file comments.
Patch headers, outer hunk coordinates, nested line prefixes, and source changes are retained.
The report discloses omitted payload lines and character counts separately from assessed
content. Malformed patches, unrecognized JSON, indexed or multiline maps, and non-map
sections remain literal evidence. Source tools still allow targeted inspection when needed;
the Action does not change repository files or automatically skip entire dependency patches.

The Action admits implementation and configuration changes before documentation paths and prose,
with alphabetical order within each group. One review conversation retains the complete changed-path
manifest and established findings, so related changes stay visible across the investigation. Diffs
up to 32,000 total characters appear directly in the initial prompt; larger changes use `read_diff`
pages of up to 32,000 characters. Pages can cross file boundaries, so reviewing many small files does
not require a separate call for each file. Every retained patch line remains available in full. One spending ledger,
128-turn allowance, 512-tool-call allowance, 5-minute deadline, and 24-finding capacity cover the entire
attempt. Findings survive an expected execution failure. Unread diff ranges prevent complete coverage;
reading every range is necessary but does not prove the model finished assessing the change.

The native Agent input projection uses literal unified diff text, avoiding JSON-escaped source and
duplicated old/new context. A large remaining input can still prevent another call before the observed spend reaches
the allowance, because admission must cover a cache miss. Refusal logs report the counted input, remaining
balance, and minimum possible request reservation. The Action's spending admission replaces the
reviewer's cumulative token quota, so reusing cached context does not force early finalization.
The 128-turn and 512-tool-call bounds accommodate diff navigation and research within the shared
spending cap. The five-minute deadline and native rollover at a 48,000-token working context
still apply; the provider's separate exact-input admission boundary remains 128,000 tokens.

The Action uses explicit-only caching with a 30-minute TTL and a stable head-based routing key.
It marks reusable instructions, the diff, and completed tool batches before the ephemeral run-status
message, retaining earlier boundaries as history grows. Cache fields are added only at the native
Effect OpenAI client boundary; canonical history and provider encoding remain unchanged. This works
with the pinned Effect `4.0.0-rc.112` client, which serializes the additional request fields unchanged.
Required finalization selects `submit_review` through the native exact-tool choice, preserving
the research tool definitions and their order in the encoded request.
Compaction can change prefixes, and routing and cache availability still affect hits. See
[OpenAI prompt caching](https://developers.openai.com/api/docs/guides/prompt-caching).

Input admission allows at most 1,000 usable files, 2,000,000 characters per patch, 8,000,000 patch
characters in total, and 8 MB of hydrated base/head source. Failed, unsupported, and oversized
candidates do not consume usable file slots. A file that exceeds the remaining source or patch
allowance is excluded without preventing smaller later files from fitting. These bounds
limit input preparation independently of the shared inference spending ceiling.

The source cache retains at most sixteen verified blobs, each bounded to 2 MB. Evicted source is
read again by its immutable blob SHA, so repository-wide searches do not retain the entire tree.

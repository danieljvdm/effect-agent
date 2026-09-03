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

For the initial cutover, seed `action-v1` with the last validated source commit
that still contains the bundle before switching workflows. Subsequent main CI
runs advance that tag automatically. When rebasing an older feature branch,
keep the deletion of `action/dist/index.mjs`.

## Review behavior

The reviewer automatically ignores known binary asset formats, including raster images,
fonts, audio/video, archives, PDFs, and compiled binaries, before fetching their contents.
Other bounded blobs containing NUL bytes are also ignored. These files count as ignored,
not incomplete coverage, and are unavailable to source tools. A binary-only PR needs no
model call. SVG, JSON, XML, and other text assets remain reviewable. API failures, malformed
responses, invalid UTF-8 without NUL bytes, and source-size limits still fail coverage checks.
Binary detection by content retains the existing file and byte read limits.
When a rename or content replacement crosses between binary and text, the textual side
is still reviewed as an addition or deletion. Explicit ignore rules continue to exclude
an entire rename when either path matches.

The shipping Action uses the **baseline** strategy. It supplies bounded patches and immutable
base/head source to one sequential discovery workflow. The review says **N patches supplied**;
discovery's declared assessment status does not establish that every file was assessed.
Straightforward changes can finish from
the diff; source tools are not an exhaustive repository audit. The host validates paths and
RIGHT-side anchors and publishes against the inspected head. A stopped run preserves findings
recorded before research ended. Preparation failures publish a failure marker. Blocking findings
request changes and fail the Action after publication; other outcomes remain comments.

The review displays discovery's declared status separately from candidate verification, excluded
paths, ignored paths, and pending batches. Its compact activity summary counts reads, file searches,
EOF-short reads, oversized rejections, unavailable source, truncated results, and dropped records.
Action logs retain at most 128 source-activity records, with stage, batch, immutable revision, path,
requested and returned line spans, outcome, and truncation. They exclude source contents, search
queries, credentials, raw causes, verifier reasons, and model reasoning. Calls, usage, reservations,
and stop reasons have stage attribution alongside one reconciled attempt total. Diagnostics are
finalized after expected failure, defect, or interruption while the process remains alive.

The **verified** strategy remains evaluation-only until the frozen comparison supports rollout.
Its host always challenges accepted candidates after discovery when shared limits permit. Supported
findings retain their original text and severity; refuted candidates remain in evaluation diagnostics
and are suppressed. Unresolved candidates remain visible as **unverified**, including inline feedback
and the copy-all block. Only supported blockers can create a new change request in that strategy.
Unresolved-only feedback produces a comment and an incomplete Action failure. Verification cannot
clear incomplete discovery, exclusions, pending batches, or earlier change requests.

Reviews with findings include a **Copy all findings** dropdown. Expand it and use the code
block's copy button to copy every finding from that review, including paths, inline line numbers
when available, and the inspected commit. The block reminds coding agents to verify findings
before making changes. It opens by default when any finding has no inline comment.

A complete pass with no new blockers can dismiss this bot's earlier change requests, but only
when the reviewer explicitly verifies every blocker in each selected review against current source.
The dismissal records the inspected commit and the fixing evidence. A clean delta, changed line,
resolved conversation, or commit message alone does not clear earlier feedback. Human and other
bots' reviews are never dismissed.

Incremental passes select prior reviews with an inline finding on an admitted changed path.
They verify those specific blockers without expanding new-defect discovery beyond the delta.
Use `@effect-agent review full` for body-only findings, fixes in other paths, or a same-head retry.
At most eight prior reviews are considered, each with its complete review body and bot comments
within 32,000 characters. Oversized feedback stays blocking; it is never truncated for verification.
Follow-up verification runs in the final patch batch under the existing spending and execution
limits. Incomplete, exhausted, excluded-path, or newly blocking results dismiss nothing.
The Action rechecks review ownership, feedback, and head before each dismissal. GitHub does not
support a conditional dismissal, so a push can still race the final API request. Dismissals happen
before the new comment is posted; if a later API call fails, the Action fails and any completed
dismissals retain their evidence in GitHub. A failed dismissal records an incomplete attempt when
GitHub still accepts review comments, so it consumes the automatic allowance. Retry with a full
review or inspect and dismiss manually.

Automatic waves use the configured limit, defaulting to two; this repository allows five.
Zero disables automatic reviews.
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
separately lists excluded paths and their reasons, including input limits, unreadable source, and
batches that never started, which count separately as pending. Excluded paths prevent a complete review even when assessment of the
supplied patches completes. The comment shows up to 30 exclusions; the Action log includes all of
them. Paths excluded only by input capacity remain available to bounded source tools, while ignore
rules and unsupported or unreadable entries continue to block access.

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

The baseline gives discovery the full $0.999999 allowance. Evaluation's verified strategy gives
discovery $0.699999, including its settled charges and outstanding reservations, and holds $0.300000
for verification. Verification may use that holdback plus unspent, unreserved discovery allowance.
Changing stages preserves the same ledger and all outstanding reservations. A discovery allowance
refusal does not close verification; uncertain charges or invalid accounting close both stages.
Each billable request, including compaction and completion, reserves before dispatch and settles
at most once. Held dollars cannot extend the shared deadline, turn, context, or tool limits.

Character admission does not guarantee a token fit. If the engine's context estimate or the
provider's exact count exceeds the input limit, the Action publishes an incomplete token-budget
result, preserves earlier findings, and lists batches that never started as unreviewed. A refusal
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

The Action admits implementation and configuration changes before documentation paths and prose,
with alphabetical order within each group. The reviewer divides admitted patches into sequential
batches of at most 256,000 patch characters, with a fresh model context for each batch. Each patch
belongs to one batch and remains complete. Every batch can read the same authorized base/head source
to investigate interactions with other files. One spending ledger, 64-turn allowance, 64-tool-call
allowance, 5-minute deadline, and 24-finding capacity cover the entire attempt. Findings survive a
later batch's expected failure; stopping leaves the remaining batches explicitly unreviewed.

The native Agent input projection uses literal unified diff text, avoiding JSON-escaped source and
duplicated old/new context. A large remaining input can still prevent another call before the observed spend reaches
$1, because admission must cover a cache miss. Refusal logs report the counted input, remaining
balance, and minimum possible request reservation. The Action's spending admission replaces the
reviewer's cumulative token quota, so reusing cached context does not force early finalization.
The 64-turn safety bound matches the tool-call allowance, so an eight-turn cutoff no longer ends
affordable serial research. The 64 tool calls, 5 minutes, and 128,000-token context bounds still apply.

The Action uses explicit-only caching with a 30-minute TTL and a stable head-based routing key.
It marks reusable instructions, the diff, and completed tool batches before the ephemeral run-status
message, retaining earlier boundaries as history grows. Cache fields are added only at the native
Effect OpenAI client boundary; canonical history and provider encoding remain unchanged. This works
with the pinned Effect `4.0.0-rc.111` client, which serializes the additional request fields unchanged.
Required finalization selects `submit_review` through the native exact-tool choice, preserving
the research tool definitions and their order in the encoded request.
Compaction can change prefixes, and routing and cache availability still affect hits. See
[OpenAI prompt caching](https://developers.openai.com/api/docs/guides/prompt-caching).

Input admission allows at most 100 candidate files, 256,000 characters per patch, and 8 MB of hydrated
base/head source. A complete patch may occupy an entire batch; there is no smaller per-file cap.
The batch size does not exclude later patches. A file that exceeds the remaining
source allowance is excluded without preventing smaller later files from fitting. These bounds
limit input preparation independently of the shared inference spending ceiling.

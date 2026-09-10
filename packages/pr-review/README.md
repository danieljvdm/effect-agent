# @effect-agent/pr-review

A provider-neutral agent that reviews an exact base-to-head change using a complete change
index, paged diffs, and immutable source tools. One conversation follows related behavior across
files. There are no separate patch batches, candidate pipeline, voting, or repository execution.

## Evidence and findings

The initial prompt includes every admitted path and its character range in one literal diff
artifact. Changes up to 32,000 characters are included directly. Larger changes use `read_diff`:
start at offset zero and follow `nextOffset`, or select a file's start offset from the index.
Pages contain at most 32,000 UTF-16 characters and can cross file boundaries or split lines.
Original unified patches, hunk coordinates, deletions, renames, and mode metadata remain intact.

`read_file` reads up to 200 lines and 20,000 characters at the exact base or head.
`find_files` searches filenames. `search_code` searches literal, case-sensitive source text to
find definitions, callers, consumers, and tests, including unchanged code. Its path filter is a
filename substring; cursor zero starts the search. Each page scans twenty authorized files,
returns up to five matching lines per file, and provides `nextCursor` for more files.
`truncated` identifies omitted matching lines and `unreadablePaths` identifies failed reads.
A partial or failed search cannot establish that a caller is absent.

The reviewer starts with the promised consumer outcome and traces supported execution paths,
including unchanged callers and consumers. It distinguishes missing promised behavior from
optional feature expansion. Before recording a defect, it checks the
strongest relevant guard, documented exception, or alternative interpretation and establishes
why the supported trigger still causes concrete impact. It checks base/head causation, boundary
values, cleanup, concurrency, and whether changed tests would detect the claimed failure.
New features must satisfy their stated contracts, including validation, limits, isolation,
and aggregation; a bypass can be a defect even when the old code also accepted that input.
Unrelated old bugs, speculation, style, compiler diagnostics, and generic test requests are
excluded. Incremental reviews limit new findings to their exact delta.

The shared finding rubric, parent review procedure, and host-supplied repository policy are
separate instructions. Policy findings identify the specific rule and applicable exceptions,
citing instruction paths and lines when available. An explicitly reviewable architecture
contract can warrant a finding without a runtime failure; its supplied severity takes precedence.

`record_finding` is the only way to add findings to the report. The model is instructed to record
each distinct root cause once and recover the saved ledger with `review_status` after `new_context`,
without re-recording an issue with revised wording or severity. Counterevidence must be checked
before recording because the ledger has no retraction or revision operation. `submit_review` is the
required native completion tool and accepts only `blockedOn` and `resolutions` metadata.
The host builds the final report directly from the ledger, so completion never rewrites or merges
findings. Extra completion fields fail validation.

The host validates changed paths and RIGHT-side line anchors, demotes invalid anchors to
top-level findings, and removes only exact repeated records. Distinct defects at the same path
and line remain separate. Up to 24 findings are retained, prioritizing blocking over important
over minor findings. Overflow always marks the result incomplete, regardless of later completion.
Finding paths, titles, and bodies retain their 512, 200, and 2,000-character bounds.

## Coverage and limits

Requests admit up to 1,000 distinct changed paths, 2,000,000 characters per patch, and 8,000,000
patch characters overall. These are host input limits, separate from the model's working context.
Source hosts can apply additional authorization and admission bounds.

The host tracks diff ranges available to completed model requests. Merely issuing a read, repeating
a page, skipping ahead, or failing a tool does not establish complete coverage. `pendingPaths`
includes partially read files. `review_status` recovers saved findings and outstanding ranges;
its optional cursor pages through the current pending list. While a range remains unread,
`submit_review` returns a recoverable error with the next unread offset. The same run continues
under its original budgets. A native budget stop preserves the pending paths and findings as
incomplete; repeated completion refusals cannot restart the run or reset its allowance.
Reading all ranges is necessary, but it does not prove that the model assessed every behavior.

`review_status` also keeps a bounded investigation notebook for the lifetime of the review.
Replace it with `notes: { text, expectedRevision }`; text is limited to 4,000 characters and
stale revisions fail without overwriting newer notes. The response supplies the current text and
revision. Notes preserve unresolved questions, exact evidence references, and next checks across
rollover. They are model-authored context, never proof of coverage or a source of findings.
Only the accepted-update count (`notesUpdates`) leaves the review; note text is not persisted in
the outcome. Children cannot update the parent's notebook.

Completion means a source-based assessment of the admitted changes and material supported
hypotheses, not proof of correctness or an exhaustive audit of every dependency. The model can
report `blockedOn` only for specific unavailable evidence, naming the affected behavior and its
attempts to retrieve that evidence. It must still review the remaining patches. The bounded
reason is retained in the outcome and summary, forces incompleteness, and prevents resolutions.
Excluded artifacts, lack of live execution, and hypothetical uncertainty do not themselves
block assessment of the admitted change.

Every parent conversation has 128 model turns, 512 tool calls, four concurrent tools, and a five-minute deadline.
The default `compaction: "rollover"` strategy uses a 48,000-token working context to bound
context growth during large reviews. Hosts can select `compaction: "prune"`
and an integer `contextTokenLimit` from 16,000 to 128,000. These settings do not widen host input
admission or create new spending, turn, or tool allowances. Invalid options fail before model work.

Native rollover starts a fresh window without a summarizer call. Its bounded recovery excerpt
may omit unseen tool results, so undelivered diff pages remain unread and must be fetched again.
Already delivered ranges and saved findings survive. Both strategies support calling `new_context`
alone with a handoff; original instructions and the complete change index remain available.

Every measured outcome includes `compactions`, an array of emitted native `CompactionPerformed`
events containing only `kind`, `turn`, `tokensBeforeEstimate`, and `tokensAfterEstimate`.
An empty array means no event was emitted; absence means the outcome supplied no measurement.
The array is bounded to 512 entries and includes events retained before a typed failure. It does
not expose source or handoff text, and events alone do not distinguish automatic from requested
rollovers. Boundaries that fail before event emission are not counted.

Without `costControl`, the engine applies a cumulative 416,000-token policy with a 160,000-token
completion reserve. A host cost estimator alone does not disable it. With `costControl`, the
host reserves the full possible charge before each provider call, replacing that token quota.
The [GitHub Action](../../action/README.md) supplies spending admission for supported OpenAI models.
Recorded findings survive a cost stop without requiring another paid call. `reservedCostMicrousd`
reports maximum additional charges for sent requests whose usage remains unknown.

Token, turn, tool, or cost exhaustion is incomplete. Expected failures preserve recorded findings
and completed model attempts, including their accounting when no finding was recorded. Context/input-token
refusals return an incomplete token-exhausted outcome before paid inference when possible.
Failures before any model attempt, finding, or budget refusal remain typed. Defects and interruption propagate, and every
resource belongs to the review's Scope. Excluded host `unreviewedPaths` remain separately disclosed.
An empty result never proves that the repository is defect-free.

## Optional research children

The default reviewer runs alone. Experiments can provide `research: { model, concurrency: 2 }`
to expose native `delegate_research`. Concurrency is either one or two (default two), with at
most two children established per review. Each child has six ordinary model turns, twelve tool
calls, a 60-second deadline, and a 32,000-token context using native pruning. The runtime can
reserve a final completion response after structural exhaustion. The host configures the child
model's output limit; the eval uses 4,000 tokens.

A delegation supplies one unresolved, falsifiable question whose answer could change a finding,
and one to three distinct admitted changed paths. Instructions ask neutrally for supporting or
refuting evidence and discourage generic second reviews; children use the same finding rubric.
The host selects their exact patches, rejecting more than 32,000 total patch characters.
Children receive the immutable revisions and current saved findings, and can use only the
three repository read tools, `record_finding`, and `finish_research`. The completion contains
an evidence summary and incomplete flag; findings go directly to the same canonical ledger.
Children cannot delegate further, establish parent diff coverage, or resolve prior reviews.

Native reservations and child fibers belong to the review's Scope. Child compaction cannot
change the parent's unread ranges. Child model requirements remain visible in the review's
Effect requirements, and usage contributes to the existing accounting. A host supplying
`costControl` must use the same admission service for both model layers, so all requests draw
from one spending cap. Without host admission, the parent and child native token policies are
separate; the parent token quota is not a combined spending cap.

Research-enabled runs break equal-severity finding ties by their complete serialized values
before retaining 24, so child completion order cannot select the survivors. Exact duplicates
are still the only records removed. A rejected delegation, failed or interrupted child,
unfinished join, child-reported incomplete result, or child budget exhaustion makes the parent
incomplete and suppresses resolutions. Defects and external interruption still propagate.

Measured outcomes include `research` counters: `delegations` counts declared delegation calls;
`started`, `completed`, `failed`, and `interrupted` count emitted native child events; `incomplete`
counts completed child results that report incomplete or exhausted work. Zero counts are measured
zero, while an absent field means no measurement was supplied. Counts contain no child source,
summary, or transcript; prestart refusals have a declaration but no child event.

## Follow-ups and hosting

Hosts can supply up to eight prior `ReviewFollowUp` values, each up to 32,000 characters. The
reviewer verifies every blocker in a follow-up against current source before returning its exact
ID and fixing evidence. Unknown or duplicate resolution IDs fail verification. Incomplete,
exhausted, pending-path, or excluded-path results return no resolutions. History selection,
credentials, dismissal authorization, and publication belong to the host.

```ts
const reviewer = makeReviewer({ model, guidance, costControl });
const program = reviewer.review(request).pipe(Effect.provideService(ReviewRepository, repository));
```

`ReviewRepository` implementations provide typed Effect operations for `readFile`, `findFiles`,
and `searchCode`. Hosts authorize source sent to models, pin immutable revisions, enforce read
bounds, and treat all source and model output as untrusted. The reviewer has no ambient filesystem
or network access. `ReviewSource.fromText` applies the shared source-range bounds.

The navigable diff approach is informed by [Pullfrog's review workflow](https://github.com/pullfrog/pullfrog/blob/0212dedb0f92b8ba4020c17dc30d3eced32415d7/modes.ts)
and [Codex's review task](https://github.com/openai/codex/blob/588b781ab4924ce7352488394028e63d74cf807f/codex-rs/core/src/tasks/review.rs).
These designs do not establish accuracy. The [eval bench](../../tooling/pr-review-eval/README.md)
measures first-trial detection, false positives, and incomplete runs against adjudicated cases.
Portions of the original review instructions were adapted from
[PR-Agent](https://github.com/The-PR-Agent/pr-agent); see `NOTICE` for its MIT attribution.

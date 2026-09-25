# PR-review eval

Replay saved PRs through the reviewer and score findings against adjudicated defects.

- Validate offline: `vp run pr-review-eval -- --cases fixtures/smoke-suite.json validate`.
- Validate the source-backed historical corpus:
  `vp run pr-review-eval -- --cases fixtures/public-effect-agent-v2.json validate`.
- Live runs require `EFFECT_AGENT_LIVE=1`, `OPENAI_API_KEY`, and `PR_REVIEW_MODEL`.
- The CLI reads the optional, git-ignored root `.env.local` as a fallback; exported environment
  variables take precedence. Keep credentials there rather than in fixtures or results.
- The current variant reuses the Action's explicit cache and input-sized trial allowance, with a
  configurable maximum of $2.50 by default.
- Run `vp run pr-review-eval -- --help` for commands and options.
- Public cases live in `fixtures/`; private cases and results belong in ignored `data/` and `results/`.

## Review variant

The current OpenAI variant uses the Action's `makeReviewOpenAi` adapter with an explicitly selected
model, `medium` reasoning by default, explicit prompt caching, and a separate spending ledger for
each trial. Set `PR_REVIEW_MODEL` (for example, `gpt-6-astra`); there is no model fallback.
Set `PR_REVIEW_EFFORT` to compare reasoning configurations. Effort accepts
`low`, `medium`, `high`, `xhigh`, or `max`; model IDs must have a supported rate card.
Set `PR_REVIEW_PRIORITY=fast` for the Fast service tier; the default is `default`.
The selected tier is recorded in the observation and used by the provider cost ledger.
Each observation records the effective settings. Use distinct variant IDs for comparisons.
Set `PR_REVIEW_COMPACTION` to `rollover` (default) or `prune`, and
`PR_REVIEW_CONTEXT_TOKENS` to an integer from 16,000 to 128,000 (default 128,000).
Rollover uses the engine's native fresh-context strategy without a summarizer call. Both strategies
retain the same spending allowance and support the model's explicit `new_context` tool.
Compare strategies at the same context limit, and record whether a run uses the production
setting or a context-pressure experiment. Confirm that compaction actually occurred
before attributing a result to its strategy. Observations require both context settings; older
observations without them are incompatible and must be rerun.
Set `PR_REVIEW_RESEARCH_CONCURRENCY` to `1` or `2` to enable native research children;
the default `0` keeps a single reviewer. The parent may delegate at most two focused questions
over host-validated changed paths. Each child uses the same model, effort, and standard service
tier, with six turns, twelve tool calls, sixty seconds, 32,000 context tokens, and 4,000 maximum
output tokens. Parent and children share one provider client and spending allowance. Children
save findings directly to the same ledger; parent diff coverage remains independent. Failed,
refused, interrupted, or incomplete delegations make the review incomplete. This is an optional
experiment; its configured concurrency and output limit are recorded in each observation.
Set `PR_REVIEW_MAX_COST_USD` to configure the maximum, between $0.01 and $100; the default is $2.50.
The variant reads this setting once. Each nonempty case receives $1 plus $1 per 100,000 admitted
patch and follow-up characters, capped at that maximum. For example, 10,000 characters receive
$1.10, while 150,000 characters receive the default $2.50 maximum. Repeated trials receive fresh
allowances; the configured maximum is per trial, not a cap on the entire evaluation suite.
Admission reserves full cache-miss input and the affordable output allowance before every
request. Unmetered requests keep their reservations. Saved usage is an estimate, not an invoice.
Only the non-inference input-token count can retry once, after a transient failure; each count
attempt has a 10-second timeout. Permanent or exhausted preflight failures prevent admission.
The variant records the `repository-review` profile, required `maxCostMicrousd`, and the
`input-size-v1` budget policy. The configured maximum stays constant across cases; the policy and
immutable case input determine each allowance. Observations without this budget metadata are
incompatible and must be rerun. One review
navigates paged diffs and immutable source, including literal code search for definitions and
callers. It reads source to resolve concrete defect questions and can explicitly report unfinished
coverage. The provider enforces spending admission without appending balance messages to the
review conversation.

Frozen source search follows the live adapter's bounds: twenty files per page, five matching lines
per file, and 200-character snippets. Follow `nextCursor` to search the remaining files;
`truncated` means additional matching lines were omitted. Every frozen entry contains exact,
complete source, so it has no unreadable entries. Cases without a snapshot return a typed source
search failure. Search covers only the files included in the snapshot.

For a replay that can search the full tracked Git trees, set
`PR_REVIEW_LOCAL_GIT_REPOSITORY` to an absolute local checkout path. The request's base and head
revisions must be full commit object IDs available there. Pass the Action workflow's complete
comma-separated `PR_REVIEW_IGNORE` value. The adapter reads only objects at those two commits:
working-tree edits, untracked files, symlinks, binary assets, and
ignored paths are unavailable. It uses the Action's source filtering and search bounds, including
the 2 MB text-blob limit and unreadable-path reporting. `validate` checks the pinned objects and
changed-path visibility offline before `run` can call a model:

```sh
PR_REVIEW_LOCAL_GIT_REPOSITORY=/absolute/repository \
PR_REVIEW_IGNORE='**/*.map,**/generated/**' \
vp run pr-review-eval -- --cases data/cases.json validate
```

The Action also uses GitHub GraphQL's `isGenerated` verdict at the trusted merge base to exclude
modified or deleted generated files. Git trees do not encode that verdict. If the Action excluded
such paths, add them explicitly to `PR_REVIEW_IGNORE` for the replay; `validate` cannot establish
generated-file parity on its own.

Observations keep `repositoryDigest` as the case fixture's frozen-snapshot digest and additionally
record `repositorySource` with a digest of the actual pinned trees and source exclusions. Reports
reject comparisons when any trial or variant of a case used a different source. The Git adapter
disables lazy fetching in partial clones and never checks out code, invokes Git hooks, contacts
GitHub, or writes to the target repository. `validate` checks that changed paths are visible in
the pinned trees; the corpus builder must also derive each supplied patch from those revisions.

## Corpus scope

`public-effect-agent-v2.json` contains seven historical cases: six known-defect cases with sixteen
adjudicated defects (four blocking and twelve important), and one clean control. The original
PR #425 case checks usage completeness and durable delegation, without requiring later API extensions. Frozen files come
from each case's exact local Git revisions, including present changed files, tracked relative
TypeScript dependencies, `AGENTS.md`, root toolchain manifests, and relevant package manifests.
Files added or deleted by the change are absent at the corresponding revision. Snapshot digests
bind this source to each observation. Whole frozen files may contain up to 1,000,000 characters;
source tool responses retain the reviewer's separate output bounds.

The largest historical case contains 23 changed files and 133,479 patch characters. This corpus
does not measure performance or recall on substantially larger pull requests. The smaller
`reviewer-quality.json` cases are source-derived development regressions and clean controls.
The synthetic smoke cases have no frozen source and cannot establish source-assisted review
quality. Offline validation checks data integrity, not whether
a model finds bugs; scripted tests establish host behavior rather than model recall.

The `synthetic-*.json` suites contain fictional, redistributable source-backed probes for a
status-and-follow-up review loop, a bounded active-record page, typed inventory failures, and an
Effect Result false-positive control. Exact historical replays belong in ignored `data/`; these
public cases do not establish production recall. The fixture test verifies that each patch
reconstructs its frozen head source and that defect anchors lie in the delivered diff.

Use `unadjudicated` for operational replay cases without an established defect oracle.
These cases have no expected defects and never count as clean controls or completed blocker
cases. A completed review on such a case does not measure defect-detection quality.
Every case must supply at least one patch or follow-up; empty cases are rejected during corpus
decoding before provider setup.

## Scoring and output

This bench replays saved requests against adjudicated defects.
Public fixtures are examples and do not support a quality claim. Live trials require
`EFFECT_AGENT_LIVE=1` and provider credentials.

Score the first trial separately. `defectRecall` counts every expected defect detected at any
severity, including important defects. `defectCases` counts a case as complete only when the
first trial completes and detects every expected defect. Blocker detection accepts an expected
blocker at any severity, while blocking recall requires `blocking`. Finding precision and blocking
precision track false positives and overstated severity separately. Unjudged or unclear findings
leave affected metrics unresolved. Later trials measure instability and cannot repair a first
trial miss. Bind named judgments to the exact observation and oracle digests. Add a new corpus
defect when the model finds a valid issue outside the expected set. Reports include
observed prune, rollover, and summary transitions. An empty transition list is a measured zero;
absent instrumentation is reported as unmeasured.
For follow-up cases, `expectedResolvedFollowUpIds` and `expectedUnresolvedFollowUpIds` partition
the supplied prior-review IDs. First-trial metrics report resolved recall and unresolved retention,
with missed and erroneous resolutions listed by ID. Incomplete trials receive no resolution credit.
Resolution scores measure first-trial `ReviewOutcome.resolutions` claims; the Action separately
gates whether GitHub dismisses prior reviews.
Judgment sets use version 2 and bind to both the exact observations and a digest of the case kinds,
expected defects, and follow-up resolution oracles. After changing an oracle, readjudicate its
findings; old judgment sets fail closed. Quality reports use version 7.
Reports also count attempted research delegations, native starts/completions/failures/interruptions,
and incomplete child results. A refusal can count as a delegation without starting a child, and
a completed child can still report incomplete research. Missing research instrumentation remains
unmeasured rather than being interpreted as zero.

Reports distinguish succeeded, incomplete, and failed trials. `incomplete` and `exhausted` results
still contribute findings, tokens, and cost, but cannot pass a clean control or count as a complete
case. Current runs refuse completion while diff ranges remain unread and retain a model's specific
missing-evidence reason as `blockedOn`; a bare model-authored incomplete flag is no longer accepted.

Run `vp run pr-review-eval -- --help` from the repository root. Validate the case selection before
writing output. The runner appends each completed trial to a new exclusive file and keeps finished
rows after interruption. Reports reject empty files, incomplete grids, malformed trailing lines,
and mismatched trial or case selections. Keep private cases and raw results in the ignored `data/`
and `results/` directories.

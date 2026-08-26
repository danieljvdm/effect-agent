# Pull-request review

## 1. Scope

`@effect-agent/pr-review` is a provider-neutral, ephemeral review agent. A host supplies a bounded
pull-request diff and receives a structured report. GitHub events, REST calls, model selection,
review history, and publication are channel concerns and live in the private
`packages/pr-review-action` workspace.

The design deliberately favors a small number of independent review passes over a continuing
conversation. Review output is advice, not durable agent state.

## 2. Package contract

- **PRR-001**: The package input and output are Effect Schemas. The input contains pull-request
  metadata, complete admitted patches, revisions, and explicit unavailable paths. The output
  contains a summary and at most twelve findings. Each finding uses one bounded category:
  `correctness`, `security`, `concurrency`, `performance`, `resources`, `reliability`,
  `error-handling`, `testing`, `maintainability`, `style`, or `docs`.
- **PRR-002**: One invocation performs exactly one `AgentRuntime.run`, with one model turn and an
  empty Toolkit. The package does not fan out, retry model calls, spawn subagents, or resume model
  context.
- **PRR-003**: Token, duration, findings, file, and patch bounds are finite. The outcome reports
  observed uncached, cached, cache-write, total input, and output tokens plus an optional host-priced
  cost estimate. A host must identify paths it did not admit rather than imply complete coverage.
  The reviewer must not infer that an absent file was unchanged because the host may have withheld
  generated, ignored, unavailable, or oversized patches.
- **PRR-004**: Model output is untrusted. Before returning it, the package removes findings for
  unknown paths, deduplicates them, and removes line anchors that are not RIGHT-side added or
  context lines in the supplied patch. Removing an invalid anchor keeps the finding available for
  top-level publication.

The package has no provider, platform, GitHub, CLI, or Action dependency. Its only public export is
the review contract and reviewer constructor.

## 3. GitHub channel

- **PRR-005**: The GitHub channel owns webhook selection, credentials, API decoding, diff
  admission, ignore policy, provider binding, current-head checks, rendering, and publication. It
  deterministically partitions a large admitted diff into at most four size-balanced shards, runs
  one package invocation per shard in a single structured-concurrency wave, and merges at most
  twelve findings. It never retries a shard or starts a second wave within one Action run. Each
  shard is limited to 48,000 input and 8,000 output tokens, bounding one wave to 192,000 input and
  32,000 output tokens.
- **PRR-006**: Automatic mode admits a consumer-configured non-negative number of review waves,
  defaulting to two. Configuration validation does not cap this setting; each admitted wave retains
  the per-wave bounds in PRR-005. Zero disables automatic reviews and notices. For a positive limit,
  the first automatic event after the configured count publishes one deterministic closing review
  without model execution. It reports the attempt count, last completed review, current head, and
  manual commands that remain available. Later automatic events at the same configured limit exit
  without publication. The final admitted attempt also visibly states that automatic reviews are
  paused. Failed waves count toward this limit and show the same pause notice, but never become
  incremental baselines. A trusted prior attempt is a terminal marker-bearing review authored by
  the configured GitHub Bot login; arbitrary comments and model-authored marker text cannot advance
  or reset the counter.
- **PRR-007**: A repository owner, member, or collaborator may request another incremental review
  with the exact comment `@effect-agent review`, or a full review with
  `@effect-agent review full`. The previous `/effect-agent review` forms remain compatibility
  aliases. The bundled channel ignores trailing whitespace but rejects trailing prose before model
  execution. It acknowledges an admitted manual command with an `eyes` reaction before reading
  pull-request state or starting model work. Manual reviews do not count as automatic reviews or
  resume them. An incremental command never authorizes a full-diff model review.
- **PRR-008**: An attempt marker stores only its version, whether the attempt was automatic, and
  whether it produced a report. A separate closing marker stores only its version and the configured
  automatic limit, making the no-model notice idempotent without counting it as another attempt.
  GitHub's review `commit_id` is the baseline for completed attempts. No transcript, finding
  continuity, signature, fingerprint, retry queue, or assurance state is persisted. Incremental
  scope comes from an exact comparison of the baseline and current commit trees across current
  pull-request paths, so amended, rebased, and force-pushed heads use the same mechanism as ordinary
  pushes. Changed paths are hydrated from the current pull-request diff. Missing or truncated trees
  and missing completed baselines make no model call rather than widening scope. Inline findings are
  revalidated against the current pull-request diff.
- **PRR-009**: The channel publishes one review only after the model settles, against the commit it
  inspected. Blocking findings use GitHub's `REQUEST_CHANGES` event and fail the Action after
  publication. Nonblocking and failed reviews remain `COMMENT` events, so a partial later pass cannot
  clear an older blocking review; a maintainer must dismiss a resolved change request explicitly. A
  failed attempt publishes only an honest failure marker and no findings. A completed attempt
  revalidates inline anchors against that pull-request diff. Other findings remain advisory. The
  no-model closing review is derived only from trusted history and never claims that the current diff
  was inspected. The channel derives severity and category labels, summary callouts, and agent-ready
  prompt blocks from the validated report; presentation does not start another model Turn. Visible
  GitHub Markdown comes from a defaulted Effect reference that an embedding host may replace. The
  channel appends its trusted terminal marker outside that replaceable presentation so overrides
  cannot weaken wave accounting. For the known GPT-5.6 model ids, the default presentation includes
  a host-computed estimate using OpenAI's published standard token rates and links to that model's
  rate card. Unknown model ids omit cost.

The workflow checks out the trusted default branch, serializes runs for the same pull request, and
passes the webhook head SHA so stale queued events stop before model execution. It does not cancel
in-flight model work: cancellation would prevent the attempt marker from being written and let a
rapid push sequence evade the automatic-attempt bound.

## 4. Input admission

The GitHub channel admits at most 100 files, 80,000 characters per patch, and 320,000 patch
characters per pass. Missing, oversized, or excess patches are listed as unavailable. Configured
ignored files are outside review scope. These limits bound spend; they are not a semantic coverage
claim.

## 5. Verification

Deterministic tests pin the diff-line parser and output sanitization (`PRR-004`) and the complete
sharding, parallel-wave, merge, and automatic/manual trigger lifecycle (`PRR-005`–`PRR-007`).
Provider behavior remains outside the offline test suite.

## 6. Model-quality evaluation

The private `examples/pr-review-eval` leaf replays versioned, schema-encoded `ReviewRequest` cases
through the public reviewer boundary. It records the exact input digest, complete model
configuration, trial index, elapsed time, and either the validated `ReviewOutcome` or one bounded
typed failure. A trial remains one package invocation under PRR-002; the eval runner may repeat a
case, but it never turns those repeats into retries within an invocation.

Committed tests use a deterministic model and make no provider request. Live trials require the
same explicit opt-in gate as other live-model evidence and do not gate ordinary pull requests.
Private source and raw live results are not committed. Expected defects are human-authored semantic
invariants with bounded evidence, not strings that model output must copy. Human adjudication is
the initial source of truth.

The primary quality claim concerns the first trial. Later identical trials measure instability and
cannot convert a first-trial miss into a pass. The leaf may report recall, precision, later-only
expected blockers, typed failures, tokens, cost when supplied by the host, and elapsed time. It does
not add a database, hosted eval service, transcript store, model grader, or production retry path.

Human judgments bind to a digest of the complete schema-encoded observation set and identify the
case, variant, trial, and emitted finding index. They distinguish expected-defect matches, new
valid defects, invalid findings, and unclear findings. Missing judgments remain explicit. A
finding satisfies blocker recall only when it matches an expected blocker and the reviewer emitted
it at blocking severity, because lower severity does not stop publication. Precision remains
unresolved while a finding is unclear or unjudged. The pure offline reduction rejects mixed case
identities, model configurations, runner versions, or incomplete trial grids rather than silently
combining them. “Overall” metrics aggregate each variant across its common case set; variants are
never averaged together. New valid findings remain explicit corpus-repair candidates because the
bench does not invent identities for them from model-authored prose.

A named eval candidate may use a built-in model-only request presentation through the reviewer
constructor. Such a presentation preserves every schema-encoded request field, patch byte, and
file order; it does not replace canonical input, change its digest, or alter PRR-002. The production
channel retains the default presentation until a frozen comparison meets its decision rule.

Each live invocation writes one stable, caller-named variant to a new observation file. Offline
reporting may combine up to eight such files, while retaining the same compatibility checks.

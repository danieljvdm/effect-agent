# Pull-request review

## 1. Scope

`@effect-agent/pr-review` is a provider-neutral, ephemeral review agent. A host supplies a bounded
pull-request diff and receives a structured report. GitHub events, REST calls, model selection,
review history, and publication are channel concerns and live in the private
`examples/pr-review` leaf.

The design deliberately favors a small number of independent review passes over a continuing
conversation. Review output is advice, not durable agent state.

## 2. Package contract

- **PRR-001**: The package input and output are Effect Schemas. The input contains pull-request
  metadata, complete admitted patches, revisions, and explicit unavailable paths. The output
  contains a summary and at most twelve findings.
- **PRR-002**: One invocation performs exactly one `AgentRuntime.run`, with one model turn and an
  empty Toolkit. The package does not fan out, retry model calls, spawn subagents, or resume model
  context.
- **PRR-003**: Token, duration, findings, file, and patch bounds are finite. The outcome reports
  observed input and output tokens. A host must identify paths it did not admit rather than imply
  complete coverage.
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
- **PRR-006**: Automatic mode admits at most two review waves: the initial wave and one later wave.
  This bounds automatic review to eight one-turn model invocations per pull request. A third
  automatic event exits successfully without model execution. Failed waves count toward this
  limit but never become incremental baselines. A trusted prior attempt is a
  terminal marker-bearing review authored by the configured GitHub Bot login; arbitrary comments
  and model-authored marker text cannot advance or reset the counter.
- **PRR-007**: A repository owner, member, or collaborator may request another incremental review
  with the exact comment `/effect-agent review`, or a full review with
  `/effect-agent review full`. Manual reviews do not count as automatic reviews.
- **PRR-008**: The marker stores only its version, whether the attempt was automatic, and whether
  it produced a report. GitHub's review `commit_id` is the baseline for completed attempts. No
  transcript, finding continuity, signature, fingerprint, retry queue, or assurance state is
  persisted. If GitHub cannot compare the baseline with the current head, the channel reviews the
  current full diff.
- **PRR-009**: The channel publishes one `COMMENT` review only after the model settles, against the
  commit it inspected. A failed attempt publishes only an honest failure marker and no findings. A
  completed attempt revalidates inline anchors against that pull-request diff. Blocking findings
  fail the Action after publication; other findings remain advisory. The channel derives severity
  and category labels, summary callouts, and agent-ready prompt blocks from the validated report;
  presentation does not start another model Turn. Visible GitHub Markdown comes from a defaulted
  Effect reference that an embedding host may replace. The channel appends its trusted terminal
  marker outside that replaceable presentation so overrides cannot weaken wave accounting.

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

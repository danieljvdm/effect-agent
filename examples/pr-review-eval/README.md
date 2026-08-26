# PR-review eval

This private leaf measures the model-dependent part of pull-request review. It replays a versioned
`ReviewRequest` through the real `@effect-agent/pr-review` package and writes one schema-encoded
observation per independent trial.

It does not fetch pull requests, shard diffs, publish GitHub reviews, or ask another model to grade
findings. Saved judgments with a named adjudicator and source evidence are the scoring authority.

## Data

An eval suite is one JSON file containing one or more cases. Each case carries the exact review
request, its SHA-256 digest, provenance, and semantic expected defects. Expected defects describe
behavior and evidence. They do not prescribe model wording.

Commit only source that is safe to redistribute. Put private corpora under `data/` and raw results
under `results/`; both directories are ignored in this workspace.

`fixtures/smoke-suite.json` is a synthetic known-defect/control pair for exercising the bench. It
is not a model-quality benchmark.

`fixtures/public-effect-agent-v1.json` is a redistributable quality screen reconstructed from
public Effect Agent pull requests. It contains source-adjudicated semantic oracles and preserves
prior automated reviews as provenance rather than ground truth; each provenance record names any
generated paths omitted by the original review.

`fixtures/public-effect-agent-v2.json` contains additional public challenge cases, including
nonblocking defects and a clean control. Historical review claims that failed source verification
were removed before candidate execution; the corrections are recorded in issue #191.
Source-verified nonblocking defects discovered during that screen were added afterward as corpus
repairs, not counted as candidate gains.

For each expected defect, trace a supported input or interleaving through its callers to an
observable incorrect result. A missing defensive check alone does not establish a defect, and a
prior review's severity is not evidence that a change must block merge.

Validate a suite without credentials or model calls:

```sh
vp run pr-review-eval -- \
  --cases data/cases.json validate
```

Use `fixtures/smoke-suite.json` in place of the private path for a credential-free smoke check.

## Live trials

Live execution requires the repository's explicit opt-in gate and an OpenAI API key. The defaults
match the current PR-review Action: `gpt-5.6-sol`, medium reasoning effort, 8,000 output tokens,
strict structured output, and `store: false`.

```sh
mkdir -p examples/pr-review-eval/results

EFFECT_AGENT_LIVE=1 OPENAI_API_KEY=... \
vp run pr-review-eval -- \
  --cases data/cases.json \
  run --variant current --output results/current.jsonl --trials 5
```

The command refuses to overwrite an existing result file. Use repeated `--case` flags on `run` to
select a subset. `--concurrency` is bounded from one to four and defaults to one.

The output is created before model execution. Each completed trial is appended and synchronized
immediately, so a later interruption preserves earlier observations. A write failure stops the run
and interrupts active calls; calls already started may still incur cost. The file remains available
for inspection after failure, including a possibly incomplete final line after an I/O failure.

Give each baseline or candidate a stable `--variant` ID. Run variants independently so a provider
failure cannot destroy an earlier result file. The report command accepts up to eight repeated
`--observations` files and rejects incompatible case sets, model configurations, or trial grids.

The shipped clean control can exercise one live call without exposing private source:

```sh
EFFECT_AGENT_LIVE=1 OPENAI_API_KEY=... \
vp run pr-review-eval -- \
  --cases fixtures/smoke-suite.json \
  run --case optional-read-control --output results/smoke.jsonl --trials 1
```

Each trial is one independent reviewer invocation. The runner records typed trial failures, but it
does not retry them. Defects and interruption stop the run.

Stop a candidate when a saved finding proves a preregistered rejection condition. Keep the
observations and judgments, but treat the stopped run as partial. Do not shrink the intended trial
grid to make its report look complete.

## Judgments and reports

The offline report command binds a judgment set to the SHA-256 digest of the exact observations it
labels. Run it once without judgments to get that digest and an explicit list of unjudged finding
references:

```sh
vp run pr-review-eval -- \
  --cases data/cases.json \
  report --observations results/current.jsonl \
  --observations results/candidate-guidance-v1.jsonl \
  --trials 5 \
  --output results/unjudged-report.json
```

Create `data/judgments.json` with the reported `observationSetDigest`. Each judgment identifies one
case, variant, trial, and zero-based finding index; records the adjudicator and rationale; and uses
one of `matches-expected`, `new-valid`, `invalid`, or `unclear`. A `matches-expected` row names one
or more case defect IDs. The other labels leave `matchedDefectIds` empty. Supply the same complete
set of observation files when generating the digest and the final report.

Then write the adjudicated report:

```sh
vp run pr-review-eval -- \
  --cases data/cases.json \
  report --observations results/current.jsonl \
  --observations results/candidate-guidance-v1.jsonl \
  --trials 5 \
  --judgments data/judgments.json --output results/report.json
```

The primary recall and precision measures use trial one. Repeats expose later-only blockers but do
not repair the first-pass score. A matched blocker counts toward blocker recall only when the model
also emitted it as `blocking`; an `important` finding would not stop the pull request. Precision is
left unresolved while any relevant finding is unclear or unjudged. Blocker detection separately
counts expected blockers found at any severity. Blocking precision treats an invalid blocking
finding, or one matching only a non-blocking expected defect, as overstated; new valid, unclear, and
unjudged blocking findings leave it unresolved. New valid findings remain listed for corpus repair
instead of being matched by prose. Reporting is deterministic and offline. `report --trials` is
required and must match the intended run. By default every suite case is required; repeat the same
`--case` selection used by `run` to report an intentional subset. Empty files, malformed lines, and
missing trials fail rather than turn an interrupted prefix into a smaller successful run. Each
variant report aggregates that declared case set; variants are not averaged together.

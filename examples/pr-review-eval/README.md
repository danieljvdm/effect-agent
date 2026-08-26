# PR-review eval

This private leaf measures the model-dependent part of pull-request review. It replays a versioned
`ReviewRequest` through the real `@effect-agent/pr-review` package and writes one schema-encoded
observation per independent trial.

It does not fetch pull requests, shard diffs, publish GitHub reviews, or decide whether a finding
is valid. Human judgments and comparison reports belong to issue #176.

## Data

An eval suite is one JSON file containing one or more cases. Each case carries the exact review
request, its SHA-256 digest, provenance, and semantic expected defects. Expected defects describe
behavior and evidence. They do not prescribe model wording.

Commit only source that is safe to redistribute. Put private corpora under `data/` and raw results
under `results/`; both directories are ignored in this workspace.

`fixtures/smoke-suite.json` is a synthetic known-defect/control pair for exercising the bench. It
is not a model-quality benchmark.

Validate a suite without credentials or model calls:

```sh
vp run @effect-agent/example-pr-review-eval#eval -- \
  --cases data/cases.json validate
```

Use `fixtures/smoke-suite.json` in place of the private path for a credential-free smoke check.

## Live trials

Live execution requires the repository's explicit opt-in gate and an OpenAI API key. The defaults
match the current PR-review Action: `gpt-5.6-sol`, medium reasoning effort, 8,000 output tokens,
strict structured output, and `store: false`.

```sh
EFFECT_AGENT_LIVE=1 OPENAI_API_KEY=... \
vp run @effect-agent/example-pr-review-eval#eval -- \
  --cases data/cases.json \
  run --output results/current.jsonl --trials 5
```

The command refuses to overwrite an existing result file. Use repeated `--case` flags before the
subcommand to select a subset. `--concurrency` is bounded from one to four and defaults to one.

The shipped clean control can exercise one live call without exposing private source:

```sh
EFFECT_AGENT_LIVE=1 OPENAI_API_KEY=... \
vp run @effect-agent/example-pr-review-eval#eval -- \
  --cases fixtures/smoke-suite.json --case optional-read-control \
  run --output results/smoke.jsonl --trials 1
```

Each trial is one independent reviewer invocation. The runner records typed trial failures, but it
does not retry them. Defects and interruption stop the run.

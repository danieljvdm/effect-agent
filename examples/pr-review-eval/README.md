# PR-review eval

Replay saved PRs through the reviewer and score findings against adjudicated defects.

- Validate offline: `vp run pr-review-eval -- --cases fixtures/smoke-suite.json validate`.
- Live runs require `EFFECT_AGENT_LIVE=1` and `OPENAI_API_KEY`.
- Run `vp run pr-review-eval -- --help` for commands and options.
- Public cases live in `fixtures/`; private cases and results belong in ignored `data/` and `results/`.

## Scoring and output

This bench replays saved requests against adjudicated defects.
Public fixtures are examples and do not support a quality claim. Live trials require
`EFFECT_AGENT_LIVE=1` and provider credentials.

Score the first trial separately. Detection accepts an expected blocker at any severity, while
blocking recall requires `blocking`. Later trials measure instability and cannot repair a first
trial miss. Bind named judgments to the exact observation digest. Add a new corpus defect when the
model finds a valid issue outside the expected set.

Reports distinguish succeeded, incomplete, and failed trials. `incomplete` and `exhausted` results
still contribute findings, tokens, and cost, but cannot pass a clean control or count as a complete
case.

Run `vp run pr-review-eval -- --help` from the repository root. Validate the case selection before
writing output. The runner appends each completed trial to a new exclusive file and keeps finished
rows after interruption. Reports reject empty files, incomplete grids, malformed trailing lines,
and mismatched trial or case selections. Keep private cases and raw results in the ignored `data/`
and `results/` directories.

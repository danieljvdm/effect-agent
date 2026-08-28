# PR-review eval

Replay saved PRs through the reviewer and score findings against adjudicated defects.

- Validate offline: `vp run pr-review-eval -- --cases fixtures/smoke-suite.json validate`.
- Live runs require `EFFECT_AGENT_LIVE=1` and `OPENAI_API_KEY`.
- Run `vp run pr-review-eval -- --help` for commands and options.
- Public cases live in `fixtures/`; private cases and results belong in ignored `data/` and `results/`.

See the [evaluation guide](../../docs/guide/testing.md#review-quality-evaluation) for scoring and output rules.

# PR-review eval

Replay frozen PR inputs and independently judge every original candidate, including suppressed
and unresolved feedback. Verification remains experimental until a frozen heldout comparison
supports rollout. Public fixtures and deterministic tests do not establish model quality.

Run commands from the repository root. The task resolves file paths from `examples/pr-review-eval`.
Private cases and observations belong in that example's ignored `data/` and `results/` directories.

```sh
vp run pr-review-eval -- --cases fixtures/verification-corpus.json validate
vp run pr-review-eval -- --help
```

## Freeze before paid execution

`fixtures/verification-corpus.json` contains seven adjudicated cases and the immutable #287
operational replay. The frozen comparison contains 48 trials with a $47.999952 ceiling. The cleanup
pair is established by `test/cleanup-oracle.test.ts` against the pinned Effect runtime. Both
variants and the #287 replay stay together in development. The related publication cases stay
together in heldout.

The 2026-09-02 comparison at reviewer revision `1feba3b18972374252daba30f379897a144b11b7`
completed all 48 trials for an estimated $6.051385. The Action stays on baseline: verification
added an incomplete heldout first trial on `incremental-publication-exposure`. The initial report
left candidate validity unjudged. No automated judge calls or replacement trials were run within
that frozen comparison.
The report separates confirmed discovery misses and trial instability from results awaiting
candidate judgments.

| Strategy | Trials | Incomplete | Failed | Estimated cost | Total trial time |
| -------- | -----: | ---------: | -----: | -------------: | ---------------: |
| Baseline |     24 |          1 |      0 |      $2.554359 |        716.281 s |
| Verified |     24 |          3 |      0 |      $3.497026 |       1005.608 s |

These totals include the unadjudicated replay. Among the three heldout first trials, baseline
had no incomplete results and verification had one. Later trials varied and do not replace those
first-trial results. The local freeze is `data/kom-28/frozen.json`; raw observations, the unjudged
report, and the independent-review packet are in `results/kom-28/`. These directories remain
ignored. The packet retains 19 scored original candidates and omits verifier decisions; six
operational-replay candidates remain available in the raw observations outside the scored packet.

Subsequent source review by three agents, without verifier decisions, adjudicated all 25 original
candidates. This is agent adjudication, not human judgment. All 19 scored candidates describe
valid defects. Of the six replay candidates, three are valid, two are invalid, and one allocation
claim remains unclear without a measured resource failure. The replay remains outside
clean-control and blocker-recall denominators.

Oracle version 2 adds `consolidated-handoff-omitted`, an important defect reported by two candidates.
Executing the exact frozen renderers with the same valid input produced 36,792-character review
bodies on both sides. Both omitted the consolidated prompt, but the base's 556-character inline
comment retained its handoff while the head's 70-character comment removed it. The correction
preserves every request and source digest. `results/kom-28/agent-adjudicated.json` binds the agent
judgments to `oracle-v2.json` and `rescored-v2.jsonl`; each rescored observation links its original
digest. Original observations and unjudged reports remain intact.

Across the four scored blockers, baseline discovered and published two. Verification discovered
all four but published only two as blockers: one valid candidate remained unverified after invalid
evidence references, and discovery labeled another baseline blocker as important. Neither strategy
produced an independently judged false blocking finding on heldout first trials, so the required
reduction in false blockers is not established.

The follow-up implementation validates verifier submissions before accepting completion. It lets
the same verifier correct rejected references within the existing shared limits and accepts
continuous source coverage across adjacent reads. Separate seeded development probes exercise
verification directly against known true and false claims. They cannot replace the original
first trials or establish rollout eligibility.

At revision `7e9f08855bb8ef2921ac3d57ed3c4c68dcc49c58`, all nine seeded verifier probes produced
the expected disposition and completed verification: three false original-effect leak claims
were refuted, three real cached-wrapper leaks were supported, and three real publication
overflows were supported. Total estimated cost was $0.802109 against a separate $8.999991 ceiling.
The manifest, executable probe, and observations are retained under `results/kom-28/` as
`seeded-probe-manifest.json`, `seeded-probe.mts`, and `seeded-probe.jsonl`.
These probes inject discovery findings without paid discovery. Consequently, the host retains
pending patch coverage in the aggregate review outcome; completion here means the verifier
stage's `diagnostics.verification`, not whole-review completion. Expected dispositions remain
assessment metadata and are never sent to the model.

Every comparison case supplies `oracleVersion`, `split` (`development` or `heldout`),
`relatedGroup`, and a digest-checked repository snapshot. Keep related revisions and both cleanup
variants in the same split. Establish clean controls independently of earlier empty reviews.
`unadjudicated` cases are operational replays. They have no expected defects and never enter clean
control or blocker recall denominators. They still consume the trial budget.

The freeze command records request, source, and oracle identities; the exact reviewer Git revision;
discovery and verification prompt digests; guidance; model settings; stage allocations; limits;
pricing; and cache policy. Oracle identity includes case kind, expected defects, and their severity.
It refuses a dirty checkout and makes no model calls.

```sh
vp run pr-review-eval -- --cases ./data/cases.json freeze --variant kom-28 --output ./data/frozen.json
vp run pr-review-eval -- --cases ./data/frozen.json run --output ./results/paired.jsonl
```

Live execution requires `EFFECT_AGENT_LIVE=1` and `OPENAI_API_KEY`. Supply the same optional
`--guidance path` at freeze and run time. Any configuration drift refuses execution. `--variant`
labels the comparison. `--strategy baseline` and `--strategy verified` select implementations;
paired runs require both, which is the default when the flag is omitted.

Freeze at most 20 adjudicated cases across both splits, two strategies, and three trials each.
All trials, including operational replays, must fit 120 runs. The maximum authorized comparison
cost is 119,999,880 microdollars, or $119.999880. This command has no warmups or automatic reruns.
Do not start another fresh-ledger run to replace a failed first trial. Any additional run requires
remaining explicitly budgeted capacity or a separately authorized budget.

Execution is serial. Cases are sorted by ID; trial numbers advance together across the corpus.
The first strategy alternates for each case/trial pair. Each trial allocates a fresh $0.999999
ledger and an isolated cache namespace. Baseline gives discovery the full allowance. Verified
gives discovery 699,999 microdollars and holds 300,000 for verification. Both share the same
five-minute deadline, 64 turns, 64 tool calls, and 24-candidate capacity. Token usage records actual
cache reads and writes; trial number never implies cache state. Prices are estimates, not invoices.

The runner appends each returned trial, including typed failures and incomplete outcomes, to a
new exclusive JSONL file and flushes it before proceeding. Finished rows survive interruption.
Defects and interruption propagate after cleanup. Available host diagnostics are emitted as one
bounded `eval-trial-finalization` JSON record on stderr, without raw causes or model reasoning.
This diagnostic does not turn an interrupted trial into a completed observation, and the missing
grid keeps the comparison ineligible for rollout.

## Independent judgments and rollout

```sh
vp run pr-review-eval -- --cases ./data/frozen.json report --observations ./results/paired.jsonl --trials 3 --output ./results/unjudged.json
vp run pr-review-eval -- --cases ./data/frozen.json report --observations ./results/paired.jsonl --judgments ./data/judgments.json --trials 3 --output ./results/judged.json
```

The unjudged report supplies `candidateId`, `observationDigest`, `oracleDigest`, and the original
candidate. Bind each named human judgment to those identities and the report's
`observationSetDigest`. Judge validity independently of verifier outcomes. `matches-expected`
names defect IDs from the oracle. `new-valid` retains a newly discovered defect and its independently
judged `severity`; it requires an oracle correction before rollout. Legacy finding-index judgments
remain readable for historical data, but cannot authorize a frozen comparison.

Reports separate discovery misses, valid candidates wrongly refuted, valid candidates withheld,
published false blockers, incomplete/failed outcomes, actual usage, estimated cost, and elapsed
time. Unresolved feedback is withheld from supported blocker counts. Pending patches and excluded
paths make the trial incomplete even if discovery declared its supplied patches assessed.
First trials determine recall and rollout. Later trials expose instability and never replace a miss.

The rollout decision uses adjudicated heldout first trials only. Eligibility requires all relevant
judgments resolved, fewer false blockers, every baseline-detected blocker preserved without
downgrading baseline blocking findings, no valid blocker wrongly refuted, and no additional
incomplete or failed first trials. Mixed or inconclusive results stay `experimental`. An `eligible`
report does not enable the Action automatically. Record the measured decision in the implementation
PR before changing the shipping strategy. This small comparison cannot establish general quality.

## Correct an oracle without repeating inference

Increment `oracleVersion` for each corrected case and remove its old `oracleDigest`. Keep requests,
source snapshots, splits, and related groups unchanged. The rescore command requires the complete
original paired grid. It keeps the frozen original configurations and results, writes a new freeze,
and records each predecessor observation digest. Original files remain unchanged.

```sh
vp run pr-review-eval -- --cases ./data/corrected-cases.json rescore --previous-cases ./data/frozen.json --observations ./results/paired.jsonl --correction-id kom-28-oracle-2 --frozen-output ./data/corrected-frozen.json --output ./results/rescored.jsonl
```

Re-adjudicate against the new observation and oracle digests, then report both strategies together.
Reports reject stale oracle identities, empty files, incomplete grids, duplicate trials, malformed
rows, reused cache namespaces, changed configurations, and incorrect execution order.

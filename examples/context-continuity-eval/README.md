# Context continuity evaluation

A real OpenAI model maintains one evolving project across 13 user updates and at least 12 native
rollovers. The model uses public context-history and durable-notes tools. The host never supplies
a summary or restores notes into the prompt for it. Reference answers stay outside model context.

All live profiles keep the same corrected-decision, unfinished-work, bounded-notes, exact-receipt,
original-source, and window-age checks. Receipt codes vary by seed. Delayed lookups must search and
read original retained evidence, including a source covered by ten windows, whose answer is absent
from notes and the current prompt. Citation chains are allowed; citing a recent recollection alone
fails. No second model awards a subjective score.
The oracle identifies the original from the accepted archive input and its first canonical model
response. An aged copied tool result or later transcript cannot substitute for that source.

| Profile                                 | Context pressure and recovery                                                                                                                                                                                | Coverage limits                                                                                                                                                    |
| --------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `explicit-rollover-sqlite-v1` (default) | Model requests native rollovers at a 16k estimated limit; services close and reacquire within one process.                                                                                                   | Continuity and SQLite reacquisition; no pressure or process-kill claim.                                                                                            |
| `pressure-restart-sqlite-v1`            | Two synthetic manifest pages create pressure at 16k. `new_context` is unavailable. Native compactor observations must match committed windows. A supervisor confirms two SIGKILLs and different process IDs. | Reduced-window pressure and actual Node process death, not production capacity.                                                                                    |
| `pressure-cloudflare-v1`                | The same pressure scenario uses `CloudflareThreadClient.submit` and `ThreadObject.make`, native alarms and SQLite. Two `ctx.abort()` boundaries force Durable Object eviction and reconstruction.            | Local workerd tests establish host API integration. Deployed-host acceptance requires its own explicit run. Eviction is not an OS process kill or a CPU benchmark. |
| `production-capacity-v1`                | `--validate` prepares the scalable manifest workload and input-cost planning for an explicit application context limit.                                                                                      | Preparation only: live dispatch is disabled. Neither reduced-window profile proves full production capacity.                                                       |

The pressure workload fills the window with synthetic tool observations; this is deliberately
induced pressure, not a claim that a production conversation naturally accumulated that volume.
The compactor, estimator, original history, and notes implementations remain native. Recovery
boundaries occur immediately before and after a canonical rollover append. Process recovery
reuses persisted usage and refuses any unsettled provider reservation. Cloudflare confirms its
evaluator-state writes before eviction; the next incarnation reads the same native notes revision.

The follow-up includes the native [recovery checkpoint capability from #380](https://github.com/danieljvdm/effect-agent/pull/380).
`ThreadStore.recoveryCheckpoints` is an optional, latest-only cache bound to a canonical batch tail;
it is separate from this evaluator's `resume.json` and Cloudflare audit/phase bookkeeping. Native
recovery validates the cache and replays its suffix, or falls back to complete canonical replay
when the cache is absent, rejected or incompatible. The canonical log and submission ledger
remain authoritative. The evaluator observes public cache metadata in `recovery-checkpoint.json`
(SQLite) or `host-snapshot.json` (Cloudflare), distinguishing missing/rejected caches from storage
failures. It keeps the full canonical transcript and original-source oracle independent of that cache.

The deterministic SIGKILL and workerd tests require a valid native checkpoint covering the last
committed rollover, alongside the existing recovery, usage and original-history checks. Checkpoint
presence does not establish fast-path selection, bounded startup work, or latency/CPU improvement.
Actual hosted Cloudflare invocation latency and CPU remain open under
[#356](https://github.com/danieljvdm/effect-agent/issues/356). The green live baseline in #372 is tied
to `ad4b70a557b6e561e8471eae2fd3b57373bfdb3b`, before #380, and provides no live acceptance claim
for this newer source. Workspace-source verification does not establish beta64 publication; the
release coordinator owns that receipt. No paid profile runs solely because a runtime merge lands.

Run `vp run context-continuity-eval --help` for configuration. One bounded live attempt:

```sh
EFFECT_AGENT_LIVE=1 vp run context-continuity-eval --require-clean --model gpt-6-astra \
  --profile pressure-restart-sqlite-v1 --max-cost-usd 10 --output-dir /tmp/context-eval-1
```

Supply `OPENAI_API_KEY` through the environment or an existing `--env-file`. Each attempt needs a
new output directory. The CLI checks source identity before and after a run. `--validate` makes no
model calls and cannot pass the live gate. Preserve failures; rerunning adds evidence rather than
repairing the first result. Never repeat an unchanged candidate just to get a passing sample.

Nightly and confirmed-unpublished-release jobs continue to run exactly the existing default
profile with a $10 ceiling. Manual Actions dispatch can select one bounded SQLite profile. There
is no paid PR trigger or profile matrix. Cloudflare and production-capacity runs are not added to
automatic jobs. Published versions skip evaluation/publication; registry errors fail closed.

For a Cloudflare host, build from a clean candidate with
`vp run -F @effect-agent/example-context-continuity-eval build`. The bundle embeds its commit and
clean/dirty state. `wrangler.jsonc` is a deployment template for a separately selected account and
evaluation Worker. Configure `OPENAI_API_KEY` and `CONTEXT_EVAL_TOKEN` as secrets. Deployment is an
explicit operation; the build and runner never deploy. The live runner requires an already deployed
HTTPS endpoint, the same exact clean source, model, seed 17, low effort, and $10 cap:

```sh
EFFECT_AGENT_LIVE=1 vp run context-continuity-eval --require-clean --model gpt-6-astra \
  --profile pressure-cloudflare-v1 --cloudflare-url https://YOUR-EVAL-WORKER.workers.dev \
  --output-dir /tmp/context-eval-cf-1
```

Export the matching `CONTEXT_EVAL_TOKEN` locally. The Worker protects all routes with that token and
uses a fresh Thread for each attempt. The test fixture intercepts all OpenAI traffic and never
contacts a paid endpoint. Its scripted answers live only in `test/`, outside the deployed bundle.

Hosted acceptance needs an explicitly selected isolated account/Worker, deployment credentials,
permission to create its SQLite DO namespace, and an owner for evidence export and teardown.
Existing consumer deployment permission does not select this target. Build and dry-run the exact
reviewed commit, preserve bundle hashes and the deployment/version IDs, then verify the authenticated
host identity before submitting one fresh Thread. A local bundle or dry-run is not deployment proof.

The template enables unsampled invocation logging. Collect Cloudflare's invocation CPU/wall-time
records, request IDs, DO IDs, outcomes and deployment version over the run's UTC interval; confirm
log-query access and units before the attempt. Collect client submit/response and phase-completion
latency separately: Worker wall time includes I/O and is not client response latency. Correlate the
two eviction boundaries with the new incarnations and preserve missing samples as missing evidence.
Namespace CPU aggregates can supplement the report, but cannot isolate a particular recovery.
See Cloudflare's [DO metrics](https://developers.cloudflare.com/durable-objects/observability/metrics-and-analytics/)
and [Worker metrics](https://developers.cloudflare.com/workers/observability/metrics-and-analytics/).

Budget hosted continuity separately: one model attempt remains capped at $10, plus an explicit
Cloudflare allowance for invocations, active duration, SQL rows, retained storage and logs. A proposed
15-minute collection window with one active 128-MB DO consumes at most 115.2 GB-s of DO duration
(about $0.00144 at the current marginal rate); this excludes the calling Worker, SQL and logs.
Reserve $1 for those host resources, verify the account's rates/quotas, and stop collection at the
deadline. This is a proposed allowance, not authorization or an enforced Cloudflare billing cap.
Export evidence before deleting the isolated data and deployment; retained storage remains billable.
See [DO pricing](https://developers.cloudflare.com/durable-objects/platform/pricing/).

The distinct #356 performance case still needs a hosted benchmark adapter for the existing
1k/10k/100k-history recovery workload, with a fixed active suffix and cold/warm cohorts. Keep model
inference out of that benchmark, exclude seeding from recovery timing, and measure canonical reads,
checkpoint selection, invocation CPU and client latency together. The continuity Worker is ready
for isolated deployment preparation; its 12-window run alone does not close this scaling gate.

Prepare a full-capacity workload without inference:

```sh
vp run context-continuity-eval --validate --profile production-capacity-v1 \
  --production-context-tokens 200000
```

Here 200,000 is an illustrative planning target, not a verified production limit. The output shows
manifest sizes and the checked-in price estimate for twelve uncached requests at 80% of that
capacity. For `gpt-6-astra` that input-only scenario is $24 before outputs, repeated history reads,
and recovery; it does not fit the $10 ceiling. Caching may affect actual cost and long-context
pricing needs separate verification. Confirm the real capacity, model, host, and total budget
before enabling a separate full-capacity input ceiling. The existing 32k paid-input guard and $10
per-attempt ceiling stay enforced; selecting this planning profile never bypasses them.

Artifacts include synthetic canonical records, exact outgoing requests, model/source/scenario
identity, cumulative provider usage, conservative estimated cost, assertions, and profile-specific
kill or eviction evidence. Process artifacts also retain SQLite and each barrier's canonical log.
A partial report, provider outage, exhausted budget, missing credential, unsettled reservation, or
failed assertion is a failed gate. Pricing is an estimate, not an invoice. There are no inference
retries or model fallbacks; server-side conversation state and automatic truncation are disabled.

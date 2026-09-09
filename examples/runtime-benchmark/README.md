# Scripted runtime benchmark

This leaf consumer measures public production packages with deterministic Effect AI responses.
It makes no provider requests. Correctness assertions fail the command; latency changes are
informational until CI variance supports workload-specific relative and absolute thresholds.

Run from the repository root with Node 24 and the repository's Bun/Vite+ toolchain. Prepare three
checkouts, install each checkout's own lockfile, and build before measuring:

```sh
git worktree add --detach /tmp/effect-agent-base <exact-base-sha>
git worktree add --detach /tmp/effect-agent-reference 596cffba70716b1211ac02de949c4d0f31734b2f
vp install --frozen-lockfile
vp run -F './packages/*' build
cd /tmp/effect-agent-base
vp install --frozen-lockfile
vp run -F './packages/*' build
rm -f test/fixtures/checkpoints.d.ts test/fixtures/storage-upgrade.d.ts test/fixtures/storage-v2.d.ts
cd /tmp/effect-agent-reference
vp install --frozen-lockfile
vp run -F './packages/*' build
rm -f test/fixtures/checkpoints.d.ts test/fixtures/storage-upgrade.d.ts test/fixtures/storage-v2.d.ts
cd <candidate-checkout>
vp run perf:compare --base-dir /tmp/effect-agent-base --reference-dir /tmp/effect-agent-reference --profile pr --out-dir /tmp/performance-001
```

Use `--require-clean` for exact-commit evidence. A local dirty checkout is labeled in the report;
its measurements do not validate the committed SHA. Each output directory preserves one attempt
and cannot replace an existing report. Do not run builds, tests, or other benchmarks during a
measurement. `perf:compare` always bypasses task caching. `--help` describes the public arguments.
The cleanup lines remove only declaration artifacts emitted by older package builds in those
disposable checkouts. Every other modified or untracked file fails clean-checkout validation.

The PR workflow uses exact PR base/head commits, an immutable release reference, Node 24.20.0,
and sequential production builds. It runs three rotating cohorts (base/head/reference,
head/reference/base, reference/base/head). Each warm cohort has two warmups and three measured
samples per case: nine measured samples per revision. Workload order reverses between samples.
All warmups, measured samples, slow values, and failures remain in JSON artifacts. The trusted
comment workflow validates artifact data and current PR identity without executing candidate code.

`--profile smoke` exercises every workload family with one sample and 16 retained records;
it checks the command, not statistical confidence. `extended` takes 30 samples per revision and
adds 8,192 records. `archive` adds 100,000 records with nine measured samples. Larger profiles
are manual workflow-dispatch options and can take substantial time. Individual sample attempts,
including setup, are bounded to three minutes; PR child processes to five minutes. The controller
stops after 19 minutes, before the comparison step's 20-minute limit and the job's 30-minute limit,
and gives interrupted children five seconds to stop before forceful termination. Timeouts retain
partial evidence and fail correctness. No scheduled or paid execution is configured here.

| Case                      | Completed work and timing boundary                                                                                                                                                                                                                                                                                                                        |
| ------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Small run/stream          | One validated final answer and exactly one model invocation; warm operation begins after application Layer acquisition.                                                                                                                                                                                                                                   |
| Fragmentation             | Exactly 65,536 JSON response bytes in 1/64/1,024/4,096 deltas. Stream delivery must reproduce every chunk and the final answer. All four sizes run on PRs.                                                                                                                                                                                                |
| Prompt history            | A small answer with 64 KiB or 1 MiB of prior text. History creation occurs before timing.                                                                                                                                                                                                                                                                 |
| Parallel tools and rounds | Eight 2 ms tools per round, concurrency four, one or four rounds. Subsequent normalized provider requests must contain every successful result. Actual overlap, call bounds, and finalizers are checked.                                                                                                                                                  |
| Fresh durable submission  | A new Submission after 0/256/2,048 retained canonical records in a file-backed SQLite database. Each sample creates its own database; setup/seeding is excluded. Timing includes reopening the full Node durable runtime, admission, execution, and settlement.                                                                                           |
| Checkpoint recovery       | Persist a native rollover checkpoint, inject the public after-save fault, close the runtime, and reopen the same file. Measure resuming that same Submission separately from fresh submission. One completed tool must not run again; the resumed answer must be canonical.                                                                               |
| Settled ledger            | Seed settled adapter rows in a separate lane without growing canonical history. Measure reopening the Node runtime, fresh admission, a full public nonterminal scan, execution, and settlement. The scan must return only the new Submission. Synthetic ledger seeding is an adapter fixture, not evidence of a production canonical settlement protocol. |

Retained-history seeds contain ThreadCreated followed by complete input/model/completion triples
matching the immediate PersistentHistory format, with shared Run identities and schema-encoded
user/assistant message suffixes. At most two RepairAnnotated records pad the requested exact
record count. These are adapter fixtures of completed retained Runs, with no outstanding seed
Submissions. The provider callback must receive every seeded question and answer before fresh
inference; checkpoint resume must receive the handoff without retired history. The separate
64 KiB/1 MiB prompt cases assert their exact history text at the same callback. Each measured
durable completion must have one new canonical RunCompleted with the exact output and retain
the entire original archive. Seed writes use the adapters' normal public
operations and existing failpoints. The benchmark introduces no persisted format or migration.
SQLite uses the production Node assembly and its default scheduling, lease, and durability
configuration; checkpoint cases additionally install the documented host rollover preparation.
Database teardown and evidence reads are outside the warm-operation interval.

Fixture `runtime-v2` builds worker-local seed templates through those same public adapter
operations, once per history/ledger size and revision. It closes the full seed runtime and rejects
any remaining WAL or SHM sidecar before copying the database to each sample's fresh directory.
Copies share no mutable database state. Fresh submission and checkpoint recovery can reuse the
same retained-history seed; every recovery sample still constructs and saves its own checkpoint,
injects the fault, closes the runtime, and resumes its own Submission. Templates are discarded
when the worker closes and never cross a cohort or revision. This removes repeated fixture setup;
it does not measure or change the cost of production mutations. The reference SHA is unchanged.

The worker composes the seed initializer, scoped template cache, sample runner, and progress
writer as Effect services. Sample arguments contain only workload data and timeout settings;
tests replace service Layers while retaining the same operation clocks and cleanup boundaries.

`modelEntryMs` ends inside `ScriptedModel.assertRequest`, where Effect AI invokes the actual
normalized provider callback. It does not use ModelStarted events. `totalMs` ends after run/stream
completion or durable settlement; it includes the operation's correctness checks where those
checks are inline. Every sample uses a new finite script, verifies exhaustion, and counts model
stream finalizers. The first recovery attempt is verified before its counters are reset.
If that attempt misses the expected checkpoint fault, its diagnostic includes bounded returned
settlement outcomes/failures and the observed compaction/checkpoint phase. A successful worker
Effect can return a failed settlement; it does not by itself prove a successful Attempt.
`checkpointCreationMs` separately measures native checkpoint construction and persistence from
`compaction:after-canonical-append` through `checkpoint:after-save`, before injecting the fault.
It includes checkpoint scans/encoding/save but excludes the already committed compaction append
and is never added to the later recovery interval. Raw samples include observed retained prompt
message counts. Incomplete or mismatched-runtime batches are explicitly reported and excluded
from comparison summaries.

`attemptMs` includes setup, the operation, verification, and scope cleanup; `setupMs` ends at the
operation's start clock and includes initial checkpoint preparation for recovery. Neither is added
to `totalMs`. The worker atomically replaces its report before a sample and at setup, checkpoint,
operation, and verification boundaries, retaining the active case, ordinal, warmup flag, and elapsed
time at the last boundary if it is killed. These filesystem writes happen outside `totalMs`.
Controller reports identify the active batch and comparison failure; child logs are written as
output arrives. Stdout and stderr share an 8 MiB raw-byte limit; exceeding it preserves the log
prefix, terminates the child, and fails the batch. Controller failure details remain in the report.
A partial report is evidence of an incomplete attempt, never a passing cohort.

Cold measurements launch a separate Node process for one small run. Their wall time includes
Node startup, all fixture imports (including the durable fixture), one run, assertions, and process
shutdown. These are labeled subprocess totals, not isolated import latency or a minimal SDK
startup claim. Warm cohorts live in separate long-running child processes.

The fixture is transpiled once without bundling, then identical JavaScript bytes are copied to
all three stages. Framework stages contain only public `dist` artifacts and npm-ready manifests;
they cannot resolve framework TypeScript source. External dependencies come from each revision's
own installation. Reports identify exact commits, dirty state, lockfile hashes, built artifact
hashes, fixture hash/version, runtime, operating system, CPU, memory, sample counts, median,
interquartile range, and process failures. The artifact includes the exact transpiled fixture.

The retained reference is `audit-beta67-node24-v1`, commit
`596cffba70716b1211ac02de949c4d0f31734b2f`. It is rebuilt and rerun on the same machine as each
candidate to expose gradual drift. Historical stopwatch numbers are never used as the baseline.
To reset it, change the version and immutable SHA in `src/contracts.ts`, both workflow validators,
and this guide together; explain the release selection and fixture/runtime change in the PR.
Keep the previous artifacts. Incompatible historical APIs must fail clearly rather than silently
substituting source code or skipping cases. A new fixture changes the measurement definition and
requires a version bump; report environment changes before interpreting across-run trends.

Read the full spread and raw samples before drawing a conclusion. Re-run a suspected regression
with another matched cohort. Small-sample p95, local source timings, or differing provider workloads
do not establish an SLO, Cloudflare CPU billing, or a competitive ranking. Deterministic engine
and adapter work-budget tests remain the PR regression gates; timing evidence complements them.
The manual diagnostics below separate fairness and lock contention from isolated run latency.
Keep timing informational until repeated matched cohorts establish a workload-specific relative
and absolute regression threshold. Current hosted runs show substantial machine and storage
variance; one small-sample tail estimate or percentage alone is not a release gate. Confirm a
suspected regression in a fresh matched run and retain both results before changing a baseline.

## Manual diagnostics

`vp run perf:diagnose` runs the separate `runtime-diagnostic-v1` fixture against clean, built
base/head checkouts. Install each checkout's own lockfile and build its public packages as above.
Run this command from the candidate checkout, with no concurrent builds, tests, or measurements:

```sh
vp run perf:diagnose --base-dir /tmp/effect-agent-base --require-clean --out-dir /tmp/diagnostic-001
```

The manual workflow's `diagnostic` choice runs the same command and skips the immutable-reference
checkout. Ordinary PRs still run the unchanged `runtime-v2` matrix and trusted report validator.
Diagnostics run base/head followed by head/base, with two warmups and five measured samples per
cohort: ten measured samples per case and revision. They use the same production-package staging,
published manifests, own-lockfile dependencies, built-artifact identities, and identical unbundled
fixture bytes. This task never uses a cached measurement.

The policy matrix crosses one/four rounds, zero/two/twenty-millisecond authorization delays, and
zero/twenty-millisecond per-Turn model-Layer acquisition delays. Each round declares eight tools
with twenty-millisecond handlers and concurrency four; every cell uses the same immediate approval
hook. The finite scripted provider must consume all successful results in declaration order. The
probe verifies authorization ordering, complete-batch approval before handler entry, actual handler
overlap, and every model/handler finalizer. The synthetic delays expose scheduling behavior; they do
not estimate provider latency or prove an optimization.

The capability cases use these bounded public operations:

| Family      | Work and timing boundary                                                                                                                                                                                                                                                                                                                                      |
| ----------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| History     | Apply unchanged, one-message, or 64-message suffixes to a native 256-message prefix; include a 64-Thread store and the 768/256-message capacity boundary. A matched two-provider, one-tool Run compares history hooks on/off. Setup and verification are excluded.                                                                                            |
| Memory      | Compare one-provider Runs with recall off, empty, or populated. Measure recall and actual fixture-file reader I/O separately; verify scoped reader release.                                                                                                                                                                                                   |
| Remembering | Compare the same two-provider, one-tool foreground Run on/off while a previous extraction is held in another host Scope. Verify no foreground extraction/profile I/O; then release the worker and report admission, background completion, and profile readiness separately. The two-job public-port fixture excludes disk durability and host queue latency. |
| MCP         | Compare the same two-provider Run and three echo results using local handlers or real MCP HTTP transport with an in-process responder. Report connection, reused calls, foreground work, and owned-Scope closure; a fresh connection verifies credentials and discovery. No network is used.                                                                  |
| Subagents   | Compare the same two-provider parent and two projected results with local handlers or two actual child providers sharing one child slot. Report preparation, actual provider entry, slot wait, and completion separately. A named ten-millisecond first-provider hold creates controlled contention and remains included in elapsed times.                    |

The ledger cases scan a closed seed with 8,192 settled and sixteen unfinished submissions, or a
768-row unfinished ledger. Each sample receives a fresh copy; the seed is constructed once per
worker through public admission/claim/settlement operations. Reports separate seed/setup cost
from scan latency and retain the actual adapter SQL counts and query plans. Healthy finalization
replay, runtime status observation, and active finalization are measured separately; status still
includes its ordinary recovery-snapshot work. The statement counter excludes the driver's
transaction BEGIN/COMMIT commands.

Contention cases compare the same replay/status operation while another Node process holds a
SQLite write transaction for zero, 25, or 100 milliseconds after a readiness handshake. That
process releases its lock independently of the observer's event loop. Observer latency excludes
process startup and handshake; the report retains both the requested-window duration and full
lock occupancy through rollback. Writer and observer timestamps share Node's same-host `hrtime`
domain; overlap uses the conservative interval after acquisition and before rollback begins.
Samples that miss the writer window remain in the report with `noWriterOverlap=1`; consult the
overlap counters before claiming contention. A healthy read may finish while the writer still
holds its lock. These cases do not measure event-loop lag or change the adapter's busy timeout.
A subsequent public write and scoped process finalization verify release.

Fairness cases run four independent tool-heavy Threads and one short Thread with one, two, or
four registered Node host workers. Each busy Run uses four 100-millisecond tools with concurrency
two. Initial-backlog and warm-arrival cases retain every Thread's admission, Attempt/provider
entry, observed settlement, active maxima, and finalizers. Request-to-Attempt time includes
admission; observed settlement includes ordinary wake and polling cadence. Warm arrival records
actual activity rather than assuming all workers are saturated. Host creation, canonical
verification, and shutdown remain outside the operation clock. These probes preserve existing
worker defaults and make no strict round-robin, preemption, or starvation guarantee.

Reports retain total elapsed wall time, named millisecond intervals, natural-number counters, and
at most 512 phase marks per sample. Marks are in-memory and included in operation timing. Atomic
phase-report writes occur before the operation clock or after its end. Authorization sums, handler
phases, and model lifetimes can overlap and must not be added together. `postAuthorizationWaitMax`
starts at each handler's own authorization completion, so it includes later serial authorizations
as well as framework dispatch and bounded scheduling. Use the final authorization mark in each
batch to derive wait after the whole barrier; neither interval isolates semaphore wait.
Native subagent span offsets flush at Run exit, including failure, so mark array order need not be
chronological; use their operation-relative monotonic offsets.
No diagnostic reports CPU time. Compare matched medians and interquartile ranges, including the
retained slow samples, and preserve the complete environment and exact revision identities.

Each sample, including fixture setup, is bounded to two minutes, each child to five minutes, and the controller to nineteen
minutes. The existing shared eight-MiB child-output cap and five-second force-kill grace apply.
Worker reports are limited to sixteen MiB when read by the controller. Partial phase marks, completed
samples, failures, and timeouts remain in atomic JSON reports. Missing, duplicated, failed, incomplete,
or mismatched-runtime batches fail the command and never enter comparison summaries.

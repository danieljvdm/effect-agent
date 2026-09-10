# Cloudflare shared memory benchmark

See the [Cloudflare memory guide](../../docs/platforms/cloudflare.md#shared-memory) for application setup.
This leaf example hosts real Thread Objects and separate Memory Objects. It registers no model and
stores only synthetic source text. The Worker refuses every HTTP call until `BENCH_TOKEN` is set.

Deploy under a fresh temporary Worker name using this example's Wrangler dependency, set a random
`BENCH_TOKEN` with `wrangler secret put`, and run from the repository root:

```sh
vp run --no-cache -F @effect-agent/example-cloudflare-memory measure \
  --url https://YOUR-TEMPORARY-WORKER.workers.dev --samples 200 \
  --inactivity-seconds 90 --revision YOUR-SOURCE-REVISION \
  --output /tmp/kom19-cloudflare-memory.json
```

Supply the same `BENCH_TOKEN` in the local environment. Keep tokens out of arguments and reports.
The measurement command does not deploy, delete, or change account configuration. Delete the temporary
Worker and its DO namespaces afterward. Ordinary builds use only `wrangler deploy --dry-run`.

The bounded workload covers 1, 4, 8, and 16 distinct 1024-byte sources, plus 128 candidates over
16 sources. Two Thread callers alternate, at concurrency 1 and 4. Five independent memory owners
permit a first request after inactivity for each case. Every sample, payload size, error, and timeout
is retained; warm p50/p95/p99 are reported in milliseconds. Egress-colo probes run after measurement.
They describe observed routing, not guaranteed physical owner placement.

Source-validation RPC time is separate from final recall rendering and full HTTP elapsed time.
Embedding/search are deliberately absent, so this benchmark makes no claim about their latency.
Worker clocks advance on I/O; zero rendering intervals are not evidence of zero CPU cost.
One first-after-inactivity sample per case cannot establish cold-start percentiles, and inactivity
does not prove eviction. Local workerd results must not be reported as deployed latency.

## Local heap measurements

Run the separate heap benchmark without deploying or supplying a model key:

```sh
vp run --no-cache -F @effect-agent/example-cloudflare-memory measure:heap \
  --out-dir /tmp/effect-agent-heap --samples 3 --objects 4
```

The command needs permission to open local HTTP and debugger ports. It saves exact Wrangler dry-run
bundles, SHA-256 hashes, esbuild metafiles, individual samples, and a schema-encoded `report.json`.
It compares the existing Worker's direct `ThreadObject` import with an equivalent root import and
builds a separate synthetic runtime Worker from `wrangler.heap.jsonc`.

Each Node sample uses a fresh process and evaluates the emitted bundle in a separate VM realm.
It records the harness baseline, evaluation delta, and explicit post-GC delta. Cloudflare external
modules are shimmed, and no handlers execute in Node. These values diagnose startup allocations;
they do not measure workerd or a Cloudflare memory allowance.

Each native sample starts fresh local workerd and reads `Runtime.getHeapUsage` through its debugger.
It samples after startup, after initializing all Thread Objects, while every Object has live tool
activity, and after all runs settle. Each Object uses a registered synthetic Effect AI model and
two registered tools, a 4096-character input, and two 16384-character tool results. A tool gate fixes
the active sampling point. The command verifies two model calls, two tool calls, and successful
settlement per Object. `--objects` accepts 1–16 and `--samples` accepts 1–20.

Native values are instantaneous JavaScript heap snapshots, not peak memory or process RSS.
The report keeps embedder and backing-storage counters separate. It does not force workerd GC.
Local results do not establish hosted capacity, per-Object isolation, or behavior with provider
SDKs, long histories, more tools, or production traffic. Cloudflare's
[memory profiler](https://developers.cloudflare.com/workers/observability/dev-tools/memory-usage/)
explains how to inspect real workloads. Hosted latency measurement remains the explicit `measure`
command above; neither command deploys anything.

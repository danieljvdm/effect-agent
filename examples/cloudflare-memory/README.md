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

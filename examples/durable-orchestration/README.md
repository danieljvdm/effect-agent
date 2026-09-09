# Durable orchestration on Node and Cloudflare

Run the same Agent declarations on a bounded Node worker pool or SQLite-backed Cloudflare Durable
Objects. Both entrypoints use OpenAI through native Effect AI model Layers. The default model is
`gpt-5.6-sol`, with low reasoning effort and a 4,096-token output budget. Set `OPENAI_MODEL`
to select another Responses API model that supports tool calling and low reasoning effort,
such as `gpt-5.6-terra`.
The agents propose plans and verification steps using real model calls; they have no filesystem
or shell tools. Deterministic models live only in the tests.

```mermaid
flowchart LR
  C[Coordinator · depth 0] -->|background start| A[Builder A · depth 1]
  C -->|background start| B[Builder B · depth 1]
  A -->|attached| SA[Scout · depth 2]
  B -->|attached| SB[Scout · depth 2]
  A -->|durable report| C
  B -->|durable report| C
  C -->|fixed peer route| P[Advisor]
  P -->|authenticated reply| C
```

The coordinator's launch Run finishes after both builders accept their inputs. Each builder
attaches a scout, then its canonical completion produces a typed report input for a later
coordinator Run. A subsequent coordinator input uses the native `build_a_follow_up` tool to
send another task to A's existing worker Thread.

The host also sends a recommendation request to an independent advisor. The advisor uses the
native `coordinator_inbox` and `coordinator_reply` tools. Host routing fixes the destination;
incoming provenance identifies the return address. Peer read and send grants are separate
from control grants, and the advisor has no worker ownership permission.

## Node

From the repository root:

```sh
vp install
export OPENAI_API_KEY='your-openai-api-key'
vp run @effect-agent/example-durable-orchestration#node
```

The key is read from the environment at startup. Missing or empty keys fail before opening the
host. Requests use OpenAI's Responses API with `store: false`; the key is not part of agent inputs
or registration digests. Model calls use your OpenAI API account.

This starts a scoped host with **three execution workers**, runs the demonstration, prints its
final state, and closes the host. Progress goes to stderr and final JSON to stdout. Allow up to
five minutes for live calls. A recorded Run failure ends the demo early with its failure details.
Expect two idle worker Threads, depths `[0, 1, 2]`, three reports, one recommendation, and completed coordinator inputs. The follow-up is sent after
the first reports so it has a separate worker Run.
The Node boundary observes `NodeHost.run` alongside the interaction, so a worker-pool failure
ends the demonstration immediately with that failure.

State persists in `examples/durable-orchestration/orchestration-gpt-5.6-sol.sqlite`. Running the command
again uses the same explicit idempotency keys and reconnects to those admissions. The host
recovers accepted work after interruption. For an independent demonstration or a different model,
choose a new database:

```sh
OPENAI_MODEL=gpt-5.6-terra ORCHESTRATION_DATABASE=orchestration-second.sqlite \
  vp run @effect-agent/example-durable-orchestration#node
```

Model identity is pinned in durable registrations. Reopening a completed demonstration reuses
its results; accepted unfinished work resumes. Reopening does not reset failures. Keep a failed
database for inspection and use a new filename to try again. The GPT-5.6 default uses a separate
file from the earlier GPT-4.1 mini and scripted examples. For Cloudflare, use fresh Durable Object
storage when changing models; existing registrations retain their original model identity.

## Cloudflare

The Worker exports a SQLite Durable Object class and includes its first migration in
[wrangler.jsonc](wrangler.jsonc). Each Thread has its own Object; persisted alarms drive
accepted work and message delivery. There is no process-local worker loop to keep alive.
The example's [CloudflareHost](src/CloudflareHost.ts) module exposes `make` and `layer`;
`CloudflareHost.make` extends `ThreadObject.make` with the demo's `command` and `status` RPCs.
The host supplies Effect `Config` from Worker vars and secrets, so the same model configuration
used on Node reads `OPENAI_API_KEY` and `OPENAI_MODEL` without an environment adapter. An unset
or empty `OPENAI_MODEL` uses the default; a missing or empty API key refuses initialization.

```sh
cp examples/durable-orchestration/.dev.vars.example examples/durable-orchestration/.dev.vars
# Set OPENAI_API_KEY in the untracked .dev.vars file (OPENAI_MODEL is optional).
vp run @effect-agent/example-durable-orchestration#dev
```

In another terminal, launch both builders:

```sh
curl http://localhost:8787/command \
  -H 'Authorization: Bearer local-orchestration-demo' \
  -H 'Content-Type: application/json' \
  -d '{"action":"launch","key":"launch-v1","mission":"Design a small durable feature"}'

curl http://localhost:8787/status \
  -H 'Authorization: Bearer local-orchestration-demo'
```

After the two initial reports arrive, continue and ask the advisor:

```sh
curl http://localhost:8787/command \
  -H 'Authorization: Bearer local-orchestration-demo' \
  -H 'Content-Type: application/json' \
  -d '{"action":"continue","key":"continue-v1","note":"Also verify the restart path"}'

curl http://localhost:8787/command \
  -H 'Authorization: Bearer local-orchestration-demo' \
  -H 'Content-Type: application/json' \
  -d '{"action":"recommend","key":"recommend-v1","question":"What should the builders prioritize?"}'
```

`202` acknowledges a Receipt or retained outbound message. Read `/status` for processing
progress. Reuse a key to retry the same request; use a new key for a new input. Requests require
the bearer token. Models cannot supply a destination Thread or principal.

To validate the deployment bundle without uploading:

```sh
vp run @effect-agent/example-durable-orchestration#build
```

To deploy to your Cloudflare account, configure the API key and a private token, then deploy
from the example:

```sh
cd examples/durable-orchestration
vp exec wrangler secret put OPENAI_API_KEY
vp exec wrangler secret put DEMO_TOKEN
vp run deploy
```

## Bounds and recovery

`maxDepth: 2` is root-relative. Each builder's `childLifetimes: ["attached"]` permits attached
scouts while prohibiting background grandchildren. A builder reserves four turns and two
tool calls while its own policy permits two turns and one call. The residual budget and one
reserved descendant invocation fund its scout. Each allocation reserves three minutes: two for
the builder and one for the scout. The coordinator's 30-minute policy accommodates the declared
pool of eight allocations; the command itself stops waiting after five minutes. Ancestors
conserve these allocations.

Reporting is declared on the existing coordinator registration with `Subagent.reporting`.
The host freezes the projected coordinator input before retrying delivery. Joined child
Receipts produce one report per actual Run. Input arriving during an active worker Run can
join that Run; it does not imply another report. Delivery distinguishes retention, destination
acceptance, and processing, with bounded retries and retained parked work.

The Node execution pool is shared by all Runs. Builders have a declared concurrency bound of
two; attached waits release execution permits so scouts can run. This is bounded capacity,
not a reserved conversation slot or a latency guarantee. A separate platform regression proves
that a nested background-builder/attached-scout path also completes with one execution worker.

The fixed demo identity and generous root pool support the illustrated inputs. Additional
work remains subject to the declared lifetime allocation; settlement does not replenish it.

## Verify

```sh
vp run @effect-agent/example-durable-orchestration#check
vp run @effect-agent/example-durable-orchestration#test
vp run @effect-agent/example-durable-orchestration#build
```

The tests inject deterministic native models into the same Node and Cloudflare host assembly,
run the Worker under real workerd, exercise native tools and reply provenance, and verify that
peer access grants no retry or worker control. The OpenAI host is exercised separately at its
HTTP boundary, including provider failure reporting; these automated tests make no billed
requests. Platform regression suites additionally restart Node after a report is frozen and
evict Cloudflare source/child Objects with wake hints dropped, checking that joined Receipts
produce one recovered report.

# Live tool selection and discovery comparison

Compare the public `AgentRuntime`, `ToolSelector.fromDecisionModel`, and
`ToolDiscovery.fromDecisionModel` APIs with `gpt-6-astra` on one 50-tool catalogue:

| Arm             | Initial business tools     | Discovery        |
| --------------- | -------------------------- | ---------------- |
| all-50          | All 50                     | None             |
| fixed-8-keyword | Eight application defaults | Literal keywords |
| jev-8-keyword   | JEV top eight              | Literal keywords |
| fixed-8-jev     | Eight application defaults | JEV relevance    |
| jev-8-jev       | JEV top eight              | JEV relevance    |

Discovery is pinned in all shortlist arms, giving nine initially exposed tools.
Initial ranking uses cutoff zero to produce eight tools; semantic discovery uses
0.5 and can return fewer than eight. Later turns retain discovery selections.

Six workloads cover a common tool, rare tool, linked record, paraphrased request,
forced paraphrase miss, and forced linked-record miss. The last two deliberately
exclude the required tool from **initial ranking only**; discovery and the all-50
baseline retain the complete catalogue. They measure fallback behavior, not natural
ranking recall. The fixed defaults already omit those tools. The normal and forced
linked-record tasks use identical input, isolating initial availability.

Success requires exact equality with the requested field values (including no extra
claims), plus actual execution of the necessary tools with correct IDs. All business
tools are local read-only fixtures; only inference is remote. This is a small synthetic
catalogue with uniform schemas, not a representative production task distribution.

```sh
vp run perf:tool-selection --output /tmp/tool-discovery-plan.json
# Supply OPENAI_API_KEY and TYPESAFEAI_API_KEY through your secret manager.
vp run perf:tool-selection --live --repetitions 5 --output /tmp/tool-discovery-live.json --log-level error
```

The default is 150 sequential samples: six tasks × five arms × five repetitions.
Five repetitions rotate each arm through every position for each task. The primary
comparison is successful completion and end-to-end latency on discovery-dependent
workloads; choose a meaningful improvement threshold before running. Report normal
and forced cohorts separately. Five repetitions per cell describe this experiment;
they do not establish broad reliability or stable tail latency.

Run with builds and tests idle. Each sample starts a new in-memory thread, but
connection and provider prefix caches are uncontrolled. Record observed cache reads
and writes, and report missing write counters as unknown, not zero. Token reduction
alone does not establish a billing reduction. Compare observed billing estimates
with a separately labeled estimate at uncached rates. There is no token-count
preflight in the measured path.

Each sample allows at most six OpenAI requests, seven JEV requests, eight tool calls,
64 KiB per OpenAI request and 2,048 output tokens per completion. Low reasoning effort,
default service tier, no retries, and a 150-second outer deadline are fixed across arms.
These bounds constrain the experiment but are not a dollar spending guarantee.

The JSON records source revision, dirty state, environment, every finished or failed
sample, exact synthetic requests, exposed names, resolved models, all provider usage,
initial selections, discovery queries, executed IDs, and answers. `elapsedMs` spans
`AgentRuntime.run`, including initial ranking, discovery, final validation, and small
local checkpoint writes. Provider Layer construction and imports are outside it.
`firstDeltaAt` is any first provider delta, potentially tool arguments. All timestamps
use the same runner wall clock. JEV calls are separated into initial/discovery phases.

The adjacent `.events.jsonl` journal checkpoints sample starts, paid request starts
and completions, and final samples. It preserves in-flight attempts if interrupted;
an unfinished paid call has unknown usage and must not be treated as free. The main
JSON is replaced atomically after each sample. Existing evidence paths are refused.
Any failed sample causes a nonzero exit after preserving the complete run. Keep raw
results in experiment artifacts, with task-level distributions and failures alongside
aggregate tables; do not publish a universal speedup from these fixtures.

## Stable catalogue and cache experiment

```sh
vp run perf:tool-selection --suite cache --context short --repetitions 4 --live --output /tmp/cache-short.json --log-level error
vp run perf:tool-selection --suite cache --context reference --repetitions 4 --live --output /tmp/cache-reference.json --log-level error
vp run perf:tool-selection --suite probe --context short --repetitions 8 --live --output /tmp/probe-short.json --log-level error
vp run perf:tool-selection --suite probe --context reference --repetitions 8 --live --output /tmp/probe-reference.json --log-level error
```

`cache` retains the five original arms and adds three controls:

| Arm                | Definitions sent | Initially callable  | Discovery |
| ------------------ | ---------------- | ------------------- | --------- |
| all-50-discovery   | 50 + discovery   | 50 + discovery      | JEV       |
| stable-fixed-8-jev | 50 + discovery   | Fixed 8 + discovery | JEV       |
| stable-jev-8-jev   | 50 + discovery   | JEV 8 + discovery   | JEV       |

The two stable arms use a **benchmark-only OpenAI client adapter**. It serializes
the authorized fixture catalogue with Effect's OpenAI schema transformer and keeps
those definitions in a fixed order. The runtime still chooses and enforces its
normal active subset; the adapter translates that subset to native `allowed_tools`.
Unknown names, schema drift, duplicate definitions and unsupported choices fail
before I/O. Stable arms expose metadata for all 51 tools to the provider. This is
not framework support for stable exposure and does not reduce schema tokens.

The cache suite adds a four-record chain with three initially unknown links. It
allows 12 model turns and 16 tool calls for that task, keeping the original limits
for the six existing tasks. The `reference` context adds 128 identical synthetic
archive entries to the instructions in every arm, making even the shortlists
cache-eligible. It is a context-size stress condition, not a real conversation
transcript. Input-token counts come from the provider, not estimates. Both contexts
retain the same task inputs, required executions and exact-answer oracle. Reference
material is not sent to JEV, whose explicit state remains the task or discovery query.

`probe` isolates the transport mechanism without an agent or JEV. Each trial makes
four sequential, identical requests requiring one shipping-tool call. Callable sets
are A/A/A/A for `probe-fixed` (eight physical definitions), or A/B/C/A for
`probe-filtered` (eight changing definitions) and `probe-allowed` (50 stable
definitions). All sets contain the required shipping tool. No handlers execute;
success means exactly one correctly parameterized call in each response.

Each probe trial salts the **first tool description** before the cacheable prefix,
then holds that salt and all message content fixed for its four requests. Report
actual first-request cache reads/writes, changed-subset requests and the return to A
separately. A new `prompt_cache_key` is not used to assert cold state. The agent suite
uses unsalted shared fixture prefixes and measures observed reuse under serial
traffic. It does not artificially warm each arm, and includes first-use writes.

The commands above produce 448 agent attempts (8 arms × 7 tasks × 4 repetitions ×
2 contexts) plus 48 four-request probe trials. Position rotates by task/repetition;
four repetitions do not balance every per-task position. Keep contexts separate in
latency comparisons because they run in separate time windows. A useful primary
target is at least 10% lower observed token cost with no observed correctness loss;
also report latency distributions, failed attempts, JEV overhead and cache counters.
This fixture scale cannot establish production reliability or universal savings.

Evidence format v3 adds `suite`, `context`, and per-request `callableTools` alongside
the transmitted `tools` and exact request JSON. Requests are bounded at 256 KiB for
all suites. The cache suite's chain allows at most 13 JEV calls; other agent tasks
retain seven. Probe trials allow four OpenAI calls and zero JEV calls. These are
request bounds, not a dollar cap. Run with builds and tests idle and retain pilot
attempts separately. Price actual cache reads, writes, other input, output and JEV
usage; show missing usage as unknown. Do not infer financial savings from cache-hit
percentage alone or silently discard failed/slow attempts.

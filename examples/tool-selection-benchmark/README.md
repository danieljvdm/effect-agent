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

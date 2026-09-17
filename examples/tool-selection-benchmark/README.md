# Live tool selection comparison

Compare the public `AgentRuntime`, `ToolDiscovery`, and `ToolSelector.fromDecisionModel`
APIs on the same 50-tool synthetic catalogue with `gpt-6-astra`:

- **all-50:** expose all 50 business tools.
- **fixed-8-discovery:** expose eight application defaults and pinned discovery.
- **jev-8-discovery:** JEV ranks all 50 descriptions once before the first turn;
  expose its top eight and pinned discovery. Later turns retain discovery selections.

The three tasks cover an initial shortlist hit, a miss, and a linked record whose
tool becomes apparent only after a first lookup. Success requires both actual
execution with the expected IDs and fixture evidence in the final answer. All
business tools are local, read-only fixtures; only inference calls are remote.

From the repository root:

```sh
vp run perf:tool-selection --output /tmp/tool-selection-plan.json
# Supply OPENAI_API_KEY and TYPESAFEAI_API_KEY through your secret manager.
vp run perf:tool-selection --live --repetitions 3 --output /tmp/tool-selection-live.json --log-level error
```

The first command performs no inference. A live run makes 27 sequential samples,
at most six OpenAI calls per sample and nine JEV calls total. Every OpenAI request
is limited to 64 KiB and 2,048 output tokens, with low reasoning effort and default
service tier. Each sample has a 150-second outer deadline. There are no inference
retries. These are request bounds, not a dollar spending guarantee.

Run with other builds and tests idle. Three repetitions rotate each arm through
each position per task. Each sample starts a new in-memory thread; process,
connection and provider prefix caches are uncontrolled and may be warm. Returned
cached-token usage is recorded. There is no token-count preflight in the measured path.

The JSON retains every completed or failed sample, revision and environment,
exact synthetic request bodies, exposed names per turn, provider usage and model
IDs, JEV selection/usage, executed tool IDs, and answers. Its timestamps use the
runner's wall clock. `elapsedMs` spans `AgentRuntime.run`, including selection,
discovery, tools, model inference, and final validation; imports and provider layer
construction are outside it. `firstDeltaAt` is the first provider delta of any
kind, which can be tool arguments rather than user-visible text. Discovery calls
are recorded in `modelCalls[].toolCalls`. A missing completion retains null usage;
do not treat it as free. Existing output paths are refused, and any failed sample
causes a nonzero exit after preserving results.

JEV's relevance cutoff is zero to compare exactly eight business tools in each
shortlist arm. Discovery is excluded from ranking because the runtime pins it
separately. This is an application policy for initial selection, not a measurement
of reranking on every turn. Selection quality and latency are workload-dependent;
retain raw samples and report task-level distributions and failures before making
performance claims. Put measured results in the PR or experiment artifacts.

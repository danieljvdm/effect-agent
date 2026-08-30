# Context management

Every model request sends the conversation again. A grep-and-read agent adds another Tool result
to each request, pays for the larger prompt on every later call, and may exceed its budget or the
model's context window before it answers. Context management bounds the prompt, reports remaining
capacity, and gives the model one constrained chance to answer when a limit is exhausted.

Everything on this page is policy-driven and on by default. The knobs live on `AgentPolicy`:

```ts
AgentPolicy.make({
  maxTurns: 12,
  maxToolCalls: 24,
  maxDuration: "5 minutes",
  toolConcurrency: 4,

  tokenBudget: 200_000, // cumulative input+output across the Run
  costBudgetMicrousd: 2_000_000, // spend, measured by your cost estimator
  contextTokenLimit: 150_000, // compact before one call exceeds this size

  toolResultBounds: ToolResultBounds.make({ maxBytes: 50 * 1024 }), // the default
  runStatus: "appended", // the default
  compaction: CompactionPolicy.make({ keepRecentTokens: 20_000 }), // the default
  onExhaustion: "final-answer", // the default
});
```

The three token quantities measure different things. `tokenBudget` is cumulative and charges the
full prompt of every call. It grows quadratically with history and stops runaway Runs rather than
measuring completed work. `costBudgetMicrousd` measures spend through an estimator that receives
cache-split usage. `contextTokenLimit` bounds one call's context. Set it from the model's context
window, and the engine compacts before the next request exceeds it.

## Tool results are bounded at the source

Every application Tool result, including MCP results, passes through `toolResultBounds` exactly once
at the settle seam, before it enters history or durable records. A result within the bound passes
through byte-identical. An oversized one becomes the canonical envelope:

```json
{
  "truncatedToolResult": true,
  "originalBytes": 412887,
  "head": "…first half of the byte budget…",
  "tail": "…last half…"
}
```

The model sees the envelope, the journal records the same envelope, and replay stays consistent.
The default is 50 KiB per result; raise it deliberately for agents whose single results must be
large. Provider-executed Tool results are exempt because the provider has already materialized
them into the response.

One oversized `git diff` no longer costs its full size on every subsequent call for the rest of
the conversation. Bound first; everything else on this page is cheaper because of it.

## The run-status message

The model cannot see policy counters unless the runtime includes them in the prompt. With
`runStatus: "appended"` (the default), each outgoing request ends with one derived line:

```text
<run-status>turn 3/12 · tool-calls 11/24 · tokens 84210/200000 · last-context 23480 · elapsed 74s/300s</run-status>
```

Past 80% of any dimension it appends a wrap-up warning, so the model converges instead of
starting new work it cannot finish. The message is derived at prompt-assembly time and is never
persisted: canonical history stays append-only, and replays are unaffected. Set
`runStatus: "off"` for prompt-sensitive evaluations.

## Warnings and the token soft landing

Crossing 80% of a configured budget emits one `BudgetWarning` Run Event per dimension. Observe it
like any other event to alert before the Run reaches the limit.

Exhaustion itself resolves through `onExhaustion`. Turn and Tool Call exhaustion soft-land as
described in [Agent definitions](/guide/agents): the over-budget batch settles synthetically,
tool use is forbidden, and one grace Turn produces the final answer. The token dimension joins
the same resolution: when cumulative usage crosses `tokenBudget`, a breaching response that
already carries a decodable answer completes the Run directly, and otherwise the Run takes at
most one constrained grace Turn (`toolChoice: "none"`, or only the Definition-owned completion
Tool; its usage is charged once and cannot re-trigger the breach). Either way the Run completes
honestly:

```ts
RunCompleted {
  finishReason: "budget-exhausted",
  exhausted: "tokens", // or "turns" | "tool-calls"
}
```

`exhausted` names the exhausted dimension. A delegated child that produces a final answer settles
as success. Its grace-Turn output flows through `projectResult` like any other child output, and
`SubagentCompleted.exhausted` makes the degradation observable to the parent. This is the marker
the budget-extension grant flow consumes when an orchestrator decides to re-delegate with a
larger allowance.

`onExhaustion: "fail"` keeps every dimension fail-fast for pipelines that must never accept a
truncated answer. No declared application Handler, including a completion Tool, starts after the
breach. Duration and cost breaches always fail typed because a grace call would extend the
wall-clock limit or increase the bill.

## Compaction

When `contextTokenLimit` is set, the engine estimates the next call's context at every pre-Turn
seam (the last provider-reported input plus a conservative estimate of newly appended parts).
Crossing the limit invokes the installed `ContextCompactor`. The default follows `CompactionPolicy`:

1. **Prune** replaces application Tool results older than the protected `keepRecentTokens` tail
   with `"[tool result cleared by compaction]"`. Message structure and call/result pairing are
   preserved; no model call is spent.
2. **Summarize** runs only if pruning was not enough (mode `"prune-then-summarize"`, the
   default). One metered call on the Run's model summarizes the goal, constraints, progress,
   decisions, next steps, and context. The model-visible view
   becomes instruction prefix + summary + the kept recent tail. The summarization call's usage
   is charged like any other.

Compaction is a view, never a rewrite. Official history and the canonical Conversation Log are
untouched; a `CompactionPerformed` event reports each reduction. In the durable assemblies (DN
and DC) each compaction also appends a canonical `CompactionCreated` record, and the journal
projection folds it. Covered records render as the summary or with cleared Tool results on every
later Attempt and every later Run of the same Conversation. This matters for long-lived
Conversations: without it, every prior Run's raw Tool output replays into every new Run's
opening prompt forever.

If a provider still rejects a prompt as too long, the engine classifies the rejection, compacts,
and issues at most one framework-level retry. Transport ambiguity can still duplicate the
external call. A second rejection, or overflow with no `contextTokenLimit` configured, fails
typed with `ContextOverflowError` instead of an opaque provider error.

### Replacing compaction

`ContextCompactor` is owned by `@effect-agent/engine` and re-exported by capabilities and the
umbrella package. Install it with a Layer to replace the strategy and token estimator without
changing the Run loop. `ContextCompactor.layer` supplies the bounded default. Existing Runs that
do not install a compactor select that same default at the Run boundary.

To keep the default strategy but use another upstream Effect AI Model:

```ts
const compactorLayer = ContextCompactor.layerWithModel(summaryModel);

const result = AgentRuntime.run(agent, input).pipe(Effect.provide(compactorLayer), Effect.scoped);
```

The Layer retains the summary Model's construction requirements. The interpreter charges its
reported usage and prices it under that Model's provider/name, then checks the remaining Run
budget before the next research call.

For a custom summary prompt, capture dependencies while constructing the Layer:

```ts
const compactorLayer = Layer.effect(
  ContextCompactor,
  Effect.gen(function* () {
    const model = yield* summaryModel.captureRequirements;
    return ContextCompactor.of({
      estimate: estimatePromptTokens,
      compact: (request) =>
        ContextCompactor.default.compact({
          ...request,
          summarize: (prompt) =>
            request.summarize(
              Prompt.fromMessages([
                Prompt.systemMessage({
                  content: "Retain unresolved decisions and exact customer identifiers.",
                }),
                ...prompt.content,
              ]),
              model,
            ),
        }),
    });
  }),
);
```

A replacement `compact` may instead emit its own `CompactionDecision` stream. Each decision
selects an exclusive source-message prefix with `through`, and either clears old Tool results
or supplies a summary. The interpreter validates decisions, preserves protected instructions
and input, rejects cuts that split Tool pairs, and retains the recent tail. It allows one prune
followed by one summary per pass, with at most one metered `request.summarize` call. All summary
model calls must use that callback. It preserves callback errors and requirements; application
strategy failures use `CompactionError`, while defects and interruption keep their Effect meaning.
Response-buffer limits and the Run deadline also apply during compaction.

`estimate` must return a non-negative finite integer. It sizes the source, appended output
contract, and Run status. The interpreter still uses provider-reported usage as the base when
the view has only grown since the previous call. A non-progressing strategy fails the context
check before provider I/O.

Custom harnesses can yield `ContextCompactor` with `ContextCompactor.layer`, call `compact`
directly with an upstream `Prompt`, `initialCompactionState()`, policy, and their own metered
`summarize` callback, and consume the decision stream. The harness owns applying each decision
to its view state and rebuilding the prompt with `buildCompactedView`. No Agent or Run is needed.

The interpreter commits each accepted decision before applying its view or pulling the next
decision. Durable coordinators map only the actually covered source prefix to complete prior-Run
records. A prompt transformation that prevents that mapping cannot authorize canonical coverage.
Canonical history remains append-only, and replay uses the committed summary after interruption.

### Supplying a Cloudflare compactor

Cloudflare Conversation Objects can install a host compactor without putting it in global state.
Build your `ContextCompactor` as a Layer, close its model/configuration dependencies from the Worker
environment, and adapt it through `contextCompactorRunContextLayer`:

```ts
export class Conversations extends makeConversationObjectClass({
  namespaceBinding: "CONVERSATIONS",
  deploymentId: "production",
  producerPrefix: "conversation",
  bindings: makeBindings,
  runContext: ({ env }) =>
    contextCompactorRunContextLayer.pipe(
      Layer.provide(makeContextCompactorLayer(env)),
    ),
});
```

Everything specific to your compactor must already be provided. The adapter itself no longer
requires `Crypto.Crypto`. The Layer is acquired once per Durable Object incarnation and rebuilt
after eviction. It installs the same compaction service used by ephemeral Runs, invoked under
context pressure after canonical prompt reconstruction. Usage, events, and `CompactionCreated`
commits follow the native path. Expected strategy failures settle as `CompactionError`; defects
remain defects for the host to supervise.

BEHAVIOR CHANGE: the former capabilities `ContextCompactor.compact(snapshot)` artifact API has
been replaced by the engine request/decision contract. Migrate implementations to the contract
above. `contextCompactorRunContextLayer` installs `RunContextPreparation.compactor`, rather than
a per-Turn prompt hook. Digest-bound `CompactionArtifact`, `digestCompactionSource`, and
`applyCompaction` remain explicit data utilities; the interpreter does not invoke a second
artifact compaction path. General prompt transformations still use `RunContextPreparation.hook`.

## Observing usage

The budget hook's snapshot is cache-aware and context-aware:

```ts
const report = Effect.gen(function* () {
  const usage = yield* budget.snapshot;
  usage.inputTokens; // cumulative input (total, including cached)
  usage.cacheReadInputTokens; // served from provider cache
  usage.cacheWriteInputTokens;
  usage.lastInputTokens; // ≈ the live context of the most recent call
  usage.lastOutputTokens;
});
```

`lastInputTokens` is the number to watch. It is the Run's actual context size, the quantity
`contextTokenLimit` bounds. Cumulative `inputTokens` will be many multiples of it on any
long Run. Prompt caching may reduce the cost of those repeated input tokens. See
[Run and stream](/guide/run-agents) for wiring the hook.

## Sizing guidance

- Set `contextTokenLimit` below the model's window to leave room for output and the summary. For a
  200k-token window, start between 150k and 170k.
- `keepRecentTokens` (default 20k) is the verbatim tail that survives a compaction. Raise it for
  agents whose recent Tool results are load-bearing; lower it for chatty ones.
- Size `tokenBudget` as a runaway stop, not a work meter. Use several multiples of
  `contextTokenLimit`, or omit it and bound spend with `costBudgetMicrousd` instead.
- Research-shaped work is cheaper delegated: a scout child reads the files and returns a bounded
  report, so the noise never enters the parent's context; Code Mode collapses many Tool
  round-trips into one brokered program pass. Both compose with everything above.

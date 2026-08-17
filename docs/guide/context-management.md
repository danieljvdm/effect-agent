# Context management

Every model request re-sends the conversation so far. Without bounds, an agent on a
grep-and-read cadence grows its prompt with every Tool result, pays for that growth again on
every later call, and eventually dies mid-task — over budget or over the model's context
window — having delivered nothing. Context management is the engine machinery that keeps the
prompt bounded, keeps the model and the host informed, and turns exhaustion into a delivered
answer instead of a silent failure.

Everything on this page is policy-driven and on by default. The knobs live on `AgentPolicy`:

```ts
AgentPolicy.make({
  maxTurns: 12,
  maxToolCalls: 24,
  maxDuration: "5 minutes",
  toolConcurrency: 4,

  tokenBudget: 200_000, // cumulative input+output across the Run — the runaway stop
  costBudgetMicrousd: 2_000_000, // spend, measured by your cost estimator
  contextTokenLimit: 150_000, // one model call's live context — the compaction trigger

  toolResultBounds: ToolResultBounds.make({ maxBytes: 50 * 1024 }), // the default
  runStatus: "appended", // the default
  compaction: CompactionPolicy.make({ keepRecentTokens: 20_000 }), // the default
  onExhaustion: "final-answer", // the default
});
```

Three token quantities are deliberately distinct, because conflating them is how agents die
confusingly. `tokenBudget` is cumulative: it charges the full prompt of every call, so it grows
quadratically with history and exists to stop runaways, not to measure work. `costBudgetMicrousd`
measures spend through your estimator, which receives cache-split usage. `contextTokenLimit`
bounds one call's live context — set it from your model's window, and compaction keeps the Run
under it.

## Tool results are bounded at the source

Every application Tool result — MCP included — passes through `toolResultBounds` exactly once,
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
large. Provider-executed Tool results are exempt — the provider already materialized them into
the response.

One oversized `git diff` no longer costs its full size on every subsequent call for the rest of
the conversation. Bound first; everything else on this page is cheaper because of it.

## The run-status message

The model cannot see policy counters, so every bound is an invisible cliff. With
`runStatus: "appended"` (the default), each outgoing request ends with one derived line:

```text
<run-status>turn 3/12 · tool-calls 11/24 · tokens 84210/200000 · last-context 23480 · elapsed 74s/300s</run-status>
```

Past 80% of any dimension it appends a wrap-up warning, so the model converges instead of
starting new work it cannot finish. The message is derived at prompt-assembly time and is never
persisted: canonical history stays append-only, and replays are unaffected. Set
`runStatus: "off"` for prompt-sensitive evaluations.

## Warnings and the token soft landing

Crossing 80% of a configured budget emits a `BudgetWarning` Run Event once per dimension —
observe it like any other event to alert before a Run is in trouble.

Exhaustion itself resolves through `onExhaustion`. Turn and Tool Call exhaustion soft-land as
described in [Agent definitions](/guide/agents): the over-budget batch settles synthetically,
tool use is forbidden, and one grace Turn produces the final answer. The token dimension joins
the same resolution: when cumulative usage crosses `tokenBudget`, a breaching response that
already carries a decodable answer completes the Run directly, and otherwise the Run takes at
most one constrained grace Turn (`toolChoice: "none"`; its usage is charged once and cannot
re-trigger the breach). Either way the Run completes honestly:

```ts
RunCompleted {
  finishReason: "budget-exhausted",
  exhausted: "tokens", // or "turns" | "tool-calls"
}
```

`exhausted` names the dimension that bound. A delegated child that soft-lands settles as
success — its grace-Turn output flows through `projectResult` like any other child output, and
`SubagentCompleted.exhausted` makes the degradation observable to the parent. This is the marker
the budget-extension grant flow consumes when an orchestrator decides to re-delegate with a
larger allowance.

`onExhaustion: "fail"` keeps every dimension fail-fast for pipelines that must never accept a
truncated answer. Duration and cost breaches always fail typed — a grace call would extend the
wall-clock contract or the bill.

## Compaction

When `contextTokenLimit` is set, the engine estimates the next call's context at every pre-Turn
seam (the last provider-reported input plus a conservative estimate of newly appended parts).
Crossing the limit triggers compaction, synchronously, per `CompactionPolicy`:

1. **Prune** replaces application Tool results older than the protected `keepRecentTokens` tail
   with `"[tool result cleared by compaction]"`. Message structure and call/result pairing are
   preserved; no model call is spent.
2. **Summarize** runs only if pruning was not enough (mode `"prune-then-summarize"`, the
   default): one metered call on the Run's own model produces a structured summary — goal,
   constraints, progress, decisions, next steps, critical context — and the model-visible view
   becomes instruction prefix + summary + the kept recent tail. The summarization call's usage
   is charged like any other.

Compaction is a view, never a rewrite. Official history and the canonical Conversation Log are
untouched; a `CompactionPerformed` event reports each reduction. In the durable assemblies (DN
and DC) each compaction also appends a canonical `CompactionCreated` record, and the journal
projection folds
it — covered records render as the summary (or with cleared Tool results) on every later Attempt
and every later Run of the same Conversation. That last part is the point for long-lived
Conversations: without it, every prior Run's raw Tool output replays into every new Run's
opening prompt forever.

If a provider still rejects a prompt as too long, the engine classifies the rejection, compacts,
and issues at most one framework-level retry (transport ambiguity can still duplicate the
external call); a second rejection — or overflow with no `contextTokenLimit` configured — fails
typed with `ContextOverflowError` instead of an opaque provider error.

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

The returned Layer may still require `Crypto.Crypto`; the Cloudflare assembly supplies
`BrowserCrypto`. Everything specific to your compactor must already be provided. The Layer is
acquired once per Durable Object incarnation and rebuilt after eviction. Each call receives the
canonical reconstructed prompt, and its digest-bound artifact changes only the next model request;
the original messages remain in canonical history. Expected compactor failures settle as
`RunContextPreparationError`; defects remain defects for the host to supervise.

## Observing usage

The budget hook's snapshot is cache-aware and context-aware:

```ts
const report = Effect.gen(function* () {
  const usage = yield* budget.snapshot;
  usage.inputTokens; // cumulative input (total, including cached)
  usage.cacheReadInputTokens; // served from provider cache — cheap
  usage.cacheWriteInputTokens;
  usage.lastInputTokens; // ≈ the live context of the most recent call
  usage.lastOutputTokens;
});
```

`lastInputTokens` is the number to watch: it is the Run's actual context size, the quantity
`contextTokenLimit` bounds. Cumulative `inputTokens` will be many multiples of it on any
long Run — that is normal, and with prompt caching most of those tokens are cheap re-reads.
See [Run & stream](/guide/run-agents) for wiring the hook.

## Sizing guidance

- Set `contextTokenLimit` from the model's window minus headroom for output and the summary —
  for a 200k-window model, 150–170k is a reasonable ceiling.
- `keepRecentTokens` (default 20k) is the verbatim tail that survives a compaction. Raise it for
  agents whose recent Tool results are load-bearing; lower it for chatty ones.
- Size `tokenBudget` as a runaway stop, not a work meter — several multiples of
  `contextTokenLimit`, or omit it and bound spend with `costBudgetMicrousd` instead.
- Research-shaped work is cheaper delegated: a scout child reads the files and returns a bounded
  report, so the noise never enters the parent's context; Code Mode collapses many Tool
  round-trips into one brokered program pass. Both compose with everything above.

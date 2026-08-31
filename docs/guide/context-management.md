---
title: Context management
description: Limit model context, compact history, and track token usage.
---

# Context management

Each model request resends its context. Long runs can spend most of their budget rereading old
tool results or exceed the model's context window. Effect Agent bounds tool output, reports the
remaining budget, and compacts old context. Final-answer policy can grant one constrained turn
after turn, tool call, or token exhaustion.

Set these limits from the model and workload. The provider does not supply them:

```ts
AgentPolicy.make({
  maxTurns: 12,
  maxToolCalls: 24,
  maxDuration: "5 minutes",
  toolConcurrency: 4,

  tokenBudget: 200_000,
  completionReserveTokens: 32_000,
  costBudgetMicrousd: 2_000_000,
  contextTokenLimit: 150_000,

  toolResultBounds: ToolResultBounds.make({ maxBytes: 50 * 1024 }),
  runStatus: "appended",
  compaction: CompactionPolicy.make({ keepRecentTokens: 20_000 }),
  onExhaustion: "final-answer",
});
```

`tokenBudget` counts cumulative input and output across the run. `costBudgetMicrousd` uses the
installed cost estimator and cache-split usage. `contextTokenLimit` bounds the live context for one
call. Set it below the model window so output and compaction have room.

## Prompt preparation order

At run start, the runtime evaluates instructions and the definition's optional
[`inputPrompt`](/guide/agents#choose-model-visible-input). Without `inputPrompt`, the model receives
the full encoded input as a JSON user message.

Before each turn, `RunOptions.context.prepare` transforms the source prompt. Compaction then works
on that prepared prompt. The runtime adds the output contract and derived run status last.
Compaction summaries use the prepared prompt. Durable recovery restores committed messages before
applying the transform.

To add application instructions to each request:

```ts twoslash
import { RunContextPreparation, type RunContextHook } from "@effect-agent/engine";
import { Effect, Layer } from "effect";
import { Prompt } from "effect/unstable/ai";

export const metricContext: RunContextHook = {
  prepare: ({ source }) =>
    Effect.succeed({
      prompt: Prompt.concat(
        Prompt.make([{ role: "system", content: "Use metric units in your answer." }]),
        source,
      ),
    }),
};

export const MetricContextLive = Layer.succeed(RunContextPreparation, { hook: metricContext });
```

Pass `metricContext` as the `context` option to `AgentRuntime.run`, `stream`, or `start`.
For durable execution, install `MetricContextLive` when [configuring the runtime](./run-agents#assemble-a-custom-durable-runtime).
Transforms change the model prompt, not stored input or history. Use
[`inputPrompt`](./agents#choose-model-visible-input) to choose which input fields the model sees.

<a id="tool-results-are-bounded-at-the-source"></a>

## Limit tool output

Every application tool result, including MCP output, passes through `toolResultBounds` once before
history or durable storage. Results within the limit keep their encoded bytes. Larger results use
one canonical envelope:

```json
{
  "truncatedToolResult": true,
  "originalBytes": 412887,
  "head": "...first half of the byte budget...",
  "tail": "...last half..."
}
```

The model and journal see the same envelope. Replay therefore stays consistent. The default limit
is 50 KiB. Provider-executed tool results are exempt because the provider has already put them in
the response.

## The run-status message

With `runStatus: "appended"`, each outgoing request ends with a derived status line:

```text
<run-status>turn 3/12 · tool-calls 11/24 · tokens 84210/200000 · research-remaining 83790 · completion-reserve 32000 · last-context 23480 · elapsed 74s/300s</run-status>
```

At 80 percent of a limit, the line asks the model to wrap up. The token warning uses the research
balance after reserving completion capacity. The runtime also warns when that balance cannot cover
another input as large as the last call.

The status line is built for each request and never enters canonical history. Set `runStatus: "off"`
for prompt-sensitive evaluations. Providers that cache at the last user message may need an
explicit cache boundary before this changing suffix. The host owns provider cache fields.

<a id="warnings-and-the-token-soft-landing"></a>

## Budget warnings and finalization

Crossing 80 percent emits one `BudgetWarning` event for that dimension. Turn, tool call, and token
exhaustion follow `onExhaustion`.

With `"final-answer"`, an over-budget tool batch runs no handlers. The next request forbids tool
use, except for the definition's singleton completion tool. Turn exhaustion allows one grace turn.
Token exhaustion completes from the breaching response when it already contains decodable output;
otherwise it allows the same single constrained turn.

The result and `RunCompleted` event report `finishReason: "budget-exhausted"`.
Their `exhausted` field names the limit: `"tokens"`, `"turns"`, or `"tool-calls"`.

Delegated child results carry the same marker through `SubagentCompleted.exhausted` and
`projectResult`. `onExhaustion: "fail"` rejects work after the breach. Duration and cost breaches
always fail because another model call would add time or cost.

## Compaction

With `contextTokenLimit`, the engine estimates the next prompt before every turn. It starts from
the last provider-reported input and estimates appended content. The default compactor then:

1. clears old application tool results outside the protected `keepRecentTokens` tail while keeping
   message structure and call/result pairs;
2. if pruning is insufficient, makes one metered summary call and keeps the instruction prefix,
   summary, and recent tail.

Compaction changes the model view. It never rewrites the conversation log. `CompactionPerformed`
reports each reduction. DN and DC also append `CompactionCreated`, so later attempts and runs use
the same compacted view.

A summary must finish successfully and contain non-whitespace text. The interpreter charges its
usage before validating it. A rejected summary leaves the previous summary and coverage in place;
already committed pruning remains. Durable summaries are limited to 65,536 characters.

If the provider reports context overflow, the engine may compact and retry once. Transport
ambiguity can duplicate that model call. A second rejection, or overflow without
`contextTokenLimit`, fails as `ContextOverflowError`.

### Replace compaction {#replacing-compaction}

Install a `ContextCompactor` Layer to change the strategy, estimator, or summary model. The default
is `ContextCompactor.layer`. All `AgentRuntime` entry points also need a
[Conversation history policy](./conversations#history-policy-and-append-ownership).

```ts
const compactorLayer = ContextCompactor.layerWithModel(summaryModel);

const result = AgentRuntime.run(agent, input).pipe(Effect.provide(compactorLayer));
```

The summary model's Layer requirements stay visible. Its usage is charged under that model's
provider and name.

A custom `compact` implementation emits `CompactionDecision` values. Each decision covers an
exclusive source prefix and either clears old tool results or supplies a summary. The interpreter
rejects cuts through tool pairs, changes to protected instructions or input, decisions that make no
progress, and more than one prune plus one summary in a turn. Summary calls must use
`request.summarize` so metering, response limits, and the run deadline still apply.

`estimate` must return a non-negative finite integer. Strategy failures use `CompactionError`.
Defects and interruption retain their Effect meaning.

Durable coordinators map the covered prefix to complete prior-run records before committing a
decision. A transform or decision that cannot map cleanly fails before the view changes. Decisions
cannot cover the current run. The canonical log remains append-only.

<a id="composing-preparation-and-tool-authorization"></a>
<a id="supplying-a-cloudflare-compactor"></a>

### Install a compactor for durable runs

`contextCompactorRunContextLayer` provides a `RunContextPreparation` service using your compactor:

```ts
import { contextCompactorRunContextLayer } from "@effect-agent/capabilities";
import { ContextCompactor } from "@effect-agent/engine";
import { OpenAiLanguageModel } from "@effect/ai-openai";
import { Layer } from "effect";

export const CompactorLive = ContextCompactor.layerWithModel(
  OpenAiLanguageModel.model("gpt-4.1-mini"),
);

export const RunContextLive = contextCompactorRunContextLayer.pipe(Layer.provide(CompactorLive));
```

Provide the summary model's client to `RunContextLive`, then install it through the
[Node host options](../platforms/node#configure-runtime-services),
[Cloudflare application layer](../platforms/cloudflare#configure-runtime-services), or
[custom runtime assembly](./run-agents#assemble-a-custom-durable-runtime).
To use a prompt transform and a custom compactor together, provide one `RunContextPreparation`
value with both `hook` and `compactor` fields.

### Manage summaries yourself {#explicit-compaction-artifacts}

`@effect-agent/capabilities` also has an application-managed data path.
`prepareModelContext` derives bounded text from a `ConversationSnapshot`.
`digestCompactionSource` binds a `CompactionArtifact` to that source. `applyCompaction` validates
the artifact before replacing covered view messages with its summary. The application creates,
stores, and applies the artifact.

`RetainedFact` values remain artifact metadata. They do not enter the prompt or a separate memory
store automatically. This path is separate from `ContextCompactor`; the interpreter does not call
`applyCompaction`. There is no persistent agent memory API.

## Track usage {#observing-usage}

The budget snapshot separates cumulative and live context usage:

```ts
const report = Effect.gen(function* () {
  const usage = yield* budget.snapshot;
  usage.inputTokens;
  usage.cacheReadInputTokens;
  usage.cacheWriteInputTokens;
  usage.lastInputTokens;
  usage.lastOutputTokens;
});
```

Watch `lastInputTokens` for current context pressure. `inputTokens` is cumulative and grows on
every call. Provider caching may lower its cost. See [Run & stream](/guide/run-agents) for hook
setup.

## Choose limits {#sizing-guidance}

- Leave output and summary room under the model window. For a 200k window, start with a
  `contextTokenLimit` between 150k and 170k.
- `keepRecentTokens` defaults to 20k. Raise it when recent tool output must remain verbatim.
- Use `tokenBudget` as a runaway limit. Use `costBudgetMicrousd` to bound estimated spend.
- Delegate noisy research to bounded children so their raw tool output stays out of the parent
  context.

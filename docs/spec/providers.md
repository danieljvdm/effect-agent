# Effect AI and model providers

Status: **Draft**  
Related decisions: D-002, D-003, D-012, D-019, D-022

## 1. Direct Effect AI integration

The framework does not define `ModelDriver`, `ModelRouter`, or provider wrapper
types. It uses these Effect AI primitives directly:

- `LanguageModel`;
- `Model`;
- `Prompt`;
- `Response`;
- `AiError`;
- `Tool`;
- `Toolkit`;
- `Chat` where its state model fits.

An Agent Binding's Model is an Effect AI `Model`, which is also a Layer providing `LanguageModel`,
`Model.ProviderName`, and `Model.ModelName`.

Provider integration is supplied directly by Effect AI provider packages and their `Model` Layers.

## 2. Interpreter boundary

The framework still owns the multi-Turn Agent state machine and durable boundaries.
It calls Effect AI rather than wrapping it.

Conceptually:

```ts
LanguageModel.streamText({
  prompt,
  toolkit,
  disableToolCallResolution: true,
});
```

The engine requests unresolved Tool Calls so it can:

- wait for a complete assistant response;
- reject truncated Tool arguments;
- apply framework budgets and durable records;
- invoke Effect AI Toolkit handlers;
- control bounded concurrency;
- commit Tool results in deterministic order;
- distinguish ordinary, durable, and uncertain effects;
- construct the next Effect AI Prompt.

This is not a second Tool system. Definitions, handlers, Schemas, failures, approval,
and response parts remain Effect AI values. If Effect AI gains a durable execution
hook that satisfies these needs, prefer using or contributing to it.

Provider-executed built-in Tools are separate capabilities. They are allowed only
when their behavior and recovery limitations are explicit.

## 3. Prompt and Response

Effect AI `Prompt.Prompt` is the model-facing conversation representation.
Framework Conversation history remains a separate durable domain because it also
contains submissions, settlement, approvals, recovery, and operational facts.

The engine maps canonical history into a Prompt and consumes
`Response.StreamPart`. It does not create a competing normalized model-event union.

The reducer validates:

- response start and completion;
- Tool Call identity and complete parameters;
- usage and finish reasons;
- no Tool execution after a length-truncated response;
- no content after terminal completion;
- provider redaction and signature fields needed for replay.

Effect AI Response values may appear in ephemeral APIs. Durable records are explicit,
versioned Schemas so stored data does not depend on TypeScript object prototypes or
an undocumented provider SDK shape.

## 4. Model selection

An Agent Definition is model-agnostic. A separate immutable Agent Binding selects one concrete
Effect AI Model.

```ts
const TriageDefinition = Agent.define("triage", {
  // ...
});

const TriageAgent = Agent.withModel(TriageDefinition, AnthropicClaude);
```

Applications that need dynamic routing can construct or select a Model through an
ordinary Effect service:

```ts
class AgentModel extends Context.Tag("@app/AgentModel")<
  AgentModel,
  Model.Model<any, LanguageModel.LanguageModel, any>
>() {}

const makeTriageAgent = Effect.map(AgentModel, (model) => Agent.withModel(TriageDefinition, model));
```

The framework does not ship a global mutable provider registry. Tenant policy,
availability, cost, and data residency are application Effects and Layers. Dynamic selection
occurs before creating the Binding; the runtime never silently chooses an ambient Model.

## 5. Capability checks

Providers and Models differ in Tool calling, structured output, reasoning, media,
usage, continuation, and context size.

Prefer Effect AI's capability and error types. If a required capability cannot be
checked before a request with current Effect AI APIs, add the smallest framework
preflight needed and propose the general capability upstream.

The runtime must not silently downgrade structured output, Tool behavior, approval,
or content policy.

## 6. Errors

Effect AI `AiError` is the model/provider error type. Effect AI Toolkit handler
errors remain inferred alongside it.

The framework adds errors only for its own behavior, such as:

- invalid Agent input or output;
- Stop Policy exhaustion;
- Conversation conflict;
- persistence and recovery failure;
- approval suspension;
- unknown external Tool outcome.

It does not translate `AiError` into a parallel framework error hierarchy.

Raw response bodies, credentials, and authorization headers are never copied into
canonical failures.

## 7. Retry

- Effect AI and provider code classify model failure.
- Agent policy decides whether and how to retry.
- Provider retry hints are bounded by Agent policy.
- Context overflow may invoke compaction before one configured retry.
- Tool handler errors stay errors unless application code explicitly catches them.
- An unresolved external side effect is never retried merely because an Effect can
  be retried.

Effect `Schedule` is the implementation mechanism, not permission to repeat a side
effect.

## 8. Provider metadata and reasoning

The canonical Conversation may retain provider/model identity, response identity, usage, returned
reasoning content, encrypted/signature fields, and provider redaction markers when Effect AI
exposes them.

The framework does not request or invent hidden reasoning. Partial Tool-argument
deltas are live-only; the completed Tool Call is persisted.

## 9. Effect version policy

- Pin one exact Effect v4 version.
- Upgrade Effect and Effect AI deliberately.
- Compile all public examples against the new version.
- Run Tool, Toolkit, Prompt, Response, provider, streaming, interruption, and
  durability tests.
- Prefer upstream fixes for general Effect AI gaps.
- Do not support multiple Effect versions during private development.

## 10. Testing

`@effect-agent/testing` provides a scripted Layer implementing Effect AI
`LanguageModel`. It can emit deterministic Response streams, failures, malformed
parts, Tool Calls, usage, and interruption.

Provider verification uses the upstream Effect AI provider Layers and covers:

- text and structured output;
- single and parallel Tool Calls;
- malformed/truncated arguments;
- cancellation and timeouts;
- finish reasons;
- usage;
- context overflow;
- rate limits;
- sensitive diagnostic redaction;
- provider-returned reasoning/signature preservation.

Live provider tests are smoke tests. Most engine correctness tests use the scripted
LanguageModel.

## 11. Requirements

- **MODEL-001:** Public model, Tool, Toolkit, Prompt, and Response APIs use Effect AI directly.
- **MODEL-002:** The framework defines no ModelDriver or provider wrapper hierarchy.
- **MODEL-003:** The engine consumes Effect AI Response streams and validates complete Tool Calls.
- **MODEL-004:** Application Tool Calls execute through Effect AI Toolkit handlers under engine
  scheduling and durable boundaries.
- **MODEL-005:** Raw provider SDK values do not become canonical state.
- **MODEL-006:** Effect AI interruption propagates through the Run Scope.
- **MODEL-007:** Retry classification and Agent retry policy remain separate.
- **MODEL-008:** Provider integration uses Effect AI provider `Model` Layers directly.
- **MODEL-009:** Every Effect upgrade passes compile-time and semantic compatibility tests.
- **MODEL-010:** Model selection is explicit in an Agent Binding, and the runtime rejects unbound
  Agent Definitions.

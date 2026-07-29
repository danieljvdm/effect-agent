# Authoring model

Status: **Draft**  
Related decisions: D-002, D-003, D-007, D-008, D-019

## 1. Goals

Agent authors use ordinary Effect vocabulary:

- Effect Schema for data;
- Effect for behavior and typed failure;
- Layer and Context for implementations and requirements;
- Scope for resources;
- Stream for progress;
- Effect AI for Tool, Toolkit, LanguageModel, Prompt, Response, Chat, and Model.

There are no render hooks, module directives, mutable global registries, parallel
validation libraries, or framework-owned copies of Effect AI primitives.

## 2. Agent Definition and Binding

An Agent Definition is immutable, model-agnostic data. An Agent Binding explicitly pairs one
Definition with one Effect AI Model and is the value interpreted by the ephemeral or durable
runtime.

```ts
import { Effect, Schema } from "effect";
import { Model, Tool, Toolkit } from "effect/unstable/ai";
import { Agent, AgentPolicy } from "@effect-agent/core";

const TriageDefinition = Agent.define("triage", {
  input: TriageInput,
  output: TriageOutput,
  instructions: ({ repo, issueNumber }) => Effect.succeed(`Triage ${repo}#${issueNumber}.`),
  toolkit: TriageToolkit,
  policy: AgentPolicy.make({
    maxTurns: 12,
    maxToolCalls: 20,
    maxDuration: "10 minutes",
    toolConcurrency: 4,
  }),
});

const Triage = Agent.withModel(TriageDefinition, AnthropicClaude);
```

`AnthropicClaude` is an Effect AI `Model`, meaning it is also a Layer that provides
`LanguageModel` plus provider/model metadata. Its Layer requirements remain visible in the bound
Agent's Run requirements.

The stable Agent ID is part of durable identity. During private development,
renaming it creates a new identity; no stored-data migration is promised.

### Required fields

- stable ID;
- input and output Schemas;
- instruction source;
- Effect AI Toolkit;
- finite Agent Policy.

An Agent Binding additionally requires one Effect AI Model. An unbound Definition is not runnable.

### Optional fields

- description and metadata;
- context transforms;
- response acceptance policy;
- compaction policy;
- content persistence policy;
- capability requirements;
- application version label.

## 3. Instructions

Instructions may be:

1. a static string;
2. a pure function of decoded input;
3. an Effectful function of decoded input and application services.

```ts
const instructions = (input: SupportInput) =>
  Effect.gen(function* () {
    const policy = yield* TenantPolicy;
    const account = yield* AccountRepo.get(input.accountId);

    return Prompt.fromMessages([
      Prompt.makeMessage("system", {
        content: `Resolve the support request within this policy: ${policy}`,
      }),
      Prompt.makeMessage("user", {
        content: Account.toModelSafeContext(account),
      }),
    ]);
  });
```

Instruction failure remains in the Run error channel. Instructions are evaluated
once per Run by default. A Turn Plan may add context at later Turn boundaries.

Secrets must not be interpolated into model-visible instructions unless an explicit
content policy permits it.

## 4. Effect Schema

Effect Schema is authoritative for:

- Agent input and structured output;
- Effect AI Tool parameters, success, and failure;
- commands and events;
- transport and durable records;
- dynamic state owned by the framework.

Effect AI derives provider-facing schema representations from Tool and structured
output Schemas. Applications do not maintain parallel Valibot, Zod, or provider
schemas.

A model-facing Schema needs a faithful encoded representation. A transform may
decode into a richer domain value, but the encoded side must still be representable
to the model/provider.

Dynamic MCP Tools use Effect AI `Tool.dynamic`. If only raw JSON Schema is available,
the untyped boundary remains visible rather than pretending a lossless static Effect
Schema exists.

## 5. Tool definitions

Use Effect AI `Tool.make` directly:

```ts
import { Tool } from "effect/unstable/ai";

const InspectIssue = Tool.make("inspect_issue", {
  description: "Inspect an issue and classify sensitivity",
  parameters: Schema.Struct({
    repo: Schema.String,
    number: Schema.Int,
  }),
  success: Schema.Struct({
    title: Schema.String,
    sensitive: Schema.Boolean,
  }),
  failure: Schema.Union([IssueNotFound, GitHubUnavailable]),
  failureMode: "error",
  dependencies: [GitHub],
  needsApproval: false,
});
```

Effect AI already owns:

- name and description;
- parameter, success, and failure Schemas;
- handler service dependencies;
- approval;
- failure mode;
- annotations;
- preliminary results.

The framework does not duplicate these fields. Framework-specific durability or
scheduling information should use Agent policy, Tool annotations, or an upstream
Effect AI addition.

Definitions do not close over live clients.

## 6. Tool handlers

Use Effect AI `Toolkit.make` and `Toolkit.toLayer`:

```ts
import { Toolkit } from "effect/unstable/ai";

const TriageToolkit = Toolkit.make(InspectIssue);

const TriageToolkitLive = TriageToolkit.toLayer({
  inspect_issue: (params, context) =>
    Effect.gen(function* () {
      const github = yield* GitHub;
      return yield* github.inspect(params.repo, params.number);
    }),
});
```

Effect AI infers decoded parameters, success, typed failure, and handler service
requirements. Its handler context supplies Tool Call identity and preliminary result
support.

Conversation/run metadata and durable Step execution are separate Effect services.
A Tool that needs them declares those services in `dependencies`. Sandbox access is
handled the same way.

## 7. Tool failure

The default follows Effect AI:

1. Parameter decode failure means the handler never starts.
2. A typed handler failure remains in the Effect error channel.
3. Provider, store, sandbox, or network failures remain errors.
4. Output encoding failure is a defect or AI error.
5. Interruption does not manufacture a Tool result.
6. A durable Tool whose external outcome cannot be established becomes an explicit
   unknown outcome during recovery.

Effect AI `failureMode: "return"` is the deliberate opt-in for turning a typed
failure into a model-visible Tool result. The framework does not do that
automatically.

A valid absence is a successful domain value such as `Option.none`.

## 8. Toolkit composition

Use Effect AI `Toolkit.make`, `Toolkit.merge`, `toHandlers`, and `toLayer` directly.
The framework follows Effect AI's naming, merging, handler, and requirement
semantics.

Tool availability may change only at a documented Turn boundary. Dynamic additions
emit a semantic framework event so durable history can explain which Tools were
available.

## 9. Agent policy

Every Agent has finite defaults:

```ts
interface AgentPolicy {
  readonly maxTurns: number;
  readonly maxToolCalls: number;
  readonly maxDuration: Duration;
  readonly toolConcurrency: number;
  readonly tokenBudget?: number;
  readonly costBudget?: Money;
  readonly repeatedFailureLimit: number;
}
```

The Tool default is bounded parallel execution. The engine wraps Effect AI Toolkit Handler effects
with Effect `Semaphore` permits and owns their structured concurrency. A Run or Tool may require
sequential behavior.

Budget exhaustion is a typed non-success outcome.

Subagent-specific policy is deferred until subagents enter the roadmap.

## 10. Turn Plans

Dynamic behavior is explicit:

```ts
type PrepareTurn<R, E> = (context: TurnContext) => Effect.Effect<TurnPlan, E, R>;

interface TurnPlan {
  readonly prompt: Prompt.Prompt;
  readonly toolkit: Toolkit.Any;
  readonly model: Model.Model<any, LanguageModel.LanguageModel, any>;
  readonly response: ResponsePolicy;
  readonly toolConcurrency: number;
}
```

This replaces hook re-rendering while preserving dynamic instructions, Toolkits, and
Models at clear boundaries.

## 11. Approval

Use Effect AI's `needsApproval` Tool option and approval request/response Prompt
parts. Approval occurs after parameter decoding and before the handler begins.

An ephemeral runtime may return a typed suspension to its caller. A durable runtime
records the approval request and releases the current Attempt. An in-memory callback
is never described as durable.

## 12. Type ergonomics

The framework provides helpers only for Agent-specific types:

```ts
type TriageRequirements = Agent.Requirements<typeof Triage>;
type TriageFailure = Agent.Failure<typeof Triage>;
type TriageInput = Agent.Input<typeof TriageDefinition>;
type TriageOutput = Agent.Output<typeof TriageDefinition>;
type TriageDefinitionRequirements = Agent.DefinitionRequirements<typeof TriageDefinition>;
```

Type tests verify that:

- Effect AI Tool handler requirements remain visible;
- Tool and instruction failures are not widened;
- Effect AI Toolkit handler signatures are preserved;
- output decoding requirements are visible;
- Model Layer requirements appear only after `Agent.withModel`;
- unbound Definitions are rejected by `AgentRuntime`;
- error messages remain readable.

## 13. Requirements

- **AUTH-001:** No application-authored Valibot/Zod schema is required.
- **AUTH-002:** Agent Definitions contain no live mutable runtime state.
- **AUTH-003:** Tools, Toolkits, handlers, approval, and Tool failure types use Effect AI directly.
- **AUTH-004:** Requirements and expected failures are inferred end-to-end.
- **AUTH-005:** Dynamic resources change only at documented Turn boundaries.
- **AUTH-006:** Every default policy is finite.
- **AUTH-007:** Approval precedes handler execution.
- **AUTH-008:** Structured output is Schema-decoded before Run success.
- **AUTH-009:** Public examples compile against the exact pinned Effect version.
- **AUTH-010:** No authoring hook depends on module-global render state.
- **AUTH-011:** The framework defines no competing Tool, Toolkit, LanguageModel, Prompt, Response,
  or Model type.
- **AUTH-012:** Agent Definitions are model-agnostic and only explicit Agent Bindings are runnable.

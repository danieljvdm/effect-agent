# Authoring model

Status: **Draft**

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

An Agent Binding also requires one Effect AI Model. An unbound Definition is not runnable.

### Optional fields

- description and metadata;
- an application run-disposition Schema and decoded-output selector;
- context transforms;
- response acceptance policy;
- compaction policy;
- content persistence policy;
- capability requirements;
- application version label.

### Application run disposition

An application that needs a durable completion classification declares a vocabulary-neutral
Schema boundary on the Definition. The application owns the vocabulary; the framework owns
validation and persistence.

```ts
const TaskRunDisposition = Schema.Literal("application-complete");

const TaskAgent = Agent.define("task-agent", {
  input: TaskInput,
  output: TaskReport,
  instructions,
  toolkit,
  policy,
  runDisposition: {
    schema: TaskRunDisposition,
    fromOutput: (report) => report.runDisposition,
  },
});

type TaskDisposition = Agent.RunDisposition<typeof TaskAgent>;
// "application-complete"
```

`fromOutput` receives the decoded output and returns an untrusted candidate or `undefined`. The
runtime Schema-encodes the candidate before it emits `RunCompleted`; validation failure is the
typed `AgentRunDispositionError`. The disposition Schema's decoding and encoding services remain
visible in the Definition and runtime requirement projections. `Agent.Failure` admits
`AgentRunDispositionError` only for a Definition that declares this boundary. A thrown selector
value is retained on that typed error as a Schema-safe diagnostic `cause`, while the canonical
`RunFailed` message is fixed and does not expose application exception text.

The selector runs only at ordinary completion. Final-answer budget exhaustion, failure,
interruption, abort, unresolved recovery, and run-less joined settlement never acquire a run
disposition. Applications must not derive one from summary prose or arbitrary successful Tool
Calls.

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

## 8.1 Code Mode Tools

Code Mode derives one native Effect AI Tool from an explicit record of selected Tools
([capability specification §9.1](./capabilities.md)). Authoring follows the Delegation
pattern: Tool selection and namespace mapping are fixed at construction, the builder returns an
ordinary Tool plus a handler Layer, and no ambient registry exists (CAP-014).

```ts
const codeMode = CodeMode.make("run_javascript", {
  description: "Run bounded JavaScript that may call the selected tools",
  tools: { warehouse: { query: warehouseQuery } },
  limits: codeModeLimits,
});
// codeMode.tool: an ordinary Effect AI Tool, annotated readonly
// codeMode.handlers: a handler Layer requiring CodeExecutor and the selected handlers
```

The selected handlers' requirements and typed failures remain visible in the composed `R` and
`E`. Construction fails closed on a Tool not annotated `readonly` (an unannotated Tool reads as
`uncertain` under the fail-closed execution-class default), a Tool that requires approval, a
sanitized-name collision, and any Schema the declaration deriver cannot render. Model-facing
declarations present the encoded wire types as documentation only; runtime validation always
uses the original Effect Schemas.

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
  readonly onExhaustion: "final-answer" | "fail";
}
```

The Tool default is bounded parallel execution. The engine wraps Effect AI Toolkit Handler effects
with Effect `Semaphore` permits and owns their structured concurrency. A Run or Tool may require
sequential behavior.

`repeatedFailureLimit` bounds consecutive terminal Tool Call failures across the whole Run. Each
completed Turn folds its terminal Tool Call outcomes into one Run-level counter in declaration
order; any terminal Tool Call success resets the counter. Reaching the limit fails the Run with
the typed policy failure (`AgentPolicyError` with `limit: "repeated-failures"`) at the Turn seam,
before the next model request. A `repeatedFailureLimit` of `0` disables the bound.

Budget exhaustion is never a plain success. `onExhaustion` selects how Turn and Tool Call
exhaustion resolve (runtime spec RUN-018/RUN-019): `"final-answer"` (the default) settles the Run
through one constrained final-answer opportunity and completes it with the distinct
`finishReason: "budget-exhausted"`; `"fail"` fails the Run with the typed `AgentPolicyError`
before the exceeding work starts. Duration, token, cost, and repeated-failure bounds are hard
rails regardless: their exhaustion is always the typed non-success failure.

Subagent policy, hierarchical reservation, and ancestor accounting are specified as a proposal in
[the Subagent specification](./subagents.md); they are not yet an
implemented Agent Policy fields.

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

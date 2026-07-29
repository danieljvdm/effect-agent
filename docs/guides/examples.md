# Target API examples

Status: Phase 0 authoring and runtime subset implemented; later-phase examples remain design targets

The framework-specific API is still a design target. The Effect AI APIs shown here
come directly from the pinned Effect v4 package and must not be wrapped.

These focused API examples complement the
[progressive Travel Planner Reference Application](travel-planner.md), which carries one
application-shaped scenario through every roadmap phase.

## 1. An Effect-native triage Agent

This Agent uses Effect Schema, Effect AI Tools, and an application service supplied through a
Layer.

```ts
import { Context, Effect, Layer, Option, Schema } from "effect";
import { Tool, Toolkit } from "effect/unstable/ai";
import { Agent, AgentPolicy } from "@effect-agent/core";

const TriageInput = Schema.Struct({
  owner: Schema.String,
  repo: Schema.String,
  issueNumber: Schema.Int,
});

const TriageOutput = Schema.Struct({
  severity: Schema.Literals(["low", "medium", "high", "critical"]),
  securityRelated: Schema.Boolean,
  nextAction: Schema.String,
  explanation: Schema.String,
});

const Issue = Schema.Struct({
  title: Schema.String,
  body: Schema.String,
  labels: Schema.Array(Schema.String),
});

class IssueNotFound extends Schema.TaggedErrorClass<IssueNotFound>()("IssueNotFound", {
  owner: Schema.String,
  repo: Schema.String,
  issueNumber: Schema.Int,
}) {}

class GitHubFailure extends Schema.TaggedErrorClass<GitHubFailure>()("GitHubFailure", {
  operation: Schema.String,
  message: Schema.String,
}) {}

interface GitHubService {
  readonly inspectIssue: (
    input: typeof TriageInput.Type,
  ) => Effect.Effect<typeof Issue.Type, IssueNotFound | GitHubFailure>;
}

class GitHub extends Context.Tag("@app/GitHub")<GitHub, GitHubService>() {}

const InspectIssue = Tool.make("inspect_issue", {
  description: "Read a GitHub issue.",
  parameters: TriageInput,
  success: Issue,
  failure: Schema.Union([IssueNotFound, GitHubFailure]),
  failureMode: "error",
  dependencies: [GitHub],
});

const TriageTools = Toolkit.make(InspectIssue);

const TriageToolsLive = TriageTools.toLayer({
  inspect_issue: (input) =>
    Effect.gen(function* () {
      const github = yield* GitHub;
      return yield* github.inspectIssue(input);
    }),
});

export const TriageDefinition = Agent.define("github-triage", {
  input: TriageInput,
  output: TriageOutput,
  instructions: ({ owner, repo, issueNumber }) =>
    Effect.succeed(`
      Triage ${owner}/${repo}#${issueNumber}.
      Inspect the issue, determine severity, and explain the next action.
      Escalate security-related issues privately and immediately.
    `),
  toolkit: TriageTools,
  policy: AgentPolicy.make({
    maxTurns: 6,
    maxToolCalls: 3,
    maxDuration: "2 minutes",
    toolConcurrency: 4,
  }),
});

export const TriageAgent = Agent.withModel(TriageDefinition, ClaudeSonnet);
```

`Tool`, `Toolkit`, and `ClaudeSonnet` are Effect AI values. `TriageDefinition` contains only the
Agent-loop configuration Effect AI does not own; `TriageAgent` is its explicit executable Model
Binding.

## 2. Provide application services and run

```ts
import { AgentRuntime } from "@effect-agent/engine";

const GitHubLive = Layer.effect(
  GitHub,
  Effect.gen(function* () {
    const config = yield* GitHubConfig;
    const http = yield* HttpClient;

    return GitHub.of({
      inspectIssue: (input) => inspectIssueWithGitHub(http, config, input),
    });
  }),
);

const AppLive = Layer.mergeAll(TriageToolsLive, GitHubLive, AgentRuntime.layer);

const program = AgentRuntime.run(TriageAgent, {
  owner: "acme",
  repo: "payments",
  issueNumber: 431,
}).pipe(
  Effect.tap((result) =>
    Effect.logInfo("triage complete", {
      severity: result.output.severity,
      turns: result.turns,
    }),
  ),
  Effect.catchTag("IssueNotFound", () => Effect.logWarning("issue disappeared before triage")),
  Effect.provide(AppLive),
);
```

The runtime uses the Effect AI `Model` stored in the Agent Binding directly.

## 3. Stream progress

```ts
const events = AgentRuntime.stream(TriageAgent, input).pipe(
  Stream.tap((event) =>
    event._tag === "ToolCallStarted"
      ? Effect.logDebug("tool started", { tool: event.toolName })
      : Effect.void,
  ),
  Stream.provide(AppLive),
);
```

The engine internally consumes Effect AI Response parts. It publishes stable Run
Events for application observers.

Disconnecting the owning consumer interrupts an ephemeral Run. Disconnecting from a
durable observation stream only detaches that observer.

## 4. Typed errors and empty results

Effect AI Tool failures remain in the error channel by default:

```ts
const FindCustomer = Tool.make("find_customer", {
  parameters: Schema.Struct({ email: Schema.String }),
  success: Schema.Option(Customer),
  failure: DatabaseFailure,
  failureMode: "error",
  dependencies: [CustomerRepository],
});

const CustomerToolsLive = CustomerTools.toLayer({
  find_customer: ({ email }) =>
    Effect.gen(function* () {
      const customers = yield* CustomerRepository;
      return yield* customers.findByEmail(email);
      // Option.none means the query succeeded and found no customer.
      // DatabaseFailure remains an Effect error.
    }),
});
```

Use `failureMode: "return"` only when the model should receive a typed failure and
continue.

## 5. Approval

Effect AI owns Tool approval:

```ts
const RefundPayment = Tool.make("refund_payment", {
  description: "Refund a captured payment.",
  parameters: Schema.Struct({
    paymentId: PaymentId,
    amount: Money,
  }),
  success: RefundReceipt,
  failure: Schema.Union([PaymentNotFound, RefundRejected]),
  failureMode: "error",
  dependencies: [Payments],
  needsApproval: ({ amount }) => Money.greaterThan(amount, Money.usd(100)),
});
```

The runtime persists Effect AI approval requests in durable mode and does not start
the handler until approval is resolved.

## 6. Bounded parallel Tools

```ts
const ResearchPolicy = AgentPolicy.make({
  maxTurns: 10,
  maxToolCalls: 30,
  maxDuration: "5 minutes",
  toolConcurrency: 4,
});
```

The engine creates a finite Effect `Semaphore` from this policy and runs each Effect AI Toolkit
Handler under one permit. Up to four Tool handlers may run at once. Results are committed in the
model's original Tool Call order.

A Run or Tool may require sequential execution when calls are not safe to overlap.

## 7. Durable submission

```ts
import { DurableAgentRuntime } from "@effect-agent/session";

const submit = Effect.gen(function* () {
  const runtime = yield* DurableAgentRuntime;

  const receipt = yield* runtime.submit(TriageAgent, input, {
    conversationId,
    idempotencyKey: request.headers.get("Idempotency-Key"),
  });

  return {
    receipt,
    events: runtime.observe(receipt),
    settlement: runtime.awaitSettlement(receipt),
  };
});
```

`submit` returns after the Submission Ledger row exists and the Conversation is materialized and
ready. The exact user input is appended to official Conversation history when a worker claims it.

Interrupting `awaitSettlement` does not abort accepted work. Abort is a separate,
authorized durable command.

## 8. Durable Steps through an Effect service

Durability is an additional handler requirement, not a replacement Tool type:

```ts
const SendInvoice = Tool.make("send_invoice", {
  parameters: SendInvoiceInput,
  success: Invoice,
  failure: BillingFailure,
  failureMode: "error",
  dependencies: [Billing, Mail, DurableStep],
});

const InvoiceToolsLive = InvoiceTools.toLayer({
  send_invoice: (input, context) =>
    Effect.gen(function* () {
      const step = yield* DurableStep;
      const billing = yield* Billing;
      const mail = yield* Mail;

      const invoice = yield* step.run("create-invoice", {
        toolCallId: context.toolCallId,
        effect: billing.createInvoice(input, {
          idempotencyKey: `${context.toolCallId}:create-invoice`,
        }),
      });

      yield* step.run("send-email", {
        toolCallId: context.toolCallId,
        effect: mail.sendInvoice(invoice, {
          idempotencyKey: `${context.toolCallId}:send-email`,
        }),
      });

      return invoice;
    }),
});
```

The Step result is recorded once, but its body may run more than once after a crash.
The external idempotency key remains necessary.

## 9. Node and Cloudflare use the same Agent

```ts
const NodeRuntimeLive = Layer.mergeAll(
  NodePlatform.layer,
  SqliteConversationStore.layer,
  SqliteSubmissionStore.layer,
);

const CloudflareRuntimeLive = Layer.mergeAll(
  CloudflarePlatform.layer,
  DurableObjectConversationStore.layer,
  DurableObjectSubmissionStore.layer,
  DurableObjectAlarm.layer,
);
```

These Layers implement the same framework services. The Agent Definition and engine
do not import Node or Cloudflare types.

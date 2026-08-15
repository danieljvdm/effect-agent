---
layout: home
title: Effect Agent
titleTemplate: false
description: Typed agents. Explicit effects. Honest recovery.
sidebar: false
aside: false
pageClass: ea-index
---

<main class="ea-home">
  <section class="ea-home__hero">
    <div class="ea-home__intro">
      <p class="ea-home__eyebrow"><span>●</span> An agent runtime for Effect applications</p>
      <h1 class="ea-home__title">Agents,<em>with cause.</em></h1>
      <p class="ea-home__lede">
        Build autonomous TypeScript agents without leaving Effect behind. Schemas stay canonical,
        failures stay typed, dependencies stay visible, and every resource stays inside a Scope.
      </p>
      <div class="ea-home__actions">
        <a class="ea-button ea-button--primary" href="/guide/getting-started">Get started →</a>
        <a class="ea-button" href="/guide/introduction#one-agent-end-to-end">See one complete Agent</a>
      </div>
    </div>
    <div class="ea-home__contract">
      <p class="ea-home__contract-label">The whole runtime contract</p>
      <ContractPanel
        success="schema-decoded output"
        failure="AI + tool + domain failures"
        requirements="model + handlers + services"
      />
      <p class="ea-home__note">
        <strong>No hidden runtime.</strong> The signature is the architecture.
      </p>
    </div>
  </section>

  <section class="ea-section">
    <div class="ea-axioms">
      <article class="ea-axiom">
        <code>01 / SCHEMA</code>
        <h3>One source of truth</h3>
        <p>Input, output, Tool, wire, and stored values begin as Effect Schemas. Provider shapes are derived.</p>
      </article>
      <article class="ea-axiom">
        <code>02 / LAYER</code>
        <h3>Requirements stay visible</h3>
        <p>Models, Tool handlers, stores, clocks, sandboxes, and policies arrive through explicit Layers.</p>
      </article>
      <article class="ea-axiom">
        <code>03 / SCOPE</code>
        <h3>Nothing outlives its owner</h3>
        <p>Model streams, Tool fibers, queues, clients, and attached children belong to the Run Scope.</p>
      </article>
      <article class="ea-axiom">
        <code>04 / RECORD</code>
        <h3>Recovery stays honest</h3>
        <p>Replay rebuilds state, never external effects. Ambiguous outcomes stay explicit rather than guessed.</p>
      </article>
    </div>
  </section>

  <section class="ea-section ea-home__code">

```ts
const Definition = Agent.define("triage", {
  input: Schema.Struct({ repo: Schema.String, issueNumber: Schema.Int }),
  output: Schema.Struct({
    severity: Schema.Literals(["low", "medium", "high", "critical"]),
    explanation: Schema.String,
  }),
  instructions: ({ repo, issueNumber }) => `Triage ${repo}#${issueNumber}.`,
  toolkit: TriageTools,
  policy: AgentPolicy.make({ maxTurns: 12, maxToolCalls: 20, maxDuration: "10 minutes" }),
});

const Triage = Agent.withModel(Definition, AnthropicLanguageModel.model("claude-sonnet-5"));

const result = AgentRuntime.run(Triage, { repo: "acme/api", issueNumber: 123 }).pipe(
  Effect.provide(AppLive),
  Effect.scoped,
); // Effect<AgentResult<Output>, AgentFailure | DomainFailure, Requirements>
```

  </section>

  <section class="ea-section">
    <div class="ea-paths">
      <a class="ea-path" href="/guide/agents">
        <span class="ea-path__index">BUILD</span>
        <h3>Define an Agent</h3>
        <p>Schema input and output, native Effect AI Tools, a bounded policy, one explicit Model binding.</p>
        <span class="ea-path__arrow">Agent definitions →</span>
      </a>
      <a class="ea-path" href="/guide/run-agents">
        <span class="ea-path__index">RUN</span>
        <h3>Run or observe</h3>
        <p>One interpreter, three views: an Effect result, a semantic Stream, or a scoped detached Run.</p>
        <span class="ea-path__arrow">Run &amp; stream →</span>
      </a>
      <a class="ea-path" href="/concepts/durability">
        <span class="ea-path__index">RECOVER</span>
        <h3>Survive interruption</h3>
        <p>Durable admission, crash recovery, and one honest terminal Settlement — on Node/SQLite and Cloudflare.</p>
        <span class="ea-path__arrow">Persistence &amp; durability →</span>
      </a>
      <a class="ea-path" href="/guide/testing">
        <span class="ea-path__index">PROVE</span>
        <h3>Test without a provider</h3>
        <p>Script model Turns, inject Layers, control time and IDs, and verify the semantic trace offline.</p>
        <span class="ea-path__arrow">Deterministic testing →</span>
      </a>
    </div>
  </section>

  <footer class="ea-home__footer">
    <strong>Pre-1.0. Every documented surface is implemented and tested.</strong>
    <a class="ea-button" href="/guide/getting-started">Build an agent →</a>
  </footer>
</main>

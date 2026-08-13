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
      <p class="ea-home__eyebrow"><span>●</span> Phase 3 available · Phase 4 next</p>
      <h1 class="ea-home__title">Agents,<em>with cause.</em></h1>
      <p class="ea-home__lede">
        Build autonomous TypeScript agents without leaving Effect behind. Schemas stay canonical,
        failures stay typed, dependencies stay visible, and every resource stays inside a Scope.
      </p>
      <div class="ea-home__actions">
        <a class="ea-button ea-button--primary" href="/guide/introduction#one-agent-end-to-end">See one complete Agent →</a>
        <a class="ea-button" href="/guide/getting-started">Build it step by step</a>
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

  <section class="ea-status-strip" aria-label="Implementation status">
    <div class="ea-status-strip__item">
      <StatusBadge status="available" />
      <strong>Bounded interpreter</strong>
      <p>Run, stream, tools, policy, and explicit model binding.</p>
    </div>
    <div class="ea-status-strip__item">
      <StatusBadge status="available" />
      <strong>Operational capabilities</strong>
      <p>Safe-seam input, approval, budgets, context, MCP, sandbox.</p>
    </div>
    <div class="ea-status-strip__item">
      <StatusBadge status="available" />
      <strong>Persistent Conversations</strong>
      <p>Replay, checkpoints, export, memory and SQLite stores.</p>
    </div>
    <div class="ea-status-strip__item">
      <StatusBadge status="next" />
      <strong>Durable accepted work</strong>
      <p>Receipts, Attempts, fencing, recovery, and Settlement.</p>
    </div>
  </section>

  <section class="ea-section">
    <header class="ea-section__header">
      <div>
        <p class="ea-section__eyebrow">Two ways to read these docs</p>
        <h2 class="ea-section__title">Use what exists. Pressure-test what comes next.</h2>
      </div>
      <p class="ea-section__summary">
        Current guides are backed by code and phase evidence. Future guides present the intended
        public interfaces before they exist, so the architecture can be judged from the outside.
        Every page carries its implementation status.
      </p>
    </header>
    <div class="ea-paths">
      <a class="ea-path" href="/guide/introduction#one-agent-end-to-end">
        <span class="ea-path__index">01 / ORIENT</span>
        <h3>See the whole Agent</h3>
        <p>Read one end-to-end file from Schema and Tool definition through Layers and execution.</p>
        <span class="ea-path__arrow">Open the example →</span>
      </a>
      <a class="ea-path" href="/guide/run-agents">
        <span class="ea-path__index">02 / EXECUTE</span>
        <h3>Run or observe</h3>
        <p>Use one interpreter as an Effect result, semantic Stream, or scoped detached Run.</p>
        <span class="ea-path__arrow">See runtime APIs →</span>
      </a>
      <a class="ea-path" href="/guide/conversations">
        <span class="ea-path__index">03 / REMEMBER</span>
        <h3>Persist Conversations</h3>
        <p>Append canonical records, rebuild projections, and resume observation without claiming durability.</p>
        <span class="ea-path__arrow">Explore persistence →</span>
      </a>
      <a class="ea-path" href="/future/durable-execution">
        <span class="ea-path__index">04 / RECOVER</span>
        <h3>Design for interruption</h3>
        <p>Follow the target path from durable admission to one honest terminal Settlement.</p>
        <span class="ea-path__arrow">Inspect Phase 4–5 →</span>
      </a>
      <a class="ea-path" href="/guide/testing">
        <span class="ea-path__index">05 / PROVE</span>
        <h3>Test without a provider</h3>
        <p>Script model Turns, inject Layers, control time and IDs, and verify the semantic trace offline.</p>
        <span class="ea-path__arrow">Open the test kit →</span>
      </a>
      <a class="ea-path" href="/reference/status">
        <span class="ea-path__index">06 / VERIFY</span>
        <h3>Check every claim</h3>
        <p>See the package, phase, evidence, maturity label, and explicit non-claim behind each surface.</p>
        <span class="ea-path__arrow">View status →</span>
      </a>
    </div>
  </section>

  <section class="ea-section">
    <header class="ea-section__header">
      <div>
        <p class="ea-section__eyebrow">Architectural axioms</p>
        <h2 class="ea-section__title">The framework does less so Effect can keep doing more.</h2>
      </div>
      <p class="ea-section__summary">
        Effect Agent owns the multi-Turn loop, Conversation, and recovery model. It deliberately
        does not duplicate Effect AI, invent another dependency container, or turn external side
        effects into magical exactly-once claims.
      </p>
    </header>
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
        <p>Replay rebuilds state, never external effects. Ambiguous outcomes remain explicit rather than guessed.</p>
      </article>
    </div>
  </section>

  <footer class="ea-home__footer">
    <strong>Start with the runtime that exists today.</strong>
    <a class="ea-button" href="/guide/getting-started">Build an agent →</a>
  </footer>
</main>

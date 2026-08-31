---
layout: home
title: Effect Agent
titleTemplate: false
description: A TypeScript agent framework built on Effect and Effect AI.
sidebar: false
aside: false
pageClass: ea-index
---

<div class="ea-home">
  <header class="ea-home__intro">
    <h1>Effect Agent</h1>
    <p>A TypeScript agent framework built on Effect and Effect AI.</p>
    <div class="ea-home__actions">
      <a href="/guide/getting-started">Get started →</a>
      <a href="/guide/introduction">Introduction →</a>
    </div>
  </header>

  <div class="ea-home__code">

::: code-group

<<< @/snippets/travel-planner/planner.ts{ts twoslash}

<<< @/snippets/travel-planner/tools.ts{ts twoslash}

<<< @/snippets/travel-planner/setup.ts{ts twoslash}

:::

  </div>

  <nav class="ea-home__guides" aria-label="Guides">
    <a href="/guide/agents">Agent definitions →</a>
    <a href="/guide/run-agents">Running and streaming →</a>
    <a href="/concepts/durability">Persistence and recovery →</a>
    <a href="/guide/testing">Testing →</a>
  </nav>
</div>

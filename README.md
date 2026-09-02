# Effect Agent

Effect Agent is a TypeScript framework for building agents with
[Effect](https://github.com/Effect-TS/effect) and Effect AI. You supply a model, tools, instructions,
and input/output schemas. It runs the agent loop, executes tool calls, and validates the result.

`AgentRuntime.run` returns an `Effect`. `AgentRuntime.stream` returns a `Stream`. Expected failures
stay in `E`, required services stay in `R`, and every resource belongs to a `Scope`.

Start with the [getting-started guide](docs/guide/getting-started.md) or browse the
[documentation](docs/index.md).

## Install

```sh
npm install --save-exact effect-agent@beta
```

You also need `effect@^4.0.0-rc.111` and an Effect AI provider, such as
`@effect/ai-openai@4.0.0-rc.111`. Your application supplies the provider Layer, credentials, and
tool handlers.

The `effect-agent` package includes core, engine, and capabilities. Persistent history, durable
hosts, storage, and sandbox adapters are separate installs. See the
[package map](docs/reference/packages.md#capability-inventory) for what each package provides.

Effect Agent is in public beta. Releases use `X.Y.Z-beta.N` versions on npm's `beta` channel.
Keep framework packages at the same exact release and choose Effect/provider versions that satisfy
their peer ranges. APIs and stored data may change incompatibly before 1.0, with no compatibility
window or migration promise. Incompatible data must fail clearly and may need a reset.

## What it does

Effect Agent uses Effect AI's tools, models, and provider integrations directly. It adds:

- [Execution limits](docs/concepts/budgets.md) for turns, tool calls, time, token usage, and cost.
- [Tool execution](docs/guide/tools.md) in bounded parallel batches, with results recorded in
  declaration order.
- [Streaming, approvals, and interactive input](docs/guide/run-agents.md). Steering and follow-up
  input wait until the current model response and tool batch finish.
- [Thread history](docs/guide/threads.md) across runs, with
  [context management](docs/guide/context-management.md) for long conversations.
- [Transient recall](docs/guide/context-management.md) from application-owned readable sources.
- [Revision-aware memory stores](docs/guide/context-management.md) with conditional corrections and
  withdrawal.
- [Resumable processing of committed Thread activity](docs/guide/context-management.md).
- [Attached subagents](docs/guide/subagents.md) with explicit permissions and budgets.
- [Scheduled input](docs/guide/operations.md#scheduled-input) and
  [event subscriptions](docs/guide/operations.md#event-subscriptions).
- [Sandbox and browser tools](docs/guide/sandbox.md) for processes, page capture, screenshots, and
  bounded crawling. The [browser guide](docs/guide/browser.md) covers adapters and host requirements.

## Durability

Saving thread history does not make a run durable. Durable execution is available on
[Node.js with SQLite](docs/platforms/node.md) and
[Cloudflare Workers with Durable Objects](docs/platforms/cloudflare.md).

Both hosts save work before acknowledging it, record one terminal settlement per accepted
submission, and reject commits from workers that have lost ownership. They support approval
suspension, joined input, and attached subagents.

Execution is at least once. There is no exactly-once guarantee for external effects. If a worker
disappears after an ordinary tool may have run, recovery records an unknown outcome and does not
replay the call automatically. The [durability guide](docs/concepts/durability.md) explains the
log, submission ledger, and recovery rules.

Cloudflare durability tests run under workerd/Miniflare. Hosted Browser Run verification and
live-model/provider suites are opt-in.

## Limits and safety

There is no hosted service, bundled chat UI, visual builder, or marketplace. Runtime Skills,
framework-owned memory extraction or sharing policy, arbitrary Thread metadata, and dynamic Turn
Plans are not implemented. Subagents cannot nest, hand off, or detach. See the
[capability inventory](docs/reference/packages.md#capability-inventory) for the full list.

Your application owns authorization and isolation. The local sandbox runs trusted code without
isolation. For MCP, your application supplies the connector transport. Read the
[host isolation requirements](docs/guide/operations.md#authorization-and-isolation) before deploying.

For GitHub integrations, see the [read-only PR reviewer](packages/pr-review/README.md).

## Development

Framework packages live in `packages/*`, and runnable examples live in `examples/*`.
Use Vite+ for repository commands. Bun is the package manager.

```sh
vp install
vp run docs:dev
vp run ready
```

`vp run ready` runs static checks, tests, package builds, and the documentation build with link
validation. Before changing code, read the [toolchain guide](docs/TOOLCHAIN.md),
[glossary](GLOSSARY.md), and [contributor instructions](AGENTS.md).

## Similar projects and inspiration

We took inspiration from [Flue](https://github.com/withastro/flue) and
[Pi](https://github.com/earendil-works/pi) for parts of the agent loop, interaction model, and
durability design.

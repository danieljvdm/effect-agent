# Reference-project analysis

Status: Point-in-time research record  
Inspected: 2026-07-28; decision follow-up verified 2026-07-29; Subagent follow-up
2026-07-30

This document records the source snapshots that informed the specification. It is
not a promise that the upstream projects retain the same APIs or internals.

## Snapshots

| Project                                       | Commit                                     | Observed package/version                   |
| --------------------------------------------- | ------------------------------------------ | ------------------------------------------ |
| [Flue](https://github.com/withastro/flue)     | `b814b82b2ce45dc941c77bb010140070e1bd48d5` | `@flue/runtime@2.0.0-nightly.202607240825` |
| [Effect](https://github.com/Effect-TS/effect) | `de2a9a69099993087e57c64df58537c765ac0224` | `effect@4.0.0-beta.102`                    |
| [Pi](https://github.com/earendil-works/pi)    | `027a5847901b5dde30270abaa1041046cd2b4b55` | core packages `0.82.1`                     |

The inspected snapshots live in shallow Git submodules at `repos/flue`, `repos/effect`, and
`repos/pi`, respectively. The submodule gitlinks are the repository record of the inspected
source. When a Flue or Pi snapshot changes, update its gitlink, the commit in this table, and any
affected findings together.

Before implementation begins, refresh this analysis, review upstream release notes,
and record any changed decision inputs. Do not silently update the target Effect
version.

## Flue findings

Flue is substantially more than a thin function around a model call. The inspected
runtime and storage adapters demonstrate concerns including:

- `"use agent"` discovery and hook-style `useModel`/`useTool` authoring;
- session and conversation projections;
- submissions with queued/running/terminalizing/settled operational states;
- settlement records and recovery paths;
- joined delivery and FIFO/session ordering behavior;
- abort intent;
- durable-step behavior;
- store implementations across Redis, relational databases, libSQL, and MongoDB;
- React/client and channel integration packages;
- OpenTelemetry and deployment integrations.

The most valuable lesson is the **accepted-work contract** and the engineering surface needed to
uphold it. The native specification restates the adopted behavior in Effect terms. Flue-specific
component lifecycle, hooks, schema boundary, public types, and store interfaces remain observations
rather than framework contracts.

At the inspected commit:

- Flue writes the durable submission row, materializes the Conversation and attachments, marks the
  submission ready, and then returns its Receipt. The submitted input is appended to canonical
  history only when execution claims it.
- One Conversation processes its oldest unsettled submission first. A running attempt may claim a
  contiguous group of later submissions as `joining`; after canonical input append they become
  `joined` and settle with the host run.
- Joined input is steered only at safe boundaries: before a response, after a completed
  assistant/Tool turn, or when the Agent would otherwise finish.
- Flue explicitly configures Pi Tool execution as parallel and commits the completed batch at the
  Turn boundary.
- Flue stores returned reasoning text, encrypted/signature fields, provider redaction markers,
  complete Tool Calls/results, stop reason, usage, and model metadata. Partial streaming Tool
  argument fragments are not canonical.
- Flue uses distinct Submission and Conversation stream stores. Canonical history is authoritative
  for applied input and terminal settlement, and recovery can repair the ledger from history.

### Behaviors to use as oracles

- acknowledgment after ledger admission plus Conversation materialization/readiness;
- input application when claimed;
- per-Conversation FIFO ordering and joined delivery at safe Turn boundaries;
- settlement after success/failure/abort;
- terminalizing recovery;
- recovery after process loss;
- durable-step replay;
- streaming and session observation;
- separate canonical Conversation stream and operational Submission store;
- canonical-history-to-ledger settlement repair;
- persistence of provider-returned reasoning and signatures.

### Behaviors to reconsider rather than copy

- hook-driven authoring;
- framework lifecycle hidden behind component evaluation;
- schema conversion as the source model;
- provider/runtime coupling inherited through Pi;
- broad parity with every storage/channel adapter;
- any claim stronger than what a native crash suite can prove.

## Pi findings

Pi offers a practical and understandable agent loop, provider abstraction, coding
agent capabilities, and event-oriented execution. It is a strong behavioral
reference for:

- alternating model and tool turns;
- normalized provider events;
- steering/follow-up interaction;
- model-context transformation;
- coding-agent tools and UI/session patterns.

At the inspected commit, Pi has no durable admission Receipt. Steering and follow-up
are in-memory queues until delivered and later persisted as ordinary session
messages. Tool batches run in parallel by default unless sequential execution is
configured or required by a Tool; results are given to the next model turn in
original call order. Steering waits until the current response and Tool batch finish.
Follow-up runs only when the Agent would otherwise stop. Pi's JSONL history stores
provider-returned thinking content and signatures.

Effect Agent's interpreter owns `Effect<A, E, R>`, Scope, Tool scheduling, and durable commit
boundaries. Pi's role in this repository is limited to the source observations above.

## Effect findings

The inspected Effect v4 tree provides stable foundations for:

- `Effect`, typed errors, and environmental requirements;
- `Schema`;
- `Context` and `Layer`;
- `Scope` and resource safety;
- `Stream`, `Queue`, and `PubSub`;
- `Fiber` and structured concurrency;
- scheduling, time, configuration, telemetry, and testing.

AI, persistence, workflow, MCP, and related facilities were observed under
`effect/unstable/*`. Tool, Toolkit, LanguageModel, Prompt, Response, Chat, and Model are direct
public building blocks. The repository pins one exact Effect v4 version and prefers upstream
contributions for missing general behavior.

Effect AI's inspected Tool defaults handler failure to the Effect error channel with
`failureMode: "error"`; `"return"` is explicit. Its LanguageModel Tool resolution
accepts a concurrency option and currently defaults it to unbounded, so the Agent
runtime will supply a finite value.

## Subagent research addendum

The 2026-07-30 follow-up compared the pinned Flue, Pi, and Effect snapshots with these current
official sources:

- [OpenAI Agents SDK: Agents and orchestration](https://openai.github.io/openai-agents-python/agents/)
  and [handoffs](https://openai.github.io/openai-agents-python/handoffs/);
- [Anthropic Managed Agents: multiagent orchestration](https://platform.claude.com/docs/en/managed-agents/multiagent-orchestration)
  and [session operations](https://platform.claude.com/docs/en/managed-agents/session-operations);
- [LangChain/LangGraph: Subagents](https://docs.langchain.com/oss/python/langchain/multi-agent/subagents)
  and [handoffs](https://docs.langchain.com/oss/python/langchain/multi-agent/handoffs);
- [Temporal Child Workflows](https://docs.temporal.io/child-workflows),
  [TypeScript child execution](https://docs.temporal.io/develop/typescript/workflows/child-workflows),
  and
  [Parent Close Policy](https://docs.temporal.io/parent-close-policy);
- [Effect Fibers](https://effect.website/docs/concurrency/fibers/),
  [Scope](https://effect.website/docs/resource-management/scope/), and the pinned v4 source.

These online sources are point-in-time observations, not compatibility dependencies. Anthropic
Managed Agents was public beta material at inspection time. Exact Effect API signatures were
checked against the pinned v4 source because public documentation may describe another release
line.

### Flue Subagents

The pinned Flue snapshot contains a substantial durable Subagent implementation:

- `useSubagent` declares a named, described target and rejects duplicate names
  (`repos/flue/packages/runtime/src/hooks/use-subagent.ts:10-71,138-153`).
- The framework exposes one `task` Tool whose input selects only a declared target and supplies a
  focused prompt, optional working directory, and attachment references
  (`repos/flue/packages/runtime/src/agent.ts:339-401`).
- Each child renders a fresh Subagent frame. It receives its own instructions, Tools, Skills, and
  nested declarations rather than the parent's prompt history
  (`repos/flue/packages/runtime/src/hooks/render.ts:139-174`).
- Child creation persists parent Conversation and Tool Call linkage, increments depth, and
  inherits or overrides Model configuration deliberately
  (`repos/flue/packages/runtime/src/session.ts:4140-4207`).
- Parent abort cascades to active child tasks, and close awaits task settlement and canonical
  flushing (`repos/flue/packages/runtime/src/session.ts:3512-3537,4189-4194`).
- Recovery locates the existing child by exact parent Tool Call identity and resumes its
  Conversation rather than starting a new child
  (`repos/flue/packages/runtime/src/session.ts:2755-2795,2890-2975,5295-5355`).
- Live sibling task calls may run concurrently, while recovery currently rejoins interrupted
  tasks sequentially before the parent batch commits.
- Delegation depth is capped at four, but no separate token, cost, Turn, Tool Call, or breadth
  budget was found. Children share the parent attempt deadline and durability configuration.

The durable link and recovery behavior are strong design inputs. Sharing the parent environment,
working directory, and broad runtime authority is not adopted as a safe default.

### Pi Subagents

Pi's pinned coding-agent repository provides Subagents as an example extension rather than a
runtime-native child abstraction:

- one model-visible Tool supports single, parallel, and chain modes
  (`repos/pi/packages/coding-agent/examples/extensions/subagent/index.ts:1-13,448-472`);
- each invocation launches a separate `pi` process with no session and transfers only a task plus
  a temporary system prompt (`index.ts:267-339`);
- a YAML Agent description may restrict child Tools and choose a Model, but the process otherwise
  inherits its operating-system environment (`agents.ts:11-19,52-71`; `index.ts:294-297`);
- parent cancellation terminates the process, waits, escalates after a deadline, and removes the
  private prompt file (`index.ts:239-246,399-428`);
- parallel mode caps eight tasks and four workers, streams progress, and preserves input-order
  results (`index.ts:33-36,583-663`);
- token and cost values are observed but not enforced as hierarchical budgets;
- no stable child ID, Parent Link, durable Conversation, Receipt, ownership, fencing, reattachment,
  or crash-recovery protocol exists.

The useful lessons are the small Tool surface, clean child context, bounded parallelism, ordered
results, progressive UI, and per-child usage. Process inheritance and in-memory-only identity are
not sufficient framework contracts.

### Current Agent-framework patterns

OpenAI's official Agents SDK distinguishes:

- **agents as Tools**, where a manager retains control and consumes a specialist result;
- **handoffs**, where the recipient receives Conversation history and takes control.

That distinction is adopted. The first Effect Agent Subagent capability is manager-style
delegation; handoff remains separate.

Anthropic Managed Agents demonstrates durable Agent-specific mechanics:

- each child has a context-isolated session thread, history, status, event stream, and parent
  identity;
- a coordinator roster and referenced Agent versions are snapshotted;
- the service limits delegation to one level, twenty unique roster entries, and twenty-five
  concurrent threads;
- Tool and MCP visibility is Agent-specific;
- sandbox, filesystem, and vault credentials are session-shared.

Stable versioning, child identity, bounded concurrency, and separate observation are useful. A
shared sandbox or session-wide credential set is not treated as authority isolation.

LangChain's current Subagent pattern likewise uses manager-called Tools, fresh context by default,
parallel calls, explicit input/output projection, and optional persistent checkpointers. The docs
also note that Tool-wrapped children are not statically discoverable for nested-state inspection.
This reinforces making child identity, projection, and observation first-class framework records
rather than hiding them inside an opaque handler.

### Durable-child and Effect lessons

Temporal Child Workflows provide a useful durability comparison without defining Agent semantics:

- a child has distinct logical identity and event history;
- the parent must await durable child establishment before relying on it;
- result join and parent-close behavior are explicit;
- parent and child share no local state;
- child execution may continue across its own run chain;
- child count is bounded because every child adds durable history;
- durable history does not make external side effects exactly once.

Effect v4 supplies the process-local execution substrate:

- `Effect.forkChild` creates supervised child work;
- `Effect.forkIn` and `Effect.forkScoped` tie work to explicit Scope ownership;
- `FiberSet.make` interrupts tracked Fibers when its Scope closes and can await emptiness;
- `Fiber.join` reflects child success or failure;
- interruption waits for termination and finalizers;
- `Semaphore.withPermit` bounds execution and releases permits under all exits.

An Effect Fiber is an Attempt mechanism, not a durable child identity. Durable Subagents still need
Conversation, Submission, Receipt, ownership, fencing, Settlement, and recovery records.

### Subagent synthesis

The proposed native design takes these combined lessons:

1. Keep delegation distinct from handoff.
2. Expose only declared targets through native Effect AI Tools.
3. Start every invocation with isolated input and context.
4. Derive explicit least-authority grants rather than cloning a runtime environment.
5. Bound depth, breadth, concurrency, Turns, Tools, duration, bytes, tokens, and cost separately.
6. Reserve parallel child budgets from the parent and charge usage to every ancestor.
7. Supervise ephemeral children in the parent Scope and forbid daemon Fibers.
8. Give durable children distinct Conversations and accepted-work obligations.
9. Persist child establishment before relying on it and reattach by exact parent Tool Call
   identity.
10. Join only verified, Schema-decoded terminal output into deterministic parent Tool order.
11. Keep full child history separately observable instead of merging it into parent context.
12. Let unresolved child external effects remain unknown rather than fabricating a result.

The normative version is [the Subagent specification](spec/subagents.md).

## Synthesis

The recommended division of labor is:

| Concern                           | Primary source of inspiration                    | Native owner                                   |
| --------------------------------- | ------------------------------------------------ | ---------------------------------------------- |
| Authoring, typing, resources      | Effect                                           | Effect Agent core                              |
| Model/tool turn loop              | Pi and Effect AI                                 | Effect Agent engine using Effect AI primitives |
| Subagent delegation               | Flue, current Agent frameworks, Effect, Temporal | Effect Agent capabilities and durable runtime  |
| Provider implementation           | Effect AI                                        | Effect AI provider Models/Layers               |
| Accepted-work durability          | Flue                                             | Effect Agent durable runtime                   |
| Application schemas               | Effect Schema                                    | Effect Agent definitions                       |
| Store/provider/platform mechanics | Upstream adapters and platform docs              | Leaf adapters                                  |

This division of responsibility makes Effect Agent's authoring, runtime, provider, and durability
contracts native Effect specifications. Flue and Pi remain the attributed research sources
described in this document.

## Research limitations

- This was source inspection, not a production load test.
- Upstream internal behavior may not be part of public compatibility promises.
- The native durability protocol is specified but unimplemented and still needs executable
  modeling, adapter conformance, and destructive crash testing.
- Licensing and attribution must be reviewed before copying any code or fixtures.
- Provider behavior and Effect unstable APIs must be revalidated at implementation
  time.

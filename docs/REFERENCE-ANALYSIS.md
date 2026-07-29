# Reference-project analysis

Status: Point-in-time research record  
Inspected: 2026-07-28; decision follow-up verified 2026-07-29

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

## Synthesis

The recommended division of labor is:

| Concern                           | Primary source of inspiration       | Native owner                                   |
| --------------------------------- | ----------------------------------- | ---------------------------------------------- |
| Authoring, typing, resources      | Effect                              | Effect Agent core                              |
| Model/tool turn loop              | Pi and Effect AI                    | Effect Agent engine using Effect AI primitives |
| Provider implementation           | Effect AI                           | Effect AI provider Models/Layers               |
| Accepted-work durability          | Flue                                | Effect Agent durable runtime                   |
| Application schemas               | Effect Schema                       | Effect Agent definitions                       |
| Store/provider/platform mechanics | Upstream adapters and platform docs | Leaf adapters                                  |

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

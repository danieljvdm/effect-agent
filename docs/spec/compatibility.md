# Effect interoperability and source-material policy

Status: Draft

Effect Agent is implemented with Effect and Effect AI. Flue and Pi are attributed source material
used to study useful behavior. They are not architectural layers: every supported API, runtime
rule, storage contract, provider boundary, and compatibility promise is defined natively in this
repository.

## 1. Effect interoperability

Public Agent code uses the pinned Effect v4 package and Effect AI primitives directly:

- Effect and Stream;
- Schema;
- Context and Layer;
- Scope, Fiber, and Semaphore;
- Tool and Toolkit;
- LanguageModel and Model;
- Prompt and Response;
- Effect AI provider Layers;
- Effect AI MCP facilities where applicable.

An application Tool is the Effect AI Tool given to the model. An Effect AI Toolkit is executed
through its native handlers under the engine's bounded scheduling and durability boundaries.

The engine introduces only concepts that Effect AI does not own: Agent Definitions, Runs,
Conversations, Submissions, Settlements, canonical history, recovery, and unknown external
outcomes.

## 2. Native compatibility boundary

Effect Agent's public contract is its own Effect-native package and record surface. Compatibility
means:

- one documented Effect version per framework release;
- direct composition with the named Effect AI primitives and provider Layers;
- stable behavior for the requirement IDs implemented by that release;
- explicit capability and maturity labels for storage and platform packages;
- schema-version checks for current persisted records.

Source research does not create runtime, API, stored-data, provider, or authoring compatibility
with the researched projects. Their names do not appear in package boundaries or runtime
configuration.

## 3. Missing Effect AI behavior

When the framework needs behavior Effect AI does not expose:

1. confirm the need in an executable Agent scenario;
2. determine whether annotations or ordinary Effect services are sufficient;
3. propose generally useful behavior upstream;
4. keep any temporary framework extension narrow and outside a duplicate primitive;
5. remove the extension when upstream support is available.

Durability-specific concepts remain framework-owned because they are agent-runtime and
accepted-work concerns, not model-provider abstractions.

## 4. Source material

The exact inspected commits and findings live in
the pull requests that introduced them. Specification text never interprets
upstream internals in detail. The matching source snapshots are checked into this repository as
shallow Git submodules: Flue at `repos/flue` and Pi at `repos/pi`. These paths provide local
reference material only; product packages and CI do not depend on them.

Flue contributed research about:

- durable admission and Receipts;
- ordered Conversation submissions and joined input;
- separate canonical history and operational obligations;
- recoverable settlement;
- unknown outcomes, recovery, and durable Steps;
- persistence of provider-returned reasoning and signatures.

Pi contributed research about:

- a small understandable model/Tool loop;
- parallel Tool execution with ordered results;
- steering after a complete response and Tool batch;
- follow-up input when an Agent would otherwise stop;
- session trees, compaction, and visible thinking persistence.

These observations are inputs to design. Once adopted, the resulting rule is stated in native
domain language in the relevant Effect Agent specification and is owned by that specification.

## 5. Behavioral source tests

Tests may encode scenarios learned from source research, including:

- queued input ordering;
- parallel Tool completion with deterministic result order;
- steering and follow-up boundaries;
- interrupted provider streams;
- terminalizing recovery;
- canonical-history-to-ledger repair;
- provider-returned reasoning round-trips.

When exact upstream behavior matters, the test or design note cites the inspected source snapshot.
The test itself runs against Effect Agent and Effect AI and asserts a native requirement ID.

## 6. Effect version policy

Private development supports one exact Effect v4 version at a time.

- the root Bun catalog pins it exactly and workspace manifests use `catalog:`;
- all packages upgrade together;
- `bun run sync:effect` checks out the matching `effect@<version>` source tag in `repos/effect`;
- public examples are compile-time tests;
- Tool/Toolkit type inference is checked;
- Response/Prompt round-trips and provider smoke tests run;
- stored current-version fixtures are verified;
- no multi-version support or compatibility shim is promised.

Effect v4 leaving beta does not automatically trigger an upgrade; the project upgrades after its
suite passes.

An Effect upgrade commits the root catalog, Bun lockfile, and Effect submodule pointer together.
CI uses installed packages and does not require the source submodule.

## 7. Stored data compatibility

There is no data migration promise during private development.

Current records are version-tagged and unsupported versions fail clearly. Developers may reset
SQLite data or replace Cloudflare development namespaces after breaking changes.

Compatibility and migrations are designed before external users depend on stored data.

## 8. Project distribution

The project is private and internally consumed. Working package names may change. Public npm
distribution, licensing, governance, and long-term compatibility commitments are deferred until
open-source preparation.

`@danieljvdm/agent-skills` is repository contributor tooling. It is neither a runtime dependency
of framework packages nor the product's runtime Skill format.

Before open source:

- choose final package names and license;
- review copied ideas, fixtures, and attribution;
- define supported Effect versions;
- decide whether stored data must survive upgrades;
- publish the actual tested capability matrix.

## 9. Requirements

- **COMPAT-001**: Public AI authoring uses Effect AI primitives directly.
- **COMPAT-002**: Normative behavior is defined by Effect Agent specifications, not by an external
  project's implementation.
- **COMPAT-003**: Flue and Pi are attributed source material and establish no runtime, API,
  stored-data, provider, or authoring compatibility contract. Their pinned source snapshots live
  at `repos/flue` and `repos/pi`.
- **COMPAT-004**: Behavior derived from source research cites the inspected snapshot and maps to a
  native requirement and test.
- **COMPAT-005**: Generally useful missing Effect AI behavior is proposed upstream before creating
  a competing abstraction.
- **COMPAT-006**: The repository pins one exact Effect v4 version.
- **COMPAT-007**: Effect upgrades run the complete type and semantic suite.
- **COMPAT-008**: Private development makes no stored-data migration promise.
- **COMPAT-009**: Public distribution decisions are deferred until open-source preparation.
- **COMPAT-010**: The root catalog, lockfile, workspace manifests, and local Effect source checkout
  resolve one exact Effect v4 release.

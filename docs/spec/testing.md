# Testing and Verification Specification

Status: Draft

The framework's central claims—typed behavior, structured concurrency,
deterministic commits, and durability—must be executable properties. Example tests
alone are insufficient.

## 1. Test layers

### 1.1 Schema tests

For every public request, event, error, persisted record, and adapter payload:

- valid generated values round-trip;
- invalid shapes fail with the intended issue;
- size/depth limits are enforced;
- redacted fields remain redacted;
- version upgrades preserve declared compatibility.

### 1.2 Unit tests

Pure transition and policy functions are tested without providers or stores:

- stream reduction;
- stop policy;
- tool scheduling plan;
- retry classification;
- recovery classification;
- budget accounting;
- authorization normalization;
- context selection and compaction coverage.

### 1.3 Service tests

Each Effect service is tested with deterministic TestClock, test providers, and
scoped resources. Tests assert typed failures and interruption cleanup, not only
returned values.

Durable progress waits additionally force both lost-wakeup interleavings (notify after subscribe
but before the canonical check, and notify after the check but before park), count canonical reads
over an advanced TestClock, broadcast to concurrent same-conversation waiters, isolate unrelated
lanes, and verify cancellation cleanup. The Cloudflare suite repeats the public client/Object
boundary with typed authorization/store failures, deterministic client cryptography, explicit
waiter-registration latches, fiber interruption, and a real `ctx.abort()` reconstruction (#94).

### 1.4 Adapter conformance

Every implementation of a framework port runs the same behavioral suite. An
adapter cannot be called compatible based on type-checking alone.

### 1.5 Integration tests

Exercise a real engine with fake deterministic model, tools, persistence, and event
subscribers.

### 1.6 End-to-end tests

Exercise supported hosts with real database processes and, in a quarantined suite,
selected real model providers.

## 2. Deterministic model

The repository provides a scripted Layer implementing Effect AI `LanguageModel` that can:

- emit text and structured deltas;
- request one or many tools;
- interleave usage and metadata;
- fail before, during, or after streaming;
- hang until interrupted;
- emit malformed protocol sequences;
- vary capability negotiation;
- assert the normalized request it received.

This Layer is the default for engine tests. Real providers are not required for
most correctness coverage.

A scripted Layer cannot catch model-visible information that is missing from the request — it
plays back its script regardless of what the request said. The engine's final-output path is
therefore additionally exercised against a live-shaped LanguageModel substitute that derives its
responses only from the model-visible request (TEST-016).

## 3. Runtime state-machine tests

Generated command sequences cover:

- zero or more model turns;
- tool-free completion;
- sequential and parallel tool batches;
- tool rejection, declared failure, defect, timeout, and interruption;
- steering and follow-up races;
- approval timing;
- compaction success/failure;
- child-agent completion/interruption;
- budget exhaustion;
- subscriber backpressure;
- parent Scope closure.

Properties include:

- terminal state is reached at most once;
- committed tool results preserve deterministic order;
- no tool result exists without a requested call;
- all started child Fibers settle before required parent completion;
- a run never continues after unresolved uncertainty;
- public event sequences satisfy the event Schema.

## 4. Durability model

The project maintains a small executable reference model of:

- admission;
- conversation order;
- Attempt ownership/epoch;
- canonical append;
- tool preparation/settlement;
- recovery classification;
- terminalization.

Random command and crash sequences run against both the model and a store adapter.
Observable state must agree.

## 5. Crash injection

Every durable boundary supports injected failure:

- before operation;
- after side effect but before return;
- after transaction commit but before acknowledgment;
- between provisional event emission and canonical append;
- during ownership renewal where the platform requires it;
- during adapter shutdown.

The suite kills actual worker processes for Node/SQLite tests and forces Durable
Object eviction/alarm retries in Cloudflare tests, not only Fiber interruption.
After recovery it asserts:

- no accepted submission disappears;
- no submission has multiple terminal outcomes;
- stale epochs cannot append;
- ordering is preserved;
- safe work resumes;
- uncertain ordinary tools do not replay automatically;
- terminalizing work eventually settles.

The public Durable Object RPC/alarm seam also carries the maintenance regressions from
[issue #93](https://github.com/danieljvdm/effect-agent/issues/93): a stable approval wait clears
its alarm; a forced caught-up alarm performs no SQLite, ledger, recovery, or canonical-history
work; mutations racing acknowledgement and mutations held after pre-arm remain dirty; and a child
evicted after settlement finalization has already committed its parent marker/generation.

The exported durable runtime/store seam carries the active-pass regression from
[issue #96](https://github.com/danieljvdm/effect-agent/issues/96): multiple nonterminal
Submissions sharing one multi-page Conversation history receive mixed deterministic recovery
decisions while the pass issues exactly the captured prefix's page count once, rather than once
per Submission. The fixture uses the real reference `ConversationStore` and `SubmissionLedger`
Layers, not the pure classifier or a fabricated recovery snapshot.

The crash-point table in [durability.md](./durability.md) is a minimum coverage
matrix and should be machine-linked to test names.

## 6. Store conformance

Required cases:

- duplicate admission with same/different payload;
- concurrent admission into one conversation;
- duplicate batch append;
- same batch ID with different content;
- stale tail conflict;
- stale producer fence;
- ownership loss and reclaim;
- concurrent terminal outcomes;
- terminal retry after lost acknowledgment;
- strong recovery read after write;
- checkpoint corruption/mismatch;
- unsupported stored versions fail before mutation;
- projection rebuild;
- retention/deletion disposition.

The in-memory adapter must implement the same conflicts so engine tests do not learn
behavior that production stores reject.

## 7. Effect AI provider verification

The engine integration and selected Effect AI provider Models are tested for:

- request normalization;
- tool Schema translation;
- ordered event translation;
- usage and finish-reason mapping;
- cancellation and resource release;
- timeout and retry classification;
- malformed provider responses;
- provider capability behavior;
- redaction of credentials and raw payloads;
- optional cassette replay.

Live tests are rate-limited and do not gate ordinary pull requests unless Effect AI
integration or provider configuration changed.

## 8. Tool conformance

Tool helpers and remote adapters are tested for:

- input/output Schema enforcement;
- defect sandboxing;
- timeout and interruption behavior;
- approval and authorization;
- output size limits and redaction;
- concurrency permits;
- idempotency key propagation;
- unknown-outcome classification.

## 8.1 Code Mode conformance

Every `CodeExecutor` adapter — the deterministic `unisolated` substitute and each isolated
adapter — runs the shared contract cases:

- successful bounded JSON computation;
- invalid and oversized source;
- wall-clock exhaustion;
- excessive captured output and final result;
- unknown host method and host-call limit exhaustion;
- malformed adapter result;
- interruption running every pass finalizer;
- honest `isolated` versus `unisolated` posture reporting;
- typed rejection of every limit or network policy the adapter cannot enforce.

Enforcement cases run only against `isolated` adapters, because the `unisolated` substitute
cannot honestly prove them and must not pass them by simulation:

- ambient network denied by default;
- a synchronous runaway program terminated by an enforced platform CPU limit.

Broker semantics are asserted once against the engine seam: direct and programmatic invocation
of the same Tool preserve parameter, handler, success, failure, requirement, and interruption
behavior; typed handler failures stay typed; approval or policy denial prevents handler start;
budget exhaustion prevents the next call; sequential identities are deterministic; and
non-allowlisted or sanitized-name collision cases fail closed.

## 8.2 Pull-request work-order evidence

The private `examples/pr-work-orders` leaf runs one work order against a
deterministic scripted implementer and a real temporary Git repository. It
proves explicit dispatch, a jailed implementation Agent, host-collected
patch/check validation, one attempt per work-order key, atomic head update,
non-publication settlements, interruption cleanup, and typed release failure.
These tests are evidence for the local host contract, not for check isolation
or GitHub ingress. See [pull-request work orders](pr-work-orders.md) and
[work-order ingress](pr-work-order-ingress.md).

## 8.3 Pull-request work-order ingress evidence

The private `examples/pr-work-order-ingress` leaf uses recorded GitHub event
fixtures and a fake GitHub API. It proves authenticated mention and reaction
dispatch, Actions payload binding, unique inline targeting, actor-id
authorization, same-repository non-fork admission, stale-anchor rejection,
file-backed duplicate-delivery replay, isolated check-process credentials,
fail-closed publisher path and identity verification, atomic compare-and-swap
fencing, and one host-authored thread reply that does not resolve the thread.
Tests do not call live GitHub and do not enable a repository workflow.

## 9. Compatibility tests

The suite tests:

- public TypeScript type examples using `tsc`;
- package export maps under ESM;
- a frozen Bun lockfile install;
- Vite+ package task ordering and library builds;
- supported Node versions;
- exact supported Effect versions;
- event decoding across the compatibility window;
- checkpoint rejection/rebuild across engine versions;
- Node and Cloudflare host equivalence for the current stored version.

Compile-time tests assert the intended `Effect<A, E, R>` environment and error
inference.

## 10. Security tests

- tenant isolation and IDOR attempts;
- policy bypass through alternate tools or subagents;
- prompt-injected requests for secrets or privileged actions;
- command/path/URL normalization;
- sandbox filesystem, network, process, and resource boundaries;
- secret and PII redaction in events, errors, spans, logs, snapshots, and cassettes;
- malicious Schema depth/size;
- forged producer epochs and receipts;
- approval replay/expiry;
- artifact integrity and content-type confusion.
- Subagent establishment without concrete child-resource authority and revocation after admission;
- parent approval replay against child actions, siblings, retries, or descendants;
- parent/child/Receipt IDOR across observer reconnects and paginated reads;
- cross-tenant artifact digest substitution, deduplication leakage, and classification laundering;
- child failure, progress, and provenance payloads containing secret-bearing values;
- hierarchical reservation races, accounting replay, and recursive fan-out attempts;
- generated Code Mode programs probing ambient network, bindings, secrets, or non-allowlisted
  Tools;
- SQL mutation, DDL, multi-statement, dangerous-function, and cross-tenant attempts against the
  read-only reference Tool;
- executor egress of credentials or raw intermediate rows through results, logs, or telemetry.

High-risk parsers and protocol adapters receive fuzz tests.

## 11. Performance tests

Benchmarks report, without hiding environment:

- engine overhead for tool-free and tool-heavy turns;
- event throughput and subscriber backpressure;
- admission and canonical append latency;
- scheduler claim throughput;
- recovery scan throughput;
- memory retained per active run and conversation;
- compaction cost;
- projection lag under load.

Performance gates should guard regressions in framework overhead, not promise model
provider latency.

## 12. Test fixtures and golden data

Golden provider/event fixtures are versioned, minimal, redacted, and annotated with
their source adapter version. Golden snapshots do not replace semantic assertions.

The Travel Planner Reference Application (the cumulative fixture tree in
`packages/testing/src/fixtures/travel-planner`) is the application-shaped fixture:

- its ordinary suite uses the scripted Effect AI `LanguageModel`, deterministic travel-service
  Layers, controllable time and IDs, and no network or credentials;
- each capability slice extends the same scenario and retains all earlier assertions;
- public API slices compile from package-local tests or fixtures rather than an application
  workspace;
- live model and supplier profiles are opt-in smoke or release tests, rate-limited and structurally
  redacted;
- persistent and durable profiles identify deployment class `P`, `DN`, or `DC` explicitly.

Test IDs use requirement references:

```ts
describe("DUR-009 ordinary tool uncertainty", () => {
  // ...
});
```

This enables generated requirement coverage reports.

## 13. CI gates

Every pull request:

- `bun install --frozen-lockfile` with the pinned Bun version;
- `bun run ready`, covering Vite+ formatting, linting, type checks, tests, and package builds;
- type-check and compile-time tests;
- unit and deterministic integration tests;
- in-memory conformance;
- package boundary and duplicate-Effect-AI-primitive check;
- docs link and requirement-ID validation.

Adapter changes additionally run the relevant conformance suite. Release candidates
run databases, process-kill crash tests, security tests, examples, package install
tests, and live provider smoke tests.

No durability milestone is complete while its crash tests are skipped.

## 14. Requirements

- **TEST-001**: Public Schemas have round-trip, invalid-input, and limit tests.
- **TEST-002**: Core engine tests use a deterministic scripted Effect AI LanguageModel.
- **TEST-003**: Runtime state-machine invariants receive generated sequence tests.
- **TEST-004**: All store adapters run a shared conformance suite.
- **TEST-005**: Durable adapters pass real process-kill crash tests.
- **TEST-006**: Every durability crash boundary maps to at least one test.
- **TEST-007**: Effect AI integration and selected provider Models run shared semantic tests.
- **TEST-008**: Security boundaries receive adversarial tests.
- **TEST-009**: Compile-time tests protect `Effect<A, E, R>` inference.
- **TEST-010**: CI rejects framework-owned duplicates of Effect AI primitives and keeps
  source-research projects outside the runtime dependency graph.
- **TEST-011**: Requirement IDs are linked to executable tests or an explicit
  future milestone.
- **TEST-012**: Skipped durability tests prevent a durable release.
- **TEST-013**: CI rejects lockfile drift, multiple workspace Effect versions, and an Effect source
  tag that disagrees with the root catalog.
- **TEST-014**: The Travel Planner Reference Application remains a cumulative compiling and
  executable fixture for the full framework surface; its ordinary suite is deterministic and
  offline, while live provider profiles are opt-in.
- **TEST-015**: Every `CodeExecutor` adapter passes the shared executor conformance suite, and
  direct versus programmatic invocation of the same Tool is observably equivalent.
- **TEST-016**: The engine's final-output path is exercised against a live-shaped LanguageModel
  substitute that derives its responses only from the model-visible request (prompt text,
  advertised schemas, tools) — never from test-known expected values — so offline suites catch
  model-visible contract information missing from the request that a scripted model would
  fabricate away.
- **TEST-017**: The trusted-local work-order proof covers success, non-publication
  settlements, path escape, false check claims, failed host checks, stale-head
  publication fencing, typed/interrupted cleanup, and one-attempt admission.

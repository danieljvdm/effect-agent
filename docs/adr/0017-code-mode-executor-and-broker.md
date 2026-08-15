# ADR-0017: Code Mode executor port and native Tool broker

- Status: Proposed (design defaults resolved by owner 2026-08-14; implementation unassigned)
- Related decisions: [D-002](../DECISIONS.md#d-002--relationship-to-effect-ai),
  [D-003](../DECISIONS.md#d-003--tool-and-toolkit-ownership),
  [D-007](../DECISIONS.md#d-007--tool-scheduling-default),
  [D-009](../DECISIONS.md#d-009--ordinary-tool-uncertainty),
  [D-025](../DECISIONS.md#d-025--slim-toolchain-and-canonical-effect-source),
  [D-030](../DECISIONS.md#d-030--durable-tool-uncertainty-steps-and-suspension-records),
  [D-035](../DECISIONS.md#d-035--code-mode-capability)
- Working plan: [CODE-MODE-PLAN.md](../CODE-MODE-PLAN.md)

## Context

A model that must chain several Tool calls today pays one model Turn per step and carries every
intermediate result through model context. Code Mode inverts this: the model writes one bounded
JavaScript program as a native Tool argument; the program runs in an isolated executor, calls an
explicit allowlist of existing Effect AI Tools through typed sandbox globals, filters and shapes
intermediate data locally, and returns one bounded final value. Cloudflare's Code Mode and
Anthropic's programmatic tool calling are prior art; neither can be adopted as the framework's
semantic layer without violating ADR-0002.

Constraints in force: no framework-owned Tool or Toolkit copy (ADR-0002/D-003); Tool concurrency
stays bounded and deterministic (D-007); model output is untrusted, fail-closed input; the
existing command-shaped `SandboxRequest` cannot represent live host callbacks and must not be
overloaded; the engine tool path has no per-Tool authorization stage and no result
redaction/size-bounds stage, and budgets are evaluated at Turn boundaries; D-025's phase gate
constrains new packages.

## Decision

1. **Capability placement.** The Code Mode Tool builder lives in `@effect-agent/capabilities`.
   The builder follows the Delegation pattern: an explicit record of selected Tools plus
   namespace mapping at construction, returning an ordinary Effect AI Tool and a handler Layer,
   with no ambient registry. Selected handler requirements and failures remain visible in `R`
   and `E`.
2. **Executor port.** A callback-capable `CodeExecutor` service is added to
   `@effect-agent/sandbox` as a sibling of the existing command-based `Sandbox`. Requests,
   results, limits, and expected errors are Effect Schemas; the per-pass `CodeExecutionHost` is an
   Effect service in the executor's requirement channel, provided at the pass edge, and its live
   host-call bindings are scoped resources that are never persisted; implementations reuse the
   `SandboxImplementation`
   isolation-posture idiom (CAP-010) and reject limits they cannot enforce. This adds the
   documented `capabilities -> sandbox` dependency edge.
3. **Engine-owned broker.** Programmatic invocation reaches original Toolkit handlers only
   through an engine-owned, per-Run broker seam in the pattern of `AgentSpawner` and
   `DurableStep`. It shares the existing per-call path (lookup, parameter handling, approval
   preflight, bounded scheduling, scoped execution, typed failure handling, encoding, per-call
   telemetry) and introduces two new behaviors: mid-pass Tool-call budget consumption, and
   broker-owned result size bounds and redaction at the sandbox boundary. Per-Tool authorization
   remains application- and handler-owned; no engine authorization stage is added.
4. **Determinism.** Inner calls receive stable identities derived from the outer `ToolCallId`
   plus a zero-based sequence index, execute strictly sequentially under the parent Tool Call's
   already-held scheduling permit (never acquiring batch permits of their own), and a host call
   issued while another is unsettled fails with a typed concurrency error.
5. **Model contract.** The generated program is one async function expression invoked by a fixed
   harness entrypoint. Model-facing TypeScript declarations are derived from the encoded side of
   the original Schemas via Effect AI's JSON-schema derivation and fail Tool construction closed
   on non-renderable schemas; runtime validation always uses the original Schemas. A failed inner
   call rejects with a Schema-encoded envelope carrying the existing framework error tags. The
   final result, captured logs, and thrown values form one model-visible egress surface under a
   single aggregate byte budget and redaction policy.
6. **First slice.** Read-only only: the builder rejects non-`readonly` and approval-requiring
   Tools at construction and annotates the outer Tool `readonly`. Deployment class `E` only. The
   reference integration is a raw-SQL Tool over curated read-only views whose safety comes from
   database authority (read-only principal, tenant scoping, statement/row/byte/timeout bounds);
   overlarge results succeed as `truncated: true` by default with a per-Tool typed-failure
   override.
7. **Cloudflare adapter.** The first isolated executor is a Dynamic Worker Layer in
   `@effect-agent/platform-cloudflare`: one fresh Worker per pass, `globalOutbound: null`, only
   the scoped broker RPC stub as capability, platform CPU limits stopping synchronous runaways, an
   executor-owned wall-clock deadline interrupting asynchronously suspended passes (cancelling
   outstanding host calls and disposing the Worker), all handles disposed in Scope finalizers. No cost or performance claim is made before measurement; any
   future stable-ID Worker caching must include tenant and binding context in cache identity.
8. **Durability is deferred.** `DN`/`DC` Code Mode (abort-and-replay with recorded inner calls, a
   new canonical record family, reads recorded by default, a content-addressed execution digest,
   and reconciliation halts at prepared-unsettled `uncertain` calls) requires its own accepted
   ADR and canonical record design before any `DN`/`DC` claim.

## Consequences

- `docs/ARCHITECTURE.md`'s dependency graph, `AGENTS.md`'s dependency-direction rules, and the CI
  package-graph check gain the `capabilities -> sandbox` edge.
- The authoring, runtime, capabilities, deployment, security-operations, and testing
  specifications gain Code Mode sections with stable requirement IDs (family choice pending; IDs
  are coverage-gate-bearing only once defined in `docs/spec/*.md`).
- Mid-pass budget consumption is new engine behavior; direct model-declared calls keep their
  current Turn-boundary accounting unchanged.
- In class `E`, inner calls produce no Canonical Records: the Conversation Log carries only the
  outer Tool Call and bounded final result, with inner-call evidence in telemetry and host-Tool
  audit metadata. This observability boundary is documented, not incidental.
- The deterministic executor substitute lives in `@effect-agent/testing` and self-identifies as
  `unisolated`; it can never masquerade as a security boundary.

## Rejected alternatives

- **Adopt `@cloudflare/codemode` as the framework abstraction.** Its Promise-first executor,
  connector definitions, and separate persistence log duplicate Effect AI Tool semantics and the
  canonical durability contract. It remains prior art only.
- **Pass database or platform bindings directly into generated code.** Grants authority outside
  Tool authorization, Schema validation, audit, tenancy, and durability policy.
- **Call Toolkit handlers directly from the Code Mode handler.** Bypasses engine scheduling,
  approval, budgets, redaction, and stable events.
- **Secure generated code through source inspection.** Parsing improves ergonomics; isolation,
  least-authority bindings, and enforced limits are the security boundary.
- **Treat raw `SELECT` detection as read-only SQL enforcement.** Writable CTEs, dangerous
  functions, and multi-statements defeat it; database authority carries the guarantee.
- **Mark the outer Tool `idempotent` to obtain durability.** The program may crash between an
  inner external effect and its record; durability must track each inner action.
- **Wrap arbitrary inner calls in `DurableStep.do`.** A Step is exactly-once-recorded but
  at-least-once-executed; it cannot upgrade an unknown non-idempotent operation.

## Validation

- Executor conformance: bounded computation, oversized source, CPU/wall-clock exhaustion, output
  and result limits, unknown host method, host-call exhaustion, malformed results, interruption
  finalizers, network denied by default, honest posture reporting.
- Broker semantics: direct and programmatic invocation of the same Tool are observably
  equivalent; typed failures stay typed; approval or policy denial prevents handler start; budget
  exhaustion prevents the next call; sequential identities are deterministic; non-allowlisted and
  collision cases fail closed; declarations present encoded wire types.
- SQL security capability: mutation/DDL, multi-statement, cross-tenant, and dangerous-function
  attempts fail closed at database authority; timeouts cancel; truncation reports honestly;
  credentials and raw results never reach executor bindings or ordinary telemetry.
- Static evidence: native Tool/Toolkit values flow unwrapped; handler failures remain in `E` and
  requirements in `R`; Cloudflare types stay in outward packages; the dependency graph remains
  inward-only.

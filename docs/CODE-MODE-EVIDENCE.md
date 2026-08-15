# Code Mode evidence

Status: growing per slice (D-035, [ADR-0017](adr/0017-code-mode-executor-and-broker.md),
[plan](CODE-MODE-PLAN.md) §13). Each completed slice records what was built, the evidence that
discharges its exit gates, and what remains explicitly unclaimed.

## C1 — Executor port and deterministic substitute

Delivered:

- `CodeExecutor` and the per-pass `CodeExecutionHost` service in `@effect-agent/sandbox`
  (`packages/sandbox/src/code-executor.ts`): Schema-first request, namespaces, limits, host-call
  envelopes, result, resource accounting, and the nine-member tagged `CodeExecutionError` union.
  Every byte, count, and duration bound is a checked Schema; namespace and method names are
  validated `JsIdentifier`s that reject ECMAScript reserved words closed.
- The deterministic in-process substitute in `@effect-agent/testing`
  (`packages/testing/src/code-executor-substitute.ts`), self-identified as
  `unisolated`/`in-process-javascript` through the shared `SandboxImplementation` posture idiom
  (CAP-010). It rejects the limits it cannot enforce (CPU, network allowlists) with typed
  unsupported errors and shadows obvious ambient globals as a usability check only.
- The shared conformance kit (`packages/testing/src/code-executor-conformance.ts`, TEST-015):
  seventeen executor-contract cases run verbatim by the substitute suite and reusable by every
  isolated adapter. Enforcement cases that only genuine isolation can prove (ambient network
  denial, synchronous CPU runaway termination) are excluded by design and owned by isolated
  adapters (testing spec §8.1).

Exit-gate evidence:

- every expected failure stays typed — `packages/sandbox/test/code-executor.test.ts` decodes the
  full error union; the conformance cases assert one specific tag per failure mode;
- source, result, host-call, output, and duration limits are observable through public behavior —
  the `TEST-015` conformance cases in `packages/testing/src/code-executor-conformance.ts`;
- interruption runs pass teardown — the interruption conformance case asserts an in-flight host
  call is interrupted when the pass fiber is interrupted;
- no test substitute claims isolation it does not provide — the posture case pins
  `unisolated`, and the substitute suite additionally proves global shadowing is labeled a
  usability check.

Unclaimed: any isolation guarantee (the substitute shares the host JavaScript engine), CPU
enforcement, network enforcement, and everything in slices C2–C4.

## C2 — Ephemeral native Tool broker and Code Mode Tool

Delivered:

- the engine-owned `ToolBroker` seam (`packages/engine/src/tool-broker.ts` plus the live factory
  in `packages/engine/src/index.ts`), provided in the `AgentSpawner`/`DurableStep` pattern: a
  fail-closed Run default shadowed per outer Tool Call. Passes are strictly sequential with
  broker-owned zero-based indices allocated exactly when a handler starts (RUN-016); a concurrent
  call fails typed without consuming an identity; every started call consumes the Run's Tool-call
  budgets before its handler is invoked and the Turn-seam `maxToolCalls` checks include the
  Run-wide programmatic count (RUN-017). Outcomes are total data — success, the Tool's declared
  Schema-encoded typed failure, or the bounded `{ errorTag, message }` projection the direct
  path's `ToolCallFailed` events use — so defects stay defects. Broker-owned result byte bounds
  and an optional redaction hook govern the sandbox boundary. Inner calls emit no Run events and
  no Canonical Records in class `E`.
- `CodeMode.make` in `@effect-agent/capabilities` (`packages/capabilities/src/code-mode.ts`,
  CAP-014): the Delegation-pattern builder over an explicit namespace record, returning an
  ordinary Tool (annotated `readonly`, `failureMode: "return"` so a model can correct a failing
  program) and a handler Layer whose `R` surfaces `CodeExecutor` and the selected handlers.
  Construction fails closed on non-`readonly` Tools, approval-requiring Tools, invalid
  identifiers, name collisions, and any Schema the declaration renderer cannot express; the
  TypeScript declarations derive from `Tool.getJsonSchema` — the encoded wire types.
- the aggregate model-visible egress policy (CAP-016): the final result, captured logs, and any
  thrown value share one byte budget with an optional redaction hook; an oversized result is a
  typed `CodeModeEgressExceeded` failure and log truncation always carries an explicit marker.
- the first manifest use of the ADR-0017 `capabilities -> sandbox` edge.

Exit-gate evidence:

- direct and programmatic invocation of the same Tool are observably equivalent —
  `packages/engine/test/tool-broker.test.ts` (RUN-016 titles, 10 cases: equivalence, identity
  determinism, concurrency rejection, unknown Tool, invalid parameters, approval preflight,
  typed handler failure, non-JSON success encoding, result bounds and redaction, mid-pass and
  Turn-seam budget accounting);
- programmatic calls cannot reach a non-allowlisted Tool, the model sees only the Code Mode Tool
  unless originals are exposed, and generated declarations present encoded wire types failing
  construction closed — `packages/capabilities/test/code-mode.test.ts` (12 cases including
  compile-time E/R proofs);
- a real generated JavaScript program composes allowlisted Tools through the substitute executor,
  the broker, and real handlers inside one Run, and observes mid-pass budget exhaustion as a
  catchable envelope — `packages/testing/test/code-mode-e2e.test.ts`.

Unclaimed: everything in C3–C4, isolation, and Code Mode durability in both the DN and DC
assemblies.

## C3 — Read-only SQL reference integration

Delivered:

- the reference warehouse fixture (`packages/testing/src/fixtures/warehouse/index.ts`, SEC-015):
  a native `Tool.make` Tool (`query_warehouse`, annotated `readonly`) over an application-owned
  query service. The guarantee is database authority and topology, not SQL text inspection: the
  Layer materializes a curated, tenant-scoped copy into a private in-memory SQLite database
  (cross-tenant rows are physically absent) and locks the connection with
  `PRAGMA query_only = ON`; write denial is detected structurally by SQLite result code 8. The
  keyword scanner (PRAGMA/ATTACH/DETACH/VACUUM/load_extension, single-statement) is defense in
  depth for the escape hatches the authority cannot police — deliberately NOT for writes and
  DDL, so the tests prove the database denies those itself;
- host-owned tenant context fixed at Layer construction, never from model-controlled arguments;
- bounded results with honest `truncated: true` (D-035 default) under row and byte caps;
- structural audit metadata only (FNV-1a query digest, parameter count, row count, truncation)
  — never SQL text, parameter values, or rows;
- fail-closed posture for unenforceable policy: the synchronous driver cannot cancel a running
  statement, so a `statementTimeout` request fails Layer construction typed instead of being
  silently unenforced.

Exit-gate evidence (`packages/testing/test/warehouse-sql.test.ts`, SEC-015 titles):

- the executor receives no database client, credentials, address, or network authority — the
  composition scenario in `packages/testing/test/code-mode-e2e.test.ts` runs a real generated
  program against the real SQLite fixture through namespace methods only, filters a broad
  result into a bounded answer, and proves the write denial end to end by catching the typed
  envelope inside the program;
- mutation and DDL fail closed at the database authority; multi-statement, PRAGMA, ATTACH, and
  `load_extension` attempts fail closed at the scanner; cross-tenant reads return nothing
  because the data is physically absent;
- over-limit output truncates honestly; the same final result is deterministic under the fixture
  seed.

Unclaimed: everything in C4, isolation, statement-level cancellation on the synchronous driver,
and Code Mode durability in both the DN and DC assemblies.

## C4 — Cloudflare Dynamic Worker adapter

Delivered:

- the Worker Loader-backed `CodeExecutor` Layer in `@effect-agent/platform-cloudflare`
  (`packages/platform-cloudflare/src/code-mode-executor.ts`, DEPLOY-011). Each pass loads one
  fresh Worker through the `worker_loaders` binding with `globalOutbound: null`, so generated
  code has no ambient network, filesystem, environment, secrets, or platform bindings; its only
  authority is the pass-scoped `CodeModeHostEntrypoint` RPC stub, which routes into the pass's
  `CodeExecutionHost` service through a module-global registry keyed by an unforgeable pass id.
  The generated source becomes a real module (`program.js`), never `eval`; a fixed harness
  installs the namespace globals and a bounded console, invokes the one async function once, and
  returns one envelope validated through Effect Schema. CPU and subrequest limits are passed to
  the loaded Worker; the executor-owned wall-clock deadline plus workerd's own hang detection
  bound a non-completing pass; the Worker handle and host-serving fiber finalize in Scope
  finalizers. Expected worker-level failures map into the typed union with bounded diagnostics —
  a module-compile error in the generated program is a `CodeSourceError`, not an infrastructure
  failure — and unexpected terminations stay `CodeExecutorTerminatedError` without fabricating a
  result.
- the first manifest use of the ADR-0017 `platform-cloudflare -> sandbox` edge; all
  Cloudflare-specific types (`WorkerLoader`, `WorkerEntrypoint`) stay inside this outward
  package.

Exit-gate evidence (`packages/platform-cloudflare/test/code-mode/code-mode-executor.test.ts`,
DEPLOY-011, running the real adapter inside a bundled worker under programmatic Miniflare):

- generated code cannot access ambient network, host bindings, or secrets — the network-denial
  case proves `fetch` fails under `globalOutbound: null`, and an unenforceable network allowlist
  is rejected typed;
- a non-completing pass is terminated typed (either the executor wall-clock deadline or workerd
  hang detection), never a fabricated result;
- Worker and RPC resources finalize on all exits (Scope finalizers);
- the real adapter passes a representative subset of the shared executor conformance suite —
  bounded JSON computation, honest isolated posture, host-call routing and ordering over the real
  Worker Loader RPC, caught and uncaught host-call failures, invalid and non-function source,
  result and host-call limits, program throws with log capture, and non-JSON results — plus an
  end-to-end host composition; the full 17-case kit runs against the deterministic substitute in
  the testing package;
- the feature still claims deployment class `E` only.

Unclaimed: a live synchronous-CPU-runaway kill is asserted only through the config-level CPU
limit and the wall-clock/hang backstop — a genuine `while(true)` in the Miniflare harness spins a
loaded isolate whose disposal is environment-dependent, so it is deliberately not exercised (the
same honesty posture as the repository's workerd/Miniflare-only Cloudflare evidence). Hosted
Cloudflare execution, cost/performance claims, and Code Mode durability in the DN and DC
assemblies remain unclaimed.

The Code Mode ephemeral slices C0–C4 are complete: the `CodeExecutor` port and deterministic
substitute (C1), the engine broker and `CodeMode.make` builder (C2), the read-only SQL reference
integration (C3), and the Cloudflare Dynamic Worker adapter (C4), all at deployment class `E`.

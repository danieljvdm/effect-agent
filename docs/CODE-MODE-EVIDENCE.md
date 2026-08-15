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

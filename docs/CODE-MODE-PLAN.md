# Code Mode Implementation Plan

Status: **Proposed**  
Date: 2026-08-14  
Owner approval: **Required before implementation changes normative Tool or durability semantics**

Governance registration: this proposal is registered as **D-035 (Proposed)** in
`docs/DECISIONS.md` and its normative architecture content lives in
[ADR-0017](adr/0017-code-mode-executor-and-broker.md). This file remains working material and
does not itself amend accepted architecture or roadmap status. The section 17 design decisions
were resolved by the owner on 2026-08-14.

## 1. Objective

Add a platform-neutral Code Mode capability to Effect Agent. A model should receive one native
Effect AI Tool whose input is bounded JavaScript source. The generated program may perform local
computation and invoke an explicit allowlist of existing Effect AI Tools through typed sandbox
globals, process intermediate results inside an isolated executor, and return a bounded final value
to the model.

The Code Mode capability must not depend on Cloudflare. Cloudflare Dynamic Workers are the first
target isolated executor because the Worker Loader can create a fresh Worker for generated code,
deny ambient network access, and expose narrow host capabilities through Workers RPC. Other
executors may be supplied later without changing the Code Mode Tool or its durable semantics.

The first useful integration slice is a read-only internal SQL query Tool. Database connections,
credentials, tenant context, and network access remain in the host. Generated code sees only a
typed RPC method and receives a Schema-encoded, bounded result.

## 2. Product definition

Code Mode is distinct from a general code interpreter:

- a **code interpreter** performs isolated computation and optional artifact manipulation;
- **Code Mode** additionally lets generated code compose selected application Tools, including
  loops, dependent calls, filtering, branching, and result shaping.

The existing Agent model writes the program as one Tool argument. The Code Mode handler does not
invoke a second model to generate code.

The model-facing surface is one native Effect AI Tool conceptually shaped as:

```ts
type CodeModeInput = {
  readonly code: string;
};

type CodeModeSuccess = {
  readonly result: Json;
  readonly logs?: ReadonlyArray<string>;
};
```

`Json` denotes Effect `Schema.Json` values; when Code Mode values later become canonical they use
the bounded `PersistedJson` family instead. The typed failure envelope carries the same bounded
`logs` capture as success, so a model can correct a failing program without a blind retry. The
final result, success logs, failure logs, and any thrown value form one model-visible egress
surface governed by a single aggregate byte budget and redaction policy (section 5.1); no channel
carries data the aggregate policy has not passed.

The exact public constructor remains a design task, but it must return or contribute an ordinary
Effect AI `Tool` / `Toolkit`; Effect Agent must not introduce a competing Tool abstraction.

## 3. Scope and non-goals

### 3.1 Initial scope

- JavaScript execution only. TypeScript declarations document sandbox globals for the model but
  are not compiled as TypeScript inside the executor.
- One explicit Tool allowlist per Code Mode Tool.
- Small catalogs described eagerly in the Code Mode Tool description.
- Sequential in-program Tool calls.
- JSON-encoded inputs, results, and final output at every sandbox boundary.
- Denied ambient network access and no ambient secrets, environment, filesystem, or host SDK.
- Deployment classes are `E`, `P`, `DN`, and `DC`; the first slice claims and tests class `E`
  only and makes no `P` or durable (`DN`/`DC`) claim.
- Read-only Tools only, beginning with a read-only SQL reference integration.
- One Cloudflare Dynamic Worker executor adapter plus a deterministic test executor.

### 3.2 Deferred scope

- Mutation Tools and approval suspension inside a generated program.
- Durable `DN` or `DC` Code Mode execution.
- Concurrent in-program Tool calls.
- Arbitrary package imports or runtime dependency installation.
- Persistent workspaces, files, or artifact stores.
- Snippet promotion and reuse.
- Progressive Tool search and description for large catalogs.
- Automatic rollback. Compensation is not transaction isolation and remains application-defined.
- Provider-native programmatic Tool calling as a substitute for the framework capability.
- A general framework-owned SQL connector package. The first SQL slice is a reference application
  Tool proving that internal Effect services can be exposed safely.

## 4. Architecture

```text
Model
  |
  | native Effect AI Tool Call: code_mode({ code })
  v
Code Mode capability (@effect-agent/capabilities)
  |                                    |
  | execute generated JavaScript       | invoke selected native Effect AI Tools
  v                                    v
CodeExecutor port                  engine-owned Tool broker
(@effect-agent/sandbox)                |
  |                                    | Schema / authorization / approval / budget /
  |                                    | scheduling / handler / encoding / redaction
  v                                    v
Dynamic Worker adapter             application Tool handler Layers
(outward Cloudflare package)       and internal services such as SQL
```

The three responsibilities remain separate:

1. **Code Mode capability:** model-facing Tool construction, type declarations, selected Tool
   catalog, code execution orchestration, and final result shaping.
2. **Executor:** one isolated pass over generated code with live host-call bindings. It is
   stateless and owns no approval, Tool, replay, or Conversation semantics.
3. **Tool broker:** invokes original Effect AI Toolkit handlers through engine policy. It is the
   only path by which generated code reaches application Tools.

This follows the useful part of Cloudflare's executor / connector / durable-runtime separation
without importing Cloudflare's Tool or persistence model into the framework.

## 5. Package and dependency ownership

### 5.1 `@effect-agent/capabilities`

Initially owns the Code Mode capability because the package already consolidates generic optional
agent modules until an independent dependency or release boundary is proven.

Responsibilities:

- construct the native model-facing Code Mode Tool;
- accept an explicit native Effect AI Toolkit or selected Tool record;
- derive model-facing declarations from the encoded side of the Tools' Effect Schemas;
- map sandbox method names back to exact Tool names without an ambient registry;
- invoke `CodeExecutor` and the engine-owned broker;
- annotate the outer Tool `readonly` for the first slice (an unannotated Tool reads as
  `uncertain` under the fail-closed execution-class default);
- enforce one aggregate model-visible egress policy: the final result, success and failure logs,
  and thrown values share a single bounded byte budget and redaction pass;
- expose no Cloudflare types.

If the capability later gains an independently useful release boundary, it may move to
`@effect-agent/code-mode` without changing its domain contract.

Consuming the executor port adds a new documented dependency edge, `capabilities -> sandbox`.
The edge is inward-only and therefore legal, but it is not yet declared: C0 updates the
dependency graph in `docs/ARCHITECTURE.md`, the dependency-direction rules in `AGENTS.md`, and
the CI package-graph check together with this plan's approval.

### 5.2 `@effect-agent/sandbox`

Owns a new callback-capable execution port adjacent to, not replacing, the existing command-based
`Sandbox` service. The existing `SandboxRequest` cannot represent live host method calls and must
not be overloaded with an unrelated execution model.

The target shape is conceptually:

```ts
interface CodeExecutor {
  readonly execute: (
    request: CodeExecutionRequest,
  ) => Effect.Effect<CodeExecutionResult, CodeExecutionError, Scope.Scope | CodeExecutionHost>;
}
```

`CodeExecutionHost` is an Effect service in `R`, not an ordinary argument: the capability provides
its per-pass Layer at the pass edge, so the executor's authority to reach host Tools stays visible
in the requirement channel and intermediate code can neither supply nor capture the
implementation — the same per-call-provided-service pattern as `DurableStep`.

Requirements:

- all public asynchronous operations return `Effect`;
- request, result, event, limit, and expected-error values use Effect Schema;
- live callback functions are scoped resources and are never persisted;
- implementations identify their isolation posture honestly, reusing the existing
  `SandboxImplementation` idiom (`isolated` versus `unisolated`, CAP-010) rather than a new
  posture vocabulary;
- an adapter rejects limits or policies it cannot enforce;
- interruption closes the workload and every transport/resource owned by the pass;
- captured output is bounded and sensitive by default.

The request declares at least:

- bounded JavaScript source;
- executor/runtime identity;
- named callable namespaces;
- network policy;
- CPU, wall-clock, output, and final-result limits;
- host-call limits: a maximum call count plus per-call argument and per-call result byte bounds;
- optional allowlisted modules in a later phase.

### 5.3 `@effect-agent/engine`

Owns a dependency-neutral service or hook for programmatic invocation of native Effect AI Tools,
provided locally per Run in the same pattern as the existing engine-owned `AgentSpawner` and
`DurableStep` services. The Code Mode capability consumes this seam; the engine must not depend
on capabilities.

The implementation first extracts the existing per-call Tool execution path into a reusable
internal operation. These stages exist today and must be shared, not duplicated:

1. Tool lookup;
2. parameter handling (Effect AI decoding, wire-form encoding, and resume-path revalidation);
3. approval preflight policy;
4. bounded scheduling;
5. scoped handler execution;
6. typed failure handling;
7. success/failure encoding;
8. per-call telemetry.

Two further behaviors are new, not extracted, and are scoped as new work:

- **Per-call budget accounting.** Tool-call and duration budgets are currently evaluated at Turn
  boundaries. Code Mode consumes and checks the Tool-call budget before each inner invocation so
  one pass cannot exceed the budget between Turn-seam checks.
- **Result size bounds and redaction.** No such stage exists in the engine Tool path today. Both
  remain broker-owned at the sandbox boundary; direct model-declared calls keep their current
  behavior unchanged.

Per-Tool authorization is not an engine stage and this work does not add one: authorization
remains application- and handler-owned, per the security specification. The engine contributes
approval policy, scheduling, budgets, encoding, and telemetry only.

This refactor must not introduce a public framework-owned Tool or Toolkit copy.

### 5.4 Cloudflare adapter placement

The initial Dynamic Worker Layer may live in `@effect-agent/platform-cloudflare`. Create a separate
`@effect-agent/sandbox-cloudflare` package only when the adapter proves independently useful from
the durable host assembly.

Cloudflare-specific values such as `WorkerLoader`, `WorkerStub`, `Fetcher`, and Workers RPC stubs
remain inside this outward adapter.

## 6. Native Tool exposure

An Agent author explicitly selects the Tools visible inside one Code Mode Tool. Selected Tools are
not automatically discovered from a root Layer or mutable registry. Unless the author separately
adds them to the model-facing Toolkit, the model sees only the Code Mode Tool and not the original
individual Tools.

Each selected Tool becomes a method within a JavaScript namespace. For example, a native Tool named
`query_warehouse` may be presented as:

```ts
declare const warehouse: {
  query(input: {
    readonly sql: string;
    readonly parameters?: ReadonlyArray<string | number | boolean | null>;
  }): Promise<{
    readonly columns: ReadonlyArray<string>;
    readonly rows: ReadonlyArray<Record<string, Json>>;
    readonly truncated: boolean;
  }>;
};
```

The declarations are model documentation only. Runtime validation always uses the original Effect
Schemas before the original handler starts and again when its result crosses the sandbox boundary.
Do not maintain a second Zod, JSON Schema, or hand-authored runtime validation model.

Declaration derivation reads the encoded side of each Schema — the JSON that actually crosses the
sandbox boundary — via the same JSON-schema derivation Effect AI already applies to Tool
parameters for providers. A schema the deriver cannot render fails Tool construction closed
rather than degrading to `unknown`. Declarations also document the failure surface: a namespace
method rejects with the Schema-encoded typed failure envelope described in section 7.2.

For a generic raw SQL Tool the output type is necessarily JSON-shaped. Domain-specific named Tools
should retain their stronger success Schemas.

## 7. Programmatic Tool broker

### 7.1 Invocation identity

Every call from generated code receives a stable programmatic call identity. The initial identity
is derived from:

```text
(outer ToolCallId, zero-based sequential call index)
```

The model cannot choose or forge this identity. A future durable record may introduce a branded
`ProgrammaticToolCallId`, but its derivation and maximum length must be Schema-defined before it
enters canonical state.

### 7.2 Invocation behavior

Per-call broker input is data only:

- the selected namespace and method;
- the encoded arguments;
- the deterministic sequence index.

The live native Toolkit handlers, engine policy context, and parent Tool Call identity are
capabilities, not invocation data: they are bound once when the per-outer-call broker service is
constructed at the Run boundary (section 5.3), so a caller inside business execution can never
substitute handlers or policy.

It returns one Schema-encoded success or typed failure envelope. Inside the generated program a
failed call surfaces as a rejected Promise carrying that encoded envelope (a stable tag plus
bounded data), so programs can catch and branch on expected failures. Raw Effects, Layers,
service objects, database clients, credentials, and Causes never cross into the sandbox.

Programmatic calls execute under the parent Tool Call's already-held scheduling permit and never
acquire Tool Batch permits of their own. The engine's batch semaphore is created per batch:
re-entrant acquisition would deadlock at `toolConcurrency: 1`, and a second batch path would let
inner calls escape the declared concurrency bound. Boundedness comes from mandatory
sequentiality, the executor host-call limit, and Tool-call budgets.

Sequential calls are mandatory in the first slice: a host call issued while another call from the
same pass is unsettled fails with a typed concurrency error. `Promise.all` over host Tools
therefore fails deterministically rather than acquiring arrival-order identities that future
approval replay could not reproduce.

An inner Tool call that requires approval fails with a typed policy failure in the first slice
and never suspends; the builder additionally rejects approval-requiring Tools from the allowlist
at construction. Approval suspension inside a generated program is deferred durable work
(section 12.3).

### 7.3 Accounting

Each programmatic Tool call consumes the same Tool-call and duration budgets as a direct Tool Call.
The outer Code Mode Tool additionally consumes one ordinary model-visible Tool Call. Policy must
define both limits explicitly so Code Mode cannot multiply an Agent's apparent Tool budget without
bound.

This per-call accounting is new engine behavior: budgets are currently evaluated at Turn
boundaries, so the broker must consume and check the Tool-call budget before every inner
invocation for "budget exhaustion prevents the next call" to hold mid-pass.

The executor also enforces its own maximum host-call count independently of engine accounting.

## 8. Read-only SQL reference slice

### 8.1 Purpose

Prove that generated code can query an internal service without exposing the service, connection,
credentials, or network to the executor. The reference Tool is application code built from native
Effect AI primitives; it is not a special framework SQL abstraction.

### 8.2 Example generated program

```js
async () => {
  const result = await warehouse.query({
    sql: `
      SELECT customer_id, SUM(amount) AS revenue
      FROM invoice_summary
      WHERE created_at >= ?
      GROUP BY customer_id
      ORDER BY revenue DESC
      LIMIT 100
    `,
    parameters: ["2026-07-01"],
  });

  return result.rows.filter((row) => Number(row.revenue) > 10000).slice(0, 10);
};
```

Only the bounded model-visible egress — the final value plus captured logs — returns to model
context. Intermediate rows never leave the pass implicitly: not through telemetry, canonical
records, or declarations. The program may explicitly return or log data it was authorized to read
through its Tools, but only within the aggregate egress budget; containment is a claim about
implicit leakage and context size, not a boundary against the program's own deliberate output.

### 8.3 Host Tool contract

The reference Tool:

- is created with native Effect AI `Tool.make`;
- declares Effect Schema parameters, success, and typed failures;
- is annotated with `ToolExecutionClass` value `readonly`;
- requires an application-owned read-only query service in `R`;
- receives tenant/principal context from the host rather than from model-controlled arguments;
- returns only Schema JSON values;
- reports whether row or byte bounds truncated the result.

### 8.4 SQL enforcement

Read-only safety cannot depend on checking whether source text begins with `SELECT`. The reference
integration must enforce defense in depth:

- a database identity that lacks mutation, DDL, administrative, and extension privileges;
- a read replica or curated read-only views where practical;
- database-enforced tenant isolation or host-owned tenant scoping;
- exactly one statement per call;
- bound parameters rather than string interpolation for values;
- statement timeout and cancellation;
- maximum rows, columns, cells, and encoded result bytes;
- denial of dangerous database functions and cross-database, filesystem, or network access;
- bounded query-plan cost where the database supports it;
- structural audit metadata containing a query digest and redacted parameters;
- a typed failure when the adapter cannot prove or enforce the configured policy.

Parsing SQL is useful defense in depth but is not the primary security boundary. Database
permissions and data topology carry the actual read-only guarantee.

### 8.5 Schema discovery

The first slice uses a small curated schema described in the Tool documentation. Later options are:

- expose named, strongly typed query Tools;
- include compact table/view declarations in the namespace description;
- add a separate read-only schema-description Tool;
- add progressive Code Mode `search` / `describe` once large catalogs justify it.

A program cannot ask the model to reason about a newly discovered schema without returning that
information for another model Turn, so schema discovery must not be presented as free single-pass
reasoning.

## 9. Cloudflare Dynamic Worker adapter

The adapter uses the Worker Loader as an execution primitive rather than adopting Cloudflare's
Code Mode runtime as the framework's semantic layer.

### 9.1 Execution

For each pass the adapter:

1. validates and bounds the `CodeExecutionRequest`;
2. generates a fixed executor harness plus the model-provided JavaScript;
3. creates one fresh Dynamic Worker with `WorkerLoader.load()`;
4. sets `globalOutbound: null` by default;
5. supplies only the scoped Tool-broker RPC stub and explicitly allowed structured values;
6. applies the configured Dynamic Worker CPU and subrequest limits;
7. invokes one fixed entrypoint;
8. validates the returned envelope through Effect Schema;
9. disposes the entrypoint and Worker handles in Scope finalizers.

The executor records no durable state. A later pass may run in a completely different isolate.

### 9.2 Security posture

- Source inspection and AST normalization are usability checks, not a security boundary.
- Generated code receives no raw bindings to D1, Hyperdrive, KV, R2, Durable Objects, service
  credentials, or the parent environment.
- Internal operations cross a narrow RPC stub whose methods route to the host Tool broker.
- Ambient `fetch` and `connect` remain denied. A future egress capability must be a separately
  authorized host Tool or a policy-enforcing outbound service.
- No raw secret may be returned by a host Tool or included in a Worker binding.
- Synchronous loops must be stopped by platform CPU limits, not only by a JavaScript timer.
- Expected adapter failures — startup, timeout, termination, protocol, and transport — map into
  the typed `CodeExecutionError` union with bounded diagnostic data; unexpected adapter defects
  remain defects.

### 9.3 Cost and operations

`WorkerLoader.load()` is appropriate for one-off generated source, but current Dynamic Workers
billing counts no-ID `.load()` use as a new Dynamic Worker per invocation. A production gate must
measure executions, approval replays, startup CPU, and generated-code uniqueness before making a
cost claim. Stable-ID `get()` caching may be evaluated only when exact code and harness identity can
be content-addressed without weakening isolation or serving stale code. A cached Worker closes over
its `env`, including the broker stub, so tenant and binding context must be part of Worker identity
or must arrive per invocation; cross-tenant reuse of a content-addressed Worker is an isolation
defect, not merely staleness.

Tail Worker or platform observability may enrich adapter diagnostics later, but canonical Code
Mode outcomes cannot depend on it.

## 10. Error model

Expected errors are Schema tagged and remain in `E`. The initial union should distinguish at least:

- invalid or oversized source;
- unsupported language or executor feature;
- executor startup failure;
- CPU or wall-clock timeout;
- output or result limit exceeded;
- malformed executor result;
- unknown or non-allowlisted namespace/method;
- invalid Tool parameters;
- Tool authorization or policy denial;
- typed Tool handler failure;
- invalid Tool success encoding;
- host-call limit exceeded;
- external executor termination, such as isolate eviction or a platform kill, where no terminal
  result is fabricated.

Interruption of the host fiber itself follows ordinary Effect interruption semantics and is never
represented in `E`. Where an inner failure belongs to the Tool rather than the executor, the
broker envelope carries the existing framework tags — `ToolInputError`, `ApprovalRequired`,
`PolicyDenied`, `BudgetExceeded`, `ToolInfrastructureError`, `ToolOutputError`, and the Tool's
own declared failures — rather than a parallel Code Mode union. New tags are reserved for
executor-side failures.

Defects, sandbox termination, and remote transport failures must not be collapsed into a generic
successful `{ error: string }` value. A Tool may explicitly choose a model-visible failure mode,
but the framework default preserves typed failure.

## 11. Resource and telemetry contract

One Code Mode pass owns one child Scope containing:

- executor/isolate handles;
- RPC dispatchers and stubs;
- timers and cancellation signals;
- temporary buffers;
- any pass-scoped host resources.

Finalizers run on success, typed failure, defect, timeout, denial, and interruption.

Telemetry includes:

- outer Tool Call and execution identity;
- executor implementation and isolation posture;
- source byte count and digest, not source text by default;
- host-call counts by bounded Tool name;
- duration, CPU data when available, and bounded result sizes;
- outcome category and typed error tag;
- SQL query digest and bounded database telemetry from the host Tool.

Generated source, SQL text, parameters, Tool results, console output, and executor diagnostics are
sensitive tenant content and are excluded from ordinary span attributes.

In the first slice inner calls produce no Canonical Records: the Conversation Log contains only
the outer Code Mode Tool Call and its bounded final result, and inner-call evidence exists only
in telemetry counts and host-Tool audit metadata such as the SQL query digest. This is the
designed observability boundary of class-`E` Code Mode; section 12 is what changes it.

## 12. Durability design for a later phase

The first slice makes no `DN` or `DC` Code Mode claim. Code Mode support in either the `DN` or
`DC` assembly requires an accepted ADR and new canonical semantics; it must not be obtained by
merely annotating the outer Code Mode Tool as `idempotent`.

### 12.1 Replay model

Durable execution follows abort-and-replay:

1. persist the bounded source and execution identity;
2. run one isolated pass;
3. give each inner Tool invocation a stable sequential identity;
4. record its encoded arguments before consequential execution;
5. record its validated result after completion;
6. on suspension or ownership loss, rerun the same source in a fresh executor;
7. return recorded results for already-settled inner calls;
8. halt at an inner call that was prepared but never settled: a `readonly` or declared-idempotent
   call may re-execute per policy, but an `uncertain` prepared-unsettled call must resolve through
   the ordinary reconciliation and Unknown Outcome protocol at its exact sequence position before
   execution continues past it;
9. require exact namespace, method, argument, and sequence agreement;
10. terminate with a replay-divergence failure on mismatch.

Nondeterministic sandbox-only computation such as time or randomness will need a Schema-backed
recorded step primitive before it may influence replayed control flow.

### 12.2 Canonical ownership

The Conversation Log remains the only recovery truth. Do not introduce a Cloudflare-specific Code
Mode database or a second generic durable execution store.

The durable record design must decide whether to extend existing Tool records with parent
programmatic-call linkage or add an explicit family such as:

- `ProgramExecutionStarted`;
- `ProgrammaticToolCallDeclared`;
- `ProgrammaticToolCallPrepared`;
- `ProgrammaticToolCallSettled`;
- `ProgramExecutionCompleted`;
- `ProgramExecutionDiverged`.

The choice requires an ADR because current Tool records assume model-declared calls in a completed
assistant response. Two constraints on that ADR: `Prepared`/`Settled` follow the existing
canonical naming, but a canonical `Declared` record has no precedent — declarations today live
inside the recorded model response — so the ADR either folds declaration into the prepared record
or explicitly justifies the new record kind. Recorded arguments and results use the bounded
`PersistedJson` family, which also supplies the Schema-defined size bounds required by
section 7.1.

### 12.3 Approval

An inner approval-required Tool cannot receive a provisional result. The runtime must:

1. canonically record the nested approval request;
2. abort the current executor pass;
3. suspend the parent Run without consuming a worker permit;
4. record the decision through the ordinary approval path;
5. rerun the same source;
6. replay earlier settled results;
7. execute the approved action exactly at its stable sequence position.

Rejecting an inner action ends the Code Mode execution but does not imply prior actions were
rolled back.

### 12.4 Unknown outcomes

Each inner Tool retains its declared execution class (`readonly`, `idempotent`, or `uncertain`,
with the fail-closed default `uncertain` for unannotated Tools):

- `readonly`: safe to re-run, subject to consistency policy;
- `idempotent`: may re-run under the Tool's declared external contract;
- `uncertain`: prepared without a settled result requires reconciliation or an Unknown Outcome.

Durable Tools are orthogonal to execution class: a Tool whose handler declares the `DurableStep`
dependency keeps its named Steps' exactly-once-recorded, at-least-once-executed semantics inside
a generated program.

Wrapping an arbitrary Tool call in `DurableStep.do` does not make its external mutation exactly
once and is not a substitute for this protocol.

### 12.5 Read consistency

A read-only SQL query is externally safe to repeat but may observe newer data. The durability ADR
must choose between:

- recording and replaying every bounded read result for deterministic program replay;
- explicitly allowing selected reads to re-execute and documenting possible branch drift;
- providing a database snapshot/reference capability for one execution.

No mutation-capable Code Mode execution may rely on re-executed reads unless its policy proves the
result change cannot duplicate or redirect a side effect.

## 13. Work plan

### C0 — Decisions and specification

Deliverables:

- acceptance of ADR-0017 (drafted and registered with D-035) for the executor / native Tool
  broker boundary before implementation;
- updates to authoring, runtime, capabilities, deployment, security, and testing specifications;
- updates to the `docs/ARCHITECTURE.md` dependency graph, the `AGENTS.md` dependency-direction
  rules, and the CI package-graph check for the new `capabilities -> sandbox` edge;
- stable requirement IDs and traceability rows, including the decision whether Code Mode extends
  the `CAP-` requirement family or opens a new prefix family (IDs become coverage-gate-bearing
  only once defined in `docs/spec/*.md`);
- a decision on the first public constructor and package placement;
- a decision on the generated-program calling convention (section 17);
- explicit deployment-class and durability non-claims.

Exit gates:

- no framework-owned Effect AI primitive is introduced;
- the engine remains independent of capabilities and Cloudflare;
- Tool authorization, Schema, error, budget, and Scope behavior is specified end to end;
- the read-only SQL trust boundary is documented.

### C1 — Executor port and deterministic substitute

Deliverables:

- Schema-first `CodeExecutionRequest`, results, events, limits, and tagged errors;
- scoped `CodeExecutor` Effect service;
- a deterministic executor substitute in `@effect-agent/testing`, self-identified as `unisolated`
  through the shared posture idiom, sufficient to prove the public contract;
- conformance cases shared by real isolated adapters where applicable.

Exit gates:

- every expected failure remains typed;
- source, result, host-call, output, and duration limits are observable through public behavior;
- interruption runs all finalizers;
- no test substitute claims isolation it does not provide.

### C2 — Ephemeral native Tool broker and Code Mode Tool

Deliverables:

- reusable engine-owned native Tool execution operation;
- per-outer-call broker service with deterministic sequential identities, executing under the
  parent call's scheduling permit;
- Code Mode Tool builder over an explicit Tool allowlist, rejecting non-`readonly` and
  approval-requiring Tools at construction and annotating the outer Tool `readonly`;
- eager TypeScript declarations derived from the encoded side of the Effect Schemas, documenting
  the typed failure envelope, and failing construction closed on non-renderable schemas;
- Schema-defined success and failure envelopes for in-program host calls;
- mid-pass tool-call budget consumption plus executor host-call accounting;
- bounded final result and sensitive log handling.

Exit gates:

- direct and programmatic invocation of the same Tool preserve parameter, handler, success,
  failure, requirement, and interruption behavior;
- programmatic calls cannot reach a non-allowlisted Tool;
- the model sees only the Code Mode Tool unless the author explicitly exposes originals;
- concurrent host calls fail clearly rather than silently acquiring nondeterministic identities;
- generated declarations present encoded wire types, and a schema the deriver cannot render fails
  Tool construction rather than weakening the declarations.

### C3 — Read-only SQL reference integration

Deliverables:

- a native read-only SQL Tool backed by an application service Layer;
- curated schema/view description;
- database-enforced read-only principal;
- tenant isolation, timeout, statement, row, and byte enforcement;
- a reference Agent scenario where code filters a larger query result into a small final answer;
- explicit SQL audit and telemetry behavior.

Exit gates:

- the executor receives no database client, credentials, address, or network authority;
- mutation, multiple statements, over-limit output, timeout, and cross-tenant attempts fail closed;
- intermediate rows never enter ordinary telemetry, and reach model context only when the program
  explicitly returns or logs them within the aggregate egress budget;
- the same final result is deterministic under the test fixture.

### C4 — Cloudflare Dynamic Worker adapter

Deliverables:

- Worker Loader-backed `CodeExecutor` Layer;
- deny-by-default egress;
- scoped RPC binding to the host Tool broker;
- CPU, subrequest, wall-clock, output, and result enforcement;
- workerd/Miniflare conformance coverage;
- adapter configuration and deployment documentation;
- current billing/limit caveat documented without a performance or cost claim.

Exit gates:

- generated code cannot access ambient network, host bindings, or secrets;
- a synchronous runaway program is terminated by an enforced platform limit;
- Worker and RPC resources finalize on all exits;
- the real adapter passes the stable executor and SQL capability scenarios;
- the feature still claims deployment class `E` only.

### C5 — Durable Code Mode

Prerequisites:

- accepted durability ADR;
- canonical record and reducer updates;
- engine suspension and nested action hooks;
- session recovery-classifier changes;
- unknown-outcome and operator-resolution integration.

Deliverables:

- stable program execution and inner-call identities;
- canonical source, call, result, approval, and divergence evidence;
- abort-and-replay in fresh executors;
- result replay and exact divergence checks;
- failpoints around every new durable mutation;
- `DN` and `DC` conformance using the same semantic cases;
- resource cleanup for completed, failed, rejected, unknown, interrupted, and expired executions.

Exit gates:

- no completed inner Tool is re-executed during ordinary approval replay;
- unresolved ordinary mutations are never replayed automatically;
- canonical replay never executes generated code or external Tools;
- stale owners cannot append or settle inner calls;
- every accepted Submission still reaches exactly one Settlement or an explicit unresolved
  obligation covered by the existing operator protocol;
- Node process-kill and Cloudflare eviction tests cover every new crash point.

### C6 — Large catalogs and reuse

Potential later deliverables:

- progressive Tool `search` and `describe`;
- reviewed snippet promotion with integrity digests and Tool-set compatibility;
- artifact references for replay values too large for canonical inline storage;
- carefully bounded read concurrency with deterministic identities;
- alternative isolated executor adapters.

These features do not block the read-only SQL slice.

## 14. Verification strategy

Committed tests protect stable capability behavior rather than implementation files. Prefer the
smallest suites that prove the new trust and durability boundaries.

### 14.1 Static and type evidence

- native Effect AI Tool and Toolkit values flow through the public API without wrappers;
- original Tool handler failures remain in the enclosing `E`;
- original handler and Schema requirements remain visible in `R`;
- Cloudflare types do not enter generic packages;
- the dependency graph remains inward-only.

### 14.2 Executor conformance

Stable cases:

- successful JSON computation;
- invalid and oversized source;
- source timeout / CPU exhaustion;
- excessive console output and final result;
- unknown host method;
- host-call limit exhaustion;
- malformed adapter result;
- interruption and finalizer execution;
- network denied by default;
- implementation identity reports isolated versus unisolated honestly.

### 14.3 Native Tool broker semantics

Stable cases:

- valid input and success encoding;
- invalid parameters prevent handler start;
- typed handler failure remains typed;
- invalid success encoding fails;
- approval or policy denial prevents handler start;
- budget exhaustion prevents the next call;
- sequential identity/order is deterministic;
- non-allowlisted and sanitized-name collision cases fail closed;
- direct and Code Mode invocation have equivalent observable Tool semantics.

### 14.4 SQL security capability

Stable cases:

- read query success;
- write/DDL attempt denied by database authority;
- multi-statement attempt denied;
- cross-tenant access denied;
- timeout cancellation;
- row, cell, and byte truncation/failure behavior;
- dangerous function denial;
- credentials and raw query results absent from executor bindings and ordinary telemetry.

### 14.5 Durable evidence

When C5 begins, add deterministic cases for:

- replay of settled inner results without handler execution;
- mismatch in sequence, Tool name, or encoded arguments;
- approval pause before handler start and resume after canonical decision;
- crash before prepare, during invocation, after return/before settlement, and after settlement;
- readonly, idempotent, uncertain, and Durable Tool classes;
- stale owner fencing;
- process kill, Durable Object eviction, and resource finalization;
- bounded stale execution expiry and operator-visible unknown outcomes.

Committed suites follow the repository testing rules: skipped durability tests block durable
milestones (TEST-012), and time and expiry cases use controllable Effect time, not wall-clock
sleeps.

## 15. Documentation and governance changes

Before implementation claims completion:

- mark D-035 and ADR-0017 accepted in `docs/DECISIONS.md` and the ADR registers;
- update the `docs/ARCHITECTURE.md` package graph, the `AGENTS.md` dependency direction, and the
  CI package-graph check with the `capabilities -> sandbox` edge;
- update `docs/spec/authoring.md` with the native Effect AI authoring surface;
- update `docs/spec/runtime.md` with programmatic invocation ordering and accounting;
- update `docs/spec/capabilities.md` with Code Mode and executor contracts;
- update `docs/spec/durability.md` only when C5 begins;
- update `docs/spec/deployment.md` with Dynamic Worker adapter configuration and claims;
- update `docs/spec/security-operations.md` with the generated-code and host-RPC trust boundary;
- update `docs/spec/testing.md` with executor and durable evidence requirements;
- assign stable requirements and update `docs/REQUIREMENTS.md` traceability;
- add evidence documentation for every completed slice;
- update the roadmap only after owner approval assigns the work.

This plan remains Proposed and does not itself amend accepted architecture or roadmap status.

## 16. Rejected shortcuts

### Adopt `@cloudflare/codemode` as the framework abstraction

Rejected for the generic layer. It owns Promise-first executor APIs, connector definitions, and a
separate durable execution log. Those would duplicate Effect AI Tool, Effect error/requirement,
and Effect Agent canonical durability semantics. Its source remains useful prior art, and an
internal prototype may wrap its executor only if no Cloudflare type leaks into public contracts.

### Pass database or platform bindings directly into generated code

Rejected because it grants authority outside native Tool authorization, Schema, audit, tenancy,
and durability policy. Generated code receives only brokered methods.

### Call Toolkit handlers directly from the Code Mode handler

Rejected because it bypasses engine Tool scheduling, approval, budgets, durable preparation,
redaction, and stable events.

### Secure generated code through source inspection

Rejected. Parsing and normalization improve ergonomics and reject unsupported syntax, but genuine
runtime isolation, least-authority bindings, and enforced limits are the security boundary.

### Treat raw `SELECT` detection as read-only SQL enforcement

Rejected because writable CTEs, dangerous functions, multi-statements, extensions, and database
configuration can violate that assumption. Database authority must be structurally read-only.

### Mark the outer Code Mode Tool idempotent to obtain durability

Rejected because the generated program may contain multiple Tool classes and may crash between an
inner external effect and its recorded result. Durability must track each inner action.

### Use `DurableStep` around arbitrary inner Tool calls

Rejected because a Step records success exactly once but may execute its body at least once. It
cannot safely upgrade an unknown non-idempotent external operation.

## 17. Open decisions

All eleven decisions below were resolved by the owner on 2026-08-14, accepting each recommended
default; the recommended entry in each item is the decided behavior. The resolutions are restated
here and are registered in `docs/DECISIONS.md` (D-035) and ADR-0017.

1. Is the initial Code Mode capability part of `@effect-agent/capabilities`, as recommended, or
   immediately split into an independently released package?
2. Does the executor port live as a sibling service in `@effect-agent/sandbox`, as recommended?
3. What is the exact public constructor for deriving one native Code Mode Tool from selected native
   Tools and handler Layers? Recommended shape: the Delegation/`Subagent.define` pattern — an
   explicit record of Tools plus namespace mapping at construction, returning an ordinary Tool and
   a handler Layer, with no registry.
4. Does the first SQL integration permit raw SQL over curated views, named query Tools only, or
   both? Recommended: raw SQL over curated read-only views; named query Tools are ordinary Tools
   that need no Code Mode support.
5. Should excessive SQL rows produce a successful `truncated: true` result, a typed limit failure,
   or a per-Tool policy choice? Recommended: `truncated: true` success by default so a program can
   adapt in-pass, with a per-Tool typed-failure override.
6. What exact budget is charged for one outer Code Mode call plus its inner calls? Recommended:
   the outer call costs one model-visible Tool Call; every inner call consumes the ordinary
   Tool-call and duration budgets mid-pass; the executor host-call cap applies on top.
7. Which canonical record shape represents durable inner calls? Recommended: a new explicit record
   family rather than widening existing Tool records (section 12.2).
8. Are read results always logged for deterministic replay, or may explicitly annotated reads
   re-execute? Recommended: record by default; a per-Tool annotation opts a read into
   re-execution.
9. What stable identity/digest binds generated source, selected Tool schemas, and executor harness
   version across durable resume? Recommended: one content-addressed execution digest over source
   bytes, harness version, per-Tool schema digests, and executor identity, recorded at program
   start; any mismatch on resume is divergence.
10. Does the first Cloudflare adapter live in `platform-cloudflare`, as recommended, or justify
    `sandbox-cloudflare` immediately?
11. What is the generated-program calling convention — an expression evaluating to one async
    function, a module with a default export, or top-level statements with a return — including
    the console capture contract? Recommended: one async function expression invoked by the fixed
    harness entrypoint, matching the section 8.2 example.

## 18. Research references

- [Cloudflare Code Mode overview](https://developers.cloudflare.com/agents/tools/codemode/)
- [Cloudflare Code Mode internals and replay](https://developers.cloudflare.com/agents/tools/codemode/how-it-works/)
- [Cloudflare Code Mode API reference](https://developers.cloudflare.com/agents/tools/codemode/api-reference/)
- [Cloudflare Dynamic Workers](https://developers.cloudflare.com/dynamic-workers/)
- [Dynamic Worker Loader API](https://developers.cloudflare.com/dynamic-workers/api-reference/)
- [Dynamic Worker capability bindings](https://developers.cloudflare.com/dynamic-workers/usage/bindings/)
- [Dynamic Worker custom limits](https://developers.cloudflare.com/dynamic-workers/usage/limits/)
- [Dynamic Workers pricing](https://developers.cloudflare.com/dynamic-workers/pricing/)
- [Anthropic Programmatic Tool Calling](https://platform.claude.com/docs/en/agents-and-tools/tool-use/programmatic-tool-calling)
- [Effect Agent architecture](ARCHITECTURE.md)
- [Authoring specification](spec/authoring.md)
- [Capability specification](spec/capabilities.md)
- [Durability specification](spec/durability.md)
- [Security and operations specification](spec/security-operations.md)
- [ADR-0002: Use Effect AI primitives directly](adr/0002-use-effect-ai-primitives.md)
- [ADR-0004: Represent uncertain external effects](adr/0004-uncertain-external-effects.md)
- [ADR-0012: Durable Tool uncertainty protocol](adr/0012-durable-tool-uncertainty-and-steps.md)
- Vendored prior art (non-normative): the `repos/flue` Cloudflare shell sandbox example and its
  workerd `workerLoader` configuration

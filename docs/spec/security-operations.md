# Security and Operations Specification

Status: Draft

Agents combine untrusted instructions, powerful credentials, external content, and
side-effecting tools. Security is therefore part of the runtime contract rather
than a collection of example-app concerns.

This document is normative. The descriptive STRIDE walk of the trust boundaries below —
one table per boundary, with the controls and the open findings — lives in the repository at
`docs/THREAT-MODEL.md`; the triaged findings register is `docs/security/FINDINGS.md`.

## 1. Trust boundaries

Treat all of these as distinct principals or trust zones:

- end user or API caller;
- agent definition author;
- model provider;
- model-generated content;
- application tool implementation;
- MCP server;
- skill package;
- sandboxed workload;
- generated Code Mode program;
- runtime operator;
- persistence administrator;
- telemetry backend.

Text produced by a model is untrusted input. Tool output, retrieved documents,
skills, and MCP content may contain prompt injection and are also untrusted.

## 2. Authentication and tenancy

Hosts authenticate external callers before admission. The framework receives a
typed `Principal` and `TenantContext`; it does not parse bearer tokens in the core
engine.

Every durable record that can contain tenant data is tenant-addressable. Store
adapters enforce tenant scoping in queries, keys, or database policy. Cross-tenant
administrative reads require an explicit operator capability and audit event.

Client idempotency keys are scoped so one principal cannot discover or collide with
another principal's submission.

## 3. Authorization

Authorization is evaluated for:

- running an agent definition;
- selecting a model/provider;
- reading or mutating a conversation;
- loading a skill;
- calling a tool;
- accessing a secret handle;
- connecting to an MCP server;
- sandbox mounts and network destinations;
- spawning a child agent;
- viewing raw events or provider payloads;
- aborting, retrying, or resolving work.

Policies consume normalized attributes and return allow, deny, or approval-required
with a reason. Denial is typed and canonical when it affects accepted work.

The engine rechecks authorization at action time. Authorization performed when
instructions were first received is insufficient for a later tool call.

## 4. Least authority

An Agent Runtime is assembled with only the Layers required for that agent. Tools
receive narrowed services, not the root runtime or arbitrary Layer registry.

Subagents receive explicit allowlists. Skills may request capabilities but cannot
grant them. Model/provider selection cannot expand tool authority.

## 5. Tool safety

Tool definitions declare:

- read-only, write, destructive, privileged, or unknown risk;
- resource selector used for policy evaluation;
- whether approval is required;
- concurrency class;
- retry and idempotency class;
- timeout and output limits;
- whether interruption can leave uncertainty;
- redaction rules.

The host may impose stricter policy than a tool declares. A tool cannot downgrade
its risk at call time.

Arguments are passed structurally. Command execution never builds a shell string
from model output unless a specifically authorized tool makes that behavior
explicit.

## 6. Prompt injection

The framework cannot solve prompt injection solely with prompting. It reduces harm
through capability control:

- external content is labeled by source and trust;
- instructions and data remain distinct structured context where providers permit;
- tools enforce authorization independently of model intent;
- high-risk actions require approval or deterministic policy;
- secrets are not included in model context unless explicitly released;
- retrieved content cannot dynamically register tools or Layers;
- model-selected URLs, paths, and resource IDs are normalized and policy-checked;
- suspicious content and denied action attempts are observable.

Applications remain responsible for domain-specific authorization.

## 7. Secrets

Secrets are represented by opaque handles and resolved as late as possible inside
the narrow service that needs them.

- never include secrets in agent instructions;
- never persist raw secrets in canonical events;
- redact secrets from errors, logs, spans, and tool results;
- constrain which provider/tool may resolve a handle;
- support rotation without rewriting agent definitions;
- include secret-access audit metadata without the value;
- treat model attempts to solicit secrets as untrusted.

## 8. Data classification and redaction

Every event field is classified as one of:

- public metadata;
- tenant content;
- sensitive;
- secret;
- prohibited from persistence.

Redaction occurs before telemetry export and before optional debug capture.
Redaction is structural, based on Schema annotations and event types, not only
regular expressions.

Reasoning content, signatures, and redaction markers exposed through Effect AI are persisted as
canonical model content. This data is sensitive and requires the same tenant isolation,
encryption, access control, and redaction as other model content.

The framework does not request or persist hidden chain-of-thought that the provider
does not expose.

## 9. Sandbox and network policy

Untrusted code or commands require an actual isolation adapter. Policy defaults:

- no host filesystem;
- empty or read-only working tree unless explicitly mounted;
- no ambient environment;
- no cloud metadata endpoints;
- deny network by default;
- allowlisted DNS names and ports where needed;
- CPU, memory, process, file, and output limits;
- immutable runtime image identity;
- artifact scanning before release to the host.

Local unisolated runners are development-only and clearly labeled.

Code Mode generated programs are model output and therefore untrusted input executing in an
isolated executor. The executor grants no ambient network, filesystem, environment,
secrets, platform bindings, or host SDK; the only host authority is the narrow, Schema-validated
Tool-broker RPC surface over the construction-time allowlist. Source inspection and AST
normalization are usability checks, never the security boundary — genuine runtime isolation,
least-authority bindings, and enforced limits are. No raw secret may be returned by a host Tool
or included in an executor binding, and the deterministic test substitute identifies itself as
`unisolated` rather than masquerading as a boundary.

The read-only SQL reference Tool's guarantee is database authority, not SQL text inspection: a
database identity without mutation, DDL, administrative, or extension privileges; denial of
side-effecting functions reachable from a `SELECT` — installed extensions and user-defined
functions included — through an explicit execution allowlist or revocation evaluated under the
effective database identity (a restricted search path where the database has one); denial of
cross-database, filesystem, and network access; host-owned tenant scoping; exactly one statement
per call with bound parameters rather than interpolation; statement timeout and cancellation;
and maximum row, column, cell, and encoded-byte bounds. A raw `SELECT` prefix check is never
read-only enforcement. The adapter fails typed when it cannot prove or enforce the configured
policy.

### 9.1 Pull-request remediation authority

Automated remediation must keep review, implementation, and publication authority separate. The
packaged reviewer remains read-only and may produce only a bounded, Schema-validated handoff. That
handoff is authenticated and binds repository, PR number, exact reviewed head, review/profile
fingerprints, and stable identities for the selected blocking/important findings. Finding text and
suggestions are untrusted evidence, never executable patches. The handoff builder must require the
review source snapshot and host publication plan to identify the same exact head; all metadata,
changed-file, anchor, and file-content evidence in that review must derive from the one immutable
source snapshot.

A separate implementation Agent may receive only explicit capabilities for a scoped worktree:
bounded reads/searches, path-jailed edits, patch inspection, and requests for named host-configured
checks. It receives no GitHub token, provider secret, unrestricted process/shell, or publishing
Tool. Deterministic host Effect code independently enforces one attempt per reviewed head, validates
finding accounting, changed paths, patch digest, and model-reported check results against the
check-capability results the host observed. Required checks are then rerun independently, and
publication occurs only by an atomic compare-and-swap against the still-current reviewed head. The
implementer cannot grade or publish its own settlement, and publication must be followed by a fresh
reviewer whose source snapshot and publication plan both identify the published head.

The current `examples/pr-remediation` adapter is explicitly trusted-local and unisolated. It is not
an enabled workflow and must not execute untrusted PR code in a process holding credentials or
provider secrets. A production adapter needs genuine runtime isolation for repository checks,
authenticated trigger/provenance checks, secret-free check environments, and equivalent head-bound
publication fencing.

## 10. Supply chain

- exact dependency versions for prerelease Effect packages;
- lockfiles committed;
- package provenance and integrity checks in CI;
- dependency and container vulnerability scans;
- generated SBOM for releases;
- signed release artifacts where distribution supports it;
- skill and plugin integrity digests;
- no remote executable skill activation without an accepted trust design;
- Effect AI/provider package updates pass the framework semantic suite before use.

## 11. Audit log

Security-relevant actions produce append-only audit records:

- authentication and authorization decisions;
- approval requests and decisions;
- tool and sandbox execution metadata;
- secret-handle access;
- skill/MCP activation;
- subagent delegation;
- administrative retry, abort, repair, and unknown-outcome resolution;
- retention/deletion actions;
- configuration and policy version changes.

Audit records link to, but do not duplicate, sensitive payloads. Operator actions
include principal, reason, and before/after state digests.

## 12. Telemetry

The OpenTelemetry surface includes:

- trace per admitted submission and attempt;
- spans for model calls, tool calls, approvals, compaction, storage, sandbox executions, and
  child runs;
- metrics for latency, tokens, estimated cost, failures, retries, queue age, Attempt ownership,
  uncertainty, and settlement age;
- structured logs for state transitions and operator actions.

High-cardinality IDs belong in traces/logs, not unrestricted metric labels.

Minimum operational alerts:

- accepted submission without timely settlement;
- repeated ownership loss or fencing rejection;
- unknown tool outcome;
- durable-store integrity or unsupported stored-version failure;
- provider failure-rate/rate-limit surge;
- approval backlog;
- projection lag beyond objective;
- sandbox policy violation;
- redaction failure;
- artifact or backup restore failure.

## 13. Cost and resource controls

Budgets may be applied globally, by tenant, agent, conversation, run, and subagent:

- model tokens;
- estimated currency cost;
- wall-clock duration;
- turns and model calls;
- tool calls;
- concurrent Fibers;
- sandbox resources;
- retained bytes.

Budget exhaustion is a typed event and follows configured settlement policy. A
child may receive only a subset of its parent's remaining budget.

## 14. Incident response

Operators require documented actions to:

- stop new admission;
- pause scheduling without losing accepted work;
- revoke a provider, skill, tool, MCP server, or secret;
- fence all existing producers;
- abort selected work;
- quarantine corrupt conversations;
- inspect and resolve unknown outcomes;
- rotate keys;
- export an audit bundle;
- restore and reconcile a durable store.

Emergency actions are authenticated, authorized, idempotent, and audited.

## 15. Abuse and denial of service

The host bounds:

- submission size;
- schema depth and collection length;
- attachment count and size;
- tool output included in context;
- model event rate;
- queued events per subscriber;
- concurrent submissions by scope;
- recursive subagent count/depth;
- retries and recovery loops;
- MCP discovery size;
- compaction frequency.

Malformed provider or tool streams must not grow unbounded buffers.

## 16. Requirements

- **SEC-001**: All external callers are authenticated before durable admission.
- **SEC-002**: Tenant scope is preserved in storage, authorization, and telemetry.
- **SEC-003**: Authorization is re-evaluated at action time.
- **SEC-004**: Agent Runtimes are assembled with least authority.
- **SEC-005**: Tool risk, retry, interruption, and redaction metadata are explicit.
- **SEC-006**: Secrets use handles and never enter canonical events as raw values.
- **SEC-007**: Model and retrieved content are treated as untrusted.
- **SEC-008**: Sensitive telemetry is structurally redacted.
- **SEC-009**: Provider-returned reasoning is treated as sensitive canonical content; hidden
  chain-of-thought is neither requested nor invented.
- **SEC-010**: Untrusted execution uses a genuine isolation adapter.
- **SEC-011**: Security-relevant operator actions are auditable and idempotent.
- **SEC-012**: Resource and cost budgets are enforced hierarchically.
- **SEC-013**: All untrusted input and stream buffers have explicit bounds.
- **SEC-014**: Generated Code Mode programs execute only in an isolated executor with no ambient
  authority; host Tools are reachable only through the brokered, Schema-validated RPC surface.
- **SEC-015**: Read-only SQL exposure is enforced by database authority and host-owned tenant
  scoping, never by source-text inspection.
- **OPS-001**: Accepted work settlement age is measurable and alertable.
- **OPS-002**: Unknown outcomes produce an immediate operational signal.
- **OPS-003**: Durable deployments have incident, backup, restore, and
  reconciliation runbooks.

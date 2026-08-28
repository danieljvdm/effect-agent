# Security and Operations Specification

Status: Draft

Agents combine untrusted instructions, privileged credentials, external content, and
side-effecting tools. Security is therefore part of the runtime contract rather
than a collection of example-app concerns.

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

The engine rechecks optional host Tool authorization for model-declared application batches at
action time, after approval and before durable preparation or any Handler starts. The host receives
canonical Run/Turn/input authority and one exact Tool Call descriptor; it owns mutation
classification and freshness policy. A recovered
durable batch is rechecked with stable identity. Denial is typed, starts no Handler and creates no
new side effect in the denied Attempt, and terminally fails accepted work so recovery cannot retry
it again. Historical effects from a prior crashed Attempt remain governed by the Tool's recovery
contract. Authorization performed when instructions were first received is insufficient for a
later Tool Call.
Programmatic `ToolBroker` calls are outside this authorization hook.

Durable input [schedules](./scheduling.md) require an explicit `ScheduleAuthorizer` for every
management call and occurrence preparation. Reads and listing are scoped to a tenant-qualified
owner. `Scheduling` contains only management operations and explicit status projections. Driver
operations belong to the separate privileged `ScheduleDriver`; neither that service nor
`ScheduleStore` belongs in a management caller or model Tool environment. Preparation authorizes completion of that one frozen delivery even if authority is later
revoked; revocation prevents future preparation, not recovery of input that may already have been
admitted. This grant does not bypass the Tool action-time checks above. Persist only bounded policy
and decision identifiers, never credentials or input-bearing diagnostics, as scheduling metadata.

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

Every event field and live diagnostic is classified as one of:

- public metadata;
- tenant content;
- sensitive;
- secret;
- prohibited from persistence;
- prohibited from automatic persistence and telemetry export.

Redaction occurs before telemetry export and before optional debug capture.
Redaction is structural, based on Schema annotations and event types rather than
regular expressions alone.

Raw Effect Causes delivered through the opt-in Tool failure observer are sensitive live diagnostics
in the last category (RUN-036). The engine never serializes, persists, logs, span-attaches, or
forwards them to the model or public events. Only the explicitly installed trusted in-process
observer receives them. Its raw correlation IDs are not filtered as telemetry IDs; the service is
not an export boundary. Applications own any deliberate reporting and its redaction policy.
Declared failure messages and payloads never enter this observer, including inner Code Mode
results. Observer defects may reach the configured `ErrorReporter` through the isolated delivery
boundary; installing an observer does not automatically report application Tool Causes there.

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
Tool-broker RPC API over the construction-time allowlist. Source inspection and AST
normalization are usability checks, never the security boundary. Genuine runtime isolation,
least-authority bindings, and enforced limits are. No raw secret may be returned by a host Tool
or included in an executor binding, and the deterministic test substitute identifies itself as
`unisolated` rather than masquerading as a boundary.

Page capture output is a rendered web page and therefore untrusted, attacker-influenced input.
Interactive browser sessions require an explicit immutable network policy. `ExactHosts` retains
URL checks for adapter navigation and intercepted requests on the owned page. It does not classify
connection addresses or contain traffic from other targets, workers, sockets, or arbitrary viewer
commands. Use it only with trusted pages and operators. `PublicWeb` requires credential-free HTTPS
and connection-time exclusion of private, reserved, and internal destinations across all those
traffic sources, including redirects and human navigation. A provider that cannot enforce it must
fail before acquisition with `InteractiveBrowserUnsupportedError`, `feature: "policy"`. Cloudflare
currently does so. A wildcard, DNS preflight, or a consumer assertion cannot enable that mode.
`Unrestricted` is an explicit opt-out from URL/host and private-network containment. Private and
internal destinations may be reachable, including through redirects, resources, and human
navigation. It never enables `PublicWeb` and does not relax application authentication,
conversation authorization, takeover ownership, session limits, or resource cleanup.
See the [deployment coverage table](./deployment.md#browser-run-interactive-browser-sessions).
One scoped browser/context/page pass permits bounded navigate, read, fill, click, screenshot, and
scroll operations; concurrent
calls fail busy, and action, elapsed-time, and per-result byte limits fail at the first violation.
Handles are never persisted, replayed, reconnected for execution, or exposed to models. Explicit
closure remains available after interruption or a policy violation and invalidates all further
actions. Cloudflare Scope cleanup emits fixed warnings when the remote outcome is uncertain;
explicit close reports typed failure. Provider diagnostics remain host-only.

Cloudflare session IDs, handoff IDs, and Live View URLs are redacted host values. Live View URLs
grant access to the remote browser and must be treated as secrets. The generic browser handle
exposes none of these values or operator controls. Hosts authenticate and authorize recipients,
serialize human and agent control, and own any private cleanup metadata and durable action
receipts. A cleanup-only connection by session ID may terminate a leaked session, but must never
return a usable browser handle or replay an unresolved action. Handoff does not suspend the
application deadline or grant controller ownership. No provider cancellation or exactly-once
handoff guarantee is implied.
Tab mode narrows the viewer UI, not the operator's authorization. Human interaction bypasses the
framework action counter, and a viewer that connected before URL expiry can remain connected
until the browser closes. The host must limit viewer recipients to trusted operators and close
the session to end their access.
The capture capability is deny-by-default: an immutable construction-time HTTPS host allowlist
governs navigation, redirects, and every browser subrequest. The request Schema rejects malformed
URLs, non-HTTPS targets, and embedded credentials; discovered links must be absolute,
credential-free HTTP(S) URLs and grant no navigation authority. Responses are byte-bounded before
buffering. Selector scrape additionally bounds selector count, aggregate elements, attributes,
strings, and geometry, and rejects malformed or excessive provider output rather than truncating it.
Rendered JavaScript may mutate
remote state, so capture Tools remain `uncertain`, are not automatically replayed, and cannot
enter readonly-only Code Mode. Structured extraction accepts only a bounded object JSON Schema;
the request rejects malformed or unsupported root and nested keywords, excessive encoded bytes,
excessive depth, oversized collections, and cycles before any provider can run. Browser RPC
authority is an explicit host-owned service. Extraction additionally requires an explicit
host authorization and accounting service for any platform-selected model provider, and results
are decoded through the caller's service-aware Effect Schema before use. Foreign provider failure
causes and inference-policy diagnostics remain host-only; the typed public policy error identifies
the provider and authorization or accounting step without copying the host's message.
Model-visible failures and cleanup logs carry only bounded,
fixed operation or HTTP-status descriptions and never interpolate foreign exception text,
rate-limit bodies, provider envelopes, or other remote error bodies.
A page that instructs the model is data, never authority.

Arbitrary CDP is not a constrained browser authority. The pinned Cloudflare client has no
session-wide egress guardrail, the provider's later hostname-only guardrail cannot distinguish
scheme or port, and page request interception can be disabled or bypassed by CDP commands that
create targets or fetch outside the page. Raw CDP therefore remains unsupported unless a future
provider boundary enforces the exact HTTPS origins or the host explicitly accepts a separately
named unrestricted browser authority.

Screenshot bytes are untrusted content. `PageScreenshot` bounds them before buffering and keeps
them out of canonical records, model context, logs, telemetry, and error diagnostics. Interactive
screenshots have the same output privacy rules. Puppeteer buffers their PNG before returning it;
the interactive adapter checks its signature and byte limit before releasing it to the caller,
but does not claim an incremental transport or provider-memory bound.

Crawl Markdown and metadata are likewise untrusted content. `PageCrawl` fixes one exact HTTPS host
and rejects off-host source or redirect metadata before emission. The Cloudflare adapter sends its
redacted API token only to the fixed `api.cloudflare.com` origin, bounds each response incrementally,
and never exposes or persists the provider job identity. Cloudflare retains crawl results for up to
14 days; callers that need a stricter retention boundary must not enable this provider. Public
diagnostics and cleanup warnings contain fixed operation descriptions rather than the token, foreign
response body, or provider envelope.

The read-only SQL reference Tool's guarantee is database authority, not SQL text inspection: a
database identity without mutation, DDL, administrative, or extension privileges; denial of
side-effecting functions reachable from a `SELECT`, including installed extensions and
user-defined functions, through an explicit execution allowlist or revocation evaluated under the
effective database identity (a restricted search path where the database has one); denial of
cross-database, filesystem, and network access; host-owned tenant scoping; exactly one statement
per call with bound parameters rather than interpolation; statement timeout and cancellation;
and maximum row, column, cell, and encoded-byte bounds. A raw `SELECT` prefix check is never
read-only enforcement. The adapter fails typed when it cannot prove or enforce the configured
policy.

### 9.1 Pull-request work-order authority

Review, implementation, and publication stay separate. The host contract is
[pull-request work orders](pr-work-orders.md). GitHub dispatch, isolation, and
network publication are [work-order ingress](pr-work-order-ingress.md). The
enabled work-order workflow runs model, untrusted repository checks, publisher,
and presenter in separate jobs. Pull-request code executes only in the
credential-free networkless check container; trusted base code authenticates
the GitHub-retained journal and owns atomic publication.

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

OpenTelemetry records:

- trace per admitted submission and attempt;
- spans for model calls, tool calls, approvals, compaction, storage, sandbox executions, and
  child runs;
- metrics for latency, tokens, estimated cost, failures, retries, queue age, Attempt ownership,
  uncertainty, and settlement age;
- structured logs for state transitions and operator actions.

High-cardinality IDs belong in traces/logs, not unrestricted metric labels.

Cloudflare journal row decoding, Conversation store encoding/decoding and failpoint wrappers, and
engine Tool/response identifier decoding retain their checks and typed failures without adding a
named span per value. Storage I/O and
meaningful operation spans and short events remain observable regardless of their duration. This
is instrumentation at operation boundaries, not runtime filtering or blanket removal of fast spans.

Opted-in native Conversation RPC tracing carries only current-span identity and sampling metadata
as a separate native argument. Stable binding and method names identify the call. Context is never
persisted or reused to parent alarms or resumed work; application observability owns those roots
and its sampling/export policy. See [the Cloudflare contract](deployment.md#native-conversation-rpc-tracing).

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
- **SEC-008**: Sensitive telemetry is structurally redacted. Raw Tool failure Causes are prohibited
  from automatic persistence and telemetry export; only an explicitly installed trusted local
  observer may receive them, without declared failure payloads (RUN-036).
- **SEC-009**: Provider-returned reasoning is treated as sensitive canonical content; hidden
  chain-of-thought is neither requested nor invented.
- **SEC-010**: Untrusted execution uses a genuine isolation adapter.
- **SEC-011**: Security-relevant operator actions are auditable and idempotent.
- **SEC-012**: Resource and cost budgets are enforced hierarchically.
- **SEC-013**: All untrusted input and stream buffers have explicit bounds.
- **SEC-014**: Generated Code Mode programs execute only in an isolated executor with no ambient
  authority; host Tools are reachable only through the brokered, Schema-validated RPC API.
- **SEC-015**: Read-only SQL exposure is enforced by database authority and host-owned tenant
  scoping, never by source-text inspection.
- **OPS-001**: Accepted work settlement age is measurable and alertable.
- **OPS-002**: Unknown outcomes produce an immediate operational signal.
- **OPS-003**: Durable deployments have incident, backup, restore, and
  reconciliation runbooks.

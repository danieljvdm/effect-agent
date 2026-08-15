# Threat Model

Status: Draft (descriptive; P7 WP5 deliverable)

This document is a descriptive STRIDE-style threat model for the framework as shipped. It is
**not** normative — [`docs/spec/security-operations.md`](spec/security-operations.md) is the
normative security contract, and this model references it. Findings from the P7 adversarial review
are triaged in [`docs/security/FINDINGS.md`](security/FINDINGS.md).

## Scope

**In scope:** the framework packages as shipped (`core`, `engine`, `capabilities`, `sandbox` /
`sandbox-local`, `session`, the storage adapters, and the Node/Cloudflare platform packages), plus
the reference assemblies used as internal agents — the Travel Planner, the docs-researcher, and the
repo-ops evidence auditor.

**Explicitly out of scope** (stated so their absence is honest, not silent):

- the **hosted Cloudflare service** — P7 does not claim a hosted control plane; the DC evidence is
  Miniflare/in-workerd only, and cross-DO isolation assumes one deployment's Objects trust each
  other (see the cross-DO boundary below and FINDINGS SEC-P7-002);
- **real supplier / provider APIs** — the supplier desk and travel services are deterministic
  fixtures; only the live _model_ profile touches a real network, opt-in and gated;
- **application-owned authentication** — the framework receives a typed `Principal` /
  `TenantContext`; it does not parse bearer tokens (SEC-001). Authentication of external callers is
  the host's responsibility;
- **artifact classification laundering** — recorded **N/A**: no `AttachmentStore` / artifact port
  exists in the framework as shipped, so there is no artifact surface to launder. Revisit
  when an attachment port is introduced during open-source preparation.

## Trust boundaries and principals

From security-operations §1, treat each of these as a distinct principal or trust zone. This model
walks each boundary that carries untrusted data or a privilege transition. STRIDE columns are
elided where a category has no meaningful instance for that boundary.

### 1. Model output (untrusted)

Text a model produces is untrusted input; it may attempt to induce privileged actions.

| STRIDE                 | Threat                                                             | Control                                                                                                                                                                                                                         |
| ---------------------- | ------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Tampering / Elevation  | Model emits a Tool Call for an undeclared or higher-risk operation | Toolkit is the closed set of callable Tools; a Tool cannot downgrade its declared risk at call time; high-risk Tools require approval or deterministic policy independent of model intent (SEC-005, security-operations §5/§6). |
| Elevation              | Model prose "asks" to release a secret or skip approval            | Secrets are opaque handles resolved late in the narrowest service; approval is a policy decision, never inferred from prose (SEC-006, CONTEXT.md "Approval").                                                                   |
| Information disclosure | Model echoes injected credential into output                       | Events are structurally redacted before telemetry/debug capture; output is bounded (SEC-008, §8).                                                                                                                               |

Evidence: `redteam-supplier-injection.test.ts`.

### 2. Tool results (untrusted)

Handler-returned content and retrieved documents are untrusted, exactly like model output.

| STRIDE                 | Threat                                                                | Control                                                                                                                                                       |
| ---------------------- | --------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Tampering              | A tool result carries embedded instructions ("call cancel_booking …") | Results are labeled tool output; the next Turn's tool calls are still capability- and approval-gated; retrieved content cannot register tools or Layers (§6). |
| DoS                    | An oversized tool result grows unbounded context                      | Output size limits and canonical persistence bounds; malformed streams do not grow unbounded buffers (SEC-013, §15).                                          |
| Information disclosure | A tool result echoes a secret into an event/span                      | Structural redaction strips scalars from previews (`Redactor`, SEC-008).                                                                                      |

Evidence: `redteam-supplier-injection.test.ts`.

### 3. MCP discovery / content (untrusted server)

An MCP server is a distinct trust zone; its discovery response and served content are untrusted.

| STRIDE    | Threat                                                                         | Control                                                                                                                                                                                                                                                                             |
| --------- | ------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Spoofing  | Server returns a different identity than requested                             | `validateMcpDiscovery` fails closed on identity mismatch.                                                                                                                                                                                                                           |
| Tampering | Served tool schemas drift from the authored toolkit                            | Discovery is digest-verified against the authored `Toolkit` byte-for-byte (`toolkitSchemaDigest`, CAP-009); a mismatch fails closed before the toolkit is registered.                                                                                                               |
| DoS       | Over-many tools, oversized descriptions, or a deeply-nested tool `inputSchema` | Tool-count (≤128), per-description-bytes, and total-discovery-bytes (≤1 MB) bounds are enforced. **Gap:** canonicalization is not depth-bounded — a deeply-nested schema can stack-overflow the connecting fiber before the byte bound applies (**FINDINGS SEC-P7-001**, deferred). |
| Elevation | Retrieved MCP content registers a tool/Layer                                   | Retrieved content cannot dynamically register tools or Layers (§6); MCP tools enter only through the validated, digest-pinned toolkit.                                                                                                                                              |

Evidence: `docs-researcher.test.ts` (CAP-009 bounds), FINDINGS SEC-P7-001.

### 4. Supplier content (untrusted)

Supplier catalog/desk replies are untrusted third-party content (modeled deterministically).

| STRIDE                | Threat                                                     | Control                                                                                                                                                                           |
| --------------------- | ---------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Tampering / Elevation | Supplier reply embeds instructions to mutate or exfiltrate | Consequential mutations are approval-gated; the handler never starts without a decision; a denial settles the Run failed with no supplier call (security-operations §6, SEC-005). |
| Repudiation           | A fabricated booking is claimed as settled                 | Never-fabricate assertion: every settled booking result must reference a booking that exists at the supplier (`assertSettledBookingsExistAtSupplier`, P5 exit gate).              |

Evidence: `redteam-supplier-injection.test.ts`, `travel-planner-phase5.test.ts`.

### 5. Traveler PII / tenant content

Traveler data is tenant content requiring isolation, redaction, and least exposure.

| STRIDE                 | Threat                                                           | Control                                                                                                                        |
| ---------------------- | ---------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| Information disclosure | PII leaks into telemetry, spans, or debug capture                | Structural redaction before export; every event field is classified (public/tenant/sensitive/secret/prohibited) (§8, SEC-008). |
| Information disclosure | A subagent child leaks its context to the parent (or vice versa) | Context isolation: a child sees only its projected brief; only the declared projection crosses back (SUB-006/SUB-015).         |

Evidence: `redteam-child-exfiltration.test.ts`, `docs-researcher.test.ts`.

### 6. Cross-tenant paths

One principal must not discover, collide with, or reach another principal's work.

| STRIDE                 | Threat                                                                                       | Control                                                                                                                                                                                                                                                                                                                                                                                 |
| ---------------------- | -------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Spoofing / Elevation   | A principal reuses another's idempotency key to hijack/replay a Submission                   | Admission keys and the store UNIQUE constraint are `(conversation, principal, idempotency key)`; a reused key under a different principal mints a distinct Submission — no replay, collision, or discovery (SEC-002).                                                                                                                                                                   |
| Information disclosure | A caller reads another tenant's Conversation via observe/explain/verify                      | A non-default `OperationAuthorizer` denies foreign targets fail-closed; every admin/observe operation consults it (D10). **Caveat:** the possession default (and the DC admin entry points) do not enforce tenant scope without a host authorizer (**FINDINGS SEC-P7-003**), and the submission-scoped operations pass no `conversationId` to the authorizer (**FINDINGS SEC-P7-004**). |
| Elevation              | Identifier knowledge used as a capability (IDOR) — e.g. a forged child at a derived identity | Establishment/join verify every immutable fact against the canonical `SubagentRequested`; a fabricated child fails Parent Link verification fail-closed (D10).                                                                                                                                                                                                                          |

Evidence: `redteam-idor-sweep.test.ts`, `crossPrincipalAdmissionScoping` conformance case,
`travel-planner-subagents-durable.test.ts` (fabricated-child IDOR).

### 7. Operator / administrative surface (new in P7 WP1)

The admin operations (`explain`, `verify`, `retry`, `wake`, `scanObligations`) and the DUR-017
resolution paths are a privileged surface.

| STRIDE                 | Threat                                             | Control                                                                                                                                                                                        |
| ---------------------- | -------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Elevation              | An unauthorized caller runs a mutating admin op    | Mutating ops carry mandatory `author`/`reason` audit fields (SEC-011); a non-default `OperationAuthorizer` gates every op fail-closed with a typed `OperationDenied` before any read or write. |
| Tampering              | `retry` re-drives settled or resolution-owned work | `retry` re-drives exactly the classifier's one decision and refuses (typed `RetryRefused`) settled work and lanes owned by `resolveUnknown`/`resolveApproval`.                                 |
| Repudiation            | An operator action leaves no trace                 | Every executed repair appends the deterministic `RepairAnnotated` audit record; operator actions carry principal + reason (SEC-011, §11).                                                      |
| Information disclosure | `explain`/`verify` read foreign state              | Authorizer-gated per boundary 6; `explain` is provably read-only (byte-identical durable state before/after).                                                                                  |

Evidence: `admin-operations.test.ts`, `redteam-idor-sweep.test.ts`, FINDINGS SEC-P7-003/004.

### 8. Storage (forged epochs / receipts)

The persistence administrator and a stale or malicious producer are distinct trust zones.

| STRIDE    | Threat                                                             | Control                                                                                                                                                                                        |
| --------- | ------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Spoofing  | A stale owner with an older Producer Epoch appends canonical state | Fencing: an append carrying a superseded epoch is rejected (`FenceRejected`); the canonical log is append-only and digest-chained.                                                             |
| Tampering | A forged or corrupted record breaks the log                        | `verifyConversationInvariants` recomputes the digest chain, record-identity uniqueness, FIFO order, and ledger↔canonical agreement; a break is a typed integrity finding, never a silent pass. |
| Spoofing  | A replayed receipt or approval is honored twice                    | Receipts are identifiers, not capabilities; admission, settlement, and approval decisions are idempotent per identity (conformance cases).                                                     |

Evidence: ledger conformance suite, `admin-operations.test.ts` (verify catches injected digest
break / identity duplicate).

### 9. Cross-DO transport envelopes (DC)

One Conversation's Durable Object executes another's request against its own local facets.

| STRIDE    | Threat                                                   | Control                                                                                                                                                                                                                                                                                                                |
| --------- | -------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Tampering | A malformed envelope corrupts the owner Object           | Every crossing value is a closed Schema union (`PortRequest`/`PortResponse`); an undecodable request answers `PortFailed(PortProtocolError)`, never a throw; diagnostics are bounded.                                                                                                                                  |
| Spoofing  | A non-answer is treated as proof of absence              | An unreachable authority answers `AdmissionIndeterminate`, never `NotAdmitted` — only `NotAdmitted` permits an admission attempt (SUB-031).                                                                                                                                                                            |
| Elevation | A caller reads/mutates a foreign lane through `portCall` | **Accepted risk:** `portCall` has no per-caller authorization — the `DurableObjectNamespace` binding is the trust boundary, and the closed subset is a system-to-system sibling-Object channel (**FINDINGS SEC-P7-002**). Operations outside the closed subset fail fast typed (honesty over accidental distribution). |

Evidence: cross-DO routing/port tests, FINDINGS SEC-P7-002.

### 10. Sandbox (untrusted workload)

Untrusted code or commands require a genuine isolation adapter.

| STRIDE    | Threat                                               | Control                                                                                                                                                                                                                                                                                              |
| --------- | ---------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Elevation | A workload escapes to the host                       | `@effect-agent/sandbox-local` is honestly labeled `isolation: "unisolated"` development-only tooling and fails closed on every isolation-requiring request (mounts, network, cpu/memory, secret handles, artifacts) (SEC-010, CAP-010, §9). A real isolation adapter is required for untrusted code. |
| Elevation | Command built from a shell string of model output    | Command and args are passed structurally, never a shell string (SEC-005); model-selected paths are normalized and policy-checked (`normalizeRepoRelativePath`).                                                                                                                                      |
| Tampering | Model-supplied report path traverses out of the jail | Path traversal (absolute, `..`, `.`, empty, backslash) is rejected fail-closed; the residual symlink consideration is **FINDINGS SEC-P7-006** (dev-only).                                                                                                                                            |

Evidence: `sandbox-local` tests, `examples/repo-ops` (evidence auditor), FINDINGS SEC-P7-006.

## Assumptions

- Ledger writes are atomic actions and recovery reads are strongly consistent (STORE-003); no
  Byzantine storage.
- `Principal` values are host-authenticated and free of the ledger's admission-key delimiter, so
  the principal segment of an admission key is unforgeable by a client-controlled idempotency key.
- One DC deployment's Conversation Objects mutually trust each other over the cross-DO port; the
  namespace binding is the isolation boundary (see boundary 9).
- The unisolated local sandbox is used only for trusted development commands; untrusted workloads
  require a real isolation adapter that this repository does not ship.

## Change log

- P7 WP5: initial descriptive model covering the ten trust boundaries of security-operations §1,
  with the operator/admin surface (new in WP1) and the cross-DO transport (new in P6) added, and
  artifact laundering recorded N/A because no artifact port ships.

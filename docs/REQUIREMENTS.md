# Requirements Index

Status: Draft

This index makes the specification executable by a large implementation project.
Each normative requirement has a stable ID. Issues, pull requests, tests, and
release evidence reference those IDs.

## Families

| Prefix       | Area                                       | Normative source                                       | First roadmap phase |
| ------------ | ------------------------------------------ | ------------------------------------------------------ | ------------------: |
| `AUTH`       | Agent and Tool authoring                   | [Authoring](spec/authoring.md)                         |               P0–P1 |
| `RUN`        | Interpreter and event runtime              | [Runtime](spec/runtime.md)                             |               P0–P2 |
| `MODEL`      | Model drivers and routing                  | [Providers](spec/providers.md)                         |               P0–P2 |
| `CAP`        | Optional capabilities                      | [Capabilities](spec/capabilities.md)                   |               P1–P5 |
| `SUB`        | Subagent delegation and child lifecycle    | [Subagents](spec/subagents.md)                         |            Proposed |
| `STORE`      | Persistence and adapters                   | [Persistence](spec/persistence.md)                     |               P3–P7 |
| `DUR`        | Accepted work and recovery                 | [Durability](spec/durability.md)                       |               P4–P7 |
| `DEPLOY`     | Hosts and deployment classes               | [Deployment](spec/deployment.md)                       |               P1–P7 |
| `SEC`, `OPS` | Security and operations                    | [Security and operations](spec/security-operations.md) |               P1–P7 |
| `TEST`       | Verification                               | [Testing](spec/testing.md)                             |         Every phase |
| `COMPAT`     | Interoperability, boundaries, and versions | [Compatibility](spec/compatibility.md)                 |               P0–P7 |

## Requirement lifecycle

A requirement is:

- **specified** when its normative source defines behavior and failure semantics;
- **planned** when a roadmap phase owns it;
- **implemented** when code and public documentation exist;
- **verified** when automated evidence covers it;
- **released** when a compatibility manifest names the package/adapter version.

Implementation status belongs in generated project tracking, not by editing the
normative wording from “must” to “done.”

## Traceability record

Each implementation issue includes:

```yaml
requirements:
  - RUN-004
  - TEST-003
decision_dependencies:
  - D-007
adrs:
  - ADR-0005
packages:
  - packages/engine
verification:
  - packages/engine/test/tool-scheduling.test.ts
```

Each pull request states which requirement behavior changed and which test provides
evidence. A requirement without an executable test must have a documented reason,
owner, and future gate.

## Cross-cutting release claims

### Effect-native core

Evidence must cover all `AUTH`, the P1 subset of `RUN`, the base `MODEL` contract,
Scope finalization, type inference, and `COMPAT-001` through `COMPAT-005`.

### Persistent conversation

Evidence must cover `STORE` replay/checkpoint requirements. It must explicitly state
that persistence is not durable accepted work.

### Durable Node/SQLite (DN)

Evidence must cover all applicable `DUR`, SQLite `STORE` conformance, process-kill
tests, Node `DEPLOY` requirements, `SEC`, and operational runbooks.

### Durable Cloudflare (DC)

Evidence additionally covers Durable Object eviction, alarm retry, SQLite-backed
storage, Effect service boundaries around bindings, and Cloudflare fault scenarios.

## Change control

- Do not reuse a retired requirement ID.
- Semantic changes update the normative source and relevant ADR.
- Splitting a requirement creates new IDs and retires the old ID with a pointer.
- Pure editorial changes retain the ID.
- Generated coverage tooling fails on duplicate or unknown IDs.

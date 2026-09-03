# Repo-ops: the evidence auditor

This leaf workspace is P7 internal agent #2 (plan §6): a genuinely useful
repository agent that reads evidence documents, extracts the test titles they
cite (the repository convention cites executable tests as curly-quoted titles
next to a test-file path), and verifies each citation against the tree.

What it exercises, deliberately:

- **Sandbox capability.** Every read is a structural `SandboxRequest`
  (`/bin/ls`, `/bin/cat`, `/usr/bin/grep`) through `@effect-agent/sandbox-local`,
  which labels itself `unisolated`. This is development tooling, not a security
  boundary (CAP-010). Network is disabled, no environment crosses, output is
  bounded, and model-supplied paths are normalized and fail closed.
- **`ToolExecutionClass` annotations.** `list_repository_path` declares
  `readonly`; `audit_evidence` stays unannotated (fail-closed `uncertain`) and
  instead proves re-entry with one named Durable Step per document
  called `audit:{document}`. The step records once, while the read-only body may execute again.
  `write_audit_report` declares `idempotent` and is the only mutating Tool.
- **Approval gating on the DN runtime.** The report write suspends on the
  canonical `ToolApprovalRequested` record until `resolveApproval`; the file
  exists only after the approved decision is canonical.
- **DN assembly and CLI Tool use.** The deterministic offline profile
  drives the agent as accepted work on `NodeDurableAgentRuntime` over a SQLite file
  and a fixture repository tree in a temp directory; the opt-in live profile
  (`EFFECT_AGENT_LIVE=1` + `OPENAI_API_KEY`, the shared P7 gate from
  `@effect-agent/testing`) runs a real model over the actual repository.

The ordinary test suite makes no network request and requires no credentials.

See `FRICTION.md` for the authoring-friction notes this agent feeds into the
P7 API-simplification work package.

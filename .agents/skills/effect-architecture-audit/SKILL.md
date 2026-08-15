---
name: effect-architecture-audit
description: Audit an Effect TypeScript codebase's services, Layers, dependency ownership, hidden requirements, test substitutes, Schema boundaries, assertions, unknown values, and typed error channels. Use for architecture reviews, service refactors, dependency-injection audits, and type-boundary investigations; do not use as a general Effect API reference.
---

# Audit Effect Architecture

Read the target repository's `node_modules/effect/AGENTS.md` completely before
evaluating Effect code. Follow its version-matched references for library APIs;
use this skill only for the application-architecture judgments it does not own.

Read [service-and-boundary-audit.md](references/service-and-boundary-audit.md),
then:

1. Establish the repository's local architecture and testing rules.
2. Inventory every service, Layer, dependency path, runtime authority, test
   substitute, and unsafe type boundary in scope.
3. Trace each capability to its owner and composition root.
4. Classify each candidate as a built-in capability, application authority,
   technology adapter, explicit value, framework boundary, or pass-through
   abstraction.
5. Report evidence-backed findings and explicit keep decisions. Do not propose
   a wrapper, service, or Schema merely to make the inventory symmetrical.

When asked only to audit or review, stop after the findings. Implement changes
only when the user also requests them.

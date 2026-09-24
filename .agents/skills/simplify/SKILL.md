---
name: simplify
description: Simplify code or workflows, including explicitly requested pre-production removal of legacy compatibility.
---

# Simplify

Start from the requested outcome and what it actually requires. Before calling
work done, review the existing or newly built solution: what is unnecessary,
overly complicated, or based on weak assumptions? Challenge those assumptions.

Prefer deleting over simplifying, simplifying over optimizing, and optimizing
over automating. First look for dependencies, alternate paths, or handoffs that
can be removed entirely. Then reassess what remains: what becomes simpler once
those pieces are gone?

Make justified changes within scope; do not stop at recommendations. If the
solution already serves the outcome well and no worthwhile simplification is
supported, leave it alone and say so. A review need not produce a diff.

Distinguish inherited assumptions from user requirements and external contracts.
Check callers, stored data, and operational dependencies before removing behavior.
If a proposed deletion would change the assignment, make that choice explicit.

Keep changes within the affected workflow. Use a reversible candidate diff and
relevant checks when dependencies are uncertain. Preserve necessary validation,
compatibility, and usable pending or error states. Fewer changed lines is not the
success criterion; less unnecessary machinery with required behavior intact is.

Optimize or automate only when the task calls for it. Do not turn an ordinary fix
into a broader simplification pass. Report the removed mechanism and any constraint
that required it to remain, when that explanation helps review the change.

## Optional mode

Only when the user explicitly requests pre-production removal of legacy or
compatibility paths, read [the no-legacy pass](references/no-legacy.md). Ordinary
simplification does not establish a greenfield premise or authorize that pass.

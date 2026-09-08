---
"@effect-agent/thread": patch
---

Allow application projection checkpoint callers to omit deprecated compatibility metadata while preserving and validating values in existing checkpoints.

BEHAVIOR CHANGE: Older binaries cannot read newly written metadata-free checkpoints, including with `verifyOnOpen`.

---
"@effect-agent/thread": patch
---

Leave untouched ready inputs for their normal worker claim while preserving interrupted input and marker repair. BEHAVIOR CHANGE: `runRecovery` and `recoverSubmission` report these inputs as `ApplyInput/deferred` until a worker runs them.

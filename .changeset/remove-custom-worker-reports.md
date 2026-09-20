---
"effect-agent": minor
---

Remove custom worker-report APIs and registration mappers. **BEHAVIOR CHANGE:** replace `Subagent.reporting` / `reportingToWorker` with `reportToParent: true` and map typed reports on receipt; drain unprepared custom-report work with its original release before upgrading.

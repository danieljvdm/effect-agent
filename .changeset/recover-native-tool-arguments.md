---
"effect-agent": patch
---

Return invalid native tool arguments to the model when the tool uses `failureMode: "return"`, before approval or handler execution. Preserve rejection evidence through durable recovery and allow corrected Code Mode arguments in the same run.

BEHAVIOR CHANGE: Custom durability hooks must persist `RunTurnResponseCommit.toolParameterRejections` and restore it through `RunTurnResume.toolParameterRejections`.

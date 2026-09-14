---
"effect-agent": patch
---

Persist completed Tool results before draining new inputs. An admission read failure after a Tool returns no longer loses its outcome, turns it into an unknown call, or blocks later submissions behind it. Preserve atomic no-tool and completion-Tool terminal commits.
